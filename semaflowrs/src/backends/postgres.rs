//! PostgreSQL backend implementation.

use std::time::Instant;

use async_trait::async_trait;

use crate::dialect::PostgresDialect;
use crate::error::{Result, SemaflowError};
use crate::executor::{ColumnMeta, QueryResult};
use crate::schema_cache::{ForeignKey, TableSchema};

use super::BackendConnection;

pub struct PostgresConnection {
    pool: deadpool_postgres::Pool,
    schema: String,
    dialect: PostgresDialect,
}

impl PostgresConnection {
    /// Create a new PostgreSQL connection from a connection string.
    ///
    /// Supports both key-value format and URL format:
    /// - `"host=localhost user=postgres dbname=mydb"`
    /// - `"postgresql://user:pass@host/db"`
    pub fn new(connection_string: &str, schema: &str) -> Result<Self> {
        tracing::info!(schema = %schema, "creating PostgreSQL connection pool");

        let config: deadpool_postgres::Config = if connection_string.starts_with("postgres") {
            // URL format
            tracing::debug!("parsing PostgreSQL URL connection string");
            let mut cfg = deadpool_postgres::Config::new();
            cfg.url = Some(connection_string.to_string());
            cfg
        } else {
            // Key-value format - parse manually
            tracing::debug!("parsing PostgreSQL key-value connection string");
            let mut cfg = deadpool_postgres::Config::new();
            for part in connection_string.split_whitespace() {
                if let Some((key, value)) = part.split_once('=') {
                    match key {
                        "host" => cfg.host = Some(value.to_string()),
                        "port" => cfg.port = value.parse().ok(),
                        "user" => cfg.user = Some(value.to_string()),
                        "password" => cfg.password = Some(value.to_string()),
                        "dbname" => cfg.dbname = Some(value.to_string()),
                        _ => {}
                    }
                }
            }
            cfg
        };

        let pool = config
            .create_pool(
                Some(deadpool_postgres::Runtime::Tokio1),
                tokio_postgres::NoTls,
            )
            .map_err(|e| {
                tracing::error!(error = %e, "failed to create PostgreSQL pool");
                SemaflowError::Execution(format!("create postgres pool: {e}"))
            })?;

        tracing::info!(
            schema = %schema,
            max_size = pool.status().max_size,
            "PostgreSQL connection pool created"
        );

        Ok(Self {
            pool,
            schema: schema.to_string(),
            dialect: PostgresDialect,
        })
    }

    /// Set the maximum pool size.
    /// Note: Currently a no-op - pool size must be set at creation time.
    pub fn with_pool_size(self, _size: usize) -> Self {
        // Deadpool doesn't allow runtime pool resizing
        // Pool size should be configured in the connection string or at creation
        self
    }

    /// Get the schema this connection uses.
    pub fn schema(&self) -> &str {
        &self.schema
    }
}

#[async_trait]
impl BackendConnection for PostgresConnection {
    fn dialect(&self) -> &(dyn crate::dialect::Dialect + Send + Sync) {
        &self.dialect
    }

    async fn fetch_schema(&self, table: &str) -> Result<TableSchema> {
        let start = Instant::now();
        let pool_status = self.pool.status();
        tracing::debug!(
            available = pool_status.available,
            size = pool_status.size,
            max_size = pool_status.max_size,
            "acquiring PostgreSQL connection for schema fetch"
        );

        let client = self.pool.get().await.map_err(|e| {
            tracing::error!(error = %e, table = %table, "failed to get PostgreSQL connection");
            SemaflowError::Execution(format!("get postgres connection: {e}"))
        })?;

        // Query columns from information_schema
        let columns_sql = r#"
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
        "#;
        let column_rows = client
            .query(columns_sql, &[&self.schema, &table])
            .await
            .map_err(|e| SemaflowError::Execution(format!("fetch columns: {e}")))?;

        let mut columns = Vec::new();
        for row in &column_rows {
            let name: String = row.get(0);
            let data_type: String = row.get(1);
            let is_nullable: String = row.get(2);
            columns.push(crate::schema_cache::ColumnSchema {
                name,
                data_type,
                nullable: is_nullable == "YES",
            });
        }

        // Query primary keys
        let pk_sql = r#"
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = $1
                AND tc.table_name = $2
                AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
        "#;
        let pk_rows = client
            .query(pk_sql, &[&self.schema, &table])
            .await
            .map_err(|e| SemaflowError::Execution(format!("fetch primary keys: {e}")))?;

        let primary_keys: Vec<String> = pk_rows.iter().map(|row| row.get(0)).collect();

        // Query foreign keys
        let fk_sql = r#"
            SELECT kcu.column_name, ccu.table_name, ccu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.table_schema
            WHERE tc.table_schema = $1
                AND tc.table_name = $2
                AND tc.constraint_type = 'FOREIGN KEY'
        "#;
        let fk_rows = client
            .query(fk_sql, &[&self.schema, &table])
            .await
            .map_err(|e| SemaflowError::Execution(format!("fetch foreign keys: {e}")))?;

        let foreign_keys: Vec<ForeignKey> = fk_rows
            .iter()
            .map(|row| ForeignKey {
                from_column: row.get(0),
                to_table: row.get(1),
                to_column: row.get(2),
            })
            .collect();

        let elapsed = start.elapsed();
        tracing::debug!(
            table = table,
            schema = self.schema.as_str(),
            ms = elapsed.as_millis(),
            "postgres fetch_schema"
        );

        Ok(TableSchema {
            columns,
            primary_keys,
            foreign_keys,
        })
    }

    async fn execute_sql(&self, sql: &str) -> Result<QueryResult> {
        let start = Instant::now();
        let pool_status = self.pool.status();
        tracing::debug!(
            available = pool_status.available,
            size = pool_status.size,
            max_size = pool_status.max_size,
            sql_len = sql.len(),
            "acquiring PostgreSQL connection for query"
        );
        tracing::trace!(sql = %sql, "executing PostgreSQL query");

        let client = self.pool.get().await.map_err(|e| {
            tracing::error!(error = %e, "failed to get PostgreSQL connection");
            SemaflowError::Execution(format!("get postgres connection: {e}"))
        })?;

        let rows = client.query(sql, &[]).await.map_err(|e| {
            tracing::error!(error = %e, "PostgreSQL query execution failed");
            SemaflowError::Execution(format!("execute query: {e}"))
        })?;

        // Convert rows to JSON
        let mut result_rows = Vec::new();
        let mut columns: Vec<ColumnMeta> = Vec::new();

        if let Some(first_row) = rows.first() {
            // Get column metadata from first row
            columns = first_row
                .columns()
                .iter()
                .map(|col| ColumnMeta {
                    name: col.name().to_string(),
                })
                .collect();
        }

        for row in &rows {
            let mut map = serde_json::Map::new();
            for (idx, col) in row.columns().iter().enumerate() {
                let value = pg_value_to_json(row, idx, col);
                map.insert(col.name().to_string(), value);
            }
            result_rows.push(map);
        }

        let elapsed = start.elapsed();
        tracing::debug!(
            rows = result_rows.len(),
            columns = columns.len(),
            ms = elapsed.as_millis(),
            "postgres execute_sql"
        );

        Ok(QueryResult {
            columns,
            rows: result_rows,
        })
    }
}

/// Convert a PostgreSQL value to JSON.
fn pg_value_to_json(
    row: &tokio_postgres::Row,
    idx: usize,
    col: &tokio_postgres::Column,
) -> serde_json::Value {
    use serde_json::Value;
    use tokio_postgres::types::Type;

    // Handle types explicitly, with fallbacks for aggregates
    match col.type_() {
        &Type::BOOL => row
            .try_get::<_, Option<bool>>(idx)
            .ok()
            .flatten()
            .map(Value::Bool)
            .unwrap_or(Value::Null),
        &Type::INT2 => row
            .try_get::<_, Option<i16>>(idx)
            .ok()
            .flatten()
            .map(|v| Value::Number(v.into()))
            .unwrap_or(Value::Null),
        &Type::INT4 => row
            .try_get::<_, Option<i32>>(idx)
            .ok()
            .flatten()
            .map(|v| Value::Number(v.into()))
            .unwrap_or(Value::Null),
        &Type::INT8 => row
            .try_get::<_, Option<i64>>(idx)
            .ok()
            .flatten()
            .map(|v| Value::Number(v.into()))
            .unwrap_or(Value::Null),
        &Type::FLOAT4 => row
            .try_get::<_, Option<f32>>(idx)
            .ok()
            .flatten()
            .and_then(|v| serde_json::Number::from_f64(v as f64).map(Value::Number))
            .unwrap_or(Value::Null),
        &Type::FLOAT8 => row
            .try_get::<_, Option<f64>>(idx)
            .ok()
            .flatten()
            .and_then(|v| serde_json::Number::from_f64(v).map(Value::Number))
            .unwrap_or(Value::Null),
        &Type::TEXT | &Type::VARCHAR | &Type::BPCHAR | &Type::NAME => row
            .try_get::<_, Option<String>>(idx)
            .ok()
            .flatten()
            .map(Value::String)
            .unwrap_or(Value::Null),
        &Type::NUMERIC => {
            // NUMERIC/DECIMAL - try f64 first (works for most aggregates),
            // then fall back to i64 for whole numbers
            if let Ok(Some(v)) = row.try_get::<_, Option<f64>>(idx) {
                serde_json::Number::from_f64(v)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            } else if let Ok(Some(v)) = row.try_get::<_, Option<i64>>(idx) {
                Value::Number(v.into())
            } else {
                Value::Null
            }
        }
        _ => {
            // For unknown types, try common conversions in order
            if let Ok(Some(v)) = row.try_get::<_, Option<String>>(idx) {
                Value::String(v)
            } else if let Ok(Some(v)) = row.try_get::<_, Option<f64>>(idx) {
                serde_json::Number::from_f64(v)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            } else if let Ok(Some(v)) = row.try_get::<_, Option<i64>>(idx) {
                Value::Number(v.into())
            } else {
                Value::Null
            }
        }
    }
}
