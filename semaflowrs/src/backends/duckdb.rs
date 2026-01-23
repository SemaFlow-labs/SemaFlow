//! DuckDB backend implementation.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema};
use async_trait::async_trait;
use tokio::sync::{Mutex, Semaphore, SemaphorePermit};

use crate::config::DuckDbConfig;
use crate::dialect::DuckDbDialect;
use crate::error::{Result, SemaflowError};
use crate::executor::{ColumnMeta, PaginatedResult, QueryResult};
use crate::pagination::Cursor;
use crate::schema_cache::{ForeignKey, TableSchema};

use super::BackendConnection;

/// DuckDB connection implementing the unified backend trait.
#[derive(Clone)]
pub struct DuckDbConnection {
    database_path: PathBuf,
    dialect: DuckDbDialect,
    limiter: Arc<Semaphore>,
    pool: Arc<Mutex<Vec<duckdb::Connection>>>,
}

impl DuckDbConnection {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self::with_config(path, DuckDbConfig::default())
    }

    /// Create a DuckDB connection with configuration.
    pub fn with_config<P: AsRef<Path>>(path: P, config: DuckDbConfig) -> Self {
        let path = path.as_ref().to_path_buf();
        tracing::info!(
            path = %path.display(),
            max_concurrency = config.max_concurrency,
            "creating DuckDB connection"
        );
        Self {
            database_path: path,
            dialect: DuckDbDialect,
            limiter: Arc::new(Semaphore::new(config.max_concurrency)),
            pool: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Configure maximum concurrent executions; callers can tune based on hardware.
    pub fn with_max_concurrency(mut self, max_in_flight: usize) -> Self {
        tracing::debug!(
            max_concurrency = max_in_flight,
            "configuring DuckDB concurrency"
        );
        self.limiter = Arc::new(Semaphore::new(max_in_flight));
        self
    }

    async fn acquire_slot(&self) -> Result<SemaphorePermit<'_>> {
        let available = self.limiter.available_permits();
        if available == 0 {
            tracing::debug!("all DuckDB slots in use, waiting for permit");
        }
        self.limiter
            .acquire()
            .await
            .map_err(|e| SemaflowError::Execution(format!("limiter closed: {e}")))
    }

    async fn checkout_connection(&self) -> Result<duckdb::Connection> {
        let mut guard = self.pool.lock().await;
        if let Some(conn) = guard.pop() {
            let pool_size = guard.len();
            drop(guard);
            tracing::trace!(
                pool_remaining = pool_size,
                "reusing pooled DuckDB connection"
            );
            return Ok(conn);
        }
        drop(guard);
        tracing::debug!(path = %self.database_path.display(), "opening new DuckDB connection");
        duckdb::Connection::open(self.database_path.clone())
            .map_err(|e| SemaflowError::Execution(format!("open duckdb: {e}")))
    }

    /// Register an Arrow table in DuckDB by creating a table from schema and appending batches.
    ///
    /// This enables zero-copy registration of DataFrames (pandas/polars) passed as Arrow.
    pub async fn register_arrow_table(
        &self,
        table_name: &str,
        schema: &Schema,
        batches: Vec<RecordBatch>,
    ) -> Result<()> {
        let table_name = table_name.to_string();
        let schema = schema.clone();
        let conn = self.checkout_connection().await?;
        let pool = self.pool.clone();

        let result = tokio::task::spawn_blocking(move || -> Result<duckdb::Connection> {
            let start = Instant::now();

            // Generate CREATE TABLE statement from Arrow schema
            let create_sql = arrow_schema_to_create_table(&table_name, &schema);
            tracing::debug!(sql = %create_sql, "creating table from Arrow schema");
            conn.execute(&create_sql, [])
                .map_err(|e| SemaflowError::Execution(format!("create table: {e}")))?;

            // Append all record batches using DuckDB's Arrow appender
            {
                let mut appender = conn
                    .appender(&table_name)
                    .map_err(|e| SemaflowError::Execution(format!("create appender: {e}")))?;

                for batch in batches {
                    appender
                        .append_record_batch(batch)
                        .map_err(|e| SemaflowError::Execution(format!("append batch: {e}")))?;
                }
            }

            let elapsed = start.elapsed();
            tracing::debug!(
                table = table_name.as_str(),
                ms = elapsed.as_millis(),
                "registered Arrow table in DuckDB"
            );

            Ok(conn)
        })
        .await
        .map_err(|e| SemaflowError::Execution(format!("task join error: {e}")))?;

        let conn = result?;
        {
            let mut guard = pool.lock().await;
            guard.push(conn);
        }
        Ok(())
    }
}

/// Convert Arrow schema to DuckDB CREATE TABLE statement.
fn arrow_schema_to_create_table(table_name: &str, schema: &Schema) -> String {
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| {
            let duck_type = arrow_type_to_duckdb(f.data_type());
            format!("\"{}\" {}", f.name(), duck_type)
        })
        .collect();

    format!("CREATE TABLE \"{}\" ({})", table_name, columns.join(", "))
}

/// Map Arrow data types to DuckDB types.
fn arrow_type_to_duckdb(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Float16 | DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",
        DataType::Binary | DataType::LargeBinary => "BLOB",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Time32(_) | DataType::Time64(_) => "TIME",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        DataType::Interval(_) => "INTERVAL",
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "DECIMAL",
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => "VARCHAR", // fallback
        DataType::Struct(_) => "VARCHAR", // fallback
        _ => "VARCHAR", // safe fallback for unknown types
    }
}

#[async_trait]
impl BackendConnection for DuckDbConnection {
    fn dialect(&self) -> &(dyn crate::dialect::Dialect + Send + Sync) {
        &self.dialect
    }

    async fn fetch_schema(&self, table: &str) -> Result<TableSchema> {
        let table = table.to_string();
        let conn = self.checkout_connection().await?;
        let pool = self.pool.clone();
        let result =
            tokio::task::spawn_blocking(move || -> Result<(TableSchema, duckdb::Connection)> {
                let start = Instant::now();
                let conn = conn;

                let pragma_sql = format!("PRAGMA table_info('{table}')");
                let mut stmt = conn.prepare(&pragma_sql)?;
                let mut rows = stmt.query([])?;
                let mut columns = Vec::new();
                let mut primary_keys = Vec::new();
                while let Some(row) = rows.next()? {
                    let name: String = row.get("name")?;
                    let data_type: String = row.get("type")?;
                    let not_null: bool = row.get("notnull")?;
                    let pk_flag: bool = row.get("pk")?;
                    if pk_flag {
                        primary_keys.push(name.clone());
                    }
                    columns.push(crate::schema_cache::ColumnSchema {
                        name,
                        data_type,
                        nullable: !not_null,
                    });
                }

                let mut foreign_keys = Vec::new();
                let fk_sql = format!("PRAGMA foreign_key_list('{table}')");
                if let Ok(mut fk_stmt) = conn.prepare(&fk_sql) {
                    let mut fk_rows = fk_stmt.query([])?;
                    while let Some(row) = fk_rows.next()? {
                        let from_column: String = row.get("from")?;
                        let to_table: String = row.get("table")?;
                        let to_column: String = row.get("to")?;
                        foreign_keys.push(ForeignKey {
                            from_column,
                            to_table,
                            to_column,
                        });
                    }
                }

                let elapsed = start.elapsed();
                tracing::debug!(
                    table = table.as_str(),
                    ms = elapsed.as_millis(),
                    "duckdb fetch_schema"
                );
                Ok((
                    TableSchema {
                        columns,
                        primary_keys,
                        foreign_keys,
                    },
                    conn,
                ))
            })
            .await
            .map_err(|e| SemaflowError::Execution(format!("task join error: {e}")))?;

        let (schema, conn) = result?;
        {
            let mut guard = pool.lock().await;
            guard.push(conn);
        }
        Ok(schema)
    }

    async fn execute_sql(&self, sql: &str) -> Result<QueryResult> {
        let sql = sql.to_string();
        let _permit = self.acquire_slot().await?;
        let conn = self.checkout_connection().await?;
        let pool = self.pool.clone();
        let result =
            tokio::task::spawn_blocking(move || -> Result<(QueryResult, duckdb::Connection)> {
                let start = Instant::now();
                let conn = conn;
                let mut stmt = conn.prepare(&sql)?;
                let mut rows_iter = stmt.query([])?;
                let stmt_ref = rows_iter
                    .as_ref()
                    .ok_or_else(|| SemaflowError::Execution("statement missing".to_string()))?;
                let mut column_names = Vec::new();
                for idx in 0..stmt_ref.column_count() {
                    let name = stmt_ref
                        .column_name(idx)
                        .map_err(|e| SemaflowError::Execution(e.to_string()))?;
                    column_names.push(name.to_string());
                }
                let mut rows = Vec::new();
                while let Some(row) = rows_iter.next()? {
                    let mut map = serde_json::Map::new();
                    for (idx, name) in column_names.iter().enumerate() {
                        let value =
                            crate::executor::duck_value_to_json(row.get_ref(idx)?.to_owned());
                        map.insert(name.clone(), value);
                    }
                    rows.push(map);
                }

                let columns: Vec<_> = column_names
                    .into_iter()
                    .map(|name| ColumnMeta { name })
                    .collect();
                let elapsed = start.elapsed();
                tracing::debug!(
                    rows = rows.len(),
                    columns = columns.len(),
                    ms = elapsed.as_millis(),
                    "duckdb execute_sql"
                );
                Ok((QueryResult { columns, rows }, conn))
            })
            .await
            .map_err(|e| SemaflowError::Execution(format!("task join error: {e}")))?;

        let (result, conn) = result?;
        {
            let mut guard = pool.lock().await;
            guard.push(conn);
        }
        Ok(result)
    }

    async fn execute_sql_paginated(
        &self,
        sql: &str,
        page_size: u32,
        cursor: Option<&Cursor>,
        query_hash: u64,
    ) -> Result<PaginatedResult> {
        // Extract offset from cursor, validate query hash
        let offset = match cursor {
            Some(c) => {
                c.validate_query_hash(query_hash)?;
                c.offset()
            }
            None => 0,
        };

        // Fetch page_size + 1 to detect if more rows exist
        let fetch_limit = page_size as u64 + 1;
        let paginated_sql = format!("{sql} LIMIT {fetch_limit} OFFSET {offset}");

        tracing::debug!(
            page_size = page_size,
            offset = offset,
            "executing paginated DuckDB query"
        );

        // Execute the paginated query
        let result = self.execute_sql(&paginated_sql).await?;

        // Determine if there are more rows
        let has_more = result.rows.len() > page_size as usize;
        let rows = if has_more {
            result.rows.into_iter().take(page_size as usize).collect()
        } else {
            result.rows
        };

        // Build next cursor if there are more rows
        let next_cursor = if has_more {
            let next_offset = offset + page_size as u64;
            let cursor = Cursor::sql(next_offset, query_hash);
            Some(cursor.encode()?)
        } else {
            None
        };

        Ok(PaginatedResult {
            columns: result.columns,
            rows,
            cursor: next_cursor,
            has_more,
            total_rows: None, // DuckDB doesn't provide total count efficiently
        })
    }
}
