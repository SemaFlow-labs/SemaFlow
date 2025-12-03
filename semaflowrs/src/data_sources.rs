use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::dialect::{Dialect, DuckDbDialect};
use crate::error::{Result, SemaflowError};
use crate::executor::{ColumnMeta, QueryResult};
use crate::schema_cache::{ForeignKey, TableSchema};
use tokio::sync::Mutex;
use std::time::Instant;

/// Unified interface for all backends (DuckDB for now).
#[async_trait]
pub trait BackendConnection: Send + Sync {
    fn dialect(&self) -> &(dyn Dialect + Send + Sync);
    async fn fetch_schema(&self, table: &str) -> Result<TableSchema>;
    async fn execute_sql(&self, sql: &str) -> Result<QueryResult>;
}

/// Minimal connection manager keyed by data source name.
#[derive(Clone, Default)]
pub struct ConnectionManager {
    connections: HashMap<String, Arc<dyn BackendConnection>>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
        }
    }

    pub fn insert(&mut self, name: impl Into<String>, conn: Arc<dyn BackendConnection>) {
        self.connections.insert(name.into(), conn);
    }

    pub fn get(&self, name: &str) -> Option<&Arc<dyn BackendConnection>> {
        self.connections.get(name)
    }
}

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
        Self {
            database_path: path.as_ref().to_path_buf(),
            dialect: DuckDbDialect,
            limiter: Arc::new(Semaphore::new(16)),
            pool: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Configure maximum concurrent executions; callers can tune based on hardware.
    pub fn with_max_concurrency(mut self, max_in_flight: usize) -> Self {
        self.limiter = Arc::new(Semaphore::new(max_in_flight));
        self
    }

    async fn acquire_slot(&self) -> Result<SemaphorePermit<'_>> {
        self.limiter
            .acquire()
            .await
            .map_err(|e| SemaflowError::Execution(format!("limiter closed: {e}")))
    }

    async fn checkout_connection(&self) -> Result<duckdb::Connection> {
        if let Some(conn) = self.pool.lock().await.pop() {
            return Ok(conn);
        }
        duckdb::Connection::open(self.database_path.clone())
            .map_err(|e| SemaflowError::Execution(format!("open duckdb: {e}")))
    }
}

#[async_trait]
impl BackendConnection for DuckDbConnection {
    fn dialect(&self) -> &(dyn Dialect + Send + Sync) {
        &self.dialect
    }

    async fn fetch_schema(&self, table: &str) -> Result<TableSchema> {
        let table = table.to_string();
        let conn = self.checkout_connection().await?;
        let pool = self.pool.clone();
        let result = tokio::task::spawn_blocking(move || -> Result<(TableSchema, duckdb::Connection)> {
            let start = Instant::now();
            let mut conn = conn;

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
        let result = tokio::task::spawn_blocking(move || -> Result<(QueryResult, duckdb::Connection)> {
            let start = Instant::now();
            let mut conn = conn;
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
                    let value = crate::executor::duck_value_to_json(row.get_ref(idx)?.to_owned());
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
}
