use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::dialect::{Dialect, DuckDbDialect};
use crate::error::{Result, SemaflowError};
use crate::executor::{ColumnMeta, QueryResult};
use crate::schema_cache::{ForeignKey, TableSchema};

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
}

impl DuckDbConnection {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            database_path: path.as_ref().to_path_buf(),
            dialect: DuckDbDialect,
            limiter: Arc::new(Semaphore::new(16)),
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
}

#[async_trait]
impl BackendConnection for DuckDbConnection {
    fn dialect(&self) -> &(dyn Dialect + Send + Sync) {
        &self.dialect
    }

    async fn fetch_schema(&self, table: &str) -> Result<TableSchema> {
        let path = self.database_path.clone();
        let table = table.to_string();
        tokio::task::spawn_blocking(move || -> Result<TableSchema> {
            let conn = duckdb::Connection::open(path)?;

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

            Ok(TableSchema {
                columns,
                primary_keys,
                foreign_keys,
            })
        })
        .await
        .map_err(|e| SemaflowError::Execution(format!("task join error: {e}")))?
    }

    async fn execute_sql(&self, sql: &str) -> Result<QueryResult> {
        let path = self.database_path.clone();
        let sql = sql.to_string();
        let _permit = self.acquire_slot().await?;
        tokio::task::spawn_blocking(move || -> Result<QueryResult> {
            let conn = duckdb::Connection::open(path)?;
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

            let columns = column_names
                .into_iter()
                .map(|name| ColumnMeta { name })
                .collect();
            Ok(QueryResult { columns, rows })
        })
        .await
        .map_err(|e| SemaflowError::Execution(format!("task join error: {e}")))?
    }
}
