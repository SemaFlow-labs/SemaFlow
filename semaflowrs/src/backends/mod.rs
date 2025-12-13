//! Database backend implementations.
//!
//! Each backend is implemented in its own file and gated behind a feature flag.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::dialect::Dialect;
use crate::error::Result;
use crate::executor::QueryResult;
use crate::schema_cache::TableSchema;

/// Unified interface for all database backends.
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

// Feature-gated backend implementations
#[cfg(feature = "duckdb")]
mod duckdb;
#[cfg(feature = "duckdb")]
pub use duckdb::DuckDbConnection;

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "postgres")]
pub use postgres::PostgresConnection;

#[cfg(feature = "bigquery")]
mod bigquery;
#[cfg(feature = "bigquery")]
pub use bigquery::BigQueryConnection;
