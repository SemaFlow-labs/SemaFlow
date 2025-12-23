//! Database backend implementations.
//!
//! Each backend is implemented in its own file and gated behind a feature flag.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::config::{ResolvedDatasourceConfig, SemaflowConfig};
use crate::dialect::Dialect;
use crate::error::Result;
use crate::executor::{PaginatedResult, QueryResult};
use crate::pagination::Cursor;
use crate::schema_cache::TableSchema;

/// Unified interface for all database backends.
#[async_trait]
pub trait BackendConnection: Send + Sync {
    fn dialect(&self) -> &(dyn Dialect + Send + Sync);
    async fn fetch_schema(&self, table: &str) -> Result<TableSchema>;
    async fn execute_sql(&self, sql: &str) -> Result<QueryResult>;

    /// Execute SQL with pagination support.
    ///
    /// # Arguments
    /// * `sql` - The SQL query to execute (without LIMIT/OFFSET - those are added by implementation)
    /// * `page_size` - Number of rows per page
    /// * `cursor` - Optional cursor from a previous paginated response
    /// * `query_hash` - Hash of the query for cursor validation
    ///
    /// # Returns
    /// A `PaginatedResult` containing the current page of rows plus pagination metadata.
    ///
    /// Backends implement this differently:
    /// - BigQuery: Uses native job pagination (max_results + page_token)
    /// - SQL backends: Use LIMIT/OFFSET
    async fn execute_sql_paginated(
        &self,
        sql: &str,
        page_size: u32,
        cursor: Option<&Cursor>,
        query_hash: u64,
    ) -> Result<PaginatedResult>;
}

/// Minimal connection manager keyed by data source name.
#[derive(Clone, Default)]
pub struct ConnectionManager {
    connections: HashMap<String, Arc<dyn BackendConnection>>,
    config: Option<SemaflowConfig>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            config: None,
        }
    }

    /// Create a connection manager with configuration.
    pub fn with_config(config: SemaflowConfig) -> Self {
        Self {
            connections: HashMap::new(),
            config: Some(config),
        }
    }

    /// Get the configuration, if set.
    pub fn config(&self) -> Option<&SemaflowConfig> {
        self.config.as_ref()
    }

    /// Get resolved configuration for a specific datasource.
    pub fn config_for(&self, name: &str) -> ResolvedDatasourceConfig {
        match &self.config {
            Some(cfg) => cfg.for_datasource(name),
            None => SemaflowConfig::default().for_datasource(name),
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
