use std::collections::HashMap;
use std::sync::Arc;

use crate::dialect::{Dialect, DuckDbDialect};
use crate::executor::QueryExecutor;

#[derive(Clone)]
pub struct DataSource {
    pub dialect: Arc<dyn Dialect + Send + Sync>,
    pub executor: Arc<dyn QueryExecutor>,
}

impl DataSource {
    pub fn duckdb<E>(executor: E) -> Self
    where
        E: QueryExecutor + 'static,
    {
        Self {
            dialect: Arc::new(DuckDbDialect),
            executor: Arc::new(executor),
        }
    }
}

#[derive(Clone, Default)]
pub struct DataSourceRegistry {
    sources: HashMap<String, DataSource>,
}

impl DataSourceRegistry {
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
        }
    }

    pub fn insert(&mut self, name: impl Into<String>, source: DataSource) {
        self.sources.insert(name.into(), source);
    }

    pub fn get(&self, name: &str) -> Option<&DataSource> {
        self.sources.get(name)
    }
}
