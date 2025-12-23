use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::config::SchemaCacheConfig;

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub from_column: String,
    pub to_table: String,
    pub to_column: String,
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub columns: Vec<ColumnSchema>,
    pub primary_keys: Vec<String>,
    pub foreign_keys: Vec<ForeignKey>,
}

/// Cache entry with timestamp for TTL tracking.
#[derive(Debug, Clone)]
struct CacheEntry {
    schema: TableSchema,
    inserted_at: Instant,
}

/// Schema cache with TTL and size limits.
#[derive(Debug)]
pub struct SchemaCache {
    schemas: HashMap<(String, String), CacheEntry>,
    ttl: Duration,
    max_size: usize,
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaCache {
    pub fn new() -> Self {
        Self::with_config(&SchemaCacheConfig::default())
    }

    /// Create a schema cache with configuration.
    pub fn with_config(config: &SchemaCacheConfig) -> Self {
        Self {
            schemas: HashMap::new(),
            ttl: Duration::from_secs(config.ttl_secs),
            max_size: config.max_size,
        }
    }

    pub fn insert(&mut self, data_source: String, table: String, schema: TableSchema) {
        // Evict oldest entry if at capacity
        if self.schemas.len() >= self.max_size {
            self.evict_oldest();
        }

        self.schemas.insert(
            (data_source, table),
            CacheEntry {
                schema,
                inserted_at: Instant::now(),
            },
        );
    }

    pub fn get(&self, data_source: &str, table: &str) -> Option<&TableSchema> {
        let key = (data_source.to_string(), table.to_string());
        self.schemas.get(&key).and_then(|entry| {
            if entry.inserted_at.elapsed() < self.ttl {
                Some(&entry.schema)
            } else {
                // Expired - treat as cache miss
                None
            }
        })
    }

    pub fn contains(&self, data_source: &str, table: &str) -> bool {
        self.get(data_source, table).is_some()
    }

    /// Remove expired entries from the cache.
    pub fn evict_expired(&mut self) {
        self.schemas
            .retain(|_, entry| entry.inserted_at.elapsed() < self.ttl);
    }

    /// Remove the oldest entry from the cache.
    fn evict_oldest(&mut self) {
        if let Some(oldest_key) = self
            .schemas
            .iter()
            .min_by_key(|(_, entry)| entry.inserted_at)
            .map(|(k, _)| k.clone())
        {
            tracing::debug!(
                data_source = %oldest_key.0,
                table = %oldest_key.1,
                "evicting oldest schema from cache"
            );
            self.schemas.remove(&oldest_key);
        }
    }

    /// Get current cache size.
    pub fn len(&self) -> usize {
        self.schemas.len()
    }

    /// Check if cache is empty.
    pub fn is_empty(&self) -> bool {
        self.schemas.is_empty()
    }

    /// Clear all cached schemas.
    pub fn clear(&mut self) {
        self.schemas.clear();
    }
}
