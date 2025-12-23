//! Configuration system for Semaflow.
//!
//! Supports TOML-based configuration with global defaults and per-datasource overrides.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{Result, SemaflowError};

/// Root configuration structure.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct SemaflowConfig {
    /// Global defaults applied to all datasources unless overridden.
    pub defaults: GlobalDefaults,

    /// Per-datasource configuration overrides (keyed by datasource name).
    #[serde(default)]
    pub datasources: HashMap<String, DatasourceConfig>,
}

/// Global default settings.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
#[derive(Default)]
pub struct GlobalDefaults {
    pub query: QueryConfig,
    pub pool: PoolConfig,
    pub schema_cache: SchemaCacheConfig,
    pub validation: ValidationConfig,
}

/// Query execution configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct QueryConfig {
    /// Query timeout in milliseconds (default: 30000).
    pub timeout_ms: u64,
    /// Maximum rows to return (0 = unlimited).
    pub max_row_limit: u64,
    /// Default row limit when not specified in request.
    pub default_row_limit: u64,
}

/// Connection pooling configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct PoolConfig {
    /// Maximum pool size (default: 16).
    pub size: usize,
    /// Idle connection timeout in seconds (default: 300).
    pub idle_timeout_secs: u64,
}

/// Schema cache configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct SchemaCacheConfig {
    /// Cache TTL in seconds (default: 3600).
    pub ttl_secs: u64,
    /// Maximum cached schemas (default: 1000).
    pub max_size: usize,
}

/// Validation configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
#[derive(Default)]
pub struct ValidationConfig {
    /// Continue on validation errors (default: false).
    pub warn_only: bool,
}

/// Per-datasource configuration (can override globals).
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct DatasourceConfig {
    pub query: Option<QueryConfig>,
    pub pool: Option<PoolConfig>,
    pub schema_cache: Option<SchemaCacheConfig>,

    /// BigQuery-specific options.
    pub bigquery: Option<BigQueryConfig>,

    /// DuckDB-specific options.
    pub duckdb: Option<DuckDbConfig>,

    /// PostgreSQL-specific options.
    pub postgres: Option<PostgresConfig>,
}

/// BigQuery-specific configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct BigQueryConfig {
    /// Enable query cache (default: true).
    pub use_query_cache: bool,
    /// Maximum bytes billed per query (0 = unlimited).
    pub maximum_bytes_billed: i64,
    /// Query timeout in milliseconds (overrides query.timeout_ms for BigQuery).
    pub query_timeout_ms: u64,
    /// Maximum concurrent queries to BigQuery (default: 40).
    /// Prevents overwhelming BigQuery and provides backpressure.
    pub max_concurrent_queries: usize,
    /// Maximum time (ms) to wait in queue before rejecting (default: 5000).
    /// When all slots are in use, requests wait up to this duration.
    /// Set to 0 for unlimited wait (not recommended for production).
    pub queue_timeout_ms: u64,
}

/// DuckDB-specific configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct DuckDbConfig {
    /// Maximum concurrent queries (default: 16).
    pub max_concurrency: usize,
}

/// PostgreSQL-specific configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct PostgresConfig {
    /// Connection pool size (overrides pool.size for Postgres).
    pub pool_size: usize,
    /// Statement timeout in milliseconds.
    pub statement_timeout_ms: u64,
}

// Default implementations

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            timeout_ms: 30_000,
            max_row_limit: 0, // 0 = unlimited
            default_row_limit: 1000,
        }
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            size: 16,
            idle_timeout_secs: 300,
        }
    }
}

impl Default for SchemaCacheConfig {
    fn default() -> Self {
        Self {
            ttl_secs: 3600,
            max_size: 1000,
        }
    }
}

impl Default for BigQueryConfig {
    fn default() -> Self {
        Self {
            use_query_cache: true,
            maximum_bytes_billed: 0, // 0 = unlimited
            query_timeout_ms: 30_000,
            max_concurrent_queries: 30,
            queue_timeout_ms: 1_500, // ~5× base latency for fast rejection
        }
    }
}

impl Default for DuckDbConfig {
    fn default() -> Self {
        Self {
            max_concurrency: 16,
        }
    }
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            pool_size: 16,
            statement_timeout_ms: 30_000,
        }
    }
}

impl SemaflowConfig {
    /// Load configuration from a TOML file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let contents = std::fs::read_to_string(path.as_ref())
            .map_err(|e| SemaflowError::Config(format!("failed to read config file: {e}")))?;
        toml::from_str(&contents)
            .map_err(|e| SemaflowError::Config(format!("failed to parse config: {e}")))
    }

    /// Load configuration from a TOML string.
    pub fn from_toml(toml_str: &str) -> Result<Self> {
        toml::from_str(toml_str)
            .map_err(|e| SemaflowError::Config(format!("failed to parse config: {e}")))
    }

    /// Load from default locations (env var, cwd, user config dir, or defaults).
    ///
    /// Search order:
    /// 1. `SEMAFLOW_CONFIG` environment variable
    /// 2. `./semaflow.toml` (current directory)
    /// 3. `~/.config/semaflow/config.toml` (user config dir)
    /// 4. Built-in defaults
    pub fn load_default() -> Self {
        // 1. Check environment variable for explicit path
        if let Ok(path) = std::env::var("SEMAFLOW_CONFIG") {
            if let Ok(cfg) = Self::from_file(&path) {
                tracing::info!(path = %path, "loaded config from SEMAFLOW_CONFIG");
                return cfg;
            }
        }

        // 2. Check current working directory
        if let Ok(cfg) = Self::from_file("semaflow.toml") {
            tracing::info!("loaded config from ./semaflow.toml");
            return cfg;
        }

        // 3. Check user config directory
        if let Some(config_dir) = dirs::config_dir() {
            let user_config = config_dir.join("semaflow").join("config.toml");
            if let Ok(cfg) = Self::from_file(&user_config) {
                tracing::info!(path = %user_config.display(), "loaded config from user config dir");
                return cfg;
            }
        }

        // 4. Return defaults
        tracing::debug!("no config file found, using defaults");
        Self::default()
    }

    /// Get resolved config for a specific datasource (merges global defaults).
    pub fn for_datasource(&self, name: &str) -> ResolvedDatasourceConfig {
        let ds_config = self.datasources.get(name);
        ResolvedDatasourceConfig::merge(&self.defaults, ds_config)
    }
}

/// Fully resolved configuration for a datasource (no Option fields).
#[derive(Debug, Clone)]
pub struct ResolvedDatasourceConfig {
    pub query: QueryConfig,
    pub pool: PoolConfig,
    pub schema_cache: SchemaCacheConfig,
    pub bigquery: BigQueryConfig,
    pub duckdb: DuckDbConfig,
    pub postgres: PostgresConfig,
}

impl ResolvedDatasourceConfig {
    fn merge(defaults: &GlobalDefaults, override_cfg: Option<&DatasourceConfig>) -> Self {
        match override_cfg {
            Some(ds) => Self {
                query: ds.query.clone().unwrap_or_else(|| defaults.query.clone()),
                pool: ds.pool.clone().unwrap_or_else(|| defaults.pool.clone()),
                schema_cache: ds
                    .schema_cache
                    .clone()
                    .unwrap_or_else(|| defaults.schema_cache.clone()),
                bigquery: ds.bigquery.clone().unwrap_or_default(),
                duckdb: ds.duckdb.clone().unwrap_or_default(),
                postgres: ds.postgres.clone().unwrap_or_default(),
            },
            None => Self {
                query: defaults.query.clone(),
                pool: defaults.pool.clone(),
                schema_cache: defaults.schema_cache.clone(),
                bigquery: BigQueryConfig::default(),
                duckdb: DuckDbConfig::default(),
                postgres: PostgresConfig::default(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let cfg = SemaflowConfig::default();
        assert_eq!(cfg.defaults.query.timeout_ms, 30_000);
        assert_eq!(cfg.defaults.pool.size, 16);
        assert_eq!(cfg.defaults.schema_cache.ttl_secs, 3600);
    }

    #[test]
    fn test_parse_toml() {
        let toml = r#"
[defaults.query]
timeout_ms = 60000
max_row_limit = 50000

[datasources.my_bq.bigquery]
use_query_cache = false
maximum_bytes_billed = 1073741824
"#;
        let cfg = SemaflowConfig::from_toml(toml).unwrap();
        assert_eq!(cfg.defaults.query.timeout_ms, 60_000);
        assert_eq!(cfg.defaults.query.max_row_limit, 50_000);

        let resolved = cfg.for_datasource("my_bq");
        assert!(!resolved.bigquery.use_query_cache);
        assert_eq!(resolved.bigquery.maximum_bytes_billed, 1073741824);
    }

    #[test]
    fn test_datasource_override() {
        let toml = r#"
[defaults.pool]
size = 8

[datasources.prod.pool]
size = 32
"#;
        let cfg = SemaflowConfig::from_toml(toml).unwrap();

        // Default datasource uses global
        let default_resolved = cfg.for_datasource("unknown");
        assert_eq!(default_resolved.pool.size, 8);

        // Named datasource uses override
        let prod_resolved = cfg.for_datasource("prod");
        assert_eq!(prod_resolved.pool.size, 32);
    }
}
