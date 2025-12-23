# Configuration Reference

SemaFlow supports TOML-based configuration for query execution, connection pooling, schema caching, and backend-specific settings.

## Configuration Loading

SemaFlow searches for configuration in this order:

1. `SEMAFLOW_CONFIG` environment variable (explicit path)
2. `./semaflow.toml` (current directory)
3. `~/.config/semaflow/config.toml` (user config directory)
4. Built-in defaults

```python
from semaflow import Config

# Load from default locations
config = Config.load()

# Load from specific file
config = Config.from_file("/path/to/semaflow.toml")

# Parse from string
config = Config.from_toml("""
[defaults.query]
timeout_ms = 60000
""")

# Pass to FlowHandle
handle = FlowHandle.from_dir("flows/", data_sources, config=config)
```

## Configuration File Format

```toml
# semaflow.toml

# ═══════════════════════════════════════════════════════════════════
# Global Defaults
# Applied to all datasources unless overridden
# ═══════════════════════════════════════════════════════════════════

[defaults.query]
timeout_ms = 30000          # Query timeout (default: 30000ms)
max_row_limit = 0           # Maximum rows returned, 0 = unlimited
default_row_limit = 1000    # Default limit when not specified

[defaults.pool]
size = 16                   # Connection pool size (default: 16)
idle_timeout_secs = 300     # Idle connection timeout (default: 300s)

[defaults.schema_cache]
ttl_secs = 3600             # Cache TTL (default: 3600s / 1 hour)
max_size = 1000             # Maximum cached schemas (default: 1000)

[defaults.validation]
warn_only = false           # Continue on validation errors (default: false)

# ═══════════════════════════════════════════════════════════════════
# Per-Datasource Overrides
# Override global defaults for specific datasources
# ═══════════════════════════════════════════════════════════════════

# DuckDB datasource example
[datasources.my_duckdb]
[datasources.my_duckdb.query]
timeout_ms = 60000          # Override query timeout

[datasources.my_duckdb.duckdb]
max_concurrency = 8         # Max concurrent queries (default: 16)

# PostgreSQL datasource example
[datasources.my_postgres]
[datasources.my_postgres.pool]
size = 32                   # Larger pool for Postgres

[datasources.my_postgres.postgres]
pool_size = 32              # Connection pool size
statement_timeout_ms = 60000 # Statement timeout

# BigQuery datasource example
[datasources.my_bigquery]
[datasources.my_bigquery.bigquery]
use_query_cache = true             # Enable BigQuery cache (default: true)
maximum_bytes_billed = 10737418240 # 10 GB limit (0 = unlimited)
query_timeout_ms = 120000          # BQ-specific timeout (default: 30000)
max_concurrent_queries = 40        # Concurrent query limit (default: 30)
queue_timeout_ms = 5000            # Wait time in queue (default: 1500ms)
```

## Configuration Options

### Query Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `timeout_ms` | u64 | 30000 | Query execution timeout in milliseconds |
| `max_row_limit` | u64 | 0 | Maximum rows to return (0 = unlimited) |
| `default_row_limit` | u64 | 1000 | Default limit when not specified in request |

### Pool Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `size` | usize | 16 | Maximum connection pool size |
| `idle_timeout_secs` | u64 | 300 | Idle connection timeout in seconds |

### Schema Cache Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `ttl_secs` | u64 | 3600 | Cache entry TTL in seconds |
| `max_size` | usize | 1000 | Maximum number of cached schemas |

### Validation Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `warn_only` | bool | false | Log validation errors as warnings instead of failing |

### DuckDB Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `max_concurrency` | usize | 16 | Maximum concurrent queries |

### PostgreSQL Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `pool_size` | usize | 16 | Connection pool size |
| `statement_timeout_ms` | u64 | 30000 | Statement timeout in milliseconds |

### BigQuery Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `use_query_cache` | bool | true | Use BigQuery's query result cache |
| `maximum_bytes_billed` | i64 | 0 | Maximum bytes billed per query (0 = unlimited) |
| `query_timeout_ms` | u64 | 30000 | Query timeout in milliseconds |
| `max_concurrent_queries` | usize | 30 | Maximum concurrent queries to BigQuery |
| `queue_timeout_ms` | u64 | 1500 | Maximum wait time in queue when at capacity |

## Programmatic Configuration

Instead of TOML files, you can configure settings in Python:

```python
from semaflow import Config, FlowHandle, DataSource

config = Config()

# Global settings
config.set_query_timeout_ms(60000)
config.set_max_row_limit(100000)
config.set_default_row_limit(500)
config.set_pool_size(32)
config.set_pool_idle_timeout_secs(600)
config.set_schema_cache_ttl_secs(7200)
config.set_schema_cache_max_size(2000)
config.set_validation_warn_only(True)

# Per-datasource BigQuery settings
config.set_bigquery_config(
    datasource_name="my_bq",
    use_query_cache=True,
    maximum_bytes_billed=10 * 1024 * 1024 * 1024,  # 10 GB
    query_timeout_ms=120000
)

# Per-datasource PostgreSQL settings
config.set_postgres_config(
    datasource_name="my_pg",
    pool_size=32,
    statement_timeout_ms=60000
)

# Per-datasource DuckDB settings
config.set_duckdb_config(
    datasource_name="my_duck",
    max_concurrency=8
)

# Use the config
handle = FlowHandle.from_dir("flows/", data_sources, config=config)
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `SEMAFLOW_CONFIG` | Path to configuration TOML file |

## Example Configurations

### Development

```toml
# semaflow.toml - Development settings
[defaults.query]
timeout_ms = 60000          # Longer timeout for debugging
max_row_limit = 10000       # Reasonable limit

[defaults.validation]
warn_only = true            # Don't fail on validation issues

[datasources.dev.duckdb]
max_concurrency = 4         # Lower concurrency for local dev
```

### Production BigQuery

```toml
# semaflow.toml - Production BigQuery
[defaults.query]
timeout_ms = 30000
max_row_limit = 100000

[defaults.schema_cache]
ttl_secs = 7200             # 2 hour cache

[datasources.prod_bq.bigquery]
use_query_cache = true
maximum_bytes_billed = 53687091200  # 50 GB limit
max_concurrent_queries = 50
queue_timeout_ms = 10000    # 10s queue wait for high traffic
```

### Multi-Backend

```toml
# semaflow.toml - Multiple backends with different settings
[defaults.query]
timeout_ms = 30000

# Fast local DuckDB for prototyping
[datasources.local.duckdb]
max_concurrency = 16

# Production Postgres
[datasources.prod_pg.pool]
size = 64
[datasources.prod_pg.postgres]
pool_size = 64
statement_timeout_ms = 30000

# BigQuery warehouse
[datasources.warehouse.bigquery]
use_query_cache = true
maximum_bytes_billed = 107374182400  # 100 GB
max_concurrent_queries = 100
```
