# Dialect Reference

SemaFlow supports multiple database backends, each with dialect-specific SQL rendering.

## Supported Backends

| Backend | Feature Flag | Status | Identifier Quoting | Filtered Aggregates |
|---------|--------------|--------|-------------------|---------------------|
| DuckDB | `duckdb` (default) | ✓ Stable | `"column"` | ✓ `FILTER (WHERE ...)` |
| PostgreSQL | `postgres` | ✓ Stable | `"column"` | ✓ `FILTER (WHERE ...)` |
| BigQuery | `bigquery` | ✓ Stable | `` `column` `` | ✗ Uses `CASE WHEN` |

## Cross-Dialect Behavior

All backends share consistent behavior for:

- **Aggregations**: `sum`, `count`, `count_distinct`, `min`, `max`, `avg`
- **Logical operators**: `AND`, `OR`, `NOT`
- **Comparisons**: `=`, `!=`, `>`, `>=`, `<`, `<=`, `IN`, `LIKE`, `ILIKE`
- **CASE expressions**: Rendered identically across backends
- **`safe_divide(a, b)`**: Guarded division returning `NULL` when divisor is zero/NULL

## DuckDB

### File-Based Databases (Recommended)

```python
ds = DataSource.duckdb("path/to/database.duckdb", name="my_db")
```

**Features**:
- Embedded database (no server required)
- Connection pooling with semaphore-based backpressure (default: 16 concurrent)
- Full SQL:2003 aggregate filter support

### In-Memory Databases

⚠️ **Limitation**: `DataSource.duckdb(":memory:")` creates an **empty, isolated** database.

This is because SemaFlow's Rust backend and Python's `duckdb` package link separate
DuckDB libraries. They cannot share in-memory databases, even with named databases
like `:memory:mydb`.

**Workaround for testing**: Use a temporary file instead:

```python
import tempfile
import os
import duckdb

# Create a temp file
db_path = os.path.join(tempfile.gettempdir(), "test.duckdb")

# Populate with Python duckdb
conn = duckdb.connect(db_path)
conn.execute("CREATE TABLE orders (id INT, amount DECIMAL)")
conn.execute("INSERT INTO orders VALUES (1, 100.0)")
conn.close()

# Now use with semaflow
ds = DataSource.duckdb(db_path, name="test")
```

> **Future**: We're exploring ways to support true in-memory databases.

**Functions**:
| Category | Functions |
|----------|-----------|
| Date/Time | `date_trunc`, `date_part` |
| String | `lower`, `upper`, `concat`, `concat_ws`, `substring`, `length`, `trim`, `ltrim`, `rtrim` |
| Null handling | `coalesce`, `ifnull` |
| Math | `greatest`, `least` |
| Type | `cast` |

**`safe_divide` rendering**:
```sql
CASE WHEN b = 0 OR b IS NULL THEN NULL ELSE a / b END
```

## PostgreSQL

**Connection**:
```python
ds = DataSource.postgres(
    "postgresql://user:pass@localhost:5432/mydb",
    schema="public",
    name="pg_db"
)
```

**Features**:
- Async connection pooling via `deadpool-postgres`
- Native `FILTER (WHERE ...)` aggregate support
- Schema-qualified table references

**Functions**:
| Category | Functions |
|----------|-----------|
| Date/Time | `date_trunc`, `date_part`, `extract` |
| String | `lower`, `upper`, `concat`, `concat_ws`, `substring`, `length`, `trim`, `ltrim`, `rtrim` |
| Null handling | `coalesce`, `nullif` |
| Math | `greatest`, `least` |
| Type | `cast`, `::type` syntax |

**`safe_divide` rendering**:
```sql
CASE WHEN b = 0 OR b IS NULL THEN NULL ELSE a / b END
```

## BigQuery

**Connection**:
```python
# Using Application Default Credentials (recommended)
ds = DataSource.bigquery(
    project_id="my-gcp-project",
    dataset="analytics",
    name="bq"
)

# Or with explicit service account
ds = DataSource.bigquery(
    project_id="my-gcp-project",
    dataset="analytics",
    service_account_path="/path/to/key.json",
    name="bq"
)
```

**Features**:
- HTTP-based client (no connection pool)
- Concurrency limiting with queue timeout (default: 30 concurrent, 1.5s queue timeout)
- Query caching support (`use_query_cache` config option)
- Bytes billed limiting (`maximum_bytes_billed` config option)
- Cursor-based pagination using native BigQuery job pagination

**Functions**:
| Category | Functions |
|----------|-----------|
| Date/Time | `TIMESTAMP_TRUNC`, `DATE_TRUNC`, `EXTRACT` |
| String | `LOWER`, `UPPER`, `CONCAT`, `SUBSTR`, `LENGTH`, `TRIM`, `LTRIM`, `RTRIM` |
| Null handling | `COALESCE`, `IFNULL` |
| Math | `GREATEST`, `LEAST` |
| Type | `CAST`, `SAFE_CAST` |

**`safe_divide` rendering**:
```sql
SAFE_DIVIDE(a, b)  -- Native BigQuery function
```

**Note**: BigQuery does not support `FILTER (WHERE ...)` syntax for aggregates. SemaFlow automatically renders filtered measures using `CASE WHEN` wrapping:
```sql
-- Instead of: SUM(amount) FILTER (WHERE country = 'US')
-- BigQuery gets: SUM(CASE WHEN country = 'US' THEN amount END)
```

## Configuration

Backend-specific settings can be configured via TOML. See [Configuration Reference](configuration.md) for details.

```toml
# semaflow.toml
[defaults.query]
timeout_ms = 30000

[datasources.my_bq.bigquery]
use_query_cache = true
maximum_bytes_billed = 10737418240  # 10 GB
max_concurrent_queries = 40
queue_timeout_ms = 5000
```

## Feature Flags (Rust/Build)

```bash
# Fast iteration (no backends compiled)
cargo check --no-default-features

# Single backend
cargo check --features postgres

# All backends
cargo build --features all-backends

# Python wheel with all backends
maturin build --features all-backends,python
```
