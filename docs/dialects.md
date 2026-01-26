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

```python
ds = DataSource.duckdb(":memory:", name="mem_db")
```

In-memory databases are ideal for **testing**, **interactive exploration**, and
**dynamic data workflows** where you want to query DataFrames through SemaFlow's
semantic layer.

#### Registering DataFrames (Recommended)

Use `register_dataframe()` to populate in-memory databases with pandas, polars, or
any Arrow-compatible data:

```python
import pyarrow as pa
import pandas as pd
from semaflow import DataSource, SemanticTable, SemanticFlow, FlowHandle, Dimension, Measure

# Create in-memory datasource
ds = DataSource.duckdb(":memory:", name="test")

# Create and register a DataFrame
df = pd.DataFrame({
    "order_id": [1, 2, 3],
    "amount": [100.0, 200.0, 150.0],
    "status": ["complete", "pending", "complete"]
})

# Register as Arrow (zero-copy transfer)
ds.register_dataframe("orders", pa.Table.from_pandas(df).to_reader())

# Define semantic layer
orders = SemanticTable(
    name="orders",
    data_source=ds,
    table="orders",
    primary_key="order_id",
    dimensions={"status": Dimension("status")},
    measures={"total": Measure("amount", "sum")}
)

flow = SemanticFlow(name="sales", base_table=orders, base_table_alias="o")
handle = FlowHandle.from_parts([orders], [flow], [ds])

# Query through semantic layer
result = await handle.execute({
    "flow": "sales",
    "dimensions": ["o.status"],
    "measures": ["o.total"]
})
# [{"o.status": "complete", "o.total": 250.0}, {"o.status": "pending", "o.total": 200.0}]
```

**Key points**:
- Pass data as a PyArrow `RecordBatchReader` (use `.to_reader()` on PyArrow Tables)
- Works with **pandas** (`pa.Table.from_pandas(df).to_reader()`)
- Works with **polars** (`df.to_arrow().to_reader()`)
- Works with any **Arrow-compatible library**
- Uses Arrow's C Data Interface for **zero-copy** data transfer
- You can register multiple tables on the same datasource

#### Why Not Python's duckdb Package?

⚠️ You **cannot** pre-populate an in-memory database using Python's `duckdb` package
and then query it via SemaFlow:

```python
import duckdb

# This WON'T work - separate library instances!
conn = duckdb.connect(":memory:")
conn.execute("CREATE TABLE orders ...")
conn.close()

ds = DataSource.duckdb(":memory:")  # This is a DIFFERENT empty database
```

This is because SemaFlow's Rust backend and Python's `duckdb` package link separate
DuckDB libraries. Named in-memory databases (`:memory:mydb`) also don't share across
library instances.

**Alternative - Temp File**: If you need to use Python's `duckdb` package directly:

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
