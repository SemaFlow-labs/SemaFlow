# SemaFlow Documentation

SemaFlow is a semantic layer engine that transforms business metric definitions into optimized SQL queries. It provides a Rust core with Python bindings, enabling high-performance query generation for analytics applications.

## Core Concepts

### Semantic Tables
Define your data sources with dimensions (categorical attributes) and measures (aggregatable metrics):

```yaml
# tables/orders.yaml
name: orders
data_source: duckdb_local
table: orders
primary_key: id
time_dimension: created_at

dimensions:
  customer_id:
    expression: customer_id
  created_at:
    expression: created_at

measures:
  order_total:
    expression: amount
    agg: sum
  order_count:
    expression: id
    agg: count
```

### Semantic Flows
Compose tables with joins to create queryable data models:

```yaml
# flows/sales.yaml
name: sales
base_table:
  semantic_table: orders
  alias: o
joins:
  c:
    semantic_table: customers
    to_table: o
    join_type: left
    join_keys:
      - left: customer_id
        right: id
```

### Query Requests
Request data using dimension and measure names:

```python
result = await handle.execute({
    "flow": "sales",
    "dimensions": ["c.country"],
    "measures": ["o.order_total", "c.customer_count"],
    "filters": [{"field": "c.country", "op": "==", "value": "US"}],
    "order": [{"column": "o.order_total", "direction": "desc"}],
    "limit": 10
})
```

## Documentation Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | High-level system architecture with diagrams |
| [rust-components.md](rust-components.md) | Detailed Rust module documentation |
| [query-lifecycle.md](query-lifecycle.md) | Request → SQL → Results data flow |
| [join-semantics.md](join-semantics.md) | Join types, filters, and NULL behavior |
| [python-bindings.md](python-bindings.md) | Python API reference |

## Quick Start

### Python (Recommended)

```python
from semaflow import DataSource, FlowHandle

# DuckDB (default backend)
handle = FlowHandle.from_dir(
    "path/to/flows",
    [DataSource.duckdb("analytics.duckdb")]
)

# PostgreSQL backend
handle = FlowHandle.from_dir(
    "path/to/flows",
    [DataSource.postgres("postgres://user:pass@localhost/db", name="postgres_db")]
)

# BigQuery backend
handle = FlowHandle.from_dir(
    "path/to/flows",
    [DataSource.bigquery("project-id", "dataset", name="bq_source")]
)

# Or define programmatically
from semaflow import SemanticTable, SemanticFlow, Dimension, Measure

orders = SemanticTable(
    name="orders",
    data_source=ds,
    table="orders",
    primary_key="id",
    dimensions={"amount": Dimension("amount")},
    measures={"total": Measure("amount", "sum")}
)
```

### Building with Feature Flags

```bash
# Fast iteration on core logic (~1-2 seconds)
cargo check --no-default-features

# Test specific backend
cargo check --features postgres

# Build with all backends
cargo build --features all-backends
```

### FastAPI Integration

```python
from semaflow import FlowHandle
from semaflow.api import create_app

handle = FlowHandle.from_dir("flows/", [...])
app = create_app(handle)
# Exposes: GET /flows, GET /flows/{name}, POST /flows/{name}/query
```

## Key Features

- **Multi-table measures**: Query measures from multiple joined tables in a single request
- **Automatic pre-aggregation**: Detects fanout risk and generates optimized CTEs
- **Multi-backend support**: DuckDB (default), PostgreSQL, and BigQuery with dialect-aware SQL generation
- **Type-safe SQL AST**: Builds queries programmatically, not via string concatenation
- **Async execution**: Non-blocking query execution with connection pooling
- **Feature flags**: Fast development builds with optional backends (`--no-default-features`)
