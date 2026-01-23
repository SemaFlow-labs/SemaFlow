<p align="left">
  <img src=".github/assets/semaflow-logo.png" alt="SemaFlow" width="300">
</p>

**A lightweight, async semantic layer for applications, APIs, and AI agents**

SemaFlow is a semantic layer you integrate as a library — not a platform you operate. Built for **developers** first.

> **Think: dbt metrics, but as a Python library you import.**
> Or: *Cube, without the infrastructure.*

---

## Table of Contents

- [What is SemaFlow?](#what-is-semaflow)
- [Why SemaFlow?](#why-semaflow)
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [YAML Schema](#yaml-schema)
- [Documentation](#documentation)
- [Examples](#examples)
- [Development](#development)
- [Roadmap](#roadmap)

---

## What is SemaFlow?

SemaFlow is a **lightweight semantic layer** written in Rust with Python bindings.

Define your semantic layer:
- **Dimensions** — attributes to group by
- **Measures** — aggregations and formulas
- **Joins** — relationships between tables
- **Time grains** — temporal hierarchies

Then serve semantic queries from:
- REST APIs
- Microservices
- Dashboards
- AI / LLM agents

All with **async execution, backpressure, and predictable scalability**.

---

## Why SemaFlow?

Most semantic layers today are:
- BI-first, synchronous, heavy platforms
- Hard to embed in applications
- Unsafe under bursty traffic
- Not designed for AI or agent workloads

SemaFlow is built for a different world:
- Real-time APIs and microservices
- Agentic systems with concurrent requests
- Horizontal scaling with bounded concurrency

### Positioning

```
                         Heavy Platform
                             ▲
                             │
                 dbt SL      │         Cube
        Looker               │
                  Metriql    │
BI / Analytics               │
 ────────────────────────────┼────────────────────────────→ App / Infra
                             │
                             │        ★ SemaFlow
                             │    (lightweight library)
         BSL                 │
(Boring semantic layer)      │
                             ▼
                       Library / SDK
```

### Key Differentiators

| Feature | Description |
|---------|-------------|
| 📦 Lightweight | No control plane, no external dependencies, minimal config |
| ⚡ Async by design | Rust async runtime with backpressure |
| 🔌 Library-first | Import and integrate — deploy however you want |
| 🤖 AI-friendly | Safe for agents, MCP, and LLM tools |
| 📈 Predictable scaling | Bounded concurrency, connection pooling |

---

## Features

- **Multi-backend support**: DuckDB, PostgreSQL, BigQuery
- **Semantic modeling**: Define dimensions, measures, and flows in YAML or Python
- **DataFrame registration**: Query pandas/polars DataFrames via in-memory DuckDB (zero-copy Arrow)
- **Automatic optimization**: Join pruning, pre-aggregation CTEs, fanout detection
- **Type-safe SQL generation**: AST-based SQL building with dialect-aware rendering
- **Async execution**: Non-blocking queries with connection pooling
- **Built-in API**: FastAPI integration for instant REST endpoints
- **Cursor pagination**: Efficient page-by-page results (native BigQuery job pagination)
- **Formula expressions**: Complex measures with inline aggregations (`formula: "sum(a) / count(b)"`)

---

## Installation

```bash
pip install semaflow

# With FastAPI support
pip install "semaflow[api]"
```

---

## Quick Start

### From YAML Files

```python
from semaflow import DataSource, FlowHandle

# Connect to your database
ds = DataSource.duckdb("analytics.duckdb", name="warehouse")
# Or: DataSource.postgres("postgresql://user:pass@host/db", schema="public", name="warehouse")
# Or: DataSource.bigquery(project_id="my-project", dataset="analytics", name="warehouse")

# Load semantic definitions
handle = FlowHandle.from_dir("flows/", [ds])

# Execute queries
result = await handle.execute({
    "flow": "sales",
    "dimensions": ["c.country"],
    "measures": ["o.order_total", "o.order_count"],
    "filters": [{"field": "c.country", "op": "==", "value": "US"}],
    "order": [{"column": "o.order_total", "direction": "desc"}],
    "limit": 100
})
```

### From Python Objects

```python
from semaflow import (
    DataSource, SemanticTable, SemanticFlow,
    Dimension, Measure, FlowJoin, JoinKey, build_flow_handles
)

ds = DataSource.duckdb("sales.duckdb", name="warehouse")

orders = SemanticTable(
    name="orders",
    data_source=ds,
    table="fct_orders",
    primary_key="order_id",
    dimensions={"status": Dimension("status")},
    measures={"total": Measure("amount", "sum")}
)

customers = SemanticTable(
    name="customers",
    data_source=ds,
    table="dim_customers",
    primary_key="customer_id",
    dimensions={"country": Dimension("country")},
    measures={"count": Measure("customer_id", "count")}
)

flow = SemanticFlow(
    name="sales",
    base_table=orders,
    base_table_alias="o",
    joins=[FlowJoin(customers, "c", "o", [JoinKey("customer_id", "customer_id")])]
)

handle = build_flow_handles({"sales": flow})
```

### From In-Memory DataFrames

Query pandas or polars DataFrames through SemaFlow's semantic layer:

```python
import pyarrow as pa
import pandas as pd
from semaflow import DataSource, SemanticTable, SemanticFlow, FlowHandle, Dimension, Measure

# Create in-memory DuckDB
ds = DataSource.duckdb(":memory:", name="test")

# Register a DataFrame (zero-copy via Arrow)
df = pd.DataFrame({"id": [1, 2, 3], "amount": [100.0, 200.0, 150.0], "status": ["a", "b", "a"]})
ds.register_dataframe("orders", pa.Table.from_pandas(df).to_reader())

# Define semantic layer and query
orders = SemanticTable(name="orders", data_source=ds, table="orders", primary_key="id",
                       dimensions={"status": Dimension("status")},
                       measures={"total": Measure("amount", "sum")})
flow = SemanticFlow(name="test", base_table=orders, base_table_alias="o")
handle = FlowHandle.from_parts([orders], [flow], [ds])

result = await handle.execute({"flow": "test", "dimensions": ["o.status"], "measures": ["o.total"]})
```

### REST API

```python
from semaflow import FlowHandle
from semaflow.api import create_app

handle = FlowHandle.from_dir("flows/", [ds])
app = create_app(handle)

# Endpoints:
# GET  /flows              - List available flows
# GET  /flows/{name}       - Get flow schema
# POST /flows/{name}/query - Execute query
```

---

## YAML Schema

### Semantic Table (`tables/orders.yaml`)

```yaml
name: orders
data_source: warehouse
table: fct_orders
primary_key: order_id
time_dimension: order_date

dimensions:
  order_status:
    expr: status
    description: Order status

measures:
  # Simple measure
  order_total:
    expr: total_amount
    agg: sum
    description: Total order amount

  order_count:
    expr: order_id
    agg: count

  # Complex measure with formula
  avg_order_amount:
    formula: "order_total / order_count"
    description: Average order amount
```

### Semantic Flow (`flows/sales.yaml`)

```yaml
name: sales
base_table:
  semantic_table: orders
  alias: o
joins:
  customers:
    semantic_table: customers
    alias: c
    to_table: o
    join_type: left
    join_keys:
      - left: customer_id
        right: customer_id
```

---

## Documentation

| Guide | Description |
|-------|-------------|
| [Core Concepts](docs/concepts.md) | Data sources, tables, flows, queries |
| [Expressions](docs/expressions.md) | Filter syntax, simple & complex measures |
| [Python API](semaflowrs/docs/python-bindings.md) | Full Python API reference |
| [Dialects](docs/dialects.md) | DuckDB, PostgreSQL, BigQuery specifics |
| [Configuration](docs/configuration.md) | TOML configuration reference |
| [Join Semantics](semaflowrs/docs/join-semantics.md) | Join types, cardinality, NULL behavior |
| [Architecture](semaflowrs/docs/architecture.md) | System design and components |
| [Contributing](CONTRIBUTING.md) | Development setup, PR guidelines |

---

## Examples

See the [`examples/`](examples/) directory for complete working examples.

```bash
# DuckDB demo
cd examples/duckdb && uv run python demo.py

# PostgreSQL demo (requires Docker)
cd examples/postgres && docker compose up -d && uv run python demo.py

# FastAPI server
uv run python examples/semantic_api.py
# Then: curl http://localhost:8080/flows/sales
```

---

## Development

```bash
# Build Python bindings
uv run maturin develop

# Run Rust tests
cargo test --features all-backends

# Run Python tests
uv run pytest tests/

# Fast Rust iteration (no backends)
cargo check --no-default-features
```

---

## Roadmap

- [ ] Snowflake backend
- [ ] Trino backend
- [ ] Result caching layer
- [ ] Time-series helpers (period-over-period, rolling windows)
- [ ] TypeScript bindings
- [ ] WASM support

---

## Who is SemaFlow For?

- **Backend / platform engineers** building data APIs
- **SaaS teams** exposing metrics to customers
- **AI engineers** building agent tools and MCP servers
- **Data platform teams** building custom analytics UIs

---

## What SemaFlow Is *Not*

- ❌ Not a BI tool or dashboard platform
- ❌ Not a dbt replacement
- ❌ Not a metrics governance UI
- ❌ Not a heavy analytics gateway

---

## License

MIT
