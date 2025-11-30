# SemaFlow

Rust-first semantic layer with Python bindings. Define semantic tables/flows, validate against backends, build SQL (AST + dialect), and execute asynchronously. Python stays thin; Rust does the heavy lifting.

## Quickstart (Python)

Install (DuckDB + FastAPI helpers):
```
uv add "semaflow[duckdb,api]"
```

```python
import asyncio
from pathlib import Path
from semaflow import DataSource, FlowHandle
from semaflow.api import create_app

ds = DataSource.duckdb("examples/demo_python.duckdb", name="duckdb_local")
flow = FlowHandle.from_dir(
    Path("examples/flows"), [ds], description="Demo sales semantic flow"
)
print(flow.list_flows())           # names + descriptions
schema = flow.get_flow("sales")    # dimensions/measures with qualified names + time metadata

# Note: FlowHandle.from_dir looks for `tables/` and `flows/` subfolders when present;
# if those are absent it will read any YAML files directly under the provided path.

request = {
    "flow": "sales",
    "dimensions": ["c.country"],
    "measures": ["o.order_total", "c.customer_count"],
    "order": [{"column": "o.order_total", "direction": "desc"}],
    "limit": 10,
}

async def demo():
    sql = await flow.build_sql(request)
    rows = await flow.execute(request)
    print(sql, rows)

asyncio.run(demo())

app = create_app(flow)  # FastAPI app with /flows, /flows/{flow}, /flows/{flow}/query
assert flow.get_flow("sales")  # handle contains the flow matching its key
```

- Or build everything in Python (no YAML) with `SemanticFlow` / `SemanticTable` classes; see `examples/python_objects_demo.py`.

## Architecture

See `ARCHITECTURE.md` for the full vision. Highlights:
- Rust core owns semantic state; Python holds handles only.
- `BackendConnection` + `ConnectionManager` pick dialect and execute; DuckDB implemented with concurrency limiter.
- SQL AST + `Dialect` renderer (DuckDB implemented). Alias-qualified field names (`alias.field`) are accepted.
- Validation uses schema cache; enforces columns/PK/time dimension/join keys/single data source per flow.
- Python bindings release the GIL and share a tokio runtime; FastAPI helper layers sit on top.

## Components
- **Expr DSL**: columns, literals, CASE, binary ops, common functions, aggregations.
- **SQL builder**: `SqlBuilder::build_for_request` → SQL AST → dialect render.
- **Runtime**: `run_query` executes via backend connection with backpressure (DuckDB semaphore now).
- **Python**: `SemanticFlow` definitions (built from `SemanticTable`, `Dimension`, `Measure` classes) plus `FlowHandle` (async build_sql/execute + list_flows/get_flow), `FastAPIBridge`/`semaflow.api` helpers.
- **Introspection**: `list_flow_summaries`/`flow_schema` surface qualified dimensions/measures with descriptions/time metadata (joins stay internal).

## Python API Modes

- Define tables with classes for autocomplete/type safety:
  ```python
  ds = DataSource.duckdb("examples/demo_python.duckdb", name="duckdb_local")
  orders = SemanticTable(
      name="orders",
      data_source=ds,
      table="orders",
      primary_key="id",
      dimensions={
          "id": Dimension("id", description="Order primary key"),
          "customer_id": Dimension("customer_id", description="FK to customers"),
      },
      measures={
          "order_total": Measure("amount", "sum", description="Total order amount"),
      },
  )
  customers = SemanticTable(
      name="customers",
      data_source=ds,
      table="customers",
      primary_key="id",
      dimensions={
          "id": Dimension("id", description="Customer primary key"),
          "country": Dimension("country", description="Customer country"),
      },
      measures={"customer_count": Measure("id", "count", description="Count of customers")},
  )
  sales_flow = SemanticFlow(
      name="sales",
      base_table=orders,
      base_table_alias="o",
      joins=[
          FlowJoin(
              semantic_table=customers,
              alias="c",
              to_table="o",
              join_type="left",
              join_keys=[JoinKey("customer_id", "id")],
          )
      ],
      description="Sales data for the company",
  )
  flow = build_flow_handles({"sales": sales_flow})
  sql = await flow.build_sql({"flow": "sales", "dimensions": ["c.country"], "measures": ["o.order_total"]})
  rows = await flow.execute({"flow": "sales", "dimensions": ["c.country"], "measures": ["o.order_total"]})
  ```
- `semaflow.api.create_app` accepts either a `FlowHandle` or a dict of flow definitions (`{"sales": sales_flow}`). It builds/validates handles internally so API routes can call `build_flow_handles` automatically. Example in `examples/semantic_api.py`.
- Use the top-level helper only for one-offs: `from semaflow import build_sql, run` (Python module). These re-parse tables/flows/data_sources every call, which is convenient for ad hoc use but not for per-request server code.

## Semantic Layer Reference

**Semantic tables (`tables/*.yaml`)**
| Field | Expected values | Example |
| --- | --- | --- |
| `name`, `data_source`, `table`, `primary_key` | Required; `data_source` must match a registered connection name. | `duckdb_local` |
| `time_dimension`, `smallest_time_grain` | Optional; grain is one of `day`, `week`, `month`, `quarter`, `year`. | `created_at`, `month` |
| `dimensions.<name>` | Either a column string (`"country"`) or object `{expression, data_type?, description?}`. | `country` or a lower() func |
| `measures.<name>.expr` | Column/expression; `agg` required (`sum`, `count`, `count_distinct`, `min`, `max`, `avg`). | `amount` + `agg: sum` |

**Semantic flows (`flows/*.yaml`)**
| Field | Expected values | Example |
| --- | --- | --- |
| `name` | Unique flow name. | `sales` |
| `base_table` | `{semantic_table, alias}` pointing at a table file. | `orders` as alias `o` |
| `joins.<alias>` | `{semantic_table, alias, to_table, join_type, join_keys[]}`; `join_type` in `inner|left|right|full`; `join_keys` are `{left,right}` column names on the referenced aliases. | join customers `c` to `o` |

**Query requests (Python/FastAPI)**
| Field | Expected values | Example |
| --- | --- | --- |
| `flow` | Flow name. | `sales` |
| `dimensions`, `measures` | Lists of field names; may be qualified (`alias.field`). | `["c.country"]` |
| `filters` | Row-level only; list of `{field, op, value}` where `field` is a dimension. | `{"field": "c.country", "op": "in", "value": ["US","CA"]}` |
| `order` | `{column, direction}`; `direction` is `asc`/`desc`. | `{"column": "o.order_total", "direction": "desc"}` |
| `limit`, `offset` | Optional pagination integers. | `limit: 100` |

**Operators for expressions/filters**
| Context | Allowed values | Example |
| --- | --- | --- |
| Filters `op` | `==`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not in`, `like`, `ilike`. | `{"op": "==", "value": "US"}` |
| Measure `agg` | `sum`, `count`, `count_distinct`, `min`, `max`, `avg`. | `agg: count_distinct` |
| Binary expressions | `add`, `subtract`, `multiply`, `divide`, `modulo` (`type: binary`). | `{"type":"binary","op":"add","left":"gross","right":"tax"}` |
| Functions | `date_trunc(grain)`, `date_part(field)`, `lower`, `upper`, `coalesce`, `if_null`, `now`, `concat`, `concat_ws(sep)`, `substring`, `length`, `greatest`, `least`, `cast(data_type)`, `trim/ltrim/rtrim`. | `type: func`, `func: {date_trunc: month}`, `args: ["created_at"]` |

## Development
- Tests: `cargo test` (Rust). Python feature build: `maturin develop -m semaflowrs/Cargo.toml -F python`.
- First-time build with `uv`: `uv` will try to build the project (and compile bundled DuckDB) before running anything, which takes a few minutes and shows `Preparing packages...`. To avoid repeated rebuilds:
  1) `uv venv && source .venv/bin/activate`
  2) `uv pip install maturin`
  3) `maturin develop -m semaflowrs/Cargo.toml -F python --locked`
  4) When running later commands, use `uv run --no-sync ...` to skip the automatic rebuild.
- Examples:
  - Python demo (YAML): `uv run --no-sync python examples/python_demo.py`
  - Python demo (pure objects): `uv run --no-sync python examples/python_objects_demo.py`
  - FastAPI: `uv run --no-sync python examples/semantic_api.py` then use `/flows`, `/flows/{flow}`, `/flows/{flow}/query`

## Roadmap (short)
- Add dialects/backends (Postgres/BigQuery/Snowflake/Trino).
- Planner: join pruning, grain inference, subquery handling.
- Per-backend pooling/backpressure knobs; optional result caching.
- Harden Python async surface and packaging for extras per backend.
