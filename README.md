# SemaFlow

Semantic layer core in Rust with Python bindings. Defines semantic tables and models, validates them against backends, builds SQL (currently DuckDB), and can execute queries. Everything IO-bound is async.

## Architecture at a Glance

- **Build/Definition**
  - Data sources: logical connections (DuckDB now; Postgres/BigQuery later).
  - Semantic tables: `name`, `data_source`, `table`, `primary_key`, `time_dimension`, dimensions, measures (Expr DSL), join keys, descriptions.
  - Semantic models: base table + joins to other semantic tables; optional always-filters.
  - Models dictionary: map of model name → semantic model; loaded from YAML or Python.
- **Startup/Deploy**
  - Load models dictionary into memory.
  - Initialize data source connections.
  - Retrieve table schemas from backends (schema cache keyed by `(data_source, table)`).
  - Validate semantic tables (fields, PK, time dimension, measure expressions).
  - Validate semantic models (join keys/types, single data source per model); fail or warn per config.
  - Expose API surface (planned): `/health`, `/list_models`, `/get_model`, `/{model}/query_model`.
- **Query/Runtime**
  - Request parsed into `QueryRequest` (dims, measures, filters, order, limit/offset).
  - Apply model always-filters; resolve joins from the graph.
  - Build internal `QueryPlan` (select/from/joins/where/group/order).
  - Compile to SQL via dialect (`DuckDbDialect` implemented).
  - Execute against backend executor; return rows as JSON-friendly structures.
- **Async**
  - Schema fetch and query execution are async; Python/Rust call paths are async-safe.

## Detailed Flow (Mermaid)

### Build / Definition phase
```mermaid
flowchart TD
  A[Start: User begins setup] --> B[Define Data Source Connections]
  B --> C{Define Semantic Tables<br/>(YAML or Python)}
  C --> C1[Set: table, primary key,<br/>dimensions, measures,<br/>time grain, descriptions]
  C --> C2[Reference a data source]
  C --> D{Define Semantic Models}
  D --> D1[Choose base Semantic Table]
  D --> D2[Add joins to other STs]
  D --> D3[Optional: set always filters]
  D --> E[Assemble Models Dictionary]
  E --> F[Expose models dictionary to API builder]
  F --> G[End of Build Phase]
```

### Startup / Deploy phase
```mermaid
flowchart TD
  A[API Starts] --> B[Load Models Dictionary into Memory]
  B --> C[Initialize Data Source Connections]
  C --> D[Retrieve Table Schemas from Backends]
  D --> E{Validate Semantic Tables}
  E --> E1[Check fields exist]
  E --> E2[Check primary key + time dimension]
  E --> E3[Validate measure expressions]
  E --> F{Validate Semantic Models}
  F --> F1[Verify join keys and types]
  F --> F2[Apply config: fail or warn]
  F --> G{Any Validation Failures?}
  G -->|Yes| H[Startup Error / Abort]
  G -->|No| I[API Marked Live]
  I --> J[Expose Endpoints:<br/>/health,<br/>/list_models,<br/>/get_model,<br/>/{model}/query_model]
  J --> K[Ready for Queries]
```

### Query / Runtime phase
```mermaid
flowchart TD
  A[POST /{model}/query_model] --> B[Lookup Semantic Model]
  B --> C{Model Found?}
  C -->|No| Z[Return 404]
  C -->|Yes| D[Parse Request JSON into QueryRequest]
  D --> E[Validate fields against Model:<br/>dims, measures, filters]
  E --> F[Apply Always Filters]
  F --> G[Resolve Required Joins]
  G --> H[Build Internal QueryPlan:<br/>select, from, joins,<br/>where, group_by]
  H --> I[Compile QueryPlan to SQL<br/>(current: DuckDB dialect)]
  I --> J[Execute SQL Async Against Backend]
  J --> K[Receive Results]
  K --> L[Serialize to JSON]
  L --> M[Return HTTP 200 Response]
```

## Components

- **Expr DSL**: columns, literals, CASE, binary ops, functions (date_trunc/part, lower/upper, coalesce/ifnull, now, concat/concat_ws, substring, length, greatest/least, trim/ltrim/rtrim, cast), aggregations (sum, count, count_distinct, min, max, avg).
- **SQL builder**: `SqlBuilder::build_for_request` renders a `QueryRequest` to SQL with dialect support (DuckDB now).
- **Validation**: schema checks; join alias uniqueness; join key existence; enforce single data source per model.
- **Runtime**: `run_query` builds SQL and dispatches to the registered executor for the model’s data source; DuckDB executor included.
- **Examples**: semantic definitions in `examples/models`, requests in `examples/requests`, shell helper `examples/run_print_sql.sh`, Rust demo `cargo run --example run_query`, Python demo `examples/python_demo.py`.
- **Python bindings** (feature `python`): PyO3 module `semaflow_core` exposes `build_sql` and `run` (validate + build + execute DuckDB). Wrapper package `semaflow` re-exports `build_sql`, `run_query`, and `load_models_from_dir` with type hints/docstrings.

## Development

- Tests: run `cargo test` from `semaflowrs/`.
  - Unit-style cases: `semaflowrs/tests/unit/`.
  - Integration/system cases: `semaflowrs/tests/integration/`.
  - Entry shims `tests/unit.rs` and `tests/integration.rs` keep Cargo’s discovery happy.
  - Filtering: `cargo test --lib` (unit-ish), `cargo test --tests` (all integration), or `cargo test --test duckdb_poc` for a specific integration file.
- Python: `uv run maturin develop -m semaflowrs/Cargo.toml -F python` to build editable bindings (ensure venv active).

## Running examples

- Print SQL from a request: `cargo run --example print_sql -- examples/models examples/requests/sales_country.json`.
- Full DuckDB round-trip: `cargo run --example run_query`.
- Python demo (after `maturin develop`): `uv run python examples/python_demo.py`.
