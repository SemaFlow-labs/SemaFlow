# SemaFlow Progress Summary

## Core capabilities
- **Semantic model registry**: load from YAML or in-memory structs; tables include `data_source`, dimensions, measures (Expr DSL), time dimension, join keys.
- **Expr DSL**: columns (string shorthand), literals, CASE, binary ops, functions (date_trunc/part, lower/upper, coalesce/ifnull, now, concat/concat_ws, substring, length, greatest/least, trim/ltrim/rtrim, cast), aggregations (sum, count, count_distinct, min, max, avg).
- **SQL generation**: `SqlBuilder::build_for_request` renders a `QueryRequest` to SQL with dialect support (DuckDB implemented via `DuckDbDialect`).
- **Validation**: checks columns/PK/time dimension, join alias uniqueness, join key columns, and enforces single data source per model. Schema cache keyed by (data_source, table).
- **Runtime**: `run_query` builds SQL and executes via the registered executor for the model’s data source; DuckDB executor provided.
- **Examples**: semantic definitions under `examples/models`, requests under `examples/requests`, SQL printer (`examples/run_print_sql.sh`), full DuckDB demo (`cargo run --example run_query`), and Python demo (`examples/python_demo.py`).

## Python bindings (feature `python`)
- PyO3 module `semaflow_core` (gated by `--features python`) exposes:
  - `build_sql(tables, models, data_sources, request) -> str`
  - `run(...) -> list[dict]` (validates, builds SQL, executes on DuckDB)
- Python wrapper package `semaflow` re-exports `build_sql`, `run_query`, and `load_models_from_dir` with type hints and docstrings.
- Accepts Python dict/list inputs; `data_sources` is `{name: duckdb_path}`. Examples under `examples/python_demo.py`.

## Tests
- Rust: unit coverage for SQL rendering/validation (`tests/query_builder_unit.rs`) and DuckDB round-trips/runtime (`tests/duckdb_poc.rs`); `cargo test` passes.
- Python: manual demo script exercises load/build/run through PyO3 bindings.

## Open items / next steps
- Broaden dialect support (Postgres/BigQuery) and extend Expr helpers as needed.
- Harden Python package (add automated tests, optional wheel build/maturin config tweaks).
- FastAPI HTTP/CLI front end calling `run_query` - allows out of the box API object with pre-built end points.
- CI with coverage reporting.
