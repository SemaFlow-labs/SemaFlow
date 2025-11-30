# SemaFlow Progress Summary

## Core capabilities
- **Semantic flow registry**: load from YAML or in-memory structs; tables include `data_source`, dimensions, measures (Expr DSL), time dimension, join keys.
- **Expr DSL**: columns (string shorthand), literals, CASE, binary ops, functions (date_trunc/part, lower/upper, coalesce/ifnull, now, concat/concat_ws, substring, length, greatest/least, trim/ltrim/rtrim, cast), aggregations (sum, count, count_distinct, min, max, avg).
- **SQL generation**: `SqlBuilder::build_for_request` builds a SQL AST and renders it with dialect support (DuckDB implemented via `DuckDbDialect`). Alias-qualified fields (`alias.field`) are accepted.
- **Validation**: checks columns/PK/time dimension, join alias uniqueness, join key columns, and enforces single data source per flow. Schema cache keyed by (data_source, table).
- **Connections/runtime**: unified `BackendConnection` trait + `ConnectionManager` (DuckDB implementation) provide dialect lookup, schema fetch, and SQL execution for runtime and validation paths.
- **Backpressure**: DuckDB backend includes a configurable max in-flight limiter.
- **Examples**: semantic definitions under `examples/flows`, Python demo (`examples/python_demo.py`), and FastAPI sample (`examples/semantic_api.py`).

## Python bindings (feature `python`)
- PyO3 module `semaflow` (gated by `--features python`) exposes class `SemanticFlow` plus low-level functions.
- Python wrapper package `semaflow` provides `DataSource`, `TableHandle`, `SemanticTable`, `FlowJoin`, `JoinKey`, and `SemanticFlow`; registry loading happens in Rust via `FlowHandle.from_dir` or `FlowHandle.from_parts`.
- Accepts Python objects; `data_sources` can be `list[DataSource]` or a name→path dict. Examples under `examples/python_demo.py` (YAML-backed) and `examples/python_objects_demo.py` (pure Python objects).
- FastAPI helper expects a dict mapping flow name to `SemanticFlow` (or a `FlowHandle`); each flow should wrap the flow of the same name.

## Tests
- Rust: unit coverage for SQL rendering/validation (`tests/query_builder_unit.rs`) and DuckDB round-trips/runtime (`tests/duckdb_poc.rs`); `cargo test` passes.
- Python: manual demo script exercises load/build/run through PyO3 bindings.

## Open items / next steps
- Broaden dialect support (Postgres/BigQuery) and extend Expr helpers as needed.
- Planner improvements: join pruning, grain inference, subquerying.
- Per-backend pooling/backpressure/timeout knobs; optional caching.
- Python package hardening (async APIs, wheel publishing, automated tests).
- CI with coverage reporting.
- Validation should walk nested expressions (CASE/func/binary) to surface missing columns before runtime.
