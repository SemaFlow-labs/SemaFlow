# SemaFlow Architecture (Vision)

## Core (Rust)
- Semantic flow ownership in Rust (tables, dimensions, measures, joins, time grain).
- AST-first SQL planner + renderer; per-dialect `Dialect` impls (DuckDB implemented; Postgres/BigQuery/Snowflake planned).
- Async backends via `BackendConnection` + `ConnectionManager`; connections embedded in semantic tables for zero-copy lookups.
- Query planner (modular `query_builder`):
  - **Resolve**: build alias map; resolve dimensions/measures by name or `alias.field`; request filters restricted to dimensions.
  - **Measures**: build base aggregates for every measure, apply measure-scoped filters via CASE, auto-include dependencies for `post_expr`. Derived measures cannot reference other derived measures; only requested measures are selected, but dependencies are computed under the hood.
  - **Joins**: required aliases drive join selection; join pruning keeps safe LEFT joins (PK-targeted) and retains INNER/ambiguous joins to preserve grain.
  - **Render**: dialect-aware SQL AST -> SQL string; functions rendered per dialect; `safe_divide` emits guarded divide.
  - Future: grain inference for multi-column PKs/explicit cardinality hints, subquery building for mismatched grains.
- Execution engine async and backpressured (DuckDB semaphore + connection reuse now; future: BigQuery job polling, Postgres/Snowflake pools).
- Schema cache keyed by (data_source, table); validation enforces PK/time dimension/join keys/single data source per flow.
- Python bindings via PyO3; GIL released around Rust work; shared tokio runtime.

## Python UX
- Thin handles only (Rust owns state). `SemanticFlow`/`FlowHandle` wrap registry + connections, exposing:
  - `build_sql`, `execute`
  - `list_flows`, `get_flow` (dimensions/measures with descriptions, qualified names, time metadata)
- FastAPI helper (`semaflow.api`) to expose HTTP routes without touching Rust.
- Flows loadable from YAML or Python classes; queries are JSON-friendly (dims/measures/filters/order/limit).

## Performance Targets
- Rust async for planning/execution; Python only routes requests.
- Backpressure per backend (DuckDB semaphore + connection reuse now; targets: BigQuery job caps, Postgres pool limits).
- GIL-free execution path; shared runtime avoids spawning per-call runtimes.

## Roadmap Highlights
- Add dialects/backends (Postgres/BigQuery/Snowflake/Trino).
- Planner features: join pruning, grain inference, subquerying for mismatched grains.
- Connection pooling/backpressure knobs per backend.
- Python async APIs and FastAPI example hardened; optional result caching.
- CI + packaging polish (extras for backends).
