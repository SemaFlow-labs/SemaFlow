# Module Reference

## Python Package (`semaflow/`)

| Module | Purpose |
|--------|---------|
| `core.py` | Re-exports from Rust bindings (DataSource, SemanticTable, etc.) |
| `handle.py` | `FlowHandle` wrapper with async methods |
| `api.py` | FastAPI integration (`create_app`, `create_router`) |
| `semaflow.pyi` | Type stubs for IDE support |

## Rust Engine (`semaflowrs/src/`)

### Core Definitions

| Module | Purpose |
|--------|---------|
| `flows.rs` | Semantic model types: tables, dimensions, measures, joins, expressions |
| `registry.rs` | In-memory registry of semantic tables/flows; lookup utilities |
| `config.rs` | TOML configuration parsing and defaults |
| `error.rs` | Error types and result aliases |

### SQL Generation

| Module | Purpose |
|--------|---------|
| `sql_ast.rs` | Typed SQL AST and renderer |
| `dialect/mod.rs` | Dialect trait for backend-specific rendering |
| `dialect/duckdb.rs` | DuckDB SQL rendering |
| `dialect/postgres.rs` | PostgreSQL SQL rendering |
| `dialect/bigquery.rs` | BigQuery SQL rendering |

### Query Builder (`query_builder/`)

| Module | Purpose |
|--------|---------|
| `mod.rs` | `SqlBuilder` entry point, orchestrates planner stages |
| `planner.rs` | Query strategy selection (Flat vs MultiGrain) |
| `analysis.rs` | Fanout detection, grain analysis, multi-table measure handling |
| `components.rs` | Request resolution: string fields → typed SQL expressions |
| `plan.rs` | Query plan types (`FlatPlan`, `MultiGrainPlan`) |
| `builders.rs` | SQL element builders |
| `filters.rs` | Row-level filter rendering |
| `joins.rs` | Join selection, pruning of unused LEFT joins |
| `measures.rs` | Measure expression handling, `post_expr` dependencies |
| `render.rs` | Expression → SQL AST conversion |
| `resolve.rs` | Alias map, field resolution (`alias.field` support) |
| `grain.rs` | Cardinality inference for join safety |

### Backends (`backends/`)

| Module | Purpose |
|--------|---------|
| `mod.rs` | `BackendConnection` trait, `ConnectionManager` |
| `duckdb.rs` | DuckDB connection with pooling + semaphore backpressure |
| `postgres.rs` | PostgreSQL async connection via `deadpool-postgres` |
| `bigquery.rs` | BigQuery HTTP client with concurrency limiting |

### Execution

| Module | Purpose |
|--------|---------|
| `executor.rs` | Query execution, result shaping (Arrow → JSON) |
| `runtime.rs` | Async runtime orchestration, GIL-releasing execution |
| `pagination.rs` | Cursor encoding/decoding, query hash validation |
| `schema_cache.rs` | Backend schema cache (table columns, types) |
| `validation.rs` | Schema validation (columns, PKs, join keys, single data source) |

### Python Bindings (`python/`)

| Module | Purpose |
|--------|---------|
| `mod.rs` | PyO3 bindings: `SemanticFlowHandle`, `DataSource`, type conversions |

### Utilities

| Module | Purpose |
|--------|---------|
| `expr_parser.rs` | Parser for concise filter/expression strings |
| `expr_utils.rs` | Expression manipulation utilities |

## Feature Flags

| Flag | Enables |
|------|---------|
| `duckdb` (default) | DuckDB backend |
| `postgres` | PostgreSQL backend |
| `bigquery` | BigQuery backend |
| `python` | PyO3 bindings |
| `all-backends` | All database backends |

## Test Organization

| Location | Content |
|----------|---------|
| `semaflowrs/src/**/tests.rs` | Rust unit tests (inline `#[cfg(test)]`) |
| `semaflowrs/tests/` | Rust integration tests |
| `tests/` | Python integration tests |
