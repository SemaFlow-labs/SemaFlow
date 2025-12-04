# Module Map (Rust core)

- **data_sources.rs**: Backend abstraction (`BackendConnection`), `ConnectionManager`, DuckDB impl with connection pool + semaphore backpressure.
- **dialect.rs / sql_ast.rs**: Dialect trait and SQL AST plus renderer. Dialects render functions/identifiers; AST stays dialect-neutral.
- **executor.rs**: Runtime execution helpers and result shaping; DuckDB value→JSON conversion.
- **flows.rs**: Semantic model types (tables, dimensions, measures, joins, expressions, functions, aggregations) shared by registry, planner, and Python bindings.
- **registry.rs**: In-memory registry of semantic tables/flows; lookup utilities for planner/introspection.
- **validation.rs**: Schema checks against backends (columns, PKs, join keys, single data source per flow), error surfacing.
- **schema_cache.rs**: Backend schema cache keyed by (data_source, table) to avoid repeated fetches.
- **query_builder/** (planner):
  - `resolve.rs`: Alias map and field resolution (`alias.field` support), filter/order field lookup.
  - `measures.rs`: Build base aggregates, apply measure filters, resolve `post_expr` with dependency rules, reject derived→derived refs.
  - `joins.rs`: Compute required joins and prune safe LEFT joins (PK-targeted) while keeping grain-safe joins.
  - `filters.rs`: Render row-level filters.
  - `render.rs`: Expr→SQL AST, plus post-expression rendering for derived measures.
  - `mod.rs`: Orchestrates planner stages and renders final SQL.
- **expr_parser.rs**: Tiny safe parser for concise filter/post_expr strings (comparisons, literals/idents, `safe_divide`).
- **python/**: PyO3 bindings and conversions to/from Rust types; GIL-releasing async wrappers and FastAPI helpers.
- **tests/unit/** + **tests/integration/**: Planner rendering, join pruning, filtered/derived measures, DuckDB round-trips.
