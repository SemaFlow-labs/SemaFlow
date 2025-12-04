# Core Concepts

## Data sources and connections
- A `DataSource` maps to a backend connection (DuckDB today). Each semantic table points at a data source; flows cannot mix data sources.
- `ConnectionManager` stores named backends. DuckDB uses a pooled set of connections with a semaphore (defaults to 16) so queries reuse connections and apply backpressure rather than spawning unbounded work.
- Backends expose a dialect for SQL rendering and handle schema discovery plus execution.

## Semantic tables
- Tables describe columns, a primary key, optional time dimension, dimensions (expressions projected as columns), and measures (aggregations over an expression).
- Tables belong to a single data source; joins are defined by FK-like key pairs between table aliases.

## Semantic flows
- A flow names a base semantic table and aliases it; joins attach additional semantic tables by alias.
- Joins can be pruned when proven safe: LEFT joins that point at the joined table’s PK are dropped if no requested field or dependency needs them. INNER/ambiguous joins stay to avoid changing grain.
- Flows are the unit of querying; requests reference dimensions/measures by name or `alias.field`.

## Dimensions
- Dimensions are projected expressions (usually columns). Selecting a dimension automatically groups by its expression.
- Dimensions can be qualified by alias when the same column exists on multiple joined tables.

## Measures
- Base measures pair `expr` with an aggregation. They always exist even if a derived output is requested.
- Derived measures use `post_expr` to combine other measures (e.g., `safe_divide(order_total, order_count)`). The planner auto-includes dependencies for computation but only emits requested outputs.
- Derived measures cannot be referenced by other derived measures to avoid accidental grain shifts. If you need reuse, keep the shared part as a base measure and layer derived outputs on top.
- Measure-level filters wrap the aggregated expression in a CASE; filter expressions are parsed from concise strings (e.g., `country == 'US'`).

## Queries
- A request includes `dimensions`, `measures`, optional row-level `filters`, `order`, `limit`, and `offset`.
- Filters operate on dimensions (not measures); measure filters live inside the measure definition.
- The planner walks requested fields to gather required aliases, prunes joins when safe, builds base aggregates, then renders any `post_expr` selections.

## Execution path
- `SqlBuilder` produces a dialect-aware SQL AST then renders SQL.
- `FlowHandle.execute` (Python) or the Rust runtime uses the connection manager to acquire a slot, run SQL, and return JSON rows while holding no GIL.
