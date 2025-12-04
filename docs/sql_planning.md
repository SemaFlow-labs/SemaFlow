# Query Planning: Current vs Planned Shapes

## Current Behavior (Baseline)
- Single `SELECT ... GROUP BY` over the base alias plus only the joins required by selected dimensions/measures/order/filter.
- Measure filters rendered as `CASE WHEN ... THEN expr END` inside aggregates (portable across dialects).
- No explicit grain enforcement; 1:N joins can double-count; all filters live in `WHERE`; no semijoin translation for dimension filters.

## Planned Behavior (Grain-Aware)
- Planner chooses a shape per query:
  - `Flat`: same as today when joins look 1:1/1:N-to-dim and there are no risky dimension filters.
  - `PreAgg`: build a fact-only derived table/CTE that:
    - Selects only needed keys/dim expressions and measure inputs.
    - Applies fact filters and dimension filters translated to semijoins on the fact keys.
    - Groups to the requested grain and renders aggregates.
    - Outer `SELECT` joins only required dimensions for projections/order, computes derived measures, applies post-agg filters/order/limit.
- Measure filters prefer `AGG(expr) FILTER (WHERE ...)` when the dialect supports it; fall back to the current CASE-wrapping otherwise.
- Join pruning and projection pruning are preserved in both shapes.

## New Components
- Dialect capability flag: `supports_filtered_aggregates() -> bool` (DuckDB = true; others added per dialect).
- Planner (`semaflowrs/src/query_builder/planner.rs`):
  - Pulls schemas/PK/FK (cached) to build grain and cardinality hints.
  - Infers base grain and join cardinalities; classifies filters (fact vs dim vs post-agg).
  - Selects `PlanShape::Flat` vs `PlanShape::PreAgg` and produces logical plan pieces.
- Renderer extension:
  - For `PreAgg`, emits fact derived table/CTE + semijoin-style dimension filters + outer joins/derived measures.
  - Reuses existing renderer unchanged for `Flat`.
- Validation (optional):
  - Uses fetched schema to warn/fail on ambiguous grain; strict mode configurable.

## Warehouses Without PK/FK Constraints
- Prefer real metadata when present; otherwise layer hints and heuristics:
  - Configured hints: per-table `grain` (natural key columns) and per-join `cardinality` (many-to-one vs one-to-many).
  - Flow-derived grain: if no hints, treat requested dimensions’ key columns and join keys as the working grain; warn when a join could expand that grain.
  - Cardinality heuristics: if dimension-side key looks unique (stats or sampling), assume many-to-one; otherwise treat as possibly 1:N and choose `PreAgg`.
  - Sampling fallback (optional): cheap `COUNT(*)` vs `COUNT(DISTINCT key)` on small samples when the engine permits; cache results.
  - Modes: `auto` (heuristic, default), `strict` (fail on ambiguity without hints), `legacy` (always Flat).

## Efficiency Commitments
- Flat path is unchanged when selected.
- PreAgg is engaged only when risk/benefit is detected (dimension filters or potential 1:N joins).
- Semijoin translation of dimension filters avoids pulling wide dimensions into pre-agg.
- Joins remain pruned; projections remain minimal.
- Dialect-specific filtered aggregates used only when supported; otherwise portable CASE-wrapping keeps compatibility.

## Suggested Next Steps
- Implement planner + dialect capability flag skeletons with no behavior change (default to Flat).
- Add PreAgg rendering path and heuristics; gate by the planner decision.
- Add small EXPLAIN/EXPLAIN ANALYZE examples in `examples/` to compare Flat vs PreAgg on the demo DuckDB.
