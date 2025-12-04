# Dialect Notes

## Cross-dialect expectations
- Expressions use identifier quoting from the active dialect; functions listed below are rendered per dialect.
- Aggregations supported everywhere: `sum`, `count`, `count_distinct`, `min`, `max`, `avg`.
- Logical/rendering rules: CASE, binary arithmetic/comparisons, and boolean `and/or` are kept dialect-neutral.
- `safe_divide(a, b)` renders as a guarded divide (`NULL` when `b` is zero/NULL) instead of naive `a / b`.

## DuckDB (implemented)
- Identifier quoting: double quotes.
- Functions: `date_trunc`, `date_part`, `coalesce/ifnull`, `lower/upper`, `concat/concat_ws`, `substring`, `length`, `greatest/least`, `trim/ltrim/rtrim`, `cast`.
- `safe_divide` renders to `CASE WHEN b = 0 OR b IS NULL THEN NULL ELSE a / b END`.
- Connection model: pooled DuckDB connections with a semaphore cap (default 16) to enforce backpressure.

## Coming soon
- Postgres, BigQuery, Snowflake dialects will reuse the same planner surface; only function rendering/quoting changes.
- Dialect-specific docs should add function coverage/edge cases (time zones, date parts, string functions) as each backend lands.
