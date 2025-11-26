# Examples

Sample semantic definitions and a request payload you can feed into the core builder:

- `models/tables/*.yaml` define semantic tables with the Expr DSL (strings map to column expressions) and a `data_source` id.
- `models/models/*.yaml` defines the `sales` semantic model and join keys.
- `requests/sales_country.json` is a query request selecting `country` and `order_total` with a filter and order.

You can print SQL for a request using the example binary:

```
cargo run --example print_sql -- examples/models examples/requests/sales_country.json
```

That will emit the SQL compiled for the request using the DuckDB dialect.

### DSL cheatsheet (supported today)

- Expressions: column (string shorthand), literal, case, binary ops (+, -, *, /, %), functions: date_trunc, date_part, lower/upper, coalesce/ifnull, now, concat/concat_ws, substring, length, greatest/least, trim/ltrim/rtrim, cast.
- Aggregations: sum, count, count_distinct, min, max, avg.
- Joins: explicit `join_keys` (left/right columns), single data source per model enforced.
- Filters: row-level only on dimensions (measure filters rejected).
