# Expressions, Measures, and Filters

## Expression building blocks
- **Columns**: `column: "orders.amount"` or `"amount"` when scoped to a table alias.
- **Literals**: strings (`'US'`), integers, floats, `null`.
- **Functions** (dialect-aware renderings): `date_trunc`, `date_part`, `lower/upper`, `coalesce/ifnull`, `now`, `concat/concat_ws`, `substring`, `length`, `greatest/least`, `trim/ltrim/rtrim`, `cast`, and `safe_divide`.
- **CASE**: `when/then/else` clauses for conditional logic.
- **Binary ops**: `+ - * / %`, comparisons (`==, !=, >, >=, <, <=`), and logical `and/or`.

## Measures
- **Base measure**: `{ expr: amount, agg: sum }` renders an aggregate over the scoped column/expression.
- **Filtered measure**: add `filter: "country == 'US'"` to wrap the aggregate in a CASE (non-matching rows contribute NULL).
- **Derived measure**: add `post_expr` to define the exposed value; dependencies are auto-included. Example:

```yaml
measures:
  order_total:
    expr: amount
    agg: sum
  order_count:
    expr: order_id
    agg: count_distinct
  avg_order_amount:
    expr: amount
    agg: sum            # base aggregate exists for reuse
    post_expr: "safe_divide(order_total, order_count)"
```

Rules:
- Derived measures cannot reference other derived measures; the planner errors on cycles or derived→derived references.
- If only `avg_order_amount` is requested, `order_total` and `order_count` are auto-added for computation but only `avg_order_amount` is selected. Requesting `order_total` explicitly will also expose it.
- Base measures are always materialised; they can be selected directly or used as dependencies in `post_expr`.

## Concise string syntax
- Supported: simple comparisons (`== != > >= < <=`), measure refs, column refs, literals, and `safe_divide(a, b)`.
- Examples:
  - Measure filter: `filter: "country == 'US'"`
  - Post expression: `post_expr: "safe_divide(order_total, order_count)"`
  - Boolean logic: `post_expr: "quantity > 0 and amount > 0"`
- For more complex logic, use the structured `Expr`/YAML form (CASE/func/binop).

## Query requests
- `filters` in a request target dimensions only (row-level). Use measure-level `filter` for metric-specific conditions.
- `order` may reference any selected dimension or measure name.
- `limit`/`offset` pass through to the rendered SQL.
