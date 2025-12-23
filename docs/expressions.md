# Expressions, Measures, and Filters

## Expression building blocks
- **Columns**: `column: "orders.amount"` or `"amount"` when scoped to a table alias.
- **Literals**: strings (`'US'`), integers, floats, `null`.
- **Functions** (dialect-aware renderings): `date_trunc`, `date_part`, `lower/upper`, `coalesce/ifnull`, `now`, `concat/concat_ws`, `substring`, `length`, `greatest/least`, `trim/ltrim/rtrim`, `cast`, and `safe_divide`.
- **CASE**: `when/then/else` clauses for conditional logic.
- **Binary ops**: `+ - * / %`, comparisons (`==, !=, >, >=, <, <=`), and logical `and/or`.

## Measures

SemaFlow supports two types of measures:

| Type | Use Case | Syntax |
|------|----------|--------|
| **Simple** | Single aggregation on a column, optional filter | `expr: amount`, `agg: sum` |
| **Complex** | Chained functions, arithmetic, inline aggregations | `formula: "round(sum(a) / count(b), 2)"` |

### Simple measures

A simple measure uses `expr` + `agg` for single-column aggregations:

```yaml
measures:
  order_total:
    expr: amount
    agg: sum
    description: Total order amount

  order_count:
    expr: order_id
    agg: count
    description: Count of orders

  # With a pre-aggregate filter
  us_order_total:
    expr: amount
    agg: sum
    filter: "country == 'US'"
    description: Total orders from US customers
```

Supported aggregations: `sum`, `count`, `count_distinct`, `min`, `max`, `avg`, `median`, `stddev`, `variance`.

### Complex measures (formula)

For calculations involving multiple aggregations, arithmetic, or functions, use the `formula` field:

```yaml
measures:
  # Inline aggregations with arithmetic
  avg_order_amount:
    formula: "sum(amount) / count(order_id)"
    description: Average order amount

  # With rounding
  avg_order_rounded:
    formula: "round(sum(amount) / count(order_id), 2)"
    description: Average order (2 decimal places)

  # Reference simple measures by name
  avg_order_v2:
    formula: "order_total / order_count"
    description: Average order using measure refs

  # Complex profit margin calculation
  profit_margin:
    formula: "safe_divide(sum(revenue) - sum(cost), sum(revenue))"
    description: Profit margin as percentage
```

**Formula syntax supports:**
- Inline aggregations: `sum(col)`, `count(col)`, `avg(col)`, `min(col)`, `max(col)`, `count_distinct(col)`
- Arithmetic: `+ - * /` with proper operator precedence
- Comparisons: `== != > >= < <=`
- Functions: `round`, `abs`, `floor`, `ceil`, `coalesce`, `ifnull`, `nullif`, `safe_divide`, `greatest`, `least`
- Parentheses for grouping: `(sum(a) + sum(b)) * 2`
- Measure references: use the name of a simple measure (e.g., `order_total`)

**Division safety:** All `/` operations are automatically wrapped in `NULLIF(divisor, 0)` to prevent divide-by-zero errors.

### Validation rules

1. **Mutually exclusive**: A measure must have either `expr + agg` OR `formula`, not both.
2. **Simple requires both**: If using simple syntax, both `expr` and `agg` are required.
3. **No formula chaining**: Formula measures can only reference simple measures, not other formula measures.
4. **No self-reference**: A formula cannot reference its own measure name.

Error messages are descriptive:
```
Measure 'avg_order' is invalid: cannot specify both 'expr'/'agg' and 'formula'.
Use 'expr' + 'agg' for simple measures, or 'formula' for complex expressions.

Measure 'profit' formula references 'derived_cost', which is also a formula measure.
Formula measures can only reference simple measures (those with 'expr' + 'agg').
```

### Derived measures (deprecated)

The `post_expr` field is deprecated in favor of `formula`. Migration is straightforward:

```yaml
# Old style (deprecated)
measures:
  order_total:
    expr: amount
    agg: sum
  order_count:
    expr: order_id
    agg: count
  avg_order:
    expr: amount
    agg: sum            # required but unused
    post_expr: "safe_divide(order_total, order_count)"

# New style (recommended)
measures:
  order_total:
    expr: amount
    agg: sum
  order_count:
    expr: order_id
    agg: count
  avg_order:
    formula: "order_total / order_count"  # or: "sum(amount) / count(order_id)"
```

## Concise string syntax
- Supported: simple comparisons (`== != > >= < <=`), measure refs, column refs, literals, and `safe_divide(a, b)`.
- Examples:
  - Measure filter: `filter: "country == 'US'"`
  - Formula: `formula: "round(sum(amount) / count(id), 2)"`
  - Boolean logic in filters: `filter: "quantity > 0 and amount > 0"`
- For more complex logic, use the structured `Expr`/YAML form (CASE/func/binop).

## Query requests
- `filters` in a request target dimensions only (row-level). Use measure-level `filter` for metric-specific conditions.
- `order` may reference any selected dimension or measure name.
- `limit`/`offset` pass through to the rendered SQL.
- `page_size`/`cursor` enable cursor-based pagination.
