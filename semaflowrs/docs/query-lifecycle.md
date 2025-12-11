# Query Lifecycle

This document traces the path of a query request from Python API to SQL result.

## Overview

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Request    │────▶│   Resolve    │────▶│   Analyze    │────▶│    Plan      │
│   Parsing    │     │  Components  │     │   Strategy   │     │   Building   │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
                                                                      │
┌──────────────┐     ┌──────────────┐     ┌──────────────┐           │
│   Results    │◀────│   Execute    │◀────│   Render     │◀──────────┘
│   (JSON)     │     │   (DuckDB)   │     │   SQL        │
└──────────────┘     └──────────────┘     └──────────────┘
```

---

## Phase 1: Request Parsing

**Entry Point**: `python/mod.rs::SemanticFlowHandle::execute()`

```python
# Python
result = await handle.execute({
    "flow": "sales",
    "dimensions": ["c.country"],
    "measures": ["o.order_total"],
    "filters": [{"field": "c.country", "op": "==", "value": "US"}],
    "limit": 10
})
```

**Rust Processing**:
```rust
// python/mod.rs
fn execute(&self, py: Python<'_>, request: &PyAny) -> PyResult<PyObject> {
    let request = parse_request(py, request)?;  // Dict → QueryRequest
    // ...
}

// QueryRequest structure
pub struct QueryRequest {
    pub flow: String,           // "sales"
    pub dimensions: Vec<String>, // ["c.country"]
    pub measures: Vec<String>,   // ["o.order_total"]
    pub filters: Vec<Filter>,    // [{field, op, value}]
    pub order: Vec<OrderItem>,
    pub limit: Option<i32>,      // 10
    pub offset: Option<i32>,
}
```

---

## Phase 2: Component Resolution

**Entry Point**: `query_builder/components.rs::resolve_components()`

The resolver converts string field names to typed SQL expressions by looking up the flow and its tables.

```
"c.country" ──▶ Look up flow "sales"
             ──▶ Find join alias "c" → customers table
             ──▶ Find dimension "country" in customers
             ──▶ ResolvedDimension {
                    name: "c.country",
                    alias: "c",
                    expr: SqlExpr::Column { table: "c", name: "country" }
                 }
```

### Resolution Steps

1. **Build alias map**: Map table aliases to SemanticTable references
2. **Resolve dimensions**: Convert dimension names to SqlExpr
3. **Resolve measures**: Convert measure names, handle `post_expr` dependencies
4. **Resolve filters**: Convert filter fields, validate no measure filters
5. **Resolve order**: Convert order columns to SqlExpr

### Output: QueryComponents

```rust
QueryComponents {
    base_alias: "o",
    base_table: TableRef { name: "orders", alias: Some("o") },
    dimensions: [
        ResolvedDimension { name: "c.country", alias: "c", expr: Column("c", "country") }
    ],
    measures: [
        ResolvedMeasure { name: "o.order_total", alias: "o", measure: Measure{...}, base_expr: Some(Aggregate{Sum, Column("o", "amount")}) }
    ],
    filters: [
        ResolvedFilter { filter: Filter{field: "c.country", ...}, expr: Column("c", "country"), alias: Some("c") }
    ],
    // ...
}
```

---

## Phase 3: Strategy Analysis

**Entry Point**: `query_builder/analysis.rs::analyze_multi_grain()`

Determines whether to use a flat query or pre-aggregated CTEs.

### Decision Tree

```
┌─────────────────────────────────────┐
│     Measures from multiple tables?  │
└──────────────────┬──────────────────┘
                   │
         ┌─────────┴─────────┐
         │                   │
        Yes                 No
         │                   │
         ▼                   ▼
  MultiGrainPlan    ┌─────────────────────────┐
  (N CTEs)          │ Fanout risk from        │
                    │ join filters?           │
                    └──────────────┬──────────┘
                                   │
                    ┌──────────────┴──────────┐
                    │                         │
                   Yes                       No
                    │                         │
                    ▼                         ▼
            MultiGrainPlan              FlatPlan
            (1 CTE + joins)        (standard SELECT)
```

### Fanout Detection

Fanout occurs when filtering on a joined table could multiply base rows:

```sql
-- Without pre-aggregation (fanout risk):
SELECT SUM(o.amount)
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
WHERE c.country = 'US'
-- If customers had multiple rows per id, SUM would be inflated
```

### Output: MultiGrainAnalysis

```rust
MultiGrainAnalysis {
    needs_multi_grain: true,
    table_grains: {
        "o": TableGrain { grain_columns: ["customer_id"] },
        "c": TableGrain { grain_columns: ["id"] },
    },
    cte_join_specs: [
        CteJoinSpec { from_alias: "c", to_alias: "o", join_type: Left, join_keys: [...] }
    ]
}
```

---

## Phase 4: Plan Building

**Entry Point**: `query_builder/planner.rs::build_query()`

### Flat Plan

For simple queries without fanout risk:

```rust
FlatPlan {
    from: TableRef { name: "orders", alias: "o" },
    select: [
        SelectItem { expr: Column("c", "country"), alias: "c.country" },
        SelectItem { expr: Aggregate(Sum, Column("o", "amount")), alias: "o.order_total" }
    ],
    joins: [
        Join { type: Left, table: "customers" as "c", on: [o.customer_id = c.id] }
    ],
    filters: [BinaryOp(Column("c", "country"), Eq, Literal("US"))],
    group_by: [Column("c", "country")],
    order_by: [...],
    limit: Some(10),
}
```

### Multi-Grain Plan

For multi-table measures or fanout protection:

```rust
MultiGrainPlan {
    ctes: [
        GrainedAggPlan {
            alias: "o_agg",
            from: "orders" as "o",
            select: [customer_id, SUM(amount) as order_total],
            group_by: [customer_id],
            filters: [],
        },
        GrainedAggPlan {
            alias: "c_agg",
            from: "customers" as "c",
            select: [id, country, COUNT(id) as customer_count],
            group_by: [id, country],
            filters: [country = 'US'],
        }
    ],
    final_query: FinalQueryPlan {
        base_cte_alias: "o_agg",
        select: [c_agg.country, o_agg.order_total, c_agg.customer_count],
        cte_joins: [CteJoin { c_agg LEFT JOIN o_agg ON customer_id = id }],
        // ...
    }
}
```

---

## Phase 5: SQL Rendering

**Entry Point**: `sql_ast.rs::SqlRenderer::render_select()`

The plan is converted to a `SelectQuery` AST, then rendered to SQL string.

### Flat Query Output

```sql
SELECT "c"."country" AS "c__country",
       SUM("o"."amount") AS "o__order_total"
FROM "orders" "o"
LEFT JOIN "customers" "c" ON ("o"."customer_id" = "c"."id")
WHERE ("c"."country" = 'US')
GROUP BY "c"."country"
ORDER BY "o__order_total" DESC
LIMIT 10
```

### Multi-Grain Query Output

```sql
SELECT "c_agg"."country" AS "c__country",
       "o_agg"."order_total" AS "o__order_total",
       "c_agg"."customer_count" AS "c__customer_count"
FROM (
    SELECT "o"."customer_id" AS "customer_id",
           SUM("o"."amount") AS "order_total"
    FROM "orders" "o"
    GROUP BY "o"."customer_id"
) "o_agg"
LEFT JOIN (
    SELECT "c"."id" AS "id",
           "c"."country" AS "country",
           COUNT("c"."id") AS "customer_count"
    FROM "customers" "c"
    WHERE ("c"."country" = 'US')
    GROUP BY "c"."id", "c"."country"
) "c_agg" ON ("o_agg"."customer_id" = "c_agg"."id")
ORDER BY "o__order_total" DESC
LIMIT 10
```

### Alias Sanitization

Column aliases containing dots are sanitized for SQL compatibility:
- `c.country` → `c__country` (in SQL)
- Results are transformed back: `c__country` → `c.country` (in API response)

---

## Phase 6: Execution

**Entry Point**: `executor.rs::execute_query()`

```rust
pub async fn execute_query(
    connections: &ConnectionManager,
    data_source: &str,
    sql: &str,
) -> Result<QueryResult> {
    let conn = connections.get(data_source)?;
    let rows = conn.execute(sql).await?;
    Ok(QueryResult { rows, sql: sql.to_string() })
}
```

### DuckDB Execution

1. Get connection from pool (lazy initialization)
2. Execute SQL query
3. Convert Arrow batches to JSON rows
4. Return `Vec<serde_json::Value>`

---

## Phase 7: Result Return

**Path back to Python**:

```rust
// python/mod.rs
let rows_json = serde_json::to_string(&result.rows)?;
let py_obj = json.loads(rows_json)?;  // Convert to Python dict
Ok(py_obj)
```

**Python wrapper transforms keys**:

```python
# handle.py
async def execute(self, request):
    rows = await asyncio.to_thread(self._inner.execute, request)
    return [_unsanitize_keys(row) for row in rows]

def _unsanitize_keys(row):
    return {k.replace("__", "."): v for k, v in row.items()}
```

**Final result**:

```python
[
    {"c.country": "US", "o.order_total": 150.0, "c.customer_count": 1}
]
```
