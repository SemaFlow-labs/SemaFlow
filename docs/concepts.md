# Core Concepts

## Data Sources and Connections

A `DataSource` represents a connection to a database backend. SemaFlow supports three backends:

| Backend | Use Case | Connection Model |
|---------|----------|------------------|
| **DuckDB** | Local analytics, embedded use | Pooled connections with semaphore backpressure |
| **PostgreSQL** | Production transactional/analytical databases | Async connection pool (`deadpool-postgres`) |
| **BigQuery** | Cloud data warehouse | HTTP client with concurrency limiting |

Each semantic table references a single data source. **Flows cannot mix data sources** - all tables in a flow must use the same connection.

```python
from semaflow import DataSource

# DuckDB
ds = DataSource.duckdb("analytics.duckdb", name="local")

# PostgreSQL
ds = DataSource.postgres("postgresql://user:pass@host/db", schema="public", name="pg")

# BigQuery
ds = DataSource.bigquery(project_id="my-project", dataset="analytics", name="bq")
```

The `ConnectionManager` stores named backends and provides dialect-aware execution. See [Dialects](dialects.md) for backend-specific SQL rendering.

## Semantic Tables

Tables describe the semantic layer over physical database tables:

| Component | Purpose |
|-----------|---------|
| `primary_key` / `primary_keys` | Grain definition for join cardinality |
| `time_dimension` | Default time column for time-series queries |
| `dimensions` | Categorical attributes for grouping/filtering |
| `measures` | Aggregatable metrics |

Tables belong to a single data source. Joins between tables are defined by foreign key-like pairs.

```yaml
# tables/orders.yaml
name: orders
data_source: local
table: orders
primary_key: id
time_dimension: created_at

dimensions:
  status:
    expression: status
    description: Order fulfillment status
  created_at:
    expression: created_at
    data_type: timestamp

measures:
  order_total:
    expression: amount
    agg: sum
  order_count:
    expression: id
    agg: count
```

## Semantic Flows

A flow names a base semantic table and composes additional tables via joins:

- The **base table** is the primary fact table (aliased, e.g., `o` for orders)
- **Joins** attach dimension tables by alias
- Queries reference fields as `alias.field` (e.g., `c.country`, `o.order_total`)

```yaml
# flows/sales.yaml
name: sales
base_table:
  semantic_table: orders
  alias: o
joins:
  customers:
    semantic_table: customers
    alias: c
    to_table: o
    join_type: left
    join_keys:
      - left: customer_id
        right: id
```

### Join Pruning

SemaFlow automatically prunes unnecessary joins:
- **LEFT joins** pointing at a joined table's primary key are dropped if no requested field needs them
- **INNER joins** and ambiguous joins stay to preserve correct grain

See [Join Semantics](../semaflowrs/docs/join-semantics.md) for detailed join behavior.

## Dimensions

Dimensions are projected expressions for grouping and filtering:

```yaml
dimensions:
  country:
    expression: country
  order_year:
    expression:
      function: date_part
      args: [year, {column: created_at}]
```

- Selecting a dimension automatically adds it to `GROUP BY`
- Dimensions can be qualified by alias when ambiguous: `c.country` vs `o.country`

## Measures

### Base Measures

Pair an expression with an aggregation:

```yaml
measures:
  order_total:
    expression: amount
    agg: sum
```

### Filtered Measures

Apply a filter before aggregation:

```yaml
measures:
  us_revenue:
    expression: amount
    agg: sum
    filter: "country == 'US'"
```

Renders as `SUM(amount) FILTER (WHERE country = 'US')` on Postgres/DuckDB, or `SUM(CASE WHEN country = 'US' THEN amount END)` on BigQuery.

### Derived Measures

Use `post_expr` to combine other measures:

```yaml
measures:
  order_total:
    expression: amount
    agg: sum
  order_count:
    expression: id
    agg: count_distinct
  avg_order_amount:
    expression: amount
    agg: sum
    post_expr: "safe_divide(order_total, order_count)"
```

Rules:
- Derived measures **cannot reference other derived measures** (prevents grain shifts)
- Dependencies are auto-included for computation but only requested outputs are selected
- Base measures are always materialized

## Queries

A request specifies what to retrieve from a flow:

```python
result = await handle.execute({
    "flow": "sales",
    "dimensions": ["c.country"],
    "measures": ["o.order_total", "o.order_count"],
    "filters": [{"field": "c.country", "op": "in", "value": ["US", "CA"]}],
    "order": [{"column": "o.order_total", "direction": "desc"}],
    "limit": 100,
    "page_size": 25,  # Enable pagination (25 rows per page)
})
```

| Field | Purpose |
|-------|---------|
| `dimensions` | Columns to group by |
| `measures` | Aggregations to compute |
| `filters` | Row-level conditions (dimension filters only) |
| `order` | Sort order |
| `limit` | Maximum total rows |
| `page_size` | Enable cursor-based pagination |
| `cursor` | Fetch next page (from previous response) |

**Note**: Filters operate on dimensions. Measure-level filters are defined in the measure definition itself.

## Execution Path

1. **SqlBuilder** resolves fields, analyzes grain, and produces a dialect-aware SQL AST
2. The AST is rendered to SQL using the appropriate dialect (DuckDB/Postgres/BigQuery)
3. **FlowHandle.execute** acquires a connection, runs SQL, and returns JSON rows
4. For paginated queries, a cursor is returned for fetching subsequent pages

```
Request → Resolve → Analyze → Plan → Render SQL → Execute → Results
                     ↓
              (FlatPlan or MultiGrainPlan)
```

The planner chooses between:
- **FlatPlan**: Simple `SELECT ... JOIN ... GROUP BY` for straightforward queries
- **MultiGrainPlan**: Pre-aggregated CTEs when measures span multiple tables or fanout risk exists
