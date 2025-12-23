# Python Bindings Reference

SemaFlow provides Python bindings via PyO3, exposing the Rust engine through a Pythonic API.

## Installation

```bash
pip install semaflow
# Or with API support:
pip install semaflow[api]
```

---

## Core Classes

### DataSource

Represents a database connection configuration. Multiple backends are supported.

```python
from semaflow import DataSource

# DuckDB connection (default backend)
ds = DataSource.duckdb("path/to/database.duckdb", name="my_db")

# With connection pool limit
ds = DataSource.duckdb("analytics.duckdb", name="analytics", max_concurrency=4)

# PostgreSQL connection
ds = DataSource.postgres(
    "postgres://user:pass@localhost:5432/mydb",
    name="postgres_db"
)

# BigQuery connection (uses Application Default Credentials)
ds = DataSource.bigquery(
    project_id="my-gcp-project",
    dataset="analytics",
    name="bq_source"
)
```

**Class Methods:**

| Method | Backend | Required Feature |
|--------|---------|------------------|
| `DataSource.duckdb(path, name, max_concurrency=None)` | DuckDB | `duckdb` (default) |
| `DataSource.postgres(connection_string, name)` | PostgreSQL | `postgres` |
| `DataSource.bigquery(project_id, dataset, name)` | BigQuery | `bigquery` |

**Attributes:**
- `name: str` - Connection identifier used in table definitions
- `uri: str` - Database path or connection string
- `max_concurrency: Optional[int]` - Connection pool size (DuckDB only)

**Methods:**
- `table(name: str) -> TableHandle` - Create a table reference for this data source

**Note:** Each backend requires its corresponding Cargo feature to be enabled at build time. The Python wheel built with `--features all-backends` includes all three.

---

### SemanticTable

Defines a database table with semantic metadata.

```python
from semaflow import SemanticTable, Dimension, Measure

orders = SemanticTable(
    name="orders",
    data_source=ds,  # or data_source="my_db"
    table="orders",
    primary_key="id",  # or primary_keys=["id", "date"] for composite
    time_dimension="created_at",
    dimensions={
        "customer_id": Dimension("customer_id"),
        "created_at": Dimension("created_at", data_type="timestamp"),
    },
    measures={
        "order_total": Measure("amount", "sum", description="Total order amount"),
        "order_count": Measure("id", "count"),
    },
    description="Order transactions"
)
```

**Parameters:**
- `name: str` - Unique table identifier
- `data_source: DataSource | str` - Connection to use
- `table: str` - Physical table name in database
- `primary_key: str` - Single primary key column
- `primary_keys: List[str]` - Composite primary key columns
- `time_dimension: Optional[str]` - Default time column for time-series queries
- `dimensions: Dict[str, Dimension]` - Categorical attributes
- `measures: Dict[str, Measure]` - Aggregatable metrics
- `description: Optional[str]` - Human-readable description

---

### Dimension

Defines a categorical attribute.

```python
from semaflow import Dimension

# Simple column reference
country = Dimension("country")

# With metadata
country = Dimension(
    expression="country",
    data_type="string",
    description="Customer country code"
)

# Computed expression (using dict for complex expressions)
year = Dimension(
    expression={"function": "year", "args": [{"column": "created_at"}]},
    data_type="integer",
    description="Year extracted from timestamp"
)
```

**Parameters:**
- `expression: str | dict` - Column name or expression definition
- `data_type: Optional[str]` - Data type hint
- `description: Optional[str]` - Human-readable description

---

### Measure

Defines an aggregatable metric.

```python
from semaflow import Measure

# Basic measure
total = Measure("amount", "sum")

# With all options
avg_order = Measure(
    expression="amount",
    agg="sum",
    data_type="float",
    description="Average order amount",
    filter={"binary_op": {"op": "gt", "left": {"column": "amount"}, "right": {"literal": 0}}},
    post_expr={"function": "safe_divide", "args": [
        {"measure_ref": "order_total"},
        {"measure_ref": "order_count"}
    ]}
)
```

**Parameters:**
- `expression: str | dict` - Column or expression to aggregate
- `agg: str` - Aggregation type: `"sum"`, `"count"`, `"count_distinct"`, `"min"`, `"max"`, `"avg"`
- `data_type: Optional[str]` - Result data type
- `description: Optional[str]` - Human-readable description
- `filter: Optional[dict]` - Measure-level filter (FILTER WHERE clause)
- `post_expr: Optional[dict]` - Derived measure expression referencing other measures

---

### SemanticFlow

Composes tables with joins into a queryable model.

```python
from semaflow import SemanticFlow, FlowJoin, JoinKey

sales_flow = SemanticFlow(
    name="sales",
    base_table=orders,
    base_table_alias="o",
    joins=[
        FlowJoin(
            semantic_table=customers,
            alias="c",
            to_table="o",
            join_keys=[JoinKey(left="customer_id", right="id")],
            join_type="left",
            description="Customer who placed the order"
        )
    ],
    description="Sales analytics flow"
)
```

**Parameters:**
- `name: str` - Flow identifier
- `base_table: SemanticTable` - Primary fact table
- `base_table_alias: str` - Query alias for base table
- `joins: List[FlowJoin]` - Join definitions
- `description: Optional[str]` - Human-readable description

---

### FlowJoin

Defines a join between tables.

```python
from semaflow import FlowJoin, JoinKey

join = FlowJoin(
    semantic_table=customers,
    alias="c",
    to_table="o",
    join_keys=[JoinKey(left="customer_id", right="id")],
    join_type="left",  # "left", "inner", "right", "full"
    description="Customer dimension"
)
```

**Parameters:**
- `semantic_table: SemanticTable` - Table to join
- `alias: str` - Query alias
- `to_table: str` - Alias of table to join to
- `join_keys: List[JoinKey]` - Join conditions
- `join_type: str` - Join type (default: `"left"`)
- `description: Optional[str]` - Human-readable description

---

### FlowHandle

High-level handle for executing queries.

```python
from semaflow import FlowHandle, DataSource

# From YAML definitions
handle = FlowHandle.from_dir(
    root="path/to/flows",
    data_sources=[DataSource.duckdb("analytics.duckdb")],
    description="Analytics handle"
)

# From Python objects
handle = FlowHandle.from_parts(
    tables=[orders, customers],
    flows=[sales_flow],
    data_sources=[ds],
    description="Sales analytics"
)
```

**Methods:**

#### `build_sql(request: dict) -> str`
Generate SQL without executing.

```python
sql = await handle.build_sql({
    "flow": "sales",
    "dimensions": ["c.country"],
    "measures": ["o.order_total"],
    "limit": 10
})
print(sql)
```

#### `execute(request: dict) -> List[dict]`
Execute query and return results.

```python
results = await handle.execute({
    "flow": "sales",
    "dimensions": ["c.country"],
    "measures": ["o.order_total"],
    "filters": [{"field": "c.country", "op": "==", "value": "US"}],
    "order": [{"column": "o.order_total", "direction": "desc"}],
    "limit": 10
})
# [{"c.country": "US", "o.order_total": 150.0}, ...]
```

#### `list_flows() -> List[dict]`
List available flows.

```python
flows = handle.list_flows()
# [{"name": "sales", "description": "Sales analytics flow"}]
```

#### `get_flow(name: str) -> dict`
Get flow schema.

```python
schema = handle.get_flow("sales")
# {
#     "name": "sales",
#     "dimensions": [{"name": "country", "qualified_name": "c.country", ...}],
#     "measures": [{"name": "order_total", "qualified_name": "o.order_total", ...}],
#     ...
# }
```

---

## Query Request Format

```python
{
    "flow": "sales",                          # Required: flow name
    "dimensions": ["c.country", "o.created_at"],  # Optional: grouping columns
    "measures": ["o.order_total", "o.order_count"],  # Optional: aggregations
    "filters": [                              # Optional: row filters
        {"field": "c.country", "op": "==", "value": "US"},
        {"field": "o.created_at", "op": ">=", "value": "2024-01-01"}
    ],
    "order": [                                # Optional: sorting
        {"column": "o.order_total", "direction": "desc"}
    ],
    "limit": 100,                             # Optional: cap on total rows
    "page_size": 25,                          # Optional: enable pagination
    "cursor": "..."                           # Optional: cursor for next page
}
```

### Filter Operators

| Operator | Description |
|----------|-------------|
| `==` | Equal |
| `!=` | Not equal |
| `>` | Greater than |
| `>=` | Greater than or equal |
| `<` | Less than |
| `<=` | Less than or equal |
| `in` | In list |
| `not in` | Not in list |
| `like` | SQL LIKE pattern |
| `ilike` | Case-insensitive LIKE |

---

## FastAPI Integration

```python
from semaflow import FlowHandle
from semaflow.api import create_app

handle = FlowHandle.from_dir("flows/", [DataSource.duckdb("data.duckdb")])
app = create_app(handle)

# Endpoints:
# GET  /flows              - List all flows
# GET  /flows/{name}       - Get flow schema
# POST /flows/{name}/query - Execute query
```

### Custom Router

```python
from semaflow.api import create_router

router = create_router(handle)
app.include_router(router, prefix="/api/v1")
```

---

## Pagination

SemaFlow supports cursor-based pagination for efficient page-by-page result retrieval.

### Enabling Pagination

Set `page_size` in your request to enable pagination:

```python
# First page
result = await handle.execute({
    "flow": "sales",
    "dimensions": ["c.country"],
    "measures": ["o.order_total"],
    "page_size": 25  # 25 rows per page
})

print(result["rows"])       # List of row dicts for this page
print(result["has_more"])   # True if more pages exist
print(result["cursor"])     # Cursor string for next page

# Fetch next page
if result["cursor"]:
    next_page = await handle.execute({
        "flow": "sales",
        "dimensions": ["c.country"],
        "measures": ["o.order_total"],
        "page_size": 25,
        "cursor": result["cursor"]
    })
```

### Response Structure

When `page_size` is set, `execute()` returns a dict instead of a list:

```python
{
    "rows": [...],           # Rows for this page
    "cursor": "base64...",   # Cursor for next page (None if last page)
    "has_more": True,        # Whether more rows exist
    "total_rows": 1000,      # Total result count (BigQuery only)
}
```

### Pagination vs Limit

| Parameter | Purpose | Behavior |
|-----------|---------|----------|
| `limit` | Cap total rows | Returns up to N rows in one response |
| `page_size` | Enable pagination | Returns N rows per page with cursor |
| Both | Combined | Up to `limit` rows total, `page_size` per page |

```python
# Cap at 500 rows, paginated 25 at a time
result = await handle.execute({
    "flow": "sales",
    "dimensions": ["c.country"],
    "measures": ["o.order_total"],
    "limit": 500,
    "page_size": 25
})
```

### Backend Behavior

| Backend | Pagination Method |
|---------|-------------------|
| **BigQuery** | Native job pagination (no query re-execution) |
| **PostgreSQL** | LIMIT/OFFSET |
| **DuckDB** | LIMIT/OFFSET |

BigQuery cursors reference cached job results, so subsequent pages are fast and don't re-run the query.

---

## Async Usage

All query operations are async-compatible:

```python
import asyncio
from semaflow import FlowHandle

async def main():
    handle = FlowHandle.from_dir("flows/", [...])

    # Concurrent queries
    results = await asyncio.gather(
        handle.execute({"flow": "sales", "measures": ["o.order_total"]}),
        handle.execute({"flow": "sales", "measures": ["o.order_count"]}),
    )

asyncio.run(main())
```

---

## Error Handling

```python
from semaflow import FlowHandle

try:
    result = await handle.execute({
        "flow": "nonexistent",
        "measures": ["invalid.measure"]
    })
except ValueError as e:
    print(f"Validation error: {e}")
except RuntimeError as e:
    print(f"Execution error: {e}")
```

Common errors:
- `ValueError`: Invalid flow name, unknown dimension/measure, invalid filter
- `RuntimeError`: Database connection failure, SQL execution error
