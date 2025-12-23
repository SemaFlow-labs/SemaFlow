# Rust Components Reference

## Module Overview

```
semaflowrs/src/
├── lib.rs                 # Public API exports
├── flows.rs               # Semantic definitions (tables, flows, measures)
├── registry.rs            # Flow storage and lookup
├── sql_ast.rs             # Typed SQL AST structures
├── executor.rs            # Query execution result types
├── validation.rs          # Schema validation
├── runtime.rs             # Query orchestration
├── error.rs               # Error types
├── expr_parser.rs         # Expression parsing
├── expr_utils.rs          # Expression utilities
├── schema_cache.rs        # Database schema caching
├── dialect/               # Database-specific SQL rendering (feature-gated)
│   ├── mod.rs             # Dialect trait + feature-gated re-exports
│   ├── duckdb.rs          # DuckDB SQL rendering (#[cfg(feature = "duckdb")])
│   ├── postgres.rs        # PostgreSQL SQL rendering (#[cfg(feature = "postgres")])
│   └── bigquery.rs        # BigQuery SQL rendering (#[cfg(feature = "bigquery")])
├── backends/              # Connection management (feature-gated)
│   ├── mod.rs             # BackendConnection trait + ConnectionManager
│   ├── duckdb.rs          # DuckDB connection + pooling (#[cfg(feature = "duckdb")])
│   ├── postgres.rs        # PostgreSQL connection (deadpool) (#[cfg(feature = "postgres")])
│   └── bigquery.rs        # BigQuery client (#[cfg(feature = "bigquery")])
├── python/
│   └── mod.rs             # PyO3 bindings
└── query_builder/
    ├── mod.rs             # SqlBuilder entry point
    ├── planner.rs         # Query strategy selection
    ├── analysis.rs        # Fanout and grain analysis
    ├── components.rs      # Request resolution
    ├── plan.rs            # Query plan types
    ├── builders.rs        # SQL element builders
    ├── filters.rs         # Filter expression rendering
    ├── joins.rs           # Join selection and pruning
    ├── measures.rs        # Measure expression handling
    ├── render.rs          # Expression to SQL conversion
    ├── resolve.rs         # Field resolution
    └── grain.rs           # Cardinality inference
```

### Feature Flags

Each backend is compiled only when its feature is enabled:

```toml
[features]
default = ["duckdb"]        # DuckDB on by default for backwards compat
duckdb = ["dep:duckdb"]     # Now optional!
postgres = ["dep:tokio-postgres", "dep:deadpool-postgres"]
bigquery = ["dep:gcp-bigquery-client"]
python = ["dep:pyo3"]
all-backends = ["duckdb", "postgres", "bigquery"]
```

**Development workflow:**
```bash
# Fast iteration on core logic (~1-2 seconds)
cargo check --no-default-features

# Test specific backend
cargo check --features postgres

# Full build with all backends
cargo build --features all-backends
```

---

## Core Definitions (`flows.rs`)

### SemanticTable
Represents a database table with semantic metadata.

```rust
pub struct SemanticTable {
    pub name: String,              // Unique identifier
    pub data_source: String,       // Connection name
    pub table: String,             // Physical table name
    pub primary_keys: Vec<String>, // PK columns
    pub time_dimension: Option<String>,
    pub dimensions: BTreeMap<String, Dimension>,
    pub measures: BTreeMap<String, Measure>,
    pub description: Option<String>,
}
```

### Dimension
A categorical attribute for grouping/filtering.

```rust
pub struct Dimension {
    pub expression: Expr,          // Column or computed expression
    pub data_type: Option<String>,
    pub description: Option<String>,
}
```

### Measure
An aggregatable metric.

```rust
pub struct Measure {
    pub expr: Expr,                // Base expression
    pub agg: Aggregation,          // SUM, COUNT, AVG, etc.
    pub filter: Option<Expr>,      // Optional measure-level filter
    pub post_expr: Option<Expr>,   // Derived measure expression
    pub data_type: Option<String>,
    pub description: Option<String>,
}

pub enum Aggregation {
    Sum, Count, CountDistinct, Min, Max, Avg
}
```

### SemanticFlow
Composes tables with joins into a queryable data model.

```rust
pub struct SemanticFlow {
    pub name: String,
    pub base_table: FlowTableRef,
    pub joins: BTreeMap<String, FlowJoin>,
    pub description: Option<String>,
}

pub struct FlowJoin {
    pub semantic_table: String,    // Table name to join
    pub alias: String,             // Query alias
    pub to_table: String,          // Table alias to join to
    pub join_type: JoinType,       // LEFT, INNER, etc.
    pub join_keys: Vec<JoinKey>,   // ON conditions
    pub cardinality: Option<Cardinality>,
}
```

### QueryRequest
Incoming query specification.

```rust
pub struct QueryRequest {
    pub flow: String,
    pub dimensions: Vec<String>,
    pub measures: Vec<String>,
    pub filters: Vec<Filter>,
    pub order: Vec<OrderItem>,
    pub limit: Option<i32>,
    pub offset: Option<i32>,
}
```

---

## Registry (`registry.rs`)

### FlowRegistry
Central storage for semantic definitions.

```rust
impl FlowRegistry {
    // Load from YAML files
    pub fn load_from_dir<P: AsRef<Path>>(dir: P) -> Result<Self>;

    // Build from code
    pub fn from_parts(tables: Vec<SemanticTable>, flows: Vec<SemanticFlow>) -> Self;

    // Lookup
    pub fn get_table(&self, name: &str) -> Option<&SemanticTable>;
    pub fn get_flow(&self, name: &str) -> Option<&SemanticFlow>;

    // Schema export (for APIs)
    pub fn flow_schema(&self, name: &str) -> Result<FlowSchema>;
    pub fn list_flow_summaries(&self) -> Vec<FlowSummary>;
}
```

---

## Query Builder (`query_builder/`)

### Entry Point (`mod.rs`)

```rust
pub struct SqlBuilder {
    supports_filtered_aggregates: bool,
}

impl SqlBuilder {
    pub fn build_for_request(
        &self,
        registry: &FlowRegistry,
        connections: &ConnectionManager,
        request: &QueryRequest,
    ) -> Result<String>;
}
```

### Planner (`planner.rs`)
Orchestrates query building by selecting the optimal strategy.

```rust
pub fn build_query(
    flow: &SemanticFlow,
    registry: &FlowRegistry,
    request: &QueryRequest,
    supports_filtered_aggregates: bool,
) -> Result<SelectQuery>;

fn build_flat_plan(components: &QueryComponents, ...) -> Result<QueryPlan>;
fn build_multi_grain_plan(components: &QueryComponents, ...) -> Result<QueryPlan>;
```

### Analysis (`analysis.rs`)
Determines when pre-aggregation is needed.

```rust
pub struct MultiGrainAnalysis {
    pub needs_multi_grain: bool,
    pub table_grains: HashMap<String, TableGrain>,
    pub cte_join_specs: Vec<CteJoinSpec>,
}

pub fn analyze_multi_grain(
    components: &QueryComponents,
    flow: &SemanticFlow,
) -> Result<MultiGrainAnalysis>;
```

**Triggers for multi-grain:**
1. Measures from multiple tables
2. Single-table measures with fanout risk from join filters

### Components (`components.rs`)
Resolves request fields to SQL expressions.

```rust
pub struct QueryComponents {
    pub base_alias: String,
    pub base_table: TableRef,
    pub dimensions: Vec<ResolvedDimension>,
    pub measures: Vec<ResolvedMeasure>,
    pub filters: Vec<ResolvedFilter>,
    pub order: Vec<OrderItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub alias_to_table: HashMap<String, SemanticTable>,
    pub join_lookup: HashMap<String, FlowJoin>,
}

pub fn resolve_components(
    flow: &SemanticFlow,
    registry: &FlowRegistry,
    request: &QueryRequest,
    supports_filtered_aggregates: bool,
) -> Result<QueryComponents>;
```

### Plan Types (`plan.rs`)

```rust
pub enum QueryPlan {
    Flat(FlatPlan),           // Simple SELECT with JOINs
    MultiGrain(MultiGrainPlan), // CTEs per table
}

pub struct FlatPlan {
    pub from: TableRef,
    pub select: Vec<SelectItem>,
    pub joins: Vec<Join>,
    pub filters: Vec<SqlExpr>,
    pub group_by: Vec<SqlExpr>,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

pub struct MultiGrainPlan {
    pub ctes: Vec<GrainedAggPlan>,
    pub final_query: FinalQueryPlan,
}
```

---

## SQL AST (`sql_ast.rs`)

### Expression Types

```rust
pub enum SqlExpr {
    Column { table: Option<String>, name: String },
    Literal(Value),
    Function { func: Function, args: Vec<SqlExpr> },
    Case { branches: Vec<(SqlExpr, SqlExpr)>, else_expr: Box<SqlExpr> },
    BinaryOp { op: SqlBinaryOperator, left: Box<SqlExpr>, right: Box<SqlExpr> },
    Aggregate { agg: Aggregation, expr: Box<SqlExpr> },
    FilteredAggregate { agg: Aggregation, expr: Box<SqlExpr>, filter: Box<SqlExpr> },
    InList { expr: Box<SqlExpr>, list: Vec<SqlExpr>, negated: bool },
    Exists { subquery: Box<SelectQuery> },
}
```

### Query Structures

```rust
pub struct SelectQuery {
    pub select: Vec<SelectItem>,
    pub from: TableRef,
    pub joins: Vec<Join>,
    pub filters: Vec<SqlExpr>,
    pub group_by: Vec<SqlExpr>,
    pub order_by: Vec<OrderItem>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

pub struct SelectItem {
    pub expr: SqlExpr,
    pub alias: Option<String>,
}
```

### SQL Renderer

```rust
pub struct SqlRenderer<'d> {
    dialect: &'d dyn Dialect,
}

impl SqlRenderer {
    pub fn render_select(&self, query: &SelectQuery) -> String;
}
```

---

## Dialect (`dialect/`)

The dialect module is organized as a directory with feature-gated implementations:

```
dialect/
├── mod.rs         # Dialect trait + feature-gated re-exports
├── duckdb.rs      # #[cfg(feature = "duckdb")]
├── postgres.rs    # #[cfg(feature = "postgres")]
└── bigquery.rs    # #[cfg(feature = "bigquery")]
```

### Dialect Trait

```rust
pub trait Dialect: Send + Sync {
    fn quote_ident(&self, ident: &str) -> String;
    fn placeholder(&self, idx: usize) -> String;  // "?" for DuckDB, "$1" for Postgres
    fn render_literal(&self, value: &Value) -> String;
    fn render_function(&self, func: &Function, args: Vec<String>) -> String;
    fn render_aggregation(&self, agg: &Aggregation, expr: &str) -> String;
    fn supports_filtered_aggregates(&self) -> bool;
}
```

### Available Dialects

| Dialect | Feature Flag | Identifier Quoting | Filtered Aggregates |
|---------|--------------|-------------------|---------------------|
| `DuckDbDialect` | `duckdb` | `"column"` | ✓ |
| `PostgresDialect` | `postgres` | `"column"` | ✓ |
| `BigQueryDialect` | `bigquery` | `` `column` `` | ✗ (uses CASE WHEN) |

---

## Backends (`backends/`)

The backends module is organized as a directory with feature-gated implementations:

```
backends/
├── mod.rs         # BackendConnection trait + ConnectionManager
├── duckdb.rs      # #[cfg(feature = "duckdb")]
├── postgres.rs    # #[cfg(feature = "postgres")]
└── bigquery.rs    # #[cfg(feature = "bigquery")]
```

### BackendConnection Trait

```rust
#[async_trait]
pub trait BackendConnection: Send + Sync {
    fn dialect(&self) -> &(dyn Dialect + Send + Sync);
    async fn fetch_schema(&self, table: &str) -> Result<TableSchema>;
    async fn execute_sql(&self, sql: &str) -> Result<QueryResult>;
}
```

### ConnectionManager

```rust
#[derive(Clone, Default)]
pub struct ConnectionManager {
    connections: HashMap<String, Arc<dyn BackendConnection>>,
}

impl ConnectionManager {
    pub fn new() -> Self;
    pub fn register(&mut self, name: String, connection: Arc<dyn BackendConnection>);
    pub fn get(&self, name: &str) -> Option<Arc<dyn BackendConnection>>;
}
```

### Available Backends

| Backend | Feature Flag | Connection Pool | Notes |
|---------|--------------|-----------------|-------|
| `DuckDbConnection` | `duckdb` | Custom semaphore-based | Bundled libduckdb |
| `PostgresConnection` | `postgres` | deadpool-postgres | Async with tokio |
| `BigQueryConnection` | `bigquery` | N/A (HTTP client) | Uses gcp-bigquery-client |

### DuckDbConnection

```rust
pub struct DuckDbConnection {
    path: String,
    max_concurrency: usize,
    pool: OnceCell<Pool<DuckDB>>,
}

impl DuckDbConnection {
    pub fn new(path: String) -> Self;
    pub fn with_max_concurrency(self, max: usize) -> Self;
}
```

### PostgresConnection

```rust
pub struct PostgresConnection {
    pool: Pool,
    dialect: PostgresDialect,
}

impl PostgresConnection {
    pub async fn new(connection_string: &str) -> Result<Self>;
}
```

### BigQueryConnection

```rust
pub struct BigQueryConnection {
    client: Client,
    project_id: String,
    dataset: String,
    dialect: BigQueryDialect,
}

impl BigQueryConnection {
    pub async fn new(project_id: &str, dataset: &str) -> Result<Self>;
}
```

### Query Execution

```rust
pub struct QueryResult {
    pub rows: Vec<Value>,
    pub sql: String,
}
```

---

## Python Bindings (`python/mod.rs`)

### SemanticFlowHandle
Main Python-facing class.

```rust
#[pyclass]
pub struct SemanticFlowHandle {
    registry: Arc<FlowRegistry>,
    connections: ConnectionManager,
}

#[pymethods]
impl SemanticFlowHandle {
    #[staticmethod]
    fn from_dir(flow_dir: &str, data_sources: &PyAny) -> PyResult<Self>;

    #[staticmethod]
    fn from_parts(tables: &PyAny, flows: &PyAny, data_sources: &PyAny) -> PyResult<Self>;

    fn build_sql(&self, request: &PyAny) -> PyResult<String>;
    fn execute(&self, request: &PyAny) -> PyResult<PyObject>;
    fn list_flows(&self) -> PyResult<PyObject>;
    fn get_flow(&self, name: &str) -> PyResult<PyObject>;
}
```

### Python Type Wrappers

```rust
#[pyclass] pub struct PyDataSource { ... }
#[pyclass] pub struct PySemanticTable { ... }
#[pyclass] pub struct PySemanticFlow { ... }
#[pyclass] pub struct PyDimension { ... }
#[pyclass] pub struct PyMeasure { ... }
#[pyclass] pub struct PyFlowJoin { ... }
```
