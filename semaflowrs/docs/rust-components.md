# Rust Components Reference

## Module Overview

```
semaflowrs/src/
├── lib.rs                 # Public API exports
├── flows.rs               # Semantic definitions (tables, flows, measures)
├── registry.rs            # Flow storage and lookup
├── sql_ast.rs             # Typed SQL AST structures
├── dialect.rs             # Database-specific SQL rendering
├── executor.rs            # Query execution
├── data_sources.rs        # Connection management
├── validation.rs          # Schema validation
├── runtime.rs             # Query orchestration
├── error.rs               # Error types
├── expr_parser.rs         # Expression parsing
├── expr_utils.rs          # Expression utilities
├── schema_cache.rs        # Database schema caching
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

## Dialect (`dialect.rs`)

```rust
pub trait Dialect: Send + Sync {
    fn quote_ident(&self, ident: &str) -> String;
    fn render_literal(&self, value: &Value) -> String;
    fn render_function(&self, func: &Function, args: Vec<String>) -> String;
    fn render_aggregation(&self, agg: &Aggregation, expr: &str) -> String;
    fn supports_filtered_aggregates(&self) -> bool;
}

pub struct DuckDbDialect;
// Implements Dialect with DuckDB-specific SQL
```

---

## Execution (`executor.rs`, `data_sources.rs`)

### ConnectionManager

```rust
pub type ConnectionManager = HashMap<String, Arc<dyn BackendConnection>>;

pub trait BackendConnection: Send + Sync {
    fn execute(&self, sql: &str) -> BoxFuture<'_, Result<Vec<Value>>>;
    fn get_schema(&self, table: &str) -> BoxFuture<'_, Result<TableSchema>>;
    fn dialect(&self) -> Box<dyn Dialect>;
}
```

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

### Query Execution

```rust
pub async fn execute_query(
    connections: &ConnectionManager,
    data_source: &str,
    sql: &str,
) -> Result<QueryResult>;

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
