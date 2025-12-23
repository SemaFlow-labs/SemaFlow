# SemaFlow Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SemaFlow Architecture                            │
└─────────────────────────────────────────────────────────────────────────┘

                              ┌─────────────┐
                              │   Python    │
                              │  API/CLI    │
                              └──────┬──────┘
                                     │ QueryPayload
                                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│                           Rust Engine (PyO3)                            │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐                │
│  │   Registry   │   │    Query     │   │   SQL AST    │                │
│  │   (flows,    │──▶│   Builder    │──▶│   Renderer   │                │
│  │   tables)    │   │  (planner)   │   │  (dialect)   │                │
│  └──────────────┘   └──────────────┘   └──────────────┘                │
│         │                  │                  │                         │
│         ▼                  ▼                  ▼                         │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐                │
│  │  Validation  │   │   Analysis   │   │   Executor   │────────────┐   │
│  │  (schema)    │   │  (fanout,    │   │  (DuckDB)    │            │   │
│  │              │   │   grain)     │   │              │            │   │
│  └──────────────┘   └──────────────┘   └──────────────┘            │   │
└────────────────────────────────────────────────────────────────────│───┘
                                                                     │
                                                                     ▼
                                                              ┌─────────────┐
                                                              │   DuckDB    │
                                                              │  Database   │
                                                              └─────────────┘
```

## Layer Responsibilities

### Python Layer (`semaflow/`)

| Component | File | Responsibility |
|-----------|------|----------------|
| FlowHandle | `handle.py` | High-level API wrapper, async execution |
| API | `api.py` | FastAPI routes for REST interface |
| Core | `core.py` | Re-exports from Rust bindings |

### Rust Engine (`semaflowrs/src/`)

| Layer | Modules | Responsibility |
|-------|---------|----------------|
| **Definitions** | `flows.rs` | Data structures for tables, flows, measures, dimensions |
| **Storage** | `registry.rs` | Flow registry, schema lookup |
| **Query Building** | `query_builder/` | SQL generation pipeline |
| **SQL Types** | `sql_ast.rs` | Typed AST for SQL queries |
| **Rendering** | `dialect.rs` | Database-specific SQL generation |
| **Execution** | `executor.rs`, `data_sources.rs` | Query execution, connection management |
| **Validation** | `validation.rs` | Schema validation against database |
| **Python Bindings** | `python/mod.rs` | PyO3 interface |

## Data Flow

```
Request                          Internal                         Output
────────────────────────────────────────────────────────────────────────

QueryPayload ──┐
{              │
  flow,        │    ┌─────────────────┐    ┌─────────────────┐
  dimensions,  │───▶│ resolve_        │───▶│ QueryComponents │
  measures,    │    │ components()    │    │ (resolved refs) │
  filters      │    └─────────────────┘    └────────┬────────┘
}              │                                    │
               │                                    ▼
               │                           ┌─────────────────┐
               │                           │ analyze_        │
               │                           │ multi_grain()   │
               │                           └────────┬────────┘
               │                                    │
               │                    ┌───────────────┴───────────────┐
               │                    ▼                               ▼
               │           ┌───────────────┐              ┌───────────────┐
               │           │  FlatPlan     │              │ MultiGrainPlan│
               │           │ (simple JOIN) │              │  (CTEs)       │
               │           └───────┬───────┘              └───────┬───────┘
               │                   │                              │
               │                   └──────────────┬───────────────┘
               │                                  ▼
               │                         ┌───────────────┐
               │                         │ SelectQuery   │
               │                         │ (typed AST)   │
               │                         └───────┬───────┘
               │                                 │
               │                                 ▼
               │                         ┌───────────────┐
               │                         │ SqlRenderer   │───▶ SQL String
               │                         │ (dialect)     │
               │                         └───────────────┘
               │
               │                                 │
               │                                 ▼
               │                         ┌───────────────┐
               └────────────────────────▶│   Executor    │───▶ QueryResult
                                         │   (DuckDB)    │     { rows, sql }
                                         └───────────────┘
```

## Query Strategy Selection

The query builder analyzes each request to choose the optimal strategy:

```
                    ┌─────────────────────────────┐
                    │     Incoming Request        │
                    └──────────────┬──────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────┐
                    │  Measures from multiple     │
                    │       tables?               │
                    └──────────────┬──────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │                             │
                   Yes                           No
                    │                             │
                    ▼                             ▼
          ┌─────────────────┐      ┌─────────────────────────┐
          │ MultiGrainPlan  │      │   Fanout risk from      │
          │ (N CTEs joined) │      │   join filters?         │
          └─────────────────┘      └──────────────┬──────────┘
                                                  │
                                   ┌──────────────┴──────────┐
                                   │                         │
                                  Yes                       No
                                   │                         │
                                   ▼                         ▼
                         ┌─────────────────┐      ┌─────────────────┐
                         │ MultiGrainPlan  │      │    FlatPlan     │
                         │ (1 CTE + joins) │      │ (standard JOIN) │
                         └─────────────────┘      └─────────────────┘
```

## Module Dependency Graph

```
                                lib.rs
                                   │
           ┌───────────────────────┼───────────────────────┐
           │                       │                       │
           ▼                       ▼                       ▼
       flows.rs              registry.rs            data_sources.rs
           │                       │                       │
           │                       ▼                       │
           │              query_builder/mod.rs             │
           │                       │                       │
           │     ┌─────────────────┼─────────────────┐     │
           │     │                 │                 │     │
           │     ▼                 ▼                 ▼     │
           │  planner.rs     analysis.rs      components.rs│
           │     │                 │                 │     │
           │     └────────────┬────┴────────────┬────┘     │
           │                  │                 │          │
           │                  ▼                 ▼          │
           │             plan.rs          builders.rs      │
           │                  │                 │          │
           │                  └────────┬────────┘          │
           │                           │                   │
           │                           ▼                   │
           │                      sql_ast.rs               │
           │                           │                   │
           │                           ▼                   │
           │                      dialect.rs               │
           │                           │                   │
           └───────────────────────────┼───────────────────┘
                                       │
                                       ▼
                                  executor.rs
                                       │
                                       ▼
                                  runtime.rs
                                       │
                                       ▼
                                python/mod.rs
```

## Connection Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                      ConnectionManager                               │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │  HashMap<String, Arc<dyn BackendConnection>>                  │  │
│  │                                                               │  │
│  │  "duckdb_local" ──▶ DuckDbConnection ──▶ Connection Pool      │  │
│  │  "duckdb_prod"  ──▶ DuckDbConnection ──▶ Connection Pool      │  │
│  │                                                               │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘

DuckDbConnection
├── path: String (database file path)
├── max_concurrency: usize (pool size)
└── pool: OnceCell<Pool<DuckDB>>
    └── Lazy initialisation on first query
```
