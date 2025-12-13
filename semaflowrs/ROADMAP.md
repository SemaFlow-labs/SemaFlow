# Semaflow Roadmap

## Current Status (v0.2.0)

### Completed
- [x] Core semantic layer engine (flows, tables, joins)
- [x] DuckDB backend (bundled, now optional)
- [x] PostgreSQL backend (`--features postgres`)
- [x] BigQuery backend (`--features bigquery`)
- [x] Python bindings via PyO3 (DataSource.duckdb/postgres/bigquery)
- [x] Structured logging with tracing
- [x] Join pruning optimisation
- [x] Filtered aggregates (FILTER WHERE clause)
- [x] Composite/derived measures
- [x] **Module restructuring** (feature = file pattern)
- [x] **DuckDB as optional feature** (fast dev builds)

### Architecture (v0.2.0)

```
src/
├── dialect/
│   ├── mod.rs              # Dialect trait + feature-gated re-exports
│   ├── duckdb.rs           # DuckDB SQL rendering
│   ├── postgres.rs         # PostgreSQL SQL rendering
│   └── bigquery.rs         # BigQuery SQL rendering
├── backends/
│   ├── mod.rs              # BackendConnection trait + ConnectionManager
│   ├── duckdb.rs           # DuckDB connection + pooling
│   ├── postgres.rs         # PostgreSQL connection (deadpool)
│   └── bigquery.rs         # BigQuery client
└── python/
    └── mod.rs              # PyO3 bindings (all in one for now)
```

### Feature Flags

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

## v0.3.0 - Arrow Support

### Native Arrow Returns
Add Arrow table support for high-performance Python workflows:

```python
# Current (JSON serialisation)
rows = handle.execute(request)  # list[dict]

# New options
arrow_table = handle.execute(request, format="arrow")    # pyarrow.Table
polars_df = handle.execute(request, format="polars")     # polars.DataFrame
pandas_df = handle.execute(request, format="pandas")     # pandas.DataFrame
```

### Implementation Plan

1. **Rust side:**
   - Keep `QueryResult` with JSON for HTTP API compatibility
   - Add `QueryResultArrow` returning `arrow::RecordBatch`
   - DuckDB already returns Arrow internally - avoid double conversion

2. **Python side:**
   - Use `arrow-rs` + `pyo3-arrow` for zero-copy Arrow <-> PyArrow
   - Add `format` parameter to `execute()`
   - Lazy-load pyarrow/polars to avoid import overhead

3. **Performance target:**
   - 10-100x faster for large result sets (100k+ rows)
   - Zero-copy from DuckDB -> PyArrow

### Dependencies
```toml
# Arrow support
arrow = { version = "56", optional = true }
pyo3-arrow = { version = "0.4", optional = true }

[features]
arrow = ["dep:arrow", "dep:pyo3-arrow"]
```

---

## v0.4.0 - Production Hardening

### Timeouts & Retries
- [ ] Query timeout configuration per data source
- [ ] Automatic retry with exponential backoff
- [ ] Circuit breaker for failing backends

### Connection Management
- [ ] Connection health checks
- [ ] Graceful pool draining on shutdown
- [ ] Per-query connection timeout

### Observability
- [ ] OpenTelemetry integration
- [ ] Query metrics (latency histograms, row counts)
- [ ] Slow query logging threshold

---

## v0.5.0 - Advanced Features

### Query Capabilities
- [ ] Window functions in measures
- [ ] Subquery support in filters
- [ ] HAVING clause support
- [ ] LIMIT/OFFSET in requests

### Caching
- [ ] Schema cache with TTL
- [ ] Query plan caching

### Additional Backends
- [ ] Snowflake (`--features snowflake`)
- [ ] Databricks (`--features databricks`)
- [ ] ClickHouse (`--features clickhouse`)

---

## Future Considerations

### Python Module Split (v0.2.x)
Split large python/mod.rs into focused modules:
```
python/
├── mod.rs              # Module init + serde helpers
├── data_source.rs      # PyDataSource, PyTableHandle
├── semantic.rs         # PySemanticTable, PySemanticFlow, PyFlowJoin
├── handle.rs           # SemanticFlowHandle
└── types.rs            # PyDimension, PyMeasure, PyJoinKey
```

### HTTP API
- REST API server mode
- GraphQL interface
- OpenAPI spec generation

### IDE Integration
- Language server for flow YAML files
- VSCode extension with autocomplete
- Flow visualisation

### Testing
- Property-based testing for SQL generation
- Fuzzing for parser robustness
- Performance regression tests

---

## Contributing

When adding a new backend:
1. Create `src/dialect/<backend>.rs` with the dialect implementation
2. Create `src/backends/<backend>.rs` with the connection implementation
3. Add feature flag to `Cargo.toml`
4. Add Python bindings in `src/python/mod.rs` (DataSource.<backend>() method)
5. Add tests in `tests/<backend>_tests.rs`
6. Update this roadmap!
