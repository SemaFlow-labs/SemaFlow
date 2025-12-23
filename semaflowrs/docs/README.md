# SemaFlow Technical Documentation

This directory contains detailed technical documentation for SemaFlow's Rust engine.

For the main project README and quick start guide, see the [root README](../../README.md).

## Documentation Index

### For Users

| Document | Description |
|----------|-------------|
| [Python API Reference](python-bindings.md) | Complete Python API documentation |
| [Join Semantics](join-semantics.md) | How joins and filters interact, NULL behavior |

### For Contributors

| Document | Description |
|----------|-------------|
| [Architecture](architecture.md) | System architecture diagrams and component overview |
| [Query Lifecycle](query-lifecycle.md) | Request → SQL → Results data flow |
| [Rust Components](rust-components.md) | Detailed Rust module documentation |

### Quick Reference

**Supported Backends:**
- DuckDB (default, embedded)
- PostgreSQL (async pooling)
- BigQuery (HTTP client)

**Key Features:**
- Multi-table measures with automatic pre-aggregation
- Cursor-based pagination (native BigQuery job pagination)
- Dialect-aware SQL rendering
- Connection pooling with backpressure

**Python Quick Start:**
```python
from semaflow import DataSource, FlowHandle

handle = FlowHandle.from_dir("flows/", [DataSource.duckdb("data.duckdb")])
result = await handle.execute({
    "flow": "sales",
    "dimensions": ["c.country"],
    "measures": ["o.order_total"],
    "page_size": 25  # Enable pagination
})
```

## Related Documentation

| Location | Content |
|----------|---------|
| [docs/concepts.md](../../docs/concepts.md) | Core semantic layer concepts |
| [docs/dialects.md](../../docs/dialects.md) | Backend-specific SQL rendering |
| [docs/configuration.md](../../docs/configuration.md) | TOML configuration reference |
| [docs/expressions.md](../../docs/expressions.md) | Expression syntax and derived measures |
| [CONTRIBUTING.md](../../CONTRIBUTING.md) | Development setup and PR guidelines |
