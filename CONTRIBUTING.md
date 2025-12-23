# Contributing to SemaFlow

Thanks for your interest in contributing! This guide covers development setup, testing, and pull request guidelines.

## Development Setup

### Prerequisites

- **Rust** (1.75+): Install via [rustup](https://rustup.rs/)
- **Python** (3.10+): Recommended to use [uv](https://github.com/astral-sh/uv) for dependency management
- **maturin**: For building Python bindings

### Quick Start

```bash
# Clone the repo
git clone https://github.com/your-org/semaflow.git
cd semaflow

# Set up Python environment and install dependencies
uv sync

# Build Python bindings (first build compiles DuckDB, takes a few minutes)
uv run maturin develop -m semaflowrs/Cargo.toml -F python --locked

# Verify installation
uv run python -c "from semaflow import FlowHandle; print('OK')"
```

### Avoiding Repeated Rebuilds

`uv` will try to rebuild the project before running commands. To skip this:

```bash
# After initial build, use --no-sync to skip rebuild check
uv run --no-sync examples/python_demo.py
```

## Project Structure

```
semaflow/
├── semaflow/               # Python package (thin wrappers)
│   ├── api.py              # FastAPI integration
│   ├── core.py             # Re-exports from Rust
│   ├── handle.py           # FlowHandle wrapper
│   └── semaflow.pyi        # Type stubs
├── semaflowrs/             # Rust crate (core engine)
│   ├── src/
│   │   ├── lib.rs          # Public API
│   │   ├── flows.rs        # Semantic definitions
│   │   ├── query_builder/  # SQL generation
│   │   ├── backends/       # DuckDB, Postgres, BigQuery
│   │   ├── dialect/        # SQL dialect rendering
│   │   └── python/         # PyO3 bindings
│   └── Cargo.toml
├── examples/               # Usage examples
├── tests/                  # Python integration/performance tests
└── docs/                   # Documentation
```

## Building & Testing

### Rust

All commands run from the **workspace root** (`semaflow/`):

```bash
# Fast iteration (no backends, ~1-2 seconds)
cargo check --no-default-features

# Check with specific backend
cargo check --features postgres

# Check with multiple backends
cargo check --features postgres,bigquery

# Run Rust tests (uses default features)
cargo test

# Run with all backends
cargo test --features all-backends

# Clippy lints
cargo clippy --features all-backends -- -D warnings
```

### Python

```bash
# Rebuild bindings after Rust changes
uv run maturin develop -m semaflowrs/Cargo.toml -F <features> --locked
```

### Feature Flags

| Flag | Purpose |
|------|---------|
| `duckdb` | DuckDB backend (default) |
| `postgres` | PostgreSQL backend |
| `bigquery` | BigQuery backend |
| `python` | PyO3 bindings |
| `all-backends` | All database backends |

```bash
# Build wheel with all backends
uv run maturin build --features all-backends
```

## Making Changes

### Code Style

**Rust:**
- Run `cargo fmt` before committing
- Follow Clippy suggestions
- Document public APIs with `///` doc comments

**Python:**
- Follow PEP 8
- Use type hints
- Keep Python code thin - heavy logic should be in Rust
- Ensure the GIL is released during long-running operations

### Commit Messages

Use conventional commit format:

```
feat: add cursor-based pagination for BigQuery
fix: handle NULL values in derived measures
docs: update dialect reference for Postgres
refactor: extract query planning into separate module
test: add integration tests for multi-grain queries
```

### Pull Request Process

1. **Fork and branch**: Create a feature branch from `main`
   ```bash
   git checkout -b feat/my-feature
   ```

2. **Make changes**: Keep PRs focused on a single concern

3. **Test locally**:
   ```bash
   cargo test --features all-backends
   cargo clippy --features all-backends -- -D warnings
   uv run maturin develop
   uv run pytest tests/
   ```

4. **Update docs**: If adding features, update relevant docs

5. **Open PR**:
   - Describe what the PR does and why
   - Link related issues
   - Include example usage for new features

6. **Review**: Address feedback, keep commits clean

## Testing Guidelines

### Unit Tests (Rust)

Located in `semaflowrs/src/**/*.rs` as `#[cfg(test)]` modules:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dimension_resolution() {
        // ...
    }
}
```

### Integration Tests (Python)

Located in `tests/`:

```python
# tests/test_pagination.py
import pytest
from semaflow import FlowHandle, DataSource

@pytest.mark.asyncio
async def test_paginated_query():
    handle = FlowHandle.from_dir("examples/flows", [...])
    result = await handle.execute({
        "flow": "sales",
        "dimensions": ["c.country"],
        "measures": ["o.order_total"],
        "page_size": 10
    })
    assert "rows" in result
    assert "has_more" in result
```

### Testing with Different Backends

```bash
# DuckDB (default)
cargo test

# PostgreSQL (requires running Postgres)
POSTGRES_URL=postgresql://user:pass@localhost/test \
  cargo test --features postgres

# BigQuery (requires GCP credentials)
GCP_PROJECT_ID=my-project BQ_DATASET=test \
  cargo test --features bigquery
```

## Adding a New Backend

1. **Add Cargo feature** in `semaflowrs/Cargo.toml`:
   ```toml
   [features]
   mybackend = ["dep:my-backend-crate"]
   ```

2. **Implement dialect** in `semaflowrs/src/dialect/mybackend.rs`:
   ```rust
   pub struct MyBackendDialect;

   impl Dialect for MyBackendDialect {
       fn quote_ident(&self, ident: &str) -> String { ... }
       // ...
   }
   ```

3. **Implement backend** in `semaflowrs/src/backends/mybackend.rs`:
   ```rust
   pub struct MyBackendConnection { ... }

   #[async_trait]
   impl BackendConnection for MyBackendConnection {
       fn dialect(&self) -> &dyn Dialect { ... }
       async fn execute_sql(&self, sql: &str) -> Result<QueryResult> { ... }
   }
   ```

4. **Add feature gates** in `mod.rs` files

5. **Add Python DataSource method** in `semaflowrs/src/python/mod.rs`

6. **Update docs**: `docs/dialects.md`, `semaflow/semaflow.pyi`

## Getting Help

- **Issues**: Open a GitHub issue for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions

## License

By contributing, you agree that your contributions will be licensed under the same terms as the project.
