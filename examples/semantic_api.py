"""
Configurable FastAPI app - switch between DuckDB and PostgreSQL.

Usage:
    # DuckDB (default)
    uv run python examples/semantic_api.py

    # PostgreSQL (requires docker compose up first)
    SEMAFLOW_BACKEND=postgres uv run python examples/semantic_api.py

    # Test endpoints
    curl http://localhost:8080/flows
    curl http://localhost:8080/flows/sales
    curl -X POST http://localhost:8080/flows/sales/query \
        -H "Content-Type: application/json" \
        -d '{"dimensions": ["c.country"], "measures": ["o.order_total"]}'

Environment Variables:
    SEMAFLOW_BACKEND    - "duckdb" (default) or "postgres"
    POSTGRES_HOST       - PostgreSQL host (default: localhost)
    POSTGRES_PORT       - PostgreSQL port (default: 5432)
    POSTGRES_USER       - Username (default: semaflow)
    POSTGRES_PASSWORD   - Password (default: semaflow_pass)
    POSTGRES_DB         - Database (default: semaflow_demo)
    POSTGRES_SCHEMA     - Schema (default: public)
"""

import os
from pathlib import Path

import uvicorn

from semaflow import DataSource, FlowHandle
from semaflow.api import create_app


def get_backend() -> str:
    return os.environ.get("SEMAFLOW_BACKEND", "duckdb").lower()


def build_duckdb_flow() -> FlowHandle:
    """Build FlowHandle with DuckDB backend."""
    import duckdb

    project_root = Path(__file__).resolve().parent
    flow_root = project_root / "duckdb" / "flows"
    db_path = project_root / "duckdb" / "api_test.duckdb"

    # Seed database
    if db_path.exists():
        db_path.unlink()
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            country VARCHAR
        );
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount DOUBLE,
            created_at TIMESTAMP
        );
        INSERT INTO customers VALUES
            (1, 'Alice', 'US'),
            (2, 'Bob', 'UK'),
            (3, 'Carla', 'US'),
            (4, 'David', 'DE');
        INSERT INTO orders VALUES
            (1, 1, 100.0, '2024-01-01'),
            (2, 1, 50.0, '2024-01-02'),
            (3, 2, 25.0, '2024-01-03'),
            (4, 3, 200.0, '2024-01-04'),
            (5, 3, 75.0, '2024-01-05');
    """)
    conn.close()

    return FlowHandle.from_dir(
        str(flow_root),
        [DataSource.duckdb(str(db_path), name="duckdb_local")],
    )


def build_postgres_flow() -> FlowHandle:
    """Build FlowHandle with PostgreSQL backend."""
    project_root = Path(__file__).resolve().parent
    flow_root = project_root / "postgres" / "flows"

    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    user = os.environ.get("POSTGRES_USER", "semaflow")
    password = os.environ.get("POSTGRES_PASSWORD", "semaflow_pass")
    database = os.environ.get("POSTGRES_DB", "semaflow_demo")
    schema = os.environ.get("POSTGRES_SCHEMA", "public")

    conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    return FlowHandle.from_dir(
        str(flow_root),
        [DataSource.postgres(conn_string, schema=schema, name="pg_local")],
    )


def build_flow() -> FlowHandle:
    backend = get_backend()
    print(f"Starting API with backend: {backend}")

    if backend == "postgres":
        return build_postgres_flow()
    else:
        return build_duckdb_flow()


flow = build_flow()
app = create_app(flow)


if __name__ == "__main__":
    uvicorn.run(
        "examples.semantic_api:app",
        host="0.0.0.0",
        port=8080,
        reload=False,
        log_level="info",
    )
