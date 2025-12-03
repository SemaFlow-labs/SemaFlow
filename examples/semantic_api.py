"""
Minimal FastAPI app exposing a SemanticFlow.

Run:
    uv run python examples/semantic_api.py
    (or rely on the __main__ block to launch uvicorn)
"""

from pathlib import Path

import duckdb
import uvicorn

from semaflow import DataSource, FlowHandle
from semaflow.api import create_app


def seed_duckdb(db_path: Path) -> None:
    if db_path.exists():
        db_path.unlink()
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
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
            (3, 'Carla', 'US');
        INSERT INTO orders VALUES
            (1, 1, 100.0, '2023-01-01'),
            (2, 1, 50.0, '2023-01-02'),
            (3, 2, 25.0, '2023-01-03');
        """
    )
    conn.close()


def build_flow():
    project_root = Path(__file__).resolve().parents[1]
    flow_root = project_root / "examples" / "flows"
    db_path = project_root / "examples" / "demo_python.duckdb"
    seed_duckdb(db_path)
    return FlowHandle.from_dir(
        flow_root,
        [DataSource.duckdb(str(db_path), name="duckdb_local")],
    )


flow = build_flow()
app = create_app(flow)


if __name__ == "__main__":
    uvicorn.run(
        "examples.semantic_api:app",
        host="0.0.0.0",
        port=8080,
        reload=False,
        loop="uvloop",
        log_level="info",
    )
