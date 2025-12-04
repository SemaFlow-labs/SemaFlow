"""
Demonstrate measure filter rendering with and without FILTER support.

By default DuckDB supports FILTER (WHERE ...), but you can force the portable
CASE-wrapped form by setting the env var SEMAFLOW_DISABLE_FILTERED_AGG=1.
"""

import asyncio
import os
from pathlib import Path

import duckdb

from semaflow import DataSource, FlowHandle


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


async def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    flow_root = project_root / "examples" / "flows"
    db_path = project_root / "examples" / "demo_python.duckdb"

    seed_duckdb(db_path)

    flow = FlowHandle.from_dir(flow_root, [DataSource.duckdb(str(db_path), name="duckdb_local")])

    request = {
        "flow": "sales",
        "dimensions": [],
        "measures": ["o.us_order_total"],
        "filters": [],
        "order": [],
        "limit": None,
    }

    sql = await flow.build_sql(request)
    print("Default (FILTER supported):")
    print(sql)
    print()

    os.environ["SEMAFLOW_DISABLE_FILTERED_AGG"] = "1"
    sql_fallback = await flow.build_sql(request)
    print("Forced fallback (CASE-wrapped aggregate):")
    print(sql_fallback)
    print()
    os.environ.pop("SEMAFLOW_DISABLE_FILTERED_AGG", None)


if __name__ == "__main__":
    asyncio.run(main())
