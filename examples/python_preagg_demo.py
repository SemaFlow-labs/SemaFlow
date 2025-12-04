"""
Demonstrate flat vs pre-aggregated SQL from Python when filtering on a join dimension.

This reuses the sample flows in examples/flows and shows how a dimension filter on the
joined customers table triggers the new pre-aggregation + EXISTS plan.
"""

import asyncio
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

    flat_request = {
        "flow": "sales",
        "dimensions": ["o.created_at"],
        "measures": ["o.order_total"],
        "filters": [],
        "order": [{"column": "o.created_at", "direction": "asc"}],
        "limit": 10,
    }

    preagg_request = {
        "flow": "sales",
        "dimensions": ["c.country"],
        "measures": ["o.order_total"],
        "filters": [{"field": "c.country", "op": "==", "value": "US"}],
        "order": [{"column": "o.order_total", "direction": "desc"}],
        "limit": 10,
    }

    print("Flat path (no join filters):")
    flat_sql = await flow.build_sql(flat_request)
    print(flat_sql)
    print()
    flat_rows = await flow.execute(flat_request)
    print("Rows:", flat_rows)
    print("\n----\n")

    print("Pre-aggregated path (join-dimension filter triggers EXISTS + derived table):")
    preagg_sql = await flow.build_sql(preagg_request)
    print(preagg_sql)
    print()
    preagg_rows = await flow.execute(preagg_request)
    print("Rows:", preagg_rows)


if __name__ == "__main__":
    asyncio.run(main())
