"""
Show a pre-aggregated plan that also carries a derived measure (post_expr).

We trigger pre-aggregation by filtering on a join dimension (customers.country),
and include the avg_order_amount derived measure to show it being computed in the
outer select after the fact pre-aggregation.
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

    request = {
        "flow": "sales",
        "dimensions": ["c.country"],
        "measures": ["o.order_total", "o.order_count", "o.avg_order_amount"],
        "filters": [{"field": "c.country", "op": "==", "value": "US"}],
        "order": [{"column": "o.order_total", "direction": "desc"}],
        "limit": 10,
    }

    sql = await flow.build_sql(request)
    print("SQL (pre-agg with derived measure):")
    print(sql)
    print()

    rows = await flow.execute(request)
    print("Rows:")
    for row in rows:
        print(row)


if __name__ == "__main__":
    asyncio.run(main())
