"""
End-to-end Python demo using the Rust core via PyO3 bindings.

Steps:
- seed a DuckDB file with sample data
- load semantic tables/flows from examples/flows
- list flows/dimensions/measures via FlowHandle
- build SQL for a request (supports alias-qualified field names)
- execute and print results
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

    print("Flows:")
    for m in flow.list_flows():
        print(f"- {m.get('name')} ({m.get('description', '')})")
    print()

    schema = flow.get_flow("sales")
    print("Sales flow dimensions (qualified):")
    for d in schema["dimensions"]:
        print(f"- {d['qualified_name']}: {d.get('description', '')}")
    print("Sales flow measures (qualified):")
    for m in schema["measures"]:
        print(f"- {m['qualified_name']}: {m.get('description', '')}")
    print(f"time_dimension: {schema.get('time_dimension')}")
    print()

    request = {
        "flow": "sales",
        "dimensions": ["c.country"],
        "measures": ["o.order_total", "o.order_count", "o.us_order_total", "o.avg_order_amount", "c.customer_count"],
        "filters": [],
        "order": [{"column": "o.order_total", "direction": "desc"}],
        "limit": 10,
    }

    sql = await flow.build_sql(request)
    print("SQL:")
    print(sql)
    print()

    rows = await flow.execute(request)
    print("Results:")
    for row in rows:
        print(row)

    request = {
        "flow": "sales",
        "dimensions": ["c.country"],
        "measures": ["o.order_total", "c.customer_count"],
        "filters": [{"field": "c.country", "op": "==", "value": "US"}],
        "order": [{"column": "o.order_total", "direction": "desc"}],
        "limit": 10,
    }

    sql = await flow.build_sql(request)
    print("SQL:")
    print(sql)
    print()

    rows = await flow.execute(request)
    print("Results:")
    for row in rows:
        print(row)


if __name__ == "__main__":
    asyncio.run(main())
