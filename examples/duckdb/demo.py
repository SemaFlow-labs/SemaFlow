"""
DuckDB demo - seed database and run queries via SemaFlow.

Usage:
    cd examples/duckdb
    uv run python demo.py
"""

import asyncio
from pathlib import Path

import duckdb

from semaflow import DataSource, FlowHandle


def seed_duckdb(db_path: Path) -> None:
    """Create and seed the DuckDB database with sample data."""
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
    print(f"Seeded DuckDB: {db_path}")


async def main() -> None:
    example_dir = Path(__file__).parent
    flow_dir = example_dir / "flows"
    db_path = example_dir / "sales.duckdb"

    # Seed database
    seed_duckdb(db_path)

    # Create FlowHandle with DuckDB data source
    ds = DataSource.duckdb(str(db_path), name="duckdb_local")
    handle = FlowHandle.from_dir(str(flow_dir), [ds])

    # List available flows
    print("\n--- Available Flows ---")
    for flow in handle.list_flows():
        print(f"  {flow['name']}: {flow.get('description', 'No description')}")

    # Show schema for sales flow
    schema = handle.get_flow("sales")
    print("\n--- Sales Flow Schema ---")
    print("Dimensions:")
    for dim in schema["dimensions"]:
        print(f"  {dim['qualified_name']}: {dim.get('description', '')}")
    print("Measures:")
    for measure in schema["measures"]:
        print(f"  {measure['qualified_name']}: {measure.get('description', '')}")

    # Query 1: Total sales by country
    print("\n--- Query 1: Sales by Country ---")
    request = {
        "flow": "sales",
        "dimensions": ["c.country"],
        "measures": ["o.order_total", "o.order_count"],
        "order": [{"column": "o.order_total", "direction": "desc"}],
    }
    sql = await handle.build_sql(request)
    print(f"SQL:\n{sql}\n")

    rows = await handle.execute(request)
    print("Results:")
    for row in rows:
        print(f"  {row}")

    # Query 2: Filter by country
    print("\n--- Query 2: US Sales Only ---")
    request = {
        "flow": "sales",
        "dimensions": ["c.country"],
        "measures": ["o.order_total", "c.customer_count"],
        "filters": [{"field": "c.country", "op": "==", "value": "US"}],
    }
    sql = await handle.build_sql(request)
    print(f"SQL:\n{sql}\n")

    rows = await handle.execute(request)
    print("Results:")
    for row in rows:
        print(f"  {row}")


if __name__ == "__main__":
    asyncio.run(main())
