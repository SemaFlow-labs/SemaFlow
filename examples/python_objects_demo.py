"""
Pure-Python construction of tables/flows and execution against DuckDB via class-based SemanticFlow definitions.

Steps:
- seed a DuckDB file with sample data
- build semantic tables/flows as Python objects (no YAML)
- execute a request and print SQL + results
"""

import asyncio
from pathlib import Path

import duckdb

from semaflow import (
    DataSource,
    Dimension,
    FlowJoin,
    JoinKey,
    Measure,
    SemanticFlow,
    SemanticTable,
    build_flow_handles,
)


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
    db_path = project_root / "examples" / "demo_python.duckdb"
    seed_duckdb(db_path)

    ds = DataSource.duckdb(str(db_path), name="duckdb_local")

    orders = SemanticTable(
        name="orders",
        data_source=ds,
        table="orders",
        primary_key="id",
        time_dimension="created_at",
        dimensions={
            "id": Dimension("id", description="Order primary key"),
            "customer_id": Dimension("customer_id", description="Foreign key to customers"),
            "created_at": Dimension("created_at", description="Order timestamp"),
        },
        measures={
            "order_total": Measure("amount", "sum", description="Total order amount"),
            "order_count": Measure("id", "count", description="Count of orders"),
            "distinct_customers": Measure(
                "customer_id", "count_distinct", description="Distinct customers ordering"
            ),
        },
        description="Orders fact table",
    )

    customers = SemanticTable(
        name="customers",
        data_source=ds,
        table="customers",
        primary_key="id",
        dimensions={
            "id": Dimension("id", description="Customer primary key"),
            "country": Dimension("country", description="Customer country"),
        },
        measures={"customer_count": Measure("id", "count", description="Count of customers")},
        description="Customers dimension",
    )

    sales_flow = SemanticFlow(
        name="sales",
        base_table=orders,
        base_table_alias="o",
        joins=[
            FlowJoin(
                semantic_table=customers,
                alias="c",
                to_table="o",
                join_type="left",
                join_keys=[JoinKey("customer_id", "id")],
            )
        ],
        description="Sales data for the company",
    )

    flow = build_flow_handles({"sales": sales_flow})

    request = {
        "flow": "sales",
        "dimensions": ["c.country"],
        "measures": ["o.order_total", "c.customer_count"],
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


if __name__ == "__main__":
    asyncio.run(main())
