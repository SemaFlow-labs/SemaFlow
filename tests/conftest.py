"""
Shared pytest fixtures for SemaFlow tests.
"""

from pathlib import Path

import duckdb
import pytest

from semaflow import (
    DataSource,
    Dimension,
    FlowHandle,
    FlowJoin,
    JoinKey,
    Measure,
    SemanticFlow,
    SemanticTable,
)


@pytest.fixture
def tmp_db_path(tmp_path: Path) -> Path:
    """Return a temporary path for a DuckDB database."""
    return tmp_path / "test.duckdb"


@pytest.fixture
def seeded_db(tmp_db_path: Path) -> Path:
    """Create a DuckDB database with test data."""
    conn = duckdb.connect(str(tmp_db_path))
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
            status VARCHAR,
            created_at TIMESTAMP
        );
        INSERT INTO customers VALUES
            (1, 'Alice', 'US'),
            (2, 'Bob', 'UK'),
            (3, 'Carla', 'US'),
            (4, 'David', 'DE');
        INSERT INTO orders VALUES
            (1, 1, 100.0, 'complete', '2024-01-01'),
            (2, 1, 50.0, 'complete', '2024-01-02'),
            (3, 2, 25.0, 'pending', '2024-01-03'),
            (4, 3, 200.0, 'complete', '2024-01-04'),
            (5, 3, 75.0, 'pending', '2024-01-05');
    """)
    conn.close()
    return tmp_db_path


@pytest.fixture
def orders_table() -> SemanticTable:
    """Create an orders semantic table."""
    return SemanticTable(
        name="orders",
        data_source="test_db",
        table="orders",
        primary_key="id",
        time_dimension="created_at",
        dimensions={
            "order_id": Dimension(expr="id"),
            "status": Dimension(expr="status"),
            "customer_id": Dimension(expr="customer_id"),
            "created_at": Dimension(expr="created_at"),
        },
        measures={
            "order_total": Measure(expr="amount", agg="sum"),
            "order_count": Measure(expr="id", agg="count"),
        },
    )


@pytest.fixture
def customers_table() -> SemanticTable:
    """Create a customers semantic table."""
    return SemanticTable(
        name="customers",
        data_source="test_db",
        table="customers",
        primary_key="id",
        dimensions={
            "customer_id": Dimension(expr="id"),
            "name": Dimension(expr="name"),
            "country": Dimension(expr="country"),
        },
        measures={
            "customer_count": Measure(expr="id", agg="count_distinct"),
        },
    )


@pytest.fixture
def simple_flow(orders_table: SemanticTable) -> SemanticFlow:
    """Create a simple flow with just orders."""
    return SemanticFlow(
        name="simple_orders",
        base_table=orders_table,
        base_table_alias="o",
    )


@pytest.fixture
def joined_flow(orders_table: SemanticTable, customers_table: SemanticTable) -> SemanticFlow:
    """Create a flow with orders joined to customers."""
    return SemanticFlow(
        name="sales",
        base_table=orders_table,
        base_table_alias="o",
        joins=[
            FlowJoin(
                semantic_table=customers_table,
                alias="c",
                to_table="o",
                join_type="left",
                join_keys=[JoinKey(left="customer_id", right="id")],
            ),
        ],
    )


@pytest.fixture
def simple_flow_handle(seeded_db: Path, orders_table: SemanticTable, simple_flow: SemanticFlow) -> FlowHandle:
    """Create a FlowHandle with a simple single-table flow."""
    return FlowHandle.from_parts(
        tables=[orders_table],
        flows=[simple_flow],
        data_sources=[DataSource.duckdb(str(seeded_db), name="test_db")],
    )


@pytest.fixture
def joined_flow_handle(
    seeded_db: Path,
    orders_table: SemanticTable,
    customers_table: SemanticTable,
    joined_flow: SemanticFlow,
) -> FlowHandle:
    """Create a FlowHandle with orders joined to customers."""
    return FlowHandle.from_parts(
        tables=[orders_table, customers_table],
        flows=[joined_flow],
        data_sources=[DataSource.duckdb(str(seeded_db), name="test_db")],
    )


@pytest.fixture
def flow_yaml_dir(seeded_db: Path, tmp_path: Path) -> Path:
    """Create a directory with YAML flow definitions."""
    flow_dir = tmp_path / "flows"
    tables_dir = flow_dir / "tables"
    flows_dir = flow_dir / "flows"
    tables_dir.mkdir(parents=True)
    flows_dir.mkdir(parents=True)

    # Write orders table YAML
    (tables_dir / "orders.yaml").write_text("""
name: orders
data_source: test_db
table: orders
primary_key: id
time_dimension: created_at

dimensions:
  status:
    expr: status
  customer_id:
    expr: customer_id

measures:
  order_total:
    expr: amount
    agg: sum
  order_count:
    expr: id
    agg: count
""")

    # Write flow YAML
    (flows_dir / "simple.yaml").write_text("""
name: simple
base_table:
  semantic_table: orders
  alias: o
""")

    return flow_dir
