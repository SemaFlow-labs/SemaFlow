"""
Shared pytest fixtures for SemaFlow tests.
"""

from pathlib import Path

import pandas as pd
import pyarrow as pa
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
def seeded_datasource() -> DataSource:
    """Create an in-memory DuckDB datasource with test data."""
    ds = DataSource.duckdb(":memory:", name="test_db")

    # Create customers DataFrame
    customers_df = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Carla", "David"],
        "country": ["US", "UK", "US", "DE"],
    })
    ds.register_dataframe("customers", pa.Table.from_pandas(customers_df).to_reader())

    # Create orders DataFrame
    orders_df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "customer_id": [1, 1, 2, 3, 3],
        "amount": [100.0, 50.0, 25.0, 200.0, 75.0],
        "status": ["complete", "complete", "pending", "complete", "pending"],
        "created_at": pd.to_datetime([
            "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"
        ]),
    })
    ds.register_dataframe("orders", pa.Table.from_pandas(orders_df).to_reader())

    return ds


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
def simple_flow_handle(seeded_datasource: DataSource, orders_table: SemanticTable, simple_flow: SemanticFlow) -> FlowHandle:
    """Create a FlowHandle with a simple single-table flow."""
    return FlowHandle.from_parts(
        tables=[orders_table],
        flows=[simple_flow],
        data_sources=[seeded_datasource],
    )


@pytest.fixture
def joined_flow_handle(
    seeded_datasource: DataSource,
    orders_table: SemanticTable,
    customers_table: SemanticTable,
    joined_flow: SemanticFlow,
) -> FlowHandle:
    """Create a FlowHandle with orders joined to customers."""
    return FlowHandle.from_parts(
        tables=[orders_table, customers_table],
        flows=[joined_flow],
        data_sources=[seeded_datasource],
    )


@pytest.fixture
def flow_yaml_dir(tmp_path: Path) -> Path:
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
