"""Public Python wrapper for the SemaFlow Rust core.

Two layers:
- Definition types: `SemanticTable`, `SemanticFlow`, `FlowJoin`, etc. (PyO3-backed Rust structs)
- Execution: `FlowHandle` (validated registry + connections) and the `build_flow_handles` helper.
"""

from .core import DataSource, Dimension, FlowJoin, JoinKey, Measure, SemanticFlow, SemanticTable, TableHandle
from .handle import FlowHandle, build_flow_handles

__all__ = [
    "FlowHandle",
    "build_flow_handles",
    "DataSource",
    "Dimension",
    "TableHandle",
    "Measure",
    "SemanticTable",
    "SemanticFlow",
    "FlowJoin",
    "JoinKey",
]

# Docstrings for Python help/autocomplete.
DataSource.__doc__ = """Connection configuration passed to tables/flows.

Args:
    name: Logical name referenced by semantic tables.
    uri: Backend-specific connection string (e.g., DuckDB file path).
    max_concurrency: Optional limit to throttle queries per backend.

Methods:
    duckdb(path, name=None, max_concurrency=None): Create a DuckDB data source.
    postgres(connection_string, schema, name=None, max_concurrency=None): Create a PostgreSQL data source.
    bigquery(project_id, dataset, service_account_path=None, name=None): Create a BigQuery data source.
    register_dataframe(table_name, data): Register an Arrow RecordBatchReader as a table (DuckDB only).
    table(name): Create a TableHandle reference to a table in this data source.

Example (in-memory with DataFrame):
    >>> import pyarrow as pa
    >>> ds = DataSource.duckdb(":memory:", name="test")
    >>> df = pd.DataFrame({"id": [1, 2], "amount": [100.0, 200.0]})
    >>> ds.register_dataframe("orders", pa.Table.from_pandas(df).to_reader())
"""

TableHandle.__doc__ = """Reference to a physical table within a data source.

Typically created via DataSource.table and passed into SemanticTable.from_table."""

JoinKey.__doc__ = """Join key mapping inside a FlowJoin.

Args:
    left: Column name on the joined table.
    right: Column name on the parent table alias."""

Dimension.__doc__ = """Dimension expression/metadata on a SemanticTable.

Args:
    expression: Column name or expression.
    data_type: Optional logical type.
    description: Optional human-friendly description."""

Measure.__doc__ = """Aggregated measure on a SemanticTable.

Args:
    expression: Column or expression to aggregate.
    agg: Aggregation name (sum, count, count_distinct, min, max, avg).
    data_type: Optional logical type.
    description: Optional human-friendly description."""

FlowJoin.__doc__ = """Declarative join between semantic tables inside a flow.

Args:
    semantic_table: Joined SemanticTable.
    alias: Alias to use for the joined table.
    to_table: Alias of the table to join against.
    join_type: One of inner, left, right, full.
    join_keys: List of JoinKey pairs.
    description: Optional human-readable description."""

SemanticTable.__doc__ = """Logical table describing dimensions/measures.

Args mirror the YAML schema:
    name: Logical table name.
    data_source: DataSource instance or name this table lives in.
    table: Physical table name (or view).
    primary_key: Column used as the primary key.
    time_dimension: Optional timestamp column for time-aware measures.
    dimensions: Mapping of name -> Dimension.
    measures: Mapping of name -> Measure.
    description: Optional human-readable description."""

SemanticFlow.__doc__ = """Flow definition stitching semantic tables together.

Args:
    name: Flow name referenced by requests.
    base_table: Base SemanticTable.
    base_table_alias: Alias for the base table.
    joins: Optional list of FlowJoin objects.
    description: Optional human-readable description."""

FlowHandle.__doc__ = """Validated, long-lived handle wrapping flows/tables/connections.

Use build_flow_handles to build one from class-based definitions. Exposes async build_sql/execute for query paths."""

build_flow_handles.__doc__ = """Build a single FlowHandle from class-based flow definitions.

Args:
    flows: Mapping of flow name -> SemanticFlow.

Returns:
    FlowHandle containing all provided flows, validated once with shared tables/connections."""
