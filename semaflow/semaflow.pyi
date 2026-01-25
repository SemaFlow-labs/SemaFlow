"""Type stubs for semaflow Rust extension module.

This module provides Python bindings for the SemaFlow semantic layer engine,
enabling type-safe query building and execution across multiple database backends.
"""

from typing import Any, Dict, List, Optional, TypedDict, Union

class PaginatedResult(TypedDict, total=False):
    """Result from a paginated query execution.

    Returned when `page_size` is set in the request.

    Attributes:
        rows: List of row dicts for this page.
        cursor: Opaque cursor string for next page (None if last page).
        has_more: Whether more rows exist after this page.
        total_rows: Total result count (BigQuery only, None for other backends).
    """

    rows: List[Dict[str, Any]]
    cursor: Optional[str]
    has_more: bool
    total_rows: Optional[int]

class DataSource:
    """Connection configuration for a database backend.

    DataSource encapsulates connection details for supported backends:
    DuckDB, PostgreSQL, and BigQuery. Use the static factory methods
    (duckdb, postgres, bigquery) for type-safe construction.

    Attributes:
        name: Unique identifier for this data source within a flow.
        uri: Backend-specific connection string or path.
        max_concurrency: Optional connection pool size limit.
    """

    name: str
    uri: str
    max_concurrency: Optional[int]

    def __init__(self, name: str, uri: str, max_concurrency: Optional[int] = None) -> None:
        """Create a generic data source (defaults to DuckDB backend).

        For type safety, prefer the static factory methods instead.
        """
        ...

    @staticmethod
    def duckdb(
        path: str,
        name: Optional[str] = None,
        max_concurrency: Optional[int] = None,
    ) -> "DataSource":
        """Create a DuckDB data source.

        Args:
            path: Path to the DuckDB database file (e.g., "data/analytics.duckdb").
                  Use ":memory:" for an in-memory database.
            name: Optional name for this data source. Defaults to "duckdb".
            max_concurrency: Optional max concurrent connections.

        Returns:
            Configured DataSource for DuckDB.

        Example:
            >>> ds = DataSource.duckdb("sales.duckdb", name="sales_db")
        """
        ...

    @staticmethod
    def postgres(
        connection_string: str,
        schema: str,
        name: Optional[str] = None,
        max_concurrency: Optional[int] = None,
    ) -> "DataSource":
        """Create a PostgreSQL data source.

        Args:
            connection_string: PostgreSQL connection URL or key-value string.
                               Example: "postgresql://user:pass@host:5432/db"
            schema: PostgreSQL schema name (e.g., "public").
            name: Optional name for this data source. Defaults to "postgres".
            max_concurrency: Optional connection pool size.

        Returns:
            Configured DataSource for PostgreSQL.

        Example:
            >>> ds = DataSource.postgres(
            ...     "postgresql://localhost/mydb",
            ...     schema="analytics",
            ...     name="pg_analytics"
            ... )
        """
        ...

    @staticmethod
    def bigquery(
        project_id: str,
        dataset: str,
        service_account_path: Optional[str] = None,
        name: Optional[str] = None,
    ) -> "DataSource":
        """Create a BigQuery data source.

        Supports two authentication methods:
        1. Service account key file (explicit credentials)
        2. Application Default Credentials (ADC) when no key file provided

        Args:
            project_id: GCP project ID containing the BigQuery dataset.
            dataset: BigQuery dataset name.
            service_account_path: Optional path to service account JSON key file.
                                  If not provided, uses Application Default Credentials
                                  (GOOGLE_APPLICATION_CREDENTIALS env var or gcloud CLI).
            name: Optional name for this data source. Defaults to "bigquery".

        Returns:
            Configured DataSource for BigQuery.

        Example:
            >>> # Using service account key
            >>> ds = DataSource.bigquery(
            ...     project_id="my-gcp-project",
            ...     dataset="analytics",
            ...     service_account_path="/path/to/key.json"
            ... )
            >>> # Using Application Default Credentials
            >>> ds = DataSource.bigquery(
            ...     project_id="my-gcp-project",
            ...     dataset="analytics"
            ... )
        """
        ...

    def table(self, name: str) -> "TableHandle":
        """Get a handle to a specific table within this data source.

        Args:
            name: Name of the table in the database.

        Returns:
            TableHandle for use in SemanticTable.from_table().
        """
        ...

    def register_dataframe(self, table_name: str, data: Any) -> None:
        """Register an Arrow RecordBatchReader as a table in this data source.

        This method enables in-memory DuckDB databases to be populated with data
        from pandas, polars, or any Arrow-compatible library via zero-copy transfer.

        Only supported for DuckDB data sources.

        Args:
            table_name: Name for the table in the database.
            data: Arrow RecordBatchReader (e.g., `pa.Table.from_pandas(df).to_reader()`
                  for pandas, or `df.to_arrow().to_reader()` for polars).

        Raises:
            ValueError: If this is not a DuckDB data source.
            RuntimeError: If table registration fails.

        Example:
            >>> import pyarrow as pa
            >>> import pandas as pd
            >>> ds = DataSource.duckdb(":memory:", name="test")
            >>> df = pd.DataFrame({"id": [1, 2], "amount": [100.0, 200.0]})
            >>> ds.register_dataframe("orders", pa.Table.from_pandas(df).to_reader())
        """
        ...

class TableHandle:
    """Reference to a specific table within a DataSource.

    Created via DataSource.table() method. Used to construct SemanticTables
    with explicit data source binding.

    Attributes:
        data_source: Name of the parent DataSource.
        table: Name of the database table.
    """

    data_source: str
    table: str

    def __init__(self, data_source: str, table: str) -> None: ...

class JoinKey:
    """Column mapping for join conditions between tables.

    Defines which columns to match when joining semantic tables.

    Attributes:
        left: Column name from the left (joining) table.
        right: Column name from the right (target) table.
    """

    left: str
    right: str

    def __init__(self, left: str, right: str) -> None:
        """Create a join key mapping.

        Args:
            left: Column from the table being joined.
            right: Column from the table being joined to.
        """
        ...

class Dimension:
    """A queryable attribute in the semantic layer.

    Dimensions represent categorical or descriptive attributes that can be
    used for grouping, filtering, and display in queries.

    Attributes:
        expr: Column reference or expression (string or Expr dict).
        data_type: Optional SQL data type hint (e.g., "VARCHAR", "DATE").
        description: Optional human-readable description.
    """

    expr: Any
    data_type: Optional[str]
    description: Optional[str]

    def __init__(
        self,
        expr: Any,
        data_type: Optional[str] = None,
        description: Optional[str] = None,
    ) -> None:
        """Create a dimension.

        Args:
            expr: Column name (string) or expression dict.
                        String values are treated as column references.
            data_type: Optional data type for documentation/validation.
            description: Optional description for discoverability.

        Example:
            >>> dim = Dimension("country", description="Customer country")
            >>> # Or with expr:
            >>> dim = Dimension({"function": {"name": "UPPER", "args": ["city"]}})
        """
        ...

class Measure:
    """An aggregatable metric in the semantic layer.

    Measures come in two types:
    - **Simple**: Uses `expr` + `agg` for single aggregations (e.g., `sum(amount)`)
    - **Formula**: Uses `formula` for chained expressions with inline aggregations

    Simple and formula are mutually exclusive.

    Formula measures use inline aggregations on columns, NOT references to other measures:
    - CORRECT: `"safe_divide(sum(total_amount), count(order_id))"`
    - WRONG: `"safe_divide(order_total, order_count)"`  # Can't reference measures

    Attributes:
        expr: Column reference or expression to aggregate (simple measures).
        agg: Aggregation function name (simple measures).
        formula: Formula string with inline aggregations (e.g., "round(sum(a) / count(b), 2)").
        data_type: Optional result data type hint.
        description: Optional human-readable description.
    """

    expr: Optional[Any]
    agg: Optional[str]
    formula: Optional[str]
    data_type: Optional[str]
    description: Optional[str]

    def __init__(
        self,
        expr: Any,
        agg: str,
        data_type: Optional[str] = None,
        description: Optional[str] = None,
        filter: Optional[Any] = None,
        post_expr: Optional[Any] = None,
    ) -> None:
        """Create a simple measure.

        For simple measures with a single aggregation. For complex measures with
        chained expressions, use formula= in YAML definitions.

        Args:
            expr: Column name (string) or expression dict to aggregate.
            agg: Aggregation type. One of: "sum", "count", "count_distinct",
                 "min", "max", "avg".
            data_type: Optional result data type for documentation.
            description: Optional description for discoverability.
            filter: Optional filter expression applied before aggregation.
            post_expr: DEPRECATED - use formula in YAML instead.

        Example:
            >>> revenue = Measure("amount", agg="sum", description="Total revenue")
            >>> # With filter:
            >>> completed_orders = Measure(
            ...     "order_id",
            ...     agg="count",
            ...     filter={"eq": [{"column": "status"}, "completed"]}
            ... )
        """
        ...

class FlowJoin:
    """Join definition between semantic tables within a flow.

    Defines how to join an additional semantic table to the flow,
    enabling multi-table queries.

    Attributes:
        semantic_table: The SemanticTable being joined.
        alias: Short alias for referencing this table in queries.
        to_table: Alias of the table to join to.
        join_type: Type of SQL join.
        join_keys: Column mappings for the join condition.
        description: Optional description of the relationship.
    """

    semantic_table: "SemanticTable"
    alias: str
    to_table: str
    join_type: str
    join_keys: List[JoinKey]
    description: Optional[str]

    def __init__(
        self,
        semantic_table: "SemanticTable",
        alias: str,
        to_table: str,
        join_keys: List[JoinKey],
        join_type: str = "left",
        description: Optional[str] = None,
    ) -> None:
        """Create a flow join.

        Args:
            semantic_table: The SemanticTable to join into the flow.
            alias: Short alias (e.g., "c" for customers) used to qualify
                   columns in queries like "c.country".
            to_table: Alias of the existing table to join to.
            join_keys: List of JoinKey mappings for the ON clause.
            join_type: Join type: "inner", "left", "right", or "full".
                       Defaults to "left".
            description: Optional description of this relationship.

        Example:
            >>> join = FlowJoin(
            ...     customers_table,
            ...     alias="c",
            ...     to_table="o",  # orders table alias
            ...     join_keys=[JoinKey("customer_id", "customer_id")],
            ...     join_type="left",
            ...     description="Customer details for each order"
            ... )
        """
        ...

class SemanticTable:
    """A semantic layer definition over a physical database table.

    SemanticTable wraps a database table with business-friendly dimensions
    and measures, enabling self-service analytics queries.

    Attributes:
        name: Unique identifier for this semantic table.
    """

    name: str

    def __init__(
        self,
        name: str,
        data_source: Union["DataSource", str],
        table: str,
        primary_key: Optional[str] = None,
        primary_keys: Optional[List[str]] = None,
        time_dimension: Optional[str] = None,
        dimensions: Optional[Dict[str, Union[Dimension, Dict[str, Any]]]] = None,
        measures: Optional[Dict[str, Union[Measure, Dict[str, Any]]]] = None,
        description: Optional[str] = None,
    ) -> None:
        """Create a semantic table.

        Args:
            name: Unique name for this semantic table.
            data_source: DataSource instance or name string.
            table: Physical table name in the database.
            primary_key: Single primary key column (use primary_keys for composite).
            primary_keys: List of primary key columns for composite keys.
            time_dimension: Optional time-based dimension for time-series analysis.
            dimensions: Dict mapping dimension names to Dimension objects or dicts.
            measures: Dict mapping measure names to Measure objects or dicts.
            description: Optional description for documentation.

        Note:
            Either primary_key or primary_keys must be specified.

        Example:
            >>> orders = SemanticTable(
            ...     name="orders",
            ...     data_source=ds,
            ...     table="fact_orders",
            ...     primary_key="order_id",
            ...     time_dimension="order_date",
            ...     dimensions={
            ...         "status": Dimension("order_status"),
            ...         "channel": Dimension("sales_channel"),
            ...     },
            ...     measures={
            ...         "revenue": Measure("amount", agg="sum"),
            ...         "order_count": Measure("order_id", agg="count"),
            ...     }
            ... )
        """
        ...

    @staticmethod
    def from_table(
        name: str,
        table_handle: TableHandle,
        primary_key: Optional[str] = None,
        primary_keys: Optional[List[str]] = None,
        time_dimension: Optional[str] = None,
        dimensions: Optional[Dict[str, Union[Dimension, Dict[str, Any]]]] = None,
        measures: Optional[Dict[str, Union[Measure, Dict[str, Any]]]] = None,
        description: Optional[str] = None,
    ) -> "SemanticTable":
        """Create a semantic table from a TableHandle.

        Alternative constructor using a TableHandle from DataSource.table().

        Args:
            name: Unique name for this semantic table.
            table_handle: TableHandle from DataSource.table().
            primary_key: Single primary key column.
            primary_keys: List of primary key columns for composite keys.
            time_dimension: Optional time-based dimension.
            dimensions: Dict of dimension definitions.
            measures: Dict of measure definitions.
            description: Optional description.

        Returns:
            Configured SemanticTable.
        """
        ...

    @property
    def data_source(self) -> Optional["DataSource"]:
        """Get the DataSource if this table was created with a DataSource instance."""
        ...

class SemanticFlow:
    """A queryable semantic model combining multiple tables.

    SemanticFlow defines a star or snowflake schema starting from a base
    table with optional joins to related tables. Queries against a flow
    can reference dimensions and measures from any joined table.

    Attributes:
        name: Unique identifier for this flow.
        base_table_alias: Alias of the base (fact) table.
        description: Optional description.
    """

    name: str
    base_table_alias: str
    description: Optional[str]

    def __init__(
        self,
        name: str,
        base_table: SemanticTable,
        base_table_alias: str,
        joins: Optional[List[FlowJoin]] = None,
        description: Optional[str] = None,
    ) -> None:
        """Create a semantic flow.

        Args:
            name: Unique name for this flow.
            base_table: The primary/fact SemanticTable.
            base_table_alias: Short alias for the base table (e.g., "o" for orders).
            joins: Optional list of FlowJoin definitions for related tables.
            description: Optional description of what this flow represents.

        Example:
            >>> flow = SemanticFlow(
            ...     name="sales_analytics",
            ...     base_table=orders_table,
            ...     base_table_alias="o",
            ...     joins=[customer_join, product_join],
            ...     description="Orders with customer and product dimensions"
            ... )
        """
        ...

    def referenced_tables(self) -> List[SemanticTable]:
        """Get all SemanticTables referenced by this flow.

        Returns:
            List of SemanticTable objects (base table + all joined tables).
        """
        ...

class Config:
    """Configuration for SemaFlow query execution and connection behavior.

    Config allows controlling query timeouts, connection pool sizes, schema cache
    settings, and per-datasource options like BigQuery billing limits.

    Can be loaded from TOML files or configured programmatically.

    Example TOML format:
        [defaults.query]
        timeout_ms = 30000
        max_row_limit = 100000

        [defaults.pool]
        size = 16

        [defaults.schema_cache]
        ttl_secs = 3600
        max_size = 1000

        [datasources.my_bigquery.bigquery]
        use_query_cache = true
        maximum_bytes_billed = 10737418240
    """

    def __init__(self) -> None:
        """Create a new config with default values."""
        ...

    @staticmethod
    def load() -> "Config":
        """Load config from default locations.

        Searches in order:
        1. SEMAFLOW_CONFIG environment variable
        2. ./semaflow.toml (current directory)
        3. ~/.config/semaflow/config.toml (user config)

        Returns default config if no file found.

        Returns:
            Loaded or default Config.
        """
        ...

    @staticmethod
    def from_file(path: str) -> "Config":
        """Load config from a specific TOML file.

        Args:
            path: Path to the TOML configuration file.

        Returns:
            Loaded Config.

        Raises:
            RuntimeError: If file cannot be read or parsed.
        """
        ...

    @staticmethod
    def from_toml(toml_str: str) -> "Config":
        """Parse config from a TOML string.

        Args:
            toml_str: TOML-formatted configuration string.

        Returns:
            Parsed Config.

        Raises:
            RuntimeError: If TOML cannot be parsed.
        """
        ...

    def set_query_timeout_ms(self, timeout_ms: int) -> None:
        """Set the default query timeout in milliseconds."""
        ...

    def set_max_row_limit(self, limit: int) -> None:
        """Set the maximum row limit for queries (0 = unlimited)."""
        ...

    def set_default_row_limit(self, limit: int) -> None:
        """Set the default row limit for queries."""
        ...

    def set_pool_size(self, size: int) -> None:
        """Set the default connection pool size."""
        ...

    def set_pool_idle_timeout_secs(self, secs: int) -> None:
        """Set the pool idle timeout in seconds."""
        ...

    def set_schema_cache_ttl_secs(self, secs: int) -> None:
        """Set the schema cache TTL in seconds."""
        ...

    def set_schema_cache_max_size(self, size: int) -> None:
        """Set the maximum schema cache size."""
        ...

    def set_validation_warn_only(self, warn_only: bool) -> None:
        """Set validation to warn-only mode."""
        ...

    def set_bigquery_config(
        self,
        datasource_name: str,
        use_query_cache: Optional[bool] = None,
        maximum_bytes_billed: Optional[int] = None,
        query_timeout_ms: Optional[int] = None,
    ) -> None:
        """Configure BigQuery settings for a specific datasource.

        Args:
            datasource_name: Name of the datasource.
            use_query_cache: Whether to use BigQuery's query cache (default: true).
            maximum_bytes_billed: Maximum bytes billed per query (0 = unlimited).
            query_timeout_ms: Query timeout in milliseconds (0 = use default).
        """
        ...

    def set_duckdb_config(self, datasource_name: str, max_concurrency: Optional[int] = None) -> None:
        """Configure DuckDB settings for a specific datasource.

        Args:
            datasource_name: Name of the datasource.
            max_concurrency: Maximum concurrent queries.
        """
        ...

    def set_postgres_config(
        self,
        datasource_name: str,
        pool_size: Optional[int] = None,
        statement_timeout_ms: Optional[int] = None,
    ) -> None:
        """Configure PostgreSQL settings for a specific datasource.

        Args:
            datasource_name: Name of the datasource.
            pool_size: Connection pool size.
            statement_timeout_ms: Statement timeout in milliseconds.
        """
        ...

class SemanticFlowHandle:
    """Validated, connection-aware handle for executing semantic queries.

    SemanticFlowHandle is the main entry point for query execution. It holds
    validated semantic definitions and active database connections, enabling
    efficient repeated queries.

    Attributes:
        description: Optional description of this handle's purpose.
    """

    description: Optional[str]

    @staticmethod
    def from_dir(
        flow_dir: str,
        data_sources: Union[Dict[str, str], List[DataSource]],
        config: Optional[Config] = None,
    ) -> "SemanticFlowHandle":
        """Load semantic definitions from YAML files in a directory.

        Args:
            flow_dir: Path to directory containing .yaml flow definitions.
            data_sources: Either a list of DataSource objects or a dict
                          mapping data source names to DuckDB file paths.
            config: Optional Config for connection and query settings.

        Returns:
            Validated SemanticFlowHandle ready for queries.
        """
        ...

    @staticmethod
    def from_parts(
        tables: List[SemanticTable],
        flows: List[SemanticFlow],
        data_sources: Union[Dict[str, str], List[DataSource]],
        config: Optional[Config] = None,
    ) -> "SemanticFlowHandle":
        """Create a handle from programmatically defined components.

        Args:
            tables: List of SemanticTable definitions.
            flows: List of SemanticFlow definitions.
            data_sources: Either a list of DataSource objects or a dict
                          mapping data source names to DuckDB file paths.
            config: Optional Config for connection and query settings.

        Returns:
            Validated SemanticFlowHandle ready for queries.
        """
        ...

    def __init__(
        self,
        tables: List[SemanticTable],
        flows: List[SemanticFlow],
        data_sources: Union[Dict[str, str], List[DataSource]],
        config: Optional[Config] = None,
    ) -> None:
        """Create and validate a SemanticFlowHandle.

        Args:
            tables: List of SemanticTable definitions.
            flows: List of SemanticFlow definitions.
            data_sources: Either a list of DataSource objects or a dict
                          mapping data source names to DuckDB file paths.
            config: Optional Config for connection and query settings.

        Raises:
            ValueError: If validation fails (missing tables, invalid references, etc.)
        """
        ...

    def build_sql(self, request: Dict[str, Any]) -> str:
        """Generate SQL for a query request without executing.

        Useful for debugging, logging, or executing manually.

        Args:
            request: Query request dict with keys:
                - flow: Name of the flow to query.
                - dimensions: List of dimension references (e.g., ["o.status", "c.country"]).
                - measures: List of measure references (e.g., ["o.revenue"]).
                - filters: Optional list of filter conditions.
                - order_by: Optional list of ordering specifications.
                - limit: Optional row limit.

        Returns:
            Generated SQL string.

        Example:
            >>> sql = handle.build_sql({
            ...     "flow": "sales_analytics",
            ...     "dimensions": ["o.status"],
            ...     "measures": ["o.revenue"],
            ...     "limit": 100
            ... })
        """
        ...

    def execute(self, request: Dict[str, Any]) -> Union[List[Dict[str, Any]], PaginatedResult]:
        """Execute a query and return results.

        Args:
            request: Query request dict with keys:
                - flow: Name of the flow to query.
                - dimensions: List of dimension references.
                - measures: List of measure references.
                - filters: Optional list of filter conditions.
                - order_by: Optional list of ordering specifications.
                - limit: Optional total row limit (caps results).
                - page_size: Optional page size (enables pagination).
                - cursor: Optional cursor for subsequent pages.

        Returns:
            If page_size is NOT set: List of result rows as dictionaries.
            If page_size IS set: PaginatedResult dict with rows, cursor, has_more, total_rows.

        Example:
            >>> # Non-paginated (backwards compatible)
            >>> results = handle.execute({
            ...     "flow": "sales_analytics",
            ...     "dimensions": ["c.country"],
            ...     "measures": ["o.revenue"],
            ...     "limit": 10
            ... })
            >>> for row in results:
            ...     print(f"{row['c.country']}: ${row['o.revenue']}")

            >>> # Paginated
            >>> page = handle.execute({
            ...     "flow": "sales_analytics",
            ...     "dimensions": ["c.country"],
            ...     "measures": ["o.revenue"],
            ...     "page_size": 15
            ... })
            >>> print(f"Got {len(page['rows'])} rows, has_more={page['has_more']}")
            >>> # Fetch next page
            >>> if page['cursor']:
            ...     next_page = handle.execute({
            ...         "flow": "sales_analytics",
            ...         "dimensions": ["c.country"],
            ...         "measures": ["o.revenue"],
            ...         "page_size": 15,
            ...         "cursor": page['cursor']
            ...     })
        """
        ...

    def list_flows(self) -> List[Dict[str, Any]]:
        """List all available flows with their names and descriptions.

        Returns:
            List of dicts with "name" and optionally "description" keys.

        Example:
            >>> for flow in handle.list_flows():
            ...     print(f"{flow['name']}: {flow.get('description', 'No description')}")
        """
        ...

    def get_flow(self, name: str) -> Dict[str, Any]:
        """Get detailed schema information for a specific flow.

        Args:
            name: Name of the flow to retrieve.

        Returns:
            Dict containing:
                - name: Flow name
                - description: Optional description
                - data_source: Name of the data source
                - time_dimension: Optional time dimension name
                - smallest_time_grain: Optional smallest time granularity
                - dimensions: List of dimension metadata dicts
                - measures: List of measure metadata dicts

        Raises:
            ValueError: If the flow name is not found.

        Example:
            >>> schema = handle.get_flow("sales_analytics")
            >>> print(f"Dimensions: {[d['name'] for d in schema['dimensions']]}")
            >>> print(f"Measures: {[m['name'] for m in schema['measures']]}")
        """
        ...
