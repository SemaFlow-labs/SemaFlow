from typing import Any, Dict, List, Optional

class DataSource:
    name: str
    uri: str
    max_concurrency: Optional[int]
    def __init__(self, name: str, uri: str, max_concurrency: Optional[int] = ...) -> None: ...
    @staticmethod
    def duckdb(path: str, name: Optional[str] = ..., max_concurrency: Optional[int] = ...) -> "DataSource": ...
    def table(self, name: str) -> "TableHandle": ...

class TableHandle:
    data_source: str
    table: str
    def __init__(self, data_source: str, table: str) -> None: ...

class JoinKey:
    left: str
    right: str
    def __init__(self, left: str, right: str) -> None: ...

class Dimension:
    expression: Any
    data_type: Optional[str]
    description: Optional[str]
    def __init__(self, expression: Any, data_type: Optional[str] = ..., description: Optional[str] = ...) -> None: ...

class Measure:
    expr: Any
    agg: str
    data_type: Optional[str]
    description: Optional[str]
    def __init__(
        self,
        expression: Any,
        agg: str,
        data_type: Optional[str] = ...,
        description: Optional[str] = ...,
    ) -> None: ...

class FlowJoin:
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
        join_type: str = ...,
        description: Optional[str] = ...,
    ) -> None: ...

class SemanticTable:
    name: str
    def __init__(
        self,
        name: str,
        data_source: DataSource | str,
        table: str,
        primary_key: str,
        time_dimension: Optional[str] = ...,
        dimensions: Optional[Dict[str, Dimension | Dict[str, Any]]] = ...,
        measures: Optional[Dict[str, Measure | Dict[str, Any]]] = ...,
        description: Optional[str] = ...,
    ) -> None: ...
    @staticmethod
    def from_table(
        name: str,
        table_handle: TableHandle,
        primary_key: str,
        time_dimension: Optional[str] = ...,
        dimensions: Optional[Dict[str, Dimension | Dict[str, Any]]] = ...,
        measures: Optional[Dict[str, Measure | Dict[str, Any]]] = ...,
        description: Optional[str] = ...,
    ) -> "SemanticTable": ...
    def data_source(self) -> Optional[DataSource]: ...

class SemanticFlow:
    name: str
    base_table_alias: str
    description: Optional[str]
    def __init__(
        self,
        name: str,
        base_table: SemanticTable,
        base_table_alias: str,
        joins: Optional[List[FlowJoin]] = ...,
        description: Optional[str] = ...,
    ) -> None: ...
    def referenced_tables(self) -> List[SemanticTable]: ...

class SemanticFlowHandle:
    @staticmethod
    def from_dir(
        flow_dir: str, data_sources: Dict[str, str] | List[DataSource], description: Optional[str] = ...
    ) -> "SemanticFlowHandle": ...
    @staticmethod
    def from_parts(
        tables: List[SemanticTable],
        flows: List[SemanticFlow],
        data_sources: Dict[str, str] | List[DataSource],
        description: Optional[str] = ...,
    ) -> "SemanticFlowHandle": ...
    def __init__(
        self,
        tables: List[SemanticTable],
        flows: List[SemanticFlow],
        data_sources: Dict[str, str] | List[DataSource],
        description: Optional[str] = ...,
    ) -> None: ...
    def build_sql(self, request: Dict[str, Any]) -> str: ...
    def execute(self, request: Dict[str, Any]) -> List[Dict[str, Any]]: ...
    def list_flows(self) -> List[Dict[str, Any]]: ...
    def get_flow(self, name: str) -> Dict[str, Any]: ...
    description: Optional[str]
