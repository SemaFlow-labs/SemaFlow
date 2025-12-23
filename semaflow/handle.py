"""Execution handle utilities for SemaFlow definitions.

Build validated, connection-aware handles from class-based flow definitions.
"""

import asyncio
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, TypedDict, Union

from .core import SemanticFlow, SemanticTable
from .semaflow import Config
from .semaflow import SemanticFlowHandle as _SemanticFlowHandle

Tables = List[SemanticTable]
Flows = List[SemanticFlow]
DataSources = Dict[str, str] | List[Any]
Request = Dict[str, Any]


class PaginatedResult(TypedDict, total=False):
    """Result from a paginated query execution.

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


# Return type for execute: list of rows (non-paginated) or PaginatedResult (paginated)
ExecuteResult = Union[List[Dict[str, Any]], PaginatedResult]


class FlowHandle:
    """Validated wrapper over the Rust ``SemanticFlowHandle`` (registry + connections).

    Use `build_flow_handles` to build one handle containing all flows; reuse it
    for async `build_sql` / `execute` calls in servers or notebooks.

    Args:
        tables: List of SemanticTable definitions.
        flows: List of SemanticFlow definitions.
        data_sources: Either dict[name -> duckdb_path] or list[DataSource].
        config: Optional Config for connection and query settings.
        description: Optional description for this handle.
    """

    def __init__(
        self,
        tables: Tables,
        flows: Flows,
        data_sources: DataSources,
        config: Optional[Config] = None,
        description: Optional[str] = None,
    ):
        self._inner = _SemanticFlowHandle(tables, flows, data_sources, config)
        self.description = description

    def __getitem__(self, key: str) -> Dict[str, Any]:
        """Return the flow schema for ``key`` (dict returned by the Rust handle)."""
        return self._inner.get_flow(key)

    @classmethod
    def from_dir(
        cls,
        root: Path | str,
        data_sources: DataSources,
        config: Optional[Config] = None,
        description: Optional[str] = None,
    ) -> "FlowHandle":
        inner = _SemanticFlowHandle.from_dir(str(root), data_sources, config)
        obj = cls.__new__(cls)
        obj._inner = inner
        obj.description = description
        return obj

    @classmethod
    def from_parts(
        cls,
        tables: Tables,
        flows: Flows,
        data_sources: List[Any] | DataSources,
        config: Optional[Config] = None,
        description: Optional[str] = None,
    ) -> "FlowHandle":
        inner = _SemanticFlowHandle.from_parts(tables, flows, data_sources, config)
        obj = cls.__new__(cls)
        obj._inner = inner
        obj.description = description
        return obj

    async def build_sql(self, request: Request) -> str:
        return await asyncio.to_thread(self._inner.build_sql, request)

    async def execute(self, request: Request) -> ExecuteResult:
        """Execute a query request.

        Args:
            request: Query request dict with flow, dimensions, measures, etc.
                     Include 'page_size' to enable pagination.
                     Include 'cursor' for subsequent pages.

        Returns:
            If page_size is NOT set: list of row dicts (backwards compatible).
            If page_size IS set: PaginatedResult dict with rows, cursor, has_more, total_rows.
        """
        result = await asyncio.to_thread(self._inner.execute, request)
        # Transform result keys from SQL-safe format (c__country) back to qualified format (c.country)
        if isinstance(result, dict):
            # Paginated result - transform rows within the dict
            return {
                "rows": [_unsanitize_keys(row) for row in result["rows"]],
                "cursor": result.get("cursor"),
                "has_more": result.get("has_more", False),
                "total_rows": result.get("total_rows"),
            }
        else:
            # Non-paginated result - list of rows
            return [_unsanitize_keys(row) for row in result]

    def list_flows(self):
        """Return a list of all flow names in this handle."""
        return self._inner.list_flows()

    def get_flow(self, name: str) -> Dict[str, Any]:
        """Return the flow schema for the given name."""
        return self._inner.get_flow(name)


def _unsanitize_keys(row: Dict[str, Any]) -> Dict[str, Any]:
    """Transform column names from SQL-safe format back to qualified format.

    Converts 'c__country' back to 'c.country' so users receive the same
    keys they used in the request.
    """
    return {k.replace("__", "."): v for k, v in row.items()}


def build_flow_handles(
    flows: Mapping[str, SemanticFlow], config: Optional[Config] = None
) -> FlowHandle:
    """Construct and validate a FlowHandle from class-based flow definitions.

    Args:
        flows: Mapping of flow name -> SemanticFlow definitions.
        config: Optional Config for connection and query settings.

    Returns:
        FlowHandle containing all flows with shared tables/connections, validated once.
    """
    if not isinstance(flows, Mapping) or not flows:
        raise TypeError("flows must be a non-empty mapping of name -> SemanticFlow")
    unique_tables: Dict[str, SemanticTable] = {}
    flow_list: List[SemanticFlow] = []
    data_sources: Dict[str, Any] = {}

    for name, flow in flows.items():
        if not isinstance(flow, SemanticFlow):
            raise TypeError("flows values must be SemanticFlow objects")
        flow_list.append(flow)
        for table in flow.referenced_tables():
            unique_tables.setdefault(table.name, table)
            ds_attr = getattr(table, "data_source", None)
            ds = ds_attr() if callable(ds_attr) else ds_attr
            if ds is None:
                raise ValueError(
                    "tables must be constructed with a DataSource instance; pass DataSource(...) into SemanticTable"
                )
            data_sources[ds.name] = ds

    return FlowHandle.from_parts(
        list(unique_tables.values()),
        flow_list,
        list(data_sources.values()),
        config=config,
    )
