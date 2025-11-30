"""Execution handle utilities for SemaFlow definitions.

Build validated, connection-aware handles from class-based flow definitions.
"""

import asyncio
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from .core import SemanticFlow, SemanticTable
from .semaflow import SemanticFlowHandle as _SemanticFlowHandle

Tables = List[SemanticTable]
Flows = List[SemanticFlow]
DataSources = Dict[str, str] | List[Any]
Request = Dict[str, Any]


class FlowHandle:
    """Validated wrapper over the Rust ``SemanticFlowHandle`` (registry + connections).

    Use `build_flow_handles` to build one handle containing all flows; reuse it
    for async `build_sql` / `execute` calls in servers or notebooks.
    """

    def __init__(
        self,
        tables: Tables,
        flows: Flows,
        data_sources: DataSources,
        description: Optional[str] = None,
    ):
        self._inner = _SemanticFlowHandle(tables, flows, data_sources)
        self.description = description

    def __getitem__(self, key: str) -> Dict[str, Any]:
        """Return the flow schema for ``key`` (dict returned by the Rust handle)."""
        return self._inner.get_flow(key)

    @classmethod
    def from_dir(
        cls, root: Path, data_sources: DataSources, description: Optional[str] = None
    ) -> "FlowHandle":
        inner = _SemanticFlowHandle.from_dir(str(root), data_sources)
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
        description: Optional[str] = None,
    ) -> "FlowHandle":
        inner = _SemanticFlowHandle.from_parts(tables, flows, data_sources)
        obj = cls.__new__(cls)
        obj._inner = inner
        obj.description = description
        return obj

    async def build_sql(self, request: Request) -> str:
        return await asyncio.to_thread(self._inner.build_sql, request)

    async def execute(self, request: Request):
        return await asyncio.to_thread(self._inner.execute, request)

    def list_flows(self) -> List[Dict[str, Any]]:
        return self._inner.list_flows()

    def get_flow(self, name: str) -> Dict[str, Any]:
        return self._inner.get_flow(name)


def build_flow_handles(flows: Mapping[str, SemanticFlow]) -> FlowHandle:
    """Construct and validate a FlowHandle from class-based flow definitions.

    Args:
        flows: Mapping of flow name -> SemanticFlow definitions.

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
            ds = table.data_source()
            if ds is None:
                raise ValueError(
                    "tables must be constructed with a DataSource instance; pass DataSource(...) into SemanticTable"
                )
            data_sources[ds.name] = ds

    return FlowHandle.from_parts(
        list(unique_tables.values()),
        flow_list,
        list(data_sources.values()),
    )
