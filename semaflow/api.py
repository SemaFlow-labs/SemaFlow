"""FastAPI integration helpers for SemaFlow.

The helpers below expose three endpoints:
- ``GET /flows``: list registered flows and optional descriptions
- ``GET /flows/{flow}``: return flow schema (dimensions/measures/time dimension)
- ``POST /flows/{flow}/query``: accept a query payload and return rows

Pass either a ready-to-use :class:`~semaflow.FlowHandle` or a ``dict`` mapping
flow names to :class:`~semaflow.SemanticFlow` definitions. The API builds the
handle as needed and uses the path segment ``{flow}`` as the flow name.

Example:
    from pathlib import Path
    from semaflow import DataSource, FlowHandle, SemanticFlow, build_flow_handles
    from semaflow.api import create_app

    # YAML-driven handle
    flow = FlowHandle.from_dir(Path("examples/flows"), {"duckdb_local": "examples/demo_python.duckdb"})
    app = create_app(flow)

    # Class-based definition
    # flows = {"sales": SemanticFlow(...)}
    # app = create_app(build_flow_handles(flows))
"""

from enum import Enum
from typing import Any, Dict, List, Optional

from semaflow import FlowHandle, SemanticFlow, build_flow_handles

try:
    from fastapi import APIRouter, FastAPI, HTTPException  # type: ignore
    from pydantic import BaseModel
except ImportError as e:  # pragma: no cover - handled at runtime
    raise RuntimeError("fastapi is required; install with `pip install semaflow[api]`") from e


class FilterOp(str, Enum):
    """Supported filter operators for the query endpoint."""

    Eq = "=="
    Neq = "!="
    Gt = ">"
    Gte = ">="
    Lt = "<"
    Lte = "<="
    In = "in"
    NotIn = "not in"
    Like = "like"
    ILike = "ilike"


class Filter(BaseModel):
    """Row-level filter applied to a dimension field."""

    field: str
    op: FilterOp
    value: Any


class OrderDirection(str, Enum):
    """Sort direction for query results."""

    Asc = "asc"
    Desc = "desc"


class OrderItem(BaseModel):
    """Column ordering applied to the result set."""

    column: str
    direction: OrderDirection


class QueryPayload(BaseModel):
    """Request body accepted by ``POST /flows/{flow}/query``.

    Fields mirror the ``SemanticFlow.execute`` request:
    - ``dimensions``: optional list of dimension names (qualified alias.field ok)
    - ``measures``: optional list of measure names (qualified alias.field ok)
    - ``filters``: optional list of :class:`Filter` objects
    - ``order``: optional list of :class:`OrderItem` objects
    - ``limit`` / ``offset``: optional pagination integers
    """

    dimensions: Optional[List[str]] = None
    measures: Optional[List[str]] = None
    filters: Optional[List[Filter]] = None
    order: Optional[List[OrderItem]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None

    model_config = {"arbitrary_types_allowed": True}


class FlowList(BaseModel):
    """Response model for listing available flows."""
    flows: Dict[str, Optional[str]]


class FlowSchemaResponse(BaseModel):
    """Response model describing a single semantic flow exposed by a flow."""
    name: str
    description: Optional[str]
    time_dimension: Optional[str]
    dimensions: Dict[str, Dict[str, Optional[str]]]
    measures: Dict[str, Dict[str, Optional[str]]]

    model_config = {"arbitrary_types_allowed": True}


def _prepare_flow_handle(flows: Any) -> FlowHandle:
    if isinstance(flows, FlowHandle):
        return flows
    if isinstance(flows, dict) and flows and all(isinstance(v, SemanticFlow) for v in flows.values()):
        return build_flow_handles(flows)
    raise TypeError("flows must be a FlowHandle or a dict[str, SemanticFlow]")


def create_router(flows: Any):
    """Build an ``APIRouter`` exposing SemaFlow flows keyed by name."""
    handle: FlowHandle = _prepare_flow_handle(flows)
    router = APIRouter()

    def _ensure_flow(flow_name: str):
        names = {m.get("name") for m in handle.list_flows()}
        if flow_name not in names:
            raise HTTPException(status_code=404, detail=f"unknown flow {flow_name}")

    @router.get("/flows", response_model=FlowList)
    async def list_flows():
        summaries: List[Dict[str, Any]] = handle.list_flows()
        flow_map: Dict[str, Optional[str]] = {}
        for summary in summaries:
            name = summary.get("name")
            if not isinstance(name, str):
                raise HTTPException(status_code=500, detail="flow summary missing name")
            description = summary.get("description")
            if description is not None and not isinstance(description, str):
                raise HTTPException(
                    status_code=500, detail=f"flow {name} description must be a string or None"
                )
            flow_map[name] = description
        return FlowList(flows=flow_map)

    @router.get("/flows/{flow}", response_model=FlowSchemaResponse)
    async def describe_flow(flow: str):
        try:
            _ensure_flow(flow)
            schema: Dict[str, Any] = handle.get_flow(flow)
            name = schema.get("name")
            if not isinstance(name, str):
                raise HTTPException(status_code=500, detail="flow schema missing name")
            description = schema.get("description")
            if description is not None and not isinstance(description, str):
                raise HTTPException(
                    status_code=500, detail=f"flow {name} description must be a string or None"
                )
            dims_map: Dict[str, Dict[str, Optional[str]]] = {}
            for dim in schema.get("dimensions", []):
                if not isinstance(dim, dict):
                    raise HTTPException(status_code=500, detail=f"invalid dimension in flow {name}")
                dim_name = dim.get("qualified_name") or dim.get("name")
                if not isinstance(dim_name, str):
                    raise HTTPException(status_code=500, detail=f"dimension name missing in flow {name}")
                dims_map[dim_name] = {
                    "description": dim.get("description") if isinstance(dim.get("description"), str) else None,
                    "data_type": dim.get("data_type") if isinstance(dim.get("data_type"), str) else None,
                }

            measures_map: Dict[str, Dict[str, Optional[str]]] = {}
            for measure in schema.get("measures", []):
                if not isinstance(measure, dict):
                    raise HTTPException(status_code=500, detail=f"invalid measure in flow {name}")
                measure_name = measure.get("qualified_name") or measure.get("name")
                if not isinstance(measure_name, str):
                    raise HTTPException(status_code=500, detail=f"measure name missing in flow {name}")
                measures_map[measure_name] = {
                    "description": measure.get("description") if isinstance(measure.get("description"), str) else None,
                    "data_type": measure.get("data_type") if isinstance(measure.get("data_type"), str) else None,
                }
            return FlowSchemaResponse(
                name=name,
                description=description,
                time_dimension=schema.get("time_dimension"),
                dimensions=dims_map,
                measures=measures_map,
            )
        except Exception as exc:  # pragma: no cover - simple pass-through
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.post("/flows/{flow}/query")
    async def query(flow: str, req: QueryPayload):
        try:
            _ensure_flow(flow)
            payload = req.dict(exclude_none=True)
            payload["flow"] = flow
            return await handle.execute(payload)
        except Exception as exc:  # pragma: no cover - simple pass-through
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return router


def create_app(flows: Any):
    """Create a ready-to-serve ``FastAPI`` app with SemaFlow routes."""
    app = FastAPI()
    app.include_router(create_router(flows))
    return app
