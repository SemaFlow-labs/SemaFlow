"""
Python wrapper for the SemaFlow core (DuckDB-only for now).

Public helpers:
- build_sql(tables, models, data_sources, request) -> str
- run_query(tables, models, data_sources, request) -> list[dict]
- load_models_from_dir(path) -> (tables, models)
"""

from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Tuple

import yaml

if TYPE_CHECKING:

    def _build_sql(
        tables: "Tables", models: "Models", data_sources: "DataSources", request: "Request"
    ) -> str: ...
    def _run(
        tables: "Tables", models: "Models", data_sources: "DataSources", request: "Request"
    ) -> List[Dict[str, Any]]: ...
else:
    from semaflow_core import build_sql as _build_sql
    from semaflow_core import run as _run

Tables = List[Dict[str, Any]]
Models = List[Dict[str, Any]]
DataSources = Dict[str, str]
Request = Dict[str, Any]


def load_models_from_dir(root: Path) -> Tuple[Tables, Models]:
    """Load semantic tables and models from a directory (tables/ and models/)."""
    tables_dir = root / "tables"
    models_dir = root / "models"
    tables: Tables = []
    models: Models = []
    for path in sorted(tables_dir.glob("*.yml")) + sorted(tables_dir.glob("*.yaml")):
        tables.append(_load_yaml(path))
    for path in sorted(models_dir.glob("*.yml")) + sorted(models_dir.glob("*.yaml")):
        models.append(_load_yaml(path))
    return tables, models


def build_sql(tables: Tables, models: Models, data_sources: DataSources, request: Request) -> str:
    """Compile a QueryRequest into SQL using the Rust core."""
    return _build_sql(tables, models, data_sources, request)


def run_query(
    tables: Tables, models: Models, data_sources: DataSources, request: Request
) -> List[Dict[str, Any]]:
    """Validate, compile, and execute a query against DuckDB. Returns rows as list of dicts."""
    return _run(tables, models, data_sources, request)


def _load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r") as f:
        return yaml.safe_load(f)


__all__ = ["build_sql", "run_query", "load_models_from_dir"]
