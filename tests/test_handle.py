"""
Tests for FlowHandle functionality.
"""

from pathlib import Path

import pytest

from semaflow import DataSource, FlowHandle


class TestFlowHandleFromParts:
    """Tests for FlowHandle.from_parts() initialization."""

    def test_creates_handle_with_single_table(self, simple_flow_handle: FlowHandle):
        """FlowHandle can be created with a single table and flow."""
        assert simple_flow_handle is not None

    def test_creates_handle_with_joins(self, joined_flow_handle: FlowHandle):
        """FlowHandle can be created with joined tables."""
        assert joined_flow_handle is not None

    def test_list_flows_returns_flow_names(self, simple_flow_handle: FlowHandle):
        """list_flows() returns configured flow names."""
        flows = simple_flow_handle.list_flows()
        flow_names = [f["name"] for f in flows]
        assert "simple_orders" in flow_names


class TestFlowHandleFromDir:
    """Tests for FlowHandle.from_dir() initialization."""

    def test_loads_from_yaml_directory(self, flow_yaml_dir: Path, seeded_datasource: DataSource):
        """FlowHandle can be loaded from a directory of YAML files."""
        handle = FlowHandle.from_dir(
            str(flow_yaml_dir),
            [seeded_datasource],
        )
        assert handle is not None
        flows = handle.list_flows()
        flow_names = [f["name"] for f in flows]
        assert "simple" in flow_names


class TestFlowHandleGetFlow:
    """Tests for FlowHandle.get_flow() schema retrieval."""

    def test_returns_flow_schema(self, simple_flow_handle: FlowHandle):
        """get_flow() returns flow schema with dimensions and measures."""
        schema = simple_flow_handle.get_flow("simple_orders")
        assert "dimensions" in schema
        assert "measures" in schema
        # dimensions/measures are lists of dicts with 'qualified_name' field
        dim_names = [d["qualified_name"] for d in schema["dimensions"]]
        measure_names = [m["qualified_name"] for m in schema["measures"]]
        assert "o.status" in dim_names
        assert "o.order_total" in measure_names

    def test_raises_for_unknown_flow(self, simple_flow_handle: FlowHandle):
        """get_flow() raises error for unknown flow name."""
        with pytest.raises(Exception):
            simple_flow_handle.get_flow("nonexistent_flow")


class TestFlowHandleBuildSql:
    """Tests for FlowHandle.build_sql() SQL generation."""

    @pytest.mark.asyncio
    async def test_builds_valid_sql(self, simple_flow_handle: FlowHandle):
        """build_sql() returns valid SQL string."""
        sql = await simple_flow_handle.build_sql({
            "flow": "simple_orders",
            "dimensions": ["o.status"],
            "measures": ["o.order_total"],
        })
        assert "SELECT" in sql
        assert "FROM" in sql
        assert "GROUP BY" in sql

    @pytest.mark.asyncio
    async def test_sql_includes_requested_columns(self, simple_flow_handle: FlowHandle):
        """build_sql() includes requested dimensions and measures."""
        sql = await simple_flow_handle.build_sql({
            "flow": "simple_orders",
            "dimensions": ["o.status"],
            "measures": ["o.order_total", "o.order_count"],
        })
        # Check SQL references the columns (accounting for aliasing)
        assert "status" in sql.lower()
        assert "amount" in sql.lower() or "order_total" in sql.lower()


class TestFlowHandleExecute:
    """Tests for FlowHandle.execute() query execution."""

    @pytest.mark.asyncio
    async def test_returns_list_of_dicts(self, simple_flow_handle: FlowHandle):
        """execute() returns list of row dicts."""
        result = await simple_flow_handle.execute({
            "flow": "simple_orders",
            "dimensions": ["o.status"],
            "measures": ["o.order_total"],
        })
        assert isinstance(result, list)
        assert len(result) > 0
        assert isinstance(result[0], dict)

    @pytest.mark.asyncio
    async def test_result_contains_requested_fields(self, simple_flow_handle: FlowHandle):
        """execute() returns rows with requested dimension and measure keys."""
        result = await simple_flow_handle.execute({
            "flow": "simple_orders",
            "dimensions": ["o.status"],
            "measures": ["o.order_total"],
        })
        row = result[0]
        assert "o.status" in row
        assert "o.order_total" in row

    @pytest.mark.asyncio
    async def test_aggregates_correctly(self, simple_flow_handle: FlowHandle):
        """execute() returns correct aggregated values."""
        result = await simple_flow_handle.execute({
            "flow": "simple_orders",
            "dimensions": ["o.status"],
            "measures": ["o.order_total", "o.order_count"],
        })
        # Find the 'complete' status row
        complete_row = next((r for r in result if r["o.status"] == "complete"), None)
        assert complete_row is not None
        # Orders: (1, 100), (2, 50), (4, 200) are complete = 350 total, 3 count
        assert complete_row["o.order_total"] == 350.0
        assert complete_row["o.order_count"] == 3

    @pytest.mark.asyncio
    async def test_with_filters(self, simple_flow_handle: FlowHandle):
        """execute() respects filter conditions."""
        result = await simple_flow_handle.execute({
            "flow": "simple_orders",
            "dimensions": ["o.status"],
            "measures": ["o.order_total"],
            "filters": [{"field": "o.status", "op": "==", "value": "complete"}],
        })
        assert len(result) == 1
        assert result[0]["o.status"] == "complete"

    @pytest.mark.asyncio
    async def test_with_order(self, simple_flow_handle: FlowHandle):
        """execute() respects ordering."""
        result = await simple_flow_handle.execute({
            "flow": "simple_orders",
            "dimensions": ["o.status"],
            "measures": ["o.order_total"],
            "order": [{"column": "o.order_total", "direction": "desc"}],
        })
        totals = [r["o.order_total"] for r in result]
        assert totals == sorted(totals, reverse=True)

    @pytest.mark.asyncio
    async def test_with_limit(self, simple_flow_handle: FlowHandle):
        """execute() respects limit."""
        result = await simple_flow_handle.execute({
            "flow": "simple_orders",
            "dimensions": ["o.status"],
            "measures": ["o.order_total"],
            "limit": 1,
        })
        assert len(result) == 1


class TestFlowHandlePagination:
    """Tests for paginated query execution."""

    @pytest.mark.asyncio
    async def test_returns_paginated_result(self, simple_flow_handle: FlowHandle):
        """execute() with page_size returns pagination metadata."""
        result = await simple_flow_handle.execute({
            "flow": "simple_orders",
            "dimensions": ["o.status"],
            "measures": ["o.order_total"],
            "page_size": 1,
        })
        assert isinstance(result, dict)
        assert "rows" in result
        assert "cursor" in result
        assert "has_more" in result

    @pytest.mark.asyncio
    async def test_pagination_cursor_fetches_next_page(self, simple_flow_handle: FlowHandle):
        """Cursor from first page can fetch subsequent pages."""
        # Get first page
        page1 = await simple_flow_handle.execute({
            "flow": "simple_orders",
            "dimensions": ["o.status"],
            "measures": ["o.order_total"],
            "page_size": 1,
        })
        assert page1["has_more"] is True
        cursor = page1["cursor"]

        # Get second page
        page2 = await simple_flow_handle.execute({
            "flow": "simple_orders",
            "dimensions": ["o.status"],
            "measures": ["o.order_total"],
            "page_size": 1,
            "cursor": cursor,
        })
        assert "rows" in page2
        # Second page should have different data
        assert page1["rows"][0] != page2["rows"][0]


class TestFlowHandleJoins:
    """Tests for queries with joins."""

    @pytest.mark.asyncio
    async def test_join_query_returns_data(self, joined_flow_handle: FlowHandle):
        """Queries with joins return combined data."""
        result = await joined_flow_handle.execute({
            "flow": "sales",
            "dimensions": ["c.country"],
            "measures": ["o.order_total"],
        })
        assert len(result) > 0
        row = result[0]
        assert "c.country" in row
        assert "o.order_total" in row

    @pytest.mark.asyncio
    async def test_join_aggregates_correctly(self, joined_flow_handle: FlowHandle):
        """Joined queries aggregate correctly."""
        result = await joined_flow_handle.execute({
            "flow": "sales",
            "dimensions": ["c.country"],
            "measures": ["o.order_total"],
        })
        # US customers: Alice (orders 1,2 = 150) + Carla (orders 4,5 = 275) = 425
        us_row = next((r for r in result if r["c.country"] == "US"), None)
        assert us_row is not None
        assert us_row["o.order_total"] == 425.0
