"""
Tests for FastAPI integration.
"""

import pytest
from httpx import ASGITransport, AsyncClient

from semaflow import FlowHandle
from semaflow.api import create_app, create_router


@pytest.fixture
def app(simple_flow_handle: FlowHandle):
    """Create a FastAPI app with the simple flow handle."""
    return create_app(simple_flow_handle)


@pytest.fixture
def joined_app(joined_flow_handle: FlowHandle):
    """Create a FastAPI app with the joined flow handle."""
    return create_app(joined_flow_handle)


@pytest.fixture
async def client(app):
    """Create an async test client."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.fixture
async def joined_client(joined_app):
    """Create an async test client for joined flow."""
    transport = ASGITransport(app=joined_app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


class TestListFlowsEndpoint:
    """Tests for GET /flows endpoint."""

    @pytest.mark.asyncio
    async def test_returns_flow_list(self, client: AsyncClient):
        """GET /flows returns dict with flows mapping."""
        response = await client.get("/flows")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)
        assert "flows" in data

    @pytest.mark.asyncio
    async def test_flow_list_contains_name(self, client: AsyncClient):
        """Flow list includes flow names."""
        response = await client.get("/flows")
        data = response.json()
        assert "simple_orders" in data["flows"]


class TestGetFlowSchemaEndpoint:
    """Tests for GET /flows/{flow} endpoint."""

    @pytest.mark.asyncio
    async def test_returns_flow_schema(self, client: AsyncClient):
        """GET /flows/{flow} returns schema with dimensions and measures."""
        response = await client.get("/flows/simple_orders")
        assert response.status_code == 200
        data = response.json()
        assert "dimensions" in data
        assert "measures" in data

    @pytest.mark.asyncio
    async def test_schema_contains_fields(self, client: AsyncClient):
        """Schema includes configured dimension and measure names."""
        response = await client.get("/flows/simple_orders")
        data = response.json()
        # Check dimensions are present (API returns a dict keyed by qualified name)
        assert "o.status" in data["dimensions"]
        # Check measures are present
        assert "o.order_total" in data["measures"]

    @pytest.mark.asyncio
    async def test_unknown_flow_returns_error(self, client: AsyncClient):
        """GET /flows/{flow} returns error for unknown flow."""
        response = await client.get("/flows/nonexistent")
        # API returns 404 for unknown flows
        assert response.status_code in (400, 404)


class TestQueryEndpoint:
    """Tests for POST /flows/{flow}/query endpoint."""

    @pytest.mark.asyncio
    async def test_basic_query_returns_rows(self, client: AsyncClient):
        """POST /flows/{flow}/query returns query results."""
        response = await client.post(
            "/flows/simple_orders/query",
            json={
                "dimensions": ["o.status"],
                "measures": ["o.order_total"],
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "rows" in data
        assert len(data["rows"]) > 0

    @pytest.mark.asyncio
    async def test_query_with_filters(self, client: AsyncClient):
        """Query endpoint respects filters."""
        response = await client.post(
            "/flows/simple_orders/query",
            json={
                "dimensions": ["o.status"],
                "measures": ["o.order_total"],
                "filters": [{"field": "o.status", "op": "==", "value": "complete"}],
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["rows"]) == 1
        assert data["rows"][0]["o.status"] == "complete"

    @pytest.mark.asyncio
    async def test_query_with_order(self, client: AsyncClient):
        """Query endpoint respects ordering."""
        response = await client.post(
            "/flows/simple_orders/query",
            json={
                "dimensions": ["o.status"],
                "measures": ["o.order_total"],
                "order": [{"column": "o.order_total", "direction": "desc"}],
            },
        )
        assert response.status_code == 200
        data = response.json()
        totals = [r["o.order_total"] for r in data["rows"]]
        assert totals == sorted(totals, reverse=True)

    @pytest.mark.asyncio
    async def test_query_with_limit(self, client: AsyncClient):
        """Query endpoint respects limit."""
        response = await client.post(
            "/flows/simple_orders/query",
            json={
                "dimensions": ["o.status"],
                "measures": ["o.order_total"],
                "limit": 1,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["rows"]) == 1

    @pytest.mark.asyncio
    async def test_unknown_flow_returns_error(self, client: AsyncClient):
        """Query endpoint returns error for unknown flow."""
        response = await client.post(
            "/flows/nonexistent/query",
            json={"dimensions": ["status"], "measures": ["total"]},
        )
        # API returns 404 for unknown flows
        assert response.status_code in (400, 404)


class TestQueryPagination:
    """Tests for paginated query execution via API."""

    @pytest.mark.asyncio
    async def test_pagination_returns_metadata(self, client: AsyncClient):
        """Query with page_size returns pagination metadata."""
        response = await client.post(
            "/flows/simple_orders/query",
            json={
                "dimensions": ["o.status"],
                "measures": ["o.order_total"],
                "page_size": 1,
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert "rows" in data
        assert "cursor" in data
        assert "has_more" in data

    @pytest.mark.asyncio
    async def test_cursor_fetches_next_page(self, client: AsyncClient):
        """Cursor from first page can fetch next page."""
        # First page
        response1 = await client.post(
            "/flows/simple_orders/query",
            json={
                "dimensions": ["o.status"],
                "measures": ["o.order_total"],
                "page_size": 1,
            },
        )
        data1 = response1.json()
        assert data1["has_more"] is True
        cursor = data1["cursor"]

        # Second page
        response2 = await client.post(
            "/flows/simple_orders/query",
            json={
                "dimensions": ["o.status"],
                "measures": ["o.order_total"],
                "page_size": 1,
                "cursor": cursor,
            },
        )
        data2 = response2.json()
        assert "rows" in data2
        # Different data on second page
        assert data1["rows"][0] != data2["rows"][0]


class TestJoinedQueries:
    """Tests for queries with joins via API."""

    @pytest.mark.asyncio
    async def test_join_query_works(self, joined_client: AsyncClient):
        """API handles queries with joined tables."""
        response = await joined_client.post(
            "/flows/sales/query",
            json={
                "dimensions": ["c.country"],
                "measures": ["o.order_total"],
            },
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data["rows"]) > 0
        row = data["rows"][0]
        assert "c.country" in row
        assert "o.order_total" in row


class TestCreateRouter:
    """Tests for create_router() function."""

    def test_creates_router(self, simple_flow_handle: FlowHandle):
        """create_router() returns an APIRouter."""
        from fastapi import APIRouter
        router = create_router(simple_flow_handle)
        assert isinstance(router, APIRouter)
