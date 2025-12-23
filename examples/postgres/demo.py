"""
PostgreSQL demo - connect to PostgreSQL and run queries via SemaFlow.

Prerequisites:
    1. Start PostgreSQL: docker compose up -d
    2. Wait for it to be ready (check with: docker compose logs -f)

Usage:
    cd examples/postgres
    uv run python demo.py

Environment Variables (optional - defaults match docker-compose.yml):
    POSTGRES_HOST     - PostgreSQL host (default: localhost)
    POSTGRES_PORT     - PostgreSQL port (default: 5432)
    POSTGRES_USER     - Username (default: semaflow)
    POSTGRES_PASSWORD - Password (default: semaflow_pass)
    POSTGRES_DB       - Database name (default: semaflow_demo)
    POSTGRES_SCHEMA   - Schema name (default: public)
"""

import asyncio
import os
from pathlib import Path

from semaflow import DataSource, FlowHandle


def get_connection_string() -> str:
    """
    Build PostgreSQL connection string from environment variables.

    Credentials can be passed in several ways:

    1. URL format (recommended for production):
       postgresql://user:password@host:port/database

    2. Key-value format:
       host=localhost port=5432 user=myuser password=mypass dbname=mydb

    3. Environment variables (shown here):
       Export POSTGRES_HOST, POSTGRES_USER, etc.

    Security note: Never commit credentials to git. Use environment
    variables or a secrets manager in production.
    """
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    user = os.environ.get("POSTGRES_USER", "semaflow")
    password = os.environ.get("POSTGRES_PASSWORD", "semaflow_pass")
    database = os.environ.get("POSTGRES_DB", "semaflow_demo")

    # URL format - password is URL-encoded if it contains special chars
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def get_schema() -> str:
    """Get PostgreSQL schema name from environment."""
    return os.environ.get("POSTGRES_SCHEMA", "public")


async def main() -> None:
    example_dir = Path(__file__).parent
    flow_dir = example_dir / "flows"

    # Build connection string from environment
    conn_string = get_connection_string()
    schema = get_schema()

    print(f"Connecting to PostgreSQL...")
    print(f"  Host: {os.environ.get('POSTGRES_HOST', 'localhost')}")
    print(f"  Database: {os.environ.get('POSTGRES_DB', 'semaflow_demo')}")
    print(f"  Schema: {schema}")

    # Create FlowHandle with PostgreSQL data source
    # The schema parameter is REQUIRED for PostgreSQL
    ds = DataSource.postgres(
        connection_string=conn_string,
        schema=schema,  # Required: PostgreSQL schema (e.g., "public")
        name="pg_local",
    )
    handle = FlowHandle.from_dir(str(flow_dir), [ds])

    # List available flows
    print("\n--- Available Flows ---")
    for flow in handle.list_flows():
        print(f"  {flow['name']}: {flow.get('description', 'No description')}")

    # Show schema for sales flow
    schema_info = handle.get_flow("sales")
    print("\n--- Sales Flow Schema ---")
    print("Dimensions:")
    for dim in schema_info["dimensions"]:
        print(f"  {dim['qualified_name']}: {dim.get('description', '')}")
    print("Measures:")
    for measure in schema_info["measures"]:
        print(f"  {measure['qualified_name']}: {measure.get('description', '')}")

    # Query 1: Total sales by country
    print("\n--- Query 1: Sales by Country ---")
    request = {
        "flow": "sales",
        "dimensions": ["c.country"],
        "measures": ["o.order_total", "o.order_count"],
        "order": [{"column": "o.order_total", "direction": "desc"}],
    }
    sql = await handle.build_sql(request)
    print(f"SQL:\n{sql}\n")

    rows = await handle.execute(request)
    print("Results:")
    for row in rows:
        print(f"  {row}")

    # Query 2: Filter by country
    print("\n--- Query 2: US Sales Only ---")
    request = {
        "flow": "sales",
        "dimensions": ["c.country"],
        "measures": ["o.order_total", "c.customer_count"],
        "filters": [{"field": "c.country", "op": "==", "value": "US"}],
    }
    sql = await handle.build_sql(request)
    print(f"SQL:\n{sql}\n")

    rows = await handle.execute(request)
    print("Results:")
    for row in rows:
        print(f"  {row}")

    # Query 3: Derived measure (average order amount)
    print("\n--- Query 3: Average Order Amount by Country ---")
    request = {
        "flow": "sales",
        "dimensions": ["c.country"],
        "measures": ["o.order_total", "o.order_count", "o.avg_order_amount"],
        "order": [{"column": "o.avg_order_amount", "direction": "desc"}],
    }
    sql = await handle.build_sql(request)
    print(f"SQL:\n{sql}\n")

    rows = await handle.execute(request)
    print("Results:")
    for row in rows:
        print(f"  {row}")


if __name__ == "__main__":
    asyncio.run(main())
