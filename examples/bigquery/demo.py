"""
BigQuery demo - connect to BQ and run queries via SemaFlow.

Required environment variables:
  - GCP_PROJECT_ID: Your GCP project ID
  - BQ_DATASET: BigQuery dataset name

Authentication:
  Uses Application Default Credentials (ADC). Run:
    gcloud auth application-default login
  Or set GOOGLE_APPLICATION_CREDENTIALS to a service account key file.
"""

import asyncio
import os
from pathlib import Path

import dotenv

from semaflow import DataSource, FlowHandle

dotenv.load_dotenv()


def get_env_or_exit(var: str) -> str:
    """Get environment variable or exit with helpful message."""
    value = os.environ.get(var)
    if not value:
        print(f"Error: {var} environment variable is required")
        print(f"Set it with: export {var}=<value>")
        exit(1)
    return value


async def main() -> None:
    example_dir = Path(__file__).parent
    flow_dir = example_dir / "flows"

    # Get configuration from environment
    project_id = get_env_or_exit("GCP_PROJECT_ID")
    dataset = get_env_or_exit("BQ_DATASET")

    print(f"Connecting to BigQuery ({project_id}.{dataset})...")

    # Create FlowHandle with BigQuery data source
    ds = DataSource.bigquery(
        project_id=project_id,
        dataset=dataset,
        name="bq",
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
