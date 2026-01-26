"""
Benchmark FlowHandle.execute without HTTP/Pydantic overhead.

This seeds DuckDB demo data using register_dataframe(), builds the flow handle
from examples/flows, and issues repeated execute calls directly against the
Rust extension.
"""

import argparse
import asyncio
import time
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pyarrow as pa

from semaflow import DataSource, FlowHandle


def create_seeded_datasource() -> DataSource:
    """Create in-memory DuckDB with test data matching the shared flows schema."""
    ds = DataSource.duckdb(":memory:", name="warehouse")

    # Dimension: customers
    customers_df = pd.DataFrame({
        "customer_id": [1, 2, 3],
        "country": ["US", "UK", "US"],
        "email": ["alice@example.com", "bob@example.com", "carla@example.com"],
        "signup_date": pd.to_datetime(["2023-01-01", "2023-02-15", "2023-03-10"]),
    })
    ds.register_dataframe("dim_customers", pa.Table.from_pandas(customers_df).to_reader())

    # Dimension: products
    products_df = pd.DataFrame({
        "product_id": [1, 2, 3],
        "product_name": ["Widget", "Gadget", "Gizmo"],
        "category": ["Electronics", "Electronics", "Home"],
        "price": [29.99, 49.99, 19.99],
    })
    ds.register_dataframe("dim_products", pa.Table.from_pandas(products_df).to_reader())

    # Fact: orders
    orders_df = pd.DataFrame({
        "order_id": [1, 2, 3, 4, 5],
        "customer_id": [1, 1, 2, 2, 3],
        "product_id": [1, 2, 1, 3, 2],
        "order_date": pd.to_datetime(["2023-06-01", "2023-06-02", "2023-06-03", "2023-06-04", "2023-06-05"]),
        "status": ["completed", "completed", "completed", "pending", "completed"],
        "quantity": [2, 1, 3, 1, 2],
        "total_amount": [59.98, 49.99, 89.97, 19.99, 99.98],
        "unit_price": [29.99, 49.99, 29.99, 19.99, 49.99],
    })
    ds.register_dataframe("fct_orders", pa.Table.from_pandas(orders_df).to_reader())

    return ds


REQUEST: Dict[str, Any] = {
    "flow": "sales",
    "dimensions": ["c.country"],
    "measures": ["o.order_total", "c.customer_count", "p.price"],
    "filters": [],
    "order": [{"column": "o.order_total", "direction": "desc"}],
    "limit": 10,
}


def percentile(values: List[float], pct: float) -> float:
    ordered = sorted(values)
    k = (len(ordered) - 1) * pct
    f = int(k)
    c = min(f + 1, len(ordered) - 1)
    if f == c:
        return ordered[f]
    return ordered[f] + (ordered[c] - ordered[f]) * (k - f)


async def bench(flow: FlowHandle, payload: Dict[str, Any], iterations: int, concurrency: int):
    latencies: List[float] = []

    async def worker():
        for _ in range(iterations):
            start = time.perf_counter()
            await flow.execute(payload)
            latencies.append((time.perf_counter() - start) * 1000)

    await asyncio.gather(*(worker() for _ in range(concurrency)))
    return latencies


async def main():
    parser = argparse.ArgumentParser(description="In-process FlowHandle.execute benchmark (no HTTP)")
    parser.add_argument("--iterations", type=int, default=100, help="Number of execute calls per worker")
    parser.add_argument("--concurrency", type=int, default=4, help="Number of concurrent workers")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    flow_root = project_root / "examples" / "flows"

    # Create in-memory datasource with test data
    ds = create_seeded_datasource()

    # Use "warehouse" to match the shared flows' data_source field
    flow = FlowHandle.from_dir(flow_root, [ds])

    total_requests = args.iterations * args.concurrency
    print(f"Running {total_requests} execute calls (concurrency={args.concurrency})...")
    start = time.perf_counter()
    latencies = await bench(flow, REQUEST, args.iterations, args.concurrency)
    elapsed = time.perf_counter() - start

    p50 = percentile(latencies, 0.5)
    p95 = percentile(latencies, 0.95)
    print(f"Total time: {elapsed:.3f}s; avg rps: {total_requests / elapsed:.1f}")
    print(f"Latency ms: p50={p50:.2f} p95={p95:.2f} min={min(latencies):.2f} max={max(latencies):.2f}")


if __name__ == "__main__":
    asyncio.run(main())
