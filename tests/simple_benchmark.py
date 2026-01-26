"""
Simple benchmark for baseline FlowHandle.execute performance.

This uses a minimal schema (2 tables, no joins in query) to measure
pure overhead without multi-grain query complexity.
"""

import argparse
import asyncio
import time
from typing import Any, Dict, List

import pandas as pd
import pyarrow as pa

from semaflow import DataSource, FlowHandle


def create_seeded_datasource() -> DataSource:
    """Create in-memory DuckDB with test data."""
    ds = DataSource.duckdb(":memory:", name="bench_db")

    customers_df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Carla"],
        "country": ["US", "UK", "US"],
    })
    ds.register_dataframe("customers", pa.Table.from_pandas(customers_df).to_reader())

    orders_df = pd.DataFrame({
        "id": [1, 2, 3],
        "customer_id": [1, 1, 2],
        "amount": [100.0, 50.0, 25.0],
        "status": ["completed", "completed", "pending"],
        "created_at": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"]),
    })
    ds.register_dataframe("orders", pa.Table.from_pandas(orders_df).to_reader())

    return ds


def create_flow_handle(ds: DataSource) -> FlowHandle:
    """Create FlowHandle with inline table/flow definitions (no YAML)."""
    from semaflow import Dimension, Measure, SemanticFlow, SemanticTable

    orders = SemanticTable(
        name="orders",
        data_source=ds,
        table="orders",
        primary_key="id",
        dimensions={
            "order_id": Dimension(expr="id"),
            "status": Dimension(expr="status"),
            "customer_id": Dimension(expr="customer_id"),
        },
        measures={
            "order_total": Measure(expr="amount", agg="sum"),
            "order_count": Measure(expr="id", agg="count"),
        },
    )

    flow = SemanticFlow(
        name="simple",
        base_table=orders,
        base_table_alias="o",
    )

    return FlowHandle.from_parts(
        tables=[orders],
        flows=[flow],
        data_sources=[ds],
    )


# Simple query - single table, no joins needed
REQUEST: Dict[str, Any] = {
    "flow": "simple",
    "dimensions": ["o.status"],
    "measures": ["o.order_total", "o.order_count"],
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
    parser = argparse.ArgumentParser(description="Simple FlowHandle.execute benchmark (baseline)")
    parser.add_argument("--iterations", type=int, default=100, help="Number of execute calls per worker")
    parser.add_argument("--concurrency", type=int, default=4, help="Number of concurrent workers")
    args = parser.parse_args()

    ds = create_seeded_datasource()
    flow = create_flow_handle(ds)

    total_requests = args.iterations * args.concurrency
    print(f"Running {total_requests} execute calls (concurrency={args.concurrency})...")
    print("Query: single-table flat query (no joins, no multi-grain)")
    start = time.perf_counter()
    latencies = await bench(flow, REQUEST, args.iterations, args.concurrency)
    elapsed = time.perf_counter() - start

    p50 = percentile(latencies, 0.5)
    p95 = percentile(latencies, 0.95)
    print(f"Total time: {elapsed:.3f}s; avg rps: {total_requests / elapsed:.1f}")
    print(f"Latency ms: p50={p50:.2f} p95={p95:.2f} min={min(latencies):.2f} max={max(latencies):.2f}")


if __name__ == "__main__":
    asyncio.run(main())
