"""
Benchmark FlowHandle.execute without HTTP/Pydantic overhead.

This seeds the DuckDB demo data, builds the flow handle from examples/flows,
and issues repeated execute calls directly against the Rust extension.
"""

import argparse
import asyncio
import time
from pathlib import Path
from typing import Any, Dict, List

import duckdb

from semaflow import DataSource, FlowHandle


def seed_duckdb(db_path: Path) -> None:
    """Create test tables matching the shared flows schema."""
    if db_path.exists():
        db_path.unlink()
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        -- Dimension: customers
        CREATE TABLE dim_customers (
            customer_id INTEGER PRIMARY KEY,
            country VARCHAR,
            email VARCHAR,
            signup_date DATE
        );

        -- Dimension: products
        CREATE TABLE dim_products (
            product_id INTEGER PRIMARY KEY,
            product_name VARCHAR,
            category VARCHAR,
            price DOUBLE
        );

        -- Fact: orders
        CREATE TABLE fct_orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            order_date DATE,
            status VARCHAR,
            quantity INTEGER,
            total_amount DOUBLE,
            unit_price DOUBLE
        );

        -- Seed customers
        INSERT INTO dim_customers VALUES
            (1, 'US', 'alice@example.com', '2023-01-01'),
            (2, 'UK', 'bob@example.com', '2023-02-15'),
            (3, 'US', 'carla@example.com', '2023-03-10');

        -- Seed products
        INSERT INTO dim_products VALUES
            (1, 'Widget', 'Electronics', 29.99),
            (2, 'Gadget', 'Electronics', 49.99),
            (3, 'Gizmo', 'Home', 19.99);

        -- Seed orders
        INSERT INTO fct_orders VALUES
            (1, 1, 1, '2023-06-01', 'completed', 2, 59.98, 29.99),
            (2, 1, 2, '2023-06-02', 'completed', 1, 49.99, 49.99),
            (3, 2, 1, '2023-06-03', 'completed', 3, 89.97, 29.99),
            (4, 2, 3, '2023-06-04', 'pending', 1, 19.99, 19.99),
            (5, 3, 2, '2023-06-05', 'completed', 2, 99.98, 49.99);
        """
    )
    conn.close()


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
    db_path = project_root / "examples" / "duckdb" / "benchmark.duckdb"
    seed_duckdb(db_path)

    # Use "warehouse" to match the shared flows' data_source field
    flow = FlowHandle.from_dir(flow_root, [DataSource.duckdb(str(db_path), name="warehouse")])

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
