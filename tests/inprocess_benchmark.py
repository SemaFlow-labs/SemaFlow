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
    if db_path.exists():
        db_path.unlink()
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            country VARCHAR
        );
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            amount DOUBLE,
            created_at TIMESTAMP
        );
        INSERT INTO customers VALUES
            (1, 'Alice', 'US'),
            (2, 'Bob', 'UK'),
            (3, 'Carla', 'US');
        INSERT INTO orders VALUES
            (1, 1, 100.0, '2023-01-01'),
            (2, 1, 50.0, '2023-01-02'),
            (3, 2, 25.0, '2023-01-03');
        """
    )
    conn.close()


REQUEST: Dict[str, Any] = {
    "flow": "sales",
    "dimensions": ["c.country"],
    "measures": ["o.order_total", "c.customer_count"],
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
    flow_root = project_root / "examples" / "duckdb" / "flows"
    db_path = project_root / "examples" / "duckdb" / "benchmark.duckdb"
    seed_duckdb(db_path)

    flow = FlowHandle.from_dir(flow_root, [DataSource.duckdb(str(db_path), name="duckdb_local")])

    total_requests = args.iterations * args.concurrency
    print(f"Running {total_requests} execute calls (concurrency={args.concurrency})...")
    start = time.perf_counter()
    latencies = await bench(flow, REQUEST, args.iterations, args.concurrency)
    elapsed = time.perf_counter() - start

    p50 = percentile(latencies, 0.5)
    p95 = percentile(latencies, 0.95)
    print(f"Total time: {elapsed:.3f}s; avg rps: {total_requests/elapsed:.1f}")
    print(
        f"Latency ms: p50={p50:.2f} p95={p95:.2f} "
        f"min={min(latencies):.2f} max={max(latencies):.2f}"
    )


if __name__ == "__main__":
    asyncio.run(main())
