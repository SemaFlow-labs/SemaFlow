"""
PostgreSQL concurrency benchmark - test connection pool under load.

Usage:
    docker compose -f examples/postgres/docker-compose.yml up -d
    uv run python examples/postgres/bench_concurrency.py
"""

import asyncio
import os
import time
from pathlib import Path

from semaflow import DataSource, FlowHandle


def get_connection_string() -> str:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    user = os.environ.get("POSTGRES_USER", "semaflow")
    password = os.environ.get("POSTGRES_PASSWORD", "semaflow_pass")
    database = os.environ.get("POSTGRES_DB", "semaflow_demo")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


async def run_query(handle: FlowHandle, query_id: int) -> tuple[int, float, dict]:
    """Run a single query and return (id, duration, result)."""
    start = time.perf_counter()

    # Vary the queries slightly
    requests = [
        {
            "flow": "sales",
            "dimensions": ["c.country"],
            "measures": ["o.order_total", "o.order_count"],
        },
        {
            "flow": "sales",
            "dimensions": ["c.country"],
            "measures": ["o.order_total"],
            "filters": [{"field": "c.country", "op": "==", "value": "US"}],
        },
        {
            "flow": "sales",
            "measures": ["o.order_total", "o.order_count", "o.avg_order_amount"],
        },
    ]

    request = requests[query_id % len(requests)]
    rows = await handle.execute(request)

    duration = time.perf_counter() - start
    return query_id, duration, rows


async def benchmark(concurrency: int, total_queries: int) -> dict:
    """Run benchmark with specified concurrency level."""
    example_dir = Path(__file__).parent
    flow_dir = example_dir / "flows"

    ds = DataSource.postgres(
        get_connection_string(),
        schema="public",
        name="pg_local",
    )
    handle = FlowHandle.from_dir(str(flow_dir), [ds])

    # Warm up - run a few queries first
    for i in range(3):
        await run_query(handle, i)

    # Run concurrent queries
    start = time.perf_counter()

    # Create batches to avoid overwhelming
    results = []
    for batch_start in range(0, total_queries, concurrency):
        batch_end = min(batch_start + concurrency, total_queries)
        tasks = [run_query(handle, i) for i in range(batch_start, batch_end)]
        batch_results = await asyncio.gather(*tasks)
        results.extend(batch_results)

    total_time = time.perf_counter() - start

    durations = [r[1] for r in results]
    return {
        "concurrency": concurrency,
        "total_queries": total_queries,
        "total_time": total_time,
        "queries_per_sec": total_queries / total_time,
        "avg_latency_ms": (sum(durations) / len(durations)) * 1000,
        "min_latency_ms": min(durations) * 1000,
        "max_latency_ms": max(durations) * 1000,
    }


async def main():
    print("PostgreSQL Concurrency Benchmark")
    print("=" * 50)
    print()

    # Test different concurrency levels
    configs = [
        (1, 20),    # Sequential baseline
        (5, 50),    # Light concurrency
        (10, 100),  # Medium concurrency
        (20, 200),  # High concurrency
        (50, 500),  # Stress test
    ]

    for concurrency, total in configs:
        print(f"Running: {total} queries @ {concurrency} concurrent...")
        try:
            result = await benchmark(concurrency, total)
            print(f"  Total time:     {result['total_time']:.2f}s")
            print(f"  Queries/sec:    {result['queries_per_sec']:.1f}")
            print(f"  Avg latency:    {result['avg_latency_ms']:.1f}ms")
            print(f"  Min latency:    {result['min_latency_ms']:.1f}ms")
            print(f"  Max latency:    {result['max_latency_ms']:.1f}ms")
            print()
        except Exception as e:
            print(f"  ERROR: {e}")
            print()


if __name__ == "__main__":
    asyncio.run(main())
