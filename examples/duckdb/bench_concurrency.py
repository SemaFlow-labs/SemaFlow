"""
DuckDB concurrency benchmark - compare with PostgreSQL.

Usage:
    uv run python examples/duckdb/bench_concurrency.py
"""

import asyncio
import time
from pathlib import Path

import duckdb

from semaflow import DataSource, FlowHandle


def seed_duckdb(db_path: Path) -> None:
    if db_path.exists():
        db_path.unlink()
    conn = duckdb.connect(str(db_path))
    conn.execute("""
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
            (3, 'Carla', 'US'),
            (4, 'David', 'DE');
        INSERT INTO orders VALUES
            (1, 1, 100.0, '2024-01-01'),
            (2, 1, 50.0, '2024-01-02'),
            (3, 2, 25.0, '2024-01-03'),
            (4, 3, 200.0, '2024-01-04'),
            (5, 3, 75.0, '2024-01-05');
    """)
    conn.close()


async def run_query(handle: FlowHandle, query_id: int) -> tuple[int, float, list[dict]]:
    """Run a single query and return (id, duration, result)."""
    start = time.perf_counter()

    requests = [
        {
            "flow": "sales",
            "dimensions": ["c.country"],
            "measures": ["o.order_total", "o.order_count", "c.customer_count"],
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
    db_path = example_dir / "bench.duckdb"

    seed_duckdb(db_path)

    ds = DataSource.duckdb(str(db_path), name="duckdb_local")
    handle = FlowHandle.from_dir(flow_dir, [ds])

    # Warm up
    for i in range(3):
        await run_query(handle, i)

    # Run concurrent queries
    start = time.perf_counter()

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
    print("DuckDB Concurrency Benchmark")
    print("=" * 50)
    print()

    configs = [
        (1, 20),
        (5, 50),
        (10, 100),
        (20, 200),
        (50, 500),
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
