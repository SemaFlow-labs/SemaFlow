"""
Async load tester for the sample semantic API.

It ramps requests per second (RPS) from a starting rate up to a max rate and
prints timing stats per stage plus an overall summary.
"""

import argparse
import asyncio
import json
import math
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

DEFAULT_PAYLOAD: Dict[str, Any] = {
    "dimensions": ["c.country"],
    "measures": ["o.order_total", "c.customer_count"],
    "filters": [],
    "order": [{"column": "o.order_total", "direction": "desc"}],
    "limit": 10,
}


def percentile(values: List[float], pct: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    k = (len(ordered) - 1) * pct
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return ordered[int(k)]
    return ordered[f] + (ordered[c] - ordered[f]) * (k - f)


def summarize(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    latencies = [r["latency_ms"] for r in results]
    failures = [r for r in results if not r["ok"]]
    errors = Counter(r["error"] for r in failures if r["error"])
    return {
        "count": len(results),
        "success": len(results) - len(failures),
        "failures": len(failures),
        "latency_ms": {
            "p50": percentile(latencies, 0.5),
            "p95": percentile(latencies, 0.95),
            "max": max(latencies) if latencies else None,
            "min": min(latencies) if latencies else None,
        },
        "errors": errors.most_common(3),
    }


async def dispatch_request(
    client: httpx.AsyncClient,
    url: str,
    payload: Dict[str, Any],
    timeout: float,
    semaphore: asyncio.Semaphore,
) -> Dict[str, Any]:
    async with semaphore:
        start = time.perf_counter()
        status: Optional[int] = None
        error: Optional[str] = None
        ok = False
        try:
            resp = await client.post(url, json=payload, timeout=timeout)
            status = resp.status_code
            ok = status == 200
            if not ok:
                error = resp.text[:200]
        except Exception as exc:  # pragma: no cover - runtime error capture
            error = str(exc)
        latency_ms = (time.perf_counter() - start) * 1000
        return {"latency_ms": latency_ms, "ok": ok, "status": status, "error": error}


async def dispatch_at(
    scheduled_time: float,
    client: httpx.AsyncClient,
    url: str,
    payload: Dict[str, Any],
    timeout: float,
    semaphore: asyncio.Semaphore,
) -> Dict[str, Any]:
    now = asyncio.get_event_loop().time()
    if scheduled_time > now:
        await asyncio.sleep(scheduled_time - now)
    return await dispatch_request(client, url, payload, timeout, semaphore)


async def run_stage(
    client: httpx.AsyncClient,
    url: str,
    payload: Dict[str, Any],
    rps: float,
    duration: float,
    timeout: float,
    max_in_flight: int,
) -> Dict[str, Any]:
    semaphore = asyncio.Semaphore(max_in_flight)
    loop = asyncio.get_event_loop()
    stage_start = loop.time()
    total_requests = max(1, int(rps * duration))
    tasks = []
    for i in range(total_requests):
        scheduled = stage_start + i / rps
        tasks.append(
            asyncio.create_task(
                dispatch_at(scheduled, client, url, payload, timeout, semaphore)
            )
        )
    results = await asyncio.gather(*tasks)
    summary = summarize(list(results))
    summary["rps_target"] = rps
    summary["duration_s"] = duration
    summary["raw"] = list(results)
    return summary


def stage_sequence(start_rps: float, max_rps: float, step: float) -> List[float]:
    if start_rps <= 0 or max_rps <= 0:
        raise ValueError("start_rps and max_rps must be positive")
    if start_rps > max_rps:
        raise ValueError("start_rps cannot exceed max_rps")
    if step <= 0:
        raise ValueError("step must be positive")
    stages: List[float] = []
    current = start_rps
    while current <= max_rps:
        stages.append(current)
        current += step
    if stages[-1] != max_rps:
        stages.append(max_rps)
    return stages


def load_payload(path: Optional[Path]) -> Dict[str, Any]:
    if not path:
        return DEFAULT_PAYLOAD
    with path.open() as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise ValueError("payload file must contain a JSON object")
    return payload


async def verify_flow_available(client: httpx.AsyncClient, host: str, flow: str) -> None:
    resp = await client.get(f"{host.rstrip('/')}/flows")
    resp.raise_for_status()
    data = resp.json()
    flows = data.get("flows") if isinstance(data, dict) else None
    if not isinstance(flows, dict) or flow not in flows:
        raise RuntimeError(f"flow '{flow}' not available at {host}")


async def main() -> None:
    parser = argparse.ArgumentParser(description="Ramp load against semantic_api.py")
    parser.add_argument("--host", default="http://127.0.0.1:8080", help="API host, including scheme")
    parser.add_argument("--flow", default="sales", help="Flow name exposed by the API")
    parser.add_argument("--start-rps", type=float, default=1.0, help="Starting requests per second")
    parser.add_argument("--max-rps", type=float, default=50.0, help="Max requests per second")
    parser.add_argument("--step", type=float, default=5.0, help="Increment between stages")
    parser.add_argument("--duration-per-stage", type=float, default=10.0, help="Stage length in seconds")
    parser.add_argument("--timeout", type=float, default=5.0, help="Per-request timeout in seconds")
    parser.add_argument(
        "--payload-file",
        type=Path,
        help="Optional JSON file with the request body (without the flow key)",
    )
    parser.add_argument(
        "--max-in-flight",
        type=int,
        default=200,
        help="Cap concurrent in-flight requests to avoid runaway backlog",
    )
    args = parser.parse_args()

    payload = load_payload(args.payload_file)
    url = f"{args.host.rstrip('/')}/flows/{args.flow}/query"
    stages = stage_sequence(args.start_rps, args.max_rps, args.step)

    print(f"Target URL: {url}")
    print(f"Stage RPS values: {stages}")
    print("Beginning ramp...\n")

    async with httpx.AsyncClient() as client:
        try:
            await verify_flow_available(client, args.host, args.flow)
        except Exception as exc:
            print(f"Flow check failed: {exc}")
            return
        stage_summaries: List[Dict[str, Any]] = []
        for rps in stages:
            summary = await run_stage(
                client=client,
                url=url,
                payload=payload,
                rps=rps,
                duration=args.duration_per_stage,
                timeout=args.timeout,
                max_in_flight=args.max_in_flight,
            )
            stage_summaries.append(summary)
            lat = summary["latency_ms"]
            print(
                f"Stage {rps:.1f} rps | "
                f"ok={summary['success']}/{summary['count']} "
                f"p50={lat['p50']:.2f}ms p95={lat['p95']:.2f}ms "
                f"min={lat['min']:.2f}ms max={lat['max']:.2f}ms "
                f"errors={summary['errors']}"
            )

    # Overall summary
    combined = summarize([r for stage in stage_summaries for r in stage["raw"]])
    print("\nSummary:")
    print(
        f"  stages: {len(stage_summaries)}; total requests: {combined['count']}; "
        f"successes: {combined['success']}; failures: {combined['failures']}"
    )
    clat = combined["latency_ms"]
    print(
        f"  overall latency p50={clat['p50']:.2f}ms p95={clat['p95']:.2f}ms "
        f"min={clat['min']:.2f}ms max={clat['max']:.2f}ms errors={combined['errors']}"
    )
    for idx, s in enumerate(stage_summaries, 1):
        lat = s["latency_ms"]
        print(
            f"  [{idx}] {s['rps_target']:.1f} rps -> ok={s['success']}/{s['count']} "
            f"p50={lat['p50']:.2f}ms p95={lat['p95']:.2f}ms max={lat['max']:.2f}ms errors={s['errors']}"
        )


if __name__ == "__main__":
    asyncio.run(main())
