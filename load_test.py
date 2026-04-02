"""
scripts/load_test.py

Simple async load test for the DPE pricing endpoint.
Measures p50, p95, p99 latencies and throughput.

Requires DPE running at localhost:8001 with Redis seeded.

Usage:
    python scripts/load_test.py --rps 100 --duration 30
"""

import argparse
import asyncio
import time
import statistics
import random
import httpx


async def single_request(
    client: httpx.AsyncClient,
    user_id: str,
    sku_id: str,
    latencies: list,
    errors: list,
):
    t = time.perf_counter()
    try:
        resp = await client.post(
            "/v1/price",
            json={
                "user_id": user_id,
                "sku_id": sku_id,
                "experiment_id": "exp_001",
            },
            timeout=2.0,
        )
        latency_ms = (time.perf_counter() - t) * 1000
        latencies.append(latency_ms)

        if resp.status_code != 200:
            errors.append(f"HTTP {resp.status_code}")
    except Exception as e:
        errors.append(str(e))


async def run_load_test(
    base_url: str,
    rps: int,
    duration_sec: int,
    n_users: int = 200,
    n_skus: int = 100,
):
    rng = random.Random(42)
    user_ids = [f"user_{i:04d}" for i in range(n_users)]
    sku_ids  = [f"sku_{i:04d}"  for i in range(n_skus)]

    latencies: list[float] = []
    errors: list[str] = []
    total_requests = 0

    interval = 1.0 / rps
    t_end = time.time() + duration_sec

    print(f"\nLoad test: {rps} RPS for {duration_sec}s → {rps * duration_sec} requests target")
    print(f"Target: {base_url}/v1/price")
    print("-" * 50)

    async with httpx.AsyncClient(base_url=base_url) as client:
        tasks = []
        while time.time() < t_end:
            user_id = rng.choice(user_ids)
            sku_id  = rng.choice(sku_ids)
            task = asyncio.create_task(
                single_request(client, user_id, sku_id, latencies, errors)
            )
            tasks.append(task)
            total_requests += 1
            await asyncio.sleep(interval)

        await asyncio.gather(*tasks, return_exceptions=True)

    # Results
    successful = len(latencies)
    error_count = len(errors)

    print(f"\nResults")
    print(f"  Total requests   : {total_requests}")
    print(f"  Successful       : {successful}")
    print(f"  Errors           : {error_count}")
    print(f"  Error rate       : {error_count / max(total_requests, 1) * 100:.2f}%")

    if latencies:
        latencies.sort()
        print(f"\nLatency (ms)")
        print(f"  p50  : {statistics.median(latencies):.2f}")
        print(f"  p90  : {latencies[int(len(latencies) * 0.90)]:.2f}")
        print(f"  p95  : {latencies[int(len(latencies) * 0.95)]:.2f}")
        print(f"  p99  : {latencies[int(len(latencies) * 0.99)]:.2f}")
        print(f"  max  : {max(latencies):.2f}")
        print(f"  mean : {statistics.mean(latencies):.2f}")

        p99 = latencies[int(len(latencies) * 0.99)]
        target = 200.0
        status = "PASS" if p99 < target else "FAIL"
        print(f"\n  p99 target (<{target}ms): {status} ({p99:.2f}ms)")

    if errors[:5]:
        print(f"\nFirst 5 errors: {errors[:5]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url",      default="http://localhost:8001")
    parser.add_argument("--rps",      type=int, default=50)
    parser.add_argument("--duration", type=int, default=20)
    parser.add_argument("--users",    type=int, default=200)
    parser.add_argument("--skus",     type=int, default=100)
    args = parser.parse_args()

    asyncio.run(run_load_test(
        base_url=args.url,
        rps=args.rps,
        duration_sec=args.duration,
        n_users=args.users,
        n_skus=args.skus,
    ))
