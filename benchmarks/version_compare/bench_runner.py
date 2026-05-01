"""Run polars-api against a local aiohttp server and emit JSON stats.

Designed to be invoked from each isolated venv (0.2.0 / 0.3.0) so we can
compare the httpx-backed async path against the aiohttp-backed one.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing as mp
import statistics
import sys
import time
from contextlib import contextmanager

import httpx
import polars as pl

import polars_api  # noqa: F401  - registers the .api namespace

HOST = "127.0.0.1"


def _serve(port: int, payload_bytes: int) -> None:
    from aiohttp import web

    payload = ("x" * payload_bytes).encode()

    async def handler(request: web.Request) -> web.Response:
        return web.Response(body=payload, content_type="text/plain")

    app = web.Application()
    app.router.add_get("/item/{i}", handler)
    web.run_app(app, host=HOST, port=port, print=None, access_log=None)


@contextmanager
def server(port: int, payload_bytes: int):
    proc = mp.Process(target=_serve, args=(port, payload_bytes), daemon=True)
    proc.start()
    deadline = time.monotonic() + 10
    base = f"http://{HOST}:{port}"
    while time.monotonic() < deadline:
        try:
            with httpx.Client(timeout=0.5) as c:
                if c.get(f"{base}/item/0").status_code == 200:
                    break
        except Exception:
            time.sleep(0.05)
    else:
        proc.terminate()
        raise RuntimeError("server failed to start")
    try:
        yield base
    finally:
        proc.terminate()
        proc.join(timeout=5)


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    k = (len(s) - 1) * pct
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def _stats_from_runs(per_run_total: list[float], per_request_ms: list[list[float]], n: int, payload_bytes: int) -> dict:
    median_total = statistics.median(per_run_total)
    rps = n / median_total if median_total > 0 else float("inf")
    flat_ms = [v for run in per_request_ms for v in run if v is not None]
    return {
        "n_requests": n,
        "repeats": len(per_run_total),
        "wall_total_s_min": min(per_run_total),
        "wall_total_s_median": median_total,
        "wall_total_s_max": max(per_run_total),
        "throughput_rps": rps,
        "throughput_MBps": (rps * payload_bytes) / (1024 * 1024),
        "latency_ms_p50": _percentile(flat_ms, 0.50),
        "latency_ms_p95": _percentile(flat_ms, 0.95),
        "latency_ms_p99": _percentile(flat_ms, 0.99),
        "latency_ms_min": min(flat_ms) if flat_ms else float("nan"),
        "latency_ms_max": max(flat_ms) if flat_ms else float("nan"),
        "latency_ms_mean": statistics.fmean(flat_ms) if flat_ms else float("nan"),
    }


def _run_async_polars(base: str, n: int, concurrency: int) -> tuple[float, list[float]]:
    df = pl.DataFrame({"url": [f"{base}/item/{i}" for i in range(n)]})
    start = time.monotonic()
    out = df.with_columns(
        pl.col("url").api.aget(max_concurrency=concurrency, with_metadata=True).alias("r")
    )
    elapsed = time.monotonic() - start
    per_req = out["r"].struct.field("elapsed_ms").to_list()
    return elapsed, per_req


def _run_sync_polars(base: str, n: int) -> tuple[float, list[float]]:
    df = pl.DataFrame({"url": [f"{base}/item/{i}" for i in range(n)]})
    start = time.monotonic()
    out = df.with_columns(
        pl.col("url").api.get(with_metadata=True).alias("r")
    )
    elapsed = time.monotonic() - start
    per_req = out["r"].struct.field("elapsed_ms").to_list()
    return elapsed, per_req


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--label", type=str, required=True)
    parser.add_argument("--out", type=str, required=True)
    args = parser.parse_args()

    async_configs = [
        (200, 50),
        (1000, 100),
        (2000, 200),
    ]
    sync_configs = [200]

    results: dict = {
        "label": args.label,
        "python": sys.version.split()[0],
        "polars": pl.__version__,
        "payload_bytes": args.payload_bytes,
        "async": [],
        "sync": [],
    }

    with server(args.port, args.payload_bytes) as base:
        # Warmup: avoids first-call import / connection overhead skewing run #1.
        _run_async_polars(base, 50, 25)

        for n, c in async_configs:
            totals: list[float] = []
            per_req_runs: list[list[float]] = []
            for _ in range(args.repeats):
                t, per_req = _run_async_polars(base, n, c)
                totals.append(t)
                per_req_runs.append(per_req)
            stats = _stats_from_runs(totals, per_req_runs, n, args.payload_bytes)
            stats["concurrency"] = c
            results["async"].append(stats)
            print(
                f"[{args.label}] async N={n:5d} c={c:3d} "
                f"median={stats['wall_total_s_median'] * 1000:7.1f}ms "
                f"rps={stats['throughput_rps']:8.1f} "
                f"p50={stats['latency_ms_p50']:6.1f}ms "
                f"p95={stats['latency_ms_p95']:6.1f}ms "
                f"p99={stats['latency_ms_p99']:6.1f}ms",
                flush=True,
            )

        for n in sync_configs:
            totals = []
            per_req_runs = []
            for _ in range(max(1, args.repeats // 2)):  # sync is slow; halve repeats
                t, per_req = _run_sync_polars(base, n)
                totals.append(t)
                per_req_runs.append(per_req)
            stats = _stats_from_runs(totals, per_req_runs, n, args.payload_bytes)
            results["sync"].append(stats)
            print(
                f"[{args.label}] sync  N={n:5d}      "
                f"median={stats['wall_total_s_median'] * 1000:7.1f}ms "
                f"rps={stats['throughput_rps']:8.1f} "
                f"p50={stats['latency_ms_p50']:6.1f}ms "
                f"p95={stats['latency_ms_p95']:6.1f}ms "
                f"p99={stats['latency_ms_p99']:6.1f}ms",
                flush=True,
            )

    with open(args.out, "w") as f:
        json.dump(results, f, indent=2)


if __name__ == "__main__":
    main()
