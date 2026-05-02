"""Benchmark polars-api (aiohttp) vs bare httpx vs bare aiohttp.

Spins up a local aiohttp server, then issues N concurrent GETs against it
with each client. Reports wall-clock time and requests/sec.

Usage:
    uv run python benchmarks/bench.py
    uv run python benchmarks/bench.py --repeats 7 --output benchmarks/results
    uv run python benchmarks/bench.py --scenarios 100/50,1000/100

Each scenario is N/concurrency. The script writes:
    <output>.json  - structured results + environment info
    <output>.md    - Markdown table for pasting into the README

Run multiple times and average to smooth out noise; benchmarks are inherently
noisy. Use the same machine and avoid concurrent CPU load for stable numbers.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import multiprocessing as mp
import platform
import socket
import statistics
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Callable

import aiohttp
import httpx
import polars as pl

import polars_api  # noqa: F401  - registers the .api namespace

HOST = "127.0.0.1"
DEFAULT_PORT = 18080


# ---------- local server ----------
def _serve(port: int) -> None:
    from aiohttp import web

    async def handler(request: web.Request) -> web.Response:
        i = request.match_info.get("i", "0")
        return web.json_response({"i": int(i), "ok": True})

    app = web.Application()
    app.router.add_get("/item/{i}", handler)
    web.run_app(app, host=HOST, port=port, print=None, access_log=None)


def _pick_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, 0))
        return s.getsockname()[1]


@contextmanager
def server(port: int):
    proc = mp.Process(target=_serve, args=(port,), daemon=True)
    proc.start()
    base = f"http://{HOST}:{port}"
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            with httpx.Client(timeout=0.5) as c:
                if c.get(f"{base}/item/0").status_code == 200:
                    break
        except Exception:
            time.sleep(0.05)
    else:
        proc.terminate()
        msg = "server failed to start"
        raise RuntimeError(msg)
    try:
        yield base
    finally:
        proc.terminate()
        proc.join(timeout=5)


# ---------- benchmarks ----------
def _urls(base: str, n: int) -> list[str]:
    return [f"{base}/item/{i}" for i in range(n)]


async def bench_httpx_async(base: str, n: int, concurrency: int) -> float:
    """Bare httpx with default limits (matches polars-api's old default client)."""
    sem = asyncio.Semaphore(concurrency)

    async def one(client: httpx.AsyncClient, url: str) -> str:
        async with sem:
            r = await client.get(url)
            return r.text

    async with httpx.AsyncClient() as client:
        start = time.monotonic()
        await asyncio.gather(*(one(client, u) for u in _urls(base, n)))
        return time.monotonic() - start


async def bench_httpx_async_tuned(base: str, n: int, concurrency: int) -> float:
    """Bare httpx with keepalive pool raised to match concurrency."""
    sem = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(
        max_connections=concurrency,
        max_keepalive_connections=concurrency,
    )

    async def one(client: httpx.AsyncClient, url: str) -> str:
        async with sem:
            r = await client.get(url)
            return r.text

    async with httpx.AsyncClient(limits=limits) as client:
        start = time.monotonic()
        await asyncio.gather(*(one(client, u) for u in _urls(base, n)))
        return time.monotonic() - start


async def bench_aiohttp(base: str, n: int, concurrency: int) -> float:
    sem = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency)

    async def one(session: aiohttp.ClientSession, url: str) -> str:
        async with sem, session.get(url) as r:
            return await r.text()

    async with aiohttp.ClientSession(connector=connector) as session:
        start = time.monotonic()
        await asyncio.gather(*(one(session, u) for u in _urls(base, n)))
        return time.monotonic() - start


def bench_polars_api(base: str, n: int, concurrency: int) -> float:
    df = pl.DataFrame({"url": _urls(base, n)})
    start = time.monotonic()
    df.with_columns(pl.col("url").api.aget(max_concurrency=concurrency).alias("body"))
    return time.monotonic() - start


def bench_polars_api_shared_client(base: str, n: int, concurrency: int) -> float:
    """polars-api reusing an aiohttp.ClientSession across the call."""
    df = pl.DataFrame({"url": _urls(base, n)})

    async def _build():
        return aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=concurrency))

    session = asyncio.run(_build())
    start = time.monotonic()
    df.with_columns(pl.col("url").api.aget(client=session, max_concurrency=concurrency).alias("body"))
    elapsed = time.monotonic() - start
    asyncio.run(session.close())
    return elapsed


# ---------- runner ----------
BENCHES: list[tuple[str, Callable]] = [
    ("polars-api (default client)", bench_polars_api),
    ("polars-api (shared client)", bench_polars_api_shared_client),
    ("bare httpx (default limits)", bench_httpx_async),
    ("bare httpx (tuned limits)", bench_httpx_async_tuned),
    ("bare aiohttp", bench_aiohttp),
]


def _run_one(fn: Callable, base: str, n: int, concurrency: int) -> float:
    if asyncio.iscoroutinefunction(fn):
        return asyncio.run(fn(base, n, concurrency))
    return fn(base, n, concurrency)


def run_scenario(base: str, n: int, concurrency: int, repeats: int) -> list[dict]:
    rows: list[dict] = []
    print(f"\nN={n}, concurrency={concurrency}, repeats={repeats}")
    for label, fn in BENCHES:
        times = [_run_one(fn, base, n, concurrency) for _ in range(repeats)]
        median = statistics.median(times)
        rps = n / median if median > 0 else float("inf")
        print(
            f"  {label:<30} median={median * 1000:7.1f} ms   rps={rps:8.1f}   "
            f"(min={min(times) * 1000:.1f}, max={max(times) * 1000:.1f})"
        )
        rows.append({
            "label": label,
            "n": n,
            "concurrency": concurrency,
            "repeats": repeats,
            "times_s": times,
            "median_s": median,
            "min_s": min(times),
            "max_s": max(times),
            "rps_median": rps,
        })
    return rows


def _pkg_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "unknown"


def env_info() -> dict:
    uname = platform.uname()
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "python": sys.version.split()[0],
        "platform": f"{uname.system} {uname.release} ({uname.machine})",
        "processor": platform.processor() or uname.machine,
        "cpu_count": mp.cpu_count(),
        "polars": _pkg_version("polars"),
        "polars_api": _pkg_version("polars-api"),
        "httpx": _pkg_version("httpx"),
        "aiohttp": _pkg_version("aiohttp"),
    }


def to_markdown(env: dict, scenarios: list[dict]) -> str:
    lines = [
        "# polars-api benchmarks",
        "",
        f"_Generated {env['timestamp_utc']}_",
        "",
        "## Environment",
        "",
        f"- Python: `{env['python']}`",
        f"- Platform: `{env['platform']}`",
        f"- CPU count: `{env['cpu_count']}`",
        f"- polars: `{env['polars']}`",
        f"- polars-api: `{env['polars_api']}`",
        f"- httpx: `{env['httpx']}`",
        f"- aiohttp: `{env['aiohttp']}`",
        "",
        "## Results",
        "",
        "Local aiohttp echo server on `127.0.0.1`. Median of N repeats.",
        "Higher rps is better.",
        "",
    ]
    for sc in scenarios:
        n = sc["n"]
        c = sc["concurrency"]
        repeats = sc["repeats"]
        lines.append(f"### N={n}, concurrency={c} (repeats={repeats})")
        lines.append("")
        lines.append("| Client | Median (ms) | rps | min (ms) | max (ms) |")
        lines.append("| --- | ---: | ---: | ---: | ---: |")
        for row in sc["rows"]:
            lines.append(
                f"| {row['label']} | {row['median_s'] * 1000:.1f} | "
                f"{row['rps_median']:.1f} | {row['min_s'] * 1000:.1f} | "
                f"{row['max_s'] * 1000:.1f} |"
            )
        lines.append("")
    return "\n".join(lines)


def parse_scenarios(raw: str) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        n_s, c_s = chunk.split("/")
        out.append((int(n_s), int(c_s)))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--scenarios",
        default="100/50,500/100,1000/100,2000/200",
        help="Comma-separated list of N/concurrency pairs (default: %(default)s).",
    )
    parser.add_argument("--repeats", type=int, default=5, help="Repetitions per benchmark (default: %(default)s).")
    parser.add_argument(
        "--output",
        default="benchmarks/results",
        help="Output file stem; .json and .md are written next to it (default: %(default)s).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Server port (0 = pick a free one).",
    )
    args = parser.parse_args()

    scenarios = parse_scenarios(args.scenarios)
    port = args.port or _pick_port()
    env = env_info()

    print("Environment:")
    for k, v in env.items():
        print(f"  {k}: {v}")

    results: list[dict] = []
    with server(port) as base:
        # warmup
        asyncio.run(bench_httpx_async(base, 50, 50))
        for n, c in scenarios:
            rows = run_scenario(base, n, c, args.repeats)
            results.append({"n": n, "concurrency": c, "repeats": args.repeats, "rows": rows})

    out_stem = Path(args.output)
    out_stem.parent.mkdir(parents=True, exist_ok=True)
    out_json = out_stem.with_suffix(".json")
    out_md = out_stem.with_suffix(".md")
    out_json.write_text(json.dumps({"env": env, "scenarios": results}, indent=2))
    out_md.write_text(to_markdown(env, results))
    print(f"\nWrote {out_json} and {out_md}")


if __name__ == "__main__":
    main()
