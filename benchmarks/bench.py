"""Benchmark polars-api (httpx) vs bare httpx vs bare aiohttp.

Spins up a local aiohttp server, then issues N concurrent GETs against it
with each client. Reports wall-clock time and requests/sec. Run multiple
times and average to smooth out noise.
"""
from __future__ import annotations

import asyncio
import multiprocessing as mp
import statistics
import time
from contextlib import contextmanager

import aiohttp
import httpx
import polars as pl

import polars_api  # noqa: F401  - registers the .api namespace

HOST = "127.0.0.1"
PORT = 18080
BASE_URL = f"http://{HOST}:{PORT}"


# ---------- local server ----------
def _serve(port: int) -> None:
    from aiohttp import web

    async def handler(request: web.Request) -> web.Response:
        i = request.match_info.get("i", "0")
        return web.json_response({"i": int(i), "ok": True})

    app = web.Application()
    app.router.add_get("/item/{i}", handler)
    web.run_app(app, host=HOST, port=port, print=None, access_log=None)


@contextmanager
def server():
    proc = mp.Process(target=_serve, args=(PORT,), daemon=True)
    proc.start()
    # wait for readiness
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            with httpx.Client(timeout=0.5) as c:
                if c.get(f"{BASE_URL}/item/0").status_code == 200:
                    break
        except Exception:
            time.sleep(0.05)
    else:
        proc.terminate()
        raise RuntimeError("server failed to start")
    try:
        yield
    finally:
        proc.terminate()
        proc.join(timeout=5)


# ---------- benchmarks ----------
def urls(n: int) -> list[str]:
    return [f"{BASE_URL}/item/{i}" for i in range(n)]


async def bench_httpx_async(n: int, concurrency: int) -> float:
    """Bare httpx with default limits (matches polars-api's default client)."""
    sem = asyncio.Semaphore(concurrency)

    async def one(client: httpx.AsyncClient, url: str) -> str:
        async with sem:
            r = await client.get(url)
            return r.text

    async with httpx.AsyncClient() as client:
        start = time.monotonic()
        await asyncio.gather(*(one(client, u) for u in urls(n)))
        return time.monotonic() - start


async def bench_httpx_async_tuned(n: int, concurrency: int) -> float:
    """Bare httpx with tuned limits (raised keepalive pool to match concurrency)."""
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
        await asyncio.gather(*(one(client, u) for u in urls(n)))
        return time.monotonic() - start


async def bench_aiohttp(n: int, concurrency: int) -> float:
    sem = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=concurrency)

    async def one(session: aiohttp.ClientSession, url: str) -> str:
        async with sem, session.get(url) as r:
            return await r.text()

    async with aiohttp.ClientSession(connector=connector) as session:
        start = time.monotonic()
        await asyncio.gather(*(one(session, u) for u in urls(n)))
        return time.monotonic() - start


def bench_polars_api(n: int, concurrency: int) -> float:
    df = pl.DataFrame({"url": urls(n)})
    start = time.monotonic()
    df.with_columns(pl.col("url").api.aget(max_concurrency=concurrency).alias("body"))
    return time.monotonic() - start


def bench_polars_api_shared_client(n: int, concurrency: int) -> float:
    """Same as polars-api but reusing an AsyncClient across the call (default limits)."""
    client = httpx.AsyncClient()
    df = pl.DataFrame({"url": urls(n)})
    start = time.monotonic()
    df.with_columns(
        pl.col("url").api.aget(client=client, max_concurrency=concurrency).alias("body")
    )
    elapsed = time.monotonic() - start
    asyncio.run(client.aclose())
    return elapsed


# ---------- runner ----------
def run(label: str, fn, n: int, concurrency: int, repeats: int) -> tuple[float, float]:
    times = []
    for _ in range(repeats):
        if asyncio.iscoroutinefunction(fn):
            t = asyncio.run(fn(n, concurrency))
        else:
            t = fn(n, concurrency)
        times.append(t)
    median = statistics.median(times)
    rps = n / median if median > 0 else float("inf")
    print(f"  {label:<30} median={median*1000:7.1f} ms   rps={rps:8.1f}   "
          f"(min={min(times)*1000:.1f}, max={max(times)*1000:.1f})")
    return median, rps


def main() -> None:
    configs = [
        (100, 50),
        (500, 100),
        (1000, 100),
        (2000, 200),
    ]
    repeats = 5

    with server():
        # warmup
        asyncio.run(bench_httpx_async(50, 50))

        for n, c in configs:
            print(f"\nN={n}, concurrency={c}, repeats={repeats}")
            run("polars-api (default client)", bench_polars_api, n, c, repeats)
            run("polars-api (shared client)", bench_polars_api_shared_client, n, c, repeats)
            run("bare httpx (default limits)", bench_httpx_async, n, c, repeats)
            run("bare httpx (tuned limits)", bench_httpx_async_tuned, n, c, repeats)
            run("bare aiohttp", bench_aiohttp, n, c, repeats)


if __name__ == "__main__":
    main()
