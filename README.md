# polars-api

[![PyPI version](https://img.shields.io/pypi/v/polars-api.svg)](https://pypi.org/project/polars-api/)
[![Python versions](https://img.shields.io/pypi/pyversions/polars-api.svg)](https://pypi.org/project/polars-api/)
[![Release](https://img.shields.io/github/v/release/diegoglozano/polars-api)](https://github.com/diegoglozano/polars-api/releases)
[![Build status](https://img.shields.io/github/actions/workflow/status/diegoglozano/polars-api/main.yml?branch=main)](https://github.com/diegoglozano/polars-api/actions/workflows/main.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/diegoglozano/polars-api/branch/main/graph/badge.svg)](https://codecov.io/gh/diegoglozano/polars-api)
[![License](https://img.shields.io/github/license/diegoglozano/polars-api)](https://github.com/diegoglozano/polars-api/blob/main/LICENSE)

**Call REST APIs from a [Polars](https://pola.rs) DataFrame, one row at a time, using native Polars expressions.**

`polars-api` registers an `.api` namespace on Polars expressions so you can issue HTTP requests (`GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `HEAD`) for every row of a DataFrame — synchronously or asynchronously — and pipe the responses straight back into your data pipeline.

```python
import polars as pl
import polars_api  # noqa: F401  — registers the `.api` namespace

(
    pl.DataFrame({"url": ["https://jsonplaceholder.typicode.com/posts/1"]})
      .with_columns(
          pl.col("url").api.get().str.json_decode().alias("response")
      )
)
```

- **Repository**: <https://github.com/diegoglozano/polars-api>
- **Documentation**: <https://diegoglozano.github.io/polars-api/>
- **PyPI**: <https://pypi.org/project/polars-api/>

---

## Why polars-api?

- **Expression-native** — works inside `with_columns`, `select`, and any other Polars expression context. No `for` loops, no manual `apply`.
- **Sync and async out of the box** — async variants (`aget`, `apost`, `aput`, `apatch`, `adelete`, `ahead`) fan out requests with `asyncio.gather` for high-throughput enrichment, with an optional `max_concurrency` cap.
- **Per-row URLs, params, bodies, headers, and auth** — every argument can be a Polars expression, so you can build them from other columns.
- **Production-ready by default** — retries with exponential backoff (incl. `Retry-After`), in-batch response caching, timing/error metadata, lifecycle hooks, and `Link: rel="next"` pagination.
- **Powered by [httpx](https://www.python-httpx.org/)** — modern HTTP client with HTTP/2, connection pooling, and pluggable transports. Bring your own preconfigured `httpx.Client` / `AsyncClient` via `client=...`.

Common use cases:

- Enrich a DataFrame with data from a REST API (geocoding, currency rates, user profiles…).
- Score rows against an ML inference endpoint.
- Hit an internal microservice in batch from a notebook or ETL job.
- Quickly prototype API-driven data pipelines without writing async boilerplate.

## Installation

```sh
# uv
uv add polars-api

# pip
pip install polars-api

# poetry
poetry add polars-api
```

Requires Python 3.9+ and Polars 1.0+.

## Quickstart

### 1. GET request per row

```python
import polars as pl
import polars_api  # noqa: F401

df = (
    pl.DataFrame({"id": [1, 2, 3]})
      .with_columns(
          ("https://jsonplaceholder.typicode.com/posts/" + pl.col("id").cast(pl.Utf8)).alias("url")
      )
      .with_columns(
          pl.col("url").api.get().str.json_decode().alias("response")
      )
)
```

### 2. GET with query parameters

Pass any Polars expression that resolves to a struct as `params`:

```python
df = (
    pl.DataFrame({"url": ["https://jsonplaceholder.typicode.com/posts"] * 3})
      .with_columns(
          pl.struct(userId=pl.Series([1, 2, 3])).alias("params"),
      )
      .with_columns(
          pl.col("url").api.get(params=pl.col("params")).str.json_decode().alias("response")
      )
)
```

### 3. POST with a JSON body

```python
df = (
    pl.DataFrame({"url": ["https://jsonplaceholder.typicode.com/posts"] * 3})
      .with_columns(
          pl.struct(
              title=pl.lit("foo"),
              body=pl.lit("bar"),
              userId=pl.Series([1, 2, 3]),
          ).alias("body"),
      )
      .with_columns(
          pl.col("url").api.post(body=pl.col("body")).str.json_decode().alias("response")
      )
)
```

### 4. Async requests for throughput

`aget` and `apost` use `asyncio.gather` under the hood, so requests run concurrently per batch:

```python
df = (
    pl.DataFrame({"url": ["https://jsonplaceholder.typicode.com/posts"] * 100})
      .with_columns(
          pl.col("url").api.aget().str.json_decode().alias("response")
      )
)
```

### 5. Timeouts

Every method accepts a `timeout` (in seconds), forwarded to `httpx`:

```python
pl.col("url").api.get(timeout=5.0)
pl.col("url").api.apost(body=pl.col("body"), timeout=10.0)
```

## API reference

All methods live under the `.api` namespace on any Polars expression that resolves to a URL string.

| Method               | HTTP verb | Mode         |
| -------------------- | --------- | ------------ |
| `get` / `aget`       | GET       | sync / async |
| `post` / `apost`     | POST      | sync / async |
| `put` / `aput`       | PUT       | sync / async |
| `patch` / `apatch`   | PATCH     | sync / async |
| `delete` / `adelete` | DELETE    | sync / async |
| `head` / `ahead`     | HEAD      | sync / async |

Arguments (all keyword-only after the positional `params` / `body`):

- **`params`** — Polars expression yielding a struct of query-string parameters per row.
- **`body`** _(POST/PUT/PATCH only)_ — Polars expression yielding a struct serialized as a JSON body per row.
- **`data`** — Polars expression yielding a struct serialized as `application/x-www-form-urlencoded`.
- **`headers`** — Polars expression yielding a struct of headers per row (e.g. tenant IDs, custom auth).
- **`client`** — preconfigured `httpx.Client` / `httpx.AsyncClient` to enable connection reuse, HTTP/2, `base_url`, cookies, and custom transports.
- **`timeout`** — request timeout in seconds.
- **`retries`** _(int, default 0)_ — retry on connection errors, timeouts, 5xx, and 429.
- **`backoff`** _(float, default 0.0)_ — exponential backoff base (seconds). 429s respect `Retry-After` if present.
- **`max_concurrency`** _(async only)_ — cap on in-flight requests via `asyncio.Semaphore`.
- **`cache`** _(bool, default False)_ — memoize identical `(method, url, params, body, data, headers)` tuples within a batch.
- **`with_metadata`** _(bool, default False)_ — return a struct `{body, status, elapsed_ms, error}` per row instead of just the body.
- **`with_response_headers`** _(bool, default False)_ — when `with_metadata=True`, also include `response_headers: List[Struct{name, value}]` on the struct.
- **`on_error`** _("null" | "raise" | "return")_ — when `with_metadata=False`, what to do on non-2xx / network errors.
- **`on_request`**, **`on_response`** — callables that receive the `httpx.Request` / `httpx.Response`. Useful for logging, metrics, and tracing.
- **`auth=("user", "pass")`** — basic auth.
- **`bearer=pl.col("token")`** — per-row bearer token (also accepts a literal string).
- **`api_key=...`**, **`api_key_header="X-API-Key"`** — shorthand for an API-key header.

By default, each method returns a `pl.Expr` of `Utf8`. With `with_metadata=True`, it returns a struct column with the schema:

```python
{"body": Utf8, "status": Int64, "elapsed_ms": Float64, "error": Utf8}
```

Use `.str.json_decode()` to parse JSON responses.

### Examples

```python
# Per-row bearer auth + retries + concurrency cap
pl.col("url").api.aget(
    bearer=pl.col("token"),
    retries=3,
    backoff=0.5,
    max_concurrency=10,
)

# Inspect status, timing, errors and response headers
pl.col("url").api.get(with_metadata=True, with_response_headers=True)

# Bring your own client (HTTP/2, keep-alive, base_url, etc.)
client = httpx.AsyncClient(http2=True, base_url="https://api.example.com")
pl.col("path").api.aget(client=client)

# Skip duplicate URLs within a batch (e.g. after a join/explode)
pl.col("url").api.aget(cache=True)

# Follow Link: rel="next" pagination
df.with_columns(
    pl.col("url").api.paginate(max_pages=20).alias("pages")
).explode("pages")
```

## Tips and patterns

- **Decode JSON immediately**: chain `.str.json_decode()` and then `.struct.unnest()` (or `pl.col("response").struct.field("…")`) to flatten the result.
- **Build URLs from columns**: use Polars string concatenation or `pl.format("https://api.example.com/users/{}", pl.col("user_id"))` to build per-row URLs.
- **Prefer `aget` / `apost` for many rows**: async variants run requests concurrently and are typically much faster for I/O-bound workloads.
- **Inspect failures**: by default, non-2xx responses become `null`. Use `with_metadata=True` to keep the body, status, elapsed time and error string, or `on_error="raise"` / `on_error="return"` to change null-on-failure behavior.

## FAQ

**Can I make HTTP requests from a Polars DataFrame?**
Yes — that is exactly what `polars-api` is for. Import the package and call `.api.get()` / `.api.post()` on a URL column.

**How do I call a REST API for every row of a Polars DataFrame?**
Place the URLs in a column and use `pl.col("url").api.get()` (or `aget` for async). Optional `params` and `body` arguments accept Polars expressions, so they can vary by row.

**Does it support async / concurrent requests?**
Yes. The async variants (`aget`, `apost`, `aput`, `apatch`, `adelete`, `ahead`) issue requests concurrently with `asyncio.gather`, which is significantly faster than the sync variants when you have more than a handful of rows. Use `max_concurrency=N` to cap in-flight requests.

**Is it lazy-frame compatible?**
Yes — because everything is built on Polars expressions, you can use it in `LazyFrame.with_columns(...)` pipelines.

**What does it return?**
By default, a `Utf8` column with the raw response body — pipe it through `.str.json_decode()` to parse JSON responses. Pass `with_metadata=True` to get a struct `{body, status, elapsed_ms, error}` per row instead. `paginate(...)` returns a `List[Utf8]` of bodies that you can `.explode(...)` to flatten paginated results.

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](./CONTRIBUTING.md). Please open an issue before starting on larger changes.

## License

[MIT](./LICENSE) © Diego Garcia Lozano

---

Repository initiated with [fpgmaas/cookiecutter-uv](https://github.com/fpgmaas/cookiecutter-uv).
