# polars-api

[![PyPI version](https://img.shields.io/pypi/v/polars-api.svg)](https://pypi.org/project/polars-api/)
[![Python versions](https://img.shields.io/pypi/pyversions/polars-api.svg)](https://pypi.org/project/polars-api/)
[![Release](https://img.shields.io/github/v/release/diegoglozano/polars-api)](https://github.com/diegoglozano/polars-api/releases)
[![Build status](https://img.shields.io/github/actions/workflow/status/diegoglozano/polars-api/main.yml?branch=main)](https://github.com/diegoglozano/polars-api/actions/workflows/main.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/diegoglozano/polars-api/branch/main/graph/badge.svg)](https://codecov.io/gh/diegoglozano/polars-api)
[![License](https://img.shields.io/github/license/diegoglozano/polars-api)](https://github.com/diegoglozano/polars-api/blob/main/LICENSE)

**Call REST APIs from a [Polars](https://pola.rs) DataFrame, one row at a time, using native Polars expressions.**

`polars-api` registers an `.api` namespace on Polars expressions so you can issue HTTP `GET` and `POST` requests for every row of a DataFrame — synchronously or asynchronously — and pipe the responses straight back into your data pipeline.

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
- **Sync and async out of the box** — async variants (`aget` / `apost`) fan out requests with `asyncio.gather` for high-throughput enrichment.
- **Per-row URLs, params, and bodies** — every argument can be a Polars expression, so you can build them from other columns.
- **Powered by [httpx](https://www.python-httpx.org/)** — modern, dependable HTTP client with timeouts and HTTP/2 ready.
- **Tiny surface area** — four methods (`get`, `aget`, `post`, `apost`) you already know how to use.

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

| Method   | HTTP verb | Mode  | Signature                                                                              |
| -------- | --------- | ----- | -------------------------------------------------------------------------------------- |
| `get`    | GET       | sync  | `get(params: pl.Expr \| None = None, timeout: float \| None = None) -> pl.Expr`        |
| `aget`   | GET       | async | `aget(params: pl.Expr \| None = None, timeout: float \| None = None) -> pl.Expr`       |
| `post`   | POST      | sync  | `post(params=None, body: pl.Expr \| None = None, timeout=None) -> pl.Expr`             |
| `apost`  | POST      | async | `apost(params=None, body: pl.Expr \| None = None, timeout=None) -> pl.Expr`            |

Arguments:

- **`params`** — Polars expression that yields a struct of query-string parameters per row. Use `pl.struct(key=...)`.
- **`body`** *(POST only)* — Polars expression that yields a struct serialized as a JSON body per row.
- **`timeout`** — request timeout in seconds (passed to `httpx`).

Each method returns a `pl.Expr` of `Utf8` (the response body as text). Use `.str.json_decode()` to parse JSON responses into a struct column you can `unnest`.

## Tips and patterns

- **Decode JSON immediately**: chain `.str.json_decode()` and then `.struct.unnest()` (or `pl.col("response").struct.field("…")`) to flatten the result.
- **Build URLs from columns**: use Polars string concatenation or `pl.format("https://api.example.com/users/{}", pl.col("user_id"))` to build per-row URLs.
- **Prefer `aget` / `apost` for many rows**: async variants run requests concurrently and are typically much faster for I/O-bound workloads.
- **Inspect failures**: the sync helpers return `null` for non-2xx responses; check for nulls in the resulting column before decoding.

## FAQ

**Can I make HTTP requests from a Polars DataFrame?**
Yes — that is exactly what `polars-api` is for. Import the package and call `.api.get()` / `.api.post()` on a URL column.

**How do I call a REST API for every row of a Polars DataFrame?**
Place the URLs in a column and use `pl.col("url").api.get()` (or `aget` for async). Optional `params` and `body` arguments accept Polars expressions, so they can vary by row.

**Does it support async / concurrent requests?**
Yes. `aget` and `apost` issue requests concurrently with `asyncio.gather`, which is significantly faster than the sync variants when you have more than a handful of rows.

**Is it lazy-frame compatible?**
Yes — because everything is built on Polars expressions, you can use it in `LazyFrame.with_columns(...)` pipelines.

**What does it return?**
A `Utf8` column with the raw response body. Pipe it through `.str.json_decode()` to parse JSON responses.

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](./CONTRIBUTING.md). Please open an issue before starting on larger changes.

## License

[MIT](./LICENSE) © Diego Garcia Lozano

---

Repository initiated with [fpgmaas/cookiecutter-uv](https://github.com/fpgmaas/cookiecutter-uv).
