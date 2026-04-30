---
title: polars-api — REST API calls from Polars DataFrames
description: Call REST APIs from a Polars DataFrame, one row at a time, using native Polars expressions. Sync and async GET/POST/PUT/PATCH/DELETE/HEAD with per-row URLs, params, bodies, headers, auth, retries, caching, hooks, and pagination.
---

# polars-api

[![PyPI version](https://img.shields.io/pypi/v/polars-api.svg)](https://pypi.org/project/polars-api/)
[![Python versions](https://img.shields.io/pypi/pyversions/polars-api.svg)](https://pypi.org/project/polars-api/)
[![Release](https://img.shields.io/github/v/release/diegoglozano/polars-api)](https://github.com/diegoglozano/polars-api/releases)
[![Build status](https://img.shields.io/github/actions/workflow/status/diegoglozano/polars-api/main.yml?branch=main)](https://github.com/diegoglozano/polars-api/actions/workflows/main.yml?query=branch%3Amain)
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

## Why polars-api?

- **Expression-native** — works inside `with_columns`, `select`, and any other Polars expression context.
- **Sync and async** — six verbs each, fanning out concurrent requests with `asyncio.gather` (and an optional `max_concurrency` cap).
- **Per-row everything** — URLs, params, JSON / form bodies, headers, and auth tokens can each be a Polars expression.
- **Production-ready** — retries with exponential backoff (incl. `Retry-After`), in-batch response caching, timing / error metadata, lifecycle hooks, and `Link: rel="next"` pagination.
- **Powered by [httpx](https://www.python-httpx.org/)** — bring your own preconfigured `httpx.Client` / `AsyncClient` for HTTP/2, connection pooling, `base_url`, cookies, and custom transports.

## Install

```sh
pip install polars-api
# or: uv add polars-api
# or: poetry add polars-api
```

Requires Python 3.9+ and Polars 1.0+.

## Quickstart

### GET request per row

```python
import polars as pl
import polars_api  # noqa: F401

(
    pl.DataFrame({"id": [1, 2, 3]})
      .with_columns(
          ("https://jsonplaceholder.typicode.com/posts/" + pl.col("id").cast(pl.Utf8)).alias("url")
      )
      .with_columns(
          pl.col("url").api.get().str.json_decode().alias("response")
      )
)
```

### POST with a JSON body

```python
(
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

### Async for throughput

```python
pl.col("url").api.aget()                              # concurrent GET
pl.col("url").api.apost(body=pl.col("body"))          # concurrent POST
pl.col("url").api.aget(max_concurrency=10)            # cap in-flight requests
```

### Per-row headers and auth

```python
pl.col("url").api.aget(
    headers=pl.struct(pl.col("tenant").alias("X-Tenant")),
    bearer=pl.col("token"),     # per-row Authorization: Bearer ...
    retries=3,
    backoff=0.5,
)
```

### Inspect status, timing, and errors

```python
pl.col("url").api.get(with_metadata=True, with_response_headers=True)
# → struct {body, status, elapsed_ms, error, response_headers}
```

### Bring your own client (HTTP/2, base_url, keep-alive)

```python
import httpx
client = httpx.AsyncClient(http2=True, base_url="https://api.example.com")
pl.col("path").api.aget(client=client)
```

### Skip duplicate requests within a batch

```python
pl.col("url").api.aget(cache=True)   # memoize identical (method, url, params, body, data, headers)
```

### Follow `Link: rel="next"` pagination

```python
df.with_columns(
    pl.col("url").api.paginate(max_pages=20).alias("pages")
).explode("pages")
```

## Methods

| Method               | HTTP verb | Mode         |
| -------------------- | --------- | ------------ |
| `get` / `aget`       | GET       | sync / async |
| `post` / `apost`     | POST      | sync / async |
| `put` / `aput`       | PUT       | sync / async |
| `patch` / `apatch`   | PATCH     | sync / async |
| `delete` / `adelete` | DELETE    | sync / async |
| `head` / `ahead`     | HEAD      | sync / async |
| `request`            | any       | sync         |
| `arequest`           | any       | async        |
| `paginate`           | any       | sync         |

All verb wrappers accept the same keyword arguments: `params`, `body`, `data`, `headers`, `client`, `timeout`, `retries`, `backoff`, `max_concurrency` (async only), `cache`, `with_metadata`, `with_response_headers`, `on_error`, `on_request`, `on_response`, `auth`, `bearer`, `api_key`, `api_key_header`. See the full [API reference](documentation.md) for the parameter list and semantics.

## Links

- **GitHub**: <https://github.com/diegoglozano/polars-api>
- **PyPI**: <https://pypi.org/project/polars-api/>
- **Issues**: <https://github.com/diegoglozano/polars-api/issues>
