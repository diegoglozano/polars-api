---
title: polars-api — REST API calls from Polars DataFrames
description: Call REST APIs from a Polars DataFrame, one row at a time, using native Polars expressions. Supports sync and async GET/POST with per-row URLs, params, and bodies.
---

<p align="center">
  <img src="assets/logo.svg" alt="polars-api" width="180" />
</p>

# polars-api

[![PyPI version](https://img.shields.io/pypi/v/polars-api.svg)](https://pypi.org/project/polars-api/)
[![Python versions](https://img.shields.io/pypi/pyversions/polars-api.svg)](https://pypi.org/project/polars-api/)
[![Release](https://img.shields.io/github/v/release/diegoglozano/polars-api)](https://github.com/diegoglozano/polars-api/releases)
[![Build status](https://img.shields.io/github/actions/workflow/status/diegoglozano/polars-api/main.yml?branch=main)](https://github.com/diegoglozano/polars-api/actions/workflows/main.yml?query=branch%3Amain)
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

## Why polars-api?

- **Expression-native** — works inside `with_columns`, `select`, and any other Polars expression context.
- **Sync and async** — async variants (`aget` / `apost`) fan out requests with `asyncio.gather` for high-throughput enrichment.
- **Per-row URLs, params, and bodies** — every argument can be a Polars expression.
- **Powered by [httpx](https://www.python-httpx.org/)** — modern HTTP client with timeouts.
- **Tiny surface area** — four methods you already know how to use.

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
pl.col("url").api.aget()                  # concurrent GET
pl.col("url").api.apost(body=pl.col("body"))  # concurrent POST
```

## Methods

| Method  | HTTP verb | Mode  |
| ------- | --------- | ----- |
| `get`   | GET       | sync  |
| `aget`  | GET       | async |
| `post`  | POST      | sync  |
| `apost` | POST      | async |

All methods accept optional `params` (struct expression for query string), `timeout` (seconds), and POST methods additionally accept `body` (struct expression serialized as JSON). Each returns a `Utf8` expression with the response body — pipe it through `.str.json_decode()` to parse JSON.

See the full [API reference](documentation.md).

## Links

- **GitHub**: <https://github.com/diegoglozano/polars-api>
- **PyPI**: <https://pypi.org/project/polars-api/>
- **Issues**: <https://github.com/diegoglozano/polars-api/issues>
