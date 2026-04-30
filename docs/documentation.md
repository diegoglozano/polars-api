---
title: API reference
description: Full API reference for polars-api — the .api expression namespace registered on Polars.
---

# API reference

`polars-api` registers an `api` namespace on every Polars expression. Import the package once and the namespace becomes available on any expression that resolves to a URL string.

```python
import polars as pl
import polars_api  # noqa: F401  — registers the `.api` namespace
```

## Methods

| Method                                     | HTTP verb | Mode  |
| ------------------------------------------ | --------- | ----- |
| [`get`](#polars_api.api.Api.get)           | GET       | sync  |
| [`aget`](#polars_api.api.Api.aget)         | GET       | async |
| [`post`](#polars_api.api.Api.post)         | POST      | sync  |
| [`apost`](#polars_api.api.Api.apost)       | POST      | async |
| [`put`](#polars_api.api.Api.put)           | PUT       | sync  |
| [`aput`](#polars_api.api.Api.aput)         | PUT       | async |
| [`patch`](#polars_api.api.Api.patch)       | PATCH     | sync  |
| [`apatch`](#polars_api.api.Api.apatch)     | PATCH     | async |
| [`delete`](#polars_api.api.Api.delete)     | DELETE    | sync  |
| [`adelete`](#polars_api.api.Api.adelete)   | DELETE    | async |
| [`head`](#polars_api.api.Api.head)         | HEAD      | sync  |
| [`ahead`](#polars_api.api.Api.ahead)       | HEAD      | async |
| [`request`](#polars_api.api.Api.request)   | any       | sync  |
| [`arequest`](#polars_api.api.Api.arequest) | any       | async |
| [`paginate`](#polars_api.api.Api.paginate) | any       | sync  |

## Return types

By default, every verb method returns a `pl.Expr` of dtype `Utf8` containing the response body for each row. Use `.str.json_decode()` to parse JSON responses.

With `with_metadata=True`, methods return a struct column instead:

```python
{"body": Utf8, "status": Int64, "elapsed_ms": Float64, "error": Utf8}
```

Add `with_response_headers=True` to also include `response_headers: List[Struct{name, value}]` on the struct.

`paginate(...)` returns a `List[Utf8]` — one list of response bodies per starting URL — which you can `.explode(...)` to flatten paginated rows back into the DataFrame.

## Common parameters

All verb wrappers accept the same keyword arguments:

- **`params`** — Polars expression yielding a struct of query-string parameters per row.
- **`body`** _(POST/PUT/PATCH only)_ — Polars expression yielding a struct serialized as a JSON body per row.
- **`data`** — Polars expression yielding a struct serialized as `application/x-www-form-urlencoded`.
- **`headers`** — Polars expression yielding a struct of headers per row.
- **`client`** — preconfigured `httpx.Client` / `httpx.AsyncClient` for connection reuse, HTTP/2, `base_url`, cookies, and custom transports.
- **`timeout`** — request timeout in seconds.
- **`retries`** _(default 0)_ — retry on connection errors, timeouts, 5xx, and 429.
- **`backoff`** _(default 0.0)_ — exponential backoff base in seconds. 429s respect `Retry-After` if present.
- **`max_concurrency`** _(async only)_ — cap on in-flight requests via `asyncio.Semaphore`.
- **`cache`** _(default False)_ — memoize identical `(method, url, params, body, data, headers)` tuples within a batch.
- **`with_metadata`** _(default False)_ — return a metadata struct per row (see above).
- **`with_response_headers`** _(default False)_ — when `with_metadata=True`, also include response headers.
- **`on_error`** _("null" | "raise" | "return")_ — when `with_metadata=False`, what to do on non-2xx / network errors.
- **`on_request`**, **`on_response`** — callables that receive the `httpx.Request` / `httpx.Response`. Useful for logging, metrics, and tracing.
- **`auth=("user", "pass")`** — basic auth.
- **`bearer=pl.col("token")`** — per-row bearer token (also accepts a literal string).
- **`api_key=...`**, **`api_key_header="X-API-Key"`** — shorthand for an API-key header.

## `polars_api.Api`

::: polars_api.api.Api
