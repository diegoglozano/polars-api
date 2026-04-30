# TODO — Feature Ideas

A running list of features to consider for `polars-api`. Ordered roughly by value vs. effort.

## Response metadata (incl. per-row timing)

Today, `get`/`post` return only the response body, and any non-2xx silently becomes `None` (`polars_api/api.py:30`, `polars_api/api.py:42`). Add an opt-in `with_metadata=True` (or similar) that returns a struct per row:

```python
{
    "body":       Utf8,
    "status":     Int32,
    "elapsed_ms": Float64,
    "error":      Utf8,   # null on success, message on failure
}
```

Generalizes the original "return time taken per row" idea and also fixes the silent-failure footgun.

## Request headers (and optional response headers)

- Add a `headers=` parameter accepting a Polars expression that resolves to a struct, so headers can be built per row (auth tokens, `Content-Type`, tenant IDs, etc.).
- When `with_metadata=True`, optionally include response headers (useful for `X-RateLimit-*`, `Link` pagination, `ETag`).

Per-row headers are required for most real-world APIs; today users have no way to send them.

## Retries with backoff

Add `retries: int = 0` and `backoff: float = 0.0` parameters. Retry on:

- Connection errors / timeouts.
- 5xx responses.
- Optionally 429 (respect `Retry-After` if present).

Critical for long batch enrichment jobs against flaky upstreams.

## Concurrency cap for async

`_arequest_many` (`polars_api/api.py:53`) currently fires every row through `asyncio.gather` at once, which DoS's small APIs and trips rate limits. Add `max_concurrency: int | None = None` implemented with an `asyncio.Semaphore`.

## More HTTP verbs

Add `put`, `patch`, `delete`, `head`. `_sync_call`/`_async_call` already take `method` as a string, so this is mostly thin wrappers + tests.

## Auth helpers

Sugar over manual `Authorization` headers:

- `auth=("user", "pass")` — basic auth.
- `bearer=pl.col("token")` — per-row bearer tokens.
- `api_key=...` with configurable header name.

## Shared client / connection pooling

The async path opens a fresh `httpx.AsyncClient` per batch (`polars_api/api.py:53`); the sync path opens a connection **per row** (`polars_api/api.py:29`). Allow callers to pass a preconfigured `httpx.Client` / `httpx.AsyncClient` to enable:

- Connection reuse / keep-alive.
- HTTP/2.
- `base_url`, cookies, default headers, custom transports.
- Mounted retry/proxy transports.

## In-batch response caching

Memoize identical `(method, url, params, body, headers)` tuples within a single batch. Common when the input DataFrame has duplicates after joins/explodes — avoids redundant network calls for free.

## Form-encoded and multipart bodies

Today only JSON bodies are supported (`json=body` at `polars_api/api.py:29`). Add support for:

- `data=` (form-encoded).
- `files=` (multipart uploads).

## Structured error / status handling

Independent of the metadata struct: let the user choose whether non-2xx responses should be `None`, raise, or be returned as-is. Current "silently null" behavior is surprising.

## Pagination helper

Optional helper that follows `Link: rel="next"` (or a user-supplied callable) and explodes paginated results into rows. Niche but very high-leverage when it fits.

## Lifecycle hooks

`on_request` / `on_response` callbacks for logging, metrics, and tracing (e.g. emit OpenTelemetry spans). Cheap to add once we have a shared client.
