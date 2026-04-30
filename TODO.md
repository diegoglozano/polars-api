# TODO — Feature Ideas

A running list of features to consider for `polars-api`. Ordered roughly by value vs. effort.

## ✅ Done

- Response metadata struct (`with_metadata=True` → `{body, status, elapsed_ms, error}`).
- Per-row request `headers=` parameter.
- Retries with backoff (connection errors / timeouts / 5xx / 429 with `Retry-After`).
- `max_concurrency` cap for the async path via `asyncio.Semaphore`.
- More verbs: `put` / `patch` / `delete` / `head` (and async siblings).
- Auth helpers: `auth=("user", "pass")`, `bearer=...`, `api_key=...` with `api_key_header=...`.
- Structured error handling: `on_error="null" | "raise" | "return"`.

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
