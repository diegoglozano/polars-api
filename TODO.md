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
- Shared client / connection pooling: pass `client=httpx.Client(...)` (sync) or `client=aiohttp.ClientSession(...)` (async). The default sync path uses a shared `httpx.Client` per batch instead of opening a connection per row.
- In-batch response caching: `cache=True` memoizes identical `(method, url, params, body, data, headers)` tuples within a batch.
- Form-encoded bodies: `data=` parameter.
- Response headers in metadata: `with_response_headers=True` adds `response_headers: List[Struct{name, value}]` to the metadata struct.
- Lifecycle hooks: `on_request` / `on_response` callbacks. Sync verbs receive `httpx.Request` / `httpx.Response`; async verbs receive `(method, url, kwargs)` / `aiohttp.ClientResponse`.
- Aiohttp-based async path: ~10× higher throughput than httpx at high concurrency.
- Pagination helper: `paginate(...)` follows `Link: rel="next"` by default, accepts a custom `next_url=` callable, and returns `List[Utf8]` of bodies per row.

## Remaining

### Multipart / file uploads (`files=`)

`data=` (form-encoded) is supported; `files=` (multipart uploads) is not. The shape doesn't fit naturally into a Polars row — file uploads typically need `(filename, content_bytes, content_type)` tuples. Worth a focused design pass.
