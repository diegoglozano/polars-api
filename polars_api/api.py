import asyncio
import base64
import re
import time
from typing import Any, Callable, Optional, Union

import httpx
import nest_asyncio
import polars as pl

OnError = str  # "null" | "raise" | "return"
RequestHook = Callable[[httpx.Request], None]
ResponseHook = Callable[[httpx.Response], None]
NextUrl = Callable[[httpx.Response], Optional[str]]

_BASE_METADATA_FIELDS: dict[str, Any] = {
    "body": pl.Utf8,
    "status": pl.Int64,
    "elapsed_ms": pl.Float64,
    "error": pl.Utf8,
}

_RESPONSE_HEADERS_DTYPE = pl.List(pl.Struct({"name": pl.Utf8, "value": pl.Utf8}))


def _metadata_dtype(with_response_headers: bool) -> pl.Struct:
    fields = dict(_BASE_METADATA_FIELDS)
    if with_response_headers:
        fields["response_headers"] = _RESPONSE_HEADERS_DTYPE
    return pl.Struct(fields)


def _arun(coro: Any) -> Any:
    nest_asyncio.apply()
    return asyncio.run(coro)


def _basic_auth_header(user: str, password: str) -> str:
    token = base64.b64encode(f"{user}:{password}".encode()).decode("ascii")
    return f"Basic {token}"


def _retry_after_seconds(response: httpx.Response) -> Optional[float]:
    raw = response.headers.get("retry-after")
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _is_retryable_status(status: int) -> bool:
    return status >= 500 or status == 429


def _serialize_response_headers(response: httpx.Response) -> list[dict[str, str]]:
    return [{"name": k, "value": v} for k, v in response.headers.items()]


def _result_struct(
    body: Optional[str],
    status: int,
    elapsed_ms: float,
    error: Optional[str],
    response_headers: Optional[list[dict[str, str]]] = None,
    *,
    include_response_headers: bool = False,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "body": body,
        "status": status,
        "elapsed_ms": elapsed_ms,
        "error": error,
    }
    if include_response_headers:
        out["response_headers"] = response_headers or []
    return out


def _coerce_body(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _hashable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool, bytes)):
        return value
    if isinstance(value, dict):
        return ("__d__", *sorted((k, _hashable(v)) for k, v in value.items()))
    if isinstance(value, (list, tuple)):
        return ("__l__", *(_hashable(v) for v in value))
    return repr(value)


def _parse_link_next(link_header: Optional[str]) -> Optional[str]:
    if not link_header:
        return None
    for part in link_header.split(","):
        if 'rel="next"' in part or "rel=next" in part:
            match = re.match(r"\s*<([^>]+)>", part)
            if match:
                return match.group(1)
    return None


def _resolve_output(
    result: dict[str, Any],
    *,
    with_metadata: bool,
    on_error: OnError,
) -> Any:
    if with_metadata:
        return result
    if result["error"] is None:
        return result["body"]
    if on_error == "raise":
        raise RuntimeError(result["error"])
    if on_error == "return":
        return result["body"]
    return None


def _build_request_kwargs(
    params: Optional[dict[str, Any]],
    body: Optional[dict[str, Any]],
    data: Optional[dict[str, Any]],
    headers: Optional[dict[str, Any]],
    timeout: Optional[float],
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {"params": params, "headers": headers}
    if body is not None:
        kwargs["json"] = body
    if data is not None:
        kwargs["data"] = data
    if timeout is not None:
        kwargs["timeout"] = timeout
    return kwargs


@pl.api.register_expr_namespace("api")
class Api:
    def __init__(self, url: pl.Expr) -> None:
        self._url = url

    @staticmethod
    def _send_sync(
        client: httpx.Client,
        method: str,
        url: str,
        params: Optional[dict[str, Any]],
        body: Optional[dict[str, Any]],
        data: Optional[dict[str, Any]],
        headers: Optional[dict[str, Any]],
        timeout: Optional[float],
        on_request: Optional[RequestHook],
        on_response: Optional[ResponseHook],
    ) -> httpx.Response:
        kwargs = _build_request_kwargs(params, body, data, headers, timeout)
        request = client.build_request(method, url, **kwargs)
        if on_request is not None:
            on_request(request)
        response = client.send(request)
        if on_response is not None:
            on_response(response)
        return response

    @staticmethod
    async def _send_async(
        client: httpx.AsyncClient,
        method: str,
        url: str,
        params: Optional[dict[str, Any]],
        body: Optional[dict[str, Any]],
        data: Optional[dict[str, Any]],
        headers: Optional[dict[str, Any]],
        timeout: Optional[float],
        on_request: Optional[RequestHook],
        on_response: Optional[ResponseHook],
    ) -> httpx.Response:
        kwargs = _build_request_kwargs(params, body, data, headers, timeout)
        request = client.build_request(method, url, **kwargs)
        if on_request is not None:
            on_request(request)
        response = await client.send(request)
        if on_response is not None:
            on_response(response)
        return response

    @classmethod
    def _sync_one(
        cls,
        client: httpx.Client,
        method: str,
        url: str,
        params: Optional[dict[str, Any]],
        body: Optional[dict[str, Any]],
        data: Optional[dict[str, Any]],
        headers: Optional[dict[str, Any]],
        timeout: Optional[float],
        retries: int,
        backoff: float,
        with_response_headers: bool,
        on_request: Optional[RequestHook],
        on_response: Optional[ResponseHook],
    ) -> dict[str, Any]:
        attempt = 0
        start = time.monotonic()
        while True:
            attempt_start = time.monotonic()
            try:
                response = cls._send_sync(
                    client,
                    method,
                    url,
                    params,
                    body,
                    data,
                    headers,
                    timeout,
                    on_request,
                    on_response,
                )
            except httpx.HTTPError as exc:
                if attempt < retries:
                    wait = backoff * (2**attempt) if backoff > 0 else 0.0
                    if wait > 0:
                        time.sleep(wait)
                    attempt += 1
                    continue
                elapsed_ms = (time.monotonic() - attempt_start) * 1000
                return _result_struct(
                    None,
                    0,
                    elapsed_ms,
                    f"{type(exc).__name__}: {exc}",
                    include_response_headers=with_response_headers,
                )

            status = response.status_code
            text = response.text
            resp_headers = _serialize_response_headers(response) if with_response_headers else None
            if response.is_success:
                elapsed_ms = (time.monotonic() - start) * 1000
                return _result_struct(
                    text,
                    status,
                    elapsed_ms,
                    None,
                    resp_headers,
                    include_response_headers=with_response_headers,
                )
            if attempt < retries and _is_retryable_status(status):
                wait = _retry_after_seconds(response)
                if wait is None:
                    wait = backoff * (2**attempt) if backoff > 0 else 0.0
                if wait > 0:
                    time.sleep(wait)
                attempt += 1
                continue
            elapsed_ms = (time.monotonic() - start) * 1000
            return _result_struct(
                text,
                status,
                elapsed_ms,
                f"HTTP {status}",
                resp_headers,
                include_response_headers=with_response_headers,
            )

    @classmethod
    async def _async_attempt(
        cls,
        client: httpx.AsyncClient,
        method: str,
        url: str,
        params: Optional[dict[str, Any]],
        body: Optional[dict[str, Any]],
        data: Optional[dict[str, Any]],
        headers: Optional[dict[str, Any]],
        timeout: Optional[float],
        attempt: int,
        retries: int,
        backoff: float,
        start: float,
        with_response_headers: bool,
        on_request: Optional[RequestHook],
        on_response: Optional[ResponseHook],
    ) -> tuple[Optional[dict[str, Any]], Optional[float]]:
        attempt_start = time.monotonic()
        try:
            response = await cls._send_async(
                client,
                method,
                url,
                params,
                body,
                data,
                headers,
                timeout,
                on_request,
                on_response,
            )
        except httpx.HTTPError as exc:
            if attempt < retries:
                wait = backoff * (2**attempt) if backoff > 0 else 0.0
                return None, wait
            elapsed_ms = (time.monotonic() - attempt_start) * 1000
            return _result_struct(
                None,
                0,
                elapsed_ms,
                f"{type(exc).__name__}: {exc}",
                include_response_headers=with_response_headers,
            ), None

        status = response.status_code
        text = response.text
        resp_headers = _serialize_response_headers(response) if with_response_headers else None
        if response.is_success:
            elapsed_ms = (time.monotonic() - start) * 1000
            return _result_struct(
                text,
                status,
                elapsed_ms,
                None,
                resp_headers,
                include_response_headers=with_response_headers,
            ), None
        if attempt < retries and _is_retryable_status(status):
            wait = _retry_after_seconds(response)
            if wait is None:
                wait = backoff * (2**attempt) if backoff > 0 else 0.0
            return None, wait
        elapsed_ms = (time.monotonic() - start) * 1000
        return _result_struct(
            text,
            status,
            elapsed_ms,
            f"HTTP {status}",
            resp_headers,
            include_response_headers=with_response_headers,
        ), None

    @classmethod
    async def _async_one(
        cls,
        client: httpx.AsyncClient,
        semaphore: Optional[asyncio.Semaphore],
        method: str,
        url: str,
        params: Optional[dict[str, Any]],
        body: Optional[dict[str, Any]],
        data: Optional[dict[str, Any]],
        headers: Optional[dict[str, Any]],
        timeout: Optional[float],
        retries: int,
        backoff: float,
        with_response_headers: bool,
        on_request: Optional[RequestHook],
        on_response: Optional[ResponseHook],
    ) -> dict[str, Any]:
        async def _go() -> dict[str, Any]:
            attempt = 0
            start = time.monotonic()
            while True:
                result, wait = await cls._async_attempt(
                    client,
                    method,
                    url,
                    params,
                    body,
                    data,
                    headers,
                    timeout,
                    attempt,
                    retries,
                    backoff,
                    start,
                    with_response_headers,
                    on_request,
                    on_response,
                )
                if result is not None:
                    return result
                if wait and wait > 0:
                    await asyncio.sleep(wait)
                attempt += 1

        if semaphore is None:
            return await _go()
        async with semaphore:
            return await _go()

    @classmethod
    def _send_sync_with_retries(
        cls,
        client: httpx.Client,
        method: str,
        url: str,
        params: Optional[dict[str, Any]],
        body: Optional[dict[str, Any]],
        data: Optional[dict[str, Any]],
        headers: Optional[dict[str, Any]],
        timeout: Optional[float],
        retries: int,
        backoff: float,
        on_request: Optional[RequestHook],
        on_response: Optional[ResponseHook],
    ) -> Optional[httpx.Response]:
        """Like ``_send_sync`` but retries on failure. Returns the last response, or None
        if every attempt failed with a network error."""
        attempt = 0
        while True:
            try:
                response = cls._send_sync(
                    client,
                    method,
                    url,
                    params,
                    body,
                    data,
                    headers,
                    timeout,
                    on_request,
                    on_response,
                )
            except httpx.HTTPError:
                if attempt < retries:
                    wait = backoff * (2**attempt) if backoff > 0 else 0.0
                    if wait > 0:
                        time.sleep(wait)
                    attempt += 1
                    continue
                return None
            if response.is_success:
                return response
            if attempt < retries and _is_retryable_status(response.status_code):
                wait = _retry_after_seconds(response)
                if wait is None:
                    wait = backoff * (2**attempt) if backoff > 0 else 0.0
                if wait > 0:
                    time.sleep(wait)
                attempt += 1
                continue
            return None

    @classmethod
    def _sync_batch(
        cls,
        method: str,
        rows: list[tuple[str, Any, Any, Any, Any]],
        *,
        client: Optional[httpx.Client],
        timeout: Optional[float],
        retries: int,
        backoff: float,
        cache: bool,
        with_response_headers: bool,
        on_request: Optional[RequestHook],
        on_response: Optional[ResponseHook],
    ) -> list[dict[str, Any]]:
        own_client = client is None
        cli: httpx.Client = httpx.Client() if own_client else client  # type: ignore[assignment]
        results: list[Optional[dict[str, Any]]] = [None] * len(rows)
        memo: dict[Any, dict[str, Any]] = {}
        try:
            for i, (url, params, body, data, headers) in enumerate(rows):
                key = None
                if cache:
                    key = (method, url, _hashable(params), _hashable(body), _hashable(data), _hashable(headers))
                    if key in memo:
                        results[i] = memo[key]
                        continue
                result = cls._sync_one(
                    cli,
                    method,
                    url,
                    params,
                    body,
                    data,
                    headers,
                    timeout,
                    retries,
                    backoff,
                    with_response_headers,
                    on_request,
                    on_response,
                )
                if cache and key is not None:
                    memo[key] = result
                results[i] = result
        finally:
            if own_client:
                cli.close()
        return [r for r in results if r is not None]

    @classmethod
    async def _async_many(
        cls,
        method: str,
        rows: list[tuple[str, Any, Any, Any, Any]],
        *,
        client: Optional[httpx.AsyncClient],
        timeout: Optional[float],
        retries: int,
        backoff: float,
        max_concurrency: Optional[int],
        cache: bool,
        with_response_headers: bool,
        on_request: Optional[RequestHook],
        on_response: Optional[ResponseHook],
    ) -> list[dict[str, Any]]:
        semaphore = asyncio.Semaphore(max_concurrency) if max_concurrency else None

        # Dedupe identical rows up-front so we only fire one task per unique key.
        unique_indices: dict[Any, int] = {}
        order: list[int] = []  # index into rows for each unique task
        result_index: list[int] = [0] * len(rows)  # row -> position in tasks list
        for i, (url, params, body, data, headers) in enumerate(rows):
            if cache:
                key = (method, url, _hashable(params), _hashable(body), _hashable(data), _hashable(headers))
                if key in unique_indices:
                    result_index[i] = unique_indices[key]
                    continue
                unique_indices[key] = len(order)
                result_index[i] = len(order)
                order.append(i)
            else:
                result_index[i] = len(order)
                order.append(i)

        own_client = client is None
        cli: httpx.AsyncClient = httpx.AsyncClient() if own_client else client  # type: ignore[assignment]
        try:
            tasks = [
                cls._async_one(
                    cli,
                    semaphore,
                    method,
                    rows[idx][0],
                    rows[idx][1],
                    rows[idx][2],
                    rows[idx][3],
                    rows[idx][4],
                    timeout,
                    retries,
                    backoff,
                    with_response_headers,
                    on_request,
                    on_response,
                )
                for idx in order
            ]
            unique_results = await asyncio.gather(*tasks)
        finally:
            if own_client:
                await cli.aclose()
        return [unique_results[result_index[i]] for i in range(len(rows))]

    @staticmethod
    def _results_to_series(
        results: list[dict[str, Any]],
        *,
        with_metadata: bool,
        with_response_headers: bool,
        on_error: OnError,
    ) -> pl.Series:
        if with_metadata:
            return pl.Series(results, dtype=_metadata_dtype(with_response_headers))
        out: list[Optional[str]] = []
        for r in results:
            if r["error"] is None:
                out.append(_coerce_body(r["body"]))
            elif on_error == "raise":
                raise RuntimeError(r["error"])
            elif on_error == "return":
                out.append(_coerce_body(r["body"]))
            else:
                out.append(None)
        return pl.Series(out, dtype=pl.Utf8)

    @staticmethod
    def _rows_from_struct(s: pl.Series) -> list[tuple[str, Any, Any, Any, Any]]:
        urls = s.struct.field("url").to_list()
        params = s.struct.field("params").to_list()
        bodies = s.struct.field("body").to_list()
        data = s.struct.field("data").to_list()
        headers = s.struct.field("headers").to_list()
        return list(zip(urls, params, bodies, data, headers))

    def _build_headers_expr(
        self,
        headers: Optional[pl.Expr],
        auth: Optional[tuple[str, str]],
        bearer: Optional[Union[str, pl.Expr]],
        api_key: Optional[Union[str, pl.Expr]],
        api_key_header: str,
    ) -> Optional[pl.Expr]:
        extras: dict[str, pl.Expr] = {}
        if auth is not None:
            user, password = auth
            extras["Authorization"] = pl.lit(_basic_auth_header(user, password))
        if bearer is not None:
            bearer_expr = bearer if isinstance(bearer, pl.Expr) else pl.lit(bearer)
            extras["Authorization"] = pl.lit("Bearer ") + bearer_expr.cast(pl.Utf8)
        if api_key is not None:
            api_key_expr = api_key if isinstance(api_key, pl.Expr) else pl.lit(api_key)
            extras[api_key_header] = api_key_expr.cast(pl.Utf8)

        if not extras and headers is None:
            return None
        if not extras:
            return headers
        extra_exprs = [expr.alias(name) for name, expr in extras.items()]
        if headers is None:
            return pl.struct(*extra_exprs)
        return headers.struct.with_fields(*extra_exprs)

    def _input_struct(
        self,
        params: Optional[pl.Expr],
        body: Optional[pl.Expr],
        data: Optional[pl.Expr],
        headers: Optional[pl.Expr],
    ) -> pl.Expr:
        return pl.struct(
            self._url.alias("url"),
            (pl.lit(None) if params is None else params).alias("params"),
            (pl.lit(None) if body is None else body).alias("body"),
            (pl.lit(None) if data is None else data).alias("data"),
            (pl.lit(None) if headers is None else headers).alias("headers"),
        )

    def _sync_call(
        self,
        method: str,
        params: Optional[pl.Expr],
        body: Optional[pl.Expr],
        data: Optional[pl.Expr],
        headers: Optional[pl.Expr],
        *,
        client: Optional[httpx.Client],
        timeout: Optional[float],
        retries: int,
        backoff: float,
        cache: bool,
        with_metadata: bool,
        with_response_headers: bool,
        on_error: OnError,
        on_request: Optional[RequestHook],
        on_response: Optional[ResponseHook],
    ) -> pl.Expr:
        return_dtype = _metadata_dtype(with_response_headers) if with_metadata else pl.Utf8
        return self._input_struct(params, body, data, headers).map_batches(
            lambda s: self._results_to_series(
                self._sync_batch(
                    method,
                    self._rows_from_struct(s),
                    client=client,
                    timeout=timeout,
                    retries=retries,
                    backoff=backoff,
                    cache=cache,
                    with_response_headers=with_response_headers,
                    on_request=on_request,
                    on_response=on_response,
                ),
                with_metadata=with_metadata,
                with_response_headers=with_response_headers,
                on_error=on_error,
            ),
            return_dtype=return_dtype,
        )

    def _async_call(
        self,
        method: str,
        params: Optional[pl.Expr],
        body: Optional[pl.Expr],
        data: Optional[pl.Expr],
        headers: Optional[pl.Expr],
        *,
        client: Optional[httpx.AsyncClient],
        timeout: Optional[float],
        retries: int,
        backoff: float,
        max_concurrency: Optional[int],
        cache: bool,
        with_metadata: bool,
        with_response_headers: bool,
        on_error: OnError,
        on_request: Optional[RequestHook],
        on_response: Optional[ResponseHook],
    ) -> pl.Expr:
        return_dtype = _metadata_dtype(with_response_headers) if with_metadata else pl.Utf8
        return self._input_struct(params, body, data, headers).map_batches(
            lambda s: self._results_to_series(
                _arun(
                    self._async_many(
                        method,
                        self._rows_from_struct(s),
                        client=client,
                        timeout=timeout,
                        retries=retries,
                        backoff=backoff,
                        max_concurrency=max_concurrency,
                        cache=cache,
                        with_response_headers=with_response_headers,
                        on_request=on_request,
                        on_response=on_response,
                    )
                ),
                with_metadata=with_metadata,
                with_response_headers=with_response_headers,
                on_error=on_error,
            ),
            return_dtype=return_dtype,
        )

    # ---- Public API: full request() / arequest() entry points ----

    def request(
        self,
        method: str,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        *,
        data: Optional[pl.Expr] = None,
        headers: Optional[pl.Expr] = None,
        client: Optional[httpx.Client] = None,
        timeout: Optional[float] = None,
        retries: int = 0,
        backoff: float = 0.0,
        cache: bool = False,
        with_metadata: bool = False,
        with_response_headers: bool = False,
        on_error: OnError = "null",
        on_request: Optional[RequestHook] = None,
        on_response: Optional[ResponseHook] = None,
        auth: Optional[tuple[str, str]] = None,
        bearer: Optional[Union[str, pl.Expr]] = None,
        api_key: Optional[Union[str, pl.Expr]] = None,
        api_key_header: str = "X-API-Key",
    ) -> pl.Expr:
        """Issue a synchronous HTTP request per row."""
        merged_headers = self._build_headers_expr(headers, auth, bearer, api_key, api_key_header)
        return self._sync_call(
            method.upper(),
            params,
            body,
            data,
            merged_headers,
            client=client,
            timeout=timeout,
            retries=retries,
            backoff=backoff,
            cache=cache,
            with_metadata=with_metadata,
            with_response_headers=with_response_headers,
            on_error=on_error,
            on_request=on_request,
            on_response=on_response,
        )

    def arequest(
        self,
        method: str,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        *,
        data: Optional[pl.Expr] = None,
        headers: Optional[pl.Expr] = None,
        client: Optional[httpx.AsyncClient] = None,
        timeout: Optional[float] = None,
        retries: int = 0,
        backoff: float = 0.0,
        max_concurrency: Optional[int] = None,
        cache: bool = False,
        with_metadata: bool = False,
        with_response_headers: bool = False,
        on_error: OnError = "null",
        on_request: Optional[RequestHook] = None,
        on_response: Optional[ResponseHook] = None,
        auth: Optional[tuple[str, str]] = None,
        bearer: Optional[Union[str, pl.Expr]] = None,
        api_key: Optional[Union[str, pl.Expr]] = None,
        api_key_header: str = "X-API-Key",
    ) -> pl.Expr:
        """Issue concurrent asynchronous HTTP requests across the batch."""
        merged_headers = self._build_headers_expr(headers, auth, bearer, api_key, api_key_header)
        return self._async_call(
            method.upper(),
            params,
            body,
            data,
            merged_headers,
            client=client,
            timeout=timeout,
            retries=retries,
            backoff=backoff,
            max_concurrency=max_concurrency,
            cache=cache,
            with_metadata=with_metadata,
            with_response_headers=with_response_headers,
            on_error=on_error,
            on_request=on_request,
            on_response=on_response,
        )

    # ---- Verb wrappers (sync) ----

    def get(self, params: Optional[pl.Expr] = None, timeout: Optional[float] = None, **kwargs: Any) -> pl.Expr:
        """Issue a synchronous GET per row."""
        return self.request("GET", params, None, timeout=timeout, **kwargs)

    def post(
        self,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        **kwargs: Any,
    ) -> pl.Expr:
        """Issue a synchronous POST per row."""
        return self.request("POST", params, body, timeout=timeout, **kwargs)

    def put(
        self,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        **kwargs: Any,
    ) -> pl.Expr:
        """Issue a synchronous PUT per row."""
        return self.request("PUT", params, body, timeout=timeout, **kwargs)

    def patch(
        self,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        **kwargs: Any,
    ) -> pl.Expr:
        """Issue a synchronous PATCH per row."""
        return self.request("PATCH", params, body, timeout=timeout, **kwargs)

    def delete(self, params: Optional[pl.Expr] = None, timeout: Optional[float] = None, **kwargs: Any) -> pl.Expr:
        """Issue a synchronous DELETE per row."""
        return self.request("DELETE", params, None, timeout=timeout, **kwargs)

    def head(self, params: Optional[pl.Expr] = None, timeout: Optional[float] = None, **kwargs: Any) -> pl.Expr:
        """Issue a synchronous HEAD per row."""
        return self.request("HEAD", params, None, timeout=timeout, **kwargs)

    # ---- Verb wrappers (async) ----

    def aget(self, params: Optional[pl.Expr] = None, timeout: Optional[float] = None, **kwargs: Any) -> pl.Expr:
        """Issue concurrent asynchronous GETs across the batch."""
        return self.arequest("GET", params, None, timeout=timeout, **kwargs)

    def apost(
        self,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        **kwargs: Any,
    ) -> pl.Expr:
        """Issue concurrent asynchronous POSTs across the batch."""
        return self.arequest("POST", params, body, timeout=timeout, **kwargs)

    def aput(
        self,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        **kwargs: Any,
    ) -> pl.Expr:
        """Issue concurrent asynchronous PUTs across the batch."""
        return self.arequest("PUT", params, body, timeout=timeout, **kwargs)

    def apatch(
        self,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        **kwargs: Any,
    ) -> pl.Expr:
        """Issue concurrent asynchronous PATCHes across the batch."""
        return self.arequest("PATCH", params, body, timeout=timeout, **kwargs)

    def adelete(self, params: Optional[pl.Expr] = None, timeout: Optional[float] = None, **kwargs: Any) -> pl.Expr:
        """Issue concurrent asynchronous DELETEs across the batch."""
        return self.arequest("DELETE", params, None, timeout=timeout, **kwargs)

    def ahead(self, params: Optional[pl.Expr] = None, timeout: Optional[float] = None, **kwargs: Any) -> pl.Expr:
        """Issue concurrent asynchronous HEADs across the batch."""
        return self.arequest("HEAD", params, None, timeout=timeout, **kwargs)

    # ---- Pagination ----

    def paginate(
        self,
        params: Optional[pl.Expr] = None,
        *,
        method: str = "GET",
        max_pages: int = 10,
        next_url: Optional[NextUrl] = None,
        headers: Optional[pl.Expr] = None,
        client: Optional[httpx.Client] = None,
        timeout: Optional[float] = None,
        retries: int = 0,
        backoff: float = 0.0,
        on_request: Optional[RequestHook] = None,
        on_response: Optional[ResponseHook] = None,
        auth: Optional[tuple[str, str]] = None,
        bearer: Optional[Union[str, pl.Expr]] = None,
        api_key: Optional[Union[str, pl.Expr]] = None,
        api_key_header: str = "X-API-Key",
    ) -> pl.Expr:
        """Synchronously paginate per row, following Link: rel="next" by default.

        Returns a column of List[Utf8] — one list of response bodies per starting
        URL. Pipe through `.list.eval(pl.element().str.json_decode())` and `.explode(...)`
        to flatten paginated rows back into the DataFrame.

        Pass `next_url=lambda response: ...` to extract the next URL from a custom
        location (e.g. a JSON field) instead of the Link header.
        """
        merged_headers = self._build_headers_expr(headers, auth, bearer, api_key, api_key_header)
        params_expr = pl.lit(None) if params is None else params
        headers_expr = pl.lit(None) if merged_headers is None else merged_headers
        extractor = next_url or (lambda r: _parse_link_next(r.headers.get("link")))
        verb = method.upper()

        def _follow_links(cli: httpx.Client, url: str, p: Any, h: Any) -> list[str]:
            bodies: list[str] = []
            current_url, current_params = url, p
            for _ in range(max_pages):
                response = self._send_sync_with_retries(
                    cli,
                    verb,
                    current_url,
                    current_params,
                    None,
                    None,
                    h,
                    timeout,
                    retries,
                    backoff,
                    on_request,
                    on_response,
                )
                if response is None:
                    break
                bodies.append(response.text)
                nxt = extractor(response)
                if not nxt:
                    break
                current_url = nxt
                current_params = None  # next URL already encodes its own query string
            return bodies

        def _paginate_batch(s: pl.Series) -> pl.Series:
            urls = s.struct.field("url").to_list()
            param_list = s.struct.field("params").to_list()
            header_list = s.struct.field("headers").to_list()
            own_client = client is None
            cli: httpx.Client = httpx.Client() if own_client else client  # type: ignore[assignment]
            try:
                out = [_follow_links(cli, url, p, h) for url, p, h in zip(urls, param_list, header_list)]
                return pl.Series(out, dtype=pl.List(pl.Utf8))
            finally:
                if own_client:
                    cli.close()

        return pl.struct(
            self._url.alias("url"),
            params_expr.alias("params"),
            headers_expr.alias("headers"),
        ).map_batches(_paginate_batch, return_dtype=pl.List(pl.Utf8))
