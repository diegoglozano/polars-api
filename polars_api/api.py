import asyncio
import base64
import time
from typing import Any, Optional, Union

import httpx
import nest_asyncio  # type: ignore[import-untyped]
import polars as pl

OnError = str  # "null" | "raise" | "return"

_METADATA_DTYPE = pl.Struct({
    "body": pl.Utf8,
    "status": pl.Int64,
    "elapsed_ms": pl.Float64,
    "error": pl.Utf8,
})


def _arun(coro: Any) -> Any:
    # nest_asyncio lets asyncio.run work inside environments (notably Jupyter)
    # that already have a running event loop.
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


def _result_struct(
    body: Optional[str],
    status: int,
    elapsed_ms: float,
    error: Optional[str],
) -> dict[str, Any]:
    return {"body": body, "status": status, "elapsed_ms": elapsed_ms, "error": error}


def _coerce_body(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


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


@pl.api.register_expr_namespace("api")
class Api:
    def __init__(self, url: pl.Expr) -> None:
        self._url = url

    @staticmethod
    def _request(
        method: str,
        url: str,
        params: Optional[dict[str, Any]],
        body: Optional[dict[str, Any]],
        headers: Optional[dict[str, Any]],
        timeout: Optional[float],
        retries: int,
        backoff: float,
    ) -> dict[str, Any]:
        attempt = 0
        last_error: Optional[str] = None
        last_status = 0
        last_body: Optional[str] = None
        start = time.monotonic()
        while True:
            attempt_start = time.monotonic()
            try:
                response = httpx.request(
                    method,
                    url,
                    params=params,
                    json=body,
                    headers=headers,
                    timeout=timeout,
                )
                last_status = response.status_code
                last_body = response.text
                if response.is_success:
                    elapsed_ms = (time.monotonic() - start) * 1000
                    return _result_struct(last_body, last_status, elapsed_ms, None)
                if attempt < retries and _is_retryable_status(last_status):
                    wait = _retry_after_seconds(response)
                    if wait is None:
                        wait = backoff * (2 ** attempt) if backoff > 0 else 0
                    if wait > 0:
                        time.sleep(wait)
                    attempt += 1
                    continue
                last_error = f"HTTP {last_status}"
                elapsed_ms = (time.monotonic() - start) * 1000
                return _result_struct(last_body, last_status, elapsed_ms, last_error)
            except httpx.HTTPError as exc:  # connection / timeout / network
                last_error = f"{type(exc).__name__}: {exc}"
                if attempt < retries:
                    wait = backoff * (2 ** attempt) if backoff > 0 else 0
                    if wait > 0:
                        time.sleep(wait)
                    attempt += 1
                    continue
                elapsed_ms = (time.monotonic() - attempt_start) * 1000
                return _result_struct(None, 0, elapsed_ms, last_error)

    @staticmethod
    async def _arequest_attempt(
        client: httpx.AsyncClient,
        method: str,
        url: str,
        params: Optional[dict[str, Any]],
        body: Optional[dict[str, Any]],
        headers: Optional[dict[str, Any]],
        timeout: Optional[float],
        attempt: int,
        retries: int,
        backoff: float,
        start: float,
    ) -> tuple[Optional[dict[str, Any]], Optional[float]]:
        attempt_start = time.monotonic()
        try:
            response = await client.request(
                method, url, params=params, json=body, headers=headers, timeout=timeout,
            )
        except httpx.HTTPError as exc:
            if attempt < retries:
                wait = backoff * (2 ** attempt) if backoff > 0 else 0.0
                return None, wait
            elapsed_ms = (time.monotonic() - attempt_start) * 1000
            return _result_struct(None, 0, elapsed_ms, f"{type(exc).__name__}: {exc}"), None

        status = response.status_code
        text = response.text
        if response.is_success:
            elapsed_ms = (time.monotonic() - start) * 1000
            return _result_struct(text, status, elapsed_ms, None), None
        if attempt < retries and _is_retryable_status(status):
            wait = _retry_after_seconds(response)
            if wait is None:
                wait = backoff * (2 ** attempt) if backoff > 0 else 0.0
            return None, wait
        elapsed_ms = (time.monotonic() - start) * 1000
        return _result_struct(text, status, elapsed_ms, f"HTTP {status}"), None

    @classmethod
    async def _arequest_one(
        cls,
        client: httpx.AsyncClient,
        semaphore: Optional[asyncio.Semaphore],
        method: str,
        url: str,
        params: Optional[dict[str, Any]],
        body: Optional[dict[str, Any]],
        headers: Optional[dict[str, Any]],
        timeout: Optional[float],
        retries: int,
        backoff: float,
    ) -> dict[str, Any]:
        async def _go() -> dict[str, Any]:
            attempt = 0
            start = time.monotonic()
            while True:
                result, wait = await cls._arequest_attempt(
                    client, method, url, params, body, headers, timeout,
                    attempt, retries, backoff, start,
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
    async def _arequest_many(
        cls,
        method: str,
        urls: list[str],
        params: list[Optional[dict[str, Any]]],
        bodies: list[Optional[dict[str, Any]]],
        headers: list[Optional[dict[str, Any]]],
        timeout: Optional[float],
        retries: int,
        backoff: float,
        max_concurrency: Optional[int],
    ) -> list[dict[str, Any]]:
        semaphore = asyncio.Semaphore(max_concurrency) if max_concurrency else None
        async with httpx.AsyncClient() as client:
            tasks = [
                cls._arequest_one(client, semaphore, method, u, p, b, h, timeout, retries, backoff)
                for u, p, b, h in zip(urls, params, bodies, headers)
            ]
            return await asyncio.gather(*tasks)

    @classmethod
    def _arequest_batch(
        cls,
        method: str,
        urls: pl.Series,
        params: pl.Series,
        bodies: pl.Series,
        headers: pl.Series,
        timeout: Optional[float],
        retries: int,
        backoff: float,
        max_concurrency: Optional[int],
        with_metadata: bool,
        on_error: OnError,
    ) -> pl.Series:
        results = _arun(cls._arequest_many(
            method,
            urls.to_list(),
            params.to_list(),
            bodies.to_list(),
            headers.to_list(),
            timeout,
            retries,
            backoff,
            max_concurrency,
        ))
        return cls._results_to_series(results, with_metadata=with_metadata, on_error=on_error)

    @staticmethod
    def _results_to_series(
        results: list[dict[str, Any]],
        *,
        with_metadata: bool,
        on_error: OnError,
    ) -> pl.Series:
        if with_metadata:
            return pl.Series(results, dtype=_METADATA_DTYPE)
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
        # Auth helpers win over user-supplied headers — they're a deliberate request from the caller.
        return headers.struct.with_fields(*extra_exprs)

    def _sync_call(
        self,
        method: str,
        params: Optional[pl.Expr],
        body: Optional[pl.Expr],
        headers: Optional[pl.Expr],
        timeout: Optional[float],
        retries: int,
        backoff: float,
        with_metadata: bool,
        on_error: OnError,
    ) -> pl.Expr:
        params_expr = pl.lit(None) if params is None else params
        body_expr = pl.lit(None) if body is None else body
        headers_expr = pl.lit(None) if headers is None else headers
        return_dtype = _METADATA_DTYPE if with_metadata else pl.Utf8
        return pl.struct(
            self._url.alias("url"),
            params_expr.alias("params"),
            body_expr.alias("body"),
            headers_expr.alias("headers"),
        ).map_elements(
            lambda x: _resolve_output(
                self._request(
                    method,
                    x["url"],
                    x["params"],
                    x["body"],
                    x["headers"],
                    timeout,
                    retries,
                    backoff,
                ),
                with_metadata=with_metadata,
                on_error=on_error,
            ),
            return_dtype=return_dtype,
        )

    def _async_call(
        self,
        method: str,
        params: Optional[pl.Expr],
        body: Optional[pl.Expr],
        headers: Optional[pl.Expr],
        timeout: Optional[float],
        retries: int,
        backoff: float,
        max_concurrency: Optional[int],
        with_metadata: bool,
        on_error: OnError,
    ) -> pl.Expr:
        params_expr = pl.lit(None) if params is None else params
        body_expr = pl.lit(None) if body is None else body
        headers_expr = pl.lit(None) if headers is None else headers
        return_dtype = _METADATA_DTYPE if with_metadata else pl.Utf8
        return pl.struct(
            self._url.alias("url"),
            params_expr.alias("params"),
            body_expr.alias("body"),
            headers_expr.alias("headers"),
        ).map_batches(
            lambda x: self._arequest_batch(
                method,
                x.struct.field("url"),
                x.struct.field("params"),
                x.struct.field("body"),
                x.struct.field("headers"),
                timeout,
                retries,
                backoff,
                max_concurrency,
                with_metadata,
                on_error,
            ),
            return_dtype=return_dtype,
        )

    def request(
        self,
        method: str,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        *,
        headers: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        retries: int = 0,
        backoff: float = 0.0,
        with_metadata: bool = False,
        on_error: OnError = "null",
        auth: Optional[tuple[str, str]] = None,
        bearer: Optional[Union[str, pl.Expr]] = None,
        api_key: Optional[Union[str, pl.Expr]] = None,
        api_key_header: str = "X-API-Key",
    ) -> pl.Expr:
        """Issue a synchronous request per row."""
        merged_headers = self._build_headers_expr(headers, auth, bearer, api_key, api_key_header)
        return self._sync_call(
            method.upper(), params, body, merged_headers, timeout, retries, backoff, with_metadata, on_error,
        )

    def arequest(
        self,
        method: str,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        *,
        headers: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        retries: int = 0,
        backoff: float = 0.0,
        max_concurrency: Optional[int] = None,
        with_metadata: bool = False,
        on_error: OnError = "null",
        auth: Optional[tuple[str, str]] = None,
        bearer: Optional[Union[str, pl.Expr]] = None,
        api_key: Optional[Union[str, pl.Expr]] = None,
        api_key_header: str = "X-API-Key",
    ) -> pl.Expr:
        """Issue concurrent asynchronous requests across the batch."""
        merged_headers = self._build_headers_expr(headers, auth, bearer, api_key, api_key_header)
        return self._async_call(
            method.upper(),
            params,
            body,
            merged_headers,
            timeout,
            retries,
            backoff,
            max_concurrency,
            with_metadata,
            on_error,
        )

    def get(
        self,
        params: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        *,
        headers: Optional[pl.Expr] = None,
        retries: int = 0,
        backoff: float = 0.0,
        with_metadata: bool = False,
        on_error: OnError = "null",
        auth: Optional[tuple[str, str]] = None,
        bearer: Optional[Union[str, pl.Expr]] = None,
        api_key: Optional[Union[str, pl.Expr]] = None,
        api_key_header: str = "X-API-Key",
    ) -> pl.Expr:
        """Issue a synchronous GET per row, returning the response body or null on failure."""
        return self.request(
            "GET", params, None,
            headers=headers, timeout=timeout, retries=retries, backoff=backoff,
            with_metadata=with_metadata, on_error=on_error,
            auth=auth, bearer=bearer, api_key=api_key, api_key_header=api_key_header,
        )

    def post(
        self,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        *,
        headers: Optional[pl.Expr] = None,
        retries: int = 0,
        backoff: float = 0.0,
        with_metadata: bool = False,
        on_error: OnError = "null",
        auth: Optional[tuple[str, str]] = None,
        bearer: Optional[Union[str, pl.Expr]] = None,
        api_key: Optional[Union[str, pl.Expr]] = None,
        api_key_header: str = "X-API-Key",
    ) -> pl.Expr:
        """Issue a synchronous POST per row, returning the response body or null on failure."""
        return self.request(
            "POST", params, body,
            headers=headers, timeout=timeout, retries=retries, backoff=backoff,
            with_metadata=with_metadata, on_error=on_error,
            auth=auth, bearer=bearer, api_key=api_key, api_key_header=api_key_header,
        )

    def put(
        self,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        *,
        headers: Optional[pl.Expr] = None,
        retries: int = 0,
        backoff: float = 0.0,
        with_metadata: bool = False,
        on_error: OnError = "null",
        auth: Optional[tuple[str, str]] = None,
        bearer: Optional[Union[str, pl.Expr]] = None,
        api_key: Optional[Union[str, pl.Expr]] = None,
        api_key_header: str = "X-API-Key",
    ) -> pl.Expr:
        """Issue a synchronous PUT per row, returning the response body or null on failure."""
        return self.request(
            "PUT", params, body,
            headers=headers, timeout=timeout, retries=retries, backoff=backoff,
            with_metadata=with_metadata, on_error=on_error,
            auth=auth, bearer=bearer, api_key=api_key, api_key_header=api_key_header,
        )

    def patch(
        self,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        *,
        headers: Optional[pl.Expr] = None,
        retries: int = 0,
        backoff: float = 0.0,
        with_metadata: bool = False,
        on_error: OnError = "null",
        auth: Optional[tuple[str, str]] = None,
        bearer: Optional[Union[str, pl.Expr]] = None,
        api_key: Optional[Union[str, pl.Expr]] = None,
        api_key_header: str = "X-API-Key",
    ) -> pl.Expr:
        """Issue a synchronous PATCH per row, returning the response body or null on failure."""
        return self.request(
            "PATCH", params, body,
            headers=headers, timeout=timeout, retries=retries, backoff=backoff,
            with_metadata=with_metadata, on_error=on_error,
            auth=auth, bearer=bearer, api_key=api_key, api_key_header=api_key_header,
        )

    def delete(
        self,
        params: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        *,
        headers: Optional[pl.Expr] = None,
        retries: int = 0,
        backoff: float = 0.0,
        with_metadata: bool = False,
        on_error: OnError = "null",
        auth: Optional[tuple[str, str]] = None,
        bearer: Optional[Union[str, pl.Expr]] = None,
        api_key: Optional[Union[str, pl.Expr]] = None,
        api_key_header: str = "X-API-Key",
    ) -> pl.Expr:
        """Issue a synchronous DELETE per row, returning the response body or null on failure."""
        return self.request(
            "DELETE", params, None,
            headers=headers, timeout=timeout, retries=retries, backoff=backoff,
            with_metadata=with_metadata, on_error=on_error,
            auth=auth, bearer=bearer, api_key=api_key, api_key_header=api_key_header,
        )

    def head(
        self,
        params: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        *,
        headers: Optional[pl.Expr] = None,
        retries: int = 0,
        backoff: float = 0.0,
        with_metadata: bool = False,
        on_error: OnError = "null",
        auth: Optional[tuple[str, str]] = None,
        bearer: Optional[Union[str, pl.Expr]] = None,
        api_key: Optional[Union[str, pl.Expr]] = None,
        api_key_header: str = "X-API-Key",
    ) -> pl.Expr:
        """Issue a synchronous HEAD per row, returning the (typically empty) body or null on failure."""
        return self.request(
            "HEAD", params, None,
            headers=headers, timeout=timeout, retries=retries, backoff=backoff,
            with_metadata=with_metadata, on_error=on_error,
            auth=auth, bearer=bearer, api_key=api_key, api_key_header=api_key_header,
        )

    def aget(
        self,
        params: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        *,
        headers: Optional[pl.Expr] = None,
        retries: int = 0,
        backoff: float = 0.0,
        max_concurrency: Optional[int] = None,
        with_metadata: bool = False,
        on_error: OnError = "null",
        auth: Optional[tuple[str, str]] = None,
        bearer: Optional[Union[str, pl.Expr]] = None,
        api_key: Optional[Union[str, pl.Expr]] = None,
        api_key_header: str = "X-API-Key",
    ) -> pl.Expr:
        """Issue concurrent asynchronous GETs across the batch, returning bodies or nulls."""
        return self.arequest(
            "GET", params, None,
            headers=headers, timeout=timeout, retries=retries, backoff=backoff,
            max_concurrency=max_concurrency, with_metadata=with_metadata, on_error=on_error,
            auth=auth, bearer=bearer, api_key=api_key, api_key_header=api_key_header,
        )

    def apost(
        self,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        *,
        headers: Optional[pl.Expr] = None,
        retries: int = 0,
        backoff: float = 0.0,
        max_concurrency: Optional[int] = None,
        with_metadata: bool = False,
        on_error: OnError = "null",
        auth: Optional[tuple[str, str]] = None,
        bearer: Optional[Union[str, pl.Expr]] = None,
        api_key: Optional[Union[str, pl.Expr]] = None,
        api_key_header: str = "X-API-Key",
    ) -> pl.Expr:
        """Issue concurrent asynchronous POSTs across the batch, returning bodies or nulls."""
        return self.arequest(
            "POST", params, body,
            headers=headers, timeout=timeout, retries=retries, backoff=backoff,
            max_concurrency=max_concurrency, with_metadata=with_metadata, on_error=on_error,
            auth=auth, bearer=bearer, api_key=api_key, api_key_header=api_key_header,
        )

    def aput(
        self,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        *,
        headers: Optional[pl.Expr] = None,
        retries: int = 0,
        backoff: float = 0.0,
        max_concurrency: Optional[int] = None,
        with_metadata: bool = False,
        on_error: OnError = "null",
        auth: Optional[tuple[str, str]] = None,
        bearer: Optional[Union[str, pl.Expr]] = None,
        api_key: Optional[Union[str, pl.Expr]] = None,
        api_key_header: str = "X-API-Key",
    ) -> pl.Expr:
        """Issue concurrent asynchronous PUTs across the batch."""
        return self.arequest(
            "PUT", params, body,
            headers=headers, timeout=timeout, retries=retries, backoff=backoff,
            max_concurrency=max_concurrency, with_metadata=with_metadata, on_error=on_error,
            auth=auth, bearer=bearer, api_key=api_key, api_key_header=api_key_header,
        )

    def apatch(
        self,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        *,
        headers: Optional[pl.Expr] = None,
        retries: int = 0,
        backoff: float = 0.0,
        max_concurrency: Optional[int] = None,
        with_metadata: bool = False,
        on_error: OnError = "null",
        auth: Optional[tuple[str, str]] = None,
        bearer: Optional[Union[str, pl.Expr]] = None,
        api_key: Optional[Union[str, pl.Expr]] = None,
        api_key_header: str = "X-API-Key",
    ) -> pl.Expr:
        """Issue concurrent asynchronous PATCHes across the batch."""
        return self.arequest(
            "PATCH", params, body,
            headers=headers, timeout=timeout, retries=retries, backoff=backoff,
            max_concurrency=max_concurrency, with_metadata=with_metadata, on_error=on_error,
            auth=auth, bearer=bearer, api_key=api_key, api_key_header=api_key_header,
        )

    def adelete(
        self,
        params: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        *,
        headers: Optional[pl.Expr] = None,
        retries: int = 0,
        backoff: float = 0.0,
        max_concurrency: Optional[int] = None,
        with_metadata: bool = False,
        on_error: OnError = "null",
        auth: Optional[tuple[str, str]] = None,
        bearer: Optional[Union[str, pl.Expr]] = None,
        api_key: Optional[Union[str, pl.Expr]] = None,
        api_key_header: str = "X-API-Key",
    ) -> pl.Expr:
        """Issue concurrent asynchronous DELETEs across the batch."""
        return self.arequest(
            "DELETE", params, None,
            headers=headers, timeout=timeout, retries=retries, backoff=backoff,
            max_concurrency=max_concurrency, with_metadata=with_metadata, on_error=on_error,
            auth=auth, bearer=bearer, api_key=api_key, api_key_header=api_key_header,
        )

    def ahead(
        self,
        params: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
        *,
        headers: Optional[pl.Expr] = None,
        retries: int = 0,
        backoff: float = 0.0,
        max_concurrency: Optional[int] = None,
        with_metadata: bool = False,
        on_error: OnError = "null",
        auth: Optional[tuple[str, str]] = None,
        bearer: Optional[Union[str, pl.Expr]] = None,
        api_key: Optional[Union[str, pl.Expr]] = None,
        api_key_header: str = "X-API-Key",
    ) -> pl.Expr:
        """Issue concurrent asynchronous HEADs across the batch."""
        return self.arequest(
            "HEAD", params, None,
            headers=headers, timeout=timeout, retries=retries, backoff=backoff,
            max_concurrency=max_concurrency, with_metadata=with_metadata, on_error=on_error,
            auth=auth, bearer=bearer, api_key=api_key, api_key_header=api_key_header,
        )
