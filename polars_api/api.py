import asyncio
from typing import Any, Optional

import httpx
import nest_asyncio  # type: ignore[import-untyped]
import polars as pl


def _arun(coro: Any) -> Any:
    # nest_asyncio lets asyncio.run work inside environments (notably Jupyter)
    # that already have a running event loop.
    nest_asyncio.apply()
    return asyncio.run(coro)


@pl.api.register_expr_namespace("api")
class Api:
    def __init__(self, url: pl.Expr) -> None:
        self._url = url

    @staticmethod
    def _request(
        method: str,
        url: str,
        params: Optional[dict[str, Any]] = None,
        body: Optional[dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Optional[str]:
        response = httpx.request(method, url, params=params, json=body, timeout=timeout)
        return response.text if response.is_success else None

    @staticmethod
    async def _arequest_one(
        client: httpx.AsyncClient,
        method: str,
        url: str,
        params: Optional[dict[str, Any]] = None,
        body: Optional[dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Optional[str]:
        response = await client.request(method, url, params=params, json=body, timeout=timeout)
        return response.text if response.is_success else None

    @classmethod
    async def _arequest_many(
        cls,
        method: str,
        urls: list[str],
        params: list[Optional[dict[str, Any]]],
        bodies: list[Optional[dict[str, Any]]],
        timeout: Optional[float],
    ) -> list[Optional[str]]:
        async with httpx.AsyncClient() as client:
            return await asyncio.gather(*[
                cls._arequest_one(client, method, url, p, b, timeout) for url, p, b in zip(urls, params, bodies)
            ])

    @classmethod
    def _arequest_batch(
        cls,
        method: str,
        urls: pl.Series,
        params: pl.Series,
        bodies: pl.Series,
        timeout: Optional[float],
    ) -> pl.Series:
        results = _arun(cls._arequest_many(method, urls.to_list(), params.to_list(), bodies.to_list(), timeout))
        return pl.Series(results, dtype=pl.Utf8)

    def _sync_call(
        self,
        method: str,
        params: Optional[pl.Expr],
        body: Optional[pl.Expr],
        timeout: Optional[float],
    ) -> pl.Expr:
        params_expr = pl.lit(None) if params is None else params
        body_expr = pl.lit(None) if body is None else body
        return pl.struct(
            self._url.alias("url"),
            params_expr.alias("params"),
            body_expr.alias("body"),
        ).map_elements(
            lambda x: self._request(method, x["url"], x["params"], x["body"], timeout),
            return_dtype=pl.Utf8,
        )

    def _async_call(
        self,
        method: str,
        params: Optional[pl.Expr],
        body: Optional[pl.Expr],
        timeout: Optional[float],
    ) -> pl.Expr:
        params_expr = pl.lit(None) if params is None else params
        body_expr = pl.lit(None) if body is None else body
        return pl.struct(
            self._url.alias("url"),
            params_expr.alias("params"),
            body_expr.alias("body"),
        ).map_batches(
            lambda x: self._arequest_batch(
                method,
                x.struct.field("url"),
                x.struct.field("params"),
                x.struct.field("body"),
                timeout,
            ),
            return_dtype=pl.Utf8,
        )

    def get(self, params: Optional[pl.Expr] = None, timeout: Optional[float] = None) -> pl.Expr:
        """Issue a synchronous GET per row, returning the response body or null on failure."""
        return self._sync_call("GET", params, None, timeout)

    def post(
        self,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
    ) -> pl.Expr:
        """Issue a synchronous POST per row, returning the response body or null on failure."""
        return self._sync_call("POST", params, body, timeout)

    def aget(self, params: Optional[pl.Expr] = None, timeout: Optional[float] = None) -> pl.Expr:
        """Issue concurrent asynchronous GETs across the batch, returning bodies or nulls."""
        return self._async_call("GET", params, None, timeout)

    def apost(
        self,
        params: Optional[pl.Expr] = None,
        body: Optional[pl.Expr] = None,
        timeout: Optional[float] = None,
    ) -> pl.Expr:
        """Issue concurrent asynchronous POSTs across the batch, returning bodies or nulls."""
        return self._async_call("POST", params, body, timeout)
