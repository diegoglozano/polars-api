import asyncio
from typing import Optional

import httpx
import polars as pl


def check_status_code(status_code):
    return status_code >= 200 and status_code < 300


@pl.api.register_expr_namespace("api")
class Api:
    def __init__(self, url: pl.Expr) -> None:
        self._url = url

    @staticmethod
    def _get(url: str, params: Optional[dict[str, str]] = None, timeout: Optional[float] = None) -> Optional[str]:
        result = httpx.get(url, params=params, timeout=timeout)
        if check_status_code(result.status_code):
            return result.text
        else:
            return None

    @staticmethod
    def _post(url: str, params: dict[str, str], body: str, timeout: float) -> Optional[str]:
        result = httpx.post(url, params=params, json=body, timeout=timeout)
        if check_status_code(result.status_code):
            return result.text
        else:
            return None

    @staticmethod
    async def _aget_one(url: str, params: str, timeout: float) -> str:
        async with httpx.AsyncClient() as client:
            r = await client.get(url, params=params, timeout=timeout)
            return r.text

    async def _aget_all(self, x, params, timeout):
        return await asyncio.gather(*[self._aget_one(url, param, timeout) for url, param in zip(x, params)])

    def _aget(self, x, params, timeout):
        return pl.Series(asyncio.run(self._aget_all(x, params, timeout)))

    @staticmethod
    async def _apost_one(url: str, params: str, body: str, timeout: Optional[float]) -> str:
        async with httpx.AsyncClient() as client:
            r = await client.post(url, params=params, json=body, timeout=timeout)
            return r.text

    async def _apost_all(self, x, params, body, timeout):
        return await asyncio.gather(*[
            self._apost_one(url, _params, _body, timeout) for url, _params, _body in zip(x, params, body)
        ])

    def _apost(self, x, params, body, timeout):
        return pl.Series(asyncio.run(self._apost_all(x, params, body, timeout)))

    def get(self, params: Optional[pl.Expr] = None, timeout: Optional[float] = None) -> pl.Expr:
        if params is None:
            params = pl.lit(None)
        return pl.struct(self._url.alias("url"), params.alias("params")).map_elements(
            lambda x: self._get(x["url"], params=x["params"], timeout=timeout),
            return_dtype=pl.Utf8,
        )

    def post(
        self, params: Optional[pl.Expr] = None, body: Optional[pl.Expr] = None, timeout: Optional[float] = None
    ) -> pl.Expr:
        if params is None:
            params = pl.lit(None)
        if body is None:
            body = pl.lit(None)
        return pl.struct(self._url.alias("url"), params.alias("params"), body.alias("body")).map_elements(
            lambda x: self._post(x["url"], params=x["params"], body=x["body"], timeout=timeout),
            return_dtype=pl.Utf8,
        )

    def aget(self, params: Optional[pl.Expr] = None, timeout: Optional[float] = None) -> pl.Expr:
        if params is None:
            params = pl.lit(None)
        return pl.struct(self._url.alias("url"), params.alias("params")).map_batches(
            lambda x: self._aget(x.struct.field("url"), params=x.struct.field("params"), timeout=timeout)
        )

    def apost(
        self, params: Optional[pl.Expr] = None, body: Optional[pl.Expr] = None, timeout: Optional[float] = None
    ) -> pl.Expr:
        if params is None:
            params = pl.lit(None)
        if body is None:
            body = pl.lit(None)
        return pl.struct(self._url.alias("url"), params.alias("params"), body.alias("body")).map_batches(
            lambda x: self._apost(
                x.struct.field("url"), params=x.struct.field("params"), body=x.struct.field("body"), timeout=timeout
            )
        )
