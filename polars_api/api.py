import asyncio
from typing import Optional

import httpx
import polars as pl


@pl.api.register_expr_namespace("api")
class Api:
    def __init__(self, url: pl.Expr) -> None:
        self._url = url

    @staticmethod
    def _get(url: str, params: Optional[str] = None) -> Optional[str]:
        result = httpx.get(url, params=params)
        if result.status_code == 200:
            return result.text
        else:
            return None

    @staticmethod
    def _post(url: str, body: str) -> Optional[str]:
        result = httpx.post(url, json=body)
        return result.text

    @staticmethod
    async def _aget_one(url: str, params: str) -> str:
        async with httpx.AsyncClient() as client:
            r = await client.get(url, params=params)
            return r.text

    async def _aget_all(self, x, params):
        return await asyncio.gather(*[self._aget_one(url, param) for url, param in zip(x, params)])

    def _aget(self, x, params):
        return pl.Series(asyncio.run(self._aget_all(x, params)))

    @staticmethod
    async def _apost_one(url: str, body: str) -> str:
        async with httpx.AsyncClient() as client:
            r = await client.post(url, json=body)
            return r.text

    async def _apost_all(self, x, body):
        return await asyncio.gather(*[self._apost_one(url, _body) for url, _body in zip(x, body)])

    def _apost(self, x, body):
        return pl.Series(asyncio.run(self._apost_all(x, body)))

    def get(self, params: Optional[pl.Expr] = None) -> pl.Expr:
        if params is None:
            params = pl.lit(None)
        return pl.struct(self._url.alias("url"), params.alias("params")).map_elements(
            lambda x: self._get(x["url"], params=x["params"]),
            return_dtype=pl.Utf8,
        )

    def post(self, body: Optional[pl.Expr] = None) -> pl.Expr:
        if body is None:
            body = pl.lit(None)
        return pl.struct(self._url.alias("url"), body.alias("body")).map_elements(
            lambda x: self._post(x["url"], body=x["body"]),
            return_dtype=pl.Utf8,
        )

    def aget(self, params: Optional[pl.Expr] = None) -> pl.Expr:
        if params is None:
            params = pl.lit(None)
        return pl.struct(self._url.alias("url"), params.alias("params")).map_batches(
            lambda x: self._aget(x.struct.field("url"), params=x.struct.field("params"))
        )

    def apost(self, body: Optional[pl.Expr] = None) -> pl.Expr:
        if body is None:
            body = pl.lit(None)
        return pl.struct(self._url.alias("url"), body.alias("body")).map_batches(
            lambda x: self._apost(x.struct.field("url"), body=x.struct.field("body"))
        )
