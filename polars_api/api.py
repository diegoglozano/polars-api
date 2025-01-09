from typing import Optional

import httpx
import polars as pl


@pl.api.register_expr_namespace("api")
class Api:
    def __init__(self, url: pl.Expr) -> None:
        self._url = url

    @staticmethod
    def _get(url: str) -> Optional[str]:
        result = httpx.get(url)
        if result.status_code == 200:
            return result.text
        else:
            return None

    @staticmethod
    def _post(url: str, body: str) -> Optional[str]:
        result = httpx.post(url, json=body)
        if result.status_code == 200:
            return result.text
        else:
            return None

    def get(self, params: Optional[pl.Expr] = None) -> pl.Expr:
        return self._url.map_elements(
            lambda x: self._get(x),
            return_dtype=pl.Utf8,
        )

    def post(self, body: Optional[pl.Expr] = None) -> pl.Expr:
        if body is None:
            body = pl.lit("")
        return pl.struct([self._url.alias("url"), body.alias("body")]).map_elements(
            lambda x: self._post(x["url"], x["body"]),
            return_dtype=pl.Utf8,
        )
