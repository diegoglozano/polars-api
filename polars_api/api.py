import asyncio
from typing import Optional

import httpx
import nest_asyncio
import polars as pl


def _check_status_code(status_code):
    """
    Check if the status code indicates a successful response.

    Parameters
    ----------
    status_code : int
        The HTTP status code to check.

    Returns
    -------
    bool
        True if the status code is between 200 and 299, False otherwise.
    """
    return status_code >= 200 and status_code < 300


@pl.api.register_expr_namespace("api")
class Api:
    def __init__(self, url: pl.Expr) -> None:
        """
        Initialize the Api class with a URL expression.

        Parameters
        ----------
        url : pl.Expr
            The URL expression.
        """
        self._url = url

    @staticmethod
    def _get(url: str, params: Optional[dict[str, str]] = None, timeout: Optional[float] = None) -> Optional[str]:
        """
        Perform a synchronous GET request.

        Parameters
        ----------
        url : str
            The URL to send the GET request to.
        params : dict[str, str], optional
            The query parameters to include in the request.
        timeout : float, optional
            The timeout for the request.

        Returns
        -------
        str or None
            The response text if the request is successful, None otherwise.
        """
        result = httpx.get(url, params=params, timeout=timeout)
        if _check_status_code(result.status_code):
            return result.text
        else:
            return None

    @staticmethod
    def _post(url: str, params: dict[str, str], body: str, timeout: float) -> Optional[str]:
        """
        Perform a synchronous POST request.

        Parameters
        ----------
        url : str
            The URL to send the POST request to.
        params : dict[str, str]
            The query parameters to include in the request.
        body : str
            The JSON body to include in the request.
        timeout : float
            The timeout for the request.

        Returns
        -------
        str or None
            The response text if the request is successful, None otherwise.
        """
        result = httpx.post(url, params=params, json=body, timeout=timeout)
        if _check_status_code(result.status_code):
            return result.text
        else:
            return None

    @staticmethod
    async def _aget_one(url: str, params: str, timeout: float) -> str:
        """
        Perform an asynchronous GET request.

        Parameters
        ----------
        url : str
            The URL to send the GET request to.
        params : str
            The query parameters to include in the request.
        timeout : float
            The timeout for the request.

        Returns
        -------
        str
            The response text.
        """
        async with httpx.AsyncClient() as client:
            r = await client.get(url, params=params, timeout=timeout)
            return r.text

    async def _aget_all(self, x, params, timeout):
        """
        Perform multiple asynchronous GET requests.

        Parameters
        ----------
        x : list
            List of URLs to send the GET requests to.
        params : list
            List of query parameters for each request.
        timeout : float
            The timeout for the requests.

        Returns
        -------
        list
            List of response texts.
        """
        return await asyncio.gather(*[self._aget_one(url, param, timeout) for url, param in zip(x, params)])

    def _aget(self, x, params, timeout):
        """
        Wrapper for performing multiple asynchronous GET requests.

        Parameters
        ----------
        x : list
            List of URLs to send the GET requests to.
        params : list
            List of query parameters for each request.
        timeout : float
            The timeout for the requests.

        Returns
        -------
        pl.Series
            Series of response texts.
        """
        return pl.Series(asyncio.run(self._aget_all(x, params, timeout)))

    @staticmethod
    async def _apost_one(url: str, params: str, body: str, timeout: Optional[float]) -> str:
        """
        Perform an asynchronous POST request.

        Parameters
        ----------
        url : str
            The URL to send the POST request to.
        params : str
            The query parameters to include in the request.
        body : str
            The JSON body to include in the request.
        timeout : float, optional
            The timeout for the request.

        Returns
        -------
        str
            The response text.
        """
        async with httpx.AsyncClient() as client:
            r = await client.post(url, params=params, json=body, timeout=timeout)
            return r.text

    async def _apost_all(self, x, params, body, timeout):
        """
        Perform multiple asynchronous POST requests.

        Parameters
        ----------
        x : list
            List of URLs to send the POST requests to.
        params : list
            List of query parameters for each request.
        body : list
            List of JSON bodies for each request.
        timeout : float
            The timeout for the requests.

        Returns
        -------
        list
            List of response texts.
        """
        return await asyncio.gather(*[
            self._apost_one(url, _params, _body, timeout) for url, _params, _body in zip(x, params, body)
        ])

    def _apost(self, x, params, body, timeout):
        """
        Wrapper for performing multiple asynchronous POST requests.

        Parameters
        ----------
        x : list
            List of URLs to send the POST requests to.
        params : list
            List of query parameters for each request.
        body : list
            List of JSON bodies for each request.
        timeout : float
            The timeout for the requests.

        Returns
        -------
        pl.Series
            Series of response texts.
        """
        return pl.Series(asyncio.run(self._apost_all(x, params, body, timeout)))

    def get(self, params: Optional[pl.Expr] = None, timeout: Optional[float] = None) -> pl.Expr:
        """
        Perform a synchronous GET request for each URL in the expression.

        Parameters
        ----------
        params : pl.Expr, optional
            The query parameters expression.
        timeout : float, optional
            The timeout for the requests.

        Returns
        -------
        pl.Expr
            Expression containing the response texts.
        """
        if params is None:
            params = pl.lit(None)
        return pl.struct(self._url.alias("url"), params.alias("params")).map_elements(
            lambda x: self._get(x["url"], params=x["params"], timeout=timeout),
            return_dtype=pl.Utf8,
        )

    def post(
        self, params: Optional[pl.Expr] = None, body: Optional[pl.Expr] = None, timeout: Optional[float] = None
    ) -> pl.Expr:
        """
        Perform a synchronous POST request for each URL in the expression.

        Parameters
        ----------
        params : pl.Expr, optional
            The query parameters expression.
        body : pl.Expr, optional
            The JSON body expression.
        timeout : float, optional
            The timeout for the requests.

        Returns
        -------
        pl.Expr
            Expression containing the response texts.
        """
        if params is None:
            params = pl.lit(None)
        if body is None:
            body = pl.lit(None)
        return pl.struct(self._url.alias("url"), params.alias("params"), body.alias("body")).map_elements(
            lambda x: self._post(x["url"], params=x["params"], body=x["body"], timeout=timeout),
            return_dtype=pl.Utf8,
        )

    def aget(self, params: Optional[pl.Expr] = None, timeout: Optional[float] = None) -> pl.Expr:
        """
        Perform an asynchronous GET request for each URL in the expression.

        Parameters
        ----------
        params : pl.Expr, optional
            The query parameters expression.
        timeout : float, optional
            The timeout for the requests.

        Returns
        -------
        pl.Expr
            Expression containing the response texts.
        """
        nest_asyncio.apply()
        if params is None:
            params = pl.lit(None)
        return pl.struct(self._url.alias("url"), params.alias("params")).map_batches(
            lambda x: self._aget(x.struct.field("url"), params=x.struct.field("params"), timeout=timeout)
        )

    def apost(
        self, params: Optional[pl.Expr] = None, body: Optional[pl.Expr] = None, timeout: Optional[float] = None
    ) -> pl.Expr:
        """
        Perform an asynchronous POST request for each URL in the expression.

        Parameters
        ----------
        params : pl.Expr, optional
            The query parameters expression.
        body : pl.Expr, optional
            The JSON body expression.
        timeout : float, optional
            The timeout for the requests.

        Returns
        -------
        pl.Expr
            Expression containing the response texts.
        """
        nest_asyncio.apply()
        if params is None:
            params = pl.lit(None)
        if body is None:
            body = pl.lit(None)
        return pl.struct(self._url.alias("url"), params.alias("params"), body.alias("body")).map_batches(
            lambda x: self._apost(
                x.struct.field("url"), params=x.struct.field("params"), body=x.struct.field("body"), timeout=timeout
            )
        )
