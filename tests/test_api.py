import json
from typing import Any, Callable

import httpx
import polars as pl
import pytest

import polars_api  # noqa: F401  registers the `api` namespace


def _patch_sync(monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]) -> None:
    transport = httpx.MockTransport(handler)

    def fake_request(method: str, url: str, **kwargs: Any) -> httpx.Response:
        with httpx.Client(transport=transport) as client:
            return client.request(method, url, **kwargs)

    monkeypatch.setattr("polars_api.api.httpx.request", fake_request)


def _patch_async(monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]) -> None:
    transport = httpx.MockTransport(handler)
    real = httpx.AsyncClient

    def factory(*args: Any, **kwargs: Any) -> httpx.AsyncClient:
        return real(*args, transport=transport, **kwargs)

    monkeypatch.setattr("polars_api.api.httpx.AsyncClient", factory)


def test_get_returns_response_text(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_sync(monkeypatch, lambda req: httpx.Response(200, text=f"ok:{req.url.path}"))

    df = pl.DataFrame({"url": ["http://x/a", "http://x/b"]})
    out = df.with_columns(pl.col("url").api.get().alias("res"))

    assert out["res"].to_list() == ["ok:/a", "ok:/b"]


def test_get_failure_returns_null(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_sync(monkeypatch, lambda req: httpx.Response(500, text="boom"))

    df = pl.DataFrame({"url": ["http://x/a"]})
    out = df.with_columns(pl.col("url").api.get().alias("res"))

    assert out["res"].to_list() == [None]


def test_get_passes_params(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req.url.query.decode())
        return httpx.Response(200, text="ok")

    _patch_sync(monkeypatch, handler)

    df = pl.DataFrame({"url": ["http://x/a", "http://x/b"]}).with_columns(
        pl.struct(userId=pl.Series([1, 2])).alias("params"),
    )
    df.with_columns(pl.col("url").api.get(params=pl.col("params")).alias("res")).collect_schema()

    assert sorted(seen) == ["userId=1", "userId=2"]


def test_post_sends_json_body(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[dict[str, Any]] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(json.loads(req.content))
        return httpx.Response(201, text="created")

    _patch_sync(monkeypatch, handler)

    df = pl.DataFrame({"url": ["http://x/posts"]}).with_columns(
        pl.struct(title=pl.lit("hi"), userId=pl.lit(7)).alias("body"),
    )
    out = df.with_columns(pl.col("url").api.post(body=pl.col("body")).alias("res"))

    assert out["res"].to_list() == ["created"]
    assert seen == [{"title": "hi", "userId": 7}]


def test_aget_returns_response_text(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_async(monkeypatch, lambda req: httpx.Response(200, text=f"ok:{req.url.path}"))

    df = pl.DataFrame({"url": ["http://x/a", "http://x/b", "http://x/c"]})
    out = df.with_columns(pl.col("url").api.aget().alias("res"))

    assert out["res"].to_list() == ["ok:/a", "ok:/b", "ok:/c"]


def test_aget_failure_returns_null(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_async(monkeypatch, lambda req: httpx.Response(404, text="nope"))

    df = pl.DataFrame({"url": ["http://x/a", "http://x/b"]})
    out = df.with_columns(pl.col("url").api.aget().alias("res"))

    assert out["res"].to_list() == [None, None]


def test_apost_sends_json_body(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[dict[str, Any]] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(json.loads(req.content))
        return httpx.Response(200, text="ok")

    _patch_async(monkeypatch, handler)

    df = pl.DataFrame({"url": ["http://x/a", "http://x/b"]}).with_columns(
        pl.struct(title=pl.lit("t"), n=pl.Series([1, 2])).alias("body"),
    )
    out = df.with_columns(pl.col("url").api.apost(body=pl.col("body")).alias("res"))

    assert out["res"].to_list() == ["ok", "ok"]
    assert sorted(seen, key=lambda d: d["n"]) == [
        {"title": "t", "n": 1},
        {"title": "t", "n": 2},
    ]
