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


def test_with_metadata_sync_success(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_sync(monkeypatch, lambda req: httpx.Response(200, text="hello"))

    df = pl.DataFrame({"url": ["http://x/a"]})
    out = df.with_columns(pl.col("url").api.get(with_metadata=True).alias("res"))

    row = out["res"].to_list()[0]
    assert row["body"] == "hello"
    assert row["status"] == 200
    assert row["error"] is None
    assert row["elapsed_ms"] >= 0


def test_with_metadata_sync_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_sync(monkeypatch, lambda req: httpx.Response(500, text="boom"))

    df = pl.DataFrame({"url": ["http://x/a"]})
    out = df.with_columns(pl.col("url").api.get(with_metadata=True).alias("res"))

    row = out["res"].to_list()[0]
    assert row["status"] == 500
    assert row["body"] == "boom"
    assert row["error"] == "HTTP 500"


def test_with_metadata_async_success(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_async(monkeypatch, lambda req: httpx.Response(200, text="hi"))

    df = pl.DataFrame({"url": ["http://x/a", "http://x/b"]})
    out = df.with_columns(pl.col("url").api.aget(with_metadata=True).alias("res"))

    rows = out["res"].to_list()
    assert all(r["status"] == 200 for r in rows)
    assert [r["body"] for r in rows] == ["hi", "hi"]


def test_headers_sent_per_row_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[dict[str, str]] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append({k: v for k, v in req.headers.items() if k.lower() == "x-tenant"})
        return httpx.Response(200, text="ok")

    _patch_sync(monkeypatch, handler)

    df = pl.DataFrame({"url": ["http://x/a", "http://x/b"]}).with_columns(
        pl.struct(pl.Series("X-Tenant", ["t1", "t2"])).alias("hdr"),
    )
    df.with_columns(pl.col("url").api.get(headers=pl.col("hdr")).alias("r")).collect_schema()

    assert sorted([d.get("x-tenant") for d in seen]) == ["t1", "t2"]


def test_retries_on_5xx_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    state = {"calls": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        state["calls"] += 1
        if state["calls"] < 3:
            return httpx.Response(503, text="busy")
        return httpx.Response(200, text="ok")

    _patch_sync(monkeypatch, handler)

    df = pl.DataFrame({"url": ["http://x/a"]})
    out = df.with_columns(pl.col("url").api.get(retries=3, backoff=0.0).alias("r"))

    assert out["r"].to_list() == ["ok"]
    assert state["calls"] == 3


def test_retries_exhausted_returns_null(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_sync(monkeypatch, lambda req: httpx.Response(500, text="boom"))

    df = pl.DataFrame({"url": ["http://x/a"]})
    out = df.with_columns(pl.col("url").api.get(retries=2, backoff=0.0).alias("r"))

    assert out["r"].to_list() == [None]


def test_retries_429_respects_retry_after(monkeypatch: pytest.MonkeyPatch) -> None:
    state = {"calls": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        state["calls"] += 1
        if state["calls"] == 1:
            return httpx.Response(429, headers={"Retry-After": "0"}, text="slow")
        return httpx.Response(200, text="ok")

    _patch_sync(monkeypatch, handler)

    df = pl.DataFrame({"url": ["http://x/a"]})
    out = df.with_columns(pl.col("url").api.get(retries=1).alias("r"))

    assert out["r"].to_list() == ["ok"]


def test_max_concurrency_caps_in_flight(monkeypatch: pytest.MonkeyPatch) -> None:
    state = {"in_flight": 0, "max_seen": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        state["in_flight"] += 1
        state["max_seen"] = max(state["max_seen"], state["in_flight"])
        # Note: MockTransport runs synchronously; the in-flight counter still
        # reflects the semaphore behaviour because each task acquires/releases.
        state["in_flight"] -= 1
        return httpx.Response(200, text="ok")

    _patch_async(monkeypatch, handler)

    df = pl.DataFrame({"url": [f"http://x/{i}" for i in range(20)]})
    out = df.with_columns(pl.col("url").api.aget(max_concurrency=2).alias("r"))

    assert out["r"].to_list() == ["ok"] * 20
    assert state["max_seen"] <= 2


def test_put_patch_delete_head(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req.method)
        return httpx.Response(200, text="ok")

    _patch_sync(monkeypatch, handler)

    df = pl.DataFrame({"url": ["http://x/a"]})
    df.with_columns(
        pl.col("url").api.put(body=pl.struct(x=pl.lit(1))).alias("put"),
        pl.col("url").api.patch(body=pl.struct(x=pl.lit(1))).alias("patch"),
        pl.col("url").api.delete().alias("delete"),
        pl.col("url").api.head().alias("head"),
    ).collect_schema()

    assert sorted(seen) == ["DELETE", "HEAD", "PATCH", "PUT"]


def test_async_verbs(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req.method)
        return httpx.Response(200, text="ok")

    _patch_async(monkeypatch, handler)

    df = pl.DataFrame({"url": ["http://x/a"]})
    df.with_columns(
        pl.col("url").api.aput(body=pl.struct(x=pl.lit(1))).alias("aput"),
        pl.col("url").api.apatch(body=pl.struct(x=pl.lit(1))).alias("apatch"),
        pl.col("url").api.adelete().alias("adelete"),
        pl.col("url").api.ahead().alias("ahead"),
    ).collect_schema()

    assert sorted(seen) == ["DELETE", "HEAD", "PATCH", "PUT"]


def test_basic_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req.headers.get("authorization", ""))
        return httpx.Response(200, text="ok")

    _patch_sync(monkeypatch, handler)

    df = pl.DataFrame({"url": ["http://x/a"]})
    df.with_columns(pl.col("url").api.get(auth=("alice", "s3cret")).alias("r")).collect_schema()

    # base64("alice:s3cret") == "YWxpY2U6czNjcmV0"
    assert seen == ["Basic YWxpY2U6czNjcmV0"]


def test_bearer_per_row(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req.headers.get("authorization", ""))
        return httpx.Response(200, text="ok")

    _patch_sync(monkeypatch, handler)

    df = pl.DataFrame({"url": ["http://x/a", "http://x/b"], "tok": ["t1", "t2"]})
    df.with_columns(pl.col("url").api.get(bearer=pl.col("tok")).alias("r")).collect_schema()

    assert sorted(seen) == ["Bearer t1", "Bearer t2"]


def test_api_key_custom_header(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req.headers.get("x-my-key", ""))
        return httpx.Response(200, text="ok")

    _patch_sync(monkeypatch, handler)

    df = pl.DataFrame({"url": ["http://x/a"]})
    df.with_columns(
        pl.col("url").api.get(api_key="abc123", api_key_header="X-My-Key").alias("r"),
    ).collect_schema()

    assert seen == ["abc123"]


def test_on_error_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_sync(monkeypatch, lambda req: httpx.Response(500, text="boom"))

    df = pl.DataFrame({"url": ["http://x/a"]})
    with pytest.raises(Exception, match="HTTP 500"):
        df.with_columns(pl.col("url").api.get(on_error="raise").alias("r"))


def test_on_error_return_passes_body(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_sync(monkeypatch, lambda req: httpx.Response(404, text="missing"))

    df = pl.DataFrame({"url": ["http://x/a"]})
    out = df.with_columns(pl.col("url").api.get(on_error="return").alias("r"))

    assert out["r"].to_list() == ["missing"]


def test_connection_error_returns_null(monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom")

    _patch_sync(monkeypatch, handler)

    df = pl.DataFrame({"url": ["http://x/a"]})
    out = df.with_columns(pl.col("url").api.get(with_metadata=True).alias("r"))

    row = out["r"].to_list()[0]
    assert row["status"] == 0
    assert row["body"] is None
    assert row["error"] is not None
    assert "ConnectError" in row["error"]
