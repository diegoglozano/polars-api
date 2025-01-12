import asyncio

import httpx
import polars as pl


async def aget(url):
    with httpx.AsyncClient() as client:
        r = await client.get(url)
    return r.text


BASE_URL = "https://jsonplaceholder.typicode.com/posts/1"
print(
    pl.DataFrame({
        "url": [BASE_URL for _ in range(10)],
    }).with_columns(
        pl.col("url").map_batches(
            lambda x: asyncio.gather(*[aget(url) for url in x]),
        )
    )
)
