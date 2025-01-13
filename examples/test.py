import polars as pl

BASE_URL = "https://jsonplaceholder.typicode.com/posts"
print(
    pl.DataFrame({
        "url": [BASE_URL for _ in range(10)],
    })
    .with_columns(
        pl.struct(
            title=pl.lit("foo"),
            body=pl.lit("bar"),
            userId=pl.arange(10),
        ).alias("body"),
    )
    .with_columns(
        pl.col("url").api.get().str.json_decode().alias("get"),
        pl.col("url").api.aget().str.json_decode().alias("aget"),
        pl.col("url").api.post(body=pl.col("body")).str.json_decode().alias("post"),
        pl.col("url").api.apost(body=pl.col("body")).str.json_decode().alias("apost"),
    )
)
