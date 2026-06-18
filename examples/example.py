import polars as pl

import polars_api  # noqa:F401

BASE_URL = "https://jsonplaceholder.typicode.com/posts"

# In an expression, `str.json_decode` requires an explicit schema. The /posts
# collection endpoint returns a JSON array, while POST /posts returns the single
# created object.
post = pl.Struct({"userId": pl.Int64, "id": pl.Int64, "title": pl.Utf8, "body": pl.Utf8})
print(
    pl.DataFrame({
        "url": [BASE_URL for _ in range(10)],
    })
    .with_columns(
        pl.struct(
            userId=3,
        ).alias("params"),
        pl.struct(
            title=pl.lit("foo"),
            body=pl.lit("bar"),
            userId=pl.arange(10),
        ).alias("body"),
    )
    .with_columns(
        pl.col("url").api.get().str.json_decode(pl.List(post)).alias("get"),
        pl.col("url").api.aget().str.json_decode(pl.List(post)).alias("aget"),
        pl.col("url").api.get(params=pl.col("params")).str.json_decode(pl.List(post)).alias("get_params"),
        pl.col("url").api.post(body=pl.col("body")).str.json_decode(post).alias("post"),
        pl.col("url").api.apost(body=pl.col("body")).str.json_decode(post).alias("apost"),
        pl.col("url").api.apost(params=pl.col("params")).alias("apost_params"),
    )
)
