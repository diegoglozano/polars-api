# polars-api

[![Release](https://img.shields.io/github/v/release/diegoglozano/polars-api)](https://img.shields.io/github/v/release/diegoglozano/polars-api)
[![Build status](https://img.shields.io/github/actions/workflow/status/diegoglozano/polars-api/main.yml?branch=main)](https://github.com/diegoglozano/polars-api/actions/workflows/main.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/diegoglozano/polars-api/branch/main/graph/badge.svg)](https://codecov.io/gh/diegoglozano/polars-api)
[![Commit activity](https://img.shields.io/github/commit-activity/m/diegoglozano/polars-api)](https://img.shields.io/github/commit-activity/m/diegoglozano/polars-api)
[![License](https://img.shields.io/github/license/diegoglozano/polars-api)](https://img.shields.io/github/license/diegoglozano/polars-api)

Polars extension for dealing with REST APIs

- **Github repository**: <https://github.com/diegoglozano/polars-api/>
- **Documentation** <https://diegoglozano.github.io/polars-api/>

## Installation

```sh
uv add polars-api
```

```sh
poetry add polars-api
```

```sh
pip install polars-api
```

## Usage

Just import the library as `import polars_api` and the new `api` namespace will be available.

In the following example:

- We set a base URL using [jsonplaceholder](https://jsonplaceholder.typicode.com/) as a fake REST API
- For each row, we generate a different body using a `struct` type
- Finally, we call different methods for getting the data:
  - `.api.get()`: sync GET
  - `.api.aget()`: async GET
  - `.api.post()`: sync POST
  - `.api.apost()`: async POST

These methods will return the result as a `string`, but with polars you can convert it easily in a struct and access its values using `.str.json_decode()` method.

```python
import polars as pl
import polars_api


BASE_URL = "https://jsonplaceholder.typicode.com/posts"
df = (
    pl
    .DataFrame({
        "url": [BASE_URL for _ in range(10)],
    })
    .with_columns(
        pl
        .struct(
            title=pl.lit("foo"),
            body=pl.lit("bar"),
            userId=pl.arange(10),
        )
        .alias("body"),
    )
    .with_columns(
        pl
        .col("url")
        .api.get()
        .str.json_decode()
        .alias("get"),
        pl
        .col("url")
        .api.aget()
        .str.json_decode()
        .alias("aget"),
        pl
        .col("url")
        .api.post(body=pl.col("body"))
        .str.json_decode()
        .alias("post"),
        pl
        .col("url")
        .api.apost(body=pl.col("body"))
        .str.json_decode()
        .alias("apost"),
    )
)

```

---

Repository initiated with [fpgmaas/cookiecutter-uv](https://github.com/fpgmaas/cookiecutter-uv).
