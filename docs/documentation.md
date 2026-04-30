---
title: API reference
description: Full API reference for polars-api — the .api expression namespace registered on Polars.
---

# API reference

`polars-api` registers an `api` namespace on every Polars expression. Import the package once and the namespace becomes available on any expression that resolves to a URL string.

```python
import polars as pl
import polars_api  # noqa: F401  — registers the `.api` namespace
```

## Methods

| Method                               | HTTP verb | Mode  |
| ------------------------------------ | --------- | ----- |
| [`get`](#polars_api.api.Api.get)     | GET       | sync  |
| [`aget`](#polars_api.api.Api.aget)   | GET       | async |
| [`post`](#polars_api.api.Api.post)   | POST      | sync  |
| [`apost`](#polars_api.api.Api.apost) | POST      | async |

All methods return a `pl.Expr` of dtype `Utf8` containing the response body for each row. Use `.str.json_decode()` to parse JSON responses.

## `polars_api.Api`

::: polars_api.api.Api
