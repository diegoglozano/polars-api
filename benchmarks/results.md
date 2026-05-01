# polars-api benchmarks

_Generated 2026-05-01T23:30:43+00:00_

## Environment

- Python: `3.11.15`
- Platform: `Linux 6.18.5 (x86_64)`
- CPU count: `4`
- polars: `1.19.0`
- polars-api: `0.0.1`
- httpx: `0.28.1`
- aiohttp: `3.13.5`

## Results

Local aiohttp echo server on `127.0.0.1`. Median of N repeats.
Higher rps is better.

### N=100, concurrency=50 (repeats=5)

| Client | Median (ms) | rps | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| polars-api (default client) | 28.8 | 3471.1 | 25.9 | 42.0 |
| polars-api (shared client) | 24.3 | 4122.0 | 21.3 | 29.7 |
| bare httpx (default limits) | 186.0 | 537.8 | 175.1 | 197.9 |
| bare httpx (tuned limits) | 425.2 | 235.2 | 374.3 | 453.5 |
| bare aiohttp | 32.9 | 3040.7 | 26.8 | 60.2 |

### N=500, concurrency=100 (repeats=5)

| Client | Median (ms) | rps | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| polars-api (default client) | 134.1 | 3727.9 | 95.6 | 168.4 |
| polars-api (shared client) | 116.7 | 4284.0 | 94.5 | 142.5 |
| bare httpx (default limits) | 1248.5 | 400.5 | 1207.2 | 1285.3 |
| bare httpx (tuned limits) | 5657.5 | 88.4 | 5583.1 | 5700.6 |
| bare aiohttp | 114.9 | 4351.5 | 101.0 | 154.2 |

### N=1000, concurrency=100 (repeats=5)

| Client | Median (ms) | rps | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| polars-api (default client) | 249.2 | 4012.4 | 198.0 | 276.4 |
| polars-api (shared client) | 267.8 | 3733.8 | 213.9 | 286.8 |
| bare httpx (default limits) | 2819.9 | 354.6 | 1604.5 | 2883.5 |
| bare httpx (tuned limits) | 7299.3 | 137.0 | 6797.2 | 8180.6 |
| bare aiohttp | 212.7 | 4701.1 | 171.5 | 290.3 |

### N=2000, concurrency=200 (repeats=5)

| Client | Median (ms) | rps | min (ms) | max (ms) |
| --- | ---: | ---: | ---: | ---: |
| polars-api (default client) | 502.5 | 3980.1 | 492.0 | 545.1 |
| polars-api (shared client) | 530.2 | 3772.2 | 465.7 | 657.8 |
| bare httpx (default limits) | 15946.4 | 125.4 | 15077.5 | 19285.7 |
| bare httpx (tuned limits) | 16316.9 | 122.6 | 11745.4 | 18440.0 |
| bare aiohttp | 446.8 | 4476.5 | 403.0 | 498.9 |
