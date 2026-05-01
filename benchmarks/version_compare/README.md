# polars-api 0.2.0 (httpx-async) vs 0.3.0 (aiohttp-async)

Each version is installed from PyPI into its own venv, then the same workload
runs against a shared local aiohttp server in both. The async path swapped
backends in 0.3.0 (`feat: switch async path to aiohttp for ~10-40x throughput`);
the sync path is httpx in both versions and serves as a sanity baseline.

## Run

```bash
# from this directory
uv venv --python 3.11 .venv-0.2.0
uv venv --python 3.11 .venv-0.3.0
uv pip install --python .venv-0.2.0/bin/python "polars-api==0.2.0" aiohttp
uv pip install --python .venv-0.3.0/bin/python "polars-api==0.3.0" aiohttp

python3 compare.py 5    # 5 = repeats per config
```

`compare.py` boots `bench_runner.py` inside each venv, points it at a fresh
local aiohttp server, and prints throughput / latency / speedup tables.

## What's measured

- **Wall-clock total** per batch (median of N repeats).
- **Throughput** in requests/second and MB/s.
- **Per-request latency** (`elapsed_ms` from `with_metadata=True`): p50, p95,
  p99, min/max.

Configs: async batches of (N=200, c=50), (N=1000, c=100), (N=2000, c=200);
sync batch of N=200 (single connection — slow on purpose, just a baseline).

## Results (Python 3.11.15, polars 1.40.1, 256-byte payload, 5 repeats)

```
ASYNC
  0.2.0 (httpx)
      N  conc  median_ms      rps    MB/s |    p50    p95    p99
    200    50      470.3    425.2    0.10 |   65.1  244.8  342.9
   1000   100     2739.2    365.1    0.09 |  129.1  636.0 1090.7
   2000   200    11140.7    179.5    0.04 | 1026.2 1349.5 1394.0

  0.3.0 (aiohttp)
      N  conc  median_ms      rps    MB/s |    p50    p95    p99
    200    50       34.4   5821.8    1.42 |    4.7   12.2   16.4
   1000   100      155.7   6422.8    1.57 |    9.0   24.6   54.4
   2000   200      346.5   5772.6    1.41 |   18.6   54.1   96.7

  Speedup 0.3.0 / 0.2.0
      N  conc    rps_x   med_x   p95_x
    200    50    13.7x   13.7x   20.0x
   1000   100    17.6x   17.6x   25.9x
   2000   200    32.2x   32.2x   24.9x

SYNC (httpx in both — sanity baseline)
  0.2.0    median=154.6ms  rps=1293.5  p50=0.6ms  p95=0.8ms
  0.3.0    median=152.6ms  rps=1310.8  p50=0.6ms  p95=0.8ms
```

Numbers will vary by host; the qualitative picture (1–2 orders of magnitude
async speedup, no regression on sync) is what to expect.
