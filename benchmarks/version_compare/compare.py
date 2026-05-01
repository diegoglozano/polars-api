"""Drive bench_runner.py inside two isolated venvs and compare results."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent
RUNNER = HERE / "bench_runner.py"


def run_in(venv: Path, label: str, port: int, out: Path, repeats: int) -> None:
    py = venv / "bin" / "python"
    cmd = [
        str(py), str(RUNNER),
        "--label", label,
        "--port", str(port),
        "--repeats", str(repeats),
        "--out", str(out),
    ]
    print(f"\n=== Running {label} via {py} ===", flush=True)
    subprocess.run(cmd, check=True)


def fmt(v: float, w: int = 8, p: int = 1) -> str:
    if v != v:  # NaN
        return f"{'-':>{w}}"
    return f"{v:>{w}.{p}f}"


def compare(a: dict, b: dict) -> None:
    la, lb = a["label"], b["label"]
    print()
    print("=" * 100)
    print(f"polars-api {la} (httpx-async)  vs  polars-api {lb} (aiohttp-async)")
    print(f"  python={a['python']}  polars={a['polars']}  payload={a['payload_bytes']}B")
    print("=" * 100)

    print("\nASYNC  (rps = req/s, higher is better; latency is per-request elapsed_ms)")
    header = (
        f"  {'N':>5} {'conc':>5}  "
        f"{'median_ms':>10} {'rps':>9} {'MB/s':>7} | "
        f"{'p50':>7} {'p95':>7} {'p99':>7} {'max':>7}"
    )
    print("\n  " + la)
    print(header)
    for s in a["async"]:
        print(
            f"  {s['n_requests']:>5} {s['concurrency']:>5}  "
            f"{fmt(s['wall_total_s_median'] * 1000, 10)} "
            f"{fmt(s['throughput_rps'], 9)} "
            f"{fmt(s['throughput_MBps'], 7, 2)} | "
            f"{fmt(s['latency_ms_p50'], 7)} "
            f"{fmt(s['latency_ms_p95'], 7)} "
            f"{fmt(s['latency_ms_p99'], 7)} "
            f"{fmt(s['latency_ms_max'], 7)}"
        )
    print("\n  " + lb)
    print(header)
    for s in b["async"]:
        print(
            f"  {s['n_requests']:>5} {s['concurrency']:>5}  "
            f"{fmt(s['wall_total_s_median'] * 1000, 10)} "
            f"{fmt(s['throughput_rps'], 9)} "
            f"{fmt(s['throughput_MBps'], 7, 2)} | "
            f"{fmt(s['latency_ms_p50'], 7)} "
            f"{fmt(s['latency_ms_p95'], 7)} "
            f"{fmt(s['latency_ms_p99'], 7)} "
            f"{fmt(s['latency_ms_max'], 7)}"
        )

    print(f"\n  Speedup ({lb} vs {la})")
    print(f"  {'N':>5} {'conc':>5}  {'rps_x':>8} {'med_x':>8} {'p95_x':>8}")
    by_key_a = {(s["n_requests"], s["concurrency"]): s for s in a["async"]}
    for sb in b["async"]:
        sa = by_key_a.get((sb["n_requests"], sb["concurrency"]))
        if not sa:
            continue
        rps_x = sb["throughput_rps"] / sa["throughput_rps"] if sa["throughput_rps"] else float("nan")
        med_x = sa["wall_total_s_median"] / sb["wall_total_s_median"] if sb["wall_total_s_median"] else float("nan")
        p95_x = sa["latency_ms_p95"] / sb["latency_ms_p95"] if sb["latency_ms_p95"] else float("nan")
        print(
            f"  {sb['n_requests']:>5} {sb['concurrency']:>5}  "
            f"{fmt(rps_x, 8, 2)}x {fmt(med_x, 8, 2)}x {fmt(p95_x, 8, 2)}x"
        )

    print("\nSYNC  (always httpx in both versions; sanity baseline)")
    print(header.replace("conc", "    "))
    print(f"  {la}")
    for s in a["sync"]:
        print(
            f"  {s['n_requests']:>5} {'':>5}  "
            f"{fmt(s['wall_total_s_median'] * 1000, 10)} "
            f"{fmt(s['throughput_rps'], 9)} "
            f"{fmt(s['throughput_MBps'], 7, 2)} | "
            f"{fmt(s['latency_ms_p50'], 7)} "
            f"{fmt(s['latency_ms_p95'], 7)} "
            f"{fmt(s['latency_ms_p99'], 7)} "
            f"{fmt(s['latency_ms_max'], 7)}"
        )
    print(f"  {lb}")
    for s in b["sync"]:
        print(
            f"  {s['n_requests']:>5} {'':>5}  "
            f"{fmt(s['wall_total_s_median'] * 1000, 10)} "
            f"{fmt(s['throughput_rps'], 9)} "
            f"{fmt(s['throughput_MBps'], 7, 2)} | "
            f"{fmt(s['latency_ms_p50'], 7)} "
            f"{fmt(s['latency_ms_p95'], 7)} "
            f"{fmt(s['latency_ms_p99'], 7)} "
            f"{fmt(s['latency_ms_max'], 7)}"
        )


def main() -> None:
    repeats = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    venv_a = HERE / ".venv-0.2.0"
    venv_b = HERE / ".venv-0.3.0"
    out_a = HERE / "result-0.2.0.json"
    out_b = HERE / "result-0.3.0.json"

    run_in(venv_a, "0.2.0", port=18081, out=out_a, repeats=repeats)
    run_in(venv_b, "0.3.0", port=18082, out=out_b, repeats=repeats)

    a = json.loads(out_a.read_text())
    b = json.loads(out_b.read_text())
    compare(a, b)


if __name__ == "__main__":
    main()
