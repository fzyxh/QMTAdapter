import argparse
import asyncio
import json
from pathlib import Path
import statistics
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from qmt_adapter import AsyncQmtClient, QmtClient


def _percentile(values, percentile):
    if not values:
        return None
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * percentile))
    return ordered[index]


def _summary(mode, count, interval_seconds, connect_ms, records):
    latencies = [item["latency_ms"] for item in records]
    lags = [item["start_lag_ms"] for item in records]
    starts = [item["actual_start"] for item in records]
    gaps = [(starts[i] - starts[i - 1]) * 1000.0 for i in range(1, len(starts))]
    return {
        "mode": mode,
        "count": count,
        "target_interval_ms": interval_seconds * 1000.0,
        "connect_ms": round(connect_ms, 3),
        "total_ms": round((records[-1]["ended"] - records[0]["target"]) * 1000.0, 3),
        "call_latency_ms": {
            "median": round(statistics.median(latencies), 3),
            "p95": round(_percentile(latencies, 0.95), 3),
            "max": round(max(latencies), 3),
        },
        "start_lag_ms": {
            "median": round(statistics.median(lags), 3),
            "p95": round(_percentile(lags, 0.95), 3),
            "max": round(max(lags), 3),
        },
        "actual_start_gap_ms": {
            "min": round(min(gaps), 3) if gaps else None,
            "median": round(statistics.median(gaps), 3) if gaps else None,
            "p95": round(_percentile(gaps, 0.95), 3) if gaps else None,
            "max": round(max(gaps), 3) if gaps else None,
        },
    }


def _sync_call(client, command, account_id, timeout):
    if command == "health":
        return client.health(timeout=timeout)
    if command == "account":
        return client.get_account(account_id, timeout=timeout)
    return client.list_positions(account_id, timeout=timeout)


async def _async_call(client, command, account_id, timeout):
    if command == "health":
        return await client.health(timeout=timeout)
    if command == "account":
        return await client.get_account(account_id, timeout=timeout)
    return await client.list_positions(account_id, timeout=timeout)


def run_sync(config_path, command, count, interval_seconds, timeout):
    started = time.perf_counter()
    with QmtClient(config_path=config_path, client_id="sync-stress") as client:
        connect_ms = (time.perf_counter() - started) * 1000.0
        account_id = client.hello["accounts"][0]["account_id"]
        schedule_start = time.perf_counter()
        records = []
        for index in range(count):
            target = schedule_start + index * interval_seconds
            remaining = target - time.perf_counter()
            if remaining > 0:
                time.sleep(remaining)
            actual_start = time.perf_counter()
            _sync_call(client, command, account_id, timeout)
            ended = time.perf_counter()
            records.append(
                {
                    "target": target,
                    "actual_start": actual_start,
                    "ended": ended,
                    "start_lag_ms": (actual_start - target) * 1000.0,
                    "latency_ms": (ended - actual_start) * 1000.0,
                }
            )
    return _summary("sync", count, interval_seconds, connect_ms, records)


async def run_async(config_path, command, count, interval_seconds, timeout):
    started = time.perf_counter()
    async with AsyncQmtClient(
        config_path=config_path, client_id="async-stress"
    ) as client:
        connect_ms = (time.perf_counter() - started) * 1000.0
        account_id = client.hello["accounts"][0]["account_id"]
        loop = asyncio.get_running_loop()
        schedule_start = loop.time()
        records = []
        for index in range(count):
            target = schedule_start + index * interval_seconds
            remaining = target - loop.time()
            if remaining > 0:
                await asyncio.sleep(remaining)
            actual_start = loop.time()
            await _async_call(client, command, account_id, timeout)
            ended = loop.time()
            records.append(
                {
                    "target": target,
                    "actual_start": actual_start,
                    "ended": ended,
                    "start_lag_ms": (actual_start - target) * 1000.0,
                    "latency_ms": (ended - actual_start) * 1000.0,
                }
            )
    return _summary("async", count, interval_seconds, connect_ms, records)


def main():
    parser = argparse.ArgumentParser(
        description="Compare serialized synchronous and asyncio QMT calls."
    )
    parser.add_argument("--mode", choices=("sync", "async", "both"), default="both")
    parser.add_argument(
        "--command", choices=("health", "account", "position"), default="account"
    )
    parser.add_argument("--count", type=int, default=50)
    parser.add_argument("--interval-ms", type=float, default=50.0)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--config")
    args = parser.parse_args()

    if args.count <= 0 or args.interval_ms < 0:
        parser.error("count must be positive and interval-ms must be non-negative")

    interval_seconds = args.interval_ms / 1000.0
    results = []
    if args.mode in ("sync", "both"):
        results.append(
            run_sync(
                args.config,
                args.command,
                args.count,
                interval_seconds,
                args.timeout,
            )
        )
    if args.mode in ("async", "both"):
        results.append(
            asyncio.run(
                run_async(
                    args.config,
                    args.command,
                    args.count,
                    interval_seconds,
                    args.timeout,
                )
            )
        )
    print(json.dumps({"command": args.command, "results": results}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
