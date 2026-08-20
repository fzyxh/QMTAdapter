import argparse
import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import random
import statistics
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from qmt_adapter import AsyncQmtClient, OrderRequest, QmtClient


CONFIRM_TEXT = "PLACE_50_SIMULATION_ORDERS"
INSTRUMENT = "601919.SH"
ORDER_COUNT = 50
INTERVAL_SECONDS = 0.05
QUANTITIES = (100, 200, 300, 400, 500)


def _position_quantity(response):
    target_code = INSTRUMENT.split(".", 1)[0]
    for item in response.get("items", []):
        code = str(item.get("instrument", "")).split(".", 1)[0]
        if code == target_code:
            return int(item.get("total_quantity") or 0)
    return 0


def _parse_utc(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _percentile(values, fraction):
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * fraction))
    return ordered[index]


def _latency_summary(values):
    if not values:
        return None
    return {
        "mean": round(statistics.mean(values), 3),
        "median": round(statistics.median(values), 3),
        "p95": round(_percentile(values, 0.95), 3),
        "max": round(max(values), 3),
    }


def _build_orders(seed, mode, account_id):
    rng = random.Random(seed)
    quantities = [rng.choice(QUANTITIES) for unused in range(ORDER_COUNT)]
    batch_id = uuid.uuid4().hex[:12]
    orders = [
        OrderRequest(
            account_id=account_id,
            instrument=INSTRUMENT,
            side="BUY",
            quantity=quantity,
            price_type="COUNTERPARTY",
            remark="%s-%s-%02d" % (mode, batch_id, index + 1),
            client_order_id=str(uuid.uuid4()),
        )
        for index, quantity in enumerate(quantities)
    ]
    return batch_id, quantities, orders


def _validate_client(client, account_id):
    if client.hello.get("environment") != "SIMULATION":
        raise RuntimeError("bridge environment is not SIMULATION")
    configured_ids = {
        str(item.get("account_id")) for item in client.hello.get("accounts", [])
    }
    if account_id not in configured_ids:
        raise RuntimeError("configured simulation account does not match")


async def _submit_one(client, order, scheduled_at, loop):
    coroutine_started = loop.time()
    try:
        receipt = await client.place_order(
            order,
            wait_for="LOCAL_ACK",
            timeout=10.0,
        )
        ended = loop.time()
        return {
            "client_order_id": receipt.client_order_id,
            "quantity": order.quantity,
            "ok": True,
            "scheduled_lag_ms": round((coroutine_started - scheduled_at) * 1000.0, 3),
            "local_ack_ms": round((ended - coroutine_started) * 1000.0, 3),
            "error": None,
        }
    except Exception as exc:
        ended = loop.time()
        return {
            "client_order_id": order.client_order_id,
            "quantity": order.quantity,
            "ok": False,
            "scheduled_lag_ms": round((coroutine_started - scheduled_at) * 1000.0, 3),
            "local_ack_ms": round((ended - coroutine_started) * 1000.0, 3),
            "error": "%s: %s" % (type(exc).__name__, exc),
        }


async def _wait_for_order_ids(
    client, account_id, client_order_ids, timeout=10.0
):
    deadline = time.monotonic() + timeout
    selected = {}
    while time.monotonic() < deadline:
        response = await client.list_orders(account_id=account_id)
        selected = {
            item["client_order_id"]: item
            for item in response.get("items", [])
            if item.get("client_order_id") in client_order_ids
        }
        if len(selected) == len(client_order_ids) and all(
            item.get("qmt_order_id") for item in selected.values()
        ):
            break
        await asyncio.sleep(0.01)
    return selected


async def _wait_for_position(
    client, account_id, expected_quantity, timeout=15.0
):
    deadline = time.monotonic() + timeout
    quantity = 0
    while time.monotonic() < deadline:
        positions = await client.list_positions(account_id)
        quantity = _position_quantity(positions)
        if quantity >= expected_quantity:
            break
        await asyncio.sleep(0.05)
    return quantity


def _wait_for_order_ids_sync(
    client, account_id, client_order_ids, timeout=10.0
):
    deadline = time.monotonic() + timeout
    selected = {}
    while time.monotonic() < deadline:
        response = client.list_orders(account_id=account_id)
        selected = {
            item["client_order_id"]: item
            for item in response.get("items", [])
            if item.get("client_order_id") in client_order_ids
        }
        if len(selected) == len(client_order_ids) and all(
            item.get("qmt_order_id") for item in selected.values()
        ):
            break
        time.sleep(0.01)
    return selected


def _wait_for_position_sync(
    client, account_id, expected_quantity, timeout=15.0
):
    deadline = time.monotonic() + timeout
    quantity = 0
    while time.monotonic() < deadline:
        positions = client.list_positions(account_id)
        quantity = _position_quantity(positions)
        if quantity >= expected_quantity:
            break
        time.sleep(0.05)
    return quantity


def _make_report(
    mode,
    account_id,
    batch_id,
    seed,
    quantities,
    submission_results,
    order_rows,
    submission_wall_ms,
    position_before,
    position_after,
):
    created_rows = sorted(
        (item for item in order_rows.values() if item.get("created_at")),
        key=lambda item: item["created_at"],
    )
    created_times = [_parse_utc(item["created_at"]) for item in created_rows]
    qmt_gaps_ms = [
        (created_times[index] - created_times[index - 1]) * 1000.0
        for index in range(1, len(created_times))
    ]
    local_ack_ms = [
        item["local_ack_ms"] for item in submission_results if item["ok"]
    ]
    return {
        "mode": mode,
        "batch_id": batch_id,
        "seed": seed,
        "account_id": account_id,
        "instrument": INSTRUMENT,
        "side": "BUY",
        "price_type": "COUNTERPARTY",
        "count": ORDER_COUNT,
        "target_interval_ms": INTERVAL_SECONDS * 1000.0,
        "quantities": quantities,
        "requested_total_quantity": sum(quantities),
        "successful_local_ack_count": sum(item["ok"] for item in submission_results),
        "failed_local_ack_count": sum(not item["ok"] for item in submission_results),
        "qmt_order_id_count": sum(
            bool(item.get("qmt_order_id")) for item in order_rows.values()
        ),
        "submission_wall_ms": round(submission_wall_ms, 3),
        "local_ack_ms": _latency_summary(local_ack_ms),
        "qmt_created_gap_ms": _latency_summary(qmt_gaps_ms),
        "position_before": position_before,
        "position_after": position_after,
        "position_increment": position_after - position_before,
        "submission_results": submission_results,
        "orders": [order_rows[key] for key in sorted(order_rows)],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


async def run_async(seed, account_id):
    mode = "async"
    batch_id, quantities, orders = _build_orders(seed, mode, account_id)

    async with AsyncQmtClient(client_id="live-order-stress") as client:
        _validate_client(client, account_id)

        positions_before = await client.list_positions(account_id)
        position_before = _position_quantity(positions_before)

        loop = asyncio.get_running_loop()
        schedule_start = loop.time()
        tasks = []
        for index, order in enumerate(orders):
            target = schedule_start + index * INTERVAL_SECONDS
            remaining = target - loop.time()
            if remaining > 0:
                await asyncio.sleep(remaining)
            tasks.append(
                asyncio.create_task(_submit_one(client, order, target, loop))
            )

        submission_results = await asyncio.gather(*tasks)
        submission_ended = loop.time()
        successful_ids = {
            item["client_order_id"] for item in submission_results if item["ok"]
        }
        order_rows = await _wait_for_order_ids(
            client, account_id, successful_ids
        )
        expected_increment = sum(
            item["quantity"] for item in submission_results if item["ok"]
        )
        position_after = await _wait_for_position(
            client, account_id, position_before + expected_increment
        )
        final_orders = await client.list_orders(account_id=account_id)
        order_rows = {
            item["client_order_id"]: item
            for item in final_orders.get("items", [])
            if item.get("client_order_id") in successful_ids
        }

    return _make_report(
        mode,
        account_id,
        batch_id,
        seed,
        quantities,
        submission_results,
        order_rows,
        (submission_ended - schedule_start) * 1000.0,
        position_before,
        position_after,
    )


def run_sync(seed, account_id):
    mode = "sync"
    batch_id, quantities, orders = _build_orders(seed, mode, account_id)
    with QmtClient(client_id="live-order-stress-sync") as client:
        _validate_client(client, account_id)
        position_before = _position_quantity(client.list_positions(account_id))
        schedule_start = time.perf_counter()
        submission_results = []
        for index, order in enumerate(orders):
            target = schedule_start + index * INTERVAL_SECONDS
            remaining = target - time.perf_counter()
            if remaining > 0:
                time.sleep(remaining)
            started = time.perf_counter()
            try:
                receipt = client.place_order(
                    order,
                    wait_for="LOCAL_ACK",
                    timeout=10.0,
                )
                ended = time.perf_counter()
                submission_results.append(
                    {
                        "client_order_id": receipt.client_order_id,
                        "quantity": order.quantity,
                        "ok": True,
                        "scheduled_lag_ms": round((started - target) * 1000.0, 3),
                        "local_ack_ms": round((ended - started) * 1000.0, 3),
                        "error": None,
                    }
                )
            except Exception as exc:
                ended = time.perf_counter()
                submission_results.append(
                    {
                        "client_order_id": order.client_order_id,
                        "quantity": order.quantity,
                        "ok": False,
                        "scheduled_lag_ms": round((started - target) * 1000.0, 3),
                        "local_ack_ms": round((ended - started) * 1000.0, 3),
                        "error": "%s: %s" % (type(exc).__name__, exc),
                    }
                )
        submission_ended = time.perf_counter()
        successful_ids = {
            item["client_order_id"] for item in submission_results if item["ok"]
        }
        order_rows = _wait_for_order_ids_sync(
            client, account_id, successful_ids
        )
        expected_increment = sum(
            item["quantity"] for item in submission_results if item["ok"]
        )
        position_after = _wait_for_position_sync(
            client, account_id, position_before + expected_increment
        )
        final_orders = client.list_orders(account_id=account_id)
        order_rows = {
            item["client_order_id"]: item
            for item in final_orders.get("items", [])
            if item.get("client_order_id") in successful_ids
        }

    return _make_report(
        mode,
        account_id,
        batch_id,
        seed,
        quantities,
        submission_results,
        order_rows,
        (submission_ended - schedule_start) * 1000.0,
        position_before,
        position_after,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Place exactly 50 simulation BUY orders for 601919.SH."
    )
    parser.add_argument("--confirm", required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--seed", type=int, default=20260819)
    parser.add_argument("--mode", choices=("sync", "async"), required=True)
    args = parser.parse_args()
    if args.confirm != CONFIRM_TEXT:
        parser.error("--confirm must equal %s" % CONFIRM_TEXT)

    if args.mode == "sync":
        report = run_sync(args.seed, args.account_id)
    else:
        report = asyncio.run(run_async(args.seed, args.account_id))
    report_dir = Path(__file__).resolve().parents[1] / "stress_results"
    report_dir.mkdir(exist_ok=True)
    report_path = report_dir / (
        "order_stress_%s_%s.json" % (report["mode"], report["batch_id"])
    )
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    summary = {
        key: value
        for key, value in report.items()
        if key not in ("orders", "submission_results")
    }
    summary["report_path"] = str(report_path)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if report["failed_local_ack_count"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
