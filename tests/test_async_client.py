import json
import os
from pathlib import Path
import tempfile
import asyncio
import unittest
import uuid

from qmt_adapter import (
    AlgoOrderRequest,
    AsyncQmtClient,
    NewIssueSubscriptionRequest,
    OrderRequest,
    ReverseRepoRequest,
)
from qmt_side import qmt_adapter_qmt as bridge
from tests.test_named_pipe_e2e import (
    ACCOUNT_ID,
    BridgeHarness,
    FakeQmtApi,
    FakeTrade,
)


@unittest.skipUnless(os.name == "nt", "Windows named pipes are required")
class AsyncClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        base = Path(self.temp_dir.name)
        self.config_path = base / "bridge_config.json"
        self.config = {
            "version": 1,
            "pipe_name": r"\\.\pipe\qmt_adapter_async_test_%s" % uuid.uuid4().hex,
            "auth_token": uuid.uuid4().hex + uuid.uuid4().hex,
            "db_path": str(base / "bridge.db"),
            "accounts": [{"account_id": ACCOUNT_ID, "account_type": "STOCK"}],
            "timer_period": "10nMilliSecond",
            "reconcile_interval_seconds": 1000000000000.0,
            "max_commands_per_tick": 20,
            "max_pending_commands": 100,
            "max_message_size": 1048576,
            "qmt_remark_max_bytes": 64,
        }
        self.config_path.write_text(
            json.dumps(self.config, ensure_ascii=True), encoding="utf-8"
        )
        self.fake_api = FakeQmtApi()
        self.harness = BridgeHarness(self.config, self.fake_api)
        self.harness.start()

    def tearDown(self):
        self.harness.stop()
        self.temp_dir.cleanup()

    async def test_async_queries_and_broker_id_callback(self):
        async with AsyncQmtClient(
            config_path=self.config_path, client_id="async-e2e"
        ) as client:
            health = await client.health()
            self.assertNotIn("environment", health)

            account = await client.get_account(ACCOUNT_ID)
            self.assertEqual(account["items"][0]["available_cash"], 20000000.0)

            positions = await client.list_positions(ACCOUNT_ID)
            self.assertEqual(positions["items"][0]["instrument"], "600000.SH")

            quote = await client.get_quote(
                "600000.SH", include_raw=True, timeout=5
            )
            quotes = await client.get_quotes(
                ["601919.SH", "600000.SH"], include_raw=True, timeout=5
            )
            self.assertEqual(quote["raw"]["full_tick"]["lastPrice"], 10.0)
            self.assertEqual(quotes["count"], 2)

            receipt = await client.place_order(
                OrderRequest(
                    account_id=ACCOUNT_ID,
                    instrument="600000.SH",
                    side="BUY",
                    quantity=100,
                    price_type="LIMIT",
                    limit_price="10.25",
                    remark="async-e2e",
                ),
                wait_for="BROKER_ID",
                timeout=5.0,
            )
            self.assertEqual(receipt.qmt_order_id, "QMT-ORDER-0001")

    async def test_async_new_issue_and_reverse_repo_interfaces(self):
        async with AsyncQmtClient(config_path=self.config_path) as client:
            issues = await client.list_new_issues("STOCK", timeout=5)
            quota = await client.get_new_issue_quota(ACCOUNT_ID, timeout=5)
            repo = await client.place_reverse_repo(
                ReverseRepoRequest(
                    account_id=ACCOUNT_ID,
                    instrument="204001.SH",
                    amount=10000,
                    annual_rate="1.8",
                    client_order_id="ASYNC-REPO-0001",
                ),
                wait_for="BROKER_ID",
                timeout=5,
            )
            subscription = await client.subscribe_new_issue(
                NewIssueSubscriptionRequest(
                    account_id=ACCOUNT_ID,
                    instrument="754001.SH",
                    issue_type="BOND",
                    quantity=10,
                    client_order_id="ASYNC-BOND-0001",
                ),
                wait_for="BROKER_ID",
                timeout=5,
            )

        self.assertEqual(issues["count"], 1)
        self.assertEqual(quota["limits"]["SH"], 10000)
        self.assertTrue(repo.qmt_order_id)
        self.assertTrue(subscription.qmt_order_id)
        self.assertEqual(len(self.fake_api.passorder_calls), 2)

    async def test_async_wait_and_trade_query(self):
        order = OrderRequest(
            account_id=ACCOUNT_ID,
            instrument="600000.SH",
            side="BUY",
            quantity=100,
            price_type="LIMIT",
            limit_price="10.25",
            client_order_id="ASYNC-WAIT-0001",
        )
        async with AsyncQmtClient(config_path=self.config_path) as client:
            await client.place_order(order, wait_for="BROKER_ID", timeout=5)
            wait_task = asyncio.create_task(
                client.wait_order(order.client_order_id, timeout=2)
            )
            await asyncio.sleep(0.05)
            trade = FakeTrade(
                self.fake_api.orders[0], "ASYNC-TRADE-0001", 10.2, 100
            )
            self.fake_api.trades.append(trade)
            bridge.deal_callback(None, trade)
            current = await wait_task
            trades = await client.list_trades(
                ACCOUNT_ID,
                scope="ADAPTER",
                client_order_id=order.client_order_id,
                timeout=5,
            )

        self.assertEqual(current["order_status"], "FILLED")
        self.assertEqual(current["filled_quantity"], 100)
        self.assertEqual(trades["count"], 1)
        self.assertEqual(trades["items"][0]["trade_id"], "ASYNC-TRADE-0001")

    async def test_fifty_serialized_orders_scheduled_every_fifty_ms(self):
        async with AsyncQmtClient(
            config_path=self.config_path, client_id="async-order-stress"
        ) as client:
            loop = asyncio.get_running_loop()
            schedule_start = loop.time()
            tasks = []
            for index in range(50):
                target = schedule_start + index * 0.05
                remaining = target - loop.time()
                if remaining > 0:
                    await asyncio.sleep(remaining)
                tasks.append(
                    asyncio.create_task(
                        client.place_order(
                            OrderRequest(
                                account_id=ACCOUNT_ID,
                                instrument="600000.SH",
                                side="BUY",
                                quantity=100,
                                price_type="LIMIT",
                                limit_price="10.25",
                                remark="async-stress-%02d" % index,
                            ),
                            wait_for="BROKER_ID",
                            timeout=5.0,
                        )
                    )
                )

            receipts = await asyncio.gather(*tasks)
            qmt_order_ids = [receipt.qmt_order_id for receipt in receipts]
            self.assertEqual(len(qmt_order_ids), 50)
            self.assertEqual(len(set(qmt_order_ids)), 50)
            self.assertEqual(len(self.fake_api.passorder_calls), 50)

    async def test_async_batch_orders_use_single_serial_schedule(self):
        orders = [
            OrderRequest(
                account_id=ACCOUNT_ID,
                instrument="600000.SH",
                side="BUY",
                quantity=100,
                price_type="LIMIT",
                limit_price="10.25",
                remark="async-batch-%02d" % index,
                client_order_id="CLIENT-ASYNC-BATCH-%02d" % index,
            )
            for index in range(3)
        ]

        async with AsyncQmtClient(config_path=self.config_path) as client:
            batch_task = asyncio.create_task(
                client.place_orders(
                    orders,
                    interval_ms=50,
                    wait_for="BROKER_ID",
                    timeout=5,
                )
            )
            await asyncio.sleep(0.01)
            self.assertFalse(batch_task.done())
            receipts = await batch_task

        self.assertEqual(
            [item.client_order_id for item in receipts],
            [item.client_order_id for item in orders],
        )
        self.assertEqual(len(self.fake_api.passorder_calls), 3)
        gaps = [
            later - earlier
            for earlier, later in zip(
                self.fake_api.passorder_call_times,
                self.fake_api.passorder_call_times[1:],
            )
        ]
        self.assertTrue(all(gap >= 0.04 for gap in gaps), gaps)

    async def test_async_algo_preview_place_and_query(self):
        order = AlgoOrderRequest(
            account_id=ACCOUNT_ID,
            instrument="601919.SH",
            side="BUY",
            target_amount="1200000",
            algo_order_id="ASYNC-ALGO-0001",
        )
        async with AsyncQmtClient(config_path=self.config_path) as client:
            preview = await client.preview_algo_order(order, timeout=5)
            receipt = await client.place_algo_order(order, timeout=5)
            call_count_at_receipt = len(self.fake_api.passorder_calls)
            current = await client.get_algo_order(receipt.algo_order_id, timeout=5)

        self.assertGreater(preview["resolved_quantity"], 0)
        self.assertEqual(receipt.algo_order_id, order.algo_order_id)
        self.assertGreater(receipt.child_count, 1)
        self.assertEqual(call_count_at_receipt, 1)
        self.assertEqual(current["resolved_quantity"], preview["resolved_quantity"])


if __name__ == "__main__":
    unittest.main()
