import json
import os
from pathlib import Path
import tempfile
import asyncio
import unittest
import uuid

from qmt_adapter import AlgoOrderRequest, AsyncQmtClient, OrderRequest
from tests.test_named_pipe_e2e import ACCOUNT_ID, BridgeHarness, FakeQmtApi


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

    async def test_async_algo_preview_place_and_query(self):
        order = AlgoOrderRequest(
            account_id=ACCOUNT_ID,
            instrument="601919.SH",
            side="BUY",
            quantity=1000,
            algo_order_id="ASYNC-ALGO-0001",
        )
        async with AsyncQmtClient(config_path=self.config_path) as client:
            preview = await client.preview_algo_order(order, timeout=5)
            receipt = await client.place_algo_order(order, timeout=5)
            current = await client.get_algo_order(receipt.algo_order_id, timeout=5)

        self.assertEqual(preview["resolved_quantity"], 1000)
        self.assertEqual(receipt.algo_order_id, order.algo_order_id)
        self.assertEqual(current["resolved_quantity"], 1000)


if __name__ == "__main__":
    unittest.main()
