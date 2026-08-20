import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
import uuid

from qmt_adapter import OrderRequest, QmtClient, RemoteError
from qmt_side import qmt_adapter_qmt as bridge


ACCOUNT_ID = "SIM-STOCK-001"


class FakeAccount(object):
    m_strAccountID = ACCOUNT_ID
    m_dAvailable = 123456.78


class FakePosition(object):
    m_strInstrumentID = "600000.SH"
    m_nVolume = 800


class FakeOrder(object):
    def __init__(self, remark, order_id):
        self.m_strRemark = remark
        self.m_strOrderSysID = order_id


class FakeContext(object):
    def is_last_bar(self):
        return True


class FakeQmtApi(object):
    def __init__(self):
        self.orders = []
        self.passorder_calls = []
        self.cancel_calls = []
        self.order_counter = 0
        self.raise_passorder = False

    def get_trade_detail_data(self, account_id, account_type, detail_type, *args):
        if account_id != ACCOUNT_ID or account_type != "STOCK":
            return []
        if detail_type == "ACCOUNT":
            return [FakeAccount()]
        if detail_type == "POSITION":
            return [FakePosition()]
        if detail_type == "ORDER":
            return []
        return []

    def passorder(self, *args):
        self.passorder_calls.append(args)
        if self.raise_passorder:
            raise RuntimeError("simulated uncertain passorder failure")
        self.order_counter += 1
        order_id = "QMT-ORDER-%04d" % self.order_counter
        self.orders.append(FakeOrder(args[9], order_id))
        callback_order = self.orders[-1]
        timer = threading.Timer(
            0.03, lambda: bridge.order_callback(None, callback_order)
        )
        timer.daemon = True
        timer.start()

    def can_cancel_order(self, qmt_order_id, account_id, account_type):
        return (
            any(order.m_strOrderSysID == qmt_order_id for order in self.orders)
            and account_id == ACCOUNT_ID
            and account_type == "STOCK"
        )

    def cancel(self, *args):
        self.cancel_calls.append(args)
        return True


class BridgeHarness(object):
    def __init__(self, config, fake_api):
        self.config = config
        self.fake_api = fake_api
        self.ready = threading.Event()
        self.stop_event = threading.Event()
        self.thread = None
        self.error = None

    def start(self):
        bridge.get_trade_detail_data = self.fake_api.get_trade_detail_data
        bridge.passorder = self.fake_api.passorder
        bridge.can_cancel_order = self.fake_api.can_cancel_order
        bridge.cancel = self.fake_api.cancel
        self.thread = threading.Thread(target=self._run, name="fake-qmt-thread")
        self.thread.start()
        if not self.ready.wait(5):
            raise RuntimeError("fake QMT bridge did not start")
        if self.error:
            raise self.error

    def _run(self):
        runtime = None
        try:
            runtime = bridge.BridgeRuntime(self.config)
            bridge._RUNTIME = runtime
            runtime.start()
            self.ready.set()
            context = FakeContext()
            while not self.stop_event.wait(0.005):
                runtime.process_pending(context)
        except Exception as exc:
            self.error = exc
            self.ready.set()
        finally:
            if runtime is not None:
                runtime.stop()
            bridge._RUNTIME = None

    def stop(self):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(5)
            if self.thread.is_alive():
                raise RuntimeError("fake QMT bridge did not stop")
        if self.error:
            raise self.error


@unittest.skipUnless(os.name == "nt", "Windows named pipes are required")
class NamedPipeEndToEndTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        base = Path(self.temp_dir.name)
        self.config_path = base / "bridge_config.json"
        self.config = {
            "version": 1,
            "pipe_name": r"\\.\pipe\qmt_adapter_test_%s" % uuid.uuid4().hex,
            "auth_token": uuid.uuid4().hex + uuid.uuid4().hex,
            "environment": "SIMULATION",
            "db_path": str(base / "bridge.db"),
            "accounts": [
                {"account_id": ACCOUNT_ID, "account_type": "STOCK"}
            ],
            "timer_period": "1nSecond",
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

    def test_account_position_and_order_lifecycle(self):
        with QmtClient(
            config_path=self.config_path, client_id="named-pipe-e2e"
        ) as client:
            health = client.health()
            self.assertEqual(health["environment"], "SIMULATION")
            account = client.get_account(ACCOUNT_ID)
            self.assertEqual(account["count"], 1)
            self.assertEqual(account["items"][0]["available_cash"], 123456.78)

            positions = client.list_positions(ACCOUNT_ID)
            self.assertEqual(positions["count"], 1)
            self.assertEqual(positions["items"][0]["instrument"], "600000.SH")
            self.assertEqual(positions["items"][0]["total_quantity"], 800)

            order = OrderRequest(
                account_id=ACCOUNT_ID,
                instrument="600000.SH",
                side="BUY",
                quantity=100,
                price_type="LIMIT",
                limit_price="10.25",
                remark="e2e",
                client_order_id="CLIENT-E2E-0001",
            )
            receipt = client.place_order(
                order,
                wait_for="BROKER_ID",
                timeout=5,
            )
            self.assertEqual(receipt.qmt_order_id, "QMT-ORDER-0001")
            self.assertEqual(len(self.fake_api.passorder_calls), 1)
            self.assertEqual(self.fake_api.passorder_calls[0][0], 23)
            self.assertEqual(self.fake_api.passorder_calls[0][1], 1101)
            self.assertEqual(self.fake_api.passorder_calls[0][4], 11)
            self.assertEqual(self.fake_api.passorder_calls[0][8], 2)

            cancelled = client.cancel_order("CLIENT-E2E-0001", timeout=5)
            self.assertTrue(cancelled["cancel_requested"])
            self.assertEqual(cancelled["order_status"], "CANCEL_PENDING")
            self.assertEqual(len(self.fake_api.cancel_calls), 1)

    def test_same_client_order_id_replays_without_second_passorder(self):
        order = OrderRequest(
            account_id=ACCOUNT_ID,
            instrument="600000.SH",
            side="BUY",
            quantity=100,
            price_type="LIMIT",
            limit_price="10.2500",
            remark="idempotent-replay",
            client_order_id="CLIENT-IDEMPOTENT-0001",
        )
        with QmtClient(config_path=self.config_path) as client:
            first = client.place_order(order, wait_for="LOCAL_ACK", timeout=5)
            replay = client.place_order(order, wait_for="BROKER_ID", timeout=5)

        self.assertFalse(first.idempotent_replay)
        self.assertTrue(replay.idempotent_replay)
        self.assertEqual(first.client_order_id, replay.client_order_id)
        self.assertEqual(replay.qmt_order_id, "QMT-ORDER-0001")
        self.assertNotEqual(first.request_id, replay.request_id)
        self.assertEqual(replay.raw["original_request_id"], first.request_id)
        self.assertEqual(len(self.fake_api.passorder_calls), 1)

    def test_native_market_price_types_are_forwarded_to_passorder(self):
        cases = (
            ("600000.SH", "MARKET_SH_CONVERT_5_CANCEL", 42),
            ("600000.SH", "MARKET_SH_CONVERT_5_LIMIT", 43),
            ("600000.SH", "MARKET_PEER_PRICE_FIRST", 44),
            ("600000.SH", "MARKET_MINE_PRICE_FIRST", 45),
            ("000001.SZ", "MARKET_SZ_INSTBUSI_RESTCANCEL", 46),
            ("000001.SZ", "MARKET_SZ_CONVERT_5_CANCEL", 47),
            ("000001.SZ", "MARKET_SZ_FULL_OR_CANCEL", 48),
        )
        with QmtClient(config_path=self.config_path) as client:
            for index, (instrument, price_type, expected_pr_type) in enumerate(cases):
                order = OrderRequest(
                    account_id=ACCOUNT_ID,
                    instrument=instrument,
                    side="BUY",
                    quantity=100,
                    price_type=price_type,
                    client_order_id="CLIENT-MARKET-%02d" % index,
                )
                receipt = client.place_order(
                    order, wait_for="BROKER_ID", timeout=5
                )
                self.assertTrue(receipt.qmt_order_id)
                call = self.fake_api.passorder_calls[index]
                self.assertEqual(call[4], expected_pr_type)
                self.assertEqual(call[5], 0.0)

        self.assertEqual(len(self.fake_api.passorder_calls), len(cases))

    def test_same_client_order_id_with_different_payload_conflicts(self):
        first_order = OrderRequest(
            account_id=ACCOUNT_ID,
            instrument="600000.SH",
            side="BUY",
            quantity=100,
            price_type="LIMIT",
            limit_price="10.25",
            client_order_id="CLIENT-CONFLICT-0001",
        )
        conflicting_order = OrderRequest(
            account_id=ACCOUNT_ID,
            instrument="600000.SH",
            side="BUY",
            quantity=200,
            price_type="LIMIT",
            limit_price="10.25",
            client_order_id="CLIENT-CONFLICT-0001",
        )
        with QmtClient(config_path=self.config_path) as client:
            client.place_order(first_order, wait_for="BROKER_ID", timeout=5)
            with self.assertRaises(RemoteError) as caught:
                client.place_order(conflicting_order, timeout=5)

        self.assertEqual(caught.exception.code, "CLIENT_ORDER_ID_CONFLICT")
        self.assertEqual(len(self.fake_api.passorder_calls), 1)

    def test_replay_survives_client_reconnect(self):
        order = OrderRequest(
            account_id=ACCOUNT_ID,
            instrument="600000.SH",
            side="BUY",
            quantity=100,
            price_type="LIMIT",
            limit_price="10.25",
            client_order_id="CLIENT-RECONNECT-0001",
        )
        with QmtClient(config_path=self.config_path) as client:
            first = client.place_order(order, wait_for="BROKER_ID", timeout=5)
        with QmtClient(config_path=self.config_path) as client:
            replay = client.place_order(order, wait_for="BROKER_ID", timeout=5)

        self.assertEqual(first.qmt_order_id, replay.qmt_order_id)
        self.assertTrue(replay.idempotent_replay)
        self.assertNotEqual(first.request_id, replay.request_id)
        self.assertEqual(replay.raw["original_request_id"], first.request_id)
        self.assertEqual(len(self.fake_api.passorder_calls), 1)

    def test_replay_survives_bridge_restart(self):
        order = OrderRequest(
            account_id=ACCOUNT_ID,
            instrument="600000.SH",
            side="BUY",
            quantity=100,
            price_type="LIMIT",
            limit_price="10.25",
            client_order_id="CLIENT-RESTART-0001",
        )
        with QmtClient(config_path=self.config_path) as client:
            first = client.place_order(order, wait_for="BROKER_ID", timeout=5)

        self.harness.stop()
        self.harness = BridgeHarness(self.config, self.fake_api)
        self.harness.start()

        with QmtClient(config_path=self.config_path) as client:
            replay = client.place_order(order, wait_for="BROKER_ID", timeout=5)

        self.assertEqual(first.qmt_order_id, replay.qmt_order_id)
        self.assertTrue(replay.idempotent_replay)
        self.assertEqual(len(self.fake_api.passorder_calls), 1)

    def test_uncertain_order_is_not_resubmitted(self):
        self.fake_api.raise_passorder = True
        order = OrderRequest(
            account_id=ACCOUNT_ID,
            instrument="600000.SH",
            side="BUY",
            quantity=100,
            price_type="LIMIT",
            limit_price="10.25",
            client_order_id="CLIENT-UNCERTAIN-0001",
        )
        with QmtClient(config_path=self.config_path) as client:
            with self.assertRaises(RemoteError) as first_error:
                client.place_order(order, timeout=5)
            with self.assertRaises(RemoteError) as replay_error:
                client.place_order(order, timeout=5)

        self.assertEqual(first_error.exception.code, "COMMAND_UNCERTAIN")
        self.assertEqual(replay_error.exception.code, "COMMAND_UNCERTAIN")
        self.assertTrue(replay_error.exception.data["idempotent_replay"])
        self.assertEqual(len(self.fake_api.passorder_calls), 1)


if __name__ == "__main__":
    unittest.main()
