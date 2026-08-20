import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
import uuid

from qmt_adapter import AlgoOrderRequest, OrderRequest, QmtClient, RemoteError
from qmt_side import qmt_adapter_qmt as bridge


ACCOUNT_ID = "SIM-STOCK-001"


class FakeAccount(object):
    m_strAccountID = ACCOUNT_ID
    m_dAvailable = 20000000.0


class FakePosition(object):
    m_strInstrumentID = "600000.SH"
    m_nVolume = 800
    m_nCanUseVolume = 800


class FakeOrder(object):
    def __init__(self, remark, order_id, quantity):
        self.m_strRemark = remark
        self.m_strOrderSysID = order_id
        self.m_nVolumeTotalOriginal = quantity
        self.m_nVolumeTraded = 0
        self.m_nVolumeTotal = quantity
        self.m_nOrderStatus = 50
        self.m_strErrorMsg = ""


class FakeContext(object):
    def __init__(self, fake_api):
        self.fake_api = fake_api

    def is_last_bar(self):
        return True

    def get_full_tick(self, instruments):
        return {
            instrument: dict(self.fake_api.full_ticks[instrument])
            for instrument in instruments
            if instrument in self.fake_api.full_ticks
        }

    def get_instrument_detail(self, instrument):
        return {
            "InstrumentName": "fake-stock",
            "PreClose": 9.9,
            "UpStopPrice": 20.0,
            "DownStopPrice": 5.0,
            "PriceTick": 0.01,
            "InstrumentStatus": 0,
            "IsTrading": True,
        }


class FakeQmtApi(object):
    def __init__(self):
        self.orders = []
        self.passorder_calls = []
        self.passorder_call_times = []
        self.cancel_calls = []
        self.cancel_callback_times = []
        self.order_counter = 0
        self.raise_passorder = False
        self.full_ticks = {
            "600000.SH": {
                "lastPrice": 10.0,
                "askPrice": [10.00, 10.01, 10.02, 10.03, 10.04],
                "askVol": [500, 300, 200, 100, 100],
                "bidPrice": [9.99, 9.98, 9.97, 9.96, 9.95],
                "bidVol": [500, 300, 200, 100, 100],
            },
            "601919.SH": {
                "lastPrice": 13.0,
                "askPrice": [13.00, 13.01, 13.02, 13.03, 13.04],
                "askVol": [1000, 800, 600, 500, 400],
                "bidPrice": [12.99, 12.98, 12.97, 12.96, 12.95],
                "bidVol": [1000, 800, 600, 500, 400],
            },
        }

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
        self.passorder_call_times.append(time.monotonic())
        if self.raise_passorder:
            raise RuntimeError("simulated uncertain passorder failure")
        self.order_counter += 1
        order_id = "QMT-ORDER-%04d" % self.order_counter
        self.orders.append(FakeOrder(args[9], order_id, args[6]))
        callback_order = self.orders[-1]
        timer = threading.Timer(
            0.03, lambda: bridge.order_callback(None, callback_order)
        )
        timer.daemon = True
        timer.start()

    def can_cancel_order(self, qmt_order_id, account_id, account_type):
        return (
            any(
                order.m_strOrderSysID == qmt_order_id
                and order.m_nOrderStatus not in (53, 54, 56, 57)
                for order in self.orders
            )
            and account_id == ACCOUNT_ID
            and account_type == "STOCK"
        )

    def cancel(self, *args):
        self.cancel_calls.append(args)
        qmt_order_id = args[0]
        order = next(
            item for item in self.orders if item.m_strOrderSysID == qmt_order_id
        )

        def complete_cancel():
            order.m_nOrderStatus = 54
            order.m_nVolumeTotal = 0
            self.cancel_callback_times.append(time.monotonic())
            bridge.order_callback(None, order)

        timer = threading.Timer(0.03, complete_cancel)
        timer.daemon = True
        timer.start()
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
            context = FakeContext(self.fake_api)
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
            self.assertNotIn("environment", health)
            account = client.get_account(ACCOUNT_ID)
            self.assertEqual(account["count"], 1)
            self.assertEqual(account["items"][0]["available_cash"], 20000000.0)

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

    def test_algo_preview_place_query_and_replay(self):
        order = AlgoOrderRequest(
            account_id=ACCOUNT_ID,
            instrument="601919.SH",
            side="BUY",
            target_amount="1200000",
            algo_order_id="ALGO-E2E-0001",
        )
        with QmtClient(config_path=self.config_path) as client:
            preview = client.preview_algo_order(order, timeout=5)
            self.assertEqual(preview["resolved_quantity"], preview["planned_quantity"])
            self.assertLessEqual(float(preview["planned_notional"]), 1200000.0)
            self.assertEqual(preview["depth"]["last_close"], "9.9")
            self.assertAlmostEqual(preview["depth"]["change_percent"], 31.313131, places=5)
            self.assertEqual(preview["depth"]["price_limits"]["upper_limit"], "20")
            self.assertEqual(preview["depth"]["price_cage"]["buy_reference"], "13")
            self.assertEqual(preview["depth"]["price_cage"]["buy_maximum"], "13.26")
            self.assertEqual(len(preview["depth"]["quote"]["ask_levels"]), 5)
            self.assertEqual(len(preview["depth"]["quote"]["bid_levels"]), 5)
            self.assertEqual(
                sum(child["quantity"] for child in preview["children"]),
                preview["resolved_quantity"],
            )

            receipt = client.place_algo_order(order, timeout=5)
            initial_call_count = len(self.fake_api.passorder_calls)
            replay = client.place_algo_order(order, timeout=5)
            current = client.get_algo_order(order.algo_order_id, timeout=5)

        self.assertGreater(receipt.child_count, 1)
        self.assertEqual(initial_call_count, receipt.child_count)
        self.assertEqual(len(self.fake_api.passorder_calls), initial_call_count)
        self.assertTrue(replay.idempotent_replay)
        self.assertEqual(current["algo_order_id"], order.algo_order_id)
        self.assertEqual(
            sum(child["quantity"] for child in current["children"]),
            current["resolved_quantity"],
        )

    def test_reserved_twap_is_rejected_without_child_order(self):
        order = AlgoOrderRequest(
            account_id=ACCOUNT_ID,
            instrument="601919.SH",
            side="BUY",
            quantity=1000,
            algorithm="TWAP",
            algo_order_id="ALGO-TWAP-RESERVED-0001",
        )
        with QmtClient(config_path=self.config_path) as client:
            with self.assertRaises(RemoteError) as caught:
                client.place_algo_order(order, timeout=5)

        self.assertEqual(caught.exception.code, "ALGORITHM_NOT_IMPLEMENTED")
        self.assertEqual(len(self.fake_api.passorder_calls), 0)

    def test_algo_retry_waits_for_cancel_terminal_callback(self):
        order = AlgoOrderRequest(
            account_id=ACCOUNT_ID,
            instrument="601919.SH",
            side="BUY",
            quantity=100,
            params={"timeout_seconds": 0.15, "max_retries": 1},
            algo_order_id="ALGO-RETRY-0001",
        )
        with QmtClient(config_path=self.config_path) as client:
            client.place_algo_order(order, timeout=5)
            deadline = time.monotonic() + 3
            current = None
            while time.monotonic() < deadline:
                current = client.get_algo_order(order.algo_order_id, timeout=5)
                if current["current_attempt"] >= 1:
                    break
                time.sleep(0.02)

        self.assertIsNotNone(current)
        self.assertEqual(current["current_attempt"], 1)
        self.assertEqual(len(self.fake_api.passorder_calls), 2)
        self.assertTrue(self.fake_api.cancel_callback_times)
        self.assertGreaterEqual(
            self.fake_api.passorder_call_times[1],
            self.fake_api.cancel_callback_times[0],
        )


if __name__ == "__main__":
    unittest.main()
