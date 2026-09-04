import contextlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
import uuid

from qmt_adapter import (
    AlgoOrderRequest,
    NewIssueSubscriptionRequest,
    OrderRequest,
    QmtClient,
    RemoteError,
    ReverseRepoRequest,
    RequestTimeout,
    ValidationError,
    __version__,
)
from qmt_side import qmt_adapter_qmt as bridge


ACCOUNT_ID = "SIM-STOCK-001"


class FakeAccount(object):
    m_strAccountID = ACCOUNT_ID
    m_dBalance = 20008800.1250000001
    m_dAvailable = 20000000.0
    m_dStockValue = 8800.125000000002
    m_dFetchBalance = 19999000.5
    m_dFrozenCash = 1000.25
    m_dPositionProfit = 600.1250000000001


class FakePosition(object):
    m_strInstrumentID = "600000.SH"
    m_strInstrumentName = "浦发银行"
    m_nVolume = 800
    m_nCanUseVolume = 800
    m_nFrozenVolume = 0
    m_dOpenPrice = 10.25
    m_dLastPrice = 11.0
    m_dMarketValue = 8800.125000000002
    m_dPositionProfit = 600.1250000000001


class FakeSentinelPosition(object):
    m_strInstrumentID = "131990"
    m_strExchangeID = "SZ"
    m_strInstrumentName = "national-standard-bond"
    m_nVolume = 4420
    m_nCanUseVolume = 4420
    m_nFrozenVolume = 0
    m_dOpenPrice = 0.0
    m_dLastPrice = sys.float_info.max
    m_dMarketValue = float("inf")
    m_dPositionProfit = 0.0


class FakeOrder(object):
    def __init__(
        self,
        remark,
        order_id,
        quantity,
        instrument="600000.SH",
        side="BUY",
    ):
        self.m_strAccountID = ACCOUNT_ID
        self.m_strRemark = remark
        self.m_strOrderSysID = order_id
        self.m_strInstrumentID = instrument.split(".", 1)[0]
        self.m_strExchangeID = instrument.rsplit(".", 1)[-1]
        self.m_strOptName = "买入" if side == "BUY" else "卖出"
        self.m_nVolumeTotalOriginal = quantity
        self.m_nVolumeTraded = 0
        self.m_nVolumeTotal = quantity
        self.m_nOrderStatus = 50
        self.m_dTradedPrice = 0.0
        self.m_dTradeAmount = 0.0
        self.m_strCancelInfo = ""
        self.m_strErrorMsg = ""


class FakeTrade(object):
    def __init__(self, order, trade_id, price, quantity):
        self.m_strAccountID = ACCOUNT_ID
        self.m_strTradeID = trade_id
        self.m_strOrderSysID = order.m_strOrderSysID
        self.m_strRemark = order.m_strRemark
        self.m_strInstrumentID = order.m_strInstrumentID
        self.m_strExchangeID = order.m_strExchangeID
        self.m_strOptName = order.m_strOptName
        self.m_dPrice = price
        self.m_nVolume = quantity
        self.m_dTradeAmount = price * quantity
        self.m_dComssion = 1.23
        self.m_strTradeDate = "20260821"
        self.m_strTradeTime = "14:30:00"


class FakeSequence(list):
    def tolist(self):
        return list(self)

    @property
    def iloc(self):
        return self

    def shift(self, periods=1):
        return FakeSequence([None] * periods + list(self[:-periods]))

    def div(self, other):
        return FakeSequence(
            None if left is None else float(left) / float(right)
            for left, right in zip(self, other)
        )

    def cumprod(self):
        product = 1.0
        result = []
        for value in self:
            if value is None:
                result.append(None)
            else:
                product *= float(value)
                result.append(product)
        return FakeSequence(result)


class FakeFrame(object):
    def __init__(self, timestamps, data):
        self.index = FakeSequence(timestamps)
        self.columns = FakeSequence(data.keys())
        self.data = dict(data)

    def __getitem__(self, field):
        return FakeSequence(self.data[field])


class FakeContext(object):
    def __init__(self, fake_api):
        self.fake_api = fake_api

    def is_last_bar(self):
        return True

    def get_full_tick(self, instruments):
        self.fake_api.full_tick_calls.append(list(instruments))
        return {
            instrument: dict(self.fake_api.full_ticks[instrument])
            for instrument in instruments
            if instrument in self.fake_api.full_ticks
        }

    def subscribe_whole_quote(self, code_list, callback=None):
        self.fake_api.whole_quote_subscription_counter += 1
        subscription_id = self.fake_api.whole_quote_subscription_counter
        self.fake_api.whole_quote_subscriptions[subscription_id] = {
            "markets": list(code_list),
            "callback": callback,
            "thread_id": threading.current_thread().ident,
        }
        return subscription_id

    def unsubscribe_quote(self, subscription_id):
        self.fake_api.whole_quote_unsubscribe_calls.append(subscription_id)
        self.fake_api.whole_quote_subscriptions.pop(subscription_id, None)

    def get_instrument_detail(self, instrument):
        self.fake_api.instrument_detail_calls.append(instrument)
        return {
            "InstrumentName": "fake-stock",
            "FloatVolume": 800000000,
            "TotalVolume": 1000000000,
            "OpenDate": 19991110,
            "PreClose": 9.9,
            "UpStopPrice": 20.0,
            "DownStopPrice": 5.0,
            "PriceTick": 0.01,
            "InstrumentStatus": 0,
            "IsTrading": True,
        }

    def get_stock_list_in_sector(self, sector_name):
        self.fake_api.sector_calls.append(sector_name)
        return ["600000.SH", "000001.SZ", "600000.SH"]

    def get_market_data_ex(
        self,
        fields,
        instruments,
        period,
        start_time,
        end_time,
        count,
        dividend_type,
        fill_data,
        subscribe,
    ):
        self.fake_api.history_calls.append(
            {
                "fields": list(fields),
                "instruments": list(instruments),
                "period": period,
                "start_time": start_time,
                "end_time": end_time,
                "count": count,
                "dividend_type": dividend_type,
                "fill_data": fill_data,
                "subscribe": subscribe,
            }
        )
        result = {}
        base = {
            "open": [9.8, 10.0],
            "high": [10.1, 10.3],
            "low": [9.7, 9.9],
            "close": [10.0, 10.2],
            "preClose": [9.8, 9.5],
            "volume": [1000, 1200],
            "amount": [1000000.0, 1224000.0],
            "settelementPrice": [0.0, 0.0],
            "openInterest": [0, 0],
            "suspendFlag": [0, 0],
        }
        if period == "60m":
            timestamps = ["20260820103000", "20260820113000"]
        elif period == "1w":
            timestamps = ["19700101"]
            base = dict((field, [0]) for field in base)
        else:
            timestamps = ["20260820", "20260821"]
        if dividend_type in ("front", "front_ratio"):
            for field in (
                "open",
                "high",
                "low",
                "close",
                "preClose",
                "settelementPrice",
            ):
                base[field] = [float(value) * 0.5 for value in base[field]]
        elif dividend_type == "back":
            for field in (
                "open",
                "high",
                "low",
                "close",
                "preClose",
                "settelementPrice",
            ):
                base[field] = [float(value) * 2 for value in base[field]]
        elif dividend_type == "back_ratio":
            base["close"] = [20.0, 21.473684210526315]
        selected = [
            index
            for index, timestamp in enumerate(timestamps)
            if (not start_time or timestamp >= start_time)
            and (not end_time or timestamp <= end_time)
        ]
        if count > 0:
            selected = selected[-count:]
        for instrument in instruments:
            result[instrument] = FakeFrame(
                [timestamps[index] for index in selected],
                dict(
                    (
                        field,
                        [base[field][index] for index in selected],
                    )
                    for field in fields
                    if field in base
                ),
            )
        return result


class FakeQmtApi(object):
    def __init__(self):
        self.orders = []
        self.trades = []
        self.positions = [FakePosition()]
        self.position_query_error = None
        self.passorder_calls = []
        self.passorder_call_times = []
        self.cancel_calls = []
        self.cancel_callback_times = []
        self.full_tick_calls = []
        self.instrument_detail_calls = []
        self.sector_calls = []
        self.history_calls = []
        self.whole_quote_subscription_counter = 0
        self.whole_quote_subscriptions = {}
        self.whole_quote_unsubscribe_calls = []
        self.order_counter = 0
        self.raise_passorder = False
        self.emit_order_callback = True
        self.return_orders_for_reconcile = False
        self.full_ticks = {
            "600000.SH": {
                "time": 1787295570000,
                "timetag": "20260821 14:59:30",
                "lastPrice": 10.0,
                "open": 9.8,
                "high": 10.2,
                "low": 9.7,
                "lastClose": 9.9,
                "amount": 1234567.8900000001,
                "volume": 12345,
                "askPrice": [10.00, 10.01, 10.02, 10.03, 10.04],
                "askVol": [500, 300, 200, 100, 100],
                "bidPrice": [9.99, 9.98, 9.97, 9.96, 9.95],
                "bidVol": [500, 300, 200, 100, 100],
            },
            "601919.SH": {
                "time": 1787295570000,
                "timetag": "20260821 14:59:30",
                "lastPrice": 13.0,
                "open": 12.5,
                "high": 13.2,
                "low": 12.4,
                "lastClose": 9.9,
                "amount": 9876543.21,
                "volume": 54321,
                "askPrice": [13.00, 13.01, 13.02, 13.03, 13.04],
                "askVol": [1000, 800, 600, 500, 400],
                "bidPrice": [12.99, 12.98, 12.97, 12.96, 12.95],
                "bidVol": [1000, 800, 600, 500, 400],
            },
        }
        self.ipo_data = {
            "STOCK": {
                "730001.SH": {
                    "name": "fake-ipo",
                    "issuePrice": 10.5,
                    "minPurchaseNum": 100,
                    "maxPurchaseNum": 5000,
                    "purchaseDate": "20260821",
                }
            },
            "BOND": {
                "754001.SH": {
                    "name": "fake-bond",
                    "issuePrice": 100.0,
                    "minPurchaseNum": 10,
                    "maxPurchaseNum": 1000,
                    "purchaseDate": "20260821",
                }
            },
        }

    def emit_whole_quote(self, data):
        subscriptions = list(self.whole_quote_subscriptions.values())
        for subscription in subscriptions:
            filtered = {}
            markets = set(subscription["markets"])
            for instrument, tick in data.items():
                if instrument.rsplit(".", 1)[-1] in markets:
                    filtered[instrument] = dict(tick)
            if filtered and subscription["callback"] is not None:
                subscription["callback"](filtered)

    def get_trade_detail_data(self, account_id, account_type, detail_type, *args):
        if account_id != ACCOUNT_ID or account_type != "STOCK":
            return []
        if detail_type == "ACCOUNT":
            return [FakeAccount()]
        if detail_type == "POSITION":
            if self.position_query_error is not None:
                raise self.position_query_error
            return list(self.positions)
        if detail_type == "ORDER":
            return list(self.orders) if self.return_orders_for_reconcile else []
        if detail_type == "DEAL":
            if args:
                return [
                    trade
                    for trade in self.trades
                    if str(trade.m_strRemark).startswith("QA")
                ]
            return list(self.trades)
        return []

    def get_ipo_data(self, issue_type):
        return dict(self.ipo_data.get(issue_type, {}))

    def get_new_purchase_limit(self, account_id):
        if account_id != ACCOUNT_ID:
            return {}
        return {"SH": 10000, "SZ": 5000}

    def passorder(self, *args):
        self.passorder_calls.append(args)
        self.passorder_call_times.append(time.monotonic())
        if self.raise_passorder:
            raise RuntimeError("simulated uncertain passorder failure")
        self.order_counter += 1
        order_id = "QMT-ORDER-%04d" % self.order_counter
        self.orders.append(
            FakeOrder(
                args[9],
                order_id,
                args[6],
                instrument=args[3],
                side="BUY" if args[0] == 23 else "SELL",
            )
        )
        callback_order = self.orders[-1]
        if self.emit_order_callback:
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
        self.runtime = None

    def start(self):
        bridge.get_trade_detail_data = self.fake_api.get_trade_detail_data
        bridge.passorder = self.fake_api.passorder
        bridge.can_cancel_order = self.fake_api.can_cancel_order
        bridge.cancel = self.fake_api.cancel
        bridge.get_ipo_data = self.fake_api.get_ipo_data
        bridge.get_new_purchase_limit = self.fake_api.get_new_purchase_limit
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
            self.runtime = runtime
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
            self.runtime = None
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
            "version": __version__,
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
            "max_clients": 4,
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
            self.assertIs(client.connect(), client)
            health = client.health()
            self.assertNotIn("environment", health)
            account = client.get_account(ACCOUNT_ID)
            account_with_raw = client.get_account(
                ACCOUNT_ID, include_raw=True
            )
            self.assertEqual(account["count"], 1)
            account_item = account["items"][0]
            self.assertEqual(account_item["total_asset"], "20008800.125")
            self.assertEqual(account_item["available_cash"], "20000000.000")
            self.assertEqual(account_item["stock_market_value"], "8800.125")
            self.assertEqual(account_item["withdrawable_cash"], "19999000.500")
            self.assertEqual(account_item["frozen_cash"], "1000.250")
            self.assertEqual(account_item["position_profit"], "600.125")
            self.assertNotIn("raw", account_item)
            self.assertEqual(
                account_with_raw["items"][0]["raw"]["m_dBalance"],
                20008800.1250000001,
            )
            with self.assertRaises(ValidationError):
                client.get_account(ACCOUNT_ID, include_raw="yes")

            positions = client.list_positions(ACCOUNT_ID)
            positions_with_raw = client.list_positions(
                ACCOUNT_ID, include_raw=True
            )
            self.assertEqual(positions["count"], 1)
            self.assertEqual(positions["items"][0]["instrument"], "600000.SH")
            self.assertEqual(positions["items"][0]["instrument_name"], "浦发银行")
            self.assertEqual(positions["items"][0]["total_quantity"], 800)
            self.assertEqual(positions["items"][0]["available_quantity"], 800)
            self.assertEqual(positions["items"][0]["frozen_quantity"], 0)
            self.assertEqual(positions["items"][0]["cost_price"], "10.250")
            self.assertEqual(positions["items"][0]["current_price"], "11.000")
            self.assertEqual(positions["items"][0]["market_value"], "8800.125")
            self.assertEqual(positions["items"][0]["position_profit"], "600.125")
            self.assertNotIn("raw", positions["items"][0])
            self.assertEqual(
                positions_with_raw["items"][0]["raw"]["m_dLastPrice"], 11.0
            )
            self.assertEqual(
                positions_with_raw["items"][0]["raw"]["m_dMarketValue"],
                8800.125000000002,
            )
            self.assertEqual(
                positions_with_raw["items"][0]["raw"]["m_strInstrumentName"],
                "浦发银行",
            )

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

    def test_oversized_response_is_structured_and_connection_remains_usable(self):
        with QmtClient(
            config_path=self.config_path, client_id="oversized-response-e2e"
        ) as client:
            self.harness.runtime.pipe.max_message_size = 900
            client.connection.max_message_size = 900
            FakePosition.m_strLargePayload = "x" * 2000
            try:
                with self.assertRaises(RemoteError) as caught:
                    client.list_positions(ACCOUNT_ID, include_raw=True)
            finally:
                del FakePosition.m_strLargePayload

            self.assertEqual(caught.exception.code, "RESPONSE_TOO_LARGE")
            self.assertEqual(
                caught.exception.data["max_message_size"], 900
            )
            self.assertGreater(caught.exception.data["encoded_size"], 900)
            self.assertEqual(client.health()["status"], "OK")

    def test_extreme_position_value_is_normalized_without_losing_raw_value(self):
        self.fake_api.positions.append(FakeSentinelPosition())

        with QmtClient(config_path=self.config_path) as client:
            positions = client.list_positions(ACCOUNT_ID, include_raw=True)

        sentinel = next(
            item for item in positions["items"] if item["instrument"] == "131990.SZ"
        )
        self.assertIsNone(sentinel["current_price"])
        self.assertIsNone(sentinel["market_value"])
        self.assertEqual(sentinel["raw"]["m_dLastPrice"], sys.float_info.max)
        self.assertIsNone(sentinel["raw"]["m_dMarketValue"])

    def test_request_error_does_not_degrade_infrastructure_health(self):
        self.fake_api.position_query_error = RuntimeError(
            "simulated position conversion failure"
        )

        with QmtClient(config_path=self.config_path) as client:
            with self.assertRaises(RemoteError) as caught:
                client.list_positions(ACCOUNT_ID)

            self.assertEqual(caught.exception.code, "INTERNAL_ERROR")
            health = client.health()
            self.assertEqual(health["status"], "OK")
            self.assertIn("simulated position conversion failure", health["last_error"])
            self.assertEqual(health["last_request_error"], health["last_error"])
            self.assertEqual(health["health_error"], "")

            self.harness.runtime.set_error("simulated infrastructure failure")
            degraded = client.health()
            self.assertEqual(degraded["status"], "DEGRADED")
            self.assertEqual(
                degraded["health_error"], "simulated infrastructure failure"
            )
            self.assertEqual(degraded["last_error"], degraded["health_error"])
            self.assertIn(
                "simulated position conversion failure",
                degraded["last_request_error"],
            )

    def test_single_and_batch_quote_raw_collection(self):
        with QmtClient(config_path=self.config_path) as client:
            single = client.get_quote("600000.sh", timeout=5)
            single_raw = client.get_quote(
                "600000.SH", timeout=5, include_raw=True
            )
            batch = client.get_quotes(
                ["601919.SH", "600000.SH"], timeout=5, include_raw=True
            )

        self.assertEqual(single["instrument"], "600000.SH")
        self.assertEqual(single["instrument_name"], "fake-stock")
        self.assertEqual(single["last_price"], "10.000")
        self.assertEqual(single["previous_close"], "9.900")
        self.assertEqual(single["change"], "0.100")
        self.assertEqual(single["change_percent"], "1.010")
        self.assertEqual(single["turnover_amount"], "1234567.890")
        self.assertEqual(single["volume_lots"], 12345)
        self.assertEqual(single["best_ask_price"], "10.000")
        self.assertEqual(single["best_bid_price"], "9.990")
        self.assertEqual(len(single["ask_levels"]), 5)
        self.assertNotIn("raw", single)
        self.assertEqual(single_raw["raw"]["full_tick"]["lastPrice"], 10.0)
        self.assertEqual(
            single_raw["raw"]["instrument_detail"]["InstrumentName"],
            "fake-stock",
        )
        self.assertEqual(batch["count"], 2)
        self.assertEqual(batch["errors"], [])
        self.assertEqual(batch["error_count"], 0)
        self.assertEqual(
            [item["instrument"] for item in batch["items"]],
            ["601919.SH", "600000.SH"],
        )
        self.assertEqual(
            self.fake_api.full_tick_calls[-1],
            ["601919.SH", "600000.SH"],
        )
        self.assertEqual(
            self.fake_api.instrument_detail_calls[-2:],
            ["601919.SH", "600000.SH"],
        )

        self.fake_api.full_ticks["600000.SH"]["askVol"] = [0, 0, 0, 0, 0]
        with QmtClient(config_path=self.config_path) as client:
            closed_book = client.get_quote("600000.SH", timeout=5)
        self.assertEqual(closed_book["ask_levels"], [])
        self.assertIsNone(closed_book["best_ask_price"])

    def test_batch_quote_returns_missing_instrument_as_item_error(self):
        with QmtClient(config_path=self.config_path) as client:
            batch = client.get_quotes(
                ["601919.SH", "999999.SH", "600000.SH"], timeout=5
            )
            with self.assertRaises(RemoteError) as caught:
                client.get_quote("999999.SH", timeout=5)

        self.assertEqual(batch["count"], 2)
        self.assertEqual(batch["error_count"], 1)
        self.assertEqual(
            [item["instrument"] for item in batch["items"]],
            ["601919.SH", "600000.SH"],
        )
        self.assertEqual(
            batch["errors"],
            [
                {
                    "instrument": "999999.SH",
                    "code": "MARKET_DATA_UNAVAILABLE",
                    "message": "QMT did not return full tick for: 999999.SH",
                }
            ],
        )
        self.assertEqual(caught.exception.code, "MARKET_DATA_UNAVAILABLE")

    def test_batch_quote_rejects_duplicates_before_qmt_call(self):
        with QmtClient(config_path=self.config_path) as client:
            with self.assertRaises(ValidationError):
                client.get_quotes(["600000.SH", "600000.sh"], timeout=5)
            self.assertEqual(self.fake_api.full_tick_calls, [])

    def test_whole_quote_delta_uses_dedicated_pipe_and_keeps_trading_available(self):
        with QmtClient(
            config_path=self.config_path, client_id="whole-quote-delta"
        ) as client:
            subscription = client.subscribe_whole_quote(
                markets=["SH"],
                mode="DELTA",
                push_interval_ms=10,
                timeout=5,
            )
            self.assertTrue(subscription["subscribed"])
            self.assertEqual(subscription["markets"], ["SH"])
            self.assertNotEqual(
                client.connection.pipe_name,
                client.quote_connection.pipe_name,
            )

            self.fake_api.emit_whole_quote(self.fake_api.full_ticks)
            event = client.get_quote_event(timeout=2)
            self.assertEqual(event["mode"], "DELTA")
            self.assertFalse(event["is_snapshot"])
            self.assertEqual(event["count"], 2)
            self.assertEqual(
                {item["instrument"] for item in event["items"]},
                {"600000.SH", "601919.SH"},
            )
            self.assertIn("push_time", event)
            self.assertNotIn("as_of", event)
            self.assertEqual(
                set(event["items"][0]),
                {
                    "instrument",
                    "quote_time",
                    "last_price",
                    "volume_lots",
                    "turnover_amount",
                    "ask_prices",
                    "ask_volume_lots",
                    "bid_prices",
                    "bid_volume_lots",
                },
            )
            self.assertEqual(
                event["items"][0]["ask_prices"],
                ["10.000", "10.010", "10.020", "10.030", "10.040"],
            )
            self.assertEqual(
                event["items"][0]["quote_time"],
                "2026-08-21T14:59:30.000+08:00",
            )
            self.assertTrue(event["push_time"].endswith("+08:00"))
            self.assertEqual(
                event["items"][0]["ask_volume_lots"],
                [500, 300, 200, 100, 100],
            )
            self.assertNotIn("raw", event["items"][0])

            receipt = client.place_order(
                OrderRequest(
                    account_id=ACCOUNT_ID,
                    instrument="600000.SH",
                    side="BUY",
                    quantity=100,
                    price_type="LIMIT",
                    limit_price="10.25",
                    client_order_id="QUOTE-PIPE-ORDER-0001",
                ),
                wait_for="BROKER_ID",
                timeout=5,
            )
            self.assertEqual(receipt.qmt_order_id, "QMT-ORDER-0001")

            unsubscribed = client.unsubscribe_whole_quote()
            self.assertFalse(unsubscribed["subscribed"])

        deadline = time.monotonic() + 1.0
        while (
            not self.fake_api.whole_quote_unsubscribe_calls
            and time.monotonic() < deadline
        ):
            time.sleep(0.01)
        self.assertEqual(len(self.fake_api.whole_quote_unsubscribe_calls), 1)

    def test_whole_quote_snapshot_is_chunked_and_can_include_raw(self):
        self.harness.runtime.config["quote_event_max_items"] = 1
        with QmtClient(
            config_path=self.config_path, client_id="whole-quote-snapshot"
        ) as client:
            client.subscribe_whole_quote(
                markets=["SH"],
                mode="SNAPSHOT",
                push_interval_ms=10,
                include_raw=True,
                timeout=5,
            )
            self.fake_api.emit_whole_quote(self.fake_api.full_ticks)
            first = client.get_quote_event(timeout=2)
            second = client.get_quote_event(timeout=2)

            self.assertEqual(first["batch_id"], second["batch_id"])
            self.assertEqual(first["chunk_count"], 2)
            self.assertEqual(second["chunk_count"], 2)
            self.assertEqual(
                {first["chunk_index"], second["chunk_index"]}, {1, 2}
            )
            self.assertEqual(first["batch_total_count"], 2)
            self.assertIn("full_tick", first["items"][0]["raw"])
            client.unsubscribe_whole_quote()

    def test_whole_quote_stock_scope_uses_qmt_stock_sector(self):
        with QmtClient(
            config_path=self.config_path, client_id="whole-quote-stock"
        ) as client:
            with self.assertRaises(ValidationError):
                client.subscribe_whole_quote(
                    markets=["SH"], instrument_scope="FUND", timeout=5
                )
            with self.assertRaises(ValidationError):
                client.subscribe_whole_quote(
                    markets=["SH"], chunk_size=-1, timeout=5
                )
            subscription = client.subscribe_whole_quote(
                markets=["SH"],
                mode="DELTA",
                instrument_scope="STOCK",
                push_interval_ms=10,
                chunk_size=0,
                timeout=5,
            )
            self.assertEqual(subscription["instrument_scope"], "STOCK")
            self.assertEqual(subscription["chunk_size"], 0)
            self.assertEqual(self.fake_api.sector_calls, ["沪深京A股"])

            self.fake_api.emit_whole_quote(self.fake_api.full_ticks)
            event = client.get_quote_event(timeout=2)
            self.assertEqual(event["instrument_scope"], "STOCK")
            self.assertEqual(event["chunk_size"], 0)
            self.assertEqual(event["chunk_count"], 1)
            self.assertEqual(event["count"], 1)
            self.assertEqual(event["items"][0]["instrument"], "600000.SH")
            client.unsubscribe_whole_quote()

    def test_whole_quote_multiple_clients_receive_only_selected_markets(self):
        sz_tick = dict(self.fake_api.full_ticks["600000.SH"])
        with QmtClient(
            config_path=self.config_path, client_id="whole-quote-sh"
        ) as sh_client, QmtClient(
            config_path=self.config_path, client_id="whole-quote-sz"
        ) as sz_client:
            sh_client.subscribe_whole_quote(
                markets=["SH"], push_interval_ms=10, timeout=5
            )
            sz_client.subscribe_whole_quote(
                markets=["SZ"], push_interval_ms=10, timeout=5
            )
            active = list(self.fake_api.whole_quote_subscriptions.values())
            self.assertEqual(len(active), 1)
            self.assertEqual(set(active[0]["markets"]), {"SH", "SZ"})

            self.fake_api.emit_whole_quote(
                {
                    "600000.SH": self.fake_api.full_ticks["600000.SH"],
                    "000001.SZ": sz_tick,
                }
            )
            sh_event = sh_client.get_quote_event(timeout=2)
            sz_event = sz_client.get_quote_event(timeout=2)
            self.assertEqual(
                [item["instrument"] for item in sh_event["items"]],
                ["600000.SH"],
            )
            self.assertEqual(
                [item["instrument"] for item in sz_event["items"]],
                ["000001.SZ"],
            )

    def test_sector_instrument_details_and_daily_history(self):
        with QmtClient(config_path=self.config_path) as client:
            sector = client.list_sector_instruments("沪深A股", timeout=5)
            details = client.get_instrument_details(
                ["600000.SH", "000001.SZ"], timeout=5, include_raw=True
            )
            history = client.get_daily_history(
                ["600000.SH", "000001.SZ"],
                start_time="20260820",
                end_time="20260821",
                fill_data=False,
                subscribe=False,
                timeout=5,
            )

        self.assertEqual(sector["items"], ["600000.SH", "000001.SZ"])
        self.assertEqual(self.fake_api.sector_calls, ["沪深A股"])
        self.assertEqual(details["count"], 2)
        self.assertEqual(details["items"][0]["instrument_name"], "fake-stock")
        self.assertEqual(details["items"][0]["float_shares"], 800000000)
        self.assertEqual(details["items"][0]["total_shares"], 1000000000)
        self.assertEqual(details["items"][0]["listing_date"], "1999-11-10")
        self.assertEqual(details["items"][0]["raw"]["OpenDate"], 19991110)
        self.assertEqual(history["period"], "1d")
        self.assertEqual(history["row_count"], 4)
        self.assertEqual(
            history["items"][0]["timestamps"], ["2026-08-20", "2026-08-21"]
        )
        self.assertEqual(
            history["items"][0]["data"]["close_price"], ["10.000", "10.200"]
        )
        self.assertEqual(
            history["items"][0]["data"]["adjustment_factor"],
            ["2", "2.105263"],
        )
        self.assertEqual(
            set(history["items"][0]["data"]),
            {
                "adjustment_factor",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume_lots",
                "turnover_amount",
                "settlement_price",
                "open_interest",
                "previous_close",
                "suspension_status",
            },
        )
        self.assertNotIn("raw", history["items"][0])
        self.assertEqual(
            self.fake_api.history_calls[0]["fields"],
            [
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "settelementPrice",
                "openInterest",
                "preClose",
                "suspendFlag",
            ],
        )
        self.assertEqual(self.fake_api.history_calls[0]["dividend_type"], "none")
        self.assertEqual(self.fake_api.history_calls[0]["subscribe"], False)
        self.assertEqual(self.fake_api.history_calls[1]["fields"], ["close"])
        self.assertEqual(
            self.fake_api.history_calls[1]["dividend_type"], "back_ratio"
        )
        self.assertEqual(self.fake_api.history_calls[1]["subscribe"], False)

    def test_daily_history_validates_dates_before_qmt_call(self):
        with QmtClient(config_path=self.config_path) as client:
            with self.assertRaises(ValidationError):
                client.get_daily_history(
                    ["600000.SH"],
                    start_time="2026-08-20",
                    end_time="20260821",
                )
        self.assertEqual(self.fake_api.history_calls, [])

    def test_daily_history_can_include_unmodified_qmt_columns(self):
        with QmtClient(config_path=self.config_path) as client:
            history = client.get_daily_history(
                ["600000.SH"], timeout=5, include_raw=True
            )

        self.assertEqual(
            history["items"][0]["data"]["adjustment_factor"],
            ["2", "2.105263"],
        )
        self.assertEqual(
            history["items"][0]["raw"]["data"]["preClose"], [9.8, 9.5]
        )
        self.assertEqual(
            history["items"][0]["raw"]["timestamps"],
            ["20260820", "20260821"],
        )

    def test_daily_history_factor_does_not_reset_at_query_start(self):
        with QmtClient(config_path=self.config_path) as client:
            history = client.get_daily_history(
                ["600000.SH"],
                start_time="20260821",
                end_time="20260821",
                timeout=5,
            )

        self.assertEqual(history["items"][0]["timestamps"], ["2026-08-21"])
        self.assertEqual(
            history["items"][0]["data"]["adjustment_factor"],
            ["2.105263"],
        )

    def test_daily_history_supports_adjusted_prices_with_stable_factor(self):
        with QmtClient(config_path=self.config_path) as client:
            history = client.get_daily_history(
                ["600000.SH"],
                adjustment="front_ratio",
                timeout=5,
                include_raw=True,
            )

        self.assertEqual(history["period"], "1d")
        self.assertEqual(history["adjustment"], "front_ratio")
        self.assertEqual(
            history["items"][0]["data"]["close_price"],
            ["5.000", "5.100"],
        )
        self.assertEqual(
            history["items"][0]["data"]["adjustment_factor"],
            ["2", "2.105263"],
        )
        self.assertEqual(
            history["items"][0]["raw"]["data"]["close"],
            [5.0, 5.1],
        )
        self.assertEqual(
            [call["dividend_type"] for call in self.fake_api.history_calls],
            ["front_ratio", "none", "back_ratio"],
        )

    def test_daily_history_rejects_unknown_adjustment_before_qmt_call(self):
        with QmtClient(config_path=self.config_path) as client:
            with self.assertRaises(ValidationError):
                client.get_daily_history(
                    ["600000.SH"], adjustment="forward_adjusted"
                )
        self.assertEqual(self.fake_api.history_calls, [])

    def test_bar_history_normalizes_period_and_intraday_timestamps(self):
        with QmtClient(config_path=self.config_path) as client:
            history = client.get_bar_history(
                ["932000.SH"],
                period="1h",
                start_time="20260820100000",
                end_time="20260820120000",
                timeout=5,
                include_raw=True,
            )

        self.assertEqual(history["period"], "60m")
        self.assertEqual(history["row_count"], 2)
        self.assertEqual(
            history["items"][0]["timestamps"],
            ["2026-08-20T10:30:00", "2026-08-20T11:30:00"],
        )
        self.assertNotIn("adjustment_factor", history["items"][0]["data"])
        self.assertEqual(
            history["items"][0]["raw"]["timestamps"],
            ["20260820103000", "20260820113000"],
        )
        self.assertEqual(
            [call["period"] for call in self.fake_api.history_calls],
            ["60m"],
        )
        self.assertEqual(self.fake_api.history_calls[0]["dividend_type"], "none")

    def test_bar_history_validates_period_and_time_before_qmt_call(self):
        with QmtClient(config_path=self.config_path) as client:
            with self.assertRaises(ValidationError):
                client.get_bar_history(["600000.SH"], period="7m")
            with self.assertRaises(ValidationError):
                client.get_bar_history(
                    ["600000.SH"],
                    period="60m",
                    start_time="20260230093000",
                )
        self.assertEqual(self.fake_api.history_calls, [])

    def test_bar_history_filters_qmt_empty_weekly_sentinel(self):
        with QmtClient(config_path=self.config_path) as client:
            history = client.get_bar_history(
                ["932000.SH"], period="1w", timeout=5
            )

        self.assertEqual(history["period"], "1w")
        self.assertEqual(history["adjustment"], "none")
        self.assertEqual(history["row_count"], 0)
        self.assertEqual(history["items"][0]["timestamps"], [])
        self.assertTrue(
            all(
                values == []
                for values in history["items"][0]["data"].values()
            )
        )

    def test_get_order_standard_fields_and_optional_raw(self):
        order = OrderRequest(
            account_id=ACCOUNT_ID,
            instrument="600000.SH",
            side="BUY",
            quantity=100,
            price_type="LIMIT",
            limit_price="10.25",
            client_order_id="CLIENT-ORDER-FIELDS-0001",
        )
        with QmtClient(config_path=self.config_path) as client:
            client.place_order(order, wait_for="BROKER_ID", timeout=5)
            callback_order = self.fake_api.orders[0]
            callback_order.m_nOrderStatus = 57
            callback_order.m_nVolumeTotal = 0
            callback_order.m_strCancelInfo = "5007|委托不合法"
            bridge.order_callback(None, callback_order)

            current = client.get_order(order.client_order_id, timeout=5)
            debug = client.get_order(
                order.client_order_id, timeout=5, include_raw=True
            )
            listed = client.list_orders(
                account_id=ACCOUNT_ID, timeout=5, include_raw=False
            )

        self.assertEqual(current["order_status"], "REJECTED")
        self.assertEqual(current["filled_quantity"], 0)
        self.assertEqual(current["remaining_quantity"], 0)
        self.assertIsNone(current["average_filled_price"])
        self.assertIsNone(current["filled_amount"])
        self.assertEqual(current["reject_reason"], "5007|委托不合法")
        self.assertNotIn("raw", current)
        self.assertEqual(debug["raw"]["m_nOrderStatus"], 57)
        self.assertNotIn("raw", listed["items"][0])

    def test_list_trades_supports_adapter_and_account_scope(self):
        order = OrderRequest(
            account_id=ACCOUNT_ID,
            instrument="600000.SH",
            side="BUY",
            quantity=100,
            price_type="LIMIT",
            limit_price="10.25",
            client_order_id="CLIENT-TRADE-LIST-0001",
        )
        with QmtClient(config_path=self.config_path) as client:
            client.place_order(order, wait_for="BROKER_ID", timeout=5)
            adapter_trade = FakeTrade(
                self.fake_api.orders[0], "TRADE-ADAPTER-1", 10.2, 100
            )
            manual_order = FakeOrder(
                "MANUAL", "QMT-MANUAL-1", 100, instrument="000001.SZ"
            )
            manual_trade = FakeTrade(manual_order, "TRADE-MANUAL-1", 12.5, 100)
            self.fake_api.trades.extend([adapter_trade, manual_trade])

            adapter = client.list_trades(ACCOUNT_ID, scope="ADAPTER", timeout=5)
            account = client.list_trades(ACCOUNT_ID, scope="ACCOUNT", timeout=5)
            filtered = client.list_trades(
                ACCOUNT_ID,
                scope="ACCOUNT",
                client_order_id=order.client_order_id,
                include_raw=True,
                timeout=5,
            )
            current = client.get_order(order.client_order_id, timeout=5)

        self.assertEqual(adapter["count"], 1)
        self.assertEqual(adapter["items"][0]["client_order_id"], order.client_order_id)
        self.assertNotIn("raw", adapter["items"][0])
        self.assertEqual(account["count"], 2)
        self.assertIsNone(
            next(
                item
                for item in account["items"]
                if item["trade_id"] == "TRADE-MANUAL-1"
            )["client_order_id"]
        )
        self.assertEqual(filtered["count"], 1)
        self.assertEqual(filtered["items"][0]["raw"]["m_strTradeID"], "TRADE-ADAPTER-1")
        self.assertEqual(current["order_status"], "FILLED")
        self.assertEqual(current["filled_quantity"], 100)
        self.assertEqual(current["remaining_quantity"], 0)
        self.assertEqual(current["average_filled_price"], "10.2")
        self.assertEqual(current["filled_amount"], "1020")
        self.assertEqual(current["trade_count"], 1)

    def test_wait_orders_are_woken_by_deal_callbacks(self):
        orders = [
            OrderRequest(
                account_id=ACCOUNT_ID,
                instrument="600000.SH",
                side="BUY",
                quantity=100,
                price_type="LIMIT",
                limit_price="10.25",
                client_order_id="CLIENT-WAIT-%02d" % index,
            )
            for index in range(2)
        ]
        with QmtClient(config_path=self.config_path) as client:
            client.place_orders(
                orders, interval_ms=0, wait_for="BROKER_ID", timeout=5
            )

            def emit_fill(index):
                trade = FakeTrade(
                    self.fake_api.orders[index],
                    "TRADE-WAIT-%02d" % index,
                    10.2 + index * 0.01,
                    100,
                )
                self.fake_api.trades.append(trade)
                bridge.deal_callback(None, trade)

            timers = [
                threading.Timer(0.05 + index * 0.03, emit_fill, args=(index,))
                for index in range(2)
            ]
            for timer in timers:
                timer.daemon = True
                timer.start()
            started = time.monotonic()
            result = client.wait_orders(
                [item.client_order_id for item in orders], timeout=2
            )
            elapsed = time.monotonic() - started

        self.assertTrue(result["completed"])
        self.assertEqual(result["pending_client_order_ids"], [])
        self.assertEqual(
            [item["order_status"] for item in result["items"]],
            ["FILLED", "FILLED"],
        )
        self.assertLess(elapsed, 0.5)

    def test_wait_order_timeout_returns_last_state_and_keeps_connection(self):
        order = OrderRequest(
            account_id=ACCOUNT_ID,
            instrument="600000.SH",
            side="BUY",
            quantity=100,
            price_type="LIMIT",
            limit_price="10.25",
            client_order_id="CLIENT-WAIT-TIMEOUT",
        )
        with QmtClient(config_path=self.config_path) as client:
            client.place_order(order, wait_for="BROKER_ID", timeout=5)
            with self.assertRaises(RequestTimeout) as caught:
                client.wait_order(order.client_order_id, timeout=0.05)
            health = client.health(timeout=5)

        self.assertEqual(caught.exception.code, "WAIT_TIMEOUT")
        self.assertEqual(
            caught.exception.data["pending_client_order_ids"],
            [order.client_order_id],
        )
        self.assertEqual(caught.exception.data["items"][0]["order_status"], "SUBMITTED")
        self.assertEqual(health["status"], "OK")

    def test_new_issue_list_and_account_quota_queries(self):
        with QmtClient(config_path=self.config_path) as client:
            issues = client.list_new_issues("ALL", timeout=5)
            quota = client.get_new_issue_quota(ACCOUNT_ID, timeout=5)

        self.assertEqual(issues["count"], 2)
        by_instrument = {item["instrument"]: item for item in issues["items"]}
        self.assertEqual(by_instrument["730001.SH"]["issue_type"], "STOCK")
        self.assertEqual(by_instrument["730001.SH"]["issue_price"], 10.5)
        self.assertEqual(by_instrument["754001.SH"]["issue_type"], "BOND")
        self.assertEqual(quota["account_id"], ACCOUNT_ID)
        self.assertEqual(quota["limits"], {"SH": 10000, "SZ": 5000})

    def test_reverse_repo_reuses_generic_order_lifecycle(self):
        order = ReverseRepoRequest(
            account_id=ACCOUNT_ID,
            instrument="204001.SH",
            amount=1000,
            annual_rate="1.825",
            remark="repo-e2e",
            client_order_id="CLIENT-REPO-0001",
        )
        with QmtClient(config_path=self.config_path) as client:
            receipt = client.place_reverse_repo(
                order, wait_for="BROKER_ID", timeout=5
            )
            current = client.get_order(order.client_order_id, timeout=5)

        call = self.fake_api.passorder_calls[0]
        self.assertEqual(receipt.qmt_order_id, "QMT-ORDER-0001")
        self.assertEqual(call[0], 24)
        self.assertEqual(call[4], 11)
        self.assertEqual(call[5], 1.825)
        self.assertEqual(call[6], 10)
        self.assertEqual(current["order_kind"], "REVERSE_REPO")
        self.assertEqual(current["quantity_type"], "REPO_UNITS")
        self.assertEqual(current["metadata"]["amount"], 1000)

    def test_new_issue_subscription_resolves_price_and_is_idempotent(self):
        order = NewIssueSubscriptionRequest(
            account_id=ACCOUNT_ID,
            instrument="730001.SH",
            issue_type="STOCK",
            quantity=1000,
            remark="ipo-e2e",
            client_order_id="CLIENT-IPO-0001",
        )
        with QmtClient(config_path=self.config_path) as client:
            first = client.subscribe_new_issue(
                order, wait_for="BROKER_ID", timeout=5
            )
            replay = client.subscribe_new_issue(
                order, wait_for="BROKER_ID", timeout=5
            )
            current = client.get_order(order.client_order_id, timeout=5)

        call = self.fake_api.passorder_calls[0]
        self.assertEqual(first.qmt_order_id, "QMT-ORDER-0001")
        self.assertTrue(replay.idempotent_replay)
        self.assertEqual(len(self.fake_api.passorder_calls), 1)
        self.assertEqual(call[0], 23)
        self.assertEqual(call[4], 11)
        self.assertEqual(call[5], 10.5)
        self.assertEqual(call[6], 1000)
        self.assertEqual(current["order_kind"], "NEW_ISSUE_SUBSCRIPTION")
        self.assertEqual(current["metadata"]["issue_type"], "STOCK")

    def test_beijing_regular_buy_accepts_one_share_increment(self):
        order = OrderRequest(
            account_id=ACCOUNT_ID,
            instrument="920047.BJ",
            side="BUY",
            quantity=101,
            price_type="LIMIT",
            limit_price="10.25",
            client_order_id="CLIENT-BJ-0001",
        )
        with QmtClient(config_path=self.config_path) as client:
            receipt = client.place_order(order, wait_for="BROKER_ID", timeout=5)

        self.assertEqual(receipt.qmt_order_id, "QMT-ORDER-0001")
        self.assertEqual(self.fake_api.passorder_calls[0][3], "920047.BJ")
        self.assertEqual(self.fake_api.passorder_calls[0][6], 101)

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

    def test_broker_id_waiter_is_completed_by_reconciliation(self):
        self.fake_api.emit_order_callback = False
        self.fake_api.return_orders_for_reconcile = True
        bridge._RUNTIME.config["reconcile_interval_seconds"] = 0.01
        bridge._RUNTIME.last_reconcile = 0.0
        order = OrderRequest(
            account_id=ACCOUNT_ID,
            instrument="600000.SH",
            side="BUY",
            quantity=100,
            price_type="LIMIT",
            limit_price="10.25",
            client_order_id="CLIENT-RECONCILE-BROKER-ID",
        )

        with QmtClient(config_path=self.config_path) as client:
            receipt = client.place_order(
                order,
                wait_for="BROKER_ID",
                timeout=2,
            )

        self.assertEqual(receipt.qmt_order_id, "QMT-ORDER-0001")
        self.assertEqual(len(self.fake_api.passorder_calls), 1)
        self.assertNotIn(
            order.client_order_id,
            bridge._RUNTIME.pending_broker_responses,
        )

    def test_broker_id_timeout_closes_client_and_releases_pipe(self):
        self.fake_api.emit_order_callback = False
        self.fake_api.return_orders_for_reconcile = False
        order = OrderRequest(
            account_id=ACCOUNT_ID,
            instrument="600000.SH",
            side="BUY",
            quantity=100,
            price_type="LIMIT",
            limit_price="10.25",
            client_order_id="CLIENT-BROKER-ID-TIMEOUT",
        )

        first = QmtClient(config_path=self.config_path).connect()
        try:
            with self.assertRaises(RequestTimeout):
                first.place_order(order, wait_for="BROKER_ID", timeout=0.15)
            self.assertFalse(first.connection.is_connected)
        finally:
            first.close()

        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if order.client_order_id not in bridge._RUNTIME.pending_broker_responses:
                break
            time.sleep(0.01)
        self.assertNotIn(
            order.client_order_id,
            bridge._RUNTIME.pending_broker_responses,
        )

        with QmtClient(config_path=self.config_path) as second:
            self.assertEqual(second.health()["status"], "OK")

    def test_sync_batch_orders_are_serialized_at_requested_interval(self):
        orders = [
            OrderRequest(
                account_id=ACCOUNT_ID,
                instrument="600000.SH",
                side="BUY",
                quantity=100,
                price_type="LIMIT",
                limit_price="10.25",
                remark="sync-batch-%02d" % index,
                client_order_id="CLIENT-SYNC-BATCH-%02d" % index,
            )
            for index in range(3)
        ]

        client_call_times = []
        with QmtClient(config_path=self.config_path) as client:
            original_place_order = client.place_order

            def timed_place_order(*args, **kwargs):
                client_call_times.append(time.monotonic())
                return original_place_order(*args, **kwargs)

            client.place_order = timed_place_order
            receipts = client.place_orders(
                orders,
                interval_ms=50,
                wait_for="LOCAL_ACK",
                timeout=5,
            )

        self.assertEqual(
            [item.client_order_id for item in receipts],
            [item.client_order_id for item in orders],
        )
        self.assertEqual(len(self.fake_api.passorder_calls), 3)
        gaps = [
            later - earlier
            for earlier, later in zip(
                client_call_times,
                client_call_times[1:],
            )
        ]
        self.assertTrue(all(gap >= 0.04 for gap in gaps), gaps)

    def test_batch_prevalidation_prevents_partial_submission(self):
        valid_order = OrderRequest(
            account_id=ACCOUNT_ID,
            instrument="600000.SH",
            side="BUY",
            quantity=100,
            price_type="LIMIT",
            limit_price="10.25",
        )

        with QmtClient(config_path=self.config_path) as client:
            with self.assertRaises(ValidationError):
                client.place_orders(
                    [valid_order, object()],
                    interval_ms=50,
                    timeout=5,
                )

        self.assertEqual(self.fake_api.passorder_calls, [])

    def test_batch_rejects_duplicate_client_order_ids_before_submission(self):
        same_payload = [
            OrderRequest(
                account_id=ACCOUNT_ID,
                instrument="600000.SH",
                side="BUY",
                quantity=100,
                price_type="LIMIT",
                limit_price="10.25",
                client_order_id="CLIENT-BATCH-DUPLICATE-SAME",
            )
            for unused in range(2)
        ]
        different_payload = [
            OrderRequest(
                account_id=ACCOUNT_ID,
                instrument="600000.SH",
                side="BUY",
                quantity=quantity,
                price_type="LIMIT",
                limit_price="10.25",
                client_order_id="CLIENT-BATCH-DUPLICATE-DIFFERENT",
            )
            for quantity in (100, 200)
        ]

        with QmtClient(config_path=self.config_path) as client:
            for orders in (same_payload, different_payload):
                with self.subTest(orders=orders):
                    with self.assertRaises(ValidationError) as caught:
                        client.place_orders(orders, interval_ms=0, timeout=5)
                    self.assertIn("duplicate client_order_id", str(caught.exception))

        self.assertEqual(self.fake_api.passorder_calls, [])

    def test_terminal_order_ignores_stale_nonterminal_callback(self):
        order = OrderRequest(
            account_id=ACCOUNT_ID,
            instrument="600000.SH",
            side="BUY",
            quantity=100,
            price_type="LIMIT",
            limit_price="10.25",
            client_order_id="CLIENT-TERMINAL-STALE-CALLBACK",
        )
        with QmtClient(config_path=self.config_path) as client:
            client.place_order(order, wait_for="BROKER_ID", timeout=5)
            callback_order = self.fake_api.orders[0]
            callback_order.m_nOrderStatus = 56
            callback_order.m_nVolumeTraded = 100
            callback_order.m_nVolumeTotal = 0
            bridge.order_callback(None, callback_order)
            filled = client.get_order(order.client_order_id, include_raw=True)

            stale = FakeOrder(
                callback_order.m_strRemark,
                callback_order.m_strOrderSysID,
                100,
            )
            stale.m_nOrderStatus = 50
            bridge.order_callback(None, stale)
            after_stale = client.get_order(order.client_order_id, include_raw=True)

        self.assertEqual(filled["order_status"], "FILLED")
        self.assertEqual(after_stale["order_status"], "FILLED")
        self.assertEqual(after_stale["raw"]["m_nOrderStatus"], 56)
        self.assertEqual(after_stale["raw"]["m_nVolumeTraded"], 100)

    def test_multiple_clients_connect_and_submit_qmt_requests(self):
        first = QmtClient(
            config_path=self.config_path, client_id="multi-client-a"
        ).connect()
        second = QmtClient(
            config_path=self.config_path, client_id="multi-client-b"
        ).connect()
        barrier = threading.Barrier(3)
        results = {}
        errors = []

        def query_account():
            try:
                barrier.wait()
                results["account"] = first.get_account(ACCOUNT_ID, timeout=5)
            except Exception as exc:
                errors.append(exc)

        def query_positions():
            try:
                barrier.wait()
                results["positions"] = second.list_positions(
                    ACCOUNT_ID, timeout=5
                )
            except Exception as exc:
                errors.append(exc)

        first_thread = threading.Thread(target=query_account)
        second_thread = threading.Thread(target=query_positions)
        started = time.monotonic()
        try:
            self.assertNotEqual(
                first.hello["connection_id"], second.hello["connection_id"]
            )
            health = first.health()
            self.assertEqual(health["active_client_count"], 2)
            self.assertEqual(health["max_clients"], 4)
            first_thread.start()
            second_thread.start()
            barrier.wait()
            first_thread.join(5)
            second_thread.join(5)
            self.assertFalse(first_thread.is_alive())
            self.assertFalse(second_thread.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(results["account"]["count"], 1)
            self.assertEqual(results["positions"]["count"], 1)
            second.close()
            deadline = time.monotonic() + 1.0
            while time.monotonic() < deadline:
                health = first.health()
                if health["active_client_count"] == 1:
                    break
                time.sleep(0.01)
            self.assertEqual(health["active_client_count"], 1)
            self.assertEqual(first.health()["status"], "OK")
        finally:
            second.close()
            first.close()
        self.assertLess(time.monotonic() - started, 5.0)

    def test_client_limit_honors_connection_timeout(self):
        clients = [
            QmtClient(
                config_path=self.config_path,
                client_id="limit-client-%s" % index,
            ).connect()
            for index in range(self.config["max_clients"])
        ]
        extra = QmtClient(config_path=self.config_path, client_id="limit-extra")
        replacement = None
        started = time.monotonic()
        try:
            with self.assertRaises(RequestTimeout):
                extra.connect(timeout=0.15)
            clients.pop().close()
            replacement = QmtClient(
                config_path=self.config_path,
                client_id="limit-replacement",
            ).connect(timeout=1.0)
            self.assertTrue(replacement.connection.is_connected)
        finally:
            extra.close()
            if replacement is not None:
                replacement.close()
            for client in clients:
                client.close()
        self.assertGreaterEqual(time.monotonic() - started, 0.1)

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

        pipe_thread = self.harness.runtime.pipe.server_thread
        self.harness.stop()
        self.assertFalse(pipe_thread.is_alive())
        self.harness = BridgeHarness(self.config, self.fake_api)
        self.harness.start()

        with QmtClient(config_path=self.config_path) as client:
            replay = client.place_order(order, wait_for="BROKER_ID", timeout=5)

        self.assertEqual(first.qmt_order_id, replay.qmt_order_id)
        self.assertTrue(replay.idempotent_replay)
        self.assertEqual(len(self.fake_api.passorder_calls), 1)

    def test_duplicate_bridge_start_reports_pipe_binding_error(self):
        duplicate = BridgeHarness(self.config, self.fake_api)

        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(bridge.BridgeError) as caught:
                duplicate.start()

        duplicate.thread.join(5)
        self.assertFalse(duplicate.thread.is_alive())
        self.assertEqual(caught.exception.code, "PIPE_START_FAILED")
        self.assertIn("CreateNamedPipeW failed", str(caught.exception))

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
            call_count_at_receipt = len(self.fake_api.passorder_calls)
            replay = client.place_algo_order(order, timeout=5)
            deadline = time.monotonic() + 2
            while (
                len(self.fake_api.passorder_calls) < receipt.child_count
                and time.monotonic() < deadline
            ):
                time.sleep(0.01)
            current = client.get_algo_order(order.algo_order_id, timeout=5)

        self.assertGreater(receipt.child_count, 1)
        self.assertEqual(call_count_at_receipt, 1)
        self.assertEqual(receipt.algo_status, "PLACING")
        self.assertEqual(len(self.fake_api.passorder_calls), receipt.child_count)
        intervals = [
            later - earlier
            for earlier, later in zip(
                self.fake_api.passorder_call_times,
                self.fake_api.passorder_call_times[1:],
            )
        ]
        self.assertTrue(intervals)
        self.assertTrue(all(interval >= 0.045 for interval in intervals))
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
                if (
                    current["current_attempt"] >= 1
                    and len(self.fake_api.passorder_calls) >= 2
                ):
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

    def test_algo_cancel_stops_children_that_have_not_been_submitted(self):
        order = AlgoOrderRequest(
            account_id=ACCOUNT_ID,
            instrument="601919.SH",
            side="BUY",
            target_amount="1200000",
            algo_order_id="ALGO-CANCEL-SCHEDULE-0001",
        )
        with QmtClient(config_path=self.config_path) as client:
            receipt = client.place_algo_order(order, timeout=5)
            self.assertGreater(receipt.child_count, 1)
            self.assertEqual(len(self.fake_api.passorder_calls), 1)
            client.cancel_algo_order(order.algo_order_id, timeout=5)
            time.sleep(0.2)
            current = client.get_algo_order(order.algo_order_id, timeout=5)

        self.assertEqual(len(self.fake_api.passorder_calls), 1)
        self.assertIn(current["algo_status"], ("CANCELING", "CANCELED"))

    def test_planned_children_resume_after_bridge_restart_with_interval(self):
        order = AlgoOrderRequest(
            account_id=ACCOUNT_ID,
            instrument="601919.SH",
            side="BUY",
            target_amount="1200000",
            algo_order_id="ALGO-RESTART-SCHEDULE-0001",
        )
        with QmtClient(config_path=self.config_path) as client:
            receipt = client.place_algo_order(order, timeout=5)
        self.assertGreater(receipt.child_count, 1)
        self.assertEqual(len(self.fake_api.passorder_calls), 1)

        self.harness.stop()
        self.harness = BridgeHarness(self.config, self.fake_api)
        self.harness.start()
        deadline = time.monotonic() + 2
        while (
            len(self.fake_api.passorder_calls) < receipt.child_count
            and time.monotonic() < deadline
        ):
            time.sleep(0.01)

        self.assertEqual(len(self.fake_api.passorder_calls), receipt.child_count)
        intervals = [
            later - earlier
            for earlier, later in zip(
                self.fake_api.passorder_call_times[1:],
                self.fake_api.passorder_call_times[2:],
            )
        ]
        self.assertTrue(all(interval >= 0.045 for interval in intervals))


if __name__ == "__main__":
    unittest.main()
