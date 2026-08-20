# -*- coding: utf-8 -*-
"""QMT-side bridge for stock account, position, order and cancel operations.

This file intentionally uses Python 3.6 compatible syntax and UTF-8 source text.
Only this module calls QMT-provided functions.
"""

from __future__ import print_function

import builtins
from collections import deque
import ctypes
from ctypes import wintypes
import datetime
import decimal
import hashlib
import json
import math
import os
import queue
import re
import sqlite3
import struct
import threading
import time
import traceback


CONFIG_PATH = os.environ.get(
    "QMT_ADAPTER_CONFIG",
    r"C:\pazq_qmt_simulate\userdata\qmt_adapter\bridge_config.json",
)

PROTOCOL_VERSION = 2
MAX_MESSAGE_SIZE = 1024 * 1024
STRATEGY_NAME = "QMT_ADAPTER_V1"
_RUNTIME = None

STOCK_NATIVE_MARKET_PRICE_TYPES = {
    "MARKET_SH_CONVERT_5_CANCEL": {"SH", "BJ"},
    "MARKET_SH_CONVERT_5_LIMIT": {"SH", "BJ"},
    "MARKET_PEER_PRICE_FIRST": {"SH", "SZ", "BJ"},
    "MARKET_MINE_PRICE_FIRST": {"SH", "SZ", "BJ"},
    "MARKET_SZ_INSTBUSI_RESTCANCEL": {"SZ"},
    "MARKET_SZ_CONVERT_5_CANCEL": {"SZ"},
    "MARKET_SZ_FULL_OR_CANCEL": {"SZ"},
}
_RUNTIME_SLOT = "_qmt_adapter_bridge_runtime_v1"


class BridgeError(Exception):
    def __init__(self, code, message, data=None):
        Exception.__init__(self, message)
        self.code = code
        self.message = message
        self.data = data or {}


class UncertainError(BridgeError):
    pass


def _utc_now_text():
    return datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )


def _canonical_json(value):
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _canonical_price(value):
    if value is None or value == "":
        return None
    number = decimal.Decimal(str(value))
    text = format(number.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _order_payload_hash(
    account_id, instrument, side, quantity, price_type, limit_price, remark
):
    price_type = str(price_type).upper()
    if price_type == "LIMIT":
        identity_price = _canonical_price(limit_price)
    elif price_type in STOCK_NATIVE_MARKET_PRICE_TYPES:
        identity_price = _canonical_price(0 if limit_price is None else limit_price)
    else:
        identity_price = _canonical_price(limit_price)
    identity = {
        "account_id": str(account_id),
        "account_type": "STOCK",
        "business_type": "CASH",
        "instrument": str(instrument).upper(),
        "limit_price": identity_price,
        "price_type": price_type,
        "quantity": int(quantity),
        "quantity_type": "SHARES",
        "remark": str(remark or ""),
        "side": str(side).upper(),
    }
    return hashlib.sha256(_canonical_json(identity).encode("ascii")).hexdigest()


def _safe_value(value, depth=0):
    if depth > 4:
        return repr(value)
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        for encoding in ("utf-8", "gbk"):
            try:
                return value.decode(encoding)
            except Exception:
                pass
        return value.hex()
    if isinstance(value, (list, tuple)):
        return [_safe_value(item, depth + 1) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _safe_value(item, depth + 1)
            for key, item in value.items()
        }
    return repr(value)


def _serialize_qmt_object(obj):
    result = {}
    for name in dir(obj):
        if name.startswith("_"):
            continue
        try:
            value = getattr(obj, name)
        except Exception:
            continue
        if callable(value):
            continue
        result[name] = _safe_value(value)
    return result


def _normalize_account(raw, configured_account_id):
    return {
        "account_id": raw.get("m_strAccountID", configured_account_id),
        "account_type": "STOCK",
        "available_cash": raw.get("m_dAvailable"),
        "raw": raw,
    }


def _normalize_position(raw, configured_account_id):
    return {
        "account_id": configured_account_id,
        "account_type": "STOCK",
        "instrument": raw.get("m_strInstrumentID"),
        "total_quantity": raw.get("m_nVolume"),
        "raw": raw,
    }


class PipeServer(object):
    PIPE_ACCESS_DUPLEX = 0x00000003
    PIPE_TYPE_BYTE = 0x00000000
    PIPE_READMODE_BYTE = 0x00000000
    PIPE_WAIT = 0x00000000
    PIPE_REJECT_REMOTE_CLIENTS = 0x00000008
    ERROR_BROKEN_PIPE = 109
    ERROR_NO_DATA = 232
    ERROR_PIPE_CONNECTED = 535
    INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value

    def __init__(self, runtime, pipe_name, auth_token, max_message_size):
        self.runtime = runtime
        self.pipe_name = pipe_name
        self.auth_token = auth_token
        self.max_message_size = int(max_message_size)
        self.stop_event = threading.Event()
        self.handle_lock = threading.Lock()
        self.write_lock = threading.Lock()
        self.current_handle = None
        self.current_connection_id = 0
        self.authenticated_connection_id = None
        self.server_thread = None
        self.kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        self._declare_api()

    def _declare_api(self):
        k32 = self.kernel32
        k32.CreateNamedPipeW.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.LPVOID,
        ]
        k32.CreateNamedPipeW.restype = wintypes.HANDLE
        k32.ConnectNamedPipe.argtypes = [wintypes.HANDLE, wintypes.LPVOID]
        k32.ConnectNamedPipe.restype = wintypes.BOOL
        k32.DisconnectNamedPipe.argtypes = [wintypes.HANDLE]
        k32.DisconnectNamedPipe.restype = wintypes.BOOL
        k32.ReadFile.argtypes = [
            wintypes.HANDLE,
            wintypes.LPVOID,
            wintypes.DWORD,
            ctypes.POINTER(wintypes.DWORD),
            wintypes.LPVOID,
        ]
        k32.ReadFile.restype = wintypes.BOOL
        k32.WriteFile.argtypes = k32.ReadFile.argtypes
        k32.WriteFile.restype = wintypes.BOOL
        k32.CloseHandle.argtypes = [wintypes.HANDLE]
        k32.CloseHandle.restype = wintypes.BOOL
        if hasattr(k32, "CancelIoEx"):
            k32.CancelIoEx.argtypes = [wintypes.HANDLE, wintypes.LPVOID]
            k32.CancelIoEx.restype = wintypes.BOOL

    def start(self):
        self.server_thread = threading.Thread(
            target=self._server_loop, name="qmt-adapter-pipe-server"
        )
        self.server_thread.daemon = True
        self.server_thread.start()

    def stop(self):
        self.stop_event.set()
        with self.handle_lock:
            handle = self.current_handle
            self.current_handle = None
            self.authenticated_connection_id = None
        if handle is not None:
            if hasattr(self.kernel32, "CancelIoEx"):
                self.kernel32.CancelIoEx(handle, None)
            self.kernel32.DisconnectNamedPipe(handle)
            self.kernel32.CloseHandle(handle)

    def _server_loop(self):
        while not self.stop_event.is_set():
            handle = self.kernel32.CreateNamedPipeW(
                self.pipe_name,
                self.PIPE_ACCESS_DUPLEX,
                self.PIPE_TYPE_BYTE
                | self.PIPE_READMODE_BYTE
                | self.PIPE_WAIT
                | self.PIPE_REJECT_REMOTE_CLIENTS,
                1,
                65536,
                65536,
                0,
                None,
            )
            if handle == self.INVALID_HANDLE_VALUE:
                self.runtime.set_error(
                    "CreateNamedPipeW failed: %s" % ctypes.WinError(ctypes.get_last_error())
                )
                return
            with self.handle_lock:
                self.current_connection_id += 1
                connection_id = self.current_connection_id
                self.current_handle = handle
                self.authenticated_connection_id = None
            try:
                connected = self.kernel32.ConnectNamedPipe(handle, None)
                if not connected and ctypes.get_last_error() != self.ERROR_PIPE_CONNECTED:
                    raise ctypes.WinError(ctypes.get_last_error())
                self.runtime.set_connected(True)
                self._read_connection(handle, connection_id)
            except Exception as exc:
                if not self.stop_event.is_set() and not self._is_peer_disconnect(exc):
                    self.runtime.set_error("pipe connection failed: %s" % exc)
            finally:
                self.runtime.set_connected(False)
                self._close_handle(handle, connection_id)

    def _read_connection(self, handle, connection_id):
        authenticated = False
        while not self.stop_event.is_set():
            header = self._read_exact(handle, 4)
            size = struct.unpack(">I", header)[0]
            if size <= 0 or size > self.max_message_size:
                raise BridgeError("PROTOCOL_ERROR", "invalid frame size")
            payload = self._read_exact(handle, size)
            message = json.loads(payload.decode("utf-8"))
            if not authenticated:
                if message.get("type") != "hello":
                    raise BridgeError("AUTH_REQUIRED", "hello is required")
                if message.get("v") != PROTOCOL_VERSION:
                    self._send_direct(
                        handle,
                        {
                            "v": PROTOCOL_VERSION,
                            "type": "hello_ack",
                            "request_id": message.get("message_id"),
                            "ok": False,
                            "code": "PROTOCOL_MISMATCH",
                            "error": {
                                "message": "client and bridge protocol versions differ",
                                "data": {
                                    "bridge_protocol_version": PROTOCOL_VERSION,
                                    "client_protocol_version": message.get("v"),
                                },
                            },
                        },
                    )
                    return
                if message.get("auth_token") != self.auth_token:
                    self._send_direct(
                        handle,
                        {
                            "v": PROTOCOL_VERSION,
                            "type": "hello_ack",
                            "request_id": message.get("message_id"),
                            "ok": False,
                            "code": "AUTH_FAILED",
                            "error": {"message": "authentication failed"},
                        },
                    )
                    return
                if message.get("environment") != self.runtime.environment:
                    self._send_direct(
                        handle,
                        {
                            "v": PROTOCOL_VERSION,
                            "type": "hello_ack",
                            "request_id": message.get("message_id"),
                            "ok": False,
                            "code": "ENVIRONMENT_MISMATCH",
                            "error": {"message": "client and bridge environment differ"},
                        },
                    )
                    return
                authenticated = True
                with self.handle_lock:
                    self.authenticated_connection_id = connection_id
                self._send_direct(handle, self.runtime.hello_response(message))
                continue
            if message.get("type") != "request":
                raise BridgeError("PROTOCOL_ERROR", "request message expected")
            if self.runtime.is_local_read_command(message.get("command")):
                self._send_direct(handle, self.runtime.process_local_request(message))
            else:
                response_queue = queue.Queue(maxsize=1)
                immediate = self.runtime.submit_request(
                    connection_id, message, response_queue
                )
                if immediate is not None:
                    self._send_direct(handle, immediate)
                    continue
                completed = False
                while not self.stop_event.is_set():
                    try:
                        response_queue.get(timeout=0.1)
                        completed = True
                        break
                    except queue.Empty:
                        continue
                if not completed:
                    return

    def send_response(self, connection_id, message):
        with self.handle_lock:
            handle = self.current_handle
            active_id = self.authenticated_connection_id
        if handle is None or connection_id != active_id:
            return False
        try:
            self._send_direct(handle, message)
            return True
        except Exception as exc:
            if not self.stop_event.is_set() and not self._is_peer_disconnect(exc):
                self.runtime.set_error("pipe write failed: %s" % exc)
            return False

    def _send_direct(self, handle, message):
        encoded = json.dumps(
            message, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        if len(encoded) > self.max_message_size:
            raise BridgeError("PROTOCOL_ERROR", "response is too large")
        frame = struct.pack(">I", len(encoded)) + encoded
        with self.write_lock:
            self._write_all(handle, frame)

    def _read_exact(self, handle, size):
        chunks = []
        remaining = size
        while remaining:
            buffer = ctypes.create_string_buffer(remaining)
            received = wintypes.DWORD()
            if not self.kernel32.ReadFile(
                handle, buffer, remaining, ctypes.byref(received), None
            ):
                raise ctypes.WinError(ctypes.get_last_error())
            if not received.value:
                raise BridgeError("CONNECTION_CLOSED", "pipe closed during read")
            chunks.append(buffer.raw[: received.value])
            remaining -= received.value
        return b"".join(chunks)

    def _is_peer_disconnect(self, exc):
        return getattr(exc, "winerror", None) in (
            self.ERROR_BROKEN_PIPE,
            self.ERROR_NO_DATA,
        )

    def _write_all(self, handle, data):
        offset = 0
        while offset < len(data):
            part = data[offset:]
            buffer = ctypes.create_string_buffer(part)
            written = wintypes.DWORD()
            if not self.kernel32.WriteFile(
                handle, buffer, len(part), ctypes.byref(written), None
            ):
                raise ctypes.WinError(ctypes.get_last_error())
            if not written.value:
                raise BridgeError("CONNECTION_CLOSED", "pipe closed during write")
            offset += written.value

    def _close_handle(self, handle, connection_id):
        should_close = False
        with self.handle_lock:
            if self.current_handle == handle:
                self.current_handle = None
                should_close = True
            if self.authenticated_connection_id == connection_id:
                self.authenticated_connection_id = None
        if should_close:
            self.kernel32.DisconnectNamedPipe(handle)
            self.kernel32.CloseHandle(handle)


class OrderStore(object):
    def __init__(self, path):
        directory = os.path.dirname(path)
        if directory and not os.path.isdir(directory):
            os.makedirs(directory)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, timeout=5, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self._create_schema()

    def close(self):
        with self.lock:
            self.conn.close()

    def _create_schema(self):
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS orders (
                client_order_id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL UNIQUE,
                wire_order_tag TEXT NOT NULL UNIQUE,
                account_id TEXT NOT NULL,
                instrument TEXT NOT NULL,
                side TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                price_type TEXT NOT NULL,
                limit_price TEXT,
                user_remark TEXT,
                qmt_remark TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                qmt_order_id TEXT,
                status TEXT NOT NULL,
                raw_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_qmt_id
            ON orders(account_id, qmt_order_id)
            WHERE qmt_order_id IS NOT NULL;
            """
        )
        columns = {
            row[1] for row in self.conn.execute("PRAGMA table_info(orders)").fetchall()
        }
        if "payload_hash" not in columns:
            self.conn.execute("ALTER TABLE orders ADD COLUMN payload_hash TEXT")
        rows = self.conn.execute(
            "SELECT client_order_id,account_id,instrument,side,quantity,price_type,"
            "limit_price,user_remark FROM orders WHERE payload_hash IS NULL"
        ).fetchall()
        for row in rows:
            payload_hash = _order_payload_hash(
                row["account_id"],
                row["instrument"],
                row["side"],
                row["quantity"],
                row["price_type"],
                row["limit_price"],
                row["user_remark"],
            )
            self.conn.execute(
                "UPDATE orders SET payload_hash=? WHERE client_order_id=?",
                (payload_hash, row["client_order_id"]),
            )
        self.conn.commit()

    def insert_order(self, request_id, payload, wire_tag, qmt_remark, payload_hash):
        now = _utc_now_text()
        with self.lock:
            self.conn.execute(
                "INSERT INTO orders(client_order_id,request_id,wire_order_tag,account_id,"
                "instrument,side,quantity,price_type,limit_price,user_remark,qmt_remark,"
                "payload_hash,status,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'PENDING_QMT',?,?)",
                (
                    payload["client_order_id"],
                    request_id,
                    wire_tag,
                    payload["account_id"],
                    payload["instrument"],
                    payload["side"],
                    payload["quantity"],
                    payload["price_type"],
                    payload.get("limit_price"),
                    payload.get("remark", ""),
                    qmt_remark,
                    payload_hash,
                    now,
                    now,
                ),
            )
            self.conn.commit()

    def update_order(self, client_order_id, status=None, qmt_order_id=None, raw=None):
        with self.lock:
            row = self.get_order(client_order_id)
            if not row:
                return
            self.conn.execute(
                "UPDATE orders SET status=?,qmt_order_id=?,raw_json=?,updated_at=? "
                "WHERE client_order_id=?",
                (
                    status or row["status"],
                    qmt_order_id if qmt_order_id is not None else row["qmt_order_id"],
                    _canonical_json(raw) if raw is not None else row["raw_json"],
                    _utc_now_text(),
                    client_order_id,
                ),
            )
            self.conn.commit()

    def get_order(self, client_order_id):
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM orders WHERE client_order_id=?", (client_order_id,)
            ).fetchone()

    def get_order_by_qmt_id(self, account_id, qmt_order_id):
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM orders WHERE account_id=? AND qmt_order_id=?",
                (account_id, qmt_order_id),
            ).fetchone()

    def get_order_by_wire_tag(self, wire_order_tag):
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM orders WHERE wire_order_tag=?", (wire_order_tag,)
            ).fetchone()

    def list_orders(self, account_id=None):
        with self.lock:
            if account_id:
                return self.conn.execute(
                    "SELECT * FROM orders WHERE account_id=? ORDER BY created_at DESC",
                    (account_id,),
                ).fetchall()
            return self.conn.execute(
                "SELECT * FROM orders ORDER BY created_at DESC"
            ).fetchall()

    def list_pending_orders(self):
        with self.lock:
            return self.conn.execute(
                "SELECT client_order_id,wire_order_tag,account_id,status FROM orders "
                "WHERE qmt_order_id IS NULL OR "
                "status IN ('PENDING_BROKER_ID','CANCEL_PENDING')"
            ).fetchall()


class BridgeRuntime(object):
    PRICE_TYPES = {
        "ASK5": 0,  # 卖五价选价后限价申报
        "ASK4": 1,  # 卖四价选价后限价申报
        "ASK3": 2,  # 卖三价选价后限价申报
        "ASK2": 3,  # 卖二价选价后限价申报
        "ASK1": 4,  # 卖一价选价后限价申报
        "LATEST": 5,  # 最新价选价后限价申报
        "BID1": 6,  # 买一价选价后限价申报
        "BID2": 7,  # 买二价选价后限价申报
        "BID3": 8,  # 买三价选价后限价申报
        "BID4": 9,  # 买四价选价后限价申报
        "BID5": 10,  # 买五价选价后限价申报
        "LIMIT": 11,  # 指定限价申报
        "LIMIT_UP_DOWN": 12,  # 买入涨停/卖出跌停价选价后限价申报
        "QUEUE": 13,  # 本方一档选价后限价申报
        "COUNTERPARTY": 14,  # 对方一档选价后限价申报
        "MARKET_SH_CONVERT_5_CANCEL": 42,  # 沪/北市最优五档即成剩撤
        "MARKET_SH_CONVERT_5_LIMIT": 43,  # 沪/北市最优五档即成剩转限价
        "MARKET_PEER_PRICE_FIRST": 44,  # 沪/深/北市对手方最优市价申报
        "MARKET_MINE_PRICE_FIRST": 45,  # 沪/深/北市本方最优市价申报
        "MARKET_SZ_INSTBUSI_RESTCANCEL": 46,  # 深市即时成交剩余撤销
        "MARKET_SZ_CONVERT_5_CANCEL": 47,  # 深市最优五档即成剩撤
        "MARKET_SZ_FULL_OR_CANCEL": 48,  # 深市全额成交或撤销
    }

    def __init__(self, config):
        self.config = config
        self.environment = config.get("environment", "SIMULATION")
        self.trading_enabled = bool(config.get("trading_enabled", False))
        self.accounts = self._load_accounts(config.get("accounts", []))
        self.inbound = queue.Queue(maxsize=int(config.get("max_pending_commands", 1000)))
        self.order_events = queue.Queue()
        self.order_event_cache = {}
        self.order_event_cache_lock = threading.Lock()
        self.pending_broker_responses = {}
        self.pending_broker_lock = threading.Lock()
        self.connected = False
        self.last_error = ""
        self.last_reconcile = 0.0
        self.tick_lock = threading.Lock()
        self.tick_count = 0
        self.last_tick_time = None
        self.tick_intervals = deque(maxlen=500)
        self.started_at = _utc_now_text()
        self.store = OrderStore(config["db_path"])
        self.pipe = PipeServer(
            self,
            config.get("pipe_name", r"\\.\pipe\pazq_qmt_adapter_v1"),
            config["auth_token"],
            int(config.get("max_message_size", MAX_MESSAGE_SIZE)),
        )

    def _load_accounts(self, values):
        result = {}
        for item in values:
            account_id = str(item.get("account_id", "")).strip()
            account_type = str(item.get("account_type", "STOCK")).upper()
            if not account_id:
                continue
            if account_type != "STOCK":
                raise BridgeError(
                    "INVALID_CONFIG", "only STOCK accounts are supported in v1"
                )
            result[account_id] = {"account_id": account_id, "account_type": "STOCK"}
        return result

    def start(self):
        self.pipe.start()

    def stop(self):
        self.pipe.stop()
        self.store.close()

    def set_connected(self, value):
        self.connected = bool(value)

    def set_error(self, value):
        self.last_error = str(value)
        print("QMT Adapter: %s" % self.last_error)

    def hello_response(self, message):
        return {
            "v": PROTOCOL_VERSION,
            "type": "hello_ack",
            "request_id": message.get("message_id"),
            "ok": True,
            "code": "OK",
            "result": {
                "protocol_version": PROTOCOL_VERSION,
                "environment": self.environment,
                "trading_enabled": self.trading_enabled,
                "idempotency_mode": "CLIENT_ORDER_ID_ENFORCED",
                "accounts": list(self.accounts.values()),
                "commands": [
                    "system.health",
                    "account.get",
                    "position.list",
                    "order.place",
                    "order.get",
                    "order.list",
                    "order.cancel",
                ],
            },
        }

    def submit_request(self, connection_id, message, response_queue):
        try:
            self.inbound.put_nowait((connection_id, message, response_queue))
            return None
        except queue.Full:
            return self._error_response(
                message, "BACKPRESSURE", "command queue is full"
            )

    def is_local_read_command(self, command):
        return str(command or "") in ("system.health", "order.get", "order.list")

    def process_local_request(self, message):
        return self._process_request(message, None)

    def process_pending(self, context_info):
        now = time.perf_counter()
        with self.tick_lock:
            if self.last_tick_time is not None:
                self.tick_intervals.append(now - self.last_tick_time)
            self.last_tick_time = now
            self.tick_count += 1
        self.process_order_events()
        max_commands = int(self.config.get("max_commands_per_tick", 20))
        for unused in range(max_commands):
            try:
                connection_id, message, response_queue = self.inbound.get_nowait()
            except queue.Empty:
                break
            response = self._process_request(message, context_info)
            if self._defer_broker_response(
                connection_id, message, response_queue, response
            ):
                continue
            self._send_completed_response(
                connection_id, response_queue, response
            )
        interval = float(self.config.get("reconcile_interval_seconds", 1.0))
        if time.monotonic() - self.last_reconcile >= interval:
            self.last_reconcile = time.monotonic()
            try:
                self.reconcile_orders()
            except Exception as exc:
                self.set_error("order reconciliation failed: %s" % exc)

    def _send_completed_response(self, connection_id, response_queue, response):
        self.pipe.send_response(connection_id, response)
        try:
            response_queue.put_nowait(True)
        except queue.Full:
            self.set_error("response queue is full for connection %s" % connection_id)

    def _defer_broker_response(
        self, connection_id, message, response_queue, response
    ):
        if str(message.get("wait_for", "")).upper() != "BROKER_ID":
            return False
        if message.get("command") != "order.place" or not response.get("ok"):
            return False
        result = response.get("result") or {}
        client_order_id = str(result.get("client_order_id", ""))
        if not client_order_id:
            return False
        row = self.store.get_order(client_order_id)
        if row and row["qmt_order_id"]:
            response["result"] = self._merge_broker_result(result, row)
            return False
        waiter = {
            "connection_id": connection_id,
            "message": message,
            "response_queue": response_queue,
            "response": response,
        }
        with self.pending_broker_lock:
            self.pending_broker_responses[client_order_id] = waiter
        row = self.store.get_order(client_order_id)
        if row and row["qmt_order_id"]:
            self._complete_broker_response(client_order_id, row)
        return True

    def _complete_broker_response(self, client_order_id, row):
        with self.pending_broker_lock:
            waiter = self.pending_broker_responses.pop(client_order_id, None)
        if waiter is None:
            return False
        response = waiter["response"]
        response["result"] = self._merge_broker_result(response["result"], row)
        response["server_time"] = _utc_now_text()
        self._send_completed_response(
            waiter["connection_id"], waiter["response_queue"], response
        )
        return True

    def _merge_broker_result(self, initial_result, row):
        request_id = initial_result.get("request_id", "")
        original_request_id = initial_result.get(
            "original_request_id", row["request_id"]
        )
        idempotent_replay = bool(
            initial_result.get("idempotent_replay", False)
        )
        merged = dict(initial_result)
        merged.update(self._order_row(row))
        merged["request_id"] = request_id
        merged["original_request_id"] = original_request_id
        merged["idempotent_replay"] = idempotent_replay
        merged["command_status"] = initial_result.get(
            "command_status", "SUCCEEDED"
        )
        return merged

    def enqueue_order_event(self, order_info):
        raw = _serialize_qmt_object(order_info)
        remark = str(raw.get("m_strRemark", ""))
        qmt_order_id = str(raw.get("m_strOrderSysID", "")).strip()
        if not remark or not qmt_order_id:
            return
        wire_order_tag = remark.split(":", 1)[0]
        with self.order_event_cache_lock:
            self.order_event_cache[wire_order_tag] = raw
        try:
            row = self._persist_order_event(raw)
            if row is not None:
                with self.order_event_cache_lock:
                    self.order_event_cache.pop(wire_order_tag, None)
                self._complete_broker_response(row["client_order_id"], row)
                return
        except Exception as exc:
            self.set_error("order callback persistence failed: %s" % exc)
        self.order_events.put(raw)

    def _persist_order_event(self, raw):
        remark = str(raw.get("m_strRemark", ""))
        qmt_order_id = str(raw.get("m_strOrderSysID", "")).strip()
        if not remark or not qmt_order_id:
            return None
        wire_order_tag = remark.split(":", 1)[0]
        row = self.store.get_order_by_wire_tag(wire_order_tag)
        if not row:
            return None
        status = row["status"]
        if status in ("PENDING_QMT", "PENDING_BROKER_ID", "UNKNOWN"):
            status = "SUBMITTED"
        self.store.update_order(
            row["client_order_id"],
            status=status,
            qmt_order_id=qmt_order_id,
            raw=raw,
        )
        return self.store.get_order(row["client_order_id"])

    def process_order_events(self):
        while True:
            try:
                raw = self.order_events.get_nowait()
            except queue.Empty:
                return
            row = self._persist_order_event(raw)
            if row is None:
                continue
            wire_order_tag = str(raw.get("m_strRemark", "")).split(":", 1)[0]
            with self.order_event_cache_lock:
                if self.order_event_cache.get(wire_order_tag) is raw:
                    self.order_event_cache.pop(wire_order_tag, None)
            self._complete_broker_response(row["client_order_id"], row)

    def _process_request(self, message, context_info):
        request_id = str(message.get("request_id", "")).strip()
        command = str(message.get("command", "")).strip()
        payload = message.get("payload") or {}
        if not request_id or not command:
            return self._error_response(
                message, "INVALID_ARGUMENT", "request_id and command are required"
            )
        try:
            result = self._dispatch(command, payload, request_id, context_info)
            return self._success_response(message, result)
        except UncertainError as exc:
            return self._error_response(message, exc.code, exc.message, exc.data)
        except BridgeError as exc:
            return self._error_response(message, exc.code, exc.message, exc.data)
        except Exception as exc:
            error = {
                "code": "INTERNAL_ERROR",
                "message": "%s: %s" % (type(exc).__name__, exc),
            }
            self.set_error(traceback.format_exc())
            return self._error_response(message, "INTERNAL_ERROR", error["message"])

    def _dispatch(self, command, payload, request_id, context_info):
        if command == "system.health":
            return self.health()
        if command == "account.get":
            return self.query_account(payload)
        if command == "position.list":
            return self.query_positions(payload)
        if command == "order.place":
            return self.place_order(payload, request_id, context_info)
        if command == "order.get":
            return self.get_order(payload)
        if command == "order.list":
            return self.list_orders(payload)
        if command == "order.cancel":
            return self.cancel_order(payload, request_id, context_info)
        raise BridgeError("UNSUPPORTED_COMMAND", "unsupported command: %s" % command)

    def health(self):
        with self.tick_lock:
            tick_count = self.tick_count
            intervals = list(self.tick_intervals)
        intervals.sort()
        if intervals:
            tick_median_ms = intervals[len(intervals) // 2] * 1000.0
            tick_min_ms = intervals[0] * 1000.0
            tick_max_ms = intervals[-1] * 1000.0
        else:
            tick_median_ms = None
            tick_min_ms = None
            tick_max_ms = None
        return {
            "status": "OK" if not self.last_error else "DEGRADED",
            "environment": self.environment,
            "trading_enabled": self.trading_enabled,
            "connected": self.connected,
            "configured_accounts": list(self.accounts.values()),
            "pending_commands": self.inbound.qsize(),
            "last_error": self.last_error,
            "started_at": self.started_at,
            "timer_tick_count": tick_count,
            "timer_interval_median_ms": tick_median_ms,
            "timer_interval_min_ms": tick_min_ms,
            "timer_interval_max_ms": tick_max_ms,
        }

    def _require_account(self, payload):
        account_id = str(payload.get("account_id", "")).strip()
        if not account_id:
            raise BridgeError("INVALID_ARGUMENT", "account_id is required")
        if account_id not in self.accounts:
            raise BridgeError(
                "ACCOUNT_NOT_ALLOWED", "account is not configured in bridge_config.json"
            )
        return account_id

    def _qmt_function(self, name):
        function = globals().get(name)
        if not callable(function):
            raise BridgeError("QMT_API_MISSING", "QMT function is unavailable: %s" % name)
        return function

    def query_account(self, payload):
        account_id = self._require_account(payload)
        objects = self._qmt_function("get_trade_detail_data")(
            account_id, "STOCK", "ACCOUNT"
        )
        items = []
        for obj in objects or []:
            raw = _serialize_qmt_object(obj)
            items.append(_normalize_account(raw, account_id))
        return {
            "account_id": account_id,
            "account_type": "STOCK",
            "items": items,
            "count": len(items),
            "as_of": _utc_now_text(),
        }

    def query_positions(self, payload):
        account_id = self._require_account(payload)
        objects = self._qmt_function("get_trade_detail_data")(
            account_id, "STOCK", "POSITION"
        )
        items = []
        for obj in objects or []:
            raw = _serialize_qmt_object(obj)
            items.append(_normalize_position(raw, account_id))
        return {
            "account_id": account_id,
            "account_type": "STOCK",
            "items": items,
            "count": len(items),
            "as_of": _utc_now_text(),
        }

    def _validate_stock_order(self, payload):
        account_id = self._require_account(payload)
        if str(payload.get("account_type", "STOCK")).upper() != "STOCK":
            raise BridgeError("INVALID_ARGUMENT", "only STOCK is supported")
        if str(payload.get("business_type", "CASH")).upper() != "CASH":
            raise BridgeError("INVALID_ARGUMENT", "only CASH stock orders are supported")
        if str(payload.get("quantity_type", "SHARES")).upper() != "SHARES":
            raise BridgeError("INVALID_ARGUMENT", "only SHARES is supported")
        instrument = str(payload.get("instrument", "")).upper()
        if not re.match(r"^[0-9]{6}\.(SH|SZ|BJ)$", instrument):
            raise BridgeError("INVALID_ARGUMENT", "invalid stock code: %s" % instrument)
        side = str(payload.get("side", "")).upper()
        if side not in ("BUY", "SELL"):
            raise BridgeError("INVALID_ARGUMENT", "side must be BUY or SELL")
        quantity = payload.get("quantity")
        if not isinstance(quantity, int) or isinstance(quantity, bool) or quantity <= 0:
            raise BridgeError("INVALID_ARGUMENT", "quantity must be a positive integer")
        if side == "BUY" and quantity % 100 != 0:
            raise BridgeError("INVALID_ARGUMENT", "stock buy quantity must be a multiple of 100")
        price_type = str(payload.get("price_type", "LATEST")).upper()
        if price_type not in self.PRICE_TYPES:
            raise BridgeError("INVALID_ARGUMENT", "unsupported stock price_type")
        market = instrument.rsplit(".", 1)[-1]
        allowed_markets = STOCK_NATIVE_MARKET_PRICE_TYPES.get(price_type)
        if allowed_markets is not None and market not in allowed_markets:
            raise BridgeError(
                "INVALID_ARGUMENT",
                "%s is not supported for market %s" % (price_type, market),
            )
        limit_price = payload.get("limit_price")
        if price_type == "LIMIT":
            try:
                limit_price = float(limit_price)
            except Exception:
                raise BridgeError("INVALID_ARGUMENT", "LIMIT requires limit_price")
            if not math.isfinite(limit_price) or limit_price <= 0:
                raise BridgeError("INVALID_ARGUMENT", "limit_price must be positive")
        elif allowed_markets is not None:
            try:
                limit_price = float(0 if limit_price is None else limit_price)
            except Exception:
                raise BridgeError(
                    "INVALID_ARGUMENT",
                    "market order protection price must be between 0 and 9999",
                )
            if (
                not math.isfinite(limit_price)
                or limit_price < 0
                or limit_price > 9999
            ):
                raise BridgeError(
                    "INVALID_ARGUMENT",
                    "market order protection price must be between 0 and 9999",
                )
        return account_id, instrument, side, quantity, price_type, limit_price

    def _qmt_remark(self, client_order_id, user_remark):
        digest = hashlib.sha1(client_order_id.encode("utf-8")).hexdigest()[:20].upper()
        wire_tag = "QA" + digest
        maximum = int(self.config.get("qmt_remark_max_bytes", 64))
        prefix = wire_tag + ":"
        remaining = max(0, maximum - len(prefix.encode("ascii")))
        encoded = str(user_remark or "").encode("gbk", "replace")[:remaining]
        while True:
            try:
                suffix = encoded.decode("gbk")
                break
            except UnicodeDecodeError:
                encoded = encoded[:-1]
        return wire_tag, prefix + suffix

    def _place_order_result(self, row, request_id, idempotent_replay):
        result = self._order_row(row)
        result["request_id"] = request_id
        result["original_request_id"] = row["request_id"]
        result["command_status"] = "SUCCEEDED"
        result["idempotent_replay"] = bool(idempotent_replay)
        return result

    def place_order(self, payload, request_id, context_info):
        account_id, instrument, side, quantity, price_type, limit_price = (
            self._validate_stock_order(payload)
        )
        client_order_id = str(payload.get("client_order_id", "")).strip()
        if not client_order_id:
            raise BridgeError("INVALID_ARGUMENT", "client_order_id is required")
        payload_hash = _order_payload_hash(
            account_id,
            instrument,
            side,
            quantity,
            price_type,
            limit_price,
            payload.get("remark", ""),
        )
        existing = self.store.get_order(client_order_id)
        if existing:
            if existing["payload_hash"] != payload_hash:
                raise BridgeError(
                    "CLIENT_ORDER_ID_CONFLICT",
                    "client_order_id is already bound to different order parameters",
                    {"client_order_id": client_order_id},
                )
            if (
                existing["status"] in ("PENDING_QMT", "UNKNOWN")
                and not existing["qmt_order_id"]
            ):
                raise UncertainError(
                    "COMMAND_UNCERTAIN",
                    "the original order crossed an uncertain QMT call boundary",
                    {
                        "client_order_id": client_order_id,
                        "original_request_id": existing["request_id"],
                        "idempotent_replay": True,
                    },
                )
            return self._place_order_result(existing, request_id, True)
        if not self.trading_enabled:
            raise BridgeError("TRADING_DISABLED", "trading_enabled is false")
        try:
            if not context_info.is_last_bar():
                raise BridgeError("QMT_NOT_READY", "QMT is not on the latest bar")
        except AttributeError:
            raise BridgeError("QMT_NOT_READY", "ContextInfo.is_last_bar is unavailable")
        wire_tag, qmt_remark = self._qmt_remark(
            client_order_id, payload.get("remark", "")
        )
        try:
            self.store.insert_order(
                request_id, payload, wire_tag, qmt_remark, payload_hash
            )
        except sqlite3.IntegrityError:
            raise BridgeError(
                "CLIENT_ORDER_ID_CONFLICT",
                "client_order_id or request_id already exists",
                {"client_order_id": client_order_id},
            )
        op_type = 23 if side == "BUY" else 24
        if price_type == "LIMIT" or price_type in STOCK_NATIVE_MARKET_PRICE_TYPES:
            model_price = float(limit_price)
        else:
            model_price = -1
        try:
            self._qmt_function("passorder")(
                op_type,
                1101,
                account_id,
                instrument,
                self.PRICE_TYPES[price_type],
                model_price,
                quantity,
                STRATEGY_NAME,
                2,
                qmt_remark,
                context_info,
            )
        except Exception as exc:
            row = self.store.get_order(client_order_id)
            if row and row["qmt_order_id"]:
                self.store.update_order(client_order_id, status="SUBMITTED")
                return self._place_order_result(
                    self.store.get_order(client_order_id), request_id, False
                )
            self.store.update_order(client_order_id, status="UNKNOWN")
            raise UncertainError(
                "COMMAND_UNCERTAIN",
                "passorder raised after the QMT call boundary: %s" % exc,
                {"client_order_id": client_order_id},
            )
        row = self.store.get_order(client_order_id)
        if row and row["qmt_order_id"]:
            self.store.update_order(client_order_id, status="SUBMITTED")
        else:
            self.store.update_order(client_order_id, status="PENDING_BROKER_ID")
        return self._place_order_result(
            self.store.get_order(client_order_id), request_id, False
        )

    def get_order(self, payload):
        client_order_id = str(payload.get("client_order_id", "")).strip()
        qmt_order_id = str(payload.get("qmt_order_id", "")).strip()
        if client_order_id:
            row = self.store.get_order(client_order_id)
        elif qmt_order_id:
            account_id = self._require_account(payload)
            row = self.store.get_order_by_qmt_id(account_id, qmt_order_id)
        else:
            raise BridgeError(
                "INVALID_ARGUMENT", "client_order_id or qmt_order_id is required"
            )
        if not row:
            raise BridgeError("ORDER_NOT_FOUND", "order was not found")
        return self._order_row(row)

    def list_orders(self, payload):
        account_id = payload.get("account_id")
        if account_id:
            account_id = self._require_account(payload)
        rows = self.store.list_orders(account_id)
        return {"items": [self._order_row(row) for row in rows], "count": len(rows)}

    def cancel_order(self, payload, request_id, context_info):
        if not self.trading_enabled:
            raise BridgeError("TRADING_DISABLED", "trading_enabled is false")
        client_order_id = str(payload.get("client_order_id", "")).strip()
        if not client_order_id:
            raise BridgeError("INVALID_ARGUMENT", "client_order_id is required")
        row = self.store.get_order(client_order_id)
        if not row:
            raise BridgeError("ORDER_NOT_FOUND", "order was not found")
        qmt_order_id = row["qmt_order_id"]
        if not qmt_order_id:
            raise BridgeError(
                "ORDER_ID_PENDING",
                "QMT order id is not available yet",
                {"client_order_id": client_order_id},
            )
        can_cancel = self._qmt_function("can_cancel_order")(
            qmt_order_id, row["account_id"], "STOCK"
        )
        if not can_cancel:
            return {
                "client_order_id": client_order_id,
                "qmt_order_id": qmt_order_id,
                "cancel_requested": False,
                "order_status": row["status"],
            }
        try:
            sent = self._qmt_function("cancel")(
                qmt_order_id, row["account_id"], "STOCK", context_info
            )
        except Exception as exc:
            raise UncertainError(
                "COMMAND_UNCERTAIN",
                "cancel raised after the QMT call boundary: %s" % exc,
                {"client_order_id": client_order_id, "qmt_order_id": qmt_order_id},
            )
        if not sent:
            return {
                "client_order_id": client_order_id,
                "qmt_order_id": qmt_order_id,
                "cancel_requested": False,
                "order_status": row["status"],
            }
        self.store.update_order(client_order_id, status="CANCEL_PENDING")
        return {
            "client_order_id": client_order_id,
            "qmt_order_id": qmt_order_id,
            "cancel_requested": True,
            "order_status": "CANCEL_PENDING",
        }

    def reconcile_orders(self):
        pending = self.store.list_pending_orders()
        if not pending:
            return
        by_account = {}
        for row in pending:
            by_account.setdefault(row["account_id"], {})[row["wire_order_tag"]] = row
        query = self._qmt_function("get_trade_detail_data")
        for account_id, tags in by_account.items():
            objects = query(account_id, "STOCK", "ORDER", STRATEGY_NAME)
            for obj in objects or []:
                raw = _serialize_qmt_object(obj)
                remark = str(raw.get("m_strRemark", ""))
                qmt_order_id = str(raw.get("m_strOrderSysID", ""))
                if not remark or not qmt_order_id:
                    continue
                for wire_tag, row in tags.items():
                    if remark.startswith(wire_tag):
                        status = row["status"]
                        if status in (
                            "PENDING_QMT",
                            "PENDING_BROKER_ID",
                            "UNKNOWN",
                        ):
                            status = "SUBMITTED"
                        self.store.update_order(
                            row["client_order_id"],
                            status=status,
                            qmt_order_id=qmt_order_id,
                            raw=raw,
                        )
                        break

    def _order_row(self, row):
        result = {
            "client_order_id": row["client_order_id"],
            "request_id": row["request_id"],
            "account_id": row["account_id"],
            "account_type": "STOCK",
            "instrument": row["instrument"],
            "side": row["side"],
            "quantity": row["quantity"],
            "price_type": row["price_type"],
            "limit_price": row["limit_price"],
            "remark": row["user_remark"],
            "qmt_remark": row["qmt_remark"],
            "qmt_order_id": row["qmt_order_id"],
            "order_status": row["status"],
            "raw": json.loads(row["raw_json"]) if row["raw_json"] else None,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
        with self.order_event_cache_lock:
            raw = self.order_event_cache.get(row["wire_order_tag"])
        if raw:
            qmt_order_id = str(raw.get("m_strOrderSysID", "")).strip()
            if qmt_order_id:
                result["qmt_order_id"] = qmt_order_id
                if result["order_status"] in (
                    "PENDING_QMT",
                    "PENDING_BROKER_ID",
                    "UNKNOWN",
                ):
                    result["order_status"] = "SUBMITTED"
                result["raw"] = raw
        return result

    def _success_response(self, message, result):
        return {
            "v": PROTOCOL_VERSION,
            "type": "response",
            "request_id": message.get("request_id"),
            "ok": True,
            "code": "OK",
            "result": result,
            "error": None,
            "server_time": _utc_now_text(),
        }

    def _error_response(self, message, code, text, data=None):
        return {
            "v": PROTOCOL_VERSION,
            "type": "response",
            "request_id": message.get("request_id") or message.get("message_id"),
            "ok": False,
            "code": code,
            "result": None,
            "error": {"message": text, "data": data or {}},
            "server_time": _utc_now_text(),
        }


def _load_config():
    with open(CONFIG_PATH, "r") as handle:
        config = json.load(handle)
    if not config.get("auth_token"):
        raise BridgeError("INVALID_CONFIG", "auth_token is required")
    if not config.get("db_path"):
        config["db_path"] = os.path.join(os.path.dirname(CONFIG_PATH), "bridge.db")
    return config


def init(ContextInfo):
    global _RUNTIME
    if _RUNTIME is not None:
        return
    try:
        previous_runtime = getattr(builtins, _RUNTIME_SLOT, None)
        if previous_runtime is not None:
            previous_runtime.stop()
            delattr(builtins, _RUNTIME_SLOT)
        config = _load_config()
        _RUNTIME = BridgeRuntime(config)
        for account in _RUNTIME.accounts.values():
            ContextInfo.set_account(account["account_id"])
        _RUNTIME.start()
        ContextInfo.run_time(
            "_qmt_adapter_tick",
            config.get("timer_period", "10nMilliSecond"),
            "2000-01-01 00:00:00",
            "SH",
        )
        setattr(builtins, _RUNTIME_SLOT, _RUNTIME)
        print("QMT Adapter bridge is ready: %s" % config.get("pipe_name"))
        print("QMT Adapter trading_enabled=%s" % _RUNTIME.trading_enabled)
    except Exception as exc:
        print("QMT Adapter startup failed: %s: %s" % (type(exc).__name__, exc))
        print(traceback.format_exc())


def _qmt_adapter_tick(ContextInfo):
    if _RUNTIME is not None:
        _RUNTIME.process_pending(ContextInfo)


def handlebar(ContextInfo):
    pass


def order_callback(ContextInfo, orderInfo):
    if _RUNTIME is not None:
        _RUNTIME.enqueue_order_event(orderInfo)


def stop(ContextInfo):
    global _RUNTIME
    runtime = _RUNTIME
    if runtime is not None:
        runtime.stop()
        _RUNTIME = None
    if getattr(builtins, _RUNTIME_SLOT, None) is runtime:
        delattr(builtins, _RUNTIME_SLOT)
