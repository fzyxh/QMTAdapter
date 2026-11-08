# coding: gbk
"""Paste this entire ASCII file into a NEW QMT strategy (3.6 or 3.12)."""

import datetime
import importlib
import inspect
import json
import math
import os
import platform
import struct
import sys
import time
import uuid

# Edit only these settings. Never use the reserved global name "account".
ACCOUNT_ID = ""
ACCOUNT_TYPE = "STOCK"
QMT_VERSION_LABEL = ""
ACCOUNT_ENVIRONMENT = "UNKNOWN"  # SIMULATION / REAL / UNKNOWN; user supplied.
ENABLE_CALLBACKS = False  # Optional: bind this account for passive callbacks.
HISTORY_START = ""  # Optional YYYYMMDD; leave both empty to skip history.
HISTORY_END = ""
INSTRUMENTS = ["000001.SH", "399001.SZ", "899050.BJ"]
OUTPUT_DIR = ""  # Empty: TEMP. A write failure falls back to the QMT log.
SAMPLE_LIMIT = 3

_REPORT_PATH = None
_STARTED = False
_QUERIED = False
_COUNTS = {}
_RUN_ID = uuid.uuid4().hex
_GLOBAL_APIS = (
    "get_trade_detail_data", "get_history_trade_detail_data",
    "get_ipo_data", "get_new_purchase_limit", "query_bank_transfer_stream",
    "passorder", "cancel", "get_value_by_order_id",
)
_CONTEXT_APIS = (
    "set_account", "run_time", "get_full_tick", "get_market_data_ex",
    "get_instrument_detail", "get_stock_list_in_sector",
    "subscribe_whole_quote", "unsubscribe_quote",
)
_PRIVATE_KEYS = (
    "account", "stockholder", "token", "password", "remark", "orderref",
    "ordersysid", "tradeid", "compactno", "localinfo", "source", "orderparam",
)


def _scrub(text):
    text = str(text)
    if ACCOUNT_ID:
        text = text.replace(ACCOUNT_ID, "<ACCOUNT>")
    for key in ("USERPROFILE", "USERNAME"):
        value = os.environ.get(key, "")
        if value:
            text = text.replace(value, "<%s>" % key)
    return text


def _error(exc):
    return {"error_type": type(exc).__name__, "message": _scrub(exc)}


def _type(value):
    cls = type(value)
    return cls.__module__ + "." + cls.__name__


def _safe(value, key="", depth=0):
    if any(part in key.lower() for part in _PRIVATE_KEYS):
        return "<REDACTED>"
    if depth > 6:
        return {"truncated_type": _type(value)}
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else {"nonfinite": repr(value)}
    if isinstance(value, str):
        return _scrub(value)
    if isinstance(value, dict):
        return {_scrub(k): _safe(v, str(k), depth + 1) for k, v in list(value.items())[:100]}
    if isinstance(value, (list, tuple)):
        return [_safe(v, depth=depth + 1) for v in value[:100]]
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if type(value).__module__.startswith("numpy") and callable(getattr(value, "item", None)):
        try:
            return _safe(value.item(), key, depth + 1)
        except Exception as exc:
            return _error(exc)
    # Do not stringify opaque broker objects (may contain account identifiers).
    return {"opaque_type": _type(value)}


def _fields(value):
    fields = {}
    names = value.keys() if isinstance(value, dict) else [n for n in dir(value) if n.startswith("m_")]
    for name in sorted(names):
        try:
            item = value[name] if isinstance(value, dict) else getattr(value, name)
            if not callable(item):
                fields[name] = {"type": _type(item), "value": _safe(item, name)}
        except Exception as exc:
            fields[name] = _error(exc)
    return fields


def _summary(value):
    result = {"return_type": _type(value), "is_none": value is None}
    if isinstance(value, (list, tuple)):
        result["count"] = len(value)
        result["samples"] = [
            {"type": _type(v), "fields": _fields(v)} if any(n.startswith("m_") for n in dir(v))
            else _summary(v) for v in value[:SAMPLE_LIMIT]
        ]
    elif isinstance(value, dict):
        result["count"] = len(value)
        result["keys"] = [_scrub(k) for k in list(value)[:100]]
        if all(v is None or isinstance(v, (str, bool, int, float)) for v in value.values()):
            result["fields"] = _fields(value)
        result["samples"] = {}
        for key, item in list(value.items())[:SAMPLE_LIMIT]:
            if hasattr(item, "columns") and hasattr(item, "index"):
                result["samples"][_scrub(key)] = {
                    "type": _type(item), "shape": list(item.shape),
                    "columns": [str(c) for c in item.columns],
                    "dtypes": {str(k): str(v) for k, v in item.dtypes.items()},
                    "index_type": _type(item.index),
                    "index_dtype": str(item.index.dtype),
                    "index_samples": [_safe(v) for v in item.index[:SAMPLE_LIMIT]],
                    "rows": _safe(item.head(SAMPLE_LIMIT).to_dict(orient="records")),
                }
            else:
                result["samples"][_scrub(key)] = _safe(item, str(key))
    else:
        result["value"] = _safe(value)
    return result


def _emit(name, status, **data):
    global _REPORT_PATH
    record = dict(schema_version=1, run_id=_RUN_ID, name=name, status=status,
                  observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
    record.update(data)
    line = json.dumps(record, ensure_ascii=True, allow_nan=False)
    print("QMT312_PROBE " + line)
    if _REPORT_PATH:
        try:
            with open(_REPORT_PATH, "a", encoding="ascii") as handle:
                handle.write(line + "\n")
        except Exception as exc:
            _REPORT_PATH = None
            print("QMT312_PROBE " + json.dumps(dict(name="report_write", status="ERROR", **_error(exc)), ensure_ascii=True))


def _call(name, function, args=(), kwargs=None):
    inputs = _safe({"args": list(args), "kwargs": kwargs or {}})
    if not callable(function):
        _emit(name, "MISSING", inputs=inputs, error_type="NameError", message="Native API is not available")
        return
    started = time.perf_counter()
    try:
        value = function(*args, **(kwargs or {}))
        elapsed = (time.perf_counter() - started) * 1000
        _emit(name, "OK", elapsed_ms=elapsed, inputs=inputs,
              result=_summary(value))
    except Exception as exc:
        _emit(name, "ERROR", inputs=inputs, elapsed_ms=(time.perf_counter() - started) * 1000, **_error(exc))


def _inventory(name, value):
    result = {"callable": callable(value)}
    if callable(value):
        try:
            result["signature"] = str(inspect.signature(value))
        except Exception as exc:
            result["signature_error"] = _error(exc)
        result["doc"] = _scrub(getattr(value, "__doc__", "") or "")[:3000]
    _emit("api." + name, "PRESENT" if callable(value) else "MISSING", **result)


def init(ContextInfo):
    global _STARTED, _REPORT_PATH
    if _STARTED:
        return
    _STARTED = True
    output_dir = OUTPUT_DIR or os.environ.get("TEMP") or os.environ.get("TMP")
    if output_dir:
        path = os.path.join(output_dir, "qmt312_probe_" + _RUN_ID + ".jsonl")
        try:
            with open(path, "x", encoding="ascii"):
                pass
            _REPORT_PATH = path
            print("QMT312_PROBE_REPORT=" + path)
        except Exception as exc:
            _emit("report_create", "ERROR", **_error(exc))
    _emit("runtime", "OK", python_version=sys.version,
          implementation=platform.python_implementation(), pointer_bits=struct.calcsize("P") * 8,
          executable=_scrub(sys.executable), prefix=_scrub(sys.prefix),
          qmt_version_label=QMT_VERSION_LABEL, account_environment=ACCOUNT_ENVIRONMENT,
          account_type=ACCOUNT_TYPE, account_configured=bool(ACCOUNT_ID),
          target_runtime=(sys.implementation.name == "cpython" and sys.version_info[:2] == (3, 12)
                          and struct.calcsize("P") == 8))
    for name in ("sqlite3", "ctypes", "threading", "numpy", "pandas"):
        try:
            module = importlib.import_module(name)
            _emit("library." + name, "OK", version=str(getattr(module, "__version__", "stdlib")),
                  sqlite_version=getattr(module, "sqlite_version", None),
                  legacy_transaction_control=getattr(module, "LEGACY_TRANSACTION_CONTROL", None))
        except ImportError as exc:
            _emit("library." + name, "IMPORT_ERROR", **_error(exc))
        except Exception as exc:
            _emit("library." + name, "ERROR", **_error(exc))
    for name in _GLOBAL_APIS:
        _inventory(name, globals().get(name))
    for name in _CONTEXT_APIS:
        _inventory("ContextInfo." + name, getattr(ContextInfo, name, None))
    if ENABLE_CALLBACKS and ACCOUNT_ID:
        _call("bind_callbacks", getattr(ContextInfo, "set_account", None), (ACCOUNT_ID,))
    else:
        _emit("bind_callbacks", "SKIPPED", reason="ENABLE_CALLBACKS is false or ACCOUNT_ID is empty")
    _emit("initialized", "OK", next_step="Queries run once in after_init or the first handlebar")


def _queries(ContextInfo):
    global _QUERIED
    if not _STARTED or _QUERIED:
        return
    _QUERIED = True
    if ACCOUNT_ID:
        for kind in ("ACCOUNT", "POSITION", "ORDER", "DEAL"):
            _call("query." + kind, globals().get("get_trade_detail_data"),
                  (ACCOUNT_ID, ACCOUNT_TYPE, kind))
        for kind in ("POSITION", "ORDER", "DEAL"):
            if HISTORY_START and HISTORY_END:
                _call("history." + kind, globals().get("get_history_trade_detail_data"),
                      (ACCOUNT_ID, ACCOUNT_TYPE, kind, HISTORY_START, HISTORY_END))
            else:
                _emit("history." + kind, "SKIPPED", reason="History dates are empty")
    else:
        _emit("account_queries", "SKIPPED", reason="ACCOUNT_ID is empty; no account auto-discovery")
    _call("quote.full_tick", getattr(ContextInfo, "get_full_tick", None), (INSTRUMENTS,))
    for instrument in INSTRUMENTS:
        _call("instrument." + instrument, getattr(ContextInfo, "get_instrument_detail", None), (instrument,))
    for period, adjustment in (("1d", "none"), ("1d", "front_ratio"), ("1m", "none")):
        _call("bars.%s.%s" % (period, adjustment), getattr(ContextInfo, "get_market_data_ex", None),
              ([], INSTRUMENTS, period, "", "", 3, adjustment, False, False))
    _emit("query_phase_complete", "OK", callback_counts=dict(_COUNTS),
          note="Empty local history is inconclusive. No broker write APIs were called.")


def after_init(ContextInfo):
    _queries(ContextInfo)


def handlebar(ContextInfo):
    _queries(ContextInfo)


def _callback(name, value):
    if not ENABLE_CALLBACKS or not ACCOUNT_ID or not _STARTED:
        return
    try:
        if getattr(value, "m_strAccountID", None) != ACCOUNT_ID:
            return
        _COUNTS[name] = _COUNTS.get(name, 0) + 1
        if _COUNTS[name] <= SAMPLE_LIMIT:
            _emit("callback." + name, "OBSERVED", count=_COUNTS[name], fields=_fields(value))
    except Exception as exc:
        _emit("callback." + name, "ERROR", **_error(exc))


def account_callback(ContextInfo, accountInfo):
    _callback("account", accountInfo)


def order_callback(ContextInfo, orderInfo):
    _callback("order", orderInfo)


def deal_callback(ContextInfo, dealInfo):
    _callback("deal", dealInfo)


def position_callback(ContextInfo, positionInfo):
    _callback("position", positionInfo)


def stop(ContextInfo):
    _emit("stopped", "OK", callback_counts=dict(_COUNTS))
