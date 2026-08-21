# -*- coding: utf-8 -*-
"""QMT-side bridge for stock account, position, order and algorithm operations.

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


CONFIG_PATH = globals().get("QMT_ADAPTER_CONFIG_PATH") or os.environ.get(
    "QMT_ADAPTER_CONFIG",
    r"C:\QMTAdapter\config\bridge_config.json",
)

PROTOCOL_VERSION = 6
# UTF-8 JSON正文上限，不包含4字节帧长度前缀。
MAX_MESSAGE_SIZE = 5 * 1024 * 1024
STRATEGY_NAME = "QMT_ADAPTER_V1"
_RUNTIME = None

ALGORITHM_BOOK_LIQUIDITY_WEIGHTED = "BOOK_LIQUIDITY_WEIGHTED"
RESERVED_EXECUTION_ALGORITHMS = ("TWAP", "VWAP")
ALGO_TERMINAL_STATUSES = ("FILLED", "CANCELED", "FAILED")
ORDER_TERMINAL_STATUSES = ("FILLED", "CANCELED", "REJECTED")
ORDER_STATUSES = (
    "PENDING_QMT",
    "PENDING_BROKER_ID",
    "SUBMITTED",
    "PARTIALLY_FILLED",
    "CANCEL_PENDING",
    "FILLED",
    "CANCELED",
    "REJECTED",
    "UNKNOWN",
)
BOOK_LIQUIDITY_DEFAULTS = {
    "big_order_threshold": "1000000",
    "min_child_notional": "10000",
    "max_child_notional": "500000",
    "primary_levels": 3,
    "max_levels": 5,
    "chase_ticks": 2,
    "child_interval_ms": 50,
    "timeout_seconds": 20.0,
    "max_retries": 3,
}

STOCK_NATIVE_MARKET_PRICE_TYPES = {
    "MARKET_SH_CONVERT_5_CANCEL": {"SH", "BJ"},
    "MARKET_SH_CONVERT_5_LIMIT": {"SH", "BJ"},
    "MARKET_PEER_PRICE_FIRST": {"SH", "SZ", "BJ"},
    "MARKET_MINE_PRICE_FIRST": {"SH", "SZ", "BJ"},
    "MARKET_SZ_INSTBUSI_RESTCANCEL": {"SZ"},
    "MARKET_SZ_CONVERT_5_CANCEL": {"SZ"},
    "MARKET_SZ_FULL_OR_CANCEL": {"SZ"},
}
REVERSE_REPO_INSTRUMENTS = {
    "204001.SH",
    "204002.SH",
    "204003.SH",
    "204004.SH",
    "204007.SH",
    "204014.SH",
    "204028.SH",
    "204091.SH",
    "204182.SH",
    "131800.SZ",
    "131801.SZ",
    "131802.SZ",
    "131803.SZ",
    "131805.SZ",
    "131806.SZ",
    "131809.SZ",
    "131810.SZ",
    "131811.SZ",
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


def _utc_text_timestamp(value):
    if not value:
        return 0.0
    try:
        parsed = datetime.datetime.strptime(
            str(value), "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        return (parsed - datetime.datetime(1970, 1, 1)).total_seconds()
    except Exception:
        return 0.0


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
    account_id,
    instrument,
    side,
    quantity,
    price_type,
    limit_price,
    remark,
    order_kind="STOCK",
    business_type="CASH",
    quantity_type="SHARES",
    metadata=None,
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
        "business_type": str(business_type).upper(),
        "instrument": str(instrument).upper(),
        "limit_price": identity_price,
        "price_type": price_type,
        "quantity": int(quantity),
        "quantity_type": str(quantity_type).upper(),
        "remark": str(remark or ""),
        "side": str(side).upper(),
    }
    if str(order_kind).upper() != "STOCK":
        identity["order_kind"] = str(order_kind).upper()
        identity["metadata"] = metadata or {}
    return hashlib.sha256(_canonical_json(identity).encode("ascii")).hexdigest()


def _algo_payload_hash(payload):
    identity = {
        "account_id": str(payload.get("account_id", "")),
        "account_type": "STOCK",
        "algorithm": str(payload.get("algorithm", "")).upper(),
        "business_type": "CASH",
        "instrument": str(payload.get("instrument", "")).upper(),
        "params": payload.get("params") or {},
        "quantity": payload.get("quantity"),
        "remark": str(payload.get("remark", "")),
        "side": str(payload.get("side", "")).upper(),
        "target_amount": _canonical_price(payload.get("target_amount")),
    }
    return hashlib.sha256(_canonical_json(identity).encode("ascii")).hexdigest()


def _positive_decimal(value, name, allow_zero=False):
    try:
        number = decimal.Decimal(str(value))
    except Exception:
        raise BridgeError("INVALID_ARGUMENT", "%s must be numeric" % name)
    if not number.is_finite() or number < 0 or (not allow_zero and number == 0):
        comparison = "non-negative" if allow_zero else "positive"
        raise BridgeError(
            "INVALID_ARGUMENT", "%s must be %s" % (name, comparison)
        )
    return number


def _normalize_book_params(raw):
    raw = raw or {}
    if not isinstance(raw, dict):
        raise BridgeError("INVALID_ARGUMENT", "params must be an object")
    unknown = sorted(set(raw) - set(BOOK_LIQUIDITY_DEFAULTS))
    if unknown:
        raise BridgeError(
            "INVALID_ARGUMENT",
            "unsupported BOOK_LIQUIDITY_WEIGHTED params: %s" % ", ".join(unknown),
        )
    values = dict(BOOK_LIQUIDITY_DEFAULTS)
    values.update(raw)
    for name in (
        "big_order_threshold",
        "min_child_notional",
        "max_child_notional",
    ):
        values[name] = _canonical_price(_positive_decimal(values[name], name))
    if decimal.Decimal(values["min_child_notional"]) > decimal.Decimal(
        values["max_child_notional"]
    ):
        raise BridgeError(
            "INVALID_ARGUMENT",
            "min_child_notional cannot exceed max_child_notional",
        )
    for name, minimum, maximum in (
        ("primary_levels", 1, 5),
        ("max_levels", 1, 5),
        ("chase_ticks", 0, 100),
        ("child_interval_ms", 10, 60000),
        ("max_retries", 0, 10),
    ):
        value = values[name]
        if not isinstance(value, int) or isinstance(value, bool):
            raise BridgeError("INVALID_ARGUMENT", "%s must be an integer" % name)
        if value < minimum or value > maximum:
            raise BridgeError(
                "INVALID_ARGUMENT",
                "%s must be between %s and %s" % (name, minimum, maximum),
            )
    if values["primary_levels"] > values["max_levels"]:
        raise BridgeError(
            "INVALID_ARGUMENT", "primary_levels cannot exceed max_levels"
        )
    timeout_seconds = _positive_decimal(values["timeout_seconds"], "timeout_seconds")
    if timeout_seconds > decimal.Decimal("86400"):
        raise BridgeError(
            "INVALID_ARGUMENT", "timeout_seconds cannot exceed 86400"
        )
    values["timeout_seconds"] = float(timeout_seconds)
    return values


def _depth_sequence(tick, prefix, alternative_prefix):
    values = tick.get(prefix)
    if isinstance(values, (list, tuple)):
        return list(values[:5])
    result = []
    for index in range(1, 6):
        candidates = (
            "%s%s" % (prefix, index),
            "%s%s_%s" % (alternative_prefix, index, "price" if "Price" in prefix else "vol"),
        )
        found = None
        for key in candidates:
            if key in tick:
                found = tick.get(key)
                break
        result.append(found)
    return result


def _normalize_depth_tick(tick, instrument, side):
    if not isinstance(tick, dict):
        raise BridgeError("MARKET_DATA_UNAVAILABLE", "QMT tick is not an object")
    ask_prices = _depth_sequence(tick, "askPrice", "ask")
    ask_volumes = _depth_sequence(tick, "askVol", "ask")
    bid_prices = _depth_sequence(tick, "bidPrice", "bid")
    bid_volumes = _depth_sequence(tick, "bidVol", "bid")
    if side == "BUY":
        prices = ask_prices
        volumes = ask_volumes
        book_side = "ASK"
    else:
        prices = bid_prices
        volumes = bid_volumes
        book_side = "BID"
    levels = []
    for index in range(min(len(prices), len(volumes), 5)):
        try:
            price = decimal.Decimal(str(prices[index]))
            volume_lots = int(volumes[index])
        except Exception:
            continue
        if not price.is_finite() or price <= 0 or volume_lots < 0:
            continue
        levels.append(
            {
                "level": index + 1,
                "price": _canonical_price(price),
                "volume_lots": volume_lots,
                "visible_quantity": volume_lots * 100,
            }
        )
    if not levels:
        raise BridgeError(
            "MARKET_DATA_UNAVAILABLE",
            "no valid opposite-side depth for %s" % instrument,
        )
    def positive_price_text(value):
        try:
            price = decimal.Decimal(str(value))
        except Exception:
            return None
        if price.is_finite() and price > 0:
            return _canonical_price(price)
        return None

    def nonnegative_lots(value):
        try:
            lots = int(value)
        except Exception:
            return None
        return lots if lots >= 0 else None

    def quote_levels(level_prices, level_volumes):
        result = []
        for index in range(5):
            price = positive_price_text(
                level_prices[index] if index < len(level_prices) else None
            )
            lots = nonnegative_lots(
                level_volumes[index] if index < len(level_volumes) else None
            )
            result.append(
                {
                    "level": index + 1,
                    "price": price,
                    "volume_lots": lots,
                    "visible_quantity": lots * 100 if lots is not None else None,
                }
            )
        return result

    def first_positive(values):
        for value in values:
            price = positive_price_text(value)
            if price is not None:
                return price
        return None

    return {
        "instrument": instrument,
        "book_side": book_side,
        "volume_unit": "LOTS",
        "lot_size": 100,
        "last_price": positive_price_text(tick.get("lastPrice")),
        "last_close": positive_price_text(tick.get("lastClose")),
        "best_ask": first_positive(ask_prices),
        "best_bid": first_positive(bid_prices),
        "timetag": tick.get("timetag") or tick.get("stime"),
        "quote": {
            "ask_levels": quote_levels(ask_prices, ask_volumes),
            "bid_levels": quote_levels(bid_prices, bid_volumes),
        },
        "levels": levels,
    }


def _balanced_price_chunks(
    quantity, price_text, source, min_child_notional, max_child_notional
):
    if quantity <= 0 or quantity % 100:
        raise BridgeError("PLAN_INVALID", "child allocation must use whole lots")
    price = decimal.Decimal(str(price_text))
    max_notional = decimal.Decimal(str(max_child_notional))
    min_notional = decimal.Decimal(str(min_child_notional))
    maximum_lots = int(max_notional / (price * 100))
    if maximum_lots < 1:
        raise BridgeError(
            "PLAN_INVALID",
            "max_child_notional is below one board lot at price %s" % price_text,
        )
    total_lots = quantity // 100
    child_count = (total_lots + maximum_lots - 1) // maximum_lots
    base_lots = total_lots // child_count
    extra_lots = total_lots % child_count
    result = []
    for index in range(child_count):
        lots = base_lots + (1 if index < extra_lots else 0)
        child_quantity = lots * 100
        child_notional = price * child_quantity
        if child_count > 1 and child_notional < min_notional:
            raise BridgeError(
                "PLAN_INVALID",
                "min_child_notional and max_child_notional cannot both be met",
            )
        result.append(
            {
                "price": _canonical_price(price),
                "quantity": child_quantity,
                "estimated_notional": _canonical_price(child_notional),
                "source": source,
            }
        )
    return result


def _weighted_lot_allocations(target_lots, levels):
    capacities = [max(0, int(level["visible_quantity"]) // 100) for level in levels]
    total_capacity = sum(capacities)
    distributed = min(target_lots, total_capacity)
    if distributed <= 0 or total_capacity <= 0:
        return [0 for unused in levels]
    allocations = [
        min(capacity, distributed * capacity // total_capacity)
        for capacity in capacities
    ]
    remaining = distributed - sum(allocations)
    order = sorted(
        range(len(levels)),
        key=lambda index: (
            (distributed * capacities[index]) % total_capacity,
            capacities[index] - allocations[index],
            -index,
        ),
        reverse=True,
    )
    while remaining > 0:
        progressed = False
        for index in order:
            if allocations[index] < capacities[index]:
                allocations[index] += 1
                remaining -= 1
                progressed = True
                if remaining == 0:
                    break
        if not progressed:
            break
    return allocations


def _price_on_tick(value, price_tick, rounding):
    units = (value / price_tick).to_integral_value(rounding=rounding)
    return units * price_tick


def _calculate_price_cage(depth, price_tick):
    def price_or_none(value):
        try:
            price = decimal.Decimal(str(value))
        except Exception:
            return None
        if not price.is_finite() or price <= 0:
            return None
        return price

    best_ask = price_or_none(depth.get("best_ask"))
    best_bid = price_or_none(depth.get("best_bid"))
    last_price = price_or_none(depth.get("last_price"))
    last_close = price_or_none(depth.get("last_close"))
    buy_reference = best_ask or best_bid or last_price or last_close
    sell_reference = best_bid or best_ask or last_price or last_close
    if buy_reference is None or sell_reference is None:
        raise BridgeError(
            "MARKET_DATA_UNAVAILABLE", "price-cage reference price is unavailable"
        )
    buy_boundary = max(
        buy_reference * decimal.Decimal("1.02"),
        buy_reference + price_tick * 10,
    )
    sell_boundary = min(
        sell_reference * decimal.Decimal("0.98"),
        sell_reference - price_tick * 10,
    )
    buy_maximum = _price_on_tick(
        buy_boundary, price_tick, decimal.ROUND_FLOOR
    )
    sell_minimum = _price_on_tick(
        sell_boundary, price_tick, decimal.ROUND_CEILING
    )
    return {
        "buy_reference": _canonical_price(buy_reference),
        "buy_maximum": _canonical_price(buy_maximum),
        "sell_reference": _canonical_price(sell_reference),
        "sell_minimum": _canonical_price(sell_minimum),
        "price_tick": _canonical_price(price_tick),
    }


def _plan_book_quantity(quantity, depth, side, params):
    if quantity <= 0 or quantity % 100:
        raise BridgeError(
            "INVALID_ARGUMENT", "algorithm quantity must be a multiple of 100"
        )
    levels = list(depth["levels"][: int(params["max_levels"])])
    price_limits = depth.get("price_limits") or {}
    try:
        upper_limit = decimal.Decimal(str(price_limits["upper_limit"]))
        lower_limit = decimal.Decimal(str(price_limits["lower_limit"]))
    except Exception:
        raise BridgeError(
            "MARKET_DATA_UNAVAILABLE", "daily price limits are unavailable"
        )
    if upper_limit <= 0 or lower_limit <= 0 or upper_limit < lower_limit:
        raise BridgeError("MARKET_DATA_UNAVAILABLE", "daily price limits are invalid")
    price_cage = depth.get("price_cage") or {}
    try:
        buy_cage_maximum = decimal.Decimal(str(price_cage["buy_maximum"]))
        sell_cage_minimum = decimal.Decimal(str(price_cage["sell_minimum"]))
        price_tick = decimal.Decimal(str(price_cage["price_tick"]))
    except Exception:
        raise BridgeError("MARKET_DATA_UNAVAILABLE", "price cage is unavailable")
    if price_tick <= 0:
        raise BridgeError("MARKET_DATA_UNAVAILABLE", "price tick is invalid")
    best_price = decimal.Decimal(levels[0]["price"])
    threshold = decimal.Decimal(params["big_order_threshold"])
    min_child = decimal.Decimal(params["min_child_notional"])
    max_child = decimal.Decimal(params["max_child_notional"])
    assignments = []
    target_lots = quantity // 100
    if best_price * quantity <= threshold:
        assignments.append((levels[0], target_lots, "BEST_OPPOSITE"))
    else:
        primary = levels[: int(params["primary_levels"])]
        allocations = _weighted_lot_allocations(target_lots, primary)
        assigned_lots = 0
        for level, lots in zip(primary, allocations):
            if lots > 0 and decimal.Decimal(level["price"]) * lots * 100 >= min_child:
                assignments.append((level, lots, "DEPTH_%s" % level["level"]))
                assigned_lots += lots
        remaining_lots = target_lots - assigned_lots
        for level in levels[len(primary) :]:
            if remaining_lots <= 0:
                break
            capacity = max(0, int(level["visible_quantity"]) // 100)
            lots = min(remaining_lots, capacity)
            if lots > 0 and decimal.Decimal(level["price"]) * lots * 100 >= min_child:
                assignments.append((level, lots, "DEPTH_%s" % level["level"]))
                assigned_lots += lots
                remaining_lots -= lots
        if remaining_lots > 0:
            last_price = decimal.Decimal(levels[-1]["price"])
            ticks = decimal.Decimal(int(params["chase_ticks"])) * price_tick
            chase_price = last_price + ticks if side == "BUY" else last_price - ticks
            clamp_source = None
            if side == "BUY":
                maximum_price = min(upper_limit, buy_cage_maximum)
                if chase_price > maximum_price:
                    chase_price = maximum_price
                    clamp_source = (
                        "CHASE_DAILY_LIMIT_CLAMPED"
                        if upper_limit <= buy_cage_maximum
                        else "CHASE_PRICE_CAGE_CLAMPED"
                    )
            else:
                minimum_price = max(lower_limit, sell_cage_minimum)
                if chase_price < minimum_price:
                    chase_price = minimum_price
                    clamp_source = (
                        "CHASE_DAILY_LIMIT_CLAMPED"
                        if lower_limit >= sell_cage_minimum
                        else "CHASE_PRICE_CAGE_CLAMPED"
                    )
            if chase_price <= 0:
                raise BridgeError("PLAN_INVALID", "calculated chase price is invalid")
            chase_level = {
                "level": "CHASE",
                "price": _canonical_price(chase_price),
                "visible_quantity": 0,
            }
            assignments.append(
                (
                    chase_level,
                    remaining_lots,
                    clamp_source or "CHASE",
                )
            )

    children = []
    for level, lots, source in assignments:
        children.extend(
            _balanced_price_chunks(
                lots * 100,
                level["price"],
                source,
                min_child,
                max_child,
            )
        )
    planned_quantity = sum(child["quantity"] for child in children)
    if planned_quantity != quantity:
        raise BridgeError(
            "PLAN_INVALID",
            "planned quantity does not equal target quantity",
            {"target_quantity": quantity, "planned_quantity": planned_quantity},
        )
    for index, child in enumerate(children):
        child_price = decimal.Decimal(child["price"])
        if child_price > upper_limit or child_price < lower_limit:
            raise BridgeError(
                "PLAN_INVALID",
                "child price is outside daily price limits",
                {
                    "child_price": child["price"],
                    "upper_limit": _canonical_price(upper_limit),
                    "lower_limit": _canonical_price(lower_limit),
                },
            )
        if side == "BUY" and child_price > buy_cage_maximum:
            raise BridgeError(
                "PLAN_INVALID", "buy child price is above the price cage"
            )
        if side == "SELL" and child_price < sell_cage_minimum:
            raise BridgeError(
                "PLAN_INVALID", "sell child price is below the price cage"
            )
        child["child_index"] = index
    return children


def _plan_notional(children):
    return sum(
        (
            decimal.Decimal(child["price"]) * int(child["quantity"])
            for child in children
        ),
        decimal.Decimal("0"),
    )


def _plan_book_target(target_amount, quantity, depth, side, params):
    if target_amount is None:
        resolved_quantity = int(quantity)
        children = _plan_book_quantity(resolved_quantity, depth, side, params)
        return resolved_quantity, children
    amount = _positive_decimal(target_amount, "target_amount")
    best_price = decimal.Decimal(depth["levels"][0]["price"])
    upper_lots = int(amount / (best_price * 100))
    if upper_lots < 1:
        raise BridgeError(
            "INVALID_ARGUMENT", "target_amount is below one board lot"
        )
    lower = 1
    upper = upper_lots
    best_lots = 0
    best_children = None
    while lower <= upper:
        middle = (lower + upper) // 2
        children = _plan_book_quantity(middle * 100, depth, side, params)
        if _plan_notional(children) <= amount:
            best_lots = middle
            best_children = children
            lower = middle + 1
        else:
            upper = middle - 1
    if not best_children:
        raise BridgeError(
            "INVALID_ARGUMENT", "target_amount cannot fund one planned board lot"
        )
    return best_lots * 100, best_children


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


def _normalize_account(raw, configured_account_id, include_raw=False):
    result = {
        "account_id": raw.get("m_strAccountID", configured_account_id),
        "account_type": "STOCK",
        "total_asset": _optional_three_decimal_text(raw.get("m_dBalance")),
        "available_cash": _optional_three_decimal_text(raw.get("m_dAvailable")),
        "stock_market_value": _optional_three_decimal_text(
            raw.get("m_dStockValue")
        ),
        "withdrawable_cash": _optional_three_decimal_text(
            raw.get("m_dFetchBalance")
        ),
        "frozen_cash": _optional_three_decimal_text(raw.get("m_dFrozenCash")),
        "position_profit": _optional_three_decimal_text(
            raw.get("m_dPositionProfit")
        ),
    }
    if include_raw:
        result["raw"] = raw
    return result


def _normalize_position(raw, configured_account_id, include_raw=False):
    result = {
        "account_id": configured_account_id,
        "account_type": "STOCK",
        "instrument": _normalized_instrument(raw),
        "total_quantity": _raw_integer(raw, "m_nVolume"),
        "available_quantity": _raw_integer(raw, "m_nCanUseVolume"),
        "frozen_quantity": _raw_integer(raw, "m_nFrozenVolume"),
        "cost_price": _optional_three_decimal_text(raw.get("m_dOpenPrice")),
        "current_price": _optional_three_decimal_text(raw.get("m_dLastPrice")),
        "market_value": _optional_three_decimal_text(raw.get("m_dMarketValue")),
        "position_profit": _optional_three_decimal_text(
            raw.get("m_dPositionProfit")
        ),
    }
    if include_raw:
        result["raw"] = raw
    return result


def _first_mapping_value(mapping, names):
    for name in names:
        if name in mapping and mapping.get(name) not in (None, ""):
            return mapping.get(name)
    return None


def _normalize_new_issue_item(key, value, issue_type):
    raw = _safe_value(value)
    if not isinstance(raw, dict):
        raw = {"value": raw}
    instrument = str(key or "").strip().upper()
    if not re.match(r"^[0-9]{6}\.(SH|SZ|BJ)$", instrument):
        candidate = _first_mapping_value(
            raw,
            (
                "purchaseCode",
                "subscribeCode",
                "issueCode",
                "stockCode",
                "code",
            ),
        )
        instrument = str(candidate or instrument).strip().upper()
    market = _first_mapping_value(raw, ("market", "exchange", "marketCode"))
    market = str(market or "").strip().upper()
    if re.match(r"^[0-9]{6}$", instrument) and market in ("SH", "SZ", "BJ"):
        instrument += "." + market
    return {
        "instrument": instrument,
        "issue_type": str(issue_type).upper(),
        "name": _first_mapping_value(
            raw, ("name", "issueName", "stockName", "instrumentName")
        ),
        "issue_price": _first_mapping_value(raw, ("issuePrice", "price")),
        "min_quantity": _first_mapping_value(
            raw, ("minPurchaseNum", "minPurchaseQuantity")
        ),
        "max_quantity": _first_mapping_value(
            raw, ("maxPurchaseNum", "maxPurchaseQuantity")
        ),
        "subscription_date": _first_mapping_value(
            raw, ("purchaseDate", "subscribeDate", "issueDate")
        ),
        "raw": raw,
    }


def _raw_integer(raw, name):
    value = raw.get(name) if raw else None
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def _optional_decimal_text(value):
    if value in (None, ""):
        return None
    try:
        number = decimal.Decimal(str(value))
    except Exception:
        return None
    if not number.is_finite():
        return None
    return _canonical_price(number)


def _optional_three_decimal_text(value):
    text = _optional_decimal_text(value)
    if text is None:
        return None
    number = decimal.Decimal(text).quantize(
        decimal.Decimal("0.001"), rounding=decimal.ROUND_HALF_UP
    )
    return format(number, ".3f")


def _positive_three_decimal_text(value):
    text = _optional_decimal_text(value)
    if text is None or decimal.Decimal(text) <= 0:
        return None
    return _optional_three_decimal_text(text)


def _quote_levels(prices, volumes):
    if not isinstance(prices, (list, tuple)):
        prices = []
    if not isinstance(volumes, (list, tuple)):
        volumes = []
    levels = []
    for index in range(min(len(prices), 5)):
        price = _positive_three_decimal_text(prices[index])
        if price is None:
            continue
        volume_lots = None
        if index < len(volumes):
            try:
                candidate = int(volumes[index])
                if candidate > 0:
                    volume_lots = candidate
            except Exception:
                pass
        if volume_lots is None:
            continue
        levels.append(
            {
                "level": index + 1,
                "price": price,
                "volume_lots": volume_lots,
            }
        )
    return levels


def _normalize_quote(tick, detail, instrument, as_of, include_raw=False):
    exchange = str(detail.get("ExchangeID", "") or "").strip().upper()
    if exchange not in ("SH", "SZ", "BJ"):
        exchange = instrument.rsplit(".", 1)[-1]
    name = str(detail.get("InstrumentName", "") or "").strip() or None
    last_price_text = _optional_decimal_text(tick.get("lastPrice"))
    previous_close_text = _optional_decimal_text(
        tick.get("lastClose")
        if tick.get("lastClose") not in (None, "")
        else detail.get("PreClose")
    )
    change = None
    change_percent = None
    try:
        last_price = decimal.Decimal(last_price_text)
        previous_close = decimal.Decimal(previous_close_text)
        if previous_close > 0:
            change = _optional_three_decimal_text(last_price - previous_close)
            change_percent = _optional_three_decimal_text(
                (last_price - previous_close)
                / previous_close
                * decimal.Decimal("100")
            )
    except Exception:
        pass

    upper_limit = _positive_three_decimal_text(detail.get("UpStopPrice"))
    lower_limit = _positive_three_decimal_text(detail.get("DownStopPrice"))
    try:
        if (
            upper_limit is None
            or lower_limit is None
            or decimal.Decimal(upper_limit) <= decimal.Decimal(lower_limit)
        ):
            upper_limit = None
            lower_limit = None
    except Exception:
        upper_limit = None
        lower_limit = None

    ask_levels = _quote_levels(tick.get("askPrice"), tick.get("askVol"))
    bid_levels = _quote_levels(tick.get("bidPrice"), tick.get("bidVol"))
    result = {
        "instrument": instrument,
        "exchange": exchange,
        "instrument_name": name,
        "trading_day": str(detail.get("TradingDay") or "") or None,
        "quote_time": str(tick.get("timetag") or "") or None,
        "quote_timestamp_ms": _raw_integer(tick, "time"),
        "last_price": _optional_three_decimal_text(tick.get("lastPrice")),
        "previous_close": _optional_three_decimal_text(previous_close_text),
        "open_price": _optional_three_decimal_text(tick.get("open")),
        "high_price": _optional_three_decimal_text(tick.get("high")),
        "low_price": _optional_three_decimal_text(tick.get("low")),
        "change": change,
        "change_percent": change_percent,
        "turnover_amount": _optional_three_decimal_text(tick.get("amount")),
        "volume_lots": _raw_integer(tick, "volume"),
        "price_tick": _positive_three_decimal_text(detail.get("PriceTick")),
        "upper_limit": upper_limit,
        "lower_limit": lower_limit,
        "best_ask_price": ask_levels[0]["price"] if ask_levels else None,
        "best_bid_price": bid_levels[0]["price"] if bid_levels else None,
        "ask_levels": ask_levels,
        "bid_levels": bid_levels,
        "as_of": as_of,
    }
    if include_raw:
        result["raw"] = {
            "full_tick": _safe_value(tick),
            "instrument_detail": _safe_value(detail),
        }
    return result


def _raw_decimal_text(raw, name):
    return _optional_decimal_text(raw.get(name) if raw else None)


def _optional_money_text(value):
    text = _optional_decimal_text(value)
    if text is None:
        return None
    amount = decimal.Decimal(text).quantize(
        decimal.Decimal("0.01"), rounding=decimal.ROUND_HALF_UP
    )
    return _canonical_price(amount)


def _normalized_instrument(raw, fallback=None):
    instrument = str((raw or {}).get("m_strInstrumentID", "") or "").strip().upper()
    if not instrument:
        return fallback
    if "." in instrument:
        return instrument
    exchange = str((raw or {}).get("m_strExchangeID", "") or "").strip().upper()
    exchange_aliases = {
        "SSE": "SH",
        "XSHG": "SH",
        "SZSE": "SZ",
        "XSHE": "SZ",
        "BSE": "BJ",
    }
    exchange = exchange_aliases.get(exchange, exchange)
    if exchange in ("SH", "SZ", "BJ"):
        return instrument + "." + exchange
    return instrument


def _normalized_trade_side(raw, order_row=None):
    if order_row is not None:
        side = str(order_row["side"] or "").upper()
        if side in ("BUY", "SELL"):
            return side
    option_name = str((raw or {}).get("m_strOptName", "") or "").strip()
    if "买" in option_name:
        return "BUY"
    if "卖" in option_name:
        return "SELL"
    return None


def _normalize_trade(raw, configured_account_id, order_row=None, include_raw=False):
    result = {
        "trade_id": str(raw.get("m_strTradeID", "") or "").strip(),
        "account_id": str(
            raw.get("m_strAccountID", configured_account_id) or configured_account_id
        ),
        "client_order_id": (
            str(order_row["client_order_id"]) if order_row is not None else None
        ),
        "qmt_order_id": str(raw.get("m_strOrderSysID", "") or "").strip(),
        "instrument": _normalized_instrument(
            raw, order_row["instrument"] if order_row is not None else None
        ),
        "side": _normalized_trade_side(raw, order_row),
        "price": _raw_decimal_text(raw, "m_dPrice"),
        "quantity": _raw_integer(raw, "m_nVolume"),
        "amount": _optional_money_text(raw.get("m_dTradeAmount")),
        "commission": _optional_money_text(
            raw.get("m_dComssion", raw.get("m_dCommission"))
        ),
        "trade_date": str(raw.get("m_strTradeDate", "") or "").strip(),
        "trade_time": str(raw.get("m_strTradeTime", "") or "").strip(),
    }
    if include_raw:
        result["raw"] = raw
    return result


def _order_reject_reason(raw):
    if not raw:
        return None
    for name in ("m_strCancelInfo", "m_strErrorMsg"):
        value = str(raw.get(name, "") or "").strip()
        if value:
            return value
    return None


def _order_filled_quantity(raw):
    value = _raw_integer(raw, "m_nVolumeTraded")
    return max(0, value or 0)


def _reported_order_status(raw, current_status):
    if not raw:
        return current_status
    order_status = _raw_integer(raw, "m_nOrderStatus")
    status_map = {
        48: "SUBMITTED",
        49: "SUBMITTED",
        50: "SUBMITTED",
        51: "CANCEL_PENDING",
        52: "CANCEL_PENDING",
        53: "CANCELED",
        54: "CANCELED",
        55: "PARTIALLY_FILLED",
        56: "FILLED",
        57: "REJECTED",
        255: "UNKNOWN",
    }
    if order_status in status_map:
        return status_map[order_status]
    original = _raw_integer(raw, "m_nVolumeTotalOriginal")
    traded = _raw_integer(raw, "m_nVolumeTraded")
    remaining = _raw_integer(raw, "m_nVolumeTotal")
    error_message = str(raw.get("m_strErrorMsg", "") or "").strip()
    if error_message:
        return "REJECTED"
    if original is not None and original > 0 and traded is not None:
        if traded >= original:
            return "FILLED"
        if traded > 0:
            if remaining == 0 and current_status == "CANCEL_PENDING":
                return "CANCELED"
            return "PARTIALLY_FILLED"
    if remaining == 0 and current_status == "CANCEL_PENDING":
        return "CANCELED"
    if current_status in ("PENDING_QMT", "PENDING_BROKER_ID", "UNKNOWN"):
        return "SUBMITTED"
    return current_status


def _derive_order_status(raw, current_status):
    if not raw:
        return current_status
    if current_status in ORDER_TERMINAL_STATUSES:
        return current_status
    reported_status = _reported_order_status(raw, current_status)
    if current_status == "CANCEL_PENDING" and reported_status in (
        "SUBMITTED",
        "PARTIALLY_FILLED",
    ):
        return "CANCEL_PENDING"
    return reported_status


def _is_stale_terminal_order_event(raw, current_status):
    return (
        current_status in ORDER_TERMINAL_STATUSES
        and _reported_order_status(raw, current_status) != current_status
    )


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
        k32.PeekNamedPipe.argtypes = [
            wintypes.HANDLE,
            wintypes.LPVOID,
            wintypes.DWORD,
            ctypes.POINTER(wintypes.DWORD),
            ctypes.POINTER(wintypes.DWORD),
            ctypes.POINTER(wintypes.DWORD),
        ]
        k32.PeekNamedPipe.restype = wintypes.BOOL
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
                        if not self._peer_is_connected(handle):
                            return
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
        """发送一帧消息；响应过大时改发可关联的结构化错误。"""
        encoded = json.dumps(
            message, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        if len(encoded) > self.max_message_size:
            if message.get("type") != "response":
                raise BridgeError("PROTOCOL_ERROR", "outbound message is too large")
            encoded_size = len(encoded)
            message = {
                "v": PROTOCOL_VERSION,
                "type": "response",
                "request_id": message.get("request_id"),
                "ok": False,
                "code": "RESPONSE_TOO_LARGE",
                "result": None,
                "error": {
                    "message": "response exceeds max_message_size",
                    "data": {
                        "max_message_size": self.max_message_size,
                        "encoded_size": encoded_size,
                    },
                },
                "server_time": _utc_now_text(),
            }
            encoded = json.dumps(
                message, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
            if len(encoded) > self.max_message_size:
                raise BridgeError(
                    "PROTOCOL_ERROR", "response-too-large error does not fit"
                )
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

    def _peer_is_connected(self, handle):
        available = wintypes.DWORD()
        if self.kernel32.PeekNamedPipe(
            handle, None, 0, None, ctypes.byref(available), None
        ):
            return True
        error = ctypes.get_last_error()
        if error in (self.ERROR_BROKEN_PIPE, self.ERROR_NO_DATA):
            return False
        raise ctypes.WinError(error)

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
        self.runtime.discard_broker_responses(connection_id)
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
                order_kind TEXT NOT NULL DEFAULT 'STOCK',
                metadata_json TEXT,
                qmt_order_id TEXT,
                status TEXT NOT NULL,
                raw_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_qmt_id
            ON orders(account_id, qmt_order_id)
            WHERE qmt_order_id IS NOT NULL;

            CREATE TABLE IF NOT EXISTS trades (
                trade_key TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                trade_id TEXT NOT NULL,
                qmt_order_id TEXT NOT NULL,
                client_order_id TEXT,
                instrument TEXT,
                side TEXT,
                price TEXT,
                quantity INTEGER,
                amount TEXT,
                commission TEXT,
                trade_date TEXT,
                trade_time TEXT,
                raw_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_trades_client_order
            ON trades(client_order_id, trade_date, trade_time);
            CREATE INDEX IF NOT EXISTS ix_trades_qmt_order
            ON trades(account_id, qmt_order_id);

            CREATE TABLE IF NOT EXISTS algo_orders (
                algo_order_id TEXT PRIMARY KEY,
                request_id TEXT NOT NULL UNIQUE,
                payload_hash TEXT NOT NULL,
                account_id TEXT NOT NULL,
                instrument TEXT NOT NULL,
                side TEXT NOT NULL,
                algorithm TEXT NOT NULL,
                target_amount TEXT,
                target_quantity INTEGER,
                resolved_quantity INTEGER NOT NULL,
                filled_quantity INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                current_attempt INTEGER NOT NULL DEFAULT 0,
                params_json TEXT NOT NULL,
                user_remark TEXT,
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                error_code TEXT,
                error_message TEXT,
                attempt_started_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS algo_children (
                algo_order_id TEXT NOT NULL,
                client_order_id TEXT NOT NULL UNIQUE,
                attempt INTEGER NOT NULL,
                child_index INTEGER NOT NULL,
                price TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                source TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY(algo_order_id, attempt, child_index),
                FOREIGN KEY(algo_order_id) REFERENCES algo_orders(algo_order_id)
            );
            CREATE INDEX IF NOT EXISTS ix_algo_children_parent
            ON algo_children(algo_order_id, attempt, child_index);
            """
        )
        columns = {
            row[1] for row in self.conn.execute("PRAGMA table_info(orders)").fetchall()
        }
        if "payload_hash" not in columns:
            self.conn.execute("ALTER TABLE orders ADD COLUMN payload_hash TEXT")
        if "order_kind" not in columns:
            self.conn.execute(
                "ALTER TABLE orders ADD COLUMN order_kind TEXT NOT NULL "
                "DEFAULT 'STOCK'"
            )
        if "metadata_json" not in columns:
            self.conn.execute("ALTER TABLE orders ADD COLUMN metadata_json TEXT")
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
                "payload_hash,order_kind,metadata_json,status,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'PENDING_QMT',?,?)",
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
                    str(payload.get("order_kind", "STOCK")).upper(),
                    _canonical_json(payload.get("metadata") or {}),
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
            next_status = status or row["status"]
            next_raw_json = (
                _canonical_json(raw) if raw is not None else row["raw_json"]
            )
            if row["status"] in ORDER_TERMINAL_STATUSES:
                next_status = row["status"]
                if raw is not None and _is_stale_terminal_order_event(
                    raw, row["status"]
                ):
                    next_raw_json = row["raw_json"]
            self.conn.execute(
                "UPDATE orders SET status=?,qmt_order_id=?,raw_json=?,updated_at=? "
                "WHERE client_order_id=?",
                (
                    next_status,
                    qmt_order_id if qmt_order_id is not None else row["qmt_order_id"],
                    next_raw_json,
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
                "WHERE status NOT IN ('FILLED','CANCELED','REJECTED')"
            ).fetchall()

    def upsert_trade(self, trade, raw):
        trade_id = str(trade.get("trade_id", "") or "").strip()
        account_id = str(trade.get("account_id", "") or "").strip()
        qmt_order_id = str(trade.get("qmt_order_id", "") or "").strip()
        trade_date = str(trade.get("trade_date", "") or "").strip()
        if not trade_id or not account_id or not qmt_order_id:
            return False
        trade_key = hashlib.sha256(
            _canonical_json(
                [account_id, trade_date, trade_id, qmt_order_id]
            ).encode("ascii")
        ).hexdigest()
        now = _utc_now_text()
        values = (
            account_id,
            trade_id,
            qmt_order_id,
            trade.get("client_order_id"),
            trade.get("instrument"),
            trade.get("side"),
            trade.get("price"),
            trade.get("quantity"),
            trade.get("amount"),
            trade.get("commission"),
            trade_date,
            trade.get("trade_time"),
            _canonical_json(raw),
            now,
        )
        with self.lock:
            exists = self.conn.execute(
                "SELECT 1 FROM trades WHERE trade_key=?", (trade_key,)
            ).fetchone()
            if exists:
                self.conn.execute(
                    "UPDATE trades SET account_id=?,trade_id=?,qmt_order_id=?,"
                    "client_order_id=?,instrument=?,side=?,price=?,quantity=?,"
                    "amount=?,commission=?,trade_date=?,trade_time=?,raw_json=?,"
                    "updated_at=? WHERE trade_key=?",
                    values + (trade_key,),
                )
            else:
                self.conn.execute(
                    "INSERT INTO trades(trade_key,account_id,trade_id,qmt_order_id,"
                    "client_order_id,instrument,side,price,quantity,amount,commission,"
                    "trade_date,trade_time,raw_json,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (trade_key,) + values[:-1] + (now, now),
                )
            self.conn.commit()
        return True

    def trade_summary(self, client_order_id):
        with self.lock:
            rows = self.conn.execute(
                "SELECT price,quantity,amount FROM trades WHERE client_order_id=?",
                (client_order_id,),
            ).fetchall()
        filled_quantity = 0
        weighted_value = decimal.Decimal("0")
        weighted_quantity = 0
        filled_amount = decimal.Decimal("0")
        has_amount = False
        for row in rows:
            quantity = int(row["quantity"] or 0)
            if quantity > 0:
                filled_quantity += quantity
                price = _optional_decimal_text(row["price"])
                if price is not None:
                    weighted_value += decimal.Decimal(price) * quantity
                    weighted_quantity += quantity
            amount = _optional_decimal_text(row["amount"])
            if amount is not None:
                filled_amount += decimal.Decimal(amount)
                has_amount = True
        average_price = None
        if weighted_quantity > 0:
            average_price = _canonical_price(
                weighted_value / decimal.Decimal(weighted_quantity)
            )
        return {
            "trade_count": len(rows),
            "filled_quantity": filled_quantity,
            "average_filled_price": average_price,
            "filled_amount": _canonical_price(filled_amount) if has_amount else None,
        }

    def create_algo_order(
        self, request_id, payload, payload_hash, resolved_quantity, params, children
    ):
        now = _utc_now_text()
        with self.lock:
            try:
                self.conn.execute(
                    "INSERT INTO algo_orders(algo_order_id,request_id,payload_hash,"
                    "account_id,instrument,side,algorithm,target_amount,target_quantity,"
                    "resolved_quantity,status,current_attempt,params_json,user_remark,"
                    "attempt_started_at,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,'PLACING',0,?,?,?,?,?)",
                    (
                        payload["algo_order_id"],
                        request_id,
                        payload_hash,
                        payload["account_id"],
                        payload["instrument"],
                        payload["side"],
                        payload["algorithm"],
                        payload.get("target_amount"),
                        payload.get("quantity"),
                        int(resolved_quantity),
                        _canonical_json(params),
                        payload.get("remark", ""),
                        now,
                        now,
                        now,
                    ),
                )
                for child in children:
                    self.conn.execute(
                        "INSERT INTO algo_children(algo_order_id,client_order_id,"
                        "attempt,child_index,price,quantity,source,created_at) "
                        "VALUES(?,?,?,?,?,?,?,?)",
                        (
                            payload["algo_order_id"],
                            child["client_order_id"],
                            0,
                            int(child["child_index"]),
                            str(child["price"]),
                            int(child["quantity"]),
                            str(child["source"]),
                            now,
                        ),
                    )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def start_algo_attempt(self, algo_order_id, attempt, children):
        now = _utc_now_text()
        with self.lock:
            try:
                for child in children:
                    self.conn.execute(
                        "INSERT INTO algo_children(algo_order_id,client_order_id,"
                        "attempt,child_index,price,quantity,source,created_at) "
                        "VALUES(?,?,?,?,?,?,?,?)",
                        (
                            algo_order_id,
                            child["client_order_id"],
                            int(attempt),
                            int(child["child_index"]),
                            str(child["price"]),
                            int(child["quantity"]),
                            str(child["source"]),
                            now,
                        ),
                    )
                self.conn.execute(
                    "UPDATE algo_orders SET current_attempt=?,status='PLACING',"
                    "attempt_started_at=?,error_code=NULL,error_message=NULL,"
                    "updated_at=? WHERE algo_order_id=?",
                    (int(attempt), now, now, algo_order_id),
                )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

    def update_algo_order(self, algo_order_id, **values):
        allowed = {
            "filled_quantity",
            "status",
            "current_attempt",
            "cancel_requested",
            "error_code",
            "error_message",
            "attempt_started_at",
        }
        assignments = []
        parameters = []
        for name, value in values.items():
            if name not in allowed:
                raise ValueError("unsupported algo_orders column: %s" % name)
            assignments.append("%s=?" % name)
            parameters.append(value)
        if not assignments:
            return
        assignments.append("updated_at=?")
        parameters.append(_utc_now_text())
        parameters.append(algo_order_id)
        with self.lock:
            self.conn.execute(
                "UPDATE algo_orders SET %s WHERE algo_order_id=?"
                % ",".join(assignments),
                tuple(parameters),
            )
            self.conn.commit()

    def get_algo_order(self, algo_order_id):
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM algo_orders WHERE algo_order_id=?", (algo_order_id,)
            ).fetchone()

    def list_algo_orders(self, account_id=None, active_only=False):
        with self.lock:
            clauses = []
            parameters = []
            if account_id:
                clauses.append("account_id=?")
                parameters.append(account_id)
            if active_only:
                clauses.append("status NOT IN ('FILLED','CANCELED','FAILED')")
            sql = "SELECT * FROM algo_orders"
            if clauses:
                sql += " WHERE " + " AND ".join(clauses)
            sql += " ORDER BY created_at DESC"
            return self.conn.execute(sql, tuple(parameters)).fetchall()

    def list_algo_children(self, algo_order_id, attempt=None):
        with self.lock:
            sql = (
                "SELECT c.algo_order_id,c.client_order_id,c.attempt,c.child_index,"
                "c.price,c.quantity,c.source,c.created_at AS child_created_at,"
                "o.request_id AS order_request_id,o.qmt_order_id,o.status AS "
                "order_status,o.raw_json,o.created_at AS order_created_at,"
                "o.updated_at AS order_updated_at FROM algo_children c "
                "LEFT JOIN orders o ON o.client_order_id=c.client_order_id "
                "WHERE c.algo_order_id=?"
            )
            parameters = [algo_order_id]
            if attempt is not None:
                sql += " AND c.attempt=?"
                parameters.append(int(attempt))
            sql += " ORDER BY c.attempt,c.child_index"
            return self.conn.execute(sql, tuple(parameters)).fetchall()


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
        self.accounts = self._load_accounts(config.get("accounts", []))
        self.inbound = queue.Queue(maxsize=int(config.get("max_pending_commands", 1000)))
        self.order_events = queue.Queue()
        self.trade_events = queue.Queue()
        self.order_event_cache = {}
        self.order_event_cache_lock = threading.Lock()
        self.order_state_condition = threading.Condition()
        self.stopping = False
        self.pending_broker_responses = {}
        self.pending_broker_lock = threading.Lock()
        self.connected = False
        self.last_error = ""
        self.last_reconcile = 0.0
        self.last_algo_scan = 0.0
        self.algo_cancel_attempts = {}
        self.algo_submission_pending = False
        self.next_algo_submit_at = 0.0
        self.tick_lock = threading.Lock()
        self.tick_count = 0
        self.last_tick_time = None
        self.tick_intervals = deque(maxlen=500)
        self.started_at = _utc_now_text()
        self.store = OrderStore(config["db_path"])
        self.algo_submission_pending = any(
            row["status"] == "PLACING"
            for row in self.store.list_algo_orders(active_only=True)
        )
        self.pipe = PipeServer(
            self,
            config.get("pipe_name", r"\\.\pipe\qmt_adapter"),
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
                    "INVALID_CONFIG", "only STOCK accounts are supported"
                )
            result[account_id] = {"account_id": account_id, "account_type": "STOCK"}
        return result

    def start(self):
        self.pipe.start()

    def stop(self):
        self.stopping = True
        with self.order_state_condition:
            self.order_state_condition.notify_all()
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
                "idempotency_mode": "CLIENT_ORDER_ID_ENFORCED",
                "accounts": list(self.accounts.values()),
                "commands": [
                    "system.health",
                    "account.get",
                    "position.list",
                    "quote.get",
                    "quote.list",
                    "new_issue.list",
                    "new_issue.quota.get",
                    "order.place",
                    "order.get",
                    "order.list",
                    "order.wait",
                    "order.cancel",
                    "trade.list",
                    "algo_order.preview",
                    "algo_order.place",
                    "algo_order.get",
                    "algo_order.list",
                    "algo_order.cancel",
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
        return str(command or "") in (
            "system.health",
            "order.get",
            "order.list",
            "order.wait",
            "algo_order.get",
            "algo_order.list",
        )

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
        self.process_trade_events()
        if time.monotonic() - self.last_algo_scan >= 0.1:
            self.last_algo_scan = time.monotonic()
            try:
                self.process_algo_orders(context_info)
            except Exception as exc:
                self.set_error("algorithm order processing failed: %s" % exc)
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
        try:
            self.process_algo_submissions(context_info)
        except Exception as exc:
            self.set_error("algorithm child submission failed: %s" % exc)
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

    def discard_broker_responses(self, connection_id):
        """Drop BROKER_ID waiters owned by a disconnected pipe client."""
        with self.pending_broker_lock:
            client_order_ids = [
                client_order_id
                for client_order_id, waiter in self.pending_broker_responses.items()
                if waiter["connection_id"] == connection_id
            ]
            for client_order_id in client_order_ids:
                self.pending_broker_responses.pop(client_order_id, None)
        return len(client_order_ids)

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

    def _notify_order_state(self):
        with self.order_state_condition:
            self.order_state_condition.notify_all()

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
        status = _derive_order_status(raw, row["status"])
        self.store.update_order(
            row["client_order_id"],
            status=status,
            qmt_order_id=qmt_order_id,
            raw=raw,
        )
        updated = self.store.get_order(row["client_order_id"])
        self._notify_order_state()
        return updated

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

    def _order_for_trade_raw(self, raw, configured_account_id):
        qmt_order_id = str(raw.get("m_strOrderSysID", "") or "").strip()
        row = None
        if qmt_order_id:
            row = self.store.get_order_by_qmt_id(
                configured_account_id, qmt_order_id
            )
        if row is not None:
            return row
        remark = str(raw.get("m_strRemark", "") or "")
        if remark:
            return self.store.get_order_by_wire_tag(remark.split(":", 1)[0])
        return None

    def _persist_trade_event(self, raw):
        account_id = str(raw.get("m_strAccountID", "") or "").strip()
        if account_id not in self.accounts:
            return None
        row = self._order_for_trade_raw(raw, account_id)
        if row is None:
            return None
        trade = _normalize_trade(raw, account_id, row, include_raw=False)
        if not self.store.upsert_trade(trade, raw):
            return None
        qmt_order_id = trade.get("qmt_order_id") or row["qmt_order_id"]
        summary = self.store.trade_summary(row["client_order_id"])
        filled_quantity = int(summary["filled_quantity"] or 0)
        status = row["status"]
        if filled_quantity >= int(row["quantity"]):
            status = "FILLED"
        elif filled_quantity > 0 and status not in ORDER_TERMINAL_STATUSES:
            status = "PARTIALLY_FILLED"
        self.store.update_order(
            row["client_order_id"],
            status=status,
            qmt_order_id=qmt_order_id,
        )
        updated = self.store.get_order(row["client_order_id"])
        self._notify_order_state()
        return updated

    def enqueue_trade_event(self, trade_info):
        raw = _serialize_qmt_object(trade_info)
        try:
            if self._persist_trade_event(raw) is not None:
                return
        except Exception as exc:
            self.set_error("trade callback persistence failed: %s" % exc)
        self.trade_events.put(raw)

    def process_trade_events(self):
        while True:
            try:
                raw = self.trade_events.get_nowait()
            except queue.Empty:
                return
            try:
                self._persist_trade_event(raw)
            except Exception as exc:
                self.set_error("queued trade persistence failed: %s" % exc)

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
        if command == "quote.get":
            return self.query_quote(payload, context_info)
        if command == "quote.list":
            return self.query_quotes(payload, context_info)
        if command == "new_issue.list":
            return self.query_new_issues(payload)
        if command == "new_issue.quota.get":
            return self.query_new_issue_quota(payload)
        if command == "order.place":
            return self.place_order(payload, request_id, context_info)
        if command == "order.get":
            return self.get_order(payload)
        if command == "order.list":
            return self.list_orders(payload)
        if command == "order.wait":
            return self.wait_orders(payload)
        if command == "order.cancel":
            return self.cancel_order(payload, request_id, context_info)
        if command == "trade.list":
            return self.query_trades(payload)
        if command == "algo_order.preview":
            return self.preview_algo_order(payload, context_info)
        if command == "algo_order.place":
            return self.place_algo_order(payload, request_id, context_info)
        if command == "algo_order.get":
            return self.get_algo_order(payload)
        if command == "algo_order.list":
            return self.list_algo_orders(payload)
        if command == "algo_order.cancel":
            return self.cancel_algo_order(payload, request_id, context_info)
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

    def _include_raw(self, payload):
        value = payload.get("include_raw", False)
        if not isinstance(value, bool):
            raise BridgeError("INVALID_ARGUMENT", "include_raw must be a boolean")
        return value

    def query_account(self, payload):
        account_id = self._require_account(payload)
        include_raw = self._include_raw(payload)
        objects = self._qmt_function("get_trade_detail_data")(
            account_id, "STOCK", "ACCOUNT"
        )
        items = []
        for obj in objects or []:
            raw = _serialize_qmt_object(obj)
            items.append(
                _normalize_account(raw, account_id, include_raw=include_raw)
            )
        return {
            "account_id": account_id,
            "account_type": "STOCK",
            "items": items,
            "count": len(items),
            "as_of": _utc_now_text(),
        }

    def query_positions(self, payload):
        account_id = self._require_account(payload)
        include_raw = self._include_raw(payload)
        objects = self._qmt_function("get_trade_detail_data")(
            account_id, "STOCK", "POSITION"
        )
        items = []
        for obj in objects or []:
            raw = _serialize_qmt_object(obj)
            items.append(
                _normalize_position(raw, account_id, include_raw=include_raw)
            )
        return {
            "account_id": account_id,
            "account_type": "STOCK",
            "items": items,
            "count": len(items),
            "as_of": _utc_now_text(),
        }

    def _query_quote_items(self, instruments, include_raw, context_info):
        self._require_latest_bar(context_info)
        ticks = context_info.get_full_tick(instruments)
        if not isinstance(ticks, dict):
            raise BridgeError(
                "QMT_DATA_ERROR", "ContextInfo.get_full_tick did not return a mapping"
            )
        missing = [instrument for instrument in instruments if instrument not in ticks]
        if missing:
            raise BridgeError(
                "MARKET_DATA_UNAVAILABLE",
                "QMT did not return full tick for: %s" % ", ".join(missing),
                {"instruments": missing},
            )
        as_of = _utc_now_text()
        items = []
        for instrument in instruments:
            tick = ticks.get(instrument)
            if not isinstance(tick, dict):
                raise BridgeError(
                    "MARKET_DATA_UNAVAILABLE",
                    "QMT full tick is not an object: %s" % instrument,
                )
            detail = context_info.get_instrument_detail(instrument)
            if detail is None:
                detail = {}
            if not isinstance(detail, dict):
                raise BridgeError(
                    "QMT_DATA_ERROR",
                    "ContextInfo.get_instrument_detail did not return an object: %s"
                    % instrument,
                )
            items.append(
                _normalize_quote(
                    tick, detail, instrument, as_of, include_raw=include_raw
                )
            )
        return items, as_of

    def query_quote(self, payload, context_info):
        instrument = str(payload.get("instrument", "") or "").strip().upper()
        if not re.match(r"^[0-9]{6}\.(SH|SZ|BJ)$", instrument):
            raise BridgeError(
                "INVALID_ARGUMENT", "invalid instrument code: %s" % instrument
            )
        include_raw = self._include_raw(payload)
        items, unused_as_of = self._query_quote_items(
            [instrument], include_raw, context_info
        )
        return items[0]

    def query_quotes(self, payload, context_info):
        instruments = payload.get("instruments")
        if not isinstance(instruments, list) or not instruments:
            raise BridgeError(
                "INVALID_ARGUMENT", "instruments must be a non-empty array"
            )
        normalized = []
        seen = set()
        for value in instruments:
            instrument = str(value or "").strip().upper()
            if not re.match(r"^[0-9]{6}\.(SH|SZ|BJ)$", instrument):
                raise BridgeError(
                    "INVALID_ARGUMENT", "invalid instrument code: %s" % instrument
                )
            if instrument in seen:
                raise BridgeError(
                    "INVALID_ARGUMENT", "duplicate instrument: %s" % instrument
                )
            seen.add(instrument)
            normalized.append(instrument)
        include_raw = self._include_raw(payload)
        items, as_of = self._query_quote_items(
            normalized, include_raw, context_info
        )
        return {"items": items, "count": len(items), "as_of": as_of}

    def query_trades(self, payload):
        account_id = self._require_account(payload)
        scope = str(payload.get("scope", "ADAPTER") or "").upper()
        if scope not in ("ADAPTER", "ACCOUNT"):
            raise BridgeError(
                "INVALID_ARGUMENT", "scope must be ADAPTER or ACCOUNT"
            )
        include_raw = self._include_raw(payload)
        requested_client_order_id = str(
            payload.get("client_order_id", "") or ""
        ).strip()
        requested_order = None
        if requested_client_order_id:
            requested_order = self.store.get_order(requested_client_order_id)
            if requested_order is None:
                raise BridgeError("ORDER_NOT_FOUND", "order was not found")
            if requested_order["account_id"] != account_id:
                raise BridgeError(
                    "INVALID_ARGUMENT",
                    "client_order_id does not belong to account_id",
                )
        query = self._qmt_function("get_trade_detail_data")
        if scope == "ADAPTER":
            objects = query(account_id, "STOCK", "DEAL", STRATEGY_NAME)
        else:
            objects = query(account_id, "STOCK", "DEAL")
        items = []
        for obj in objects or []:
            raw = _serialize_qmt_object(obj)
            raw.setdefault("m_strAccountID", account_id)
            order_row = self._order_for_trade_raw(raw, account_id)
            if requested_order is not None:
                if order_row is None or (
                    order_row["client_order_id"] != requested_client_order_id
                ):
                    continue
            if order_row is not None:
                self._persist_trade_event(raw)
                order_row = self.store.get_order(order_row["client_order_id"])
            items.append(
                _normalize_trade(
                    raw, account_id, order_row, include_raw=include_raw
                )
            )
        items.sort(
            key=lambda item: (
                item.get("trade_date") or "",
                item.get("trade_time") or "",
                item.get("trade_id") or "",
            )
        )
        return {
            "account_id": account_id,
            "account_type": "STOCK",
            "scope": scope,
            "items": items,
            "count": len(items),
            "as_of": _utc_now_text(),
        }

    def _new_issue_items(self, issue_type):
        """Read and normalize QMT's current issue mapping for one issue type."""
        raw = self._qmt_function("get_ipo_data")(issue_type)
        if raw is None:
            raw = {}
        if not isinstance(raw, dict):
            raise BridgeError(
                "QMT_DATA_ERROR", "get_ipo_data did not return a mapping"
            )
        return [
            _normalize_new_issue_item(key, value, issue_type)
            for key, value in raw.items()
        ]

    def query_new_issues(self, payload):
        """Return current stock and/or bond subscription data from QMT."""
        issue_type = str(payload.get("issue_type", "ALL")).upper()
        if issue_type not in ("ALL", "STOCK", "BOND"):
            raise BridgeError(
                "INVALID_ARGUMENT", "issue_type must be ALL, STOCK or BOND"
            )
        types = ("STOCK", "BOND") if issue_type == "ALL" else (issue_type,)
        items = []
        for current_type in types:
            items.extend(self._new_issue_items(current_type))
        items.sort(key=lambda item: (item["issue_type"], item["instrument"]))
        return {
            "issue_type": issue_type,
            "items": items,
            "count": len(items),
            "as_of": _utc_now_text(),
        }

    def query_new_issue_quota(self, payload):
        """Return QMT's unmodified new-purchase quota mapping for one account."""
        account_id = self._require_account(payload)
        raw = _safe_value(
            self._qmt_function("get_new_purchase_limit")(account_id)
        )
        return {
            "account_id": account_id,
            "account_type": "STOCK",
            "limits": raw,
            "raw": raw,
            "as_of": _utc_now_text(),
        }

    def _require_latest_bar(self, context_info):
        try:
            if not context_info.is_last_bar():
                raise BridgeError("QMT_NOT_READY", "QMT is not on the latest bar")
        except AttributeError:
            raise BridgeError(
                "QMT_NOT_READY", "ContextInfo.is_last_bar is unavailable"
            )

    def _validate_algo_order(self, payload):
        account_id = self._require_account(payload)
        if str(payload.get("account_type", "STOCK")).upper() != "STOCK":
            raise BridgeError("INVALID_ARGUMENT", "only STOCK is supported")
        if str(payload.get("business_type", "CASH")).upper() != "CASH":
            raise BridgeError("INVALID_ARGUMENT", "only CASH is supported")
        algo_order_id = str(payload.get("algo_order_id", "")).strip()
        if not algo_order_id:
            raise BridgeError("INVALID_ARGUMENT", "algo_order_id is required")
        instrument = str(payload.get("instrument", "")).upper()
        if not re.match(r"^[0-9]{6}\.(SH|SZ|BJ)$", instrument):
            raise BridgeError("INVALID_ARGUMENT", "invalid stock code: %s" % instrument)
        if instrument.endswith(".BJ"):
            raise BridgeError(
                "UNSUPPORTED_MARKET",
                "algorithm orders do not support Beijing Stock Exchange yet",
            )
        side = str(payload.get("side", "")).upper()
        if side not in ("BUY", "SELL"):
            raise BridgeError("INVALID_ARGUMENT", "side must be BUY or SELL")
        algorithm = str(payload.get("algorithm", "")).upper()
        if algorithm in RESERVED_EXECUTION_ALGORITHMS:
            raise BridgeError(
                "ALGORITHM_NOT_IMPLEMENTED",
                "%s is reserved but not implemented" % algorithm,
            )
        if algorithm != ALGORITHM_BOOK_LIQUIDITY_WEIGHTED:
            raise BridgeError(
                "INVALID_ARGUMENT", "unsupported algorithm: %s" % algorithm
            )
        has_amount = payload.get("target_amount") is not None
        has_quantity = payload.get("quantity") is not None
        if has_amount == has_quantity:
            raise BridgeError(
                "INVALID_ARGUMENT",
                "exactly one of target_amount and quantity is required",
            )
        target_amount = None
        quantity = None
        if has_amount:
            target_amount = _canonical_price(
                _positive_decimal(payload.get("target_amount"), "target_amount")
            )
        else:
            quantity = payload.get("quantity")
            if (
                not isinstance(quantity, int)
                or isinstance(quantity, bool)
                or quantity <= 0
                or quantity % 100
            ):
                raise BridgeError(
                    "INVALID_ARGUMENT",
                    "algorithm quantity must be a positive multiple of 100",
                )
        params = _normalize_book_params(payload.get("params") or {})
        return {
            "algo_order_id": algo_order_id,
            "account_id": account_id,
            "account_type": "STOCK",
            "business_type": "CASH",
            "instrument": instrument,
            "side": side,
            "algorithm": algorithm,
            "target_amount": target_amount,
            "quantity": quantity,
            "params": params,
            "remark": str(payload.get("remark", "") or ""),
        }

    def _read_stock_depth(self, instrument, side, context_info):
        get_full_tick = getattr(context_info, "get_full_tick", None)
        if not callable(get_full_tick):
            raise BridgeError(
                "QMT_API_MISSING", "ContextInfo.get_full_tick is unavailable"
            )
        ticks = get_full_tick([instrument])
        if not ticks or instrument not in ticks:
            raise BridgeError(
                "MARKET_DATA_UNAVAILABLE", "no full tick for %s" % instrument
            )
        depth = _normalize_depth_tick(ticks[instrument], instrument, side)
        get_instrument_detail = getattr(context_info, "get_instrument_detail", None)
        if not callable(get_instrument_detail):
            raise BridgeError(
                "QMT_API_MISSING", "ContextInfo.get_instrument_detail is unavailable"
            )
        detail = get_instrument_detail(instrument)
        if not isinstance(detail, dict):
            raise BridgeError(
                "MARKET_DATA_UNAVAILABLE", "instrument detail is unavailable"
            )
        try:
            upper_limit = _positive_decimal(
                detail.get("UpStopPrice"), "UpStopPrice"
            )
            lower_limit = _positive_decimal(
                detail.get("DownStopPrice"), "DownStopPrice"
            )
            price_tick = _positive_decimal(detail.get("PriceTick"), "PriceTick")
        except BridgeError:
            raise BridgeError(
                "MARKET_DATA_UNAVAILABLE",
                "daily price limits or price tick are unavailable",
            )
        if detail.get("IsTrading") is False:
            raise BridgeError(
                "MARKET_NOT_TRADING", "%s is not tradable" % instrument
            )
        try:
            pre_close = _positive_decimal(
                depth.get("last_close") or detail.get("PreClose"), "PreClose"
            )
        except BridgeError:
            raise BridgeError(
                "MARKET_DATA_UNAVAILABLE", "previous close is unavailable"
            )
        depth["last_close"] = _canonical_price(pre_close)
        try:
            last_price = _positive_decimal(depth.get("last_price"), "lastPrice")
            if last_price > 0 and pre_close > 0:
                depth["change_percent"] = float(
                    (last_price / pre_close - 1) * 100
                )
            else:
                depth["change_percent"] = None
        except BridgeError:
            depth["change_percent"] = None
        depth["price_limits"] = {
            "upper_limit": _canonical_price(upper_limit),
            "lower_limit": _canonical_price(lower_limit),
        }
        depth["price_cage"] = _calculate_price_cage(depth, price_tick)
        depth["instrument_detail"] = {
            "instrument_name": detail.get("InstrumentName"),
            "pre_close": _canonical_price(pre_close),
            "price_tick": _canonical_price(price_tick),
            "instrument_status": detail.get("InstrumentStatus"),
            "is_trading": detail.get("IsTrading"),
        }
        return depth

    def _account_available_cash(self, account_id):
        objects = self._qmt_function("get_trade_detail_data")(
            account_id, "STOCK", "ACCOUNT"
        )
        if not objects:
            raise BridgeError("ACCOUNT_DATA_UNAVAILABLE", "account data is empty")
        raw = _serialize_qmt_object(objects[0])
        try:
            available = decimal.Decimal(str(raw["m_dAvailable"]))
        except Exception:
            raise BridgeError(
                "ACCOUNT_DATA_UNAVAILABLE", "m_dAvailable is unavailable"
            )
        if not available.is_finite() or available < 0:
            raise BridgeError(
                "ACCOUNT_DATA_UNAVAILABLE", "m_dAvailable is invalid"
            )
        return available

    def _position_available_quantity(self, account_id, instrument):
        objects = self._qmt_function("get_trade_detail_data")(
            account_id, "STOCK", "POSITION"
        )
        for obj in objects or []:
            raw = _serialize_qmt_object(obj)
            if str(raw.get("m_strInstrumentID", "")).upper() != instrument:
                continue
            if "m_nCanUseVolume" not in raw:
                raise BridgeError(
                    "POSITION_DATA_UNAVAILABLE", "m_nCanUseVolume is unavailable"
                )
            try:
                return max(0, int(raw["m_nCanUseVolume"]))
            except Exception:
                raise BridgeError(
                    "POSITION_DATA_UNAVAILABLE", "m_nCanUseVolume is invalid"
                )
        return 0

    def _check_algo_resources(self, payload, resolved_quantity, planned_notional):
        if payload["side"] == "BUY":
            available_cash = self._account_available_cash(payload["account_id"])
            if planned_notional > available_cash:
                raise BridgeError(
                    "INSUFFICIENT_CASH",
                    "available cash is below planned order notional",
                    {
                        "available_cash": _canonical_price(available_cash),
                        "planned_notional": _canonical_price(planned_notional),
                    },
                )
            return {
                "available_cash": _canonical_price(available_cash),
                "available_quantity": None,
            }
        available_quantity = self._position_available_quantity(
            payload["account_id"], payload["instrument"]
        )
        if resolved_quantity > available_quantity:
            raise BridgeError(
                "INSUFFICIENT_POSITION",
                "available position is below planned sell quantity",
                {
                    "available_quantity": available_quantity,
                    "planned_quantity": resolved_quantity,
                },
            )
        return {
            "available_cash": None,
            "available_quantity": available_quantity,
        }

    def _build_algo_preview(self, payload, context_info):
        self._require_latest_bar(context_info)
        depth = self._read_stock_depth(
            payload["instrument"], payload["side"], context_info
        )
        resolved_quantity, children = _plan_book_target(
            payload.get("target_amount"),
            payload.get("quantity"),
            depth,
            payload["side"],
            payload["params"],
        )
        planned_quantity = sum(child["quantity"] for child in children)
        if planned_quantity != resolved_quantity:
            raise BridgeError(
                "PLAN_INVALID",
                "planned quantity does not equal resolved quantity",
                {
                    "resolved_quantity": resolved_quantity,
                    "planned_quantity": planned_quantity,
                },
            )
        planned_notional = _plan_notional(children)
        if payload.get("target_amount") is not None and payload["side"] == "BUY":
            if planned_notional > decimal.Decimal(payload["target_amount"]):
                raise BridgeError(
                    "PLAN_INVALID", "buy plan exceeds target_amount"
                )
        resources = self._check_algo_resources(
            payload, resolved_quantity, planned_notional
        )
        return {
            "algo_order_id": payload["algo_order_id"],
            "account_id": payload["account_id"],
            "instrument": payload["instrument"],
            "side": payload["side"],
            "algorithm": payload["algorithm"],
            "target_amount": payload.get("target_amount"),
            "target_quantity": payload.get("quantity"),
            "resolved_quantity": resolved_quantity,
            "planned_quantity": planned_quantity,
            "planned_notional": _canonical_price(planned_notional),
            "child_count": len(children),
            "children": children,
            "depth": depth,
            "resources": resources,
            "params": payload["params"],
            "as_of": _utc_now_text(),
        }

    def preview_algo_order(self, payload, context_info):
        normalized = self._validate_algo_order(payload)
        return self._build_algo_preview(normalized, context_info)

    def _prepare_algo_children(self, algo_order_id, attempt, children):
        prepared = []
        for child in children:
            item = dict(child)
            item["client_order_id"] = "%s:a%s:c%s" % (
                algo_order_id,
                attempt,
                child["child_index"],
            )
            prepared.append(item)
        return prepared

    def _algo_place_result(self, row, request_id, idempotent_replay):
        result = self._algo_row(row)
        result["request_id"] = request_id
        result["original_request_id"] = row["request_id"]
        result["command_status"] = "SUCCEEDED"
        result["idempotent_replay"] = bool(idempotent_replay)
        return result

    def place_algo_order(self, payload, request_id, context_info):
        normalized = self._validate_algo_order(payload)
        payload_hash = _algo_payload_hash(normalized)
        legacy_payload_hash = None
        if normalized["params"].get("child_interval_ms") == 50:
            legacy_normalized = dict(normalized)
            legacy_params = dict(normalized["params"])
            del legacy_params["child_interval_ms"]
            legacy_normalized["params"] = legacy_params
            legacy_payload_hash = _algo_payload_hash(legacy_normalized)
        existing = self.store.get_algo_order(normalized["algo_order_id"])
        if existing:
            if existing["payload_hash"] not in (payload_hash, legacy_payload_hash):
                raise BridgeError(
                    "ALGO_ORDER_ID_CONFLICT",
                    "algo_order_id is already bound to different parameters",
                    {"algo_order_id": normalized["algo_order_id"]},
                )
            return self._algo_place_result(existing, request_id, True)

        preview = self._build_algo_preview(normalized, context_info)
        children = self._prepare_algo_children(
            normalized["algo_order_id"], 0, preview["children"]
        )
        try:
            self.store.create_algo_order(
                request_id,
                normalized,
                payload_hash,
                preview["resolved_quantity"],
                normalized["params"],
                children,
            )
        except sqlite3.IntegrityError:
            raise BridgeError(
                "ALGO_ORDER_ID_CONFLICT",
                "algo_order_id or request_id already exists",
                {"algo_order_id": normalized["algo_order_id"]},
            )
        self.algo_submission_pending = True
        self._submit_algo_attempt(normalized["algo_order_id"], 0, context_info)
        row = self.store.get_algo_order(normalized["algo_order_id"])
        return self._algo_place_result(row, request_id, False)

    def _child_order_payload(self, parent, child):
        remark = "algo a%s/c%s %s" % (
            child["attempt"],
            child["child_index"],
            parent["user_remark"] or "",
        )
        return {
            "client_order_id": child["client_order_id"],
            "account_id": parent["account_id"],
            "account_type": "STOCK",
            "business_type": "CASH",
            "instrument": parent["instrument"],
            "side": parent["side"],
            "quantity_type": "SHARES",
            "quantity": int(child["quantity"]),
            "price_type": "LIMIT",
            "limit_price": child["price"],
            "remark": remark,
        }

    def _submit_algo_attempt(self, algo_order_id, attempt, context_info):
        parent = self.store.get_algo_order(algo_order_id)
        if not parent:
            raise BridgeError("ALGO_ORDER_NOT_FOUND", "algorithm order was not found")
        if parent["status"] != "PLACING" or parent["cancel_requested"]:
            return False
        children = self.store.list_algo_children(algo_order_id, attempt)
        if any(child["order_status"] == "UNKNOWN" for child in children):
            return False
        if any(child["order_status"] == "REJECTED" for child in children):
            return False
        planned = [child for child in children if child["order_status"] is None]
        if not planned:
            self.store.update_algo_order(
                algo_order_id,
                status="WORKING",
                attempt_started_at=_utc_now_text(),
                error_code=None,
                error_message=None,
            )
            return False
        now = time.monotonic()
        if now < self.next_algo_submit_at:
            return False
        child = planned[0]
        try:
            child_request_id = "ALG" + hashlib.sha256(
                child["client_order_id"].encode("utf-8")
            ).hexdigest()
            self.place_order(
                self._child_order_payload(parent, child),
                child_request_id,
                context_info,
            )
        except UncertainError:
            self.store.update_algo_order(algo_order_id, status="UNKNOWN")
            raise
        except BridgeError as exc:
            self.store.update_algo_order(
                algo_order_id,
                status="FINAL_CANCELING",
                error_code=exc.code,
                error_message=exc.message,
            )
            raise
        params = json.loads(parent["params_json"])
        self.next_algo_submit_at = time.monotonic() + (
            float(params.get("child_interval_ms", 50)) / 1000.0
        )
        remaining = [
            item
            for item in self.store.list_algo_children(algo_order_id, attempt)
            if item["order_status"] is None
        ]
        if not remaining:
            self.store.update_algo_order(
                algo_order_id,
                status="WORKING",
                attempt_started_at=_utc_now_text(),
                error_code=None,
                error_message=None,
            )
        return True

    def process_algo_submissions(self, context_info):
        if not self.algo_submission_pending:
            return
        if time.monotonic() < self.next_algo_submit_at:
            return
        parents = [
            parent
            for parent in reversed(self.store.list_algo_orders(active_only=True))
            if parent["status"] == "PLACING" and not parent["cancel_requested"]
        ]
        submitted = False
        for parent in parents:
            if self._submit_algo_attempt(
                parent["algo_order_id"], int(parent["current_attempt"]), context_info
            ):
                submitted = True
                break
        self.algo_submission_pending = submitted or any(
            parent["status"] == "PLACING" and not parent["cancel_requested"]
            for parent in self.store.list_algo_orders(active_only=True)
        )

    def _algo_child_row(self, row):
        raw = json.loads(row["raw_json"]) if row["raw_json"] else None
        status = row["order_status"] or "PLANNED"
        if raw:
            status = _derive_order_status(raw, status)
        return {
            "client_order_id": row["client_order_id"],
            "attempt": row["attempt"],
            "child_index": row["child_index"],
            "price": row["price"],
            "quantity": row["quantity"],
            "estimated_notional": _canonical_price(
                decimal.Decimal(row["price"]) * int(row["quantity"])
            ),
            "source": row["source"],
            "qmt_order_id": row["qmt_order_id"],
            "order_status": status,
            "filled_quantity": _order_filled_quantity(raw),
            "raw": raw,
            "created_at": row["order_created_at"] or row["child_created_at"],
            "updated_at": row["order_updated_at"],
        }

    def _algo_row(self, row):
        children = [
            self._algo_child_row(child)
            for child in self.store.list_algo_children(row["algo_order_id"])
        ]
        filled_quantity = sum(child["filled_quantity"] for child in children)
        current_children = [
            child for child in children if child["attempt"] == row["current_attempt"]
        ]
        return {
            "algo_order_id": row["algo_order_id"],
            "request_id": row["request_id"],
            "account_id": row["account_id"],
            "account_type": "STOCK",
            "instrument": row["instrument"],
            "side": row["side"],
            "algorithm": row["algorithm"],
            "target_amount": row["target_amount"],
            "target_quantity": row["target_quantity"],
            "resolved_quantity": row["resolved_quantity"],
            "filled_quantity": filled_quantity,
            "remaining_quantity": max(0, row["resolved_quantity"] - filled_quantity),
            "algo_status": row["status"],
            "current_attempt": row["current_attempt"],
            "params": json.loads(row["params_json"]),
            "remark": row["user_remark"],
            "cancel_requested": bool(row["cancel_requested"]),
            "error": (
                {"code": row["error_code"], "message": row["error_message"]}
                if row["error_code"] or row["error_message"]
                else None
            ),
            "child_count": len(children),
            "current_attempt_child_count": len(current_children),
            "current_attempt_planned_quantity": sum(
                child["quantity"] for child in current_children
            ),
            "children": children,
            "attempt_started_at": row["attempt_started_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def get_algo_order(self, payload):
        algo_order_id = str(payload.get("algo_order_id", "")).strip()
        if not algo_order_id:
            raise BridgeError("INVALID_ARGUMENT", "algo_order_id is required")
        row = self.store.get_algo_order(algo_order_id)
        if not row:
            raise BridgeError("ALGO_ORDER_NOT_FOUND", "algorithm order was not found")
        return self._algo_row(row)

    def list_algo_orders(self, payload):
        account_id = payload.get("account_id")
        if account_id:
            account_id = self._require_account(payload)
        rows = self.store.list_algo_orders(account_id)
        return {"items": [self._algo_row(row) for row in rows], "count": len(rows)}

    def _algo_child_states(self, algo_order_id):
        return [
            self._algo_child_row(child)
            for child in self.store.list_algo_children(algo_order_id)
        ]

    def _cancel_algo_children(self, parent, context_info):
        results = []
        now = time.monotonic()
        for child in self._algo_child_states(parent["algo_order_id"]):
            if child["order_status"] in ORDER_TERMINAL_STATUSES:
                continue
            if child["order_status"] == "CANCEL_PENDING":
                continue
            if not child["qmt_order_id"]:
                continue
            last_attempt = self.algo_cancel_attempts.get(child["client_order_id"], 0.0)
            if now - last_attempt < 1.0:
                continue
            self.algo_cancel_attempts[child["client_order_id"]] = now
            try:
                result = self.cancel_order(
                    {"client_order_id": child["client_order_id"]},
                    "ALGO-CANCEL-%s" % child["client_order_id"],
                    context_info,
                )
                results.append(result)
            except BridgeError as exc:
                results.append(
                    {
                        "client_order_id": child["client_order_id"],
                        "cancel_requested": False,
                        "error": {"code": exc.code, "message": exc.message},
                    }
                )
        return results

    def cancel_algo_order(self, payload, request_id, context_info):
        algo_order_id = str(payload.get("algo_order_id", "")).strip()
        if not algo_order_id:
            raise BridgeError("INVALID_ARGUMENT", "algo_order_id is required")
        row = self.store.get_algo_order(algo_order_id)
        if not row:
            raise BridgeError("ALGO_ORDER_NOT_FOUND", "algorithm order was not found")
        if row["status"] in ALGO_TERMINAL_STATUSES:
            result = self._algo_row(row)
            result["cancel_requests"] = []
            result["idempotent_replay"] = bool(row["cancel_requested"])
            return result
        was_cancel_requested = bool(row["cancel_requested"])
        self.store.update_algo_order(
            algo_order_id, cancel_requested=1, status="CANCELING"
        )
        row = self.store.get_algo_order(algo_order_id)
        cancel_requests = self._cancel_algo_children(row, context_info)
        result = self._algo_row(self.store.get_algo_order(algo_order_id))
        result["cancel_requests"] = cancel_requests
        result["idempotent_replay"] = was_cancel_requested
        return result

    def _retry_algo_order(self, parent, remaining_quantity, context_info):
        params = json.loads(parent["params_json"])
        depth = self._read_stock_depth(
            parent["instrument"], parent["side"], context_info
        )
        children = _plan_book_quantity(
            remaining_quantity, depth, parent["side"], params
        )
        planned_notional = _plan_notional(children)
        existing_children = self._algo_child_states(parent["algo_order_id"])
        if parent["side"] == "BUY" and parent["target_amount"] is not None:
            spent_upper_bound = sum(
                (
                    decimal.Decimal(child["price"])
                    * int(child["filled_quantity"])
                    for child in existing_children
                ),
                decimal.Decimal("0"),
            )
            remaining_budget = decimal.Decimal(parent["target_amount"]) - spent_upper_bound
            if planned_notional > remaining_budget:
                raise BridgeError(
                    "TARGET_AMOUNT_EXHAUSTED",
                    "retry plan would exceed the original target_amount",
                    {
                        "remaining_budget": _canonical_price(max(remaining_budget, 0)),
                        "retry_planned_notional": _canonical_price(planned_notional),
                    },
                )
        resource_payload = {
            "account_id": parent["account_id"],
            "instrument": parent["instrument"],
            "side": parent["side"],
        }
        self._check_algo_resources(
            resource_payload, remaining_quantity, planned_notional
        )
        next_attempt = int(parent["current_attempt"]) + 1
        prepared = self._prepare_algo_children(
            parent["algo_order_id"], next_attempt, children
        )
        self.store.start_algo_attempt(
            parent["algo_order_id"], next_attempt, prepared
        )
        self.algo_submission_pending = True
        self._submit_algo_attempt(
            parent["algo_order_id"], next_attempt, context_info
        )

    def _refresh_algo_order(self, parent, context_info):
        algo_order_id = parent["algo_order_id"]
        states = self._algo_child_states(algo_order_id)
        unknown = [child for child in states if child["order_status"] == "UNKNOWN"]
        missing = [child for child in states if child["order_status"] == "PLANNED"]

        filled_quantity = sum(child["filled_quantity"] for child in states)
        if filled_quantity != int(parent["filled_quantity"]):
            self.store.update_algo_order(
                algo_order_id, filled_quantity=filled_quantity
            )
            parent = self.store.get_algo_order(algo_order_id)
        if filled_quantity >= int(parent["resolved_quantity"]):
            self.store.update_algo_order(
                algo_order_id,
                status="FILLED",
                error_code=None,
                error_message=None,
            )
            return
        if unknown:
            if parent["status"] != "UNKNOWN":
                self.store.update_algo_order(algo_order_id, status="UNKNOWN")
            return

        active = [
            child
            for child in states
            if child["order_status"] not in ORDER_TERMINAL_STATUSES
            and child["order_status"] != "PLANNED"
        ]
        rejected = [
            child for child in states if child["order_status"] == "REJECTED"
        ]
        status = parent["status"]
        if parent["cancel_requested"] or status == "CANCELING":
            if active:
                self._cancel_algo_children(parent, context_info)
                return
            self.store.update_algo_order(algo_order_id, status="CANCELED")
            return

        if rejected and status not in ("FINAL_CANCELING", "RETRY_CANCELING"):
            self.store.update_algo_order(
                algo_order_id,
                status="FINAL_CANCELING",
                error_code="CHILD_REJECTED",
                error_message="one or more child orders were rejected",
            )
            parent = self.store.get_algo_order(algo_order_id)
            if active:
                self._cancel_algo_children(parent, context_info)
                return
            status = "FINAL_CANCELING"

        if status == "FINAL_CANCELING":
            if active:
                self._cancel_algo_children(parent, context_info)
                return
            self.store.update_algo_order(algo_order_id, status="FAILED")
            return

        if status == "RETRY_CANCELING":
            if active:
                self._cancel_algo_children(parent, context_info)
                return
            params = json.loads(parent["params_json"])
            if int(parent["current_attempt"]) >= int(params["max_retries"]):
                self.store.update_algo_order(
                    algo_order_id,
                    status="FAILED",
                    error_code="MAX_RETRIES_EXCEEDED",
                    error_message="algorithm child timeout retries exhausted",
                )
                return
            remaining_quantity = int(parent["resolved_quantity"]) - filled_quantity
            try:
                self._retry_algo_order(parent, remaining_quantity, context_info)
            except BridgeError as exc:
                self.store.update_algo_order(
                    algo_order_id,
                    status="FAILED",
                    error_code=exc.code,
                    error_message=exc.message,
                )
            return

        if missing and status == "PLACING":
            return

        if not active:
            self.store.update_algo_order(
                algo_order_id,
                status="FAILED",
                error_code="INCOMPLETE_WITHOUT_ACTIVE_CHILD",
                error_message="algorithm is incomplete but no child order is active",
            )
            return

        timeout_seconds = float(json.loads(parent["params_json"])["timeout_seconds"])
        attempt_started = _utc_text_timestamp(parent["attempt_started_at"])
        now_utc = time.time()
        if attempt_started and now_utc - attempt_started >= timeout_seconds:
            self.store.update_algo_order(algo_order_id, status="RETRY_CANCELING")
            self._cancel_algo_children(
                self.store.get_algo_order(algo_order_id), context_info
            )
        elif status == "UNKNOWN":
            self.store.update_algo_order(algo_order_id, status="WORKING")

    def process_algo_orders(self, context_info):
        for parent in self.store.list_algo_orders(active_only=True):
            try:
                self._refresh_algo_order(parent, context_info)
            except Exception as exc:
                self.set_error(
                    "algorithm order %s refresh failed: %s"
                    % (parent["algo_order_id"], exc)
                )

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
        market = instrument.rsplit(".", 1)[-1]
        if market == "BJ":
            if quantity > 1000000:
                raise BridgeError(
                    "INVALID_ARGUMENT",
                    "Beijing Stock Exchange quantity must not exceed 1000000",
                )
            if side == "BUY" and quantity < 100:
                raise BridgeError(
                    "INVALID_ARGUMENT",
                    "Beijing Stock Exchange buy quantity must be at least 100",
                )
        elif side == "BUY" and quantity % 100 != 0:
            raise BridgeError(
                "INVALID_ARGUMENT", "stock buy quantity must be a multiple of 100"
            )
        price_type = str(payload.get("price_type", "LATEST")).upper()
        if price_type not in self.PRICE_TYPES:
            raise BridgeError("INVALID_ARGUMENT", "unsupported stock price_type")
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

    def _existing_order_result(self, client_order_id, payload_hash, request_id):
        existing = self.store.get_order(client_order_id)
        if not existing:
            return None
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

    def _submit_resolved_order(
        self,
        payload,
        payload_hash,
        request_id,
        context_info,
        op_type,
        pr_type,
        model_price,
        volume,
    ):
        client_order_id = payload["client_order_id"]
        self._require_latest_bar(context_info)
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
        try:
            self._qmt_function("passorder")(
                op_type,
                1101,
                payload["account_id"],
                payload["instrument"],
                pr_type,
                model_price,
                volume,
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

    def _place_stock_order(self, payload, request_id, context_info):
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
        replay = self._existing_order_result(
            client_order_id, payload_hash, request_id
        )
        if replay is not None:
            return replay
        normalized = dict(payload)
        normalized["order_kind"] = "STOCK"
        normalized["account_id"] = account_id
        normalized["instrument"] = instrument
        normalized["side"] = side
        normalized["quantity"] = quantity
        normalized["price_type"] = price_type
        normalized["limit_price"] = limit_price
        normalized["metadata"] = {}
        if price_type == "LIMIT" or price_type in STOCK_NATIVE_MARKET_PRICE_TYPES:
            model_price = float(limit_price)
        else:
            model_price = -1
        return self._submit_resolved_order(
            normalized,
            payload_hash,
            request_id,
            context_info,
            23 if side == "BUY" else 24,
            self.PRICE_TYPES[price_type],
            model_price,
            quantity,
        )

    def _place_reverse_repo(self, payload, request_id, context_info):
        """Validate and submit one exchange reverse-repo order."""
        account_id = self._require_account(payload)
        client_order_id = str(payload.get("client_order_id", "")).strip()
        instrument = str(payload.get("instrument", "")).strip().upper()
        if not client_order_id:
            raise BridgeError("INVALID_ARGUMENT", "client_order_id is required")
        if str(payload.get("account_type", "STOCK")).upper() != "STOCK":
            raise BridgeError("INVALID_ARGUMENT", "only STOCK is supported")
        if str(payload.get("business_type", "")).upper() != "REVERSE_REPO":
            raise BridgeError("INVALID_ARGUMENT", "invalid reverse repo business_type")
        if str(payload.get("quantity_type", "")).upper() != "REPO_UNITS":
            raise BridgeError("INVALID_ARGUMENT", "invalid reverse repo quantity_type")
        if instrument not in REVERSE_REPO_INSTRUMENTS:
            raise BridgeError(
                "INVALID_ARGUMENT", "unsupported reverse repo instrument: %s" % instrument
            )
        amount = payload.get("amount")
        quantity = payload.get("quantity")
        if (
            not isinstance(amount, int)
            or isinstance(amount, bool)
            or amount <= 0
            or amount % 1000
        ):
            raise BridgeError(
                "INVALID_ARGUMENT", "amount must be a positive multiple of 1000 yuan"
            )
        if (
            not isinstance(quantity, int)
            or isinstance(quantity, bool)
            or quantity != amount // 100
        ):
            raise BridgeError(
                "INVALID_ARGUMENT", "reverse repo quantity does not match amount"
            )
        annual_rate = _positive_decimal(payload.get("limit_price"), "annual_rate")
        canonical_rate = _canonical_price(annual_rate)
        metadata = {"amount": amount}
        payload_hash = _order_payload_hash(
            account_id,
            instrument,
            "SELL",
            quantity,
            "LIMIT",
            canonical_rate,
            payload.get("remark", ""),
            "REVERSE_REPO",
            "REVERSE_REPO",
            "REPO_UNITS",
            metadata,
        )
        replay = self._existing_order_result(
            client_order_id, payload_hash, request_id
        )
        if replay is not None:
            return replay
        normalized = {
            "order_kind": "REVERSE_REPO",
            "client_order_id": client_order_id,
            "account_id": account_id,
            "instrument": instrument,
            "side": "SELL",
            "quantity": quantity,
            "price_type": "LIMIT",
            "limit_price": canonical_rate,
            "remark": str(payload.get("remark", "") or ""),
            "metadata": metadata,
        }
        return self._submit_resolved_order(
            normalized,
            payload_hash,
            request_id,
            context_info,
            24,
            self.PRICE_TYPES["LIMIT"],
            float(annual_rate),
            quantity,
        )

    def _place_new_issue_subscription(self, payload, request_id, context_info):
        """Resolve QMT's current issue price and submit one subscription."""
        account_id = self._require_account(payload)
        client_order_id = str(payload.get("client_order_id", "")).strip()
        instrument = str(payload.get("instrument", "")).strip().upper()
        issue_type = str(payload.get("issue_type", "")).upper()
        quantity = payload.get("quantity")
        if not client_order_id:
            raise BridgeError("INVALID_ARGUMENT", "client_order_id is required")
        if str(payload.get("account_type", "STOCK")).upper() != "STOCK":
            raise BridgeError("INVALID_ARGUMENT", "only STOCK is supported")
        if (
            str(payload.get("business_type", "")).upper()
            != "NEW_ISSUE_SUBSCRIPTION"
        ):
            raise BridgeError(
                "INVALID_ARGUMENT", "invalid new issue subscription business_type"
            )
        if issue_type not in ("STOCK", "BOND"):
            raise BridgeError("INVALID_ARGUMENT", "issue_type must be STOCK or BOND")
        if not re.match(r"^[0-9]{6}\.(SH|SZ|BJ)$", instrument):
            raise BridgeError("INVALID_ARGUMENT", "invalid issue code: %s" % instrument)
        if not isinstance(quantity, int) or isinstance(quantity, bool) or quantity <= 0:
            raise BridgeError("INVALID_ARGUMENT", "quantity must be a positive integer")
        metadata_identity = {"issue_type": issue_type}
        payload_hash = _order_payload_hash(
            account_id,
            instrument,
            "BUY",
            quantity,
            "LIMIT",
            None,
            payload.get("remark", ""),
            "NEW_ISSUE_SUBSCRIPTION",
            "NEW_ISSUE_SUBSCRIPTION",
            "SUBSCRIPTION_UNITS",
            metadata_identity,
        )
        replay = self._existing_order_result(
            client_order_id, payload_hash, request_id
        )
        if replay is not None:
            return replay
        item = None
        for candidate in self._new_issue_items(issue_type):
            if candidate["instrument"] == instrument:
                item = candidate
                break
        if item is None:
            raise BridgeError(
                "NEW_ISSUE_NOT_FOUND",
                "instrument is not present in QMT current %s issue data" % issue_type,
                {"instrument": instrument, "issue_type": issue_type},
            )
        issue_price = _positive_decimal(item.get("issue_price"), "issue_price")
        for field_name in ("min_quantity", "max_quantity"):
            raw_limit = item.get(field_name)
            if raw_limit in (None, ""):
                continue
            try:
                decimal_limit = decimal.Decimal(str(raw_limit))
                if decimal_limit != decimal_limit.to_integral_value():
                    raise ValueError()
                limit = int(decimal_limit)
            except Exception:
                raise BridgeError(
                    "QMT_DATA_ERROR", "%s is not an integer in QMT issue data" % field_name
                )
            if field_name == "min_quantity" and quantity < limit:
                raise BridgeError(
                    "INVALID_ARGUMENT",
                    "quantity is below QMT issue minimum",
                    {"quantity": quantity, "minimum": limit},
                )
            if field_name == "max_quantity" and quantity > limit:
                raise BridgeError(
                    "INVALID_ARGUMENT",
                    "quantity exceeds QMT issue maximum",
                    {"quantity": quantity, "maximum": limit},
                )
        canonical_issue_price = _canonical_price(issue_price)
        metadata = {
            "issue_type": issue_type,
            "issue_price": canonical_issue_price,
            "min_quantity": item.get("min_quantity"),
            "max_quantity": item.get("max_quantity"),
        }
        normalized = {
            "order_kind": "NEW_ISSUE_SUBSCRIPTION",
            "client_order_id": client_order_id,
            "account_id": account_id,
            "instrument": instrument,
            "side": "BUY",
            "quantity": quantity,
            "price_type": "LIMIT",
            "limit_price": canonical_issue_price,
            "remark": str(payload.get("remark", "") or ""),
            "metadata": metadata,
        }
        return self._submit_resolved_order(
            normalized,
            payload_hash,
            request_id,
            context_info,
            23,
            self.PRICE_TYPES["LIMIT"],
            float(issue_price),
            quantity,
        )

    def place_order(self, payload, request_id, context_info):
        order_kind = str(payload.get("order_kind", "STOCK")).upper()
        if order_kind == "STOCK":
            return self._place_stock_order(payload, request_id, context_info)
        if order_kind == "REVERSE_REPO":
            return self._place_reverse_repo(payload, request_id, context_info)
        if order_kind == "NEW_ISSUE_SUBSCRIPTION":
            return self._place_new_issue_subscription(
                payload, request_id, context_info
            )
        raise BridgeError(
            "INVALID_ARGUMENT", "unsupported order_kind: %s" % order_kind
        )

    def get_order(self, payload):
        include_raw = self._include_raw(payload)
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
        return self._order_row(row, include_raw=include_raw)

    def list_orders(self, payload):
        include_raw = self._include_raw(payload)
        account_id = payload.get("account_id")
        if account_id:
            account_id = self._require_account(payload)
        rows = self.store.list_orders(account_id)
        return {
            "items": [
                self._order_row(row, include_raw=include_raw) for row in rows
            ],
            "count": len(rows),
        }

    def wait_orders(self, payload):
        client_order_ids = payload.get("client_order_ids")
        if not isinstance(client_order_ids, list) or not client_order_ids:
            raise BridgeError(
                "INVALID_ARGUMENT", "client_order_ids must be a non-empty list"
            )
        normalized_ids = []
        seen = set()
        for value in client_order_ids:
            client_order_id = str(value or "").strip()
            if not client_order_id:
                raise BridgeError(
                    "INVALID_ARGUMENT", "client_order_ids cannot contain blanks"
                )
            if client_order_id in seen:
                raise BridgeError(
                    "INVALID_ARGUMENT",
                    "duplicate client_order_id: %s" % client_order_id,
                )
            seen.add(client_order_id)
            normalized_ids.append(client_order_id)
        statuses = payload.get("statuses")
        if not isinstance(statuses, list) or not statuses:
            raise BridgeError(
                "INVALID_ARGUMENT", "statuses must be a non-empty list"
            )
        normalized_statuses = set()
        for value in statuses:
            status = str(value or "").strip().upper()
            if not status:
                raise BridgeError(
                    "INVALID_ARGUMENT", "statuses cannot contain blanks"
                )
            if status not in ORDER_STATUSES:
                raise BridgeError(
                    "INVALID_ARGUMENT", "unsupported order status: %s" % status
                )
            normalized_statuses.add(status)
        include_raw = self._include_raw(payload)
        try:
            timeout_seconds = float(payload.get("timeout_seconds"))
        except Exception:
            raise BridgeError(
                "INVALID_ARGUMENT", "timeout_seconds must be positive"
            )
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise BridgeError(
                "INVALID_ARGUMENT", "timeout_seconds must be positive"
            )
        deadline = time.monotonic() + timeout_seconds
        with self.order_state_condition:
            while True:
                if self.stopping:
                    raise BridgeError("CONNECTION_CLOSED", "QMT Bridge is stopping")
                rows = []
                for client_order_id in normalized_ids:
                    row = self.store.get_order(client_order_id)
                    if row is None:
                        raise BridgeError(
                            "ORDER_NOT_FOUND",
                            "order was not found",
                            {"client_order_id": client_order_id},
                        )
                    rows.append(row)
                items = [
                    self._order_row(row, include_raw=include_raw) for row in rows
                ]
                pending = [
                    item["client_order_id"]
                    for item in items
                    if item["order_status"] not in normalized_statuses
                ]
                result = {
                    "items": items,
                    "count": len(items),
                    "statuses": sorted(normalized_statuses),
                    "completed": not pending,
                    "pending_client_order_ids": pending,
                }
                if not pending:
                    return result
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BridgeError(
                        "WAIT_TIMEOUT",
                        "orders did not reach the requested statuses before timeout",
                        result,
                    )
                self.order_state_condition.wait(remaining)

    def cancel_order(self, payload, request_id, context_info):
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
        self._notify_order_state()
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
                        status = _derive_order_status(raw, row["status"])
                        self.store.update_order(
                            row["client_order_id"],
                            status=status,
                            qmt_order_id=qmt_order_id,
                            raw=raw,
                        )
                        self._notify_order_state()
                        updated = self.store.get_order(row["client_order_id"])
                        if updated is not None:
                            self._complete_broker_response(
                                row["client_order_id"], updated
                            )
                        break

    def _order_row(self, row, include_raw=False):
        order_kind = str(row["order_kind"] or "STOCK").upper()
        metadata = (
            json.loads(row["metadata_json"])
            if row["metadata_json"]
            else {}
        )
        business_types = {
            "STOCK": "CASH",
            "REVERSE_REPO": "REVERSE_REPO",
            "NEW_ISSUE_SUBSCRIPTION": "NEW_ISSUE_SUBSCRIPTION",
        }
        quantity_types = {
            "STOCK": "SHARES",
            "REVERSE_REPO": "REPO_UNITS",
            "NEW_ISSUE_SUBSCRIPTION": "SUBSCRIPTION_UNITS",
        }
        raw = json.loads(row["raw_json"]) if row["raw_json"] else None
        effective_qmt_order_id = row["qmt_order_id"]
        effective_status = row["status"]
        with self.order_event_cache_lock:
            cached_raw = self.order_event_cache.get(row["wire_order_tag"])
        if cached_raw:
            cached_qmt_order_id = str(
                cached_raw.get("m_strOrderSysID", "") or ""
            ).strip()
            if cached_qmt_order_id:
                raw = cached_raw
                effective_qmt_order_id = cached_qmt_order_id
                effective_status = _derive_order_status(raw, effective_status)

        trade_summary = self.store.trade_summary(row["client_order_id"])
        raw_filled_quantity = _raw_integer(raw, "m_nVolumeTraded")
        trade_filled_quantity = int(trade_summary["filled_quantity"] or 0)
        filled_quantity = max(raw_filled_quantity or 0, trade_filled_quantity)
        raw_remaining_quantity = _raw_integer(raw, "m_nVolumeTotal")
        if (
            raw_remaining_quantity is not None
            and raw_filled_quantity is not None
            and raw_filled_quantity >= trade_filled_quantity
        ):
            remaining_quantity = max(0, raw_remaining_quantity)
        else:
            remaining_quantity = max(0, int(row["quantity"]) - filled_quantity)
        average_filled_price = None
        filled_amount = None
        if filled_quantity > 0:
            raw_average = _raw_decimal_text(raw, "m_dTradedPrice")
            if raw_average is not None and decimal.Decimal(raw_average) > 0:
                average_filled_price = raw_average
            else:
                average_filled_price = trade_summary["average_filled_price"]
            raw_amount = _optional_money_text(
                raw.get("m_dTradeAmount") if raw else None
            )
            if raw_amount is not None and decimal.Decimal(raw_amount) > 0:
                filled_amount = raw_amount
            else:
                filled_amount = trade_summary["filled_amount"]

        result = {
            "client_order_id": row["client_order_id"],
            "request_id": row["request_id"],
            "account_id": row["account_id"],
            "account_type": "STOCK",
            "order_kind": order_kind,
            "business_type": business_types.get(order_kind),
            "quantity_type": quantity_types.get(order_kind),
            "instrument": row["instrument"],
            "side": row["side"],
            "quantity": row["quantity"],
            "price_type": row["price_type"],
            "limit_price": row["limit_price"],
            "remark": row["user_remark"],
            "qmt_remark": row["qmt_remark"],
            "qmt_order_id": effective_qmt_order_id,
            "order_status": effective_status,
            "filled_quantity": filled_quantity,
            "remaining_quantity": remaining_quantity,
            "average_filled_price": average_filled_price,
            "filled_amount": filled_amount,
            "trade_count": int(trade_summary["trade_count"] or 0),
            "reject_reason": _order_reject_reason(raw),
            "metadata": metadata,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
        if include_raw:
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
        install_root = os.path.dirname(os.path.dirname(CONFIG_PATH))
        config["db_path"] = os.path.join(install_root, "data", "bridge.db")
    return config


def init(ContextInfo):
    global _RUNTIME
    if _RUNTIME is not None:
        return
    runtime = None
    try:
        previous_runtime = getattr(builtins, _RUNTIME_SLOT, None)
        if previous_runtime is not None:
            previous_runtime.stop()
            delattr(builtins, _RUNTIME_SLOT)
        config = _load_config()
        runtime = BridgeRuntime(config)
        for account in runtime.accounts.values():
            ContextInfo.set_account(account["account_id"])
        runtime.start()
        _RUNTIME = runtime
        ContextInfo.run_time(
            "_qmt_adapter_tick",
            config.get("timer_period", "10nMilliSecond"),
            "2000-01-01 00:00:00",
            "SH",
        )
        setattr(builtins, _RUNTIME_SLOT, runtime)
        print("QMT Adapter bridge is ready: %s" % config.get("pipe_name"))
    except Exception as exc:
        if _RUNTIME is runtime:
            _RUNTIME = None
        if getattr(builtins, _RUNTIME_SLOT, None) is runtime:
            delattr(builtins, _RUNTIME_SLOT)
        cleanup_error = None
        if runtime is not None:
            try:
                runtime.stop()
            except Exception as stop_exc:
                cleanup_error = stop_exc
        print("QMT Adapter startup failed: %s: %s" % (type(exc).__name__, exc))
        print(traceback.format_exc())
        if cleanup_error is not None:
            print(
                "QMT Adapter startup cleanup failed: %s: %s"
                % (type(cleanup_error).__name__, cleanup_error)
            )


def _qmt_adapter_tick(ContextInfo):
    if _RUNTIME is not None:
        _RUNTIME.process_pending(ContextInfo)


def handlebar(ContextInfo):
    pass


def order_callback(ContextInfo, orderInfo):
    if _RUNTIME is not None:
        _RUNTIME.enqueue_order_event(orderInfo)


def deal_callback(ContextInfo, dealInfo):
    if _RUNTIME is not None:
        _RUNTIME.enqueue_trade_event(dealInfo)


def stop(ContextInfo):
    global _RUNTIME
    runtime = _RUNTIME
    if runtime is not None:
        runtime.stop()
        _RUNTIME = None
    if getattr(builtins, _RUNTIME_SLOT, None) is runtime:
        delattr(builtins, _RUNTIME_SLOT)
