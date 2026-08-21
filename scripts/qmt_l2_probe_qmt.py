# -*- coding: gbk -*-
"""Read-only Level-2 capability probe for the built-in QMT Python runtime.

Copy this whole file into a separate QMT Python strategy and start it.  The
probe subscribes to a small set of Level-2 streams, waits three seconds, prints
up to three latest raw records from each stream, and then unsubscribes.

It does not call account, order, cancel, or database APIs.
"""

from __future__ import print_function


PROBE_SYMBOL = "601919.SH"
RECORD_COUNT = 3
WAIT_TICKS = 3
L2_PERIODS = (
    "l2quote",
    "l2quoteaux",
    "l2order",
    "l2transaction",
    "l2transactioncount",
    "l2orderqueue",
)

_TICK_COUNT = 0
_FINISHED = False
_SUBSCRIPTIONS = []


def _safe_value(value, depth=0):
    if depth > 5:
        return repr(value)
    if value is None or isinstance(value, (bool, int, float, str)):
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
    item_method = getattr(value, "item", None)
    if callable(item_method):
        try:
            return _safe_value(item_method(), depth + 1)
        except Exception:
            pass
    return repr(value)


def _frame_summary(value):
    if value is None:
        return {"row_count": 0, "columns": [], "records": []}

    tail = value
    tail_method = getattr(value, "tail", None)
    if callable(tail_method):
        try:
            tail = tail_method(RECORD_COUNT)
        except Exception:
            tail = value

    columns = getattr(value, "columns", None)
    if columns is not None:
        try:
            columns = [str(item) for item in list(columns)]
        except Exception:
            columns = [repr(columns)]
    elif isinstance(value, dict):
        columns = sorted(str(key) for key in value.keys())
    else:
        columns = []

    records = []
    to_dict = getattr(tail, "to_dict", None)
    if callable(to_dict):
        try:
            indexed = to_dict("index")
            for index, row in indexed.items():
                records.append(
                    {
                        "index": _safe_value(index),
                        "data": _safe_value(row),
                    }
                )
        except Exception:
            try:
                records = _safe_value(to_dict())
            except Exception:
                records = [_safe_value(tail)]
    elif isinstance(tail, dict):
        records = [_safe_value(tail)]
    else:
        records = [_safe_value(tail)]

    try:
        row_count = len(value)
    except Exception:
        row_count = None
    return {
        "row_count": row_count,
        "columns": columns,
        "records": records,
    }


def _symbol_value(result):
    if not isinstance(result, dict):
        return None
    if PROBE_SYMBOL in result:
        return result.get(PROBE_SYMBOL)
    wanted = PROBE_SYMBOL.upper()
    for key, value in result.items():
        if str(key).upper() == wanted:
            return value
    return None


def _unsubscribe_all(ContextInfo):
    while _SUBSCRIPTIONS:
        period, subscription_id = _SUBSCRIPTIONS.pop()
        try:
            ContextInfo.unsubscribe_quote(subscription_id)
            print("unsubscribe %s: PASS id=%s" % (period, subscription_id))
        except Exception as exc:
            print(
                "unsubscribe %s: FAIL %s: %s"
                % (period, type(exc).__name__, exc)
            )


def _run_probe(ContextInfo):
    print("=" * 72)
    print("QMT LEVEL-2 CAPABILITY PROBE (READ ONLY)")
    print("symbol=%s record_count=%s" % (PROBE_SYMBOL, RECORD_COUNT))
    print("=" * 72)

    try:
        full_tick = ContextInfo.get_full_tick([PROBE_SYMBOL])
        print("baseline get_full_tick: PASS")
        print("baseline raw: %s" % _safe_value(full_tick))
    except Exception as exc:
        print(
            "baseline get_full_tick: FAIL %s: %s"
            % (type(exc).__name__, exc)
        )

    for period in L2_PERIODS:
        print("-" * 72)
        print("period=%s" % period)
        try:
            result = ContextInfo.get_market_data_ex(
                [],
                [PROBE_SYMBOL],
                period=period,
                start_time="",
                end_time="",
                count=RECORD_COUNT,
                dividend_type="none",
                fill_data=True,
                subscribe=False,
            )
            value = _symbol_value(result)
            summary = _frame_summary(value)
            status = "PASS" if summary["row_count"] else "EMPTY"
            print(
                "%s: %s rows=%s columns=%s"
                % (
                    period,
                    status,
                    summary["row_count"],
                    summary["columns"],
                )
            )
            for index, record in enumerate(summary["records"]):
                print("%s raw[%s]: %s" % (period, index, record))
        except Exception as exc:
            print("%s: FAIL %s: %s" % (period, type(exc).__name__, exc))

    _unsubscribe_all(ContextInfo)
    print("=" * 72)
    print("Probe finished. No account, order, cancel, or database API was called.")
    print("=" * 72)


def init(ContextInfo):
    global _TICK_COUNT, _FINISHED
    _TICK_COUNT = 0
    _FINISHED = False
    del _SUBSCRIPTIONS[:]

    print("QMT L2 probe is subscribing; data will be read after %s seconds." % WAIT_TICKS)
    for period in L2_PERIODS:
        try:
            subscription_id = ContextInfo.subscribe_quote(
                PROBE_SYMBOL,
                period=period,
                dividend_type="none",
                result_type="dict",
            )
            try:
                accepted = int(subscription_id) >= 0
            except Exception:
                accepted = False
            if accepted:
                _SUBSCRIPTIONS.append((period, subscription_id))
                print("subscribe %s: PASS id=%s" % (period, subscription_id))
            else:
                print(
                    "subscribe %s: REJECTED id=%s"
                    % (period, subscription_id)
                )
        except Exception as exc:
            print(
                "subscribe %s: FAIL %s: %s"
                % (period, type(exc).__name__, exc)
            )

    ContextInfo.run_time(
        "_qmt_l2_probe_tick",
        "1nSecond",
        "2000-01-01 00:00:00",
        "SH",
    )


def _qmt_l2_probe_tick(ContextInfo):
    global _TICK_COUNT, _FINISHED
    if _FINISHED:
        return
    _TICK_COUNT += 1
    if _TICK_COUNT < WAIT_TICKS:
        return
    _FINISHED = True
    _run_probe(ContextInfo)


def handlebar(ContextInfo):
    pass


def stop(ContextInfo):
    global _FINISHED
    _FINISHED = True
    _unsubscribe_all(ContextInfo)
