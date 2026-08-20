# -*- coding: gbk -*-
"""Read-only probe for account discovery in the installed QMT model runtime."""

from __future__ import print_function


_HAS_RUN = False


def _text(value):
    try:
        return repr(value)
    except Exception as exc:
        return "<repr failed: %s>" % exc


def _object_fields(value):
    result = {}
    for name in dir(value):
        if name.startswith("_"):
            continue
        try:
            item = getattr(value, name)
        except Exception as exc:
            result[name] = "<getattr failed: %s>" % exc
            continue
        if callable(item):
            continue
        result[name] = _text(item)
    return result


def init(ContextInfo):
    global _HAS_RUN
    if _HAS_RUN:
        return
    _HAS_RUN = True

    print("=" * 60)
    print("QMT ACCOUNT DISCOVERY PROBE (READ ONLY)")
    print("=" * 60)

    global_account_names = []
    for name in globals():
        if "account" in name.lower():
            global_account_names.append(name)
    print("global account-related names: %s" % sorted(global_account_names))

    context_values = {}
    for name in dir(ContextInfo):
        if "account" not in name.lower():
            continue
        try:
            value = getattr(ContextInfo, name)
            context_values[name] = "<callable>" if callable(value) else _text(value)
        except Exception as exc:
            context_values[name] = "<getattr failed: %s>" % exc
    print("ContextInfo account-related values: %s" % context_values)

    try:
        values = get_trade_detail_data("", "STOCK", "ACCOUNT")
        print("empty-account ACCOUNT query count: %s" % len(values or []))
        for index, value in enumerate(values or []):
            print("empty-account result[%s]: %s" % (index, _object_fields(value)))
    except Exception as exc:
        print("empty-account ACCOUNT query failed: %s: %s" % (type(exc).__name__, exc))

    print("This probe did not call set_account, passorder, cancel, or other write APIs.")
    print("=" * 60)


def handlebar(ContextInfo):
    pass
