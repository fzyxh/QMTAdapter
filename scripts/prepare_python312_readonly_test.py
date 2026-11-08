"""Prepare an isolated, guarded QMT-side migration test; never starts QMT."""

import argparse
import json
from pathlib import Path
import sys
import tempfile
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from qmt_adapter.deploy import deploy


TEST_WRAPPER = '''

# Test-only guards. The deployed production source above is unchanged.
_TEST_READ_COMMANDS = frozenset((
    "system.health", "account.get", "account.flow.list", "position.list", "quote.get", "quote.list",
    "sector.instruments.list", "instrument.detail.list", "market.history.bar.get",
    "market.history.daily.get", "new_issue.list", "new_issue.quota.get",
    "order.get", "order.list", "order.account.list", "trade.list", "algo_order.get", "algo_order.list",
))
_TEST_NATIVE_READ_APIS = frozenset((
    "get_trade_detail_data", "get_ipo_data", "get_new_purchase_limit",
))
_test_original_init = init
_test_original_load_config = _load_config


class _IndexOnlyQuoteContext(object):
    def __init__(self, context):
        self.context = context

    def __getattr__(self, name):
        return getattr(self.context, name)

    def subscribe_whole_quote(self, code_list, callback=None):
        indices = ("000001.SH", "399001.SZ", "899050.BJ")
        codes = [code for code in indices if code.rsplit(".", 1)[-1] in code_list]
        return self.context.subscribe_whole_quote(codes, callback=callback)


class _ReadOnlyTestRuntime(BridgeRuntime):
    def _dispatch(self, command, payload, request_id, context_info):
        if command not in _TEST_READ_COMMANDS:
            raise BridgeError("READONLY_TEST", "This migration test accepts read-only queries only")
        return super(_ReadOnlyTestRuntime, self)._dispatch(command, payload, request_id, context_info)

    def _qmt_function(self, name):
        if name not in _TEST_NATIVE_READ_APIS:
            raise BridgeError("READONLY_TEST", "Broker write functions are disabled in this test")
        return super(_ReadOnlyTestRuntime, self)._qmt_function(name)

    def hello_response(self, message, connection_id=None, channel="command"):
        response = super(_ReadOnlyTestRuntime, self).hello_response(message, connection_id, channel)
        if channel == "command":
            response["result"]["commands"] = [c for c in response["result"]["commands"] if c in _TEST_READ_COMMANDS]
        return response

    def _reconcile_quote_subscription(self, context_info, force=False):
        return super(_ReadOnlyTestRuntime, self)._reconcile_quote_subscription(
            _IndexOnlyQuoteContext(context_info), force=force)

    def health(self):
        result = super(_ReadOnlyTestRuntime, self).health()
        result["readonly_test"] = {"enabled": True, "isolated_database": True, "native_quote_source": "THREE_INDICES_ONLY"}
        return result


BridgeRuntime = _ReadOnlyTestRuntime


def _load_config():
    config = _test_original_load_config()
    if config.get("python312_readonly_test") is not True:
        raise RuntimeError("This loader requires its generated isolated test configuration")
    config["accounts"] = [{"account_id": TEST_ACCOUNT_ID.strip(), "account_type": "STOCK"}]
    return config


def init(ContextInfo):
    _require_qmt_host()
    previous = getattr(builtins, _RUNTIME_SLOT, None)
    if previous is not None and previous is not _RUNTIME:
        raise RuntimeError("Stop the existing QMTAdapter strategy before this isolated test; it was left running")
    if not TEST_ACCOUNT_ID.strip():
        raise RuntimeError("Fill TEST_ACCOUNT_ID in this loader on your computer, then run again")
    _test_original_init(ContextInfo)
    if _RUNTIME is not None:
        print("QMT312_READONLY_TEST_READY config=" + CONFIG_PATH)
'''


def prepare(root=None):
    target = Path(root) if root else Path(tempfile.gettempdir()) / (
        'qmtadapter-py312-readonly-' + uuid.uuid4().hex[:12]
    )
    if not target.is_absolute():
        raise ValueError('Test root must be absolute')
    str(target).encode('ascii')
    if target.exists():
        raise ValueError('Test root already exists; choose a new empty location')
    result = deploy(root=target, account_ids=['FILL_ACCOUNT_IN_TEST_LOADER'])
    config = json.loads(result['config_path'].read_text(encoding='ascii'))
    pipe_id = uuid.uuid4().hex
    config.update(pipe_name=r'\\.\pipe\qmt_py312_readonly_' + pipe_id,
                  quote_pipe_name=r'\\.\pipe\qmt_py312_readonly_quote_' + pipe_id,
                  python312_readonly_test=True)
    result['config_path'].write_text(json.dumps(config, ensure_ascii=True, indent=2) + '\n', encoding='ascii')
    loader = result['loader_path'].read_text(encoding='ascii')
    loader = loader.replace('# This file is copied once into a QMT Python strategy.',
                            '# Isolated read-only migration test. Fill the account locally.\nTEST_ACCOUNT_ID = ""', 1)
    result['loader_path'].write_text(loader + TEST_WRAPPER, encoding='ascii')
    compile(result['loader_path'].read_bytes(), str(result['loader_path']), 'exec')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', help='New absolute ASCII test directory; defaults to a unique TEMP directory')
    args = parser.parse_args()
    result = prepare(args.root)
    print(json.dumps({key: str(result[key]) for key in ('root', 'loader_path', 'config_path')}, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
