import builtins
from contextlib import redirect_stdout
import importlib.util
import io
import json
import os
from pathlib import Path
import runpy
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from tests import test_named_pipe_e2e as fixtures


ROOT = Path(__file__).resolve().parents[1]


def script(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'scripts' / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ReadOnlyMigrationToolTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.preparer = script('prepare_python312_readonly_test')
        self.result = self.preparer.prepare(Path(self.temp.name) / 'new-test')
        self.config = json.loads(self.result['config_path'].read_text())
        loaded = runpy.run_path(str(self.result['loader_path']))
        self.namespace = loaded['init'].__globals__

    def tearDown(self):
        self.temp.cleanup()

    def test_fresh_config_isolated_and_existing_directory_refused(self):
        self.assertTrue(self.result['loader_path'].read_bytes().isascii())
        self.assertTrue(self.config['python312_readonly_test'])
        self.assertIn('qmt_py312_readonly_', self.config['pipe_name'])
        self.assertNotEqual(self.config['pipe_name'], self.config['quote_pipe_name'])
        self.assertEqual(Path(self.config['db_path']).parent.parent, self.result['root'])
        self.assertFalse(Path(self.config['db_path']).exists())
        with self.assertRaisesRegex(ValueError, 'already exists'):
            self.preparer.prepare(self.result['root'])

    def test_guards_reject_mutations_and_native_writes(self):
        namespace = self.namespace
        runtime = namespace['BridgeRuntime'].__new__(namespace['BridgeRuntime'])
        for command in ('order.place', 'order.cancel', 'algo_order.place', 'algo_order.cancel', 'history.cleanup'):
            with self.subTest(command=command), self.assertRaises(namespace['BridgeError']) as error:
                runtime._dispatch(command, {}, 'test', None)
            self.assertEqual(error.exception.code, 'READONLY_TEST')
        for name in ('passorder', 'cancel', 'query_bank_transfer_stream'):
            with self.subTest(name=name), self.assertRaises(namespace['BridgeError']) as error:
                runtime._qmt_function(name)
            self.assertEqual(error.exception.code, 'READONLY_TEST')

    def test_existing_runtime_is_not_stopped(self):
        previous = Mock()
        namespace = self.namespace
        namespace['TEST_ACCOUNT_ID'] = fixtures.ACCOUNT_ID
        with patch.object(sys, 'version_info', (3, 12, 10)), \
                patch.object(builtins, namespace['_RUNTIME_SLOT'], previous, create=True), \
                patch.dict(namespace, _test_original_init=Mock()):
            with self.assertRaisesRegex(RuntimeError, 'Stop the existing'):
                namespace['init'](Mock())
            namespace['_test_original_init'].assert_not_called()
            previous.stop.assert_not_called()
        self.assertFalse(Path(self.config['db_path']).exists())

    def test_native_quote_subscription_is_limited_to_three_indices(self):
        context = Mock()
        limited = self.namespace['_IndexOnlyQuoteContext'](context)
        callback = Mock()
        limited.subscribe_whole_quote(['SH', 'BJ'], callback)
        context.subscribe_whole_quote.assert_called_once_with(['000001.SH', '899050.BJ'], callback=callback)

    @unittest.skipUnless(os.name == 'nt', 'Windows named pipes are required')
    def test_smoke_script_against_generated_guarded_bridge(self):
        smoke = script('python312_readonly_smoke')
        namespace = self.namespace
        fake = fixtures.FakeQmtApi()
        native_query = fake.get_trade_detail_data

        def query(account, account_type, kind, *args):
            values = native_query(account, account_type, kind, *args)
            if kind == 'ACCOUNT':
                for value in values:
                    value.m_strStatus = '\u767b\u5f55\u6210\u529f'
            return values

        for code in smoke.INDICES:
            fake.full_ticks[code] = dict(fake.full_ticks['600000.SH'])
        namespace['get_trade_detail_data'] = query
        namespace['get_ipo_data'] = fake.get_ipo_data
        namespace['get_new_purchase_limit'] = fake.get_new_purchase_limit
        namespace['TEST_ACCOUNT_ID'] = fixtures.ACCOUNT_ID
        config = namespace['_load_config']()
        runtime = namespace['BridgeRuntime'](config)
        namespace['_RUNTIME'] = runtime
        context = fixtures.FakeContext(fake)
        stop = threading.Event()
        errors = []

        def tick():
            try:
                while not stop.wait(0.005):
                    runtime.process_pending(context)
            except Exception as exc:
                errors.append(exc)

        runtime.start()
        worker = threading.Thread(target=tick)
        worker.start()
        try:
            with redirect_stdout(io.StringIO()):
                # Only the reported test-harness identity is substituted on 3.11;
                # genuine host identity is covered by cross-interpreter tests.
                identity = namespace['_runtime_identity']()
                identity['python_version'] = '3.12.10'
                with patch.dict(namespace, _runtime_identity=lambda: identity):
                    result = smoke.run(self.result['config_path'], Path(self.temp.name) / 'report.json')
            self.assertEqual(result['status'], 'PASS', result)
            self.assertEqual(fake.passorder_calls, [])
            self.assertEqual(fake.cancel_calls, [])
            self.assertEqual(errors, [])
        finally:
            stop.set()
            worker.join(5)
            self.assertFalse(worker.is_alive())
            runtime.stop()
