import builtins
import platform
from pathlib import Path
import sqlite3
import struct
import sys
import tempfile
import tomllib
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from qmt_adapter.version import __version__
from qmt_side import qmt_adapter_qmt as bridge


class Python312MigrationTests(unittest.TestCase):
    def test_init_rejects_unsupported_hosts(self):
        for version, bits, implementation in (((3, 6, 8), 8, 'cpython'), ((3, 11, 9), 8, 'cpython'),
                                               ((3, 12, 10), 4, 'cpython'), ((3, 12, 0), 8, 'pypy')):
            fake_sys = SimpleNamespace(implementation=SimpleNamespace(name=implementation),
                                       version_info=version, version=str(version))
            fake_struct = SimpleNamespace(calcsize=lambda _: bits)
            with self.subTest(version=version, bits=bits * 8, implementation=implementation), \
                    patch.object(bridge, 'sys', fake_sys), patch.object(bridge, 'struct', fake_struct):
                with self.assertRaisesRegex(RuntimeError, '64-bit CPython 3.12'):
                    bridge.init(Mock())

    def test_init_rejects_before_stopping_previous_runtime(self):
        previous = Mock()
        with patch.object(bridge.sys, 'version_info', (3, 6, 8)), \
                patch.object(builtins, bridge._RUNTIME_SLOT, previous, create=True), \
                patch.object(bridge, '_RUNTIME', None), \
                patch.object(bridge, '_load_config') as config, \
                patch.object(bridge, 'BridgeRuntime') as runtime:
            with self.assertRaisesRegex(RuntimeError, '64-bit CPython 3.12'):
                bridge.init(Mock())
            previous.stop.assert_not_called()
            config.assert_not_called()
            runtime.assert_not_called()
            self.assertIs(getattr(builtins, bridge._RUNTIME_SLOT), previous)

    def test_runtime_identity_and_packaged_versions(self):
        identity = bridge._runtime_identity()
        self.assertEqual(identity, dict(adapter_version=__version__, protocol_version=9,
                                       python_implementation=platform.python_implementation(),
                                       python_version=platform.python_version(),
                                       pointer_bits=struct.calcsize('P') * 8,
                                       sqlite_version=sqlite3.sqlite_version))
        root = Path(__file__).resolve().parents[1]
        metadata = tomllib.loads((root / 'pyproject.toml').read_text(encoding='utf-8'))
        self.assertEqual(metadata['project']['version'], __version__)
        self.assertEqual(metadata['project']['requires-python'], '>=3.11')

    def test_store_preserves_legacy_commit_rollback_and_executescript(self):
        with tempfile.TemporaryDirectory() as temp:
            store = bridge.OrderStore(str(Path(temp) / 'isolated.db'))
            try:
                con = store.conn
                self.assertEqual(con.isolation_level, '')
                if sys.version_info >= (3, 12):
                    self.assertEqual(con.autocommit, sqlite3.LEGACY_TRANSACTION_CONTROL)
                self.assertEqual(con.execute('PRAGMA journal_mode').fetchone()[0], 'wal')
                self.assertEqual(con.execute('PRAGMA synchronous').fetchone()[0], 2)
                self.assertEqual(con.execute('PRAGMA busy_timeout').fetchone()[0], 5000)
                con.execute('CREATE TABLE probe (value INTEGER)')
                con.execute('INSERT INTO probe VALUES (1)')
                self.assertTrue(con.in_transaction)
                con.rollback()
                self.assertEqual(con.execute('SELECT COUNT(*) FROM probe').fetchone()[0], 0)
                con.execute('INSERT INTO probe VALUES (2)')
                con.commit()
                con.execute('INSERT INTO probe VALUES (3)')
                con.executescript('INSERT INTO probe VALUES (4);')
                self.assertFalse(con.in_transaction)
                con.rollback()
                self.assertEqual([r[0] for r in con.execute('SELECT value FROM probe')], [2, 3, 4])
            finally:
                store.close()


if __name__ == '__main__':
    unittest.main()
