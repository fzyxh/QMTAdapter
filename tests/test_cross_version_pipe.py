import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

from tests.test_named_pipe_e2e import ACCOUNT_ID


def interpreter(version):
    configured = os.environ.get('QMT_TEST_PYTHON' + version.replace('.', ''))
    if configured:
        return configured
    if sys.version_info[:2] == tuple(map(int, version.split('.'))):
        return sys.executable
    result = subprocess.run(['py', '-' + version, '-c', 'import sys; print(sys.executable)'],
                            capture_output=True, text=True, timeout=10)
    if result.returncode:
        raise RuntimeError('Install Python %s or set QMT_TEST_PYTHON%s' % (version, version.replace('.', '')))
    return result.stdout.strip()


@unittest.skipUnless(os.name == 'nt', 'Windows named pipes are required')
class CrossVersionPipeTests(unittest.TestCase):
    def test_python311_client_to_python312_bridge(self):
        py311, py312 = interpreter('3.11'), interpreter('3.12')
        worker = str(Path(__file__).with_name('cross_version_worker.py'))
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = root / 'config.json'
            ready = root / 'ready'
            config.write_text(json.dumps({
                'version': 'stale-config-version',
                'pipe_name': r'\\.\pipe\qmt_cross_' + uuid.uuid4().hex,
                'auth_token': uuid.uuid4().hex,
                'db_path': str(root / 'bridge.db'),
                'accounts': [{'account_id': ACCOUNT_ID, 'account_type': 'STOCK'}],
                'reconcile_interval_seconds': 1e12,
            }))
            server = subprocess.Popen([py312, worker, 'server', str(config), str(ready)],
                                      stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                      text=True)
            try:
                deadline = time.monotonic() + 10
                while not ready.exists() and time.monotonic() < deadline and server.poll() is None:
                    time.sleep(0.02)
                self.assertTrue(ready.exists(), '3.12 bridge failed to start')
                client = subprocess.run([py311, worker, 'client', str(config)],
                                        capture_output=True, text=True, timeout=20)
                self.assertEqual(client.returncode, 0, client.stdout + client.stderr)
            finally:
                try:
                    output, errors = server.communicate('\n', timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()
                    output, errors = server.communicate()
                    self.fail('3.12 bridge failed to stop: ' + output + errors)
                self.assertEqual(server.returncode, 0, output + errors)
