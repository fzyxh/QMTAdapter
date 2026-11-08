"""Subprocess helper; only FakeQmtApi and temporary named pipes are used."""
import json
import os
from pathlib import Path
import sys

# Also support the isolated python.org embeddable interpreter.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main():
    role, config_path = sys.argv[1:3]
    if role == 'server':
        from qmt_side import qmt_adapter_qmt as bridge
        from tests.test_named_pipe_e2e import BridgeHarness, FakeQmtApi
        bridge._require_qmt_host()
        harness = BridgeHarness(json.loads(Path(config_path).read_text()), FakeQmtApi())
        try:
            harness.start()
            Path(sys.argv[3]).write_text('ready')
            sys.stdin.readline()
        finally:
            harness.stop()
    else:
        assert sys.version_info[:2] == (3, 11), sys.version
        if os.environ.get('QMT_TEST_CLIENT_SDK_ROOT'):
            sys.path.insert(0, os.environ['QMT_TEST_CLIENT_SDK_ROOT'])
        from qmt_adapter import QmtClient, __version__
        if os.environ.get('QMT_TEST_CLIENT_SDK_VERSION'):
            assert __version__ == os.environ['QMT_TEST_CLIENT_SDK_VERSION']
        from tests.test_named_pipe_e2e import ACCOUNT_ID
        with QmtClient(config_path=config_path) as client:
            health = client.health()
            assert health['runtime']['python_version'].startswith('3.12.'), health
            assert health['runtime']['pointer_bits'] == 64
            assert health['runtime']['protocol_version'] == 9
            assert health['status'] == 'OK'
            assert client.get_account(ACCOUNT_ID)
            assert client.list_positions(ACCOUNT_ID)
            assert client.get_quote('600000.SH')
        print('3.11 client -> 3.12 fake bridge: OK')


if __name__ == '__main__':
    main()
