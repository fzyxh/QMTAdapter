import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from qmt_adapter import QmtClient, RemoteError


class ProtocolVersionTests(unittest.TestCase):
    def test_new_client_rejects_old_bridge_protocol(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "bridge_config.json"
            config_path.write_text(
                json.dumps(
                    {
                        "pipe_name": r"\\.\pipe\unused_protocol_test",
                        "auth_token": "test-token",
                        "environment": "SIMULATION",
                    }
                ),
                encoding="utf-8",
            )
            client = QmtClient(config_path=config_path)
            client.connection.connect = mock.Mock(return_value=client.connection)
            client.connection.request = mock.Mock(
                return_value={
                    "v": 1,
                    "type": "hello_ack",
                    "ok": True,
                    "code": "OK",
                    "result": {"protocol_version": 1},
                }
            )
            client.connection.close = mock.Mock()

            with self.assertRaises(RemoteError) as caught:
                client.connect()

            self.assertEqual(caught.exception.code, "PROTOCOL_MISMATCH")
            client.connection.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
