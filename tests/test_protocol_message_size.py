import json
import struct
import threading
import unittest
from unittest import mock

from qmt_adapter.protocol import MAX_MESSAGE_SIZE
from qmt_side import qmt_adapter_qmt as bridge


class ProtocolMessageSizeTests(unittest.TestCase):
    def test_default_limit_is_five_mib(self):
        self.assertEqual(MAX_MESSAGE_SIZE, 5 * 1024 * 1024)
        self.assertEqual(bridge.MAX_MESSAGE_SIZE, MAX_MESSAGE_SIZE)

    def test_oversized_response_becomes_structured_error(self):
        server = object.__new__(bridge.PipeServer)
        server.max_message_size = 512
        server.write_lock = threading.Lock()
        server._write_all = mock.Mock()
        response = {
            "v": bridge.PROTOCOL_VERSION,
            "type": "response",
            "request_id": "request-oversized-1",
            "ok": True,
            "code": "OK",
            "result": {"payload": "x" * 1000},
            "error": None,
            "server_time": "2026-08-21T00:00:00Z",
        }

        server._send_direct(123, response)

        server._write_all.assert_called_once()
        handle, frame = server._write_all.call_args.args
        self.assertEqual(handle, 123)
        encoded_size = struct.unpack(">I", frame[:4])[0]
        self.assertEqual(encoded_size, len(frame) - 4)
        message = json.loads(frame[4:].decode("utf-8"))
        self.assertEqual(message["request_id"], "request-oversized-1")
        self.assertFalse(message["ok"])
        self.assertEqual(message["code"], "RESPONSE_TOO_LARGE")
        self.assertIsNone(message["result"])
        self.assertEqual(
            message["error"]["data"]["max_message_size"], 512
        )
        self.assertGreater(message["error"]["data"]["encoded_size"], 512)


if __name__ == "__main__":
    unittest.main()
