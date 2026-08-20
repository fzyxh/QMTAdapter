import ctypes
import os
import time
import unittest

from qmt_adapter import RequestTimeout
from qmt_adapter.protocol import ERROR_SEM_TIMEOUT, NamedPipeConnection


@unittest.skipUnless(os.name == "nt", "Windows named pipes are required")
class ProtocolConnectionTests(unittest.TestCase):
    def test_error_sem_timeout_is_retried_until_client_deadline(self):
        class FakeKernel32(object):
            def __init__(self):
                self.wait_count = 0

            def WaitNamedPipeW(self, pipe_name, wait_ms):
                self.wait_count += 1
                ctypes.set_last_error(ERROR_SEM_TIMEOUT)
                return False

        connection = NamedPipeConnection(r"\\.\pipe\qmt_adapter_sem_timeout_test")
        fake_kernel32 = FakeKernel32()
        connection._kernel32 = fake_kernel32
        started = time.monotonic()

        with self.assertRaises(RequestTimeout):
            connection.connect(timeout=0.06)

        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 0.05)
        self.assertGreater(fake_kernel32.wait_count, 1)


if __name__ == "__main__":
    unittest.main()
