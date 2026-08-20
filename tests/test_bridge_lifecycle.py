import builtins
from contextlib import redirect_stdout
import io
import unittest
from unittest.mock import patch

from qmt_side import qmt_adapter_qmt as bridge


ACCOUNT_ID = "SIM-STOCK-INIT"


class FakeRuntime(object):
    instances = []

    def __init__(self, config):
        self.accounts = {
            ACCOUNT_ID: {"account_id": ACCOUNT_ID, "account_type": "STOCK"}
        }
        self.start_count = 0
        self.stop_count = 0
        self.__class__.instances.append(self)

    def start(self):
        self.start_count += 1

    def stop(self):
        self.stop_count += 1


class WorkingContext(object):
    def set_account(self, account_id):
        return None

    def run_time(self, *args):
        return None


class BridgeLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.original_runtime = bridge._RUNTIME
        self.original_slot = getattr(builtins, bridge._RUNTIME_SLOT, None)
        bridge._RUNTIME = None
        if hasattr(builtins, bridge._RUNTIME_SLOT):
            delattr(builtins, bridge._RUNTIME_SLOT)
        FakeRuntime.instances = []

    def tearDown(self):
        current = bridge._RUNTIME
        if current is not None and current is not self.original_runtime:
            current.stop()
        bridge._RUNTIME = self.original_runtime
        if hasattr(builtins, bridge._RUNTIME_SLOT):
            delattr(builtins, bridge._RUNTIME_SLOT)
        if self.original_slot is not None:
            setattr(builtins, bridge._RUNTIME_SLOT, self.original_slot)

    def _patched_init(self, context, runtime_class=FakeRuntime):
        with patch.object(bridge, "BridgeRuntime", runtime_class), patch.object(
            bridge,
            "_load_config",
            return_value={"pipe_name": "audit", "timer_period": "10nMilliSecond"},
        ), redirect_stdout(io.StringIO()):
            bridge.init(context)

    def test_set_account_failure_cleans_runtime_and_allows_retry(self):
        class FailingContext(WorkingContext):
            def set_account(self, account_id):
                raise RuntimeError("set_account failed")

        self._patched_init(FailingContext())

        failed_runtime = FakeRuntime.instances[0]
        self.assertIsNone(bridge._RUNTIME)
        self.assertEqual(failed_runtime.start_count, 0)
        self.assertEqual(failed_runtime.stop_count, 1)
        self.assertFalse(hasattr(builtins, bridge._RUNTIME_SLOT))

        self._patched_init(WorkingContext())
        self.assertIs(bridge._RUNTIME, FakeRuntime.instances[1])
        self.assertEqual(FakeRuntime.instances[1].start_count, 1)

    def test_timer_registration_failure_stops_started_runtime(self):
        class FailingContext(WorkingContext):
            def run_time(self, *args):
                raise RuntimeError("run_time failed")

        self._patched_init(FailingContext())

        failed_runtime = FakeRuntime.instances[0]
        self.assertIsNone(bridge._RUNTIME)
        self.assertEqual(failed_runtime.start_count, 1)
        self.assertEqual(failed_runtime.stop_count, 1)
        self.assertFalse(hasattr(builtins, bridge._RUNTIME_SLOT))

        self._patched_init(WorkingContext())
        self.assertIs(bridge._RUNTIME, FakeRuntime.instances[1])
        self.assertEqual(FakeRuntime.instances[1].start_count, 1)

    def test_pipe_start_failure_cleans_runtime_and_allows_retry(self):
        class StartFailingRuntime(FakeRuntime):
            def start(self):
                self.start_count += 1
                raise RuntimeError("pipe start failed")

        self._patched_init(WorkingContext(), runtime_class=StartFailingRuntime)

        failed_runtime = FakeRuntime.instances[0]
        self.assertIsNone(bridge._RUNTIME)
        self.assertEqual(failed_runtime.start_count, 1)
        self.assertEqual(failed_runtime.stop_count, 1)
        self.assertFalse(hasattr(builtins, bridge._RUNTIME_SLOT))

        self._patched_init(WorkingContext())
        self.assertIs(bridge._RUNTIME, FakeRuntime.instances[1])
        self.assertEqual(FakeRuntime.instances[1].start_count, 1)


if __name__ == "__main__":
    unittest.main()
