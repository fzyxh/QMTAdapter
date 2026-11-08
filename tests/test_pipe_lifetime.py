import os
import threading
import time
import unittest
from unittest import mock

from qmt_adapter import QmtClient
from tests import test_named_pipe_e2e as pipe_fixture


@unittest.skipUnless(os.name == "nt", "Windows named pipes are required")
class PipeLifetimeTests(unittest.TestCase):
    # Reuse the isolated fake-QMT fixture without inheriting its test methods.
    setUp = pipe_fixture.NamedPipeEndToEndTests.setUp
    tearDown = pipe_fixture.NamedPipeEndToEndTests.tearDown

    def test_ten_restarts_release_threads_and_handles_with_eight_clients(self):
        import ctypes
        import gc
        from ctypes import wintypes
        k32 = ctypes.WinDLL('kernel32', use_last_error=True)
        k32.GetCurrentProcess.restype = wintypes.HANDLE
        k32.GetProcessHandleCount.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        k32.GetProcessHandleCount.restype = wintypes.BOOL
        def handles():
            count = wintypes.DWORD()
            self.assertTrue(k32.GetProcessHandleCount(k32.GetCurrentProcess(), ctypes.byref(count)))
            return count.value
        self.harness.stop()
        self.config['max_clients'] = 8
        self.config['max_quote_clients'] = 8
        gc.collect()
        baseline_handles = handles()
        baseline_threads = set(threading.enumerate())
        for iteration in range(10):
            with self.subTest(iteration=iteration):
                self.harness = pipe_fixture.BridgeHarness(self.config, self.fake_api)
                self.harness.start()
                clients = []
                try:
                    for _ in range(8):
                        client = QmtClient(config_path=self.config_path).connect()
                        clients.append(client)
                        client.subscribe_whole_quote(markets=['SH'], push_interval_ms=10)
                    self.assertEqual(clients[0].health()['active_client_count'], 8)
                    self.assertEqual(clients[0].health()['runtime']['protocol_version'], 9)
                finally:
                    for client in clients:
                        client.close()
                    self.harness.stop()
                    self.harness.stop()
                remaining = [t.name for t in threading.enumerate() if t not in baseline_threads
                             and ('qmt' in t.name.lower() or 'quote' in t.name.lower())]
                self.assertEqual(remaining, [])
                # Python locks/events may own OS handles while test objects live.
                clients.clear()
                del client
                gc.collect()
                self.assertLessEqual(handles(), baseline_handles + 2)

    def _wait_for_close(self, server, connection_id):
        deadline = time.monotonic() + 2
        with server.connection_condition:
            while connection_id in server.connections:
                remaining = deadline - time.monotonic()
                self.assertGreater(remaining, 0, "pipe handle was not released")
                server.connection_condition.wait(remaining)

    def _peer_disconnect_during_io(self, paused_worker):
        server = self.harness.runtime.quote_pipe
        blocked = threading.Event()
        armed = threading.Event()
        release = threading.Event()
        other_done = threading.Event()
        target_handle = [None]
        closed_handles = []
        invalid_handles = []
        original_peek = server._peer_is_connected
        original_send = server._send_direct
        original_close = server._close_raw_handle
        other_loop_name = (
            "_event_writer_loop"
            if paused_worker == "monitor"
            else "_connection_loop"
        )
        original_other_loop = getattr(server, other_loop_name)

        def pause(handle):
            target_handle[0] = handle
            blocked.set()
            if not release.wait(5):
                raise RuntimeError("test I/O gate timed out")

        def peek(handle):
            if paused_worker == "monitor" and armed.is_set() and not blocked.is_set():
                pause(handle)
            try:
                return original_peek(handle)
            except OSError as exc:
                invalid_handles.append(exc.winerror)
                raise

        def send(handle, message, write_lock=None):
            if (
                paused_worker == "writer"
                and message.get("type") == "event"
                and not blocked.is_set()
            ):
                pause(handle)
            try:
                return original_send(handle, message, write_lock)
            except OSError as exc:
                invalid_handles.append(exc.winerror)
                raise

        def close(handle):
            if handle == target_handle[0]:
                closed_handles.append(handle)
            return original_close(handle)

        def other_loop(state):
            try:
                original_other_loop(state)
            finally:
                other_done.set()

        server._peer_is_connected = peek
        server._send_direct = send
        server._close_raw_handle = close
        setattr(server, other_loop_name, other_loop)
        client = QmtClient(config_path=self.config_path).connect()
        try:
            client.subscribe_whole_quote(markets=["SH"], push_interval_ms=10)
            connection_id = client.quote_hello["connection_id"]
            with server.connection_condition:
                state = server.connections[connection_id]
            armed.set()
            if paused_worker == "writer":
                self.fake_api.emit_whole_quote(self.fake_api.full_ticks)
            self.assertTrue(blocked.wait(2))
            client.unsubscribe_whole_quote()
            if paused_worker == "monitor":
                self.fake_api.emit_whole_quote(self.fake_api.full_ticks)
            self.assertTrue(other_done.wait(2))

            # The other worker has exited, but this worker can still enter a
            # Win32 call. A closed flag alone cannot protect this interval.
            self.assertEqual(closed_handles, [])
            release.set()
            for worker in (state["thread"], state["event_thread"]):
                worker.join(2)
                self.assertFalse(worker.is_alive())
            self._wait_for_close(server, connection_id)
            self.assertEqual(closed_handles, [target_handle[0]])
            self.assertNotIn(6, invalid_handles)
            self.assertEqual(client.health()["status"], "OK")
            self.assertEqual(client.health()["health_error"], "")
            self.assertEqual(client.health()["last_connection_error"], "")
            self.assertEqual(client.get_account(pipe_fixture.ACCOUNT_ID)["count"], 1)

            server._peer_is_connected = original_peek
            server._send_direct = original_send
            client.subscribe_whole_quote(markets=["SH"], push_interval_ms=10)
            self.fake_api.emit_whole_quote(self.fake_api.full_ticks)
            self.assertEqual(client.get_quote_event(timeout=2)["count"], 2)
            self.assertEqual(client.health()["status"], "OK")
        finally:
            release.set()
            client.close()

    def test_monitor_keeps_handle_until_it_exits_after_writer_disconnect(self):
        self._peer_disconnect_during_io("monitor")

    def test_writer_keeps_handle_until_it_exits_after_monitor_disconnect(self):
        self._peer_disconnect_during_io("writer")

    def test_repeated_unsubscribe_during_quote_delivery_keeps_health_ok(self):
        server = self.harness.runtime.quote_pipe
        with QmtClient(config_path=self.config_path) as client:
            for attempt in range(20):
                with self.subTest(attempt=attempt):
                    client.subscribe_whole_quote(markets=["SH"], push_interval_ms=10)
                    with server.connection_condition:
                        state = server.connections[client.quote_hello["connection_id"]]
                    client.unsubscribe_whole_quote()
                    self.fake_api.emit_whole_quote(self.fake_api.full_ticks)
                    state["thread"].join(2)
                    state["event_thread"].join(2)
                    self._wait_for_close(server, state["connection_id"])
                    self.assertFalse(state["thread"].is_alive())
                    self.assertFalse(state["event_thread"].is_alive())
                    health = client.health()
                    self.assertEqual(health["status"], "OK")
                    self.assertEqual(health["last_connection_error"], "")

    def test_unexpected_connection_error_is_recorded_without_poisoning_health(self):
        server = self.harness.runtime.quote_pipe
        original_peek = server._peer_is_connected
        fault_enabled = threading.Event()

        def peek(handle):
            if fault_enabled.is_set():
                raise server._io_error("PeekNamedPipe", 6)
            return original_peek(handle)

        with QmtClient(config_path=self.config_path, client_id="error-isolation") as client:
            client.connect()
            with mock.patch.object(
                server, "_peer_is_connected", side_effect=peek
            ):
                client.subscribe_whole_quote(markets=["SH"], push_interval_ms=10)
                with server.connection_condition:
                    state = server.connections[client.quote_hello["connection_id"]]
                fault_enabled.set()
                state["thread"].join(2)
                state["event_thread"].join(2)
                self.assertFalse(state["thread"].is_alive())
                self.assertFalse(state["event_thread"].is_alive())

            health = client.health()
            self.assertEqual(health["status"], "OK")
            self.assertEqual(health["health_error"], "")
            diagnostic = health["last_connection_error"]
            self.assertIn("[WinError 6]", diagnostic)
            self.assertIn("channel=quote", diagnostic)
            self.assertIn("operation=PeekNamedPipe", diagnostic)
            self.assertIn("connection_id=%s" % state["connection_id"], diagnostic)
            self.assertIn("client_id=error-isolation", diagnostic)
            self.assertEqual(health["last_error"], diagnostic)

            # Reconnecting must not erase a real persistence/main-loop failure.
            self.harness.runtime.set_error("simulated persistence failure")
            client.unsubscribe_whole_quote()
            client.subscribe_whole_quote(markets=["SH"], push_interval_ms=10)
            self.fake_api.emit_whole_quote(self.fake_api.full_ticks)
            self.assertEqual(client.get_quote_event(timeout=2)["count"], 2)
            health = client.health()
            self.assertEqual(health["status"], "DEGRADED")
            self.assertEqual(health["health_error"], "simulated persistence failure")
            self.assertEqual(health["last_connection_error"], diagnostic)

    def test_stop_cancels_blocked_read_and_write_before_releasing_handles(self):
        runtime = self.harness.runtime
        server = runtime.quote_pipe
        writing = threading.Event()
        write_returned = threading.Event()
        releases_during_write = []
        original_write = server._write_all
        original_close = server._close_raw_handle
        target_handle = [None]

        def write(handle, data):
            if len(data) <= 65536:
                return original_write(handle, data)
            target_handle[0] = handle
            writing.set()
            try:
                return original_write(handle, data)
            finally:
                write_returned.set()

        def close(handle):
            if handle == target_handle[0] and not write_returned.is_set():
                releases_during_write.append(handle)
            return original_close(handle)

        server._write_all = write
        server._close_raw_handle = close
        client = QmtClient(config_path=self.config_path).connect()
        try:
            # Complete the subscribe exchange but deliberately do not start
            # the streaming reader: a large event will fill the pipe buffer.
            client._connect_quote(timeout=2)
            client._quote_request(
                "quote.stream.subscribe", {"markets": ["SH"]}, timeout=2
            )
            connection_id = client.quote_hello["connection_id"]
            server.queue_event(connection_id, {
                "v": 8, "type": "event", "event": "quote.batch",
                "result": {"padding": "x" * 250000},
            })
            self.assertTrue(writing.wait(2))
            self.assertFalse(write_returned.wait(0.05))
            with runtime.pipe.connection_condition:
                states = list(runtime.pipe.connections.values())
            with server.connection_condition:
                states.extend(server.connections.values())
            started = time.monotonic()
            self.harness.stop()
            self.assertLess(time.monotonic() - started, 5)
            self.assertTrue(write_returned.is_set())
            self.assertEqual(releases_during_write, [])
            self.assertEqual(runtime.pipe.connections, {})
            self.assertEqual(server.connections, {})
            self.assertEqual(runtime.health_error, "")
            self.assertEqual(runtime.last_connection_error, "")
            for state in states:
                for key in ("thread", "event_thread"):
                    if state[key] is not None:
                        self.assertFalse(state[key].is_alive())
        finally:
            client.close()

    def test_stop_disconnects_read_issued_after_cancellation(self):
        server = self.harness.runtime.pipe
        blocked = threading.Event()
        release = threading.Event()
        reading = threading.Event()
        read_returned = threading.Event()
        stopped = threading.Event()
        failures = []
        target_handle = [None]
        original_read = server._read_exact
        original_cancel = server.kernel32.CancelIoEx
        read_calls = [0]

        def read(handle, size):
            read_calls[0] += 1
            # The third read waits for the next command after the hello frame.
            if read_calls[0] != 3:
                return original_read(handle, size)
            target_handle[0] = handle
            blocked.set()
            if not release.wait(3):
                raise RuntimeError("test read gate timed out")
            reading.set()
            try:
                return original_read(handle, size)
            finally:
                read_returned.set()

        def cancel(handle, overlapped):
            result = original_cancel(handle, overlapped)
            if handle == target_handle[0] and not release.is_set():
                release.set()
                reading.wait(2)
                read_returned.wait(0.02)
            return result

        def stop():
            try:
                self.harness.stop()
            except Exception as exc:
                failures.append(exc)
            finally:
                stopped.set()

        server._read_exact = read
        client = QmtClient(config_path=self.config_path).connect()
        stopper = threading.Thread(target=stop)
        try:
            self.assertTrue(blocked.wait(2))
            with mock.patch.object(server.kernel32, "CancelIoEx", side_effect=cancel):
                stopper.start()
                self.assertTrue(stopped.wait(2), "late ReadFile blocked shutdown")
            self.assertTrue(read_returned.is_set())
            self.assertEqual(failures, [])
            self.assertEqual(server.connections, {})
        finally:
            release.set()
            # Also unblocks a broken implementation, so a failed assertion
            # cannot leave the isolated harness running after this test.
            client.close()
            if stopper.ident is not None:
                stopper.join(5)

    def test_stop_cancels_accept_started_after_initial_cancellation(self):
        server = self.harness.runtime.pipe
        blocked = threading.Event()
        release = threading.Event()
        stopped = threading.Event()
        failures = []
        original_connect = server.kernel32.ConnectNamedPipe
        original_cancel = server._cancel_synchronous_io

        def connect(handle, overlapped):
            if server.current_connection_id == 0:
                return original_connect(handle, overlapped)
            blocked.set()
            if not release.wait(3):
                raise RuntimeError("test accept gate timed out")
            return original_connect(handle, overlapped)

        def cancel(thread):
            result = original_cancel(thread)
            release.set()
            return result

        def stop():
            try:
                self.harness.stop()
            except Exception as exc:
                failures.append(exc)
            finally:
                stopped.set()

        client = QmtClient(config_path=self.config_path)
        stopper = threading.Thread(target=stop)
        with mock.patch.object(server.kernel32, "ConnectNamedPipe", side_effect=connect):
            try:
                client.connect()
                self.assertTrue(blocked.wait(2))
                with mock.patch.object(
                    server.kernel32, "CreateFileW", return_value=server.INVALID_HANDLE_VALUE
                ), mock.patch.object(server, "_cancel_synchronous_io", side_effect=cancel):
                    stopper.start()
                    self.assertTrue(stopped.wait(2), "late accept blocked shutdown")
                self.assertEqual(failures, [])
                self.assertIsNone(server.listener_handle)
                self.assertEqual(server.connections, {})
            finally:
                release.set()
                wake = server.kernel32.CreateFileW(
                    server.pipe_name, server.GENERIC_READ | server.GENERIC_WRITE,
                    0, None, server.OPEN_EXISTING, 0, None,
                )
                if wake != server.INVALID_HANDLE_VALUE:
                    server.kernel32.CloseHandle(wake)
                client.close()
                if stopper.ident is not None:
                    stopper.join(5)


if __name__ == "__main__":
    unittest.main()
