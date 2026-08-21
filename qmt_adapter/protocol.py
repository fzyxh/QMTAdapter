import ctypes
from ctypes import wintypes
import json
import queue
import struct
import threading
import time

from .exceptions import ConnectionClosed, RequestTimeout


GENERIC_READ = 0x80000000
GENERIC_WRITE = 0x40000000
OPEN_EXISTING = 3
INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
ERROR_BROKEN_PIPE = 109
ERROR_SEM_TIMEOUT = 121
ERROR_PIPE_BUSY = 231
ERROR_NO_DATA = 232
ERROR_PIPE_NOT_CONNECTED = 233
ERROR_FILE_NOT_FOUND = 2
# UTF-8 JSON正文上限，不包含4字节帧长度前缀。
MAX_MESSAGE_SIZE = 5 * 1024 * 1024


class NamedPipeConnection:
    def __init__(self, pipe_name, max_message_size=MAX_MESSAGE_SIZE):
        self.pipe_name = pipe_name
        self.max_message_size = int(max_message_size)
        self._handle = None
        self._handle_lock = threading.Lock()
        self._request_lock = threading.Lock()
        self._events = queue.Queue()
        self._active_incoming = None
        self._closed = threading.Event()
        self._kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        self._declare_api()

    def _declare_api(self):
        k32 = self._kernel32
        k32.WaitNamedPipeW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD]
        k32.WaitNamedPipeW.restype = wintypes.BOOL
        k32.CreateFileW.argtypes = [
            wintypes.LPCWSTR,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.LPVOID,
            wintypes.DWORD,
            wintypes.DWORD,
            wintypes.HANDLE,
        ]
        k32.CreateFileW.restype = wintypes.HANDLE
        k32.ReadFile.argtypes = [
            wintypes.HANDLE,
            wintypes.LPVOID,
            wintypes.DWORD,
            ctypes.POINTER(wintypes.DWORD),
            wintypes.LPVOID,
        ]
        k32.ReadFile.restype = wintypes.BOOL
        k32.WriteFile.argtypes = k32.ReadFile.argtypes
        k32.WriteFile.restype = wintypes.BOOL
        k32.CloseHandle.argtypes = [wintypes.HANDLE]
        k32.CloseHandle.restype = wintypes.BOOL
        if hasattr(k32, "CancelIoEx"):
            k32.CancelIoEx.argtypes = [wintypes.HANDLE, wintypes.LPVOID]
            k32.CancelIoEx.restype = wintypes.BOOL

    def connect(self, timeout=5.0):
        if self._handle is not None:
            return self
        deadline = time.monotonic() + float(timeout)
        handle = INVALID_HANDLE_VALUE
        while handle == INVALID_HANDLE_VALUE:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RequestTimeout("named pipe connection timed out")
            wait_ms = max(1, min(50, int(remaining * 1000)))
            if not self._kernel32.WaitNamedPipeW(self.pipe_name, wait_ms):
                error_code = ctypes.get_last_error()
                if error_code in (
                    ERROR_FILE_NOT_FOUND,
                    ERROR_SEM_TIMEOUT,
                    ERROR_PIPE_BUSY,
                    ERROR_PIPE_NOT_CONNECTED,
                ):
                    time.sleep(min(0.005, remaining))
                    continue
                raise ctypes.WinError(error_code)
            handle = self._kernel32.CreateFileW(
                self.pipe_name,
                GENERIC_READ | GENERIC_WRITE,
                0,
                None,
                OPEN_EXISTING,
                0,
                None,
            )
            if handle == INVALID_HANDLE_VALUE:
                error_code = ctypes.get_last_error()
                if error_code in (
                    ERROR_FILE_NOT_FOUND,
                    ERROR_SEM_TIMEOUT,
                    ERROR_PIPE_BUSY,
                    ERROR_PIPE_NOT_CONNECTED,
                ):
                    time.sleep(min(0.005, remaining))
                    continue
                raise ctypes.WinError(error_code)
        self._closed.clear()
        with self._handle_lock:
            self._handle = handle
        return self

    def close(self):
        self._closed.set()
        with self._handle_lock:
            handle = self._handle
            incoming = self._active_incoming
            self._handle = None
            self._active_incoming = None
        if incoming is not None:
            incoming.put(ConnectionClosed())
        if handle is not None:
            if hasattr(self._kernel32, "CancelIoEx"):
                self._kernel32.CancelIoEx(handle, None)
            self._kernel32.CloseHandle(handle)

    @property
    def is_connected(self):
        return self._handle is not None and not self._closed.is_set()

    def request(self, message, timeout=10.0, correlation_field="request_id"):
        key = message.get(correlation_field) or message.get("message_id")
        deadline = time.monotonic() + float(timeout)
        with self._request_lock:
            incoming = queue.Queue()
            reader_thread = None
            try:
                # The pipe handle uses synchronous I/O, so finish the write before
                # starting a blocking reader.  Queue.get provides an event-driven
                # timeout without repeatedly polling PeekNamedPipe.
                self.send(message)
                handle = self._get_handle()
                reader_thread = threading.Thread(
                    target=self._read_response,
                    args=(handle, incoming),
                    name="qmt-adapter-pipe-reader",
                )
                reader_thread.daemon = True
                with self._handle_lock:
                    if self._handle != handle:
                        raise ConnectionClosed()
                    self._active_incoming = incoming
                reader_thread.start()
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise RequestTimeout(request_id=key)
                    try:
                        response = incoming.get(timeout=remaining)
                    except queue.Empty:
                        raise RequestTimeout(request_id=key)
                    if isinstance(response, BaseException):
                        if isinstance(response, (ConnectionClosed, OSError)):
                            raise response
                        raise ConnectionClosed(
                            "named pipe reader failed: %s" % response
                        )
                    response_key = response.get("request_id") or response.get("message_id")
                    if response_key != key:
                        raise ConnectionClosed(
                            "unexpected response id: expected %s, got %s"
                            % (key, response_key)
                        )
                    return response
            except (RequestTimeout, ConnectionClosed, OSError):
                # A timed-out request can still produce a late response.  The
                # stream can no longer be correlated safely, so force a fresh
                # handshake before the caller performs any follow-up query.
                self.close()
                raise
            finally:
                with self._handle_lock:
                    if self._active_incoming is incoming:
                        self._active_incoming = None

    def send(self, message):
        encoded = json.dumps(
            message, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        if len(encoded) > self.max_message_size:
            raise ValueError("message exceeds maximum size")
        frame = struct.pack(">I", len(encoded)) + encoded
        handle = self._get_handle()
        self._write_all(handle, frame)

    def get_event(self, timeout=None):
        return self._events.get(timeout=timeout)

    def _get_handle(self):
        with self._handle_lock:
            handle = self._handle
        if handle is None:
            raise ConnectionClosed()
        return handle

    def _read_response(self, handle, incoming):
        try:
            while True:
                message = self._read_message(handle)
                if message.get("type") == "event":
                    self._events.put(message)
                else:
                    incoming.put(message)
                    return
        except Exception as exc:
            incoming.put(exc)

    def _read_message(self, handle):
        header = self._read_exact(handle, 4)
        size = struct.unpack(">I", header)[0]
        if size <= 0 or size > self.max_message_size:
            raise ConnectionClosed("invalid named-pipe frame size: %s" % size)
        payload = self._read_exact(handle, size)
        return json.loads(payload.decode("utf-8"))

    def _read_exact(self, handle, size):
        chunks = []
        remaining = size
        while remaining:
            buffer = ctypes.create_string_buffer(remaining)
            received = wintypes.DWORD()
            ok = self._kernel32.ReadFile(
                handle, buffer, remaining, ctypes.byref(received), None
            )
            if not ok:
                raise ctypes.WinError(ctypes.get_last_error())
            if not received.value:
                raise ConnectionClosed("named pipe closed during read")
            chunks.append(buffer.raw[: received.value])
            remaining -= received.value
        return b"".join(chunks)

    def _write_all(self, handle, data):
        offset = 0
        while offset < len(data):
            part = data[offset:]
            buffer = ctypes.create_string_buffer(part)
            written = wintypes.DWORD()
            ok = self._kernel32.WriteFile(
                handle, buffer, len(part), ctypes.byref(written), None
            )
            if not ok:
                raise ctypes.WinError(ctypes.get_last_error())
            if not written.value:
                raise ConnectionClosed("named pipe closed during write")
            offset += written.value
