import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from types import TracebackType
from typing import Any, Callable, Dict, Optional, Type, TypeVar

from .client import QmtClient
from .config import ConfigPath
from .models import OrderReceipt, OrderRequest


T = TypeVar("T")


class AsyncQmtClient:
    """大 QMT Adapter 的 asyncio 客户端。

    所有公开方法都在一个工作线程和一个持久命名管道连接上严格串行执行，
    因此不会并行调用 QMT。异步接口的作用是避免阻塞调用方事件循环。

    Args:
        config_path: ``bridge_config.json`` 路径。为 ``None`` 时使用默认路径。
        client_id: 发送给 Bridge 的调用方标识。

    Note:
        构造对象不会自动连接。请调用 :meth:`connect`，或使用
        ``async with``。对象关闭后不能再次连接。
    """

    def __init__(
        self,
        config_path: ConfigPath = None,
        client_id: str = "async-qmt-adapter-client",
    ) -> None:
        self._client = QmtClient(config_path=config_path, client_id=client_id)
        self._executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="qmt-adapter-async"
        )
        self._operation_lock = asyncio.Lock()
        self._closed = False

    @property
    def hello(self) -> Optional[Dict[str, Any]]:
        """连接握手结果；连接前为 ``None``。"""
        return self._client.hello

    @property
    def is_connected(self) -> bool:
        """当前底层命名管道是否处于连接状态。"""
        return self._client.connection.is_connected

    async def connect(self, timeout: float = 5.0) -> "AsyncQmtClient":
        """异步连接 QMT Bridge，并返回当前客户端自身。

        Args:
            timeout: 等待命名管道和握手响应的最长秒数。
        """
        await self._call(self._client.connect, timeout=timeout)
        return self

    async def close(self) -> None:
        """异步关闭连接和内部工作线程；重复调用是安全的。"""
        if self._closed:
            return
        try:
            await self._call(self._client.close)
        finally:
            self._closed = True
            self._executor.shutdown(wait=True)

    async def __aenter__(self) -> "AsyncQmtClient":
        return await self.connect()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self.close()

    async def health(self, timeout: float = 5.0) -> Dict[str, Any]:
        """异步查询 Bridge 运行状态；返回值与 ``QmtClient.health`` 相同。"""
        return await self._call(self._client.health, timeout=timeout)

    async def get_account(
        self, account_id: str, timeout: float = 5.0
    ) -> Dict[str, Any]:
        """异步查询股票账户资金；参数和返回值同 ``QmtClient.get_account``。"""
        return await self._call(
            self._client.get_account, account_id, timeout=timeout
        )

    async def list_positions(
        self, account_id: str, timeout: float = 5.0
    ) -> Dict[str, Any]:
        """异步查询股票持仓；参数和返回值同 ``QmtClient.list_positions``。"""
        return await self._call(
            self._client.list_positions, account_id, timeout=timeout
        )

    async def place_order(
        self,
        order: OrderRequest,
        wait_for: str = "LOCAL_ACK",
        timeout: float = 10.0,
    ) -> OrderReceipt:
        """异步提交股票委托。

        参数、返回值和异常语义与 :meth:`QmtClient.place_order` 相同。多个
        协程同时调用时，请求会在本客户端内部排队并按顺序执行。
        """
        return await self._call(
            self._client.place_order,
            order,
            wait_for=wait_for,
            timeout=timeout,
        )

    async def get_order(
        self, client_order_id: str, timeout: float = 5.0
    ) -> Dict[str, Any]:
        """异步查询一笔适配器委托；返回值同 ``QmtClient.get_order``。"""
        return await self._call(
            self._client.get_order, client_order_id, timeout=timeout
        )

    async def list_orders(
        self, account_id: Optional[str] = None, timeout: float = 5.0
    ) -> Dict[str, Any]:
        """异步列出适配器委托；返回值同 ``QmtClient.list_orders``。"""
        return await self._call(
            self._client.list_orders, account_id=account_id, timeout=timeout
        )

    async def cancel_order(
        self,
        client_order_id: str,
        timeout: float = 10.0,
    ) -> Dict[str, Any]:
        """异步请求撤单；参数和返回值同 ``QmtClient.cancel_order``。"""
        return await self._call(
            self._client.cancel_order,
            client_order_id,
            timeout=timeout,
        )

    async def _call(
        self, function: Callable[..., T], *args: Any, **kwargs: Any
    ) -> T:
        if self._closed:
            raise RuntimeError("async QMT client is closed")
        async with self._operation_lock:
            loop = asyncio.get_running_loop()
            future = loop.run_in_executor(
                self._executor, partial(function, *args, **kwargs)
            )
            return await asyncio.shield(future)
