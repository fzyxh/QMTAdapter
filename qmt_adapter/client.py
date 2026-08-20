import time
import uuid
from types import TracebackType
from typing import Any, Dict, Iterable, List, Optional, Type

from .config import ConfigPath, load_config
from .exceptions import RemoteError, RequestTimeout, ValidationError
from .models import AlgoOrderReceipt, AlgoOrderRequest, OrderReceipt, OrderRequest
from .protocol import NamedPipeConnection
from .version import __version__


PROTOCOL_VERSION = 3


class QmtClient:
    """大 QMT Adapter 的同步客户端。

    客户端通过一个持久化的 Windows 命名管道连接与 QMT Bridge 通信，
    本模块不导入也不依赖任何 QMT 函数。

    Args:
        config_path: ``bridge_config.json`` 路径。为 ``None`` 时使用默认路径。
        client_id: 发送给 Bridge 的调用方标识，只用于连接识别和排查问题。

    Note:
        构造对象不会自动连接。请调用 :meth:`connect`，或使用 ``with``。
        同一个客户端可以连续执行多条命令，不需要每次重新连接。
    """

    def __init__(
        self,
        config_path: ConfigPath = None,
        client_id: str = "qmt-adapter-client",
    ) -> None:
        self.config = load_config(config_path)
        self.client_id = client_id
        self.connection = NamedPipeConnection(
            self.config["pipe_name"],
            int(self.config.get("max_message_size", 1024 * 1024)),
        )
        self.hello: Optional[Dict[str, Any]] = None

    def connect(self, timeout: float = 5.0) -> "QmtClient":
        """连接 QMT Bridge 并完成协议握手。

        Args:
            timeout: 等待命名管道和握手响应的最长秒数。

        Returns:
            当前客户端自身，因此可使用 ``QmtClient(...).connect()``。

        Raises:
            RequestTimeout: 在指定时间内无法完成连接。
            RemoteError: Bridge 拒绝鉴权或协议版本不匹配。
            OSError: Windows 命名管道连接失败。
        """
        self.connection.connect(timeout=timeout)
        message_id = str(uuid.uuid4())
        response = self.connection.request(
            {
                "v": PROTOCOL_VERSION,
                "type": "hello",
                "message_id": message_id,
                "client_id": self.client_id,
                "client_version": __version__,
                "auth_token": self.config["auth_token"],
            },
            timeout=timeout,
            correlation_field="message_id",
        )
        self._raise_for_error(response)
        self.hello = response["result"]
        if self.hello.get("protocol_version") != PROTOCOL_VERSION:
            self.close()
            raise RemoteError(
                "QMT Bridge protocol version does not match client",
                code="PROTOCOL_MISMATCH",
                data={
                    "client_protocol_version": PROTOCOL_VERSION,
                    "bridge_protocol_version": self.hello.get("protocol_version"),
                },
            )
        return self

    def close(self) -> None:
        """关闭当前命名管道连接；重复调用是安全的。"""
        self.connection.close()

    def __enter__(self) -> "QmtClient":
        return self.connect()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.close()

    def health(self, timeout: float = 5.0) -> Dict[str, Any]:
        """查询 QMT Bridge 的运行状态。

        Args:
            timeout: 等待响应的最长秒数。

        Returns:
            包含环境、连接状态、命令队列和 QMT 定时器统计的字典。
        """
        return self._request("system.health", {}, timeout=timeout)

    def get_account(
        self, account_id: str, timeout: float = 5.0
    ) -> Dict[str, Any]:
        """查询一个已配置股票账户的资金信息。

        Args:
            account_id: ``bridge_config.json`` 白名单中的资金账号。
            timeout: 等待响应的最长秒数。

        Returns:
            账户查询字典。标准字段包括 ``available_cash``，QMT 原始字段
            保存在每个项目的 ``raw`` 中。
        """
        return self._request(
            "account.get", {"account_id": str(account_id)}, timeout=timeout
        )

    def list_positions(
        self, account_id: str, timeout: float = 5.0
    ) -> Dict[str, Any]:
        """查询一个已配置股票账户的全部持仓。

        Args:
            account_id: ``bridge_config.json`` 白名单中的资金账号。
            timeout: 等待响应的最长秒数。

        Returns:
            持仓查询字典。标准字段包括 ``instrument`` 和
            ``total_quantity``，其余 QMT 字段保存在 ``raw`` 中。
        """
        return self._request(
            "position.list", {"account_id": str(account_id)}, timeout=timeout
        )

    def place_order(
        self,
        order: OrderRequest,
        wait_for: str = "LOCAL_ACK",
        timeout: float = 10.0,
    ) -> OrderReceipt:
        """提交一笔普通股票买入或卖出委托。

        Args:
            order: :class:`OrderRequest`。其 ``client_order_id`` 是逻辑委托
                唯一 ID；使用同一 ID 和相同参数重试不会再次调用 QMT 下单。
            wait_for: 返回时点，可选 ``LOCAL_ACK``、``QMT_CALLED`` 或
                ``BROKER_ID``。使用 ``BROKER_ID`` 时会等待 QMT 委托回报。
            timeout: 等待响应的最长秒数。

        Returns:
            :class:`OrderReceipt`。只有 ``wait_for="BROKER_ID"`` 正常返回时
            才保证 ``qmt_order_id`` 非空。

        Raises:
            ValidationError: ``order`` 类型或 ``wait_for`` 不合法。
            RequestTimeout: 等待响应或 QMT 委托 ID 超时。超时不表示委托
                一定没有提交，禁止未经查询直接重下。
            RemoteError: QMT Bridge 明确拒绝该命令。

        Note:
            同一 ``client_order_id`` 不能绑定不同委托参数，否则 Bridge 返回
            ``CLIENT_ORDER_ID_CONFLICT``。不确定状态返回
            ``COMMAND_UNCERTAIN``，客户端不会自动生成新 ID 重下。
        """
        if not isinstance(order, OrderRequest):
            raise ValidationError("order must be an OrderRequest")
        payload = order.to_payload()
        wait_for = str(wait_for).upper()
        if wait_for not in ("LOCAL_ACK", "QMT_CALLED", "BROKER_ID"):
            raise ValidationError(
                "wait_for must be LOCAL_ACK, QMT_CALLED or BROKER_ID"
            )
        result = self._request(
            "order.place",
            payload,
            timeout=timeout,
            wait_for=wait_for,
        )
        receipt = OrderReceipt.from_dict(result)
        if wait_for in ("LOCAL_ACK", "QMT_CALLED"):
            return receipt
        if not receipt.qmt_order_id:
            raise RequestTimeout(
                "QMT order id is still pending",
                request_id=receipt.request_id,
                data={"client_order_id": receipt.client_order_id},
            )
        return receipt

    def place_orders(
        self,
        orders: Iterable[OrderRequest],
        interval_ms: int,
        wait_for: str = "LOCAL_ACK",
        timeout: float = 10.0,
    ) -> List[OrderReceipt]:
        """按指定最小时间间隔串行提交多笔普通股票委托。

        本方法会先校验全部委托，再依次复用 :meth:`place_order` 提交。
        ``interval_ms`` 约束相邻两次单笔调用的开始时间；如果上一笔调用耗时
        已经超过该间隔，下一笔会在上一笔返回后立即开始。批量内不会并行下单。

        Args:
            orders: 按提交顺序排列的 :class:`OrderRequest` 可迭代对象。
            interval_ms: 相邻两笔单笔调用开始时间的最小间隔，单位毫秒；
                必须为非负整数。
            wait_for: 每笔委托的返回时点，含义同 :meth:`place_order`。
            timeout: 每笔委托各自的响应超时时间，单位秒，不是整批总超时。

        Returns:
            与输入顺序一致的 :class:`OrderReceipt` 列表。空输入返回空列表。

        Raises:
            ValidationError: 批量参数或任一委托不合法。此类校验会在第一笔
                提交前完成。
            RequestTimeout: 当前委托等待响应或 QMT 委托 ID 超时。
            RemoteError: QMT Bridge 明确拒绝当前委托。

        Note:
            运行期异常会立即停止后续提交；异常前已经成功返回的委托不会撤销。
            调用方仍可使用原始 ``OrderRequest.client_order_id`` 查询这些委托。
        """
        if not isinstance(interval_ms, int) or isinstance(interval_ms, bool):
            raise ValidationError("interval_ms must be a non-negative integer")
        if interval_ms < 0:
            raise ValidationError("interval_ms must be a non-negative integer")

        try:
            batch = list(orders)
        except TypeError:
            raise ValidationError("orders must be an iterable of OrderRequest")

        normalized_wait_for = str(wait_for).upper()
        if normalized_wait_for not in ("LOCAL_ACK", "QMT_CALLED", "BROKER_ID"):
            raise ValidationError(
                "wait_for must be LOCAL_ACK, QMT_CALLED or BROKER_ID"
            )
        client_order_ids = set()
        for order in batch:
            if not isinstance(order, OrderRequest):
                raise ValidationError("orders must contain only OrderRequest")
            payload = order.to_payload()
            client_order_id = payload["client_order_id"]
            if client_order_id in client_order_ids:
                raise ValidationError(
                    "duplicate client_order_id in orders: %s" % client_order_id
                )
            client_order_ids.add(client_order_id)

        receipts: List[OrderReceipt] = []
        interval_seconds = interval_ms / 1000.0
        previous_started_at: Optional[float] = None
        for order in batch:
            if previous_started_at is not None:
                remaining = interval_seconds - (
                    time.monotonic() - previous_started_at
                )
                if remaining > 0:
                    time.sleep(remaining)
            previous_started_at = time.monotonic()
            receipts.append(
                self.place_order(
                    order,
                    wait_for=normalized_wait_for,
                    timeout=timeout,
                )
            )
        return receipts

    def get_order(
        self, client_order_id: str, timeout: float = 5.0
    ) -> Dict[str, Any]:
        """按客户端委托 ID 查询适配器持久化的委托记录。

        Args:
            client_order_id: ``OrderReceipt.client_order_id``。
            timeout: 等待响应的最长秒数。

        Returns:
            委托字典，包含 ``qmt_order_id``、内部状态及最新 QMT 回调
            ``raw``。该函数不会重新提交委托。
        """
        return self._request(
            "order.get",
            {"client_order_id": str(client_order_id)},
            timeout=timeout,
        )

    def list_orders(
        self, account_id: Optional[str] = None, timeout: float = 5.0
    ) -> Dict[str, Any]:
        """列出适配器持久化的委托记录。

        Args:
            account_id: 可选资金账号；为 ``None`` 时返回所有已配置账号的
                适配器委托。
            timeout: 等待响应的最长秒数。

        Returns:
            ``{"items": [...], "count": N}`` 形式的字典。
        """
        payload = {}
        if account_id is not None:
            payload["account_id"] = str(account_id)
        return self._request("order.list", payload, timeout=timeout)

    def cancel_order(
        self,
        client_order_id: str,
        timeout: float = 10.0,
    ) -> Dict[str, Any]:
        """撤销一笔由本适配器提交的委托。

        Args:
            client_order_id: 要撤销的客户端委托 ID。
            timeout: 等待撤单请求响应的最长秒数。

        Returns:
            包含 ``cancel_requested``、``qmt_order_id`` 和
            ``order_status`` 的字典。``cancel_requested=True`` 只表示撤单
            请求已发出，不代表柜台最终撤单成功。
        """
        return self._request(
            "order.cancel",
            {"client_order_id": str(client_order_id)},
            timeout=timeout,
        )

    def preview_algo_order(
        self,
        order: AlgoOrderRequest,
        timeout: float = 10.0,
    ) -> Dict[str, Any]:
        """只生成并校验算法拆单计划，不下单也不写入父子委托表。

        Args:
            order: :class:`AlgoOrderRequest`。
            timeout: 等待 QMT 读取账户、持仓和五档行情的最长秒数。

        Returns:
            标准化五档行情、目标股数、预计金额和全部子单计划。

        Raises:
            ValidationError: ``order`` 类型或参数不合法。
            RemoteError: 行情不可用、资金/可用持仓不足，或算法尚未实现。
        """
        if not isinstance(order, AlgoOrderRequest):
            raise ValidationError("order must be an AlgoOrderRequest")
        return self._request(
            "algo_order.preview",
            order.to_payload(),
            timeout=timeout,
        )

    def place_algo_order(
        self,
        order: AlgoOrderRequest,
        timeout: float = 30.0,
    ) -> AlgoOrderReceipt:
        """提交一笔股票算法父委托。

        Bridge 会在 QMT 主线程内读取盘口、生成完整计划、检查数量守恒，
        将父单和全部子单计划持久化，然后按 ``child_interval_ms`` 非阻塞地
        逐笔调用 ``passorder``。本方法在 Bridge 接受计划后返回，不等待全部
        子单提交或成交。同一 ``algo_order_id`` 和相同参数重试不会重复创建父单。
        """
        if not isinstance(order, AlgoOrderRequest):
            raise ValidationError("order must be an AlgoOrderRequest")
        result = self._request(
            "algo_order.place",
            order.to_payload(),
            timeout=timeout,
        )
        return AlgoOrderReceipt.from_dict(result)

    def get_algo_order(
        self, algo_order_id: str, timeout: float = 5.0
    ) -> Dict[str, Any]:
        """按父委托 ID 查询算法状态及全部子单。"""
        return self._request(
            "algo_order.get",
            {"algo_order_id": str(algo_order_id)},
            timeout=timeout,
        )

    def list_algo_orders(
        self, account_id: Optional[str] = None, timeout: float = 5.0
    ) -> Dict[str, Any]:
        """列出算法父委托；可选按资金账号过滤。"""
        payload: Dict[str, Any] = {}
        if account_id is not None:
            payload["account_id"] = str(account_id)
        return self._request("algo_order.list", payload, timeout=timeout)

    def cancel_algo_order(
        self, algo_order_id: str, timeout: float = 10.0
    ) -> Dict[str, Any]:
        """停止算法并请求撤销该父单当前所有可撤子单。

        重复调用是允许的；返回当前父单和各子单的撤单请求结果。
        """
        return self._request(
            "algo_order.cancel",
            {"algo_order_id": str(algo_order_id)},
            timeout=timeout,
        )

    def _request(
        self,
        command: str,
        payload: Dict[str, Any],
        timeout: float = 10.0,
        request_id: Optional[str] = None,
        wait_for: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not self.connection.is_connected:
            raise RuntimeError("client is not connected")
        request_id = request_id or str(uuid.uuid4())
        message = {
            "v": PROTOCOL_VERSION,
            "type": "request",
            "message_id": str(uuid.uuid4()),
            "request_id": request_id,
            "command": command,
            "payload": payload,
        }
        if wait_for is not None:
            message["wait_for"] = str(wait_for).upper()
        response = self.connection.request(message, timeout=timeout)
        self._raise_for_error(response)
        return response.get("result")

    @staticmethod
    def _raise_for_error(response: Dict[str, Any]) -> None:
        if response.get("ok"):
            return
        error = response.get("error") or {}
        raise RemoteError(
            error.get("message", response.get("code", "QMT Bridge error")),
            code=response.get("code", "REMOTE_ERROR"),
            request_id=response.get("request_id"),
            data=error.get("data") or {},
        )
