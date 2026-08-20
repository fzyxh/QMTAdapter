from typing import Any, Dict, Optional


class QmtAdapterError(Exception):
    """外部适配器异常基类，提供 ``code``、``request_id`` 和 ``data``。"""

    def __init__(
        self,
        message: str,
        code: str = "ADAPTER_ERROR",
        request_id: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.request_id = request_id
        self.data = data or {}


class ConnectionClosed(QmtAdapterError):
    """命名管道连接已经关闭。"""

    def __init__(
        self, message: str = "QMT adapter connection is closed", **kwargs: Any
    ) -> None:
        super().__init__(message, code="CONNECTION_CLOSED", **kwargs)


class RequestTimeout(QmtAdapterError):
    """连接、命令响应或QMT委托ID等待超时。"""

    def __init__(
        self, message: str = "QMT adapter request timed out", **kwargs: Any
    ) -> None:
        super().__init__(message, code="WAIT_TIMEOUT", **kwargs)


class RemoteError(QmtAdapterError):
    """QMT Bridge 明确返回的远端错误。"""


class ValidationError(QmtAdapterError):
    """外部调用参数校验失败。"""

    def __init__(self, message: str, **kwargs: Any) -> None:
        super().__init__(message, code="INVALID_ARGUMENT", **kwargs)
