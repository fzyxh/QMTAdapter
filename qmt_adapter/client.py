import time
import uuid
import math
import re
from types import TracebackType
from typing import Any, Dict, Iterable, List, Optional, Type

from .config import ConfigPath, load_config
from .exceptions import RemoteError, RequestTimeout, ValidationError
from .models import (
    AlgoOrderReceipt,
    AlgoOrderRequest,
    NewIssueSubscriptionRequest,
    OrderReceipt,
    OrderRequest,
    ReverseRepoRequest,
)
from .protocol import (
    ERROR_BROKEN_PIPE,
    ERROR_NO_DATA,
    ERROR_PIPE_NOT_CONNECTED,
    MAX_MESSAGE_SIZE,
    NamedPipeConnection,
    StreamingNamedPipeConnection,
)
from .version import __version__


PROTOCOL_VERSION = 7
ORDER_STATUSES = {
    "PENDING_QMT",
    "PENDING_BROKER_ID",
    "SUBMITTED",
    "PARTIALLY_FILLED",
    "CANCEL_PENDING",
    "FILLED",
    "CANCELED",
    "REJECTED",
    "UNKNOWN",
}
TERMINAL_ORDER_STATUSES = {"FILLED", "CANCELED", "REJECTED"}
INSTRUMENT_PATTERN = re.compile(r"^[0-9]{6}\.(SH|SZ|BJ)$")
BAR_PERIOD_ALIASES = {"1h": "60m"}
BAR_PERIODS = {
    "1m",
    "3m",
    "5m",
    "10m",
    "15m",
    "30m",
    "60m",
    "2h",
    "3h",
    "4h",
    "1d",
    "2d",
    "3d",
    "5d",
    "1w",
    "1mon",
    "1q",
    "1hy",
    "1y",
}
DAILY_ADJUSTMENTS = {
    "none",
    "front",
    "back",
    "front_ratio",
    "back_ratio",
}
QUOTE_MARKETS = {"SH", "SZ", "BJ"}
QUOTE_PUSH_MODES = {"DELTA", "SNAPSHOT"}
QUOTE_INSTRUMENT_SCOPES = {"ALL", "STOCK"}


def _validated_include_raw(value: bool) -> bool:
    if not isinstance(value, bool):
        raise ValidationError("include_raw must be a boolean")
    return value


def _normalized_instruments(instruments: Iterable[str]) -> List[str]:
    if isinstance(instruments, str):
        raise ValidationError("instruments must be an iterable of instrument codes")
    try:
        values = list(instruments)
    except TypeError:
        raise ValidationError("instruments must be an iterable of instrument codes")
    if not values:
        raise ValidationError("instruments must not be empty")
    result = []
    seen = set()
    for value in values:
        instrument = str(value or "").strip().upper()
        if not INSTRUMENT_PATTERN.match(instrument):
            raise ValidationError("invalid instrument code: %s" % instrument)
        if instrument in seen:
            raise ValidationError("duplicate instrument: %s" % instrument)
        seen.add(instrument)
        result.append(instrument)
    return result


def _validated_daily_date(value: Optional[str], name: str) -> str:
    normalized = str(value or "").strip()
    if normalized and not re.match(r"^[0-9]{8}$", normalized):
        raise ValidationError("%s must be empty or YYYYMMDD" % name)
    return normalized


def _normalized_bar_period(value: str) -> str:
    if not isinstance(value, str):
        raise ValidationError("period must be a string")
    normalized = value.strip().lower()
    normalized = BAR_PERIOD_ALIASES.get(normalized, normalized)
    if normalized not in BAR_PERIODS:
        raise ValidationError("unsupported bar period: %s" % value)
    return normalized


def _normalized_daily_adjustment(value: str) -> str:
    if not isinstance(value, str):
        raise ValidationError("adjustment must be a string")
    normalized = value.strip().lower()
    if normalized not in DAILY_ADJUSTMENTS:
        raise ValidationError("unsupported daily adjustment: %s" % value)
    return normalized


def _validated_bar_time(value: Optional[str], name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    if re.match(r"^[0-9]{8}$", normalized):
        date_format = "%Y%m%d"
    elif re.match(r"^[0-9]{14}$", normalized):
        date_format = "%Y%m%d%H%M%S"
    else:
        raise ValidationError("%s must be empty, YYYYMMDD or YYYYMMDDHHMMSS" % name)
    try:
        time.strptime(normalized, date_format)
    except ValueError:
        raise ValidationError("%s is not a valid date/time" % name)
    return normalized


def _bar_time_bound(value: str, is_end: bool) -> str:
    if len(value) == 8:
        return value + ("235959" if is_end else "000000")
    return value


def _normalized_wait_statuses(statuses: Optional[Iterable[str]]) -> List[str]:
    if statuses is None:
        return sorted(TERMINAL_ORDER_STATUSES)
    try:
        values = [statuses] if isinstance(statuses, str) else list(statuses)
    except TypeError:
        raise ValidationError("statuses must be an iterable of order status strings")
    if not values:
        raise ValidationError("statuses must not be empty")
    result = []
    seen = set()
    for value in values:
        status = str(value or "").strip().upper()
        if status not in ORDER_STATUSES:
            raise ValidationError("unsupported order status: %s" % status)
        if status not in seen:
            seen.add(status)
            result.append(status)
    return result


def _normalized_quote_markets(markets: Optional[Iterable[str]]) -> List[str]:
    if markets is None:
        return ["SH", "SZ", "BJ"]
    if isinstance(markets, str):
        raise ValidationError("markets must be an iterable of market codes")
    try:
        values = list(markets)
    except TypeError:
        raise ValidationError("markets must be an iterable of market codes")
    if not values:
        raise ValidationError("markets must not be empty")
    result = []
    seen = set()
    for value in values:
        market = str(value or "").strip().upper()
        if market not in QUOTE_MARKETS:
            raise ValidationError("unsupported quote market: %s" % market)
        if market in seen:
            raise ValidationError("duplicate quote market: %s" % market)
        seen.add(market)
        result.append(market)
    return result


def _normalized_quote_mode(mode: str) -> str:
    normalized = str(mode or "").strip().upper()
    if normalized not in QUOTE_PUSH_MODES:
        raise ValidationError("unsupported quote push mode: %s" % mode)
    return normalized


def _normalized_quote_instrument_scope(instrument_scope: str) -> str:
    normalized = str(instrument_scope or "").strip().upper()
    if normalized not in QUOTE_INSTRUMENT_SCOPES:
        raise ValidationError(
            "unsupported quote instrument scope: %s" % instrument_scope
        )
    return normalized


class QmtClient:
    """大 QMT Adapter 的同步客户端。

    客户端通过持久化 Windows 命名管道与 QMT Bridge 通信。本模块不导入也
    不依赖任何QMT函数；只有使用全推行情时才额外建立专用行情管道。

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
            int(self.config.get("max_message_size", MAX_MESSAGE_SIZE)),
        )
        quote_pipe_name = self.config.get("quote_pipe_name") or (
            str(self.config["pipe_name"]) + "_quote"
        )
        self.quote_connection = StreamingNamedPipeConnection(
            quote_pipe_name,
            int(self.config.get("max_message_size", MAX_MESSAGE_SIZE)),
            int(self.config.get("quote_event_queue_size", 256)),
        )
        self.hello: Optional[Dict[str, Any]] = None
        self.quote_hello: Optional[Dict[str, Any]] = None
        self._quote_subscription: Optional[Dict[str, Any]] = None

    def connect(self, timeout: float = 5.0) -> "QmtClient":
        """连接 QMT Bridge 并完成协议握手。

        Args:
            timeout: 等待命名管道和握手响应的最长秒数。

        Returns:
            当前客户端自身，因此可使用 ``QmtClient(...).connect()``。已经完成
            握手时重复调用会直接返回，不会再次发送 ``hello``。

        Raises:
            RequestTimeout: 在指定时间内无法完成连接。
            RemoteError: Bridge 拒绝鉴权或协议版本不匹配。
            OSError: Windows 命名管道连接失败。
        """
        if self.connection.is_connected and self.hello is not None:
            return self
        if self.connection.is_connected:
            self.connection.close()
        deadline = time.monotonic() + float(timeout)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RequestTimeout("named pipe connection timed out")
            self.connection.connect(timeout=remaining)
            message_id = str(uuid.uuid4())
            try:
                response = self.connection.request(
                    {
                        "v": PROTOCOL_VERSION,
                        "type": "hello",
                        "message_id": message_id,
                        "client_id": self.client_id,
                        "client_version": __version__,
                        "auth_token": self.config["auth_token"],
                    },
                    timeout=remaining,
                    correlation_field="message_id",
                )
                break
            except OSError as exc:
                if getattr(exc, "winerror", None) not in (
                    ERROR_BROKEN_PIPE,
                    ERROR_NO_DATA,
                    ERROR_PIPE_NOT_CONNECTED,
                ):
                    raise
                self.connection.close()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RequestTimeout("named pipe connection timed out")
                time.sleep(min(0.005, remaining))
        self._raise_for_error(response)
        self.hello = response["result"]
        if self.hello.get("protocol_version") != PROTOCOL_VERSION:
            bridge_protocol_version = self.hello.get("protocol_version")
            self.close()
            raise RemoteError(
                "QMT Bridge protocol version does not match client",
                code="PROTOCOL_MISMATCH",
                data={
                    "client_protocol_version": PROTOCOL_VERSION,
                    "bridge_protocol_version": bridge_protocol_version,
                },
            )
        return self

    def close(self) -> None:
        """关闭当前命名管道连接；重复调用是安全的。"""
        self.quote_connection.close()
        self.quote_hello = None
        self._quote_subscription = None
        self.connection.close()
        self.hello = None

    def __enter__(self) -> "QmtClient":
        """进入上下文时连接Bridge并返回当前客户端。"""
        return self.connect()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """离开上下文时关闭命名管道连接。"""
        self.close()

    def health(self, timeout: float = 5.0) -> Dict[str, Any]:
        """查询 QMT Bridge 的运行状态。

        Args:
            timeout: 等待响应的最长秒数。

        Returns:
            包含连接状态、命令队列和 QMT 定时器统计的字典。
        """
        return self._request("system.health", {}, timeout=timeout)

    def get_account(
        self,
        account_id: str,
        timeout: float = 5.0,
        include_raw: bool = False,
    ) -> Dict[str, Any]:
        """查询一个已配置股票账户的资金信息。

        Args:
            account_id: ``bridge_config.json`` 白名单中的资金账号。
            timeout: 等待响应的最长秒数。
            include_raw: 是否附带QMT原始账户字段；默认关闭。

        Returns:
            账户查询字典。每项包含总资产、可用资金、股票市值、可取资金、
            冻结资金和持仓总盈亏。标准金额固定输出三位小数字符串；仅当
            ``include_raw=True`` 时包含 ``raw``。
        """
        include_raw = _validated_include_raw(include_raw)
        return self._request(
            "account.get",
            {
                "account_id": str(account_id),
                "include_raw": include_raw,
            },
            timeout=timeout,
        )

    def list_positions(
        self,
        account_id: str,
        timeout: float = 5.0,
        include_raw: bool = False,
    ) -> Dict[str, Any]:
        """查询一个已配置股票账户的全部持仓。

        Args:
            account_id: ``bridge_config.json`` 白名单中的资金账号。
            timeout: 等待响应的最长秒数。
            include_raw: 是否为每项附带QMT原始持仓字段；默认关闭。

        Returns:
            持仓查询字典。每项的 ``total_quantity`` 是持有股数，
            ``available_quantity`` 是当前可用股数，``frozen_quantity`` 是
            冻结股数；另含证券代码、证券名称、成本价、当前价、市值和持仓
            盈亏。仅当 ``include_raw=True`` 时包含 ``raw``。
        """
        include_raw = _validated_include_raw(include_raw)
        return self._request(
            "position.list",
            {
                "account_id": str(account_id),
                "include_raw": include_raw,
            },
            timeout=timeout,
        )

    def get_quote(
        self,
        instrument: str,
        timeout: float = 5.0,
        include_raw: bool = False,
    ) -> Dict[str, Any]:
        """查询一只证券的最新价格信息。

        Args:
            instrument: 带交易所后缀的证券代码，例如 ``601919.SH``。
            timeout: 等待大QMT返回行情的最长秒数。
            include_raw: 是否附带 ``get_full_tick`` 与
                ``get_instrument_detail`` 的QMT原始返回；默认关闭。

        Returns:
            单只证券的标准化行情字典。仅当 ``include_raw=True`` 时包含
            ``raw``。
        """
        items = _normalized_instruments([instrument])
        include_raw = _validated_include_raw(include_raw)
        return self._request(
            "quote.get",
            {"instrument": items[0], "include_raw": include_raw},
            timeout=timeout,
        )

    def get_quotes(
        self,
        instruments: Iterable[str],
        timeout: float = 5.0,
        include_raw: bool = False,
    ) -> Dict[str, Any]:
        """批量查询多只证券的最新价格信息。

        大QMT行情在一次 ``get_full_tick`` 调用中批量读取，返回顺序与
        ``instruments`` 一致。

        Args:
            instruments: 不重复的证券代码集合，代码必须带 ``.SH``、
                ``.SZ`` 或 ``.BJ`` 后缀。
            timeout: 等待大QMT返回行情的最长秒数。
            include_raw: 是否为每只证券附带QMT原始行情和静态信息。

        Returns:
            包含 ``items``、``count``、``errors``、``error_count`` 和
            ``as_of`` 的字典。大QMT未返回行情的证券只进入 ``errors``，
            不影响同批其他证券的正常结果。
        """
        normalized = _normalized_instruments(instruments)
        include_raw = _validated_include_raw(include_raw)
        return self._request(
            "quote.list",
            {"instruments": normalized, "include_raw": include_raw},
            timeout=timeout,
        )

    def subscribe_whole_quote(
        self,
        markets: Optional[Iterable[str]] = None,
        mode: str = "DELTA",
        instrument_scope: str = "ALL",
        push_interval_ms: int = 50,
        chunk_size: Optional[int] = None,
        include_raw: bool = False,
        timeout: float = 10.0,
    ) -> Dict[str, Any]:
        """通过专用行情管道订阅大QMT全推行情。

        Args:
            markets: 市场代码集合，默认订阅 ``SH``、``SZ``、``BJ``。
            mode: ``DELTA`` 只发送周期内发生更新的证券；``SNAPSHOT``
                每个周期发送当前缓存的全部证券快照。
            instrument_scope: ``ALL`` 接收交易所全部二级市场标的；
                ``STOCK`` 只接收大QMT“沪深京A股”板块中的股票。
            push_interval_ms: 推送聚合周期，范围为10至60000毫秒。
            chunk_size: 单个事件最多包含的证券数；``None`` 使用服务端
                默认值，``0`` 表示不按证券数量分片，但仍受5 MiB消息上限约束。
            include_raw: 是否在每个标准行情项中附带QMT原始全推字段。
            timeout: 连接专用行情管道并完成订阅的最长秒数。

        Returns:
            订阅信息，包含订阅ID、市场、模式、周期和行情管道名称。

        Note:
            每个客户端连接维护一个全推订阅；再次调用会替换原订阅。
            行情数据由 :meth:`get_quote_event` 持续读取，交易和普通查询仍
            使用原命名管道，二者互不占用对方的响应流。
        """
        normalized_markets = _normalized_quote_markets(markets)
        normalized_mode = _normalized_quote_mode(mode)
        normalized_instrument_scope = _normalized_quote_instrument_scope(
            instrument_scope
        )
        if isinstance(push_interval_ms, bool) or not isinstance(
            push_interval_ms, int
        ):
            raise ValidationError("push_interval_ms must be an integer")
        if push_interval_ms < 10 or push_interval_ms > 60000:
            raise ValidationError(
                "push_interval_ms must be between 10 and 60000"
            )
        if chunk_size is not None:
            if isinstance(chunk_size, bool) or not isinstance(chunk_size, int):
                raise ValidationError("chunk_size must be an integer or None")
            if chunk_size < 0 or chunk_size > 100000:
                raise ValidationError(
                    "chunk_size must be between 0 and 100000"
                )
        include_raw = _validated_include_raw(include_raw)
        if self.quote_connection.is_streaming:
            self.quote_connection.close()
            self.quote_hello = None
            self._quote_subscription = None
        self._connect_quote(timeout=timeout)
        result = self._quote_request(
            "quote.stream.subscribe",
            {
                "markets": normalized_markets,
                "mode": normalized_mode,
                "instrument_scope": normalized_instrument_scope,
                "push_interval_ms": push_interval_ms,
                "chunk_size": chunk_size,
                "include_raw": include_raw,
            },
            timeout=timeout,
        )
        self._quote_subscription = dict(result)
        try:
            self.quote_connection.start_reader()
        except Exception:
            self.quote_connection.close()
            self.quote_hello = None
            self._quote_subscription = None
            raise
        return result

    def get_quote_event(self, timeout: Optional[float] = None) -> Dict[str, Any]:
        """等待并返回下一批全推行情。

        返回字典包含 ``subscription_id``、``sequence``、``batch_id``、
        ``chunk_index``、``chunk_count``、``mode``、``push_time``、精简的
        ``items`` 和丢弃计数。每个行情项包含证券代码、北京时间、最新
        价量额以及按档位对齐的买卖价格和数量数组。
        ``SNAPSHOT`` 的一个周期可能拆成多批，可按 ``batch_id`` 和分片字段
        重新组合。``timeout=None`` 表示一直等待。
        """
        if not self.quote_connection.is_streaming or self.quote_hello is None:
            raise RuntimeError("quote stream is not connected")
        event = self.quote_connection.get_event(timeout=timeout)
        if event.get("event") == "quote.error":
            error = event.get("result") or {}
            raise RemoteError(
                error.get("message", "QMT quote stream error"),
                code=error.get("code", "QUOTE_STREAM_ERROR"),
                data=error,
            )
        if event.get("event") != "quote.batch":
            raise RemoteError(
                "unexpected quote event: %s" % event.get("event"),
                code="PROTOCOL_ERROR",
                data={"event": event},
            )
        return event.get("result") or {}

    def unsubscribe_whole_quote(self) -> Dict[str, Any]:
        """取消当前客户端的全推订阅，并关闭专用行情管道。"""
        if not self.quote_connection.is_connected or self.quote_hello is None:
            return {"subscribed": False}
        subscription_id = (
            (self._quote_subscription or {}).get("subscription_id")
        )
        self.quote_connection.close()
        self.quote_hello = None
        self._quote_subscription = None
        return {
            "subscribed": False,
            "subscription_id": subscription_id,
        }

    def list_sector_instruments(
        self,
        sector_name: str,
        timeout: float = 10.0,
    ) -> Dict[str, Any]:
        """查询大QMT板块中的证券代码。

        Args:
            sector_name: 大QMT客户端中的板块名称，例如 ``沪深A股``。
            timeout: 等待大QMT返回板块成分的最长秒数。

        Returns:
            ``{"sector_name": ..., "items": [...], "count": N}``。证券代码
            保留大QMT返回顺序并去重。
        """
        normalized = str(sector_name or "").strip()
        if not normalized:
            raise ValidationError("sector_name must not be empty")
        if len(normalized) > 128:
            raise ValidationError("sector_name must contain at most 128 characters")
        return self._request(
            "sector.instruments.list",
            {"sector_name": normalized},
            timeout=timeout,
        )

    def get_instrument_details(
        self,
        instruments: Iterable[str],
        timeout: float = 10.0,
        include_raw: bool = False,
    ) -> Dict[str, Any]:
        """批量查询沪深北证券名称、股本和上市日期。

        返回项目包含 ``instrument_name``、``float_shares``、
        ``total_shares`` 和 ``listing_date``；设置 ``include_raw=True`` 时还会
        返回大QMT ``get_instrument_detail`` 的完整原始字典。名称查询适用于
        大QMT能够识别的股票、ETF、可转债和逆回购等标的；无法识别的代码对应
        详情字段为 ``None``。
        """
        normalized = _normalized_instruments(instruments)
        include_raw = _validated_include_raw(include_raw)
        return self._request(
            "instrument.detail.list",
            {"instruments": normalized, "include_raw": include_raw},
            timeout=timeout,
        )

    def get_daily_history(
        self,
        instruments: Iterable[str],
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        count: int = -1,
        fill_data: bool = False,
        subscribe: bool = False,
        timeout: float = 30.0,
        include_raw: bool = False,
        adjustment: str = "none",
    ) -> Dict[str, Any]:
        """从大QMT模型上下文读取一批证券的历史日线。

        ``subscribe=False`` 时只读取大QMT本地已有历史数据；设置为 ``True``
        时由大QMT按自身规则订阅并尝试补充请求数据，但不保证补齐任意长区间。
        为避免超过命名管道单帧上限，长时间范围应由调用方拆成较小证券批次。

        返回 ``items`` 中每项固定包含全部标准字段，使用紧凑列式结构：
        ``timestamps`` 是索引时间，``data`` 按字段保存等长数组。价格和成交额
        固定输出三位小数字符串，数量与状态输出整数。``adjustment_factor``
        是截至对应交易日的完整历史累计复权因子，不随查询起点变化，最多输出
        六位小数。``adjustment`` 默认是 ``none``，也可指定 ``front``、
        ``back``、``front_ratio`` 或 ``back_ratio``；它只改变标准价格字段，
        不改变累计复权因子的定义。``include_raw=True`` 时额外返回QMT原始列。
        """
        normalized_start = _validated_daily_date(start_time, "start_time")
        normalized_end = _validated_daily_date(end_time, "end_time")
        normalized_adjustment = _normalized_daily_adjustment(adjustment)
        return self._request_bar_history(
            instruments,
            period="1d",
            start_time=normalized_start,
            end_time=normalized_end,
            count=count,
            fill_data=fill_data,
            subscribe=subscribe,
            timeout=timeout,
            include_raw=include_raw,
            adjustment=normalized_adjustment,
        )

    def get_bar_history(
        self,
        instruments: Iterable[str],
        period: str,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        count: int = -1,
        fill_data: bool = False,
        subscribe: bool = False,
        timeout: float = 30.0,
        include_raw: bool = False,
    ) -> Dict[str, Any]:
        """从大QMT模型上下文批量读取标准化历史K线。

        Args:
            instruments: 证券代码集合，例如 ``["932000.SH"]``。
            period: K线周期。支持分钟、小时、日、周、月、季、半年和年等
                大QMT合成周期；``1h`` 会规范为 ``60m``。
            start_time: 起始时间，格式为 ``YYYYMMDD`` 或
                ``YYYYMMDDHHMMSS``，空值表示不限制。
            end_time: 结束时间，格式同 ``start_time``。
            count: 返回条数；``-1`` 表示时间范围内全部数据。
            fill_data: 是否让大QMT填充缺失K线。
            subscribe: 是否让大QMT订阅该行情；``False`` 只读取本地数据。
            timeout: 请求超时秒数。
            include_raw: 是否附带大QMT原始列和原始时间索引。

        Returns:
            紧凑列式结构。每个 ``item`` 包含 ``timestamps``、``data`` 和
            ``count``。所有周期的价格均为不复权价格；分钟、小时、周、月等
            周期返回固定通用字段，``1d`` 额外返回最多六位的完整历史累计
            复权因子。价格和成交额保留三位小数。
        """
        return self._request_bar_history(
            instruments,
            period=period,
            start_time=start_time,
            end_time=end_time,
            count=count,
            fill_data=fill_data,
            subscribe=subscribe,
            timeout=timeout,
            include_raw=include_raw,
            adjustment="none",
        )

    def _request_bar_history(
        self,
        instruments: Iterable[str],
        period: str,
        start_time: Optional[str],
        end_time: Optional[str],
        count: int,
        fill_data: bool,
        subscribe: bool,
        timeout: float,
        include_raw: bool,
        adjustment: str,
    ) -> Dict[str, Any]:
        normalized_instruments = _normalized_instruments(instruments)
        normalized_period = _normalized_bar_period(period)
        normalized_adjustment = _normalized_daily_adjustment(adjustment)
        if normalized_period != "1d" and normalized_adjustment != "none":
            raise ValidationError("adjustment is only supported for period 1d")
        normalized_start = _validated_bar_time(start_time, "start_time")
        normalized_end = _validated_bar_time(end_time, "end_time")
        if (
            normalized_start
            and normalized_end
            and _bar_time_bound(normalized_start, False)
            > _bar_time_bound(normalized_end, True)
        ):
            raise ValidationError("start_time must not be later than end_time")
        if isinstance(count, bool) or not isinstance(count, int):
            raise ValidationError("count must be an integer")
        if count == 0 or count < -1:
            raise ValidationError("count must be -1 or a positive integer")
        if not isinstance(fill_data, bool):
            raise ValidationError("fill_data must be a boolean")
        if not isinstance(subscribe, bool):
            raise ValidationError("subscribe must be a boolean")
        include_raw = _validated_include_raw(include_raw)
        return self._request(
            "market.history.bar.get",
            {
                "instruments": normalized_instruments,
                "period": normalized_period,
                "start_time": normalized_start,
                "end_time": normalized_end,
                "count": count,
                "fill_data": fill_data,
                "subscribe": subscribe,
                "include_raw": include_raw,
                "adjustment": normalized_adjustment,
            },
            timeout=timeout,
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
        return self._place_order_payload(
            order.to_payload(), wait_for=wait_for, timeout=timeout
        )

    def place_reverse_repo(
        self,
        order: ReverseRepoRequest,
        wait_for: str = "LOCAL_ACK",
        timeout: float = 10.0,
    ) -> OrderReceipt:
        """提交一笔交易所国债逆回购委托。

        Args:
            order: :class:`ReverseRepoRequest`，金额以人民币元填写，年化
                收益率作为限价传入QMT。
            wait_for: 返回时点，可选 ``LOCAL_ACK``、``QMT_CALLED`` 或
                ``BROKER_ID``。
            timeout: 等待响应的最长秒数。

        Returns:
            与普通股票委托一致的 :class:`OrderReceipt`，后续可使用
            :meth:`get_order` 查询或 :meth:`cancel_order` 撤单。

        Raises:
            ValidationError: 请求类型、金额、代码或收益率不合法。
            RequestTimeout: 等待Bridge响应或QMT委托ID超时。
            RemoteError: QMT Bridge 或柜台明确拒绝委托。
        """
        if not isinstance(order, ReverseRepoRequest):
            raise ValidationError("order must be a ReverseRepoRequest")
        return self._place_order_payload(
            order.to_payload(), wait_for=wait_for, timeout=timeout
        )

    def subscribe_new_issue(
        self,
        order: NewIssueSubscriptionRequest,
        wait_for: str = "LOCAL_ACK",
        timeout: float = 10.0,
    ) -> OrderReceipt:
        """提交一笔新股或新债申购委托。

        Bridge 会从QMT当日发行数据中读取申购代码对应的发行价，并在调用
        ``passorder`` 前检查QMT返回的最小/最大申购数量。接口不会自动按
        账户额度满额申购，也不会替调用方选择申购标的。

        Args:
            order: :class:`NewIssueSubscriptionRequest`。
            wait_for: 返回时点，可选 ``LOCAL_ACK``、``QMT_CALLED`` 或
                ``BROKER_ID``。
            timeout: 等待响应的最长秒数。

        Returns:
            :class:`OrderReceipt`；可继续用通用委托查询和撤单接口处理。

        Raises:
            ValidationError: 请求类型或参数不合法。
            RequestTimeout: 等待Bridge响应或QMT委托ID超时。
            RemoteError: 当日发行数据不存在、数量越界或QMT拒绝委托。
        """
        if not isinstance(order, NewIssueSubscriptionRequest):
            raise ValidationError(
                "order must be a NewIssueSubscriptionRequest"
            )
        return self._place_order_payload(
            order.to_payload(), wait_for=wait_for, timeout=timeout
        )

    def list_new_issues(
        self, issue_type: str = "ALL", timeout: float = 5.0
    ) -> Dict[str, Any]:
        """查询QMT当前提供的新股和/或新债发行数据。

        Args:
            issue_type: ``ALL``、``STOCK`` 或 ``BOND``。
            timeout: 等待QMT查询结果的最长秒数。

        Returns:
            ``{"items": [...], "count": N}`` 字典。每项包含标准化代码、
            发行价、最小/最大申购数量；QMT完整原始字段保存在 ``raw``。

        Raises:
            ValidationError: ``issue_type`` 不在允许值中。
            RemoteError: QMT发行数据接口不可用或返回格式不正确。
        """
        normalized = str(issue_type or "").upper()
        if normalized not in ("ALL", "STOCK", "BOND"):
            raise ValidationError("issue_type must be ALL, STOCK or BOND")
        return self._request(
            "new_issue.list", {"issue_type": normalized}, timeout=timeout
        )

    def get_new_issue_quota(
        self, account_id: str, timeout: float = 5.0
    ) -> Dict[str, Any]:
        """查询一个股票账户的新股新债申购额度。

        Args:
            account_id: ``bridge_config.json`` 白名单中的股票资金账号。
            timeout: 等待QMT查询结果的最长秒数。

        Returns:
            包含 ``account_id``、``limits``、``raw`` 和查询时间的字典。
            ``limits`` 保留QMT返回的市场额度结构，不擅自换算单位。

        Raises:
            ValidationError: 资金账号为空。
            RemoteError: 账号未配置或QMT额度查询接口不可用。
        """
        normalized = str(account_id or "").strip()
        if not normalized:
            raise ValidationError("account_id is required")
        return self._request(
            "new_issue.quota.get",
            {"account_id": normalized},
            timeout=timeout,
        )

    def _place_order_payload(
        self,
        payload: Dict[str, Any],
        wait_for: str,
        timeout: float,
    ) -> OrderReceipt:
        """发送已由请求模型验证的统一委托负载并解析回执。"""
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
        self,
        client_order_id: str,
        timeout: float = 5.0,
        include_raw: bool = False,
    ) -> Dict[str, Any]:
        """按客户端委托 ID 查询适配器持久化的委托记录。

        Args:
            client_order_id: ``OrderReceipt.client_order_id``。
            timeout: 等待响应的最长秒数。
            include_raw: 是否附带最新QMT委托对象的全部原始字段；默认关闭。

        Returns:
            委托字典，包含 ``qmt_order_id``、内部状态及最新 QMT 回调
            解析出的成交汇总。仅当 ``include_raw=True`` 时包含 ``raw``。
            该函数不会重新提交委托。
        """
        include_raw = _validated_include_raw(include_raw)
        return self._request(
            "order.get",
            {
                "client_order_id": str(client_order_id),
                "include_raw": include_raw,
            },
            timeout=timeout,
        )

    def list_orders(
        self,
        account_id: Optional[str] = None,
        timeout: float = 5.0,
        include_raw: bool = False,
    ) -> Dict[str, Any]:
        """列出适配器持久化的委托记录。

        Args:
            account_id: 可选资金账号；为 ``None`` 时返回所有已配置账号的
                适配器委托。
            timeout: 等待响应的最长秒数。
            include_raw: 是否为每项附带QMT原始委托字段；默认关闭。

        Returns:
            ``{"items": [...], "count": N}`` 形式的字典。
        """
        include_raw = _validated_include_raw(include_raw)
        payload = {"include_raw": include_raw}
        if account_id is not None:
            payload["account_id"] = str(account_id)
        return self._request("order.list", payload, timeout=timeout)

    def list_trades(
        self,
        account_id: str,
        scope: str = "ADAPTER",
        client_order_id: Optional[str] = None,
        include_raw: bool = False,
        timeout: float = 5.0,
    ) -> Dict[str, Any]:
        """查询账户当日成交明细。

        Args:
            account_id: ``bridge_config.json`` 白名单中的股票资金账号。
            scope: ``ADAPTER`` 只查询本Adapter策略产生的成交；``ACCOUNT``
                查询该资金账户当日全部成交，包括手工及其他策略委托。
            client_order_id: 可选的Adapter委托ID；传入时只返回该委托成交。
            include_raw: 是否为每笔成交附带QMT原始对象；默认关闭。
            timeout: 等待QMT查询结果的最长秒数。

        Returns:
            ``{"scope": ..., "items": [...], "count": N}``。标准字段包括
            成交编号、委托ID、证券、方向、成交价、数量、金额和成交时间。
        """
        normalized_account_id = str(account_id or "").strip()
        if not normalized_account_id:
            raise ValidationError("account_id is required")
        normalized_scope = str(scope or "").strip().upper()
        if normalized_scope not in ("ADAPTER", "ACCOUNT"):
            raise ValidationError("scope must be ADAPTER or ACCOUNT")
        include_raw = _validated_include_raw(include_raw)
        payload: Dict[str, Any] = {
            "account_id": normalized_account_id,
            "scope": normalized_scope,
            "include_raw": include_raw,
        }
        if client_order_id is not None:
            normalized_client_order_id = str(client_order_id or "").strip()
            if not normalized_client_order_id:
                raise ValidationError("client_order_id must not be blank")
            payload["client_order_id"] = normalized_client_order_id
        return self._request("trade.list", payload, timeout=timeout)

    def wait_order(
        self,
        client_order_id: str,
        statuses: Optional[Iterable[str]] = None,
        timeout: float = 30.0,
        include_raw: bool = False,
    ) -> Dict[str, Any]:
        """等待一笔委托进入任一目标状态并返回最新委托。

        默认等待 ``FILLED``、``CANCELED`` 或 ``REJECTED``。Bridge 由QMT
        委托/成交回报唤醒，不按固定周期轮询。超时抛出
        :class:`RequestTimeout`，异常 ``data`` 包含最后状态。
        """
        result = self.wait_orders(
            [client_order_id],
            statuses=statuses,
            timeout=timeout,
            include_raw=include_raw,
        )
        return result["items"][0]

    def wait_orders(
        self,
        client_order_ids: Iterable[str],
        statuses: Optional[Iterable[str]] = None,
        timeout: float = 30.0,
        include_raw: bool = False,
    ) -> Dict[str, Any]:
        """等待一批委托全部进入任一目标状态。

        Args:
            client_order_ids: 需要等待的唯一Adapter委托ID集合。
            statuses: 每笔委托满足其中任一状态即完成；默认等待三个终态。
            timeout: 整批共享的最长等待秒数。
            include_raw: 是否在返回的每笔委托中包含QMT原始字段。

        Returns:
            包含 ``items``、``count``、``statuses`` 和 ``completed`` 的字典。
        """
        try:
            normalized_timeout = float(timeout)
        except (TypeError, ValueError):
            raise ValidationError("timeout must be positive")
        if not math.isfinite(normalized_timeout) or normalized_timeout <= 0:
            raise ValidationError("timeout must be positive")
        include_raw = _validated_include_raw(include_raw)
        if isinstance(client_order_ids, str):
            raise ValidationError("client_order_ids must be an iterable of order IDs")
        try:
            values = list(client_order_ids)
        except TypeError:
            raise ValidationError("client_order_ids must be an iterable")
        if not values:
            raise ValidationError("client_order_ids must not be empty")
        normalized_ids = []
        seen = set()
        for value in values:
            client_order_id = str(value or "").strip()
            if not client_order_id:
                raise ValidationError("client_order_ids cannot contain blanks")
            if client_order_id in seen:
                raise ValidationError(
                    "duplicate client_order_id: %s" % client_order_id
                )
            seen.add(client_order_id)
            normalized_ids.append(client_order_id)
        normalized_statuses = _normalized_wait_statuses(statuses)
        return self._request(
            "order.wait",
            {
                "client_order_ids": normalized_ids,
                "statuses": normalized_statuses,
                "timeout_seconds": normalized_timeout,
                "include_raw": include_raw,
            },
            timeout=normalized_timeout + 1.0,
        )

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

    def _connect_quote(self, timeout: float = 5.0) -> None:
        if self.quote_connection.is_connected and self.quote_hello is not None:
            return
        if self.quote_connection.is_connected:
            self.quote_connection.close()
        deadline = time.monotonic() + float(timeout)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RequestTimeout("quote pipe connection timed out")
        self.quote_connection.connect(timeout=remaining)
        message_id = str(uuid.uuid4())
        response = self.quote_connection.request(
            {
                "v": PROTOCOL_VERSION,
                "type": "hello",
                "message_id": message_id,
                "client_id": self.client_id,
                "client_version": __version__,
                "auth_token": self.config["auth_token"],
                "channel": "quote",
            },
            timeout=max(0.001, deadline - time.monotonic()),
            correlation_field="message_id",
        )
        self._raise_for_error(response)
        self.quote_hello = response.get("result") or {}
        if self.quote_hello.get("protocol_version") != PROTOCOL_VERSION:
            bridge_protocol_version = self.quote_hello.get("protocol_version")
            self.quote_connection.close()
            self.quote_hello = None
            raise RemoteError(
                "QMT quote protocol version does not match client",
                code="PROTOCOL_MISMATCH",
                data={
                    "client_protocol_version": PROTOCOL_VERSION,
                    "bridge_protocol_version": bridge_protocol_version,
                },
            )

    def _quote_request(
        self,
        command: str,
        payload: Dict[str, Any],
        timeout: float,
    ) -> Dict[str, Any]:
        request_id = str(uuid.uuid4())
        response = self.quote_connection.request(
            {
                "v": PROTOCOL_VERSION,
                "type": "request",
                "message_id": str(uuid.uuid4()),
                "request_id": request_id,
                "command": command,
                "payload": payload,
            },
            timeout=timeout,
        )
        self._raise_for_error(response)
        return response.get("result") or {}

    @staticmethod
    def _raise_for_error(response: Dict[str, Any]) -> None:
        if response.get("ok"):
            return
        error = response.get("error") or {}
        if response.get("code") == "WAIT_TIMEOUT":
            raise RequestTimeout(
                error.get("message", "QMT adapter request timed out"),
                request_id=response.get("request_id"),
                data=error.get("data") or {},
            )
        raise RemoteError(
            error.get("message", response.get("code", "QMT Bridge error")),
            code=response.get("code", "REMOTE_ERROR"),
            request_id=response.get("request_id"),
            data=error.get("data") or {},
        )
