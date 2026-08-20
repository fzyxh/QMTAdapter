from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Mapping, Optional, Union
import uuid

from .exceptions import ValidationError


ACCOUNT_TYPES = {"STOCK"}
STOCK_SIDES = {"BUY", "SELL"}
STOCK_PRICE_TYPES = {
    "ASK5",  # prType=0：读取卖五价后，按该价格限价申报
    "ASK4",  # prType=1：读取卖四价后，按该价格限价申报
    "ASK3",  # prType=2：读取卖三价后，按该价格限价申报
    "ASK2",  # prType=3：读取卖二价后，按该价格限价申报
    "ASK1",  # prType=4：读取卖一价后，按该价格限价申报
    "LATEST",  # prType=5：读取最新价后，按该价格限价申报
    "BID1",  # prType=6：读取买一价后，按该价格限价申报
    "BID2",  # prType=7：读取买二价后，按该价格限价申报
    "BID3",  # prType=8：读取买三价后，按该价格限价申报
    "BID4",  # prType=9：读取买四价后，按该价格限价申报
    "BID5",  # prType=10：读取买五价后，按该价格限价申报
    "LIMIT",  # prType=11：使用 limit_price 进行限价申报
    "LIMIT_UP_DOWN",  # prType=12：买入取涨停价、卖出取跌停价后限价申报
    "QUEUE",  # prType=13：读取本方一档价后限价申报
    "COUNTERPARTY",  # prType=14：读取对方一档价后限价申报
    "MARKET_SH_CONVERT_5_CANCEL",  # prType=42：沪/北市最优五档即成剩撤
    "MARKET_SH_CONVERT_5_LIMIT",  # prType=43：沪/北市最优五档即成剩转限价
    "MARKET_PEER_PRICE_FIRST",  # prType=44：沪/深/北市对手方最优市价申报
    "MARKET_MINE_PRICE_FIRST",  # prType=45：沪/深/北市本方最优市价申报
    "MARKET_SZ_INSTBUSI_RESTCANCEL",  # prType=46：深市即时成交剩余撤销
    "MARKET_SZ_CONVERT_5_CANCEL",  # prType=47：深市最优五档即成剩撤
    "MARKET_SZ_FULL_OR_CANCEL",  # prType=48：深市全额成交或撤销
}

STOCK_NATIVE_MARKET_PRICE_TYPES = {
    "MARKET_SH_CONVERT_5_CANCEL": {"SH", "BJ"},
    "MARKET_SH_CONVERT_5_LIMIT": {"SH", "BJ"},
    "MARKET_PEER_PRICE_FIRST": {"SH", "SZ", "BJ"},
    "MARKET_MINE_PRICE_FIRST": {"SH", "SZ", "BJ"},
    "MARKET_SZ_INSTBUSI_RESTCANCEL": {"SZ"},
    "MARKET_SZ_CONVERT_5_CANCEL": {"SZ"},
    "MARKET_SZ_FULL_OR_CANCEL": {"SZ"},
}

STOCK_EXECUTION_ALGORITHMS = {
    "BOOK_LIQUIDITY_WEIGHTED",  # 盘口流动性加权拆单（已实现）
    "TWAP",  # 时间加权拆单（仅预留标识，尚未实现）
    "VWAP",  # 成交量加权拆单（仅预留标识，尚未实现）
}

BOOK_LIQUIDITY_WEIGHTED_DEFAULTS = {
    "big_order_threshold": "1000000",
    "min_child_notional": "10000",
    "max_child_notional": "500000",
    "primary_levels": 3,
    "max_levels": 5,
    "chase_ticks": 2,
    "child_interval_ms": 50,
    "timeout_seconds": 20.0,
    "max_retries": 3,
}


PriceValue = Union[str, int, float, Decimal]


def new_id() -> str:
    # Python stdlib has no UUIDv7 in every supported external runtime yet.
    # UUID4 is generated once per OrderRequest and remains stable across retries.
    return str(uuid.uuid4())


def _decimal_text(value: PriceValue) -> str:
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


@dataclass(frozen=True)
class OrderRequest:
    """普通股票委托请求。

    Args:
        account_id: 配置白名单中的股票资金账号。
        instrument: ``代码.市场`` 格式，例如 ``601919.SH``。
        side: ``BUY`` 或 ``SELL``。
        quantity: 委托股数；买入必须为100股的整数倍。
        price_type: 统一价格模式，默认 ``LATEST``。
        limit_price: ``LIMIT`` 模式的限价；原生市价申报中作为保护限价，
            不传时使用 ``0``，即由QMT按对应市场规则处理。
        remark: 用户委托备注。
        account_type: 当前只能为 ``STOCK``。
        business_type: 当前只能为 ``CASH``。
        quantity_type: 当前只能为 ``SHARES``。
        client_order_id: 逻辑委托的唯一ID；不传时在对象创建时生成一次UUID4。
    """
    account_id: str
    instrument: str
    side: str
    quantity: int
    price_type: str = "LATEST"
    limit_price: Optional[PriceValue] = None
    remark: str = ""
    account_type: str = "STOCK"
    business_type: str = "CASH"
    quantity_type: str = "SHARES"
    client_order_id: str = field(default_factory=new_id)

    def to_payload(self) -> Dict[str, Any]:
        """验证字段并转换为 Bridge 协议负载字典。

        Returns:
            可直接放入 ``order.place`` 请求的字典。

        Raises:
            ValidationError: 账号、标的、方向、数量或价格参数不合法。
        """
        account_type = self.account_type.upper()
        side = self.side.upper()
        price_type = self.price_type.upper()
        quantity_type = self.quantity_type.upper()
        business_type = self.business_type.upper()
        client_order_id = str(self.client_order_id or "").strip()

        if not client_order_id:
            raise ValidationError("client_order_id is required")
        if not self.account_id or not str(self.account_id).strip():
            raise ValidationError("account_id is required")
        if account_type not in ACCOUNT_TYPES:
            raise ValidationError("unsupported account_type: %s" % account_type)
        if side not in STOCK_SIDES:
            raise ValidationError("side must be BUY or SELL")
        if not self.instrument or "." not in self.instrument:
            raise ValidationError("instrument must use code.market format")
        instrument = self.instrument.strip().upper()
        if not isinstance(self.quantity, int) or isinstance(self.quantity, bool):
            raise ValidationError("quantity must be an integer")
        if self.quantity <= 0:
            raise ValidationError("quantity must be greater than zero")
        if quantity_type != "SHARES":
            raise ValidationError("v1 stock orders only support SHARES")
        if business_type != "CASH":
            raise ValidationError("v1 only supports CASH stock orders")
        if price_type not in STOCK_PRICE_TYPES:
            raise ValidationError("unsupported price_type: %s" % price_type)
        allowed_markets = STOCK_NATIVE_MARKET_PRICE_TYPES.get(price_type)
        market = instrument.rsplit(".", 1)[-1]
        if allowed_markets is not None and market not in allowed_markets:
            raise ValidationError(
                "%s is not supported for market %s" % (price_type, market)
            )
        normalized_price = (
            _decimal_text(self.limit_price) if self.limit_price is not None else None
        )
        if price_type == "LIMIT":
            try:
                price = Decimal(str(self.limit_price))
            except (InvalidOperation, TypeError, ValueError):
                raise ValidationError("limit_price must be positive for LIMIT")
            if not price.is_finite() or price <= 0:
                raise ValidationError("limit_price must be positive for %s" % price_type)
            normalized_price = _decimal_text(self.limit_price)
        elif allowed_markets is not None:
            raw_price = 0 if self.limit_price is None else self.limit_price
            try:
                price = Decimal(str(raw_price))
            except (InvalidOperation, TypeError, ValueError):
                raise ValidationError(
                    "limit_price must be between 0 and 9999 for market orders"
                )
            if not price.is_finite() or price < 0 or price > 9999:
                raise ValidationError(
                    "limit_price must be between 0 and 9999 for market orders"
                )
            normalized_price = _decimal_text(raw_price)

        return {
            "client_order_id": client_order_id,
            "account_id": str(self.account_id).strip(),
            "account_type": account_type,
            "instrument": instrument,
            "side": side,
            "business_type": business_type,
            "position_effect": "DEFAULT",
            "quantity_type": quantity_type,
            "quantity": self.quantity,
            "price_type": price_type,
            "limit_price": normalized_price,
            "remark": str(self.remark or ""),
        }


@dataclass(frozen=True)
class OrderReceipt:
    """Bridge 接受下单命令后返回的结构化回执。

    ``command_status`` 表示命令调用结果，不表示委托已经成交；最终委托信息
    应使用 ``client_order_id`` 调用 ``get_order`` 或 ``list_orders`` 查询。
    """
    request_id: str
    client_order_id: str
    command_status: str
    order_status: str
    qmt_order_id: Optional[str] = None
    idempotent_replay: bool = False
    raw: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "OrderReceipt":
        """从 Bridge 返回字典构造回执对象。"""
        return cls(
            request_id=value.get("request_id", ""),
            client_order_id=value["client_order_id"],
            command_status=value.get("command_status", "UNKNOWN"),
            order_status=value.get("order_status", "UNKNOWN"),
            qmt_order_id=value.get("qmt_order_id"),
            idempotent_replay=bool(value.get("idempotent_replay", False)),
            raw=dict(value),
        )


@dataclass(frozen=True)
class AlgoOrderRequest:
    """股票算法委托请求。

    Args:
        account_id: 配置白名单中的股票资金账号。
        instrument: ``代码.市场`` 格式，例如 ``601919.SH``。
        side: ``BUY`` 或 ``SELL``。
        algorithm: 执行算法。当前仅实现 ``BOOK_LIQUIDITY_WEIGHTED``；
            ``TWAP``、``VWAP`` 是保留标识，QMT Bridge 会明确拒绝执行。
        target_amount: 目标金额。与 ``quantity`` 必须且只能填写一个。
        quantity: 目标股数。与 ``target_amount`` 必须且只能填写一个；
            买入必须为100股的整数倍。
        params: 算法参数。盘口流动性加权拆单支持
            ``big_order_threshold``、``min_child_notional``、
            ``max_child_notional``、``primary_levels``、``max_levels``、
            ``chase_ticks``、``child_interval_ms``、``timeout_seconds``、
            ``max_retries``。
        remark: 父委托备注；子单备注由 Bridge 自动生成。
        algo_order_id: 父委托唯一ID；不传时在对象创建时生成一次UUID4。

    Note:
        ``target_amount`` 不保证最终成交金额精确等于该值。Bridge 会按100股
        整手换算目标数量，并保证买入计划金额不超过该上限。
    """

    account_id: str
    instrument: str
    side: str
    algorithm: str = "BOOK_LIQUIDITY_WEIGHTED"
    target_amount: Optional[PriceValue] = None
    quantity: Optional[int] = None
    params: Mapping[str, Any] = field(default_factory=dict)
    remark: str = ""
    algo_order_id: str = field(default_factory=new_id)

    def to_payload(self) -> Dict[str, Any]:
        """验证字段并转换为 ``algo_order.place`` 协议负载。"""
        algo_order_id = str(self.algo_order_id or "").strip()
        account_id = str(self.account_id or "").strip()
        instrument = str(self.instrument or "").strip().upper()
        side = str(self.side or "").upper()
        algorithm = str(self.algorithm or "").upper()

        if not algo_order_id:
            raise ValidationError("algo_order_id is required")
        if not account_id:
            raise ValidationError("account_id is required")
        if not instrument or "." not in instrument:
            raise ValidationError("instrument must use code.market format")
        if side not in STOCK_SIDES:
            raise ValidationError("side must be BUY or SELL")
        if algorithm not in STOCK_EXECUTION_ALGORITHMS:
            raise ValidationError("unsupported algorithm: %s" % algorithm)
        has_amount = self.target_amount is not None
        has_quantity = self.quantity is not None
        if has_amount == has_quantity:
            raise ValidationError(
                "exactly one of target_amount and quantity is required"
            )

        normalized_amount: Optional[str] = None
        if has_amount:
            try:
                amount = Decimal(str(self.target_amount))
            except (InvalidOperation, TypeError, ValueError):
                raise ValidationError("target_amount must be positive")
            if not amount.is_finite() or amount <= 0:
                raise ValidationError("target_amount must be positive")
            normalized_amount = _decimal_text(self.target_amount)  # type: ignore[arg-type]

        if has_quantity:
            if not isinstance(self.quantity, int) or isinstance(self.quantity, bool):
                raise ValidationError("quantity must be an integer")
            if self.quantity <= 0:
                raise ValidationError("quantity must be greater than zero")
            if self.quantity % 100 != 0:
                raise ValidationError(
                    "algorithm quantity must be a multiple of 100"
                )

        if not isinstance(self.params, Mapping):
            raise ValidationError("params must be a mapping")

        return {
            "algo_order_id": algo_order_id,
            "account_id": account_id,
            "account_type": "STOCK",
            "business_type": "CASH",
            "instrument": instrument,
            "side": side,
            "algorithm": algorithm,
            "target_amount": normalized_amount,
            "quantity": self.quantity,
            "params": dict(self.params),
            "remark": str(self.remark or ""),
        }


@dataclass(frozen=True)
class AlgoOrderReceipt:
    """Bridge 接受算法父委托后返回的回执。"""

    request_id: str
    algo_order_id: str
    command_status: str
    algo_status: str
    resolved_quantity: int
    child_count: int
    idempotent_replay: bool = False
    raw: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "AlgoOrderReceipt":
        """从 Bridge 返回字典构造算法委托回执。"""
        children = value.get("children") or []
        return cls(
            request_id=str(value.get("request_id", "")),
            algo_order_id=str(value["algo_order_id"]),
            command_status=str(value.get("command_status", "UNKNOWN")),
            algo_status=str(value.get("algo_status", "UNKNOWN")),
            resolved_quantity=int(value.get("resolved_quantity", 0)),
            child_count=int(value.get("child_count", len(children))),
            idempotent_replay=bool(value.get("idempotent_replay", False)),
            raw=dict(value),
        )
