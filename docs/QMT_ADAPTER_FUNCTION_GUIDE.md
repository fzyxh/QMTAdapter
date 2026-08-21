# QMT Adapter 部署与函数调用说明

本文档对应QMTAdapter `0.6.0`、命名管道协议6，说明大 QMT 端脚本的部署方式，
以及外部封装库的同步、异步函数调用方法。

## 1. 当前支持范围

当前版本支持以下股票账户业务：

- 查询股票资金账户；
- 查询股票持仓；
- 查询单只或一批证券的最新普通行情，可选返回QMT原始字段；
- 普通股票买入、卖出；
- 北交所普通股票委托；
- 沪深交易所国债逆回购；
- 查询当日新股、新债发行数据和账户申购额度；
- 新股、新债申购；
- 按指定时间间隔串行提交多笔普通股票委托；
- 查询本适配器发出的委托；
- 查询当日成交，可选择本Adapter委托或账户全部成交；
- 由QMT回报驱动地等待单笔或一批委托进入目标状态；
- 撤销本适配器发出的委托；
- 盘口流动性加权算法父单的预览、提交、查询和整体撤单；
- 算法父单及其确定性子单关系的 SQLite 持久化；
- 同步调用和 `asyncio` 异步调用；
- 以 `client_order_id` 为唯一身份的下单幂等重放与冲突检测。

`TWAP`、`VWAP` 当前只保留算法标识和请求结构，尚未实现。当前不支持信用、
期货、期权、组合、实时行情推送和L2行情封装。

当前没有单独的公开 `idempotency_key`。`client_order_id` 同时承担逻辑委托 ID
和下单幂等键的作用：相同 ID 与相同委托参数只执行一次；相同 ID 与不同参数
会被拒绝。每一笔新的逻辑委托必须使用新的 `client_order_id`。

## 2. 组成与运行关系

### 2.1 大 QMT 端

文件：

```text
qmt_side/qmt_adapter_qmt.py
```

该脚本在大 QMT 的 Python 策略中运行，是唯一调用 QMT 内置交易函数的部分。它负责：

- 调用 `get_trade_detail_data` 查询账户、持仓、委托和成交；
- 调用 `get_ipo_data` 和 `get_new_purchase_limit` 查询当日发行信息及申购额度；
- 调用 `ContextInfo.get_full_tick` 和 `get_instrument_detail` 完成普通行情查询，
  并读取算法下单所需的当前五档盘口、涨跌停价和最小价位；
- 调用 `passorder` 提交股票、逆回购和新股新债申购委托；
- 调用 `can_cancel_order` 和 `cancel` 撤单；
- 接收 QMT 的 `order_callback` 委托回报；
- 接收 QMT 的 `deal_callback` 成交回报；
- 通过 Windows 命名管道接收外部命令并返回结果；
- 使用 SQLite 在调用 `passorder` 前保存委托、参数指纹，并持久化 QMT
  委托 ID 和委托回调原始数据。

### 2.2 外部封装库

目录：

```text
qmt_adapter/
```

外部封装库不导入、不依赖任何 QMT 模块，只通过本机命名管道与大 QMT 端通信。

公开对象：

```python
from qmt_adapter import (
    AsyncQmtClient,
    AlgoOrderReceipt,
    AlgoOrderRequest,
    BOOK_LIQUIDITY_WEIGHTED_DEFAULTS,
    ConnectionClosed,
    NewIssueSubscriptionRequest,
    OrderReceipt,
    OrderRequest,
    QmtAdapterError,
    QmtClient,
    RemoteError,
    ReverseRepoRequest,
    RequestTimeout,
    STOCK_EXECUTION_ALGORITHMS,
    ValidationError,
    __version__,
)
```

## 3. 大 QMT 端部署

### 3.1 安装外部库

在虚拟环境中安装当前仓库：

```powershell
.\.venv\Scripts\python.exe -m pip install -e .
```

外部库安装在该虚拟环境中；QMT 端运行文件由下面的部署命令复制到固定目录，
两者不是同一个安装位置。

### 3.2 首次部署

首次部署时传入股票资金账号：

```powershell
.\.venv\Scripts\qmt-adapter.exe deploy --account-id YOUR_ACCOUNT_ID
```

默认创建以下目录：

```text
C:\QMTAdapter\
├── qmt_adapter_loader.py
├── runtime\qmt_adapter_qmt.py
├── config\bridge_config.json
└── data\
    ├── bridge.db
    ├── bridge.db-wal
    └── bridge.db-shm
```

所有路径均为与券商软件安装目录无关的绝对路径。`auth_token` 由部署命令随机生成。
`bridge.db` 不会被预先创建，而是在 QMT Bridge 第一次启动时由 QMT 端创建。
SQLite 启用 WAL 后，运行期间可能在同一目录生成 `bridge.db-wal` 和
`bridge.db-shm`。

| 文件 | 固定绝对路径 | 创建或更新方 |
|---|---|---|
| QMT 短加载器 | `C:\QMTAdapter\qmt_adapter_loader.py` | 部署命令更新 |
| QMT Bridge | `C:\QMTAdapter\runtime\qmt_adapter_qmt.py` | 部署命令更新 |
| JSON 配置 | `C:\QMTAdapter\config\bridge_config.json` | 首次部署时创建；升级时只删除已废弃的 `environment` 字段 |
| SQLite 数据库 | `C:\QMTAdapter\data\bridge.db` | QMT Bridge 首次启动时创建 |
| SQLite WAL/SHM | `C:\QMTAdapter\data\bridge.db-wal`、`bridge.db-shm` | SQLite 按运行状态创建或删除 |

生成的配置结构如下：

```json
{
  "version": 1,
  "pipe_name": "\\\\.\\pipe\\qmt_adapter_v1",
  "auth_token": "由部署命令生成的64位十六进制字符串",
  "db_path": "C:\\QMTAdapter\\data\\bridge.db",
  "accounts": [
    {
      "account_id": "股票资金账号",
      "account_type": "STOCK"
    }
  ],
  "timer_period": "10nMilliSecond",
  "reconcile_interval_seconds": 5.0,
  "max_commands_per_tick": 20,
  "max_pending_commands": 1000,
  "max_message_size": 1048576,
  "qmt_remark_max_bytes": 64
}
```

配置字段：

| 字段 | 说明 |
|---|---|
| `version` | 当前固定为 `1` |
| `pipe_name` | 本机 Windows 命名管道名称，外部库必须使用相同配置 |
| `auth_token` | 部署时随机生成的连接鉴权令牌，外部库和 QMT 端读取同一配置 |
| `db_path` | QMT 端委托持久化 SQLite 文件路径 |
| `accounts` | 允许查询和交易的股票资金账号白名单 |
| `timer_period` | QMT 处理命令的定时器周期，当前实测可用值为 `10nMilliSecond` |
| `reconcile_interval_seconds` | 委托回调遗漏或重启恢复时的对账周期 |
| `max_commands_per_tick` | 每个 QMT 定时器周期最多处理的命令数 |
| `max_pending_commands` | QMT 主线程待处理命令队列上限 |
| `max_message_size` | 单个管道消息最大字节数 |
| `qmt_remark_max_bytes` | 传入 QMT 的委托备注最大字节数 |

配置中的 `version: 1` 是配置文件格式版本，默认管道名末尾的
`qmt_adapter_v1` 是为兼容既有部署而保留的固定标识；它们都不是Python包版本或
命名管道协议版本。包版本由 `qmt_adapter.__version__` 返回，当前协议版本为6。

### 3.3 创建并运行 QMT 策略

1. 在大 QMT 的“模型交易”中创建 Python 策略。
2. 将 `C:\QMTAdapter\qmt_adapter_loader.py` 的完整内容放入策略代码。
   加载器只包含 ASCII 字符并声明为 GBK，避免复制时发生编码转换问题。
3. 账户类型选择“股票账号”。
4. 资金账号选择配置文件中同一个账号。
5. 主图代码可以使用 `000300`，运行周期使用“日线”。主图只用于启动策略运行环境，不参与适配器交易标的选择。
6. 需要实际调用下单、撤单函数时，在模型交易中使用交易运行模式。
7. 启动后确认日志出现：

```text
QMT Adapter bridge is ready: \\.\pipe\qmt_adapter_v1
```

短加载器只在策略启动时读取和编译一次
`C:\QMTAdapter\runtime\qmt_adapter_qmt.py`。加载完成后，QMT 直接调用外部脚本
定义的入口函数，不会为每次回调、查询或下单增加文件读取或额外转发。

QMT 自动调用以下入口，不需要外部程序直接调用：

| QMT入口 | 作用 |
|---|---|
| `init(ContextInfo)` | 加载配置、设置账号、启动管道和10 ms命令定时器 |
| `_qmt_adapter_tick(ContextInfo)` | 在QMT主线程中处理账户、持仓、下单和撤单命令 |
| `handlebar(ContextInfo)` | 当前为空；适配器不依赖K线触发 |
| `order_callback(ContextInfo, orderInfo)` | 接收委托回报、保存QMT委托ID及原始字段 |
| `deal_callback(ContextInfo, dealInfo)` | 接收成交回报、按成交编号持久化并更新委托汇总 |
| `stop(ContextInfo)` | 停止管道线程并关闭SQLite |

### 3.4 升级 QMT 端脚本

安装新版 Python 包后再次执行：

```powershell
.\.venv\Scripts\qmt-adapter.exe deploy
```

部署命令会原子替换完整 Bridge 和加载器，但不会覆盖
`C:\QMTAdapter\config` 与 `C:\QMTAdapter\data` 下的配置、数据库或 WAL 文件。
随后停止并重新启动 QMT 策略即可加载新代码，不需要再次把完整 Bridge 复制进 QMT。

## 4. 外部库使用准备

在策略自己的虚拟环境中安装 `qmt-adapter` 即可。外部库只使用 Python 标准库，
不需要安装 QMT Python 包。

外部库的公开类和方法包含内联 Type Hints，并提供 `qmt_adapter/py.typed`
标记。支持 PEP 561 的 IDE 和静态检查工具可以识别参数类型、返回类型、
`OrderRequest` 和 `OrderReceipt` 字段类型。Type Hints 只用于开发期检查，
不会改变 Python 的运行时参数校验规则。

不传配置路径时默认读取：

```python
CONFIG_PATH = r"C:\QMTAdapter\config\bridge_config.json"
```

外部库读取同一个 JSON 配置中的 `pipe_name` 和 `auth_token`。
外部库不会直接读取或写入 SQLite；实时请求和响应只通过命名管道传输。

## 5. 同步客户端 QmtClient

### 5.1 推荐连接方式

```python
from qmt_adapter import QmtClient


with QmtClient(config_path=CONFIG_PATH, client_id="my-strategy") as client:
    health = client.health()
    print(health)
```

离开 `with` 代码块时自动关闭命名管道连接。

也可以手动管理：

```python
client = QmtClient(config_path=CONFIG_PATH, client_id="my-strategy")
client.connect(timeout=5.0)
try:
    print(client.health())
finally:
    client.close()
```

### 5.2 连接信息

连接成功后可读取：

```python
client.hello
```

主要字段：

```python
{
    "protocol_version": 6,
    "idempotency_mode": "CLIENT_ORDER_ID_ENFORCED",
    "accounts": [...],
    "commands": [...],
}
```

### 5.3 health

```python
result = client.health(timeout=5.0)
```

用途：检查 QMT Bridge 是否运行、命令队列和 QMT 定时器状态。

主要返回字段：

| 字段 | 说明 |
|---|---|
| `status` | `OK` 或 `DEGRADED` |
| `configured_accounts` | 配置的账号白名单 |
| `pending_commands` | 等待QMT主线程处理的命令数 |
| `last_error` | Bridge最近一次错误 |
| `timer_interval_median_ms` | QMT命令定时器间隔中位数 |

### 5.4 get_account

```python
result = client.get_account("YOUR_ACCOUNT_ID", timeout=5.0)
```

返回结构：

```python
{
    "account_id": "YOUR_ACCOUNT_ID",
    "account_type": "STOCK",
    "items": [
        {
            "account_id": "YOUR_ACCOUNT_ID",
            "account_type": "STOCK",
            "available_cash": 1000000.0,
            "raw": {...},
        }
    ],
    "count": 1,
    "as_of": "UTC时间",
}
```

`available_cash` 来自 QMT 字段 `m_dAvailable`；其他未标准化账户字段保留在 `raw` 中。

### 5.5 list_positions

```python
result = client.list_positions(
    "YOUR_ACCOUNT_ID",
    timeout=5.0,
    include_raw=False,
)
```

每个持仓项目主要字段：

```python
{
    "account_id": "YOUR_ACCOUNT_ID",
    "account_type": "STOCK",
    "instrument": "601919.SH",
    "total_quantity": 1000,
    "available_quantity": 800,
    "frozen_quantity": 200,
    "cost_price": "16.200",
    "current_price": "16.640",
    "market_value": "16640.000",
    "position_profit": "440.000",
}
```

字段分别来自大 QMT 持仓对象的 `m_nVolume`、`m_nCanUseVolume`、
`m_nFrozenVolume`、`m_dOpenPrice`、`m_dLastPrice`、`m_dMarketValue` 和
`m_dPositionProfit`。默认不返回 QMT 原始对象；调试时可设置
`include_raw=True`。四个规范化小数字段统一使用四舍五入并固定输出三位
小数；`raw` 中的 QMT 原始数值不执行该舍入。

### 5.6 get_quote 和 get_quotes

单只询价：

```python
quote = client.get_quote(
    "601919.SH",
    timeout=5.0,
    include_raw=False,
)
```

批量询价：

```python
quotes = client.get_quotes(
    ["688648.SH", "601919.SH", "204001.SH"],
    timeout=5.0,
    include_raw=False,
)
```

批量接口只调用一次大QMT的 `get_full_tick`，返回顺序与传入代码一致；
`get_instrument_detail` 仍按证券逐只读取。证券代码必须带 `.SH`、`.SZ`
或 `.BJ` 后缀，批内不允许重复。

单只接口直接返回一个行情项目；批量接口返回：

```python
{
    "items": [
        {
            "instrument": "601919.SH",
            "exchange": "SH",
            "instrument_name": "中远海控",
            "trading_day": "20260821",
            "quote_time": "20260821 14:59:30",
            "quote_timestamp_ms": 1787295570000,
            "last_price": "17.000",
            "previous_close": "16.640",
            "open_price": "16.700",
            "high_price": "17.080",
            "low_price": "16.640",
            "change": "0.360",
            "change_percent": "2.163",
            "turnover_amount": "1875981800.000",
            "volume_lots": 1108443,
            "price_tick": "0.010",
            "upper_limit": "18.300",
            "lower_limit": "14.980",
            "best_ask_price": "17.010",
            "best_bid_price": "17.010",
            "ask_levels": [
                {"level": 1, "price": "17.010", "volume_lots": 12205}
            ],
            "bid_levels": [
                {"level": 1, "price": "17.010", "volume_lots": 12205}
            ],
            "as_of": "UTC时间",
        }
    ],
    "count": 1,
    "as_of": "UTC时间",
}
```

`volume_lots`、盘口档位中的 `volume_lots` 直接使用大QMT `volume`、
`askVol` 和 `bidVol` 的“手”值，不自动换算成股票股数或逆回购金额。
标准价格、成交额及涨跌幅字段四舍五入并固定输出三位小数。标准盘口只保留
价格大于 `0` 且挂单量大于 `0` 的档位。收盘后QMT可能保留非零价格但把挂单量
清零；这类档位不会进入标准盘口，但仍保留在raw中。

`include_raw=True` 时，每项增加：

```python
{
    "raw": {
        "full_tick": {...},
        "instrument_detail": {...},
    }
}
```

raw 中的原始浮点数、空值、状态字段和盘口数组不做舍入或过滤。

逆回购代码也可以使用普通询价接口。此时 `last_price`、开高低和买卖档价格表示
年化收益率百分数，而不是人民币价格；例如 `"0.870"` 表示年化 `0.870%`。
逆回购原始涨跌停字段可能是无意义占位值，标准接口会在上下限无效时返回 `None`。

### 5.7 list_new_issues

```python
result = client.list_new_issues(issue_type="ALL", timeout=5.0)
```

`issue_type` 可为 `ALL`、`STOCK` 或 `BOND`。返回项目包含标准化的
`instrument`、`issue_type`、`issue_price`、`min_quantity`、
`max_quantity` 和 `subscription_date`；QMT返回的完整字段保存在 `raw`。

### 5.8 get_new_issue_quota

```python
result = client.get_new_issue_quota("YOUR_ACCOUNT_ID", timeout=5.0)
```

返回中的 `limits` 和 `raw` 保留 `get_new_purchase_limit` 的原始字典结构，
适配器不猜测券商版本返回值的单位，也不把该额度自动转换成申购委托。

### 5.9 place_reverse_repo 和 subscribe_new_issue

```python
from qmt_adapter import (
    NewIssueSubscriptionRequest,
    ReverseRepoRequest,
)


repo_receipt = client.place_reverse_repo(
    ReverseRepoRequest(
        account_id="YOUR_ACCOUNT_ID",
        instrument="204001.SH",
        amount=10000,
        annual_rate="1.80",
        remark="repo-example",
    ),
    wait_for="BROKER_ID",
    timeout=10.0,
)

issue_receipt = client.subscribe_new_issue(
    NewIssueSubscriptionRequest(
        account_id="YOUR_ACCOUNT_ID",
        instrument="730001.SH",
        issue_type="STOCK",
        quantity=1000,
        remark="ipo-example",
    ),
    wait_for="BROKER_ID",
    timeout=10.0,
)
```

逆回购 `amount` 以人民币元填写，必须为1000元整数倍。QMT 的 `volume`
按张数传入，每张对应100元标准券，因此1000元对应 `volume=10`。
`annual_rate` 是传给QMT的限价年化收益率。

申购不接收调用方填写的发行价。Bridge 使用 `get_ipo_data(issue_type)` 查找
同一申购代码，读取当日 `issuePrice`，并按QMT返回的最小、最大申购数量检查
范围后调用 `passorder`。接口不会自动选择标的，也不会按账户额度自动满额申购。

两类委托都返回 `OrderReceipt`，并复用 `get_order`、`list_orders`、
`cancel_order`、`client_order_id` 幂等和 `wait_for` 语义。

### 5.10 place_order

函数签名：

```python
receipt = client.place_order(
    order,
    wait_for="LOCAL_ACK",
    timeout=10.0,
)
```

`order` 必须是 `OrderRequest`。

`OrderRequest` 创建时会生成一次 `client_order_id`（也可由调用方显式传入）。
同一个对象重复提交时，该 ID 不会变化。Bridge 对委托字段做规范化后计算参数
指纹，并按以下规则处理：

| 情况 | 处理 |
|---|---|
| 新 `client_order_id` | 先持久化，再调用一次 `passorder` |
| 相同 ID、相同规范化参数 | 返回已有委托，`idempotent_replay=True`，不再调用 `passorder` |
| 相同 ID、不同参数 | 返回 `CLIENT_ORDER_ID_CONFLICT` |
| 已越过可能调用 `passorder` 的边界，但没有可靠 QMT 委托 ID | 返回 `COMMAND_UNCERTAIN`，不自动重下 |

价格指纹会规范化十进制表示，例如限价 `10.2500` 与 `10.25` 视为相同值。

对手方最优买入示例：

```python
from qmt_adapter import OrderRequest, QmtClient


order = OrderRequest(
    account_id="YOUR_ACCOUNT_ID",
    instrument="601919.SH",
    side="BUY",
    quantity=500,
    price_type="COUNTERPARTY",
    remark="strategy-a-buy",
)

with QmtClient(config_path=CONFIG_PATH) as client:
    receipt = client.place_order(
        order,
        wait_for="BROKER_ID",
        timeout=10.0,
    )
    print(receipt.client_order_id)
    print(receipt.qmt_order_id)
```

限价卖出示例：

```python
order = OrderRequest(
    account_id="YOUR_ACCOUNT_ID",
    instrument="600105.SH",
    side="SELL",
    quantity=200,
    price_type="LIMIT",
    limit_price="40.50",
    remark="strategy-a-sell",
)
```

#### wait_for

| 值 | 当前实现的返回时点 | `qmt_order_id` |
|---|---|---|
| `LOCAL_ACK` | SQLite记录已创建，且 `passorder` 已调用并且未抛异常 | 通常暂时为 `None` |
| `QMT_CALLED` | 当前版本与 `LOCAL_ACK` 的实际返回时点相同 | 通常暂时为 `None` |
| `BROKER_ID` | `order_callback` 或Bridge内部对账已关联QMT委托ID，并通过原命名管道请求直接返回 | 正常情况下非空 |

`BROKER_ID` 不依赖外部轮询 SQLite。等待期间同一个客户端连接不能发送下一条请求。

如果等待超时，不能据此认定 QMT 没有接收委托。应保留原始
`OrderRequest`（或至少完整参数和 `client_order_id`），重新连接后先调用
`get_order` 查询；需要重试 `place_order` 时必须提交原对象或相同 ID、相同参数，
Bridge 会重放已有结果而不会重复调用 `passorder`。

### 5.11 place_orders

函数签名：

```python
receipts = client.place_orders(
    orders,
    interval_ms=50,
    wait_for="LOCAL_ACK",
    timeout=10.0,
)
```

该方法接收按提交顺序排列的 `OrderRequest` 可迭代对象，先校验全部委托，再依次
调用现有 `place_order()`。返回值是与输入顺序一致的 `OrderReceipt` 列表。
批内每笔委托必须使用不同的 `client_order_id`；发现重复ID时，整批会在第一笔
提交前被拒绝，不会把重复ID解释成幂等重放。

`interval_ms` 是相邻两次 `place_order()` 调用开始时间的最小间隔，必须为非负
整数。上一笔调用耗时小于该间隔时，客户端等待剩余时间；上一笔耗时已经超过该
间隔时，下一笔在上一笔返回后立即开始。任何情况下都不会并行提交，也不会把落后
的委托连续突发出去。`timeout` 是每笔委托各自的超时时间，不是整批总超时。

```python
from qmt_adapter import OrderRequest, QmtClient


orders = [
    OrderRequest(
        account_id="YOUR_ACCOUNT_ID",
        instrument="601919.SH",
        side="BUY",
        quantity=100,
        price_type="COUNTERPARTY",
        remark="batch-%02d" % index,
    )
    for index in range(10)
]

with QmtClient(config_path=CONFIG_PATH) as client:
    receipts = client.place_orders(
        orders,
        interval_ms=50,
        wait_for="LOCAL_ACK",
        timeout=10.0,
    )
```

任一运行期异常都会停止后续提交；异常前已经提交成功的委托不会自动撤销。调用方
可以使用原始 `OrderRequest.client_order_id` 查询这些委托。空输入返回空列表。

### 5.12 get_order

```python
order = client.get_order(
    client_order_id,
    timeout=5.0,
    include_raw=False,
)
```

该函数查询本适配器持久化的委托记录，不是直接向柜台重新发起一次委托查询。

主要字段：

| 字段 | 说明 |
|---|---|
| `client_order_id` | 外部统一委托ID |
| `request_id` | 原始 `order.place` 协议请求 ID |
| `qmt_order_id` | QMT委托ID，尚未收到回报时可能为空 |
| `order_kind` | `STOCK`、`REVERSE_REPO` 或 `NEW_ISSUE_SUBSCRIPTION` |
| `business_type` | 业务类型 |
| `quantity_type` | `SHARES`、`REPO_UNITS` 或 `SUBSCRIPTION_UNITS` |
| `instrument` | `代码.市场` |
| `side` | `BUY` 或 `SELL` |
| `quantity` | 按 `quantity_type` 解释的委托数量 |
| `price_type` | 统一价格类型 |
| `order_status` | 适配器内部状态 |
| `filled_quantity` | 累计成交数量 |
| `remaining_quantity` | 剩余未成交数量 |
| `average_filled_price` | 成交均价；尚未成交时为空 |
| `filled_amount` | 累计成交金额；尚未成交时为空 |
| `trade_count` | 已持久化的成交明细条数 |
| `reject_reason` | 废单原因；非废单时为空 |
| `raw` | 仅在 `include_raw=True` 时返回的QMT原始委托字段 |
| `created_at` | QMT端创建记录的UTC时间 |
| `updated_at` | 最近回调持久化的UTC时间 |

### 5.13 list_orders

查询指定账号的适配器委托：

```python
result = client.list_orders(
    account_id="YOUR_ACCOUNT_ID",
    timeout=5.0,
    include_raw=False,
)
```

查询全部已配置账号的适配器委托：

```python
result = client.list_orders(timeout=5.0)
```

返回：

```python
{
    "items": [...],
    "count": 50,
}
```

`get_order` 和 `list_orders` 默认不通过命名管道返回QMT原始对象。排查券商字段时
可临时传入 `include_raw=True`。标准化的成交数量、成交金额和废单原因不受该参数影响。

Bridge 已将 QMT 普通股票委托状态 48 至 57 标准化为
`SUBMITTED`、`CANCEL_PENDING`、`PARTIALLY_FILLED`、`CANCELED`、
`FILLED` 和 `REJECTED`。开启 `include_raw` 后可核对以下原始字段：

- `m_nVolumeTotalOriginal`：原始委托数量；
- `m_nVolumeTraded`：已成交数量；
- `m_nVolumeTotal`：剩余数量；
- `m_dTradedPrice`：成交均价；
- `m_dTradeAmount`：成交金额；
- `m_nOrderStatus`：QMT原始委托状态码；
- `m_strErrorMsg`：QMT错误信息。

`FILLED`、`CANCELED` 和 `REJECTED` 是不可逆状态。迟到的旧回报不会把终态
降回 `SUBMITTED` 或 `PARTIALLY_FILLED`，也不会覆盖已经保存的终态原始数据。

算法父单的成交汇总以每个子单的 `m_nVolumeTraded` 求和。某一笔子单已经成交、
撤销或被拒绝，并不表示整个父单也已经结束。

### 5.14 list_trades

只查询本Adapter策略产生的当日成交：

```python
trades = client.list_trades(
    account_id="YOUR_ACCOUNT_ID",
    scope="ADAPTER",
    include_raw=False,
    timeout=5.0,
)
```

查询账户当日全部成交，包括手工委托及其他策略委托：

```python
trades = client.list_trades(
    account_id="YOUR_ACCOUNT_ID",
    scope="ACCOUNT",
)
```

还可传入 `client_order_id`，只返回该Adapter委托的成交。每项标准字段包括
`trade_id`、`client_order_id`、`qmt_order_id`、`instrument`、`side`、
`price`、`quantity`、`amount`、`commission`、`trade_date` 和 `trade_time`。
`include_raw=True` 时才附带QMT原始成交对象。

### 5.15 wait_order 和 wait_orders

```python
final_order = client.wait_order(client_order_id, timeout=30.0)

batch = client.wait_orders(
    client_order_ids,
    statuses={"FILLED", "CANCELED", "REJECTED"},
    timeout=60.0,
)
```

默认目标状态为 `FILLED`、`CANCELED` 和 `REJECTED`。`wait_orders` 要求列表中
每笔委托都进入任一目标状态后才返回。Bridge 使用 `order_callback`、
`deal_callback` 或内部委托对账的状态更新直接唤醒等待请求，不按固定周期查询
SQLite。等待期间占用当前客户端的持久连接，适合在整批委托提交完成后调用。

超时抛出 `RequestTimeout`；异常的 `data` 包含每笔委托的最后状态及仍未完成的
`pending_client_order_ids`。该超时不会关闭持久连接，也不能据此重新下单。

### 5.16 cancel_order

```python
result = client.cancel_order(
    client_order_id,
    timeout=10.0,
)
```

撤单使用 `client_order_id`，Bridge 从持久化记录中找到对应的 `qmt_order_id`，然后调用 QMT 撤单函数。

返回示例：

```python
{
    "client_order_id": "...",
    "qmt_order_id": "...",
    "cancel_requested": True,
    "order_status": "CANCEL_PENDING",
}
```

`cancel_requested=True` 只表示撤单请求已经发出，不代表柜台最终撤单成功。
当前幂等规则仅用于 `order.place`；撤单没有独立幂等键。重复调用撤单时，Bridge
会重新根据 QMT 的 `can_cancel_order` 结果决定是否再次调用 `cancel`，不会因为
`client_order_id` 相同而直接禁止撤单。

## 6. OrderRequest 参数

构造函数：

```python
OrderRequest(
    account_id,
    instrument,
    side,
    quantity,
    price_type="LATEST",
    limit_price=None,
    remark="",
    account_type="STOCK",
    business_type="CASH",
    quantity_type="SHARES",
    client_order_id=None,
)
```

| 参数 | 要求 |
|---|---|
| `account_id` | 必须存在于配置文件账号白名单 |
| `instrument` | 必须使用 `601919.SH`、`000001.SZ`、`920047.BJ` 格式 |
| `side` | `BUY` 或 `SELL` |
| `quantity` | 正整数；沪深买入必须为100股整数倍；北交所买入不少于100股且可按1股递增，单笔不超过100万股 |
| `price_type` | 见价格类型表 |
| `limit_price` | `LIMIT` 时为必填限价；交易所原生市价申报时为可选保护限价，默认 `0` |
| `remark` | 用户备注；Bridge会在前面添加用于回报关联的标签 |
| `account_type` | 当前只能为 `STOCK` |
| `business_type` | 当前只能为 `CASH` |
| `quantity_type` | 当前只能为 `SHARES` |
| `client_order_id` | 可选；不传时在 `OrderRequest` 创建时生成一次 UUID4。它是逻辑委托唯一 ID，也是下单幂等身份 |

不要为同一笔逻辑委托重新构造一个没有显式 `client_order_id` 的
`OrderRequest`，否则会生成新 ID，并被视为另一笔有效委托。反过来，多笔参数完全
相同但业务上独立的委托，应当分别使用不同 ID。

选价后限价申报类型：

| 值 | QMT `prType` | 实际申报含义 |
|---|---:|---|
| `ASK5`～`ASK1` | 0～4 | 读取卖五至卖一价格后，按具体价格限价申报 |
| `LATEST` | 5 | 读取最新价后限价申报，不是原生市价单 |
| `BID1`～`BID5` | 6～10 | 读取买一至买五价格后，按具体价格限价申报 |
| `LIMIT` | 11 | 使用 `limit_price` 限价申报 |
| `LIMIT_UP_DOWN` | 12 | 买入取涨停价、卖出取跌停价后限价申报 |
| `QUEUE` | 13 | 读取本方一档价格后限价申报 |
| `COUNTERPARTY` | 14 | 读取对方一档价格后限价申报；买入等于 `ASK1`，卖出等于 `BID1` |

交易所原生市价申报类型：

| 值 | QMT `prType` | 适用市场 | 申报含义 |
|---|---:|---|---|
| `MARKET_SH_CONVERT_5_CANCEL` | 42 | `.SH`、`.BJ` | 最优五档即时成交，剩余撤销 |
| `MARKET_SH_CONVERT_5_LIMIT` | 43 | `.SH`、`.BJ` | 最优五档即时成交，剩余转限价 |
| `MARKET_PEER_PRICE_FIRST` | 44 | `.SH`、`.SZ`、`.BJ` | 对手方最优价格市价申报 |
| `MARKET_MINE_PRICE_FIRST` | 45 | `.SH`、`.SZ`、`.BJ` | 本方最优价格市价申报 |
| `MARKET_SZ_INSTBUSI_RESTCANCEL` | 46 | `.SZ` | 即时成交，剩余撤销 |
| `MARKET_SZ_CONVERT_5_CANCEL` | 47 | `.SZ` | 最优五档即时成交，剩余撤销 |
| `MARKET_SZ_FULL_OR_CANCEL` | 48 | `.SZ` | 全额成交，否则全部撤销 |

Bridge 和外部 `OrderRequest` 都会校验价格模式与证券市场是否匹配。原生市价
申报的 `limit_price` 会传入 QMT 的 `price` 参数作为保护限价；不传时传 `0`。
QMT 当前官方枚举说明标注 `42～48` 不支持模拟交易，因此模拟环境可能返回
废单或明确错误；是否能由实盘柜台接受必须在对应市场、交易时段和账户权限下验证。

## 7. OrderReceipt 字段

`place_order` 返回 `OrderReceipt`：

```python
receipt.request_id
receipt.client_order_id
receipt.command_status
receipt.order_status
receipt.qmt_order_id
receipt.idempotent_replay
receipt.raw
```

注意：

- `command_status="SUCCEEDED"` 只说明QMT下单函数调用未抛异常，不等于成交；
- 使用 `LOCAL_ACK` 或 `QMT_CALLED` 时，`qmt_order_id` 可以为空；
- 使用 `BROKER_ID` 时，正常返回会包含 `qmt_order_id`；
- `idempotent_replay=True` 表示本次请求命中了同一 `client_order_id` 的已存委托，
  本次没有再次调用 `passorder`；
- `request_id` 是本次通信尝试的 ID；幂等重放时它会变化，不是逻辑委托身份。

## 8. 异步客户端 AsyncQmtClient

`AsyncQmtClient` 提供与 `QmtClient` 对应的异步函数：

```python
async with AsyncQmtClient(config_path=CONFIG_PATH) as client:
    health = await client.health()
    account = await client.get_account("YOUR_ACCOUNT_ID")
    positions = await client.list_positions("YOUR_ACCOUNT_ID")
```

完整方法：

| 异步函数 | 对应同步函数 |
|---|---|
| `await connect(timeout)` | `connect(timeout)` |
| `await close()` | `close()` |
| `await health(timeout)` | `health(timeout)` |
| `await get_account(account_id, timeout)` | `get_account(...)` |
| `await list_positions(account_id, timeout, include_raw)` | `list_positions(...)` |
| `await get_quote(instrument, timeout, include_raw)` | `get_quote(...)` |
| `await get_quotes(instruments, timeout, include_raw)` | `get_quotes(...)` |
| `await list_new_issues(issue_type, timeout)` | `list_new_issues(...)` |
| `await get_new_issue_quota(account_id, timeout)` | `get_new_issue_quota(...)` |
| `await place_order(order, wait_for, timeout)` | `place_order(...)` |
| `await place_reverse_repo(order, wait_for, timeout)` | `place_reverse_repo(...)` |
| `await subscribe_new_issue(order, wait_for, timeout)` | `subscribe_new_issue(...)` |
| `await place_orders(orders, interval_ms, wait_for, timeout)` | `place_orders(...)` |
| `await get_order(client_order_id, timeout, include_raw)` | `get_order(...)` |
| `await list_orders(account_id, timeout, include_raw)` | `list_orders(...)` |
| `await list_trades(account_id, scope, client_order_id, include_raw, timeout)` | `list_trades(...)` |
| `await wait_order(client_order_id, statuses, timeout, include_raw)` | `wait_order(...)` |
| `await wait_orders(client_order_ids, statuses, timeout, include_raw)` | `wait_orders(...)` |
| `await cancel_order(client_order_id, timeout)` | `cancel_order(...)` |
| `await preview_algo_order(order, timeout)` | `preview_algo_order(...)` |
| `await place_algo_order(order, timeout)` | `place_algo_order(...)` |
| `await get_algo_order(algo_order_id, timeout)` | `get_algo_order(...)` |
| `await list_algo_orders(account_id, timeout)` | `list_algo_orders(...)` |
| `await cancel_algo_order(algo_order_id, timeout)` | `cancel_algo_order(...)` |

内部实现只有一个工作线程、一个命名管道连接和一把异步锁，因此同一个 `AsyncQmtClient` 的所有请求严格串行，不会并行调用 QMT 的 `passorder`。

单笔异步下单：

```python
import asyncio

from qmt_adapter import AsyncQmtClient, OrderRequest


async def main():
    order = OrderRequest(
        account_id="YOUR_ACCOUNT_ID",
        instrument="601919.SH",
        side="BUY",
        quantity=100,
        price_type="COUNTERPARTY",
        remark="async-buy",
    )

    async with AsyncQmtClient(config_path=CONFIG_PATH) as client:
        receipt = await client.place_order(
            order,
            wait_for="BROKER_ID",
            timeout=10.0,
        )
        print(receipt.client_order_id, receipt.qmt_order_id)


asyncio.run(main())
```

按固定间隔串行提交多笔委托：

```python
receipts = await client.place_orders(
    orders,
    interval_ms=50,
    wait_for="LOCAL_ACK",
    timeout=10.0,
)
```

异步批量接口在内部工作线程中复用同步批量接口，因此不阻塞调用方事件循环。整批
操作独占该客户端的一条持久命名管道，批量中的委托不会与同一客户端的其他请求
交错，实际 QMT 请求仍按顺序执行。

批量下单阶段使用 `LOCAL_ACK` 可以避免每笔都等待委托ID。全部发送完成后，再根据每个 `receipt.client_order_id` 调用 `get_order`，或者统一调用 `list_orders` 查询QMT委托ID和回调字段。

## 9. 异常处理

```python
from qmt_adapter import QmtAdapterError


try:
    receipt = client.place_order(order, wait_for="BROKER_ID", timeout=10.0)
except QmtAdapterError as exc:
    print(exc.code)
    print(exc.request_id)
    print(exc.data)
    print(str(exc))
```

异常类型：

| 类型 | 说明 |
|---|---|
| `ValidationError` | 外部请求参数不合法 |
| `RemoteError` | QMT Bridge明确返回错误 |
| `RequestTimeout` | 连接或请求等待超时 |
| `ConnectionClosed` | 命名管道连接已经关闭 |
| `QmtAdapterError` | 上述适配器异常的基类 |

重要规则：下单请求一旦跨过 `passorder` 调用边界，连接断开、超时或
`COMMAND_UNCERTAIN` 都不能证明委托没有提交。Bridge 不会自动重下不确定命令。
先查询原 `client_order_id`；若重试下单，只能使用原 ID 和完全相同的委托参数。
使用新 ID 会被当成新委托执行。

## 10. 通信、持久化和并发限制

- 命名管道负责实时请求和响应；
- SQLite只由QMT端用于委托映射、回调持久化和重启恢复；
- 外部库不通过SQLite向QMT发送命令，也不通过SQLite等待实时结果；
- SQLite 的写入只发生在 QMT Bridge 进程内；它用于持久化和幂等判断，不承担
  双向通信；
- 旧版 `orders` 表会在 QMT Bridge 启动时自动增加并回填 `payload_hash`；
- 当前管道服务只允许一个活动客户端连接；
- 单个同步或异步客户端也只允许一条请求在途；
- 正常断开后，服务端会重新创建管道并接受下一次连接；
- QMT账户、持仓、普通行情、成交、发行数据、申购额度、普通下单、逆回购、
  申购、算法下单和撤单命令在QMT主线程的定时器中执行；
- `system.health`、`order.get/list/wait` 和 `algo_order.get/list` 是本地持久化读取，
  不需要调用QMT交易函数。

## 11. 推荐的基本调用顺序

```text
启动大QMT并登录账号
  -> 启动QMT Adapter策略
  -> 外部客户端connect
  -> health确认Bridge运行状态
  -> get_account/list_positions确认账号
  -> get_quote/get_quotes按需查询最新普通行情
  -> 新股新债：list_new_issues/get_new_issue_quota后由调用方决定是否申购
  -> 普通委托：place_order并保存client_order_id
  -> 逆回购/申购：对应独立下单函数并保存client_order_id
  -> 算法委托：preview_algo_order后place_algo_order并保存algo_order_id
  -> get_order/list_orders查看QMT委托ID及回调
  -> wait_order/wait_orders等待目标状态，list_trades查询成交明细
  -> 如需撤单则cancel_order(client_order_id)
  -> close
```

外部业务程序至少应持久化普通单的 `client_order_id` 或算法父单的
`algo_order_id` 及原始请求参数，避免程序重启后失去委托关联。

## 12. 升级说明

本版本的命名管道协议为6。外部库和 QMT 端脚本必须同时升级；协议版本不同会收到
`PROTOCOL_MISMATCH`，不会继续发送交易命令。升级时先停止外部客户端，替换并
重启 QMT 策略，再启动外部客户端。

## 13. 盘口流动性加权算法委托

### 13.1 统一模型

执行算法和子单报价类型是两个不同维度：

- `algorithm="BOOK_LIQUIDITY_WEIGHTED"` 表示父单如何拆分、跟踪和重试；
- 算法生成的每笔子单固定使用 `price_type="LIMIT"`；
- `TWAP`、`VWAP` 与盘口算法使用同一个 `AlgoOrderRequest` 模型，但当前调用
  会返回 `ALGORITHM_NOT_IMPLEMENTED`，不会写父单或调用 `passorder`；
- 所有算法共用同一个大 QMT 模型和同一条 Bridge，不需要为每种算法新建策略。

### 13.2 请求对象

```python
from qmt_adapter import AlgoOrderRequest


order = AlgoOrderRequest(
    account_id="YOUR_ACCOUNT_ID",
    instrument="601919.SH",
    side="BUY",
    target_amount="10000000",
    algorithm="BOOK_LIQUIDITY_WEIGHTED",
    params={
        "big_order_threshold": "1000000",
        "min_child_notional": "10000",
        "max_child_notional": "500000",
        "primary_levels": 3,
        "max_levels": 5,
        "chase_ticks": 2,
        "child_interval_ms": 50,
        "timeout_seconds": 20.0,
        "max_retries": 3,
    },
    remark="book-liquidity",
)
```

`target_amount` 与 `quantity` 必须且只能传一个。金额买入会换算成不超过该金额
的最大整手目标数量；明确数量必须是100股的整数倍。

参数默认值和含义：

| 参数 | 默认值 | 含义 |
|---|---:|---|
| `big_order_threshold` | 1000000 | 预计金额不超过该值时只使用对手一档，但仍执行单笔上限拆分 |
| `min_child_notional` | 10000 | 大单中某盘口档位参与分配的最小金额 |
| `max_child_notional` | 500000 | 所有子单（包括追价子单）的硬性金额上限 |
| `primary_levels` | 3 | 首次按流动性权重分配的档位数 |
| `max_levels` | 5 | 最多使用的可见盘口档位数 |
| `chase_ticks` | 2 | 五档仍不足时，相对最后有效档位的追价跳数；每跳取 QMT `PriceTick`，最终同时受涨跌停价和价格笼子约束 |
| `child_interval_ms` | 50 | QMT 端相邻两次算法子单 `passorder` 调用的最小间隔；允许10至60000毫秒 |
| `timeout_seconds` | 20.0 | 当前轮次子单超时秒数 |
| `max_retries` | 3 | 初始轮次之外允许重新读取盘口和重算的次数 |

### 13.3 先预览、后提交

```python
from qmt_adapter import QmtClient


with QmtClient() as client:
    preview = client.preview_algo_order(order, timeout=10.0)
    print(preview["depth"])
    print(preview["resolved_quantity"], preview["planned_notional"])
    print(preview["children"])

    assert preview["planned_quantity"] == preview["resolved_quantity"]
    receipt = client.place_algo_order(order, timeout=30.0)
    print(receipt.algo_order_id, receipt.algo_status, receipt.child_count)
```

`preview_algo_order()` 不写 SQLite、不调用 `passorder`。它返回：

- 大 QMT 模型上下文返回的买卖完整五档（不连接外部 `xtdata`）；
- 最新价、昨收、实时涨跌幅；
- `get_instrument_detail` 返回的当日涨跌停价、最小价位和交易状态；
- 由大 QMT 盘口与最小价位按交易所规则计算的动态价格笼子；
- `volume_lots`（手）与 `visible_quantity`（股，按100股/手换算）；
- 账户可用资金或标的可用持仓；
- 解析后的目标股数、预计委托金额和全部子单。

`place_algo_order()` 会重新读取即时盘口并重新执行相同的资源和守恒检查，不能
假设其结果与更早的预览完全一致。新父单的处理顺序固定为：生成完整计划 →
验证总量与金额 → 在一个 SQLite 事务中写父单和全部子单计划 → 第一笔可立即
提交，其余子单由 QMT 定时回调按 `child_interval_ms` 逐笔调用 `passorder`。
该方法返回表示 Bridge 已接受并开始调度，不表示全部子单已经提交。

### 13.4 查询和整体撤单

```python
current = client.get_algo_order(receipt.algo_order_id)
items = client.list_algo_orders(account_id="YOUR_ACCOUNT_ID")
cancelled = client.cancel_algo_order(receipt.algo_order_id)
```

父单的主要状态为 `PLACING`、`WORKING`、`RETRY_CANCELING`、`CANCELING`、
`UNKNOWN`、`FILLED`、`CANCELED`、`FAILED`。查询结果包含全部轮次子单、每笔
QMT 委托号、标准化委托状态、已成交数量和原始回报。

整体撤单允许重复调用。Bridge 只对当前可撤子单发出撤单请求；确认所有子单都已
成交、撤销或被拒绝，不会再继续成交后，父单才变为 `CANCELED`。算法超时重试
也遵守同样顺序：先撤当前轮次，等待 QMT 确认每笔旧子单的结果，再用“父单目标
数量减去所有轮次累计成交数量”生成下一轮子单。因此不会在旧单仍可能成交时
直接按原数量重下。

### 13.5 同步与异步方法

同步客户端公开：

- `preview_algo_order()`
- `place_algo_order()`
- `get_algo_order()`
- `list_algo_orders()`
- `cancel_algo_order()`

`AsyncQmtClient` 提供同名异步方法。它们仍通过一个工作线程和一条持久命名管道
严格串行发送请求，不会并行执行 `passorder`。同步调用在等待 Bridge 接受计划时
阻塞当前线程；异步调用只是不阻塞调用方的 asyncio 事件循环。两者使用完全相同的
QMT 端50毫秒子单调度，不会因为使用异步客户端而绕过或缩短间隔。
