# QMTAdapter

由于交易接入环境变化，许多原本运行于 MiniQMT 的量化策略需要迁移至大 QMT。为了降低迁移及后续维护成本，同时保持策略代码的独立性和灵活性，本项目提供了一套基于大 QMT 的交易适配方案。

项目由大 QMT 端桥接脚本和外部 Python 客户端库组成。策略侧无需导入或直接调用 QMT Python API，即可通过统一接口完成账户查询、持仓查询、股票委托、委托查询和撤单等操作。

欢迎试用。如遇问题或有改进建议，请提交 Issue。

## 已成功测试客户端

- 东北证券NET专业版v2.1.19.0
- 东北证券NET专业版v1.0.0.38788
- 平安证券量盈QMT策略交易平台v2.1.17.0

## 当前功能

- 股票账户查询；
- 股票持仓查询；
- 单只及批量证券行情查询（可选返回QMT原始行情）；
- 沪深北行情持续推送，可选择全部标的、沪深京A股、明确证券或股票与明确证券
  的组合，并支持增量或完整快照模式及自定义推送周期；
- 大QMT板块成分、股票/ETF/转债等证券的批量合约详情及多周期历史K线查询；
- 历史日线提供不随查询起点变化的每日累计复权因子；
- 普通现金账户股票买入和卖出；
- 北交所普通股票委托（买入不少于100股，之后可按1股递增）；
- 指定时间间隔串行批量下单；
- 沪深北交易所原生股票市价申报（按各市场支持的申报类型校验）；
- 沪深交易所国债逆回购；
- 当日新股、新债发行信息及账户申购额度查询；
- 新股、新债申购；
- 委托查询和撤单；
- 当日成交查询，可选择仅查询本Adapter委托或查询账户全部成交；
- 由QMT委托/成交回报驱动的单笔和批量委托状态等待；
- 多个外部策略进程分别保持持久连接，QMT命令统一进入主线程串行执行；
- 按盘口流动性拆分并按固定间隔提交大额股票委托，支持预览、提交、查询和撤单。

## 性能与即时性

QMTAdapter 使用本机持久化双向命名管道，默认允许最多8个外部策略客户端分别
保持连接，客户端不需要在每条命令前重新连接。每个客户端最多一条请求在途；
不同客户端可以同时排队，但所有QMT函数仍由QMT主线程串行调用。每条连接的
管道读写由独立线程处理，单个客户端断开或停止读取不会阻塞QMT主线程及其他客户端。
持续行情使用独立的 `qmt_adapter_quote` 管道，行情分片和慢消费者不会占用交易
命令管道的响应流。
QMT 端默认以 `10ms` 定时器处理命令队列，即约每 `10ms` 进入一次
QMT 主线程处理。命名管道本身的读写往返延迟处于微秒级，显著低于 QMT
的命令调度周期。

以下为同一台 Windows 主机上不同通信方式的实测往返数据：

| 通信方式 | 中位数 | P95 | 测量内容 |
|---|---:|---:|---|
| 命名管道 | `0.021ms` | `0.047ms` | Win32 命名管道帧回显往返 |
| 本机 TCP | `0.025ms` | `0.109ms` | `127.0.0.1` 帧回显往返 |
| SQLite WAL | `1.981ms` | `2.726ms` | 持久化写入和读取往返 |
| 普通文件 | `6.052ms` | `8.761ms` | 打开、写入和读取往返 |

Bridge 真实运行时的 QMT 定时器间隔中位数实测为 `10.003ms`，与默认
`10ms` 周期一致。下表是常用函数真实 QMT 环境下的客户端端到端总延迟，从函数调用
开始计时直到返回完整响应：

| 客户端函数 | 中位数 | P95 | 最大值 |
|---|---:|---:|---:|
| `health()` | `19.797ms` | `31.344ms` | `31.408ms` |
| `get_account()` | `28.852ms` | `30.992ms` | `31.190ms` |
| `list_positions()` | `20.720ms` | `30.940ms` | `31.466ms` |
| `get_quote()` | `28.560ms` | `31.036ms` | `31.388ms` |
| `get_quotes()`（5只证券） | `29.087ms` | `31.740ms` | `37.264ms` |

周期性全推完整快照另按“从服务端开始生成已缓存快照，到客户端收齐全部分片”
计时。以下结果来自同机大QMT实测：`instrument_scope="STOCK"`、沪深京A股
共5558只、`include_raw=False`、22轮稳定样本。它们不包含建立订阅、主动调用
`get_full_tick` 和首次标准化全量行情的时间。默认每片2000只，在首次收到数据的
速度和收齐全量快照的延迟之间较为均衡：

| `chunk_size` | 每轮分片数 | 首片延迟中位数 | 收齐延迟中位数 | 收齐延迟P95 |
|---:|---:|---:|---:|---:|
| `500` | 12 | `11.809ms` | `234.223ms` | `317.966ms` |
| `2000`（默认） | 3 | `52.611ms` | `135.040ms` | `207.654ms` |
| `0`（不按数量分片） | 1 | `125.768ms` | `125.768ms` | `158.481ms` |

`initial_snapshot=True` 是另一种计时口径：Bridge需要主动取得所选证券行情，完成
全量标准化、分片并发送。2026-09-04收盘后使用QMT缓存行情进行的同机实测中，
底层沪深全推已经保持连接时，仅上证指数和深证成指的完整初始快照中位数为
`138.541ms`；沪深股票加这两个指数共5219只时为 `1094.868ms`。同条件下仅
沪深股票5217只为
`1126.254ms`，说明增加两个指数本身没有显著开销。`initial_snapshot=False`
时只建立订阅，不主动生成首批行情；底层全推未保持和已经保持连接的两组10轮
订阅调用，中位数分别为 `95.218ms` 和 `110.886ms`。
收盘后 `DELTA` 模式可能一直没有事件；盘中首批增量延迟取决于下一次QMT回调和
设置的聚合周期。

首次连接和协议握手本次实测为 `7.569ms`，不计入上表的单次调用延迟。
这些数据用于展示当前实现的延迟量级，不是跨机器、跨 QMT 版本的性能保证。

## 安装与部署

### 1. 安装外部 Python 库

以下两种方式二选一。

#### 方式一：从 GitHub Latest Release 安装

在外部策略项目目录中创建虚拟环境，然后执行一条命令自动获取并安装最新 Release 的 wheel：

```powershell
python -m venv .venv
$version = (Invoke-RestMethod "https://api.github.com/repos/fzyxh/QMTAdapter/releases/latest").tag_name.TrimStart("v")
.\.venv\Scripts\python.exe -m pip install "https://github.com/fzyxh/QMTAdapter/releases/latest/download/qmt_adapter-$version-py3-none-any.whl"
```

#### 方式二：从源码安装

克隆仓库，在仓库根目录创建虚拟环境并安装：

```powershell
git clone https://github.com/fzyxh/QMTAdapter.git
Set-Location QMTAdapter
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e .
```

两种方式都会将 `qmt_adapter` 安装到该虚拟环境，并在 Windows 下生成 `.\.venv\Scripts\qmt-adapter.exe`。外部策略通过它调用大 QMT，不需要导入 QMT Python 模块。

### 2. 生成配置和 QMT 端脚本

首次部署时传入股票资金账号：

```powershell
.\.venv\Scripts\qmt-adapter.exe deploy --account-id YOUR_ACCOUNT_ID
```

部署及 QMT 策略首次启动后使用以下目录：

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

部署目录固定为 `C:\QMTAdapter`，与券商软件安装目录无关。配置文件和随机鉴权令牌在首次部署时生成；SQLite 数据库在 QMT 策略首次启动时创建。单条协议消息默认上限为5 MiB。

### 3. 在大 QMT 创建并启动策略

1. 打开大 QMT 的“模型研究”，新建 Python 策略。

   ![在大 QMT 中创建 QMT_ADAPTER_LOADER 策略](assets/readme/qmt-strategy-list.png)

2. 将 `C:\QMTAdapter\qmt_adapter_loader.py` 的完整内容复制到策略代码中并保存。

   ![将短加载器复制到大 QMT 策略编辑器](assets/readme/qmt-loader-editor.png)

3. 打开“模型交易”，点击“新建策略交易”，策略类型选择刚才创建的策略。
4. 账户类型选择“股票账号”，资金账号选择部署时填写的同一账号，建议勾选终端启动后自动运行。
5. 主图代码可填写 `000300`，运行周期选择“日线”。主图不决定实际交易标的。
6. 创建后将运行模式切换为“实盘”并启动策略。这里的“实盘”是 QMT 的策略运行模式，不代表所选资金账号一定是真实账户。

   ![在模型交易中使用实盘运行模式启动策略](assets/readme/qmt-trading-mode.png)

7. 确认 QMT 日志出现：

```text
QMT Adapter bridge [vx.x.x] is ready: \\.\pipe\qmt_adapter
QMT Adapter quote stream is ready: \\.\pipe\qmt_adapter_quote
```

短加载器只在策略启动时读取一次完整 Bridge，不会在每次查询或下单时重复加载。

### 4. 验证账户和持仓

保持 QMT 策略运行，直接通过已安装的 Python 包查询：

```powershell
.\.venv\Scripts\python.exe -c "from qmt_adapter import QmtClient; c = QmtClient().connect(); print(c.health()); print(c.get_account('YOUR_ACCOUNT_ID')); print(c.list_positions('YOUR_ACCOUNT_ID')); c.close()"
```

能够返回 Bridge 状态、账户资金和持仓即表示部署成功。从源码安装时，其他辅助脚本的用途和运行方式见 [`scripts/README.md`](scripts/README.md)。

历史K线通过大QMT模型上下文读取，不需要在外部Python中导入 `xtquant`。长时间范围
请拆成较小证券批次，避免超过单条5 MiB消息上限：

```python
from qmt_adapter import QmtClient


with QmtClient() as client:
    history = client.get_bar_history(
        ["932000.SH"],
        period="60m",
        start_time="20260825",
        end_time="20260827",
        subscribe=False,
        timeout=60.0,
        include_raw=False,
    )
```

`get_bar_history()` 支持 `1m`、`3m`、`5m`、`10m`、`15m`、`30m`、
`60m`、`2h`～`4h`、`1d`、`2d`～`5d`、`1w`、`1mon`、`1q`、
`1hy` 和 `1y`；`1h` 作为 `60m` 的输入别名，120分钟线使用 `2h`。合成周期
依赖本地对应的基础周期数据，例如历史 `60m` 和 `2h` 由 `5m` 合成，周线和
月线由日线合成。

`subscribe=False` 时，接口只读取大QMT本地已有历史数据；
设置为 `True` 时由大QMT按自身规则订阅并尝试补充请求的数据。实测订阅可以补充
最近多日日线，但不保证补齐任意长区间；需要完整的长区间数据时，应先在大QMT
客户端完成对应基础周期下载。`start_time` 只限定请求范围，不代表券商行情服务器
一定提供该日期之后的全部历史；服务器权限或数据留存不足时，接口按实际可用数据
返回部分区间或空结果，不会用其他周期推算缺失的分钟K线。

`get_bar_history()` 的开高低收默认均为不复权价格；设置 `include_raw=True` 才会
额外附带QMT原始列。分钟、小时、周、月等周期返回固定通用字段，只有 `1d`
额外返回 `adjustment_factor`：它是截至对应交易日的完整历史累计复权因子，不随
查询起点变化，最多保留六位小数。`get_daily_history()` 保留为
`period="1d"` 的兼容快捷接口，并可通过 `adjustment="front"`、`"back"`、
`"front_ratio"` 或 `"back_ratio"` 请求复权日线；默认 `"none"`。该参数只
改变日线标准价格字段，不改变 `adjustment_factor` 的定义。

### 5. 全市场行情推送

全推通过独立行情管道工作，不占用查询和交易命令管道。`DELTA` 会把一个周期内
发生更新的证券合并后发送，同一证券只保留该周期最后一条；`SNAPSHOT` 每个周期
发送当前缓存中的完整市场快照。完整快照可能超过5 MiB，因此会自动拆成多个事件，
调用方按相同 `batch_id`、`chunk_index` 和 `chunk_count` 组合。
默认行情项包含证券代码、北京时间、最新价、昨收、开高低、成交量额以及买卖
档位数组；批次推送时间只在事件最外层出现一次，以减少全市场消息体积。需要
QMT原始字段时再设置 `include_raw=True`。

```python
from qmt_adapter import QmtClient


with QmtClient() as client:
    subscription = client.subscribe_whole_quote(
        markets=["SH", "SZ", "BJ"],
        mode="DELTA",              # 或 "SNAPSHOT"
        instrument_scope="STOCK",  # 只推送沪深京A股；"ALL" 为全部标的
        push_interval_ms=50,
        chunk_size=2000,           # None用服务端默认值；0表示不按数量分片
        include_raw=False,
        initial_snapshot=True,     # 首个事件主动返回当前完整快照
    )
    while True:
        event = client.get_quote_event(timeout=5.0)
        for quote in event["items"]:
            print(quote["instrument"], quote["last_price"])
```

每个客户端只维护一个全推订阅，再次订阅会替换原订阅。调用
`unsubscribe_whole_quote()` 或关闭客户端都会释放订阅。行情回调只负责按证券代码
合并最新原始值并唤醒后台线程；标准化、周期聚合、分片和管道发送均在后台完成。
`instrument_scope="STOCK"` 使用大QMT的“沪深京A股”板块过滤股票，不使用证券
代码前缀推断；默认 `"ALL"` 保持交易所全部二级市场标的。需要沪深A股并额外
包含上证指数、深证成指时，使用：

```python
client.subscribe_whole_quote(
    markets=["SH", "SZ"],
    instrument_scope="STOCK",
    include_instruments=["000001.SH", "399001.SZ"],
)
```

只订阅这两个指数时，将 `instrument_scope` 设为 `None`。最终证券范围等于
`instrument_scope` 对应的基础范围与 `include_instruments` 的并集；明确加入的
证券会自动去重，且市场后缀必须包含在 `markets` 中。`ALL` 已包含市场全部标的，
因此不能再指定 `include_instruments`。
`initial_snapshot=True` 会在订阅建立时主动取得所选证券的当前行情，并将首批事件
标记为 `initial_snapshot=True`、`is_snapshot=True`；之后仍按指定的 `DELTA` 或
`SNAPSHOT` 模式推送。该参数默认关闭，避免全市场订阅无意增加启动开销。
关闭时，`subscribe_whole_quote()` 返回只表示订阅已经建立，不表示已经收到行情；
`DELTA` 必须等待后续行情更新，收盘后可能没有事件。
每次订阅生成新的UUID4 `subscription_id`，它只用于当前订阅生命周期内关联事件，
不按交易日重置，也不是可持久化恢复的业务编号。
`chunk_size` 可为每次订阅单独设置：`None` 使用服务端
`quote_event_max_items=2000` 的默认值，正整数表示每片最多证券数，`0` 表示
不按数量分片。无论如何设置，单条
消息仍受5 MiB硬上限约束。

### 6. 升级

安装新版代码后执行：

```powershell
.\.venv\Scripts\qmt-adapter.exe deploy
```

该命令更新加载器和 Bridge，不覆盖账号、鉴权令牌及数据库。完成后停止并重新启动
QMT 策略，使新代码生效。

## 交易验证

调用下单或撤单接口时，大 QMT 策略必须使用交易运行模式。

盘口流动性加权拆单使用独立的 `algorithm` 字段，不属于
`STOCK_PRICE_TYPES`。算法生成的每笔子单都是明确价格的 `LIMIT` 委托。
建议先预览，再提交同一个请求对象：

```python
from qmt_adapter import AlgoOrderRequest, QmtClient


order = AlgoOrderRequest(
    account_id="YOUR_ACCOUNT_ID",
    instrument="601919.SH",
    side="BUY",
    target_amount="10000000",
    algorithm="BOOK_LIQUIDITY_WEIGHTED",
    remark="liquidity-example",
)

with QmtClient() as client:
    preview = client.preview_algo_order(order)
    assert preview["planned_quantity"] == preview["resolved_quantity"]
    receipt = client.place_algo_order(order)
    current = client.get_algo_order(receipt.algo_order_id)
```

`preview_algo_order()` 只从大 QMT 模型上下文读取当前买卖五档、最新价、昨收、
涨跌停价、最小价位以及账户资金或可用持仓，并返回实时涨跌幅和动态价格笼子；
不连接外部 `xtdata`，不写数据库且不调用 `passorder`。
`place_algo_order()` 会重新读取即时盘口并执行，所以预览与实单的价格和数量可能
随行情变化。

普通批量下单直接复用单笔委托接口。`interval_ms` 表示相邻两笔调用开始时间的
最小间隔，整批始终串行提交：

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

with QmtClient() as client:
    receipts = client.place_orders(
        orders,
        interval_ms=50,
        wait_for="LOCAL_ACK",
        timeout=10.0,
    )
```

查询当日新股新债、账户申购额度并申购：

```python
from qmt_adapter import NewIssueSubscriptionRequest, QmtClient


with QmtClient() as client:
    issues = client.list_new_issues("ALL")
    quota = client.get_new_issue_quota("YOUR_ACCOUNT_ID")
    receipt = client.subscribe_new_issue(
        NewIssueSubscriptionRequest(
            account_id="YOUR_ACCOUNT_ID",
            instrument="申购代码.SH",
            issue_type="STOCK",  # 新债使用 BOND
            quantity=1000,
            remark="ipo-example",
        ),
        wait_for="BROKER_ID",
    )
```

逆回购按人民币金额填写，必须为1000元整数倍；Bridge 会按每张100元标准券
转换成 QMT 使用的张数，因此1000元对应 `volume=10`：

```python
from qmt_adapter import QmtClient, ReverseRepoRequest


with QmtClient() as client:
    receipt = client.place_reverse_repo(
        ReverseRepoRequest(
            account_id="YOUR_ACCOUNT_ID",
            instrument="204001.SH",
            amount=10000,
            annual_rate="1.80",
            remark="repo-example",
        ),
        wait_for="BROKER_ID",
    )
```

申购价由 QMT 当日发行数据确定；接口不会自动选择标的或按额度满额申购。
逆回购和申购均复用普通委托的 `client_order_id`、查询、撤单和幂等规则。

## Asyncio 客户端

`AsyncQmtClient` 的交易和查询使用一个串行工作线程；全推行情使用独立管道和独立
事件线程。等待 `get_quote_event()` 不会阻塞同一客户端提交交易命令，QMT API
调用本身仍在QMT主线程串行执行。

```python
import asyncio

from qmt_adapter import AsyncQmtClient, OrderRequest


async def main():
    async with AsyncQmtClient() as client:
        order = OrderRequest(
            account_id="YOUR_ACCOUNT_ID",
            instrument="600000.SH",
            side="BUY",
            quantity=100,
            price_type="COUNTERPARTY",
            remark="async-example",
        )
        receipt = await client.place_order(
            order, wait_for="BROKER_ID", timeout=10.0
        )
        print(receipt.client_order_id, receipt.qmt_order_id)


asyncio.run(main())
```
