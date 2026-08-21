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
- 单只及批量证券行情查询，可选返回QMT原始行情；
- 普通现金账户股票买入和卖出；
- 北交所普通股票委托（买入不少于100股，之后可按1股递增）；
- 按指定时间间隔串行批量下单；
- 沪深北交易所原生股票市价申报（按各市场支持的申报类型校验）；
- 沪深交易所国债逆回购；
- 当日新股、新债发行信息及账户申购额度查询；
- 新股、新债申购；
- 委托查询和撤单；
- 当日成交查询，可选择仅查询本Adapter委托或查询账户全部成交；
- 由QMT委托/成交回报驱动的单笔和批量委托状态等待；
- 按盘口流动性拆分并按固定间隔提交大额股票委托，支持预览、提交、查询和撤单。

## 文件说明

- `qmt_side/qmt_adapter_qmt.py`：由部署命令更新到固定目录的大 QMT 端完整脚本。
- `qmt_adapter/`：外部同步客户端和 asyncio 客户端，均不依赖或导入 QMT 模块。
- `scripts/query_account.py`：只读的账户和持仓验证脚本。
- `scripts/place_stock_order.py`：需要显式确认字符串的单笔真实下单脚本；它不会判断
  当前账号是否为模拟账户，完成只读验证前不要使用。
- `scripts/stress_test_calls.py`：只读的同步/异步调用耗时对比脚本。
- `scripts/stress_test_orders.py`：会真实连续提交50笔委托的压力测试脚本，不是单元测试。
- `scripts/qmt_l2_probe_qmt.py`：独立的只读大QMT L2能力探针，不属于Adapter运行组件。
- `docs/`：完整函数调用说明和算法委托设计文档。
- 默认部署根目录：`C:\QMTAdapter`，不依赖券商软件的安装目录。

## 安装与部署

当前仓库可以作为 Python 包安装到虚拟环境：

```powershell
.\.venv\Scripts\python.exe -m pip install -e .
```

首次部署时提供股票资金账号：

```powershell
.\.venv\Scripts\qmt-adapter.exe deploy --account-id YOUR_ACCOUNT_ID
```

命令会创建：

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

以上均为与券商软件安装目录无关的绝对路径。部署命令首次创建
`C:\QMTAdapter\config\bridge_config.json` 并生成随机鉴权令牌；配置中的
`db_path` 固定为 `C:\QMTAdapter\data\bridge.db`。数据库不会由部署命令预先
创建，而是在 QMT Bridge 第一次启动时创建。启用 WAL 后，SQLite 运行期间还可能
生成同目录的 `bridge.db-wal` 和 `bridge.db-shm`。

第一次使用时，在大 QMT“模型交易”中新建一个 Python 策略，把
`C:\QMTAdapter\qmt_adapter_loader.py` 的完整内容复制进去。该加载器只在模型
启动时读取并执行一次外部 Bridge 脚本；之后 QMT 直接调用已经加载的函数，
不会在行情回调、查询或下单时重复读取文件。

升级时再次执行 `qmt-adapter deploy`。命令只更新
`C:\QMTAdapter\runtime\qmt_adapter_qmt.py` 和加载器，并从旧配置中删除已经废弃的
`environment` 字段；账号、鉴权令牌、数据库路径以及 `data` 目录中的文件均保持
不变。更新后停止并重新启动 QMT 策略，使其加载新代码。

## 第一步：验证账户和持仓

1. 确认 `C:\QMTAdapter\config\bridge_config.json` 中的账号正确。
2. 在 QMT 模型交易中启动已经复制短加载器的 Python 策略。
3. 在本仓库目录中执行：

```powershell
.\.venv\Scripts\python.exe .\scripts\query_account.py --account-id YOUR_ACCOUNT_ID
```

脚本返回Bridge状态、账户资金和标准化持仓。账户项目包含QMT原始字段；持仓默认
只返回标准字段，需要排查QMT原始持仓对象时可直接调用
`client.list_positions(account_id, include_raw=True)`。

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

`AsyncQmtClient` 使用一个工作线程和一条命名管道连接。调用按提交顺序串行执行，
不会并行调用 QMT API。

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

只读调用耗时对比：

```powershell
.\.venv\Scripts\python.exe -u .\scripts\stress_test_calls.py --mode both --command account --count 50 --interval-ms 50
```
