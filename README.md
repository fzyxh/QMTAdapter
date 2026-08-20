# QMTAdapter

由于交易接入环境变化，许多原本运行于 MiniQMT 的量化策略需要迁移至大 QMT。为了降低迁移及后续维护成本，同时保持策略代码的独立性和灵活性，本项目提供了一套基于大 QMT 的交易适配方案。

项目由大 QMT 端桥接脚本和外部 Python 客户端库组成。策略侧无需导入或直接调用 QMT Python API，即可通过统一接口完成账户查询、持仓查询、股票委托、委托查询和撤单等操作。

欢迎试用。如遇问题或有改进建议，请提交 Issue。

## 当前实现（截至 2026-08-20）

- 股票账户查询；
- 股票持仓查询；
- 普通现金账户股票买入和卖出；
- 沪深交易所原生股票市价申报；
- 委托查询和撤单；
- 盘口流动性加权拆单：父单预览、提交、查询和整体撤单；
- 子单超时后先撤单；确认旧子单已经成交、撤销或被拒绝，不会再继续成交后，
  再按剩余未成交数量重新读取盘口并计算下一轮委托；
- 使用 SQLite 持久化客户端委托 ID 与 QMT 委托 ID 的映射；
- 以 `client_order_id` 为唯一逻辑标识的持久化幂等下单。
- 以 `algo_order_id` 为唯一逻辑标识的算法父单幂等和父子单持久化。

`client_order_id` 是委托的唯一逻辑标识。同一个 `client_order_id` 携带完全
相同的规范化委托参数再次提交时，Adapter 会返回已保存的委托，不会再次调用
`passorder`；如果参数不同，则拒绝请求。对外接口不再提供单独的
`idempotency_key`。

`TWAP`、`VWAP` 已保留统一算法标识和请求结构，但尚未实现，调用时会明确返回
`ALGORITHM_NOT_IMPLEMENTED`，不会产生子单。

当前暂不支持信用交易、期货、期权、组合交易、行情订阅或多个客户端并发接入。

## 文件说明

- `qmt_side/qmt_adapter_qmt.py`：由部署命令更新到固定目录的大 QMT 端完整脚本。
- `qmt_adapter/`：外部同步客户端和 asyncio 客户端，均不依赖或导入 QMT 模块。
- `scripts/query_account.py`：只读的账户和持仓验证脚本。
- `scripts/place_stock_order.py`：带安全检查的模拟账户下单脚本；完成只读验证前不要使用。
- `scripts/stress_test_calls.py`：只读的同步/异步调用耗时对比脚本。
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
`C:\QMTAdapter\runtime\qmt_adapter_qmt.py` 和加载器，不覆盖 `config` 与
`data` 目录中的配置、数据库和 WAL 文件。更新后停止并重新启动 QMT 策略，
使其加载新代码。

## 第一步：验证账户和持仓

1. 确认 `C:\QMTAdapter\config\bridge_config.json` 中的账号正确。
2. 在 QMT 模型交易中启动已经复制短加载器的 Python 策略。
3. 在本仓库目录中执行：

```powershell
.\.venv\Scripts\python.exe .\scripts\query_account.py --account-id YOUR_ACCOUNT_ID
```

返回结果有意保留 `raw` 原始对象，供首次实机检查 QMT 字段使用。v1 规范化字段只使用
QMT 手册明确列出的 `m_dAvailable`、`m_strInstrumentID` 和 `m_nVolume`。

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
