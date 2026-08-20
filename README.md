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
- 使用 SQLite 持久化客户端委托 ID 与 QMT 委托 ID 的映射；
- 以 `client_order_id` 为唯一逻辑标识的持久化幂等下单。

`client_order_id` 是委托的唯一逻辑标识。同一个 `client_order_id` 携带完全
相同的规范化委托参数再次提交时，Adapter 会返回已保存的委托，不会再次调用
`passorder`；如果参数不同，则拒绝请求。对外接口不再提供单独的
`idempotency_key`。

当前暂不支持信用交易、期货、期权、算法任务、组合交易、行情或多个客户端并发接入。

## 文件说明

- `qmt_side/qmt_adapter_qmt.py`：作为 QMT Python 模型导入或粘贴到 QMT 中运行。
- `qmt_adapter/`：外部同步客户端和 asyncio 客户端，均不依赖或导入 QMT 模块。
- `scripts/query_account.py`：只读的账户和持仓验证脚本。
- `scripts/place_stock_order.py`：带安全检查的模拟账户下单脚本；完成只读验证前不要使用。
- `scripts/stress_test_calls.py`：只读的同步/异步调用耗时对比脚本。
- QMT 配置文件：QMT 安装目录下的 `userdata/qmt_adapter/bridge_config.json`。

## 第一步：验证账户和持仓

1. 保持 `trading_enabled` 为 `false`。
2. 在 `bridge_config.json` 的 `accounts` 数组中填写需要验证的股票模拟资金账号。
3. 在 QMT 模型研究中使用 `qmt_side/qmt_adapter_qmt.py` 新建 Python 模型并运行。
4. 在本仓库目录中执行：

```powershell
.\.venv\Scripts\python.exe .\scripts\query_account.py --account-id YOUR_ACCOUNT_ID
```

返回结果有意保留 `raw` 原始对象，供首次实机检查 QMT 字段使用。v1 规范化字段只使用
QMT 手册明确列出的 `m_dAvailable`、`m_strInstrumentID` 和 `m_nVolume`。

## 交易验证

检查账户和持仓返回结果前，应保持交易功能关闭。启用交易并选择模拟委托，应作为单独且明确的操作进行。

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
