# 辅助脚本

以下脚本用于部署验证或专项测试，不属于 QMTAdapter 的公共 API。命令均从仓库根
目录执行。

## 账户和持仓验证

`query_account.py` 只读取 Bridge 状态、账户资金和持仓，不会提交委托：

```powershell
.\.venv\Scripts\python.exe .\scripts\query_account.py --account-id YOUR_ACCOUNT_ID
```

## 单笔真实下单

`place_stock_order.py` 会向当前 QMT 资金账号提交真实委托，运行时必须填写显式确认
字符串。脚本无法判断当前账号是模拟账号还是真实账号，完成只读验证前不要运行。

参数说明：

```powershell
.\.venv\Scripts\python.exe .\scripts\place_stock_order.py --help
```

## 调用耗时测试

`stress_test_calls.py` 对比同步和异步客户端的只读调用耗时，不会提交委托：

```powershell
.\.venv\Scripts\python.exe -u .\scripts\stress_test_calls.py --mode both --command account --count 50 --interval-ms 50
```

## 真实委托压力测试

`stress_test_orders.py` 会连续提交真实委托，不是单元测试。运行前先查看参数和确认
机制：

```powershell
.\.venv\Scripts\python.exe .\scripts\stress_test_orders.py --help
```

## QMT 账号发现

`qmt_account_discovery_qmt.py` 需要直接放入大 QMT 运行，用于检查模型上下文能够读取
到的账号信息。

## QMT Level-2 能力探测

`qmt_l2_probe_qmt.py` 需要直接放入大 QMT 运行。它只检查当前客户端和行情权限能够
提供哪些 Level-2 数据，不调用账户、下单、撤单或数据库接口，也不属于 Adapter
运行组件。
