# 辅助脚本

以下脚本用于部署验证或专项测试，不属于 QMTAdapter 的公共 API。命令均从仓库根
目录执行。

## Python 3.12 迁移探针

将 `qmt_python312_probe_qmt.py` 全文复制到大 QMT 的**新建独立策略**，选择内置
Python 3.12。脚本为纯 ASCII/GBK 入口，也可在 3.6 运行以形成对照。
默认不绑定或自动发现账户；填写 `ACCOUNT_ID` 后查询资金、持仓、委托、成交。
它不会调用下单、撤单、转账、申购或清理函数，不启动 Adapter。

先填写 `QMT_VERSION_LABEL`（关于界面的完整版本）及 `ACCOUNT_ENVIRONMENT`。
`ENABLE_CALLBACKS=True` 才调用 `set_account` 被动采集回调；每种最多保存三条样本。
历史交易查询需同时填写 `HISTORY_START`、`HISTORY_END`（YYYYMMDD）。
查询在 `after_init` 或首次 `handlebar` 执行一次，不依赖定时线程。

日志输出 `QMT312_PROBE_REPORT=...` 给出 TEMP 目录中的 JSONL 报告位置。
写文件被限制时会保留错误并将记录输出到 QMT 日志；不会绕过宿主限制。
账号、账号 key、投资备注等会脱敏，报告仍包含资金和持仓样本，勿提交 Git。
完整说明见 [迁移验证说明](../docs/python312-migration-validation.md)。

## Python 3.12 隔离只读桥接验证

在源码目录运行 `py -3.11 scripts/prepare_python312_readonly_test.py`，会在TEMP下生成独立部署及短加载器。
加载器开头填写 `TEST_ACCOUNT_ID`，复制到新的QMT策略，选择Python3.12，并在策略交易界面以交易模式启动。
该入口带测试专用命令及原生函数白名单，拒绝下单、撤单、算法提交及清理；原生行情订阅仅请求三个指数。
它发现同进程旧Adapter实例仍存在时会拒绝启动，需先停止旧策略；不会自动停止或替换旧实例。

保持测试策略运行，在外部终端执行：

```powershell
py -3.11 scripts/python312_readonly_smoke.py --config "生成目录\config\bridge_config.json"
```

脚本校验专用测试标记与实际运行时身份，再检查只读接口、多连接、初始快照及订阅期间命令响应。
输出仅保留运行时、状态、数量和日期，不输出账号、token或资金/持仓数值。它不会改动生产部署。
交易生命周期、实时DELTA及全市场压力不属于这轮只读测试。

若连接返回 `WinError 5`，先核对客户端与QMT的Windows运行身份和权限级别。
本机实测中普通进程被拒绝，而经Windows标准UAC启动的管理员Python3.11客户端完成全部18项检查。
Codex离开沙箱并不等于Windows管理员提权；不要通过放宽管道ACL解决该问题。

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
