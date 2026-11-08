# Python 3.12 迁移研究与验证

2026-11-08 更新。**v0.10.0 已完成 QMT 真实宿主只读桥接联调，模拟账户交易生命周期等验收仍待完成。**

## 基线与范围

本次从 v0.9.0（e0640c3）升级到 v0.10.0，使用命名管道协议 9。

QMT 宿主限定 CPython 3.12.x / 64 位，SDK 最低 Python 3.11，验证 3.11/3.12。
不引入 XtQuant、不更改交易算法或费用精度，不操作生产部署或真实账户。

## 查到的资料与结论

以下是 2026-11-06 查阅的一手资料。未找到能完整区分各券商 3.6/3.12 原生 API 的官方差异表。
不能把“解释器升级”直接解释为“所有交易字段都改名”，也不能用 MiniQMT 的支持范围代替内置 Python。

| 项目 | 有资料支持的事实 | 本次处理 |
| --- | --- | --- |
| 内置 Python 文档 | [现行知识库相关说明](https://knowledge.thinktrader.net/docs/python/related_instructions.html)仍写 3.6.8/64 位，存在文档版本滞后 | 在策略内部读取 `sys.version`、实现和位数；客户端版本由关于界面填写 |
| 交接中的公告 | [2026-10-15 帖子](https://www.xuntou.net/forum.php?mod=viewthread&tid=6928)主题是策略配置存取，当前可抓取正文没有给出完整的 3.12 API/字段变更表 | 不用该链接独立推断所有券商的最低可用版本；以本机报告补证 |
| 费用字段 | [官方数据结构](https://dict.thinktrader.net/innerApi/data_structure.html)将 `m_dCommission` 的旧拼写记为 `m_dComission`；[官方回调示例](https://dict.thinktrader.net/innerApi/callback_function.html)又实际展示 `m_dComssion` | 三种拼写分别采样，不相加；目前 Adapter 优先 `m_dComssion`、回退 `m_dCommission` 的逻辑保持，第三种是否必要待实测 |
| 历史交易 | [官方交易函数](https://dict.thinktrader.net/innerApi/trading_function.html)记载 `get_history_trade_detail_data(accountID, accountType, dataType, startDate, endDate)`，日期 YYYYMMDD，返回结构含时间标识及对象 | 探针先记录存在性，填写日期后才调用；缺失记 MISSING/NameError，异常记类型和原因，不变成空列表 |
| 行情结构 | [官方行情函数](https://knowledge.thinktrader.net/docs/python/data_function.html)记载 `get_market_data_ex` 返回代码到 DataFrame 的映射，并提示不建议在 init 中使用 | 探针在 after_init/首次 handlebar 查询；记录列、dtype、索引类型、时间样本。使用 subscribe=False，只读本地少量 1m/1d 数据 |
| 快照和回调 | [官方数据结构](https://knowledge.thinktrader.net/docs/python/data_structure.html)分别定义快照、历史和订阅对象 | 不凭名称相似直接统一不同对象；探测沪深北指数盘口、字段与原始值 |
| 账户查询与回调 | [官方常见问题](https://knowledge.thinktrader.net/docs/python/question_answer.html)说明查询读取客户端缓存，回调需要绑定账号，策略线程不可阻塞 | 保持 QMT 主线程调度；被动回调无事件不判失败，不为制造回报下单 |
| Python 移除项 | [Python 3.12 变更](https://docs.python.org/3.12/whatsnew/3.12.html)移除 imp、distutils 等旧接口 | 审计 Adapter 直接导入与调用，未发现依赖这些移除项；不复制 3.6 的二进制库 |
| SQLite 事务 | [Python 3.12 sqlite3](https://docs.python.org/3.12/library/sqlite3.html#transaction-control)新增 autocommit，旧行为对应 LEGACY_TRANSACTION_CONTROL | 显式固定旧事务模式及 isolation_level，验证提交、回滚与 executescript 的隐式提交 |

未根据网上第三方包列表硬编码 pandas/NumPy 版本；由探针导入券商自带库后记录实际版本及错误。
未把 `m_nOrderStrategyType` 的异常整数用于策略归属。资金回调数量不能当资金变动次数。
费用小数原样记录，仅生产协议保持既有舍入行为。

## 已实施的适配

- 按用户确认的升级方式，正式加载器沿用 v0.9.0 的纯 ASCII、`# coding: gbk` 模板，通过二进制读取和 compile 加载 UTF-8 主体。原路径升级可保留编辑器中已有的策略代码。
- 主体 init 在访问共享实例、停止旧实例、读配置、打开数据库或启动监听前检查宿主。
- 保持同一 globals 执行主体，让 QMT 入口、定时器和回调可见。
- 新增 `health().runtime`，版本来自运行代码和实际解释器；ready 日志也不再使用可能过期的配置版本。
- SQLite 使用 WAL、FULL、5000ms busy timeout，事务语义保持。没有新增 schema 或数据清理。
- SDK 和部署工具可由 3.11 执行，正式宿主检查由主体的策略入口 `init` 执行。
- 保持命令/行情两个管道、句柄类型声明、取消 I/O、线程退出后关闭句柄的既有规则；没有改 ACL。
- 修复初始化尚未创建实例及重复停止时，清理不存在的共享槽可能抛出 AttributeError 的问题，保留原始启动错误。
- 发布工作流增加 3.11/3.12 全套测试与跨解释器进程验证；工作流尚未在 GitHub 执行。

## 用户本机 QMT 测试：先运行探针

1. 新建独立 Python 策略，把 [`qmt_python312_probe_qmt.py`](../scripts/qmt_python312_probe_qmt.py)
   **全文**复制进去。保留第一行 GBK 声明；文件本身全为 ASCII。
2. 设置内置 Python 为 3.12。填写 `QMT_VERSION_LABEL`（券商名称与关于界面的完整版本号）。
   `ACCOUNT_ENVIRONMENT` 填 `SIMULATION`、`REAL` 或 `UNKNOWN`，该标签只用于记录，不会切换账户环境。
3. 第一轮可保留 `ACCOUNT_ID = ""`，只验证宿主、库和行情；需要查询账户时，在本机填账号及
   `ACCOUNT_TYPE = "STOCK"`。不读取 Adapter 配置，不自动发现账号，也不会输出完整账号。
4. 用策略运行模式启动，避免把回测结果当实际交易环境证据。默认回调开关为 False。
   要观察被动回调才将 `ENABLE_CALLBACKS = True`，脚本仅绑定填写的账号，每类最多记录三条样本。
5. 要测历史交易，同时填写 `HISTORY_START` 和 `HISTORY_END`，如 `20261105` / `20261106`。
   日期留空时明确记 SKIPPED。`query_bank_transfer_stream` 仅探测存在性/签名，未调用未验证的签名。
6. 等待日志出现 `query_phase_complete`。若启用了回调，可继续运行约 30 秒再停止；盘后缺少回调是未覆盖。
7. 日志 `QMT312_PROBE_REPORT=...` 给出 TEMP 目录下的 `qmt312_probe_<随机ID>.jsonl`。
   如文件写入被限制，保留错误并复制以 `QMT312_PROBE ` 开头的日志；不调整权限或绕过限制。
8. 如果旧 3.6 仍可安全运行，同一脚本、同一输入再跑一份，用实际字段及类型对照。

探针会脱敏账号、账号 key、投资备注和委托标识。原始报告仍含资金、持仓及成交价格样本，请在本机保存。

成功返回空数据只证明本次查询未返回样本；不能证明历史权限、行情可用性或回调完整性。
每类交易对象采样前三条，不能据此断言其他证券类型具有完全相同的字段。
探针不会安装依赖、调用交易写接口、创建 Adapter 管道或打开交易数据库。

## 真实宿主验证

验证环境为东北证券 NET 2.1.33.0。策略内部报告 CPython 3.12.10 / 64 位、SQLite 3.49.1、NumPy 2.4.4 和 pandas 3.0.2。

| 检查 | 实测结果 |
| --- | --- |
| 协议与运行时 | Python 3.11.9 客户端通过协议 9 从宿主读取到 0.10.0 的运行身份。 |
| 只读接口 | 0.10.0 SDK 与原版 0.9.0 SDK 各通过 18 项检查，覆盖账户、持仓、两种成交 scope、委托列表、行情、合约、板块、发行数据、申购额度和历史 K 线。 |
| 双管道 | 8 个命令连接和 8 个行情连接同时工作，各行情客户端收到三个指数的初始快照。订阅期间命令查询正常。 |
| 错误处理 | 未配置账户的查询返回 `ACCOUNT_NOT_ALLOWED`，随后服务健康状态为 `OK`。Windows 客户端与 QMT 的权限级别需要匹配。 |
| 原生字段 | 成交样本同时提供 `m_dCommission` 和 `m_dComssion`。K 线保留 `settelementPrice` 拼写，时间索引为字符串。该宿主未注入 `get_history_trade_detail_data`。 |

可按[只读测试脚本说明](../scripts/README.md)生成独立测试目录，使用其中的测试加载器和外部验证脚本复现。测试入口限制交易写请求，并将行情订阅限制为三个指数。正式策略升级时沿用原加载器。

## 离线验证与复现

已用 v0.9.0 tag 中的生成函数核对正式加载器。在相同部署路径下，新旧模板生成的字节完全相同，重复部署保留加载器的修改时间。
使用券商配套解释器在独立进程中运行原版加载器，3.12.10 成功加载 0.10.0 主体，3.6.8 在主体 `init` 开头报告宿主版本错误。两次加载检查后均未创建数据库。

两套 Python 都需 64 位。独立安装路径可通过 `QMT_TEST_PYTHON311` / `QMT_TEST_PYTHON312` 指定；
未指定时使用当前解释器或 Windows `py` 启动器查找。缺少所需解释器时跨进程测试明确失败，不静默跳过。

```powershell
py -3.11 -m unittest discover -s tests -v -b
py -3.12 -m unittest discover -s tests -v -b
git diff --check
```

本机测试宿主使用 Python.org 官方 [3.12.10 Windows embeddable 64-bit](https://www.python.org/downloads/release/python-31210/)，
放在临时目录，已核对官网归档校验值及可执行文件 PSF 有效签名。其 SQLite 为 3.49.1。
这项独立假宿主测试本身不证明 QMT 内部版本；上节用户探针才是内部版本证据。不得据此替换券商运行库。

测试覆盖旧宿主拒绝且不触碰旧实例、加载器编码边界、运行身份、SQLite 事务、部署保留配置/数据、
订单幂等与重启、既有协议/行情/算法路径，以及十轮停止重启。十轮测试每轮连接8个命令及8个行情客户端，
验证停止后无后台 QMT 线程，释放测试对象后进程句柄数不持续增长。

跨进程测试启动真实 3.12 FakeQmtApi 服务，再由3.11客户端通过 Windows 命名管道查询。
另从 v0.9.0 tag 提取原版 SDK 到临时目录，通过 `QMT_TEST_CLIENT_SDK_ROOT` 和
`QMT_TEST_CLIENT_SDK_VERSION=0.9.0` 验证旧 SDK 读取新增 runtime 字段仍可工作。
这没有运行 BridgeQMT 1.4.2 产品本身。

2026-11-08本机结果：Python 3.11 **133 项通过**，券商配套Python 3.12.10独立进程 **133 项通过**，均无跳过。
新增覆盖测试部署隔离、写请求拒绝、旧实例保留、原生订阅限量、完整只读冒烟脚本，以及初始化失败/重复停止。
旧版 0.9.0 SDK 独立跨进程检查 **1 项通过**。完整输出为 `dist/tests-python311.txt`、
`dist/tests-python312.txt`。本地构建产物的校验值见 `dist/SHA256SUMS.txt`。

## 上线前仍需完成

- 补充盘中数据新鲜度、特殊证券、除权样本及实时回调验证。
- 先确认独立模拟实例，使用独立目录、两个独立管道；同进程内全局 `_RUNTIME_SLOT` 也会替换旧实例，换目录不构成隔离。
- 保存现有配置/token/数据库的一致备份；不直接复制正在写的 WAL 数据库，不删除 WAL/SHM。
- 已在真实QMT完成独立只读部署、新旧SDK各18项联调及默认8+8客户端验证；生产配置/数据库升级及真实宿主停止重启恢复仍需验证。
- 用户确认的模拟账户交易生命周期、错误恢复、权限等级及慢行情消费者验证；不得用真实委托脚本作默认验收。
- 使用 BridgeQMT 1.4.2 产品完成联调，并按上述项目记录生产验收结果。

若券商安全策略禁止主体文件加载、SQLite 或命名管道，保留原始错误并确认受支持的接入方式；
不替换 DLL、不关闭限制、不转用未经允许的绕过方式。
