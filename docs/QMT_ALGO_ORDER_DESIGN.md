# QMT Adapter 算法委托设计

> 实现状态：`BOOK_LIQUIDITY_WEIGHTED` 已实现；`TWAP`、`VWAP` 仅预留统一
> 标识和扩展接口，尚未实现。

## 1. 设计边界

算法委托仍运行在现有的单个大 QMT 模型 `QMTADAPTERBRIDGE` 中，不为每种算法
创建新模型。短加载器只负责启动时装载 Bridge，不参与每次拆单和下单。

执行算法与报价类型分开：

- `algorithm` 决定一个逻辑父单如何生成、调度和重试子单；
- `price_type` 决定一笔普通子单如何向交易所申报；
- `STOCK_PRICE_TYPES` 只保存 QMT/交易所报价类型；
- `BOOK_LIQUIDITY_WEIGHTED`、`TWAP`、`VWAP` 保存于
  `STOCK_EXECUTION_ALGORITHMS`；
- 当前盘口算法生成的子单全部是 `LIMIT` 委托。

这种边界允许后续增加 TWAP/VWAP，而不修改普通订单的价格类型语义，也不需要
增加第二套父子单协议。

## 2. 公开模型和命令

外部请求统一使用 `AlgoOrderRequest`：

```python
AlgoOrderRequest(
    account_id="YOUR_ACCOUNT_ID",
    instrument="601919.SH",
    side="BUY",
    algorithm="BOOK_LIQUIDITY_WEIGHTED",
    target_amount="10000000",  # 与 quantity 二选一
    quantity=None,
    params={},
    remark="",
    algo_order_id="稳定且唯一的父单ID",
)
```

Bridge 命令：

| 命令 | 是否调用QMT交易函数 | 作用 |
|---|---|---|
| `algo_order.preview` | 否 | 读取盘口和账户/持仓，生成计划但不持久化、不下单 |
| `algo_order.place` | 是 | 持久化完整计划，立即提交第一笔，再按设定间隔逐笔提交其余子单 |
| `algo_order.get` | 否 | 查询父单、所有轮次子单及累计成交 |
| `algo_order.list` | 否 | 列出 Adapter 的算法父单 |
| `algo_order.cancel` | 是 | 停止算法并撤销所有当前可撤子单 |

同步和异步客户端公开同名语义的方法。两者都会在 Bridge 接受完整计划并开始调度
后返回，不等待全部子单提交或成交。`QmtClient` 在等待该回执时阻塞当前线程；
`AsyncQmtClient` 不阻塞调用方的 asyncio 事件循环。它们不会改变 QMT 端的调度
方式，Bridge 仍只在 QMT 主线程中逐笔调用 `passorder`。

## 3. 五档行情规范

QMT 端只使用模型上下文中的 `ContextInfo.get_full_tick([instrument])` 和
`ContextInfo.get_instrument_detail(instrument)`，不连接外部 `xtdata`。前者
提供最新价、昨收和买卖五档，后者提供 `PreClose`、`UpStopPrice`、
`DownStopPrice`、`PriceTick` 与交易状态。规范化结果的每档包含：

```python
{
    "level": 1,
    "price": "13.00",
    "volume_lots": 1000,
    "visible_quantity": 100000,
}
```

Adapter 把 `askVol`/`bidVol` 解释为手，并按股票每手100股转换为
`visible_quantity`。预览的 `depth.quote.ask_levels` 和
`depth.quote.bid_levels` 固定返回完整五档槽位，并同时返回最新价、昨收、实时
涨跌幅、当日涨跌停价和动态价格笼子。如果没有有效对手盘价格，命令返回
`MARKET_DATA_UNAVAILABLE`，不会生成追价单。

## 4. 盘口流动性加权拆单

### 4.1 默认参数

| 参数 | 默认值 |
|---|---:|
| 大单阈值 | 1,000,000元 |
| 子单参考最小金额 | 10,000元 |
| 子单最大金额 | 500,000元 |
| 加权档位 | 前3档 |
| 最大可见档位 | 前5档 |
| 追价 | 最后一档外2个最小价位 |
| 子单提交间隔 | 50毫秒 |
| 子单超时 | 20秒 |
| 最大重试 | 3轮（不含初始轮次） |

### 4.2 目标金额换算

明确传入 `quantity` 时，目标股数必须是100股整数倍。

传入 `target_amount` 时，先用对手一档估算整手数量上界，再通过二分搜索选择
满足以下条件的最大整手数量：

```text
全部子单的 价格 × 数量 之和 <= target_amount
```

因此买入不会因为深档价格较高而突破目标金额。金额是上限，不保证最终成交金额
精确等于该值。

### 4.3 小单

按计划金额判断。未超过大单阈值时，全部数量使用对手一档价格；如果总金额超过
单笔最大金额，仍按单笔上限均匀拆成多笔。单笔上限对小单、可见盘口子单和追价
子单全部生效。

### 4.4 大单前三档分配

设目标整手数为 `T`，前三档可见整手数为 `V1,V2,V3`。先确定前三档最多可承接：

```text
D = min(T, V1 + V2 + V3)
```

每档以同一个原始值 `D` 计算，而不是对递减余额重复乘权重：

```text
Ai = floor(D × Vi / (V1 + V2 + V3))
```

整手取整产生的余数按小数余量从大到小补足，同时不超过各档可见数量。这样
50%/30%/20%的盘口会得到50%/30%/20%，不会出现原实现的50%/15%/7%。

低于 `min_child_notional` 的小档分配不直接丢弃，其数量返回剩余量，后续由更深
档或追价档承接。

### 4.5 第四、五档和追价

前三档容量不足时，按价格优先顺序使用第四、五档，各档不超过显示数量。五档
仍不足的全部剩余数量进入追价档：

- 买入：最后有效卖档价 `+ chase_ticks × PriceTick`；
- 卖出：最后有效买档价 `- chase_ticks × PriceTick`。

连续竞价阶段的价格笼子以大 QMT 的对手一档为优先基准，并按交易所的
“2%或十个最小价位孰宽”规则计算。追价同时受当日涨跌停价和动态价格笼子
约束，截价来源分别标记为 `CHASE_DAILY_LIMIT_CLAMPED` 或
`CHASE_PRICE_CAGE_CLAMPED`；无法取得相关输入时拒绝生成计划。

追价数量同样按 `max_child_notional` 均匀切分。所有阶段结束后执行硬校验：

```text
sum(child.quantity) == resolved_quantity
child.quantity % 100 == 0
child.price × child.quantity <= max_child_notional
```

任何一项失败都返回 `PLAN_INVALID`，在进入 `passorder` 前终止。

## 5. 交易前资源检查

买入读取账户 `m_dAvailable`，要求当前计划金额不超过可用资金。卖出按证券代码
读取持仓 `m_nCanUseVolume`，要求计划数量不超过可用数量；不会用总持仓
`m_nVolume` 替代可用数量。

`preview` 与 `place` 分别读取即时数据。`place` 不信任较早的预览结果，会重新
执行盘口、目标数量、总量守恒、金额和资金/持仓检查。

## 6. 父子单身份和持久化

父单以 `algo_order_id` 作为逻辑身份和幂等键。同一 ID 与同一规范化负载重放时
返回原父单；同一 ID 携带不同参数返回 `ALGO_ORDER_ID_CONFLICT`。

每个子单 ID 可确定重建：

```text
{algo_order_id}:a{attempt}:c{child_index}
```

例如 `parent-1:a0:c3` 表示父单 `parent-1` 初始轮次的第4个子单。子单继续复用
普通 `order.place` 的持久化幂等、QMT备注哈希关联和 `m_strOrderSysID` 回报绑定。

SQLite 表：

- `algo_orders`：父单请求指纹、目标、解析数量、当前轮次、累计成交、状态和参数；
- `algo_children`：父单、轮次、序号与普通 `client_order_id` 的确定性映射；
- `orders`：实际子单、QMT 委托号和原始回报。

新父单在一个 SQLite 事务中写入父记录和全部初始子单计划，提交成功后才调用
`passorder`。第一笔可立即提交，后续每次 QMT 定时回调最多提交一笔，并保证相邻
两次调用至少间隔 `child_interval_ms`。这里不会在 QMT 主线程中调用 `sleep`。
新重试轮次也在一个事务中写入全部子单并切换当前轮次。SQLite 只用于 QMT 端
持久化；实时请求和结果仍走全双工命名管道。Bridge 重启后会从尚未提交的子单
继续，不会重新提交已经写入普通委托表的子单。

## 7. 状态和成交聚合

普通子单状态按 QMT 状态码标准化：

| QMT状态 | Adapter状态 |
|---:|---|
| 48、49、50 | `SUBMITTED` |
| 51、52 | `CANCEL_PENDING` |
| 53、54 | `CANCELED` |
| 55 | `PARTIALLY_FILLED` |
| 56 | `FILLED` |
| 57 | `REJECTED` |
| 255 | `UNKNOWN` |

父单累计成交量是所有轮次、所有子单 `m_nVolumeTraded` 的总和。只有累计成交量
达到父单解析数量时才进入 `FILLED`，不会由任一子单单独完成父单。

父单主要状态：

```text
PLACING -> WORKING -> FILLED
                  -> RETRY_CANCELING -> PLACING
                  -> CANCELING -> CANCELED
                  -> FINAL_CANCELING -> FAILED
                  -> UNKNOWN
```

## 8. 超时撤单和重算

当前轮次达到 `timeout_seconds` 后：

1. 父单进入 `RETRY_CANCELING`；
2. 对当前可撤子单调用 `can_cancel_order` 和 `cancel`；
3. 等待 QMT 确认每笔旧子单已经成交、撤销或被拒绝，不会再继续成交；
4. 累加所有轮次的真实成交数量；
5. 计算 `remaining = resolved_quantity - filled_quantity`；
6. 重新读取五档，只为 `remaining` 生成新轮次；
7. 持久化完整新轮次后再提交子单。

`timeout_seconds` 从当前轮次最后一笔子单提交后开始计算，子单排队等待发送的时间
不计入挂单超时。用户在提交过程中撤销父委托时，尚未提交的子单不会继续发送。

所有旧子单的结果确认前绝不发下一轮，因此不会因旧单撤单未确认而按原数量
重复下单。
买入金额父单的重试还会用“子单限价 × 已成交数量”计算保守已用金额上界；新
计划会突破原始金额时返回 `TARGET_AMOUNT_EXHAUSTED` 并停止。

用户调用 `cancel_algo_order` 时使用同一撤单流程，但不会生成下一轮。重复调用
整体撤单是允许的。

## 9. 不确定边界

`passorder` 调用与 SQLite 不可能组成跨系统原子事务。若 QMT 调用跨界后抛错，
普通子单和父单进入 `UNKNOWN`，Bridge 不会直接重下。后续只能依靠确定性的
QMT备注、`order_callback` 和 `ORDER` 恢复对账确认原委托；确认后才能继续提交
尚未调用的确定性子单。

## 10. TWAP/VWAP 扩展点

已经保留：

- `STOCK_EXECUTION_ALGORITHMS` 中的 `TWAP`、`VWAP` 标识；
- `AlgoOrderRequest` 的统一目标、`params`、父单 ID 和备注；
- `algo_order.preview/place/get/list/cancel` 命令族；
- `algo_orders`、`algo_children` 持久化模型；
- 父单状态、子单身份、成交聚合和整体撤单机制；
- QMT 10毫秒定时器中的非阻塞算法状态机。

尚未实现且不会假装执行：TWAP 时间片生成、VWAP 成交量曲线、参与率控制和对应
参数校验。调用这两个标识会在创建父单前返回 `ALGORITHM_NOT_IMPLEMENTED`。
