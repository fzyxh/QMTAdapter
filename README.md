# QMTAdapter

Current implementation scope is intentionally limited to:

- stock account query;
- stock position query;
- ordinary cash stock buy/sell;
- exchange-specific native stock market-order declarations;
- adapter order query and cancel;
- SQLite-backed mapping from client order ids to QMT order ids;
- durable idempotent order placement keyed by `client_order_id`.

`client_order_id` is the single logical order identity. Reusing it with the same
canonical order parameters replays the stored order without calling `passorder`
again. Reusing it with different parameters is rejected. There is no separate
public `idempotency_key`.

It does not implement credit, futures, options, algorithmic tasks, baskets, market data, or multiple clients.

## Files

- `qmt_side/qmt_adapter_qmt.py`: paste/import this file as a QMT Python model.
- `qmt_adapter/`: external synchronous and asyncio clients; neither imports QMT modules.
- `scripts/query_account.py`: read-only account and position validation.
- `scripts/place_stock_order.py`: guarded simulation order command. Do not use before read-only validation.
- `scripts/stress_test_calls.py`: read-only synchronous/asyncio timing comparison.
- QMT config: `userdata/qmt_adapter/bridge_config.json` under the local Ping An QMT installation.

## First validation: account and positions

1. Keep `trading_enabled` set to `false`.
2. Add exactly the stock simulation account to the `accounts` array in `bridge_config.json`.
3. In QMT Model Research, create a Python model from `qmt_side/qmt_adapter_qmt.py` and run it.
4. Run from this repository:

```powershell
.\.venv\Scripts\python.exe .\scripts\query_account.py --account-id YOUR_ACCOUNT_ID
```

The returned `raw` objects are intentionally preserved for the first real QMT field inspection. Normalized v1 fields only use names explicitly shown in the installed QMT manual: `m_dAvailable`, `m_strInstrumentID`, and `m_nVolume`.

## Trading validation

Trading remains disabled until the account and position response has been reviewed. Enabling it and selecting a simulation order must be a separate explicit step.

## Asyncio client

`AsyncQmtClient` uses one worker thread and one named-pipe connection. Calls are
serialized in submission order; it does not call QMT APIs in parallel.

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

Read-only timing comparison:

```powershell
.\.venv\Scripts\python.exe -u .\scripts\stress_test_calls.py --mode both --command account --count 50 --interval-ms 50
```
