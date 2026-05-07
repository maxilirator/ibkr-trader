# Virtual Trading

Virtual trading is the local simulation path for RL and operator testing when an
IBKR paper account cannot provide the required live market data.

The virtual path uses the same durable instruction, runtime, ledger, read-model,
and dashboard surfaces as real broker execution. The difference is the account
target:

- `account.account_key` starts with `virtual`, for example `virtual0001`
- the API normalizes it to uppercase, for example `VIRTUAL0001`
- no order is sent to IB Gateway
- no IBKR market-data subscription is required for virtual-only runtime work
- rows written to account, order, fill, position, instruction, and deployment
  tables carry `is_virtual=true`

## Execution Contract

Use the normal instruction contract and set only the account target to a virtual
account key.

```json
{
  "schema_version": "2026-04-10",
  "source": {
    "system": "q-training",
    "batch_id": "virtual-smoke-2026-04-27",
    "generated_at": "2026-04-27T09:00:00Z",
    "strategy_id": "trial_27",
    "policy_id": "virtual_regression"
  },
  "instructions": [
    {
      "instruction_id": "virtual-smoke-SIVE-long-01",
      "account": {
        "account_key": "virtual0001",
        "book_key": "rl_virtual_book",
        "book_role": "virtual",
        "book_side": "LONG"
      },
      "instrument": {
        "symbol": "SIVE",
        "security_type": "STK",
        "exchange": "XSTO",
        "currency": "SEK"
      },
      "intent": {
        "side": "BUY",
        "position_side": "LONG"
      },
      "sizing": {
        "mode": "target_quantity",
        "target_quantity": "100"
      },
      "entry": {
        "order_type": "LIMIT",
        "submit_at": "2026-04-27T09:00:00Z",
        "expire_at": "2026-04-27T16:00:00Z",
        "limit_price": "10.50",
        "time_in_force": "DAY",
        "max_submit_count": 1,
        "cancel_unfilled_at_expiry": true
      },
      "exit": {
        "take_profit_pct": "0.10"
      },
      "trace": {
        "reason_code": "virtual_smoke_test"
      }
    }
  ]
}
```

Current virtual fill semantics:

- `target_quantity` is used directly and must be a whole-share quantity
- `target_notional` is converted to whole shares using the entry limit price,
  or the current virtual market price for market orders, rounded down
- exit orders use the actual filled entry quantity
- each fill carries a fixed commission of `15` `SEK`
- a market order fills when a usable virtual quote exists
- a buy limit fills when the virtual action price is less than or equal to the
  limit price
- a sell limit fills when the virtual action price is greater than or equal to
  the limit price
- a buy stop fills when the virtual action price is greater than or equal to the
  stop price
- a sell stop fills when the virtual action price is less than or equal to the
  stop price

Action price selection:

- buy orders prefer `ask_price`, then `last_price`, then `midpoint_price`, then
  `bid_price`
- sell orders prefer `bid_price`, then `last_price`, then `midpoint_price`, then
  `ask_price`
- when midpoint is not supplied but bid and ask exist, midpoint is derived from
  them

## API Contract

### `POST /v1/virtual/accounts`

Creates or refreshes a virtual broker-account row and writes a virtual account
snapshot.

Request:

```json
{
  "account_key": "virtual0001",
  "base_currency": "SEK",
  "account_label": "RL virtual sandbox",
  "cash_balance": "200000"
}
```

Response:

```json
{
  "accepted": true,
  "virtual_account": {
    "account_key": "VIRTUAL0001",
    "broker_kind": "VIRTUAL",
    "account_label": "RL virtual sandbox",
    "base_currency": "SEK",
    "is_virtual": true,
    "cash_balance": "200000",
    "snapshot_id": 1
  }
}
```

### `POST /v1/virtual/market-watch`

Publishes a virtual market-watch quote. Publishing a quote also evaluates open
virtual orders for the same account, symbol, security type, and currency.
Matching open orders are filled immediately and written to the ledger.

This is a price-observation endpoint only. Do not call it as an instruction
preflight check. For pre-submit checks, use `POST /v1/instructions/validate`;
then use `POST /v1/instructions/submit` when the validated instruction should
be persisted.

Request:

```json
{
  "account_key": "virtual0001",
  "observed_at": "2026-04-27T09:01:00Z",
  "symbol": "SIVE",
  "security_type": "STK",
  "exchange": "XSTO",
  "currency": "SEK",
  "bid_price": "10.00",
  "ask_price": "10.00",
  "last_price": "10.00",
  "source": "rl_virtual_market_watch",
  "metadata": {
    "scenario": "entry_fill"
  }
}
```

Response:

```json
{
  "accepted": true,
  "virtual_market_watch": {
    "quote": {
      "quote_id": 1,
      "account_key": "VIRTUAL0001",
      "observed_at": "2026-04-27T09:01:00Z",
      "symbol": "SIVE",
      "exchange": "XSTO",
      "currency": "SEK",
      "security_type": "STK",
      "primary_exchange": null,
      "local_symbol": null,
      "bid_price": "10.00",
      "ask_price": "10.00",
      "last_price": "10.00",
      "midpoint_price": null,
      "source": "rl_virtual_market_watch",
      "metadata": {
        "scenario": "entry_fill"
      }
    },
    "filled_order_count": 0,
    "filled_orders": []
  }
}
```

If the quote crosses a resting virtual order, `filled_order_count` is positive
and `filled_orders` contains the virtual broker order ID, execution ID, price,
commission, and commission currency for each generated fill.

### `GET /v1/virtual/market-watch`

Returns recent virtual quotes.

Query parameters:

- `account_key`: optional virtual account filter
- `limit`: optional positive limit, maximum `1000`, default `100`

Example:

```http
GET /v1/virtual/market-watch?account_key=virtual0001&limit=20
```

### Instruction and Runtime Endpoints

The existing instruction and runtime endpoints are unchanged:

- `POST /v1/instructions/submit`
- `POST /v1/instructions/{instruction_id}/submit-entry`
- `POST /v1/instructions/{instruction_id}/cancel-entry`
- `POST /v1/runtime/run-once`

When the instruction account is virtual, these endpoints route to the virtual
execution adapter. A runtime cycle with only virtual work skips the IBKR runtime
snapshot fetch.

### Direct Manual Order Endpoint

`POST /v1/orders/submit` also supports a single virtual-account instruction.
The response mode is `manual_virtual_submit`, `session_client_id` is `null`, and
the broker payload contains `broker_kind="VIRTUAL"` plus
`virtual_execution`.

`POST /v1/orders/{order_id}/cancel` cancels virtual orders locally when the
stored broker order has `is_virtual=true`.

### RL Deployment Contract

RL deployments may use virtual mode:

```json
{
  "deployment_key": "short_trial36_virtual_01",
  "model_key": "short_trial36_v1",
  "account_key": "virtual0001",
  "book_key": "rl_short_trial36_virtual_01",
  "mode": "virtual",
  "status": "running",
  "allowed_symbols": ["SIVE", "VOLV-B"]
}
```

The RL dashboard summary includes `virtual_deployment_count`, and deployment and
action rows include `is_virtual`.

## Ledger And Dashboard Flags

Virtual rows are flagged consistently so they can be filtered quickly:

- `trader_deployment.is_virtual`
- `instruction.is_virtual`
- `broker_account.is_virtual`
- `broker_order.is_virtual`
- `execution_fill.is_virtual`
- `account_snapshot.is_virtual`
- `position_snapshot.is_virtual`

Operator dashboard account, position, order, and fill rows expose `is_virtual`.
Ledger dashboard focus instruction, instruction events, broker order events, and
fills expose `is_virtual`. The Svelte dashboard shows a `Virtual` badge wherever
those rows appear.

## Smoke Test Sequence

1. Create or refresh the account.
2. Publish an initial quote.
3. Submit a virtual instruction.
4. Run the runtime once to submit and fill the entry if the price condition is
   already met.
5. Run the runtime again to reconcile the entry fill and create configured exits.
6. Publish a later quote that crosses the exit price.
7. Run the runtime again to mark the instruction completed.

The regression test `tests/test_virtual_trading.py` follows that sequence with
`SIVE`, buys virtually at `10.00`, sells virtually at `11.50`, records two
`15 SEK` commissions, and ends with virtual position quantity `0`.
