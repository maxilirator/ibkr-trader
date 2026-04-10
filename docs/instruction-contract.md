# Instruction Contract

This is the proposed contract for the upstream inference service.

The design goal is simple:

- the inference service should emit the execution contract directly
- the execution API should validate, persist, and act on that contract
- normalization should be minimal

Current runtime scope:

- the contract is designed to stay reusable
- the current runtime implementation is Stockholm-first
- examples in this document should prefer Stockholm equities and Stockholm-local timestamps

## Design rules

### 1. No tabular transport wrapper

Do **not** send:

- `headers`
- `rows`

That is a dataframe export shape, not an execution API shape.

The payload should use a normal object envelope with an `instructions` array.

### 2. No `activate` flag in the payload

Do **not** embed transport behavior into the instruction itself.

Instead:

- `POST /v1/instructions/validate` means validate only
- `POST /v1/instructions/submit` currently means validate, schedule, and persist for execution
- live broker order placement happens later in the execution worker

The endpoint determines the action. The payload stays the same.

### 3. Send absolute timestamps

Do **not** send:

- `trade_date`
- `timezone`
- `entry_start_local`
- `entry_end_local`
- `entry_delay_minutes`

for execution timing.

Send:

- `entry.submit_at`
- `entry.expire_at`

as ISO-8601 timestamps with timezone offsets.

### 4. Send decimal values as strings

Use strings for:

- prices
- percentages
- notionals
- quantities

That avoids float drift across systems.

### 5. Separate execution fields from trace/provenance fields

Execution should contain only what the execution service truly needs.

Everything else belongs in `trace`.

Examples of trace data:

- company name
- aliases
- policy name
- release ID
- research file paths
- anchor price and offset used to derive the order price

### 6. Keep market prices and trades in instrument currency

The execution contract should treat the instrument's trading currency as the price currency.

That means:

- `instrument.currency` is the native trading currency for the resolved IBKR contract
- `entry.limit_price` is expressed in `instrument.currency`
- `sizing.target_notional`, when used directly, is expressed in `instrument.currency`
- future bars, ticks, fills, and execution prices should also be stored in `instrument.currency`

Do **not** send converted account-currency prices as the primary trading price.

Account-currency math is still allowed for:

- account NAV
- buying power and margin checks
- portfolio-level risk
- `fraction_of_account_nav` sizing before conversion into instrument currency

So the rule is:

- market data and execution stay in instrument currency
- account and portfolio controls may use account currency
- FX conversion is an explicit step, not an implicit normalization

## Canonical payload

```json
{
  "schema_version": "2026-04-10",
  "source": {
    "system": "q-training",
    "batch_id": "trial_27-2026-04-10-prod-long-01",
    "generated_at": "2026-04-10T02:15:44Z",
    "release_id": "live_release_prod_20260326_long_prevclose_short_prevclose_intraday",
    "strategy_id": "trial_27",
    "policy_id": "risk_policy_agent_v3_long_no_leverage_20260324_fixed_long_trial44_width1_component_exit_v1"
  },
  "instructions": [
    {
      "instruction_id": "2026-04-10-GTW05-long_risk_book-SIVE-long-01",
      "account": {
        "account_key": "GTW05",
        "book_key": "long_risk_book",
        "book_role": "prod",
        "book_side": "LONG"
      },
      "instrument": {
        "symbol": "SIVE",
        "security_type": "STK",
        "exchange": "XSTO",
        "currency": "SEK",
        "isin": "SE0003917798",
        "aliases": ["SIVE.ST", "sivers-ima"]
      },
      "intent": {
        "side": "BUY",
        "position_side": "LONG"
      },
      "sizing": {
        "mode": "fraction_of_account_nav",
        "target_fraction_of_account": "1.0"
      },
      "entry": {
        "order_type": "LIMIT",
        "submit_at": "2026-04-10T09:25:00+02:00",
        "expire_at": "2026-04-10T17:30:00+02:00",
        "limit_price": "11.3131",
        "time_in_force": "DAY",
        "max_submit_count": 1,
        "cancel_unfilled_at_expiry": true
      },
      "exit": {
        "take_profit_pct": "0.02",
        "catastrophic_stop_loss_pct": "0.15",
        "force_exit_next_session_open": true
      },
      "trace": {
        "reason_code": "risk_policy_orderbook",
        "execution_policy": "long_entry5_prevclose-50bp_slnone_tp200bp",
        "trade_date": "2026-04-10",
        "data_cutoff_date": "2026-04-09",
        "company_name": "Sivers Semiconductors",
        "metadata": {
          "entry_reference_type": "prev_close",
          "entry_reference_price": "11.37",
          "entry_offset_pct": "-0.005",
          "borsdata_id": "489",
          "risk_policy_path": ".../best_risk_policy.json",
          "risk_policy_run_dir": ".../risk_policy_agent_v3_long_no_leverage_20260324_fixed_long_trial44_width1_component_exit_v1",
          "live_release_manifest": "/home/mattias/dev/q-training/configs/live_release.prod.yaml"
        }
      }
    }
  ]
}
```

## What changes from the current payload

### Remove entirely

- `headers`
- `rows`
- `activate`
- `company_name` from the execution root
- `share_class`
- `account_label` style duplication
- empty-string placeholders
- null fields that are not active for the chosen instruction mode

### Move into `trace.metadata`

- `entry_anchor_type`
- `entry_anchor_price`
- `entry_rel_pct`
- `seed_expert`
- `policy_active_seed_list`
- `policy_active_instrument_list`
- filesystem paths
- research-system-only identifiers

### Convert into explicit execution fields

- `entry_start_local` + `trade_date` + `timezone` -> `entry.submit_at`
- `entry_end_local` + `trade_date` + `timezone` -> `entry.expire_at`
- `take_profit_pct_from_fill` -> `exit.take_profit_pct`
- `catastrophic_stop_loss_pct_from_fill` -> `exit.catastrophic_stop_loss_pct`
- `fallback_exit_mode=next_open_fallback` -> `exit.force_exit_next_session_open=true`

## Strong opinion on sizing

The execution contract should carry **one** sizing mechanism per instruction.

Allowed:

- fraction of account NAV
- target notional
- target quantity

Not allowed:

- multiple sizing targets in the same instruction
- account-level and book-level sizing targets that disagree

If the portfolio construction layer needs richer intermediate math, keep that upstream and emit one final execution sizing decision.

## Currency semantics

To avoid ambiguity, the contract uses two currencies with different roles:

- `instrument.currency`: the native price currency of the tradable contract
- broker account currency: used for NAV, buying power, and account-level controls

Examples:

- `SIVE` on Stockholm should use `SEK` bars, `SEK` limit prices, `SEK` fills, and `SEK` direct notionals
- `AAPL` on NASDAQ should use `USD` bars, `USD` limit prices, `USD` fills, and `USD` direct notionals

If an instruction uses `fraction_of_account_nav`, the execution service may convert account currency into `instrument.currency` for sizing. After that conversion, the downstream order preview and execution remain in `instrument.currency`.
