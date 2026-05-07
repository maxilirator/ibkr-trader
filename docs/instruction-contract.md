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
        "target_fraction_of_account": "1.0",
        "funding_basis": "cash",
        "allow_leverage": false
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

## Virtual Account Targeting

To run the same contract through the virtual execution path, keep the payload
shape unchanged and set `account.account_key` to a key that starts with
`virtual`, for example `virtual0001`.

The API normalizes that key to uppercase, marks all persisted execution rows
with `is_virtual=true`, and routes order submission, cancellation, market-price
reads, fills, account snapshots, and position snapshots through the local
virtual adapter instead of IB Gateway.

Minimal account fragment:

```json
{
  "account": {
    "account_key": "virtual0001",
    "book_key": "rl_virtual_book",
    "book_role": "virtual",
    "book_side": "LONG"
  }
}
```

Virtual execution uses the same sizing fields as normal execution. A
`target_quantity` must be whole shares. A `target_notional` is converted into a
whole-share quantity by dividing by the entry limit price, or by the current
virtual market price for market orders, and rounding down. Each virtual fill
uses a fixed commission of `15 SEK`.

See [Virtual Trading](virtual-trading.md) for the full API contract and smoke
test sequence.

## Model-Routed Candidate Lists

Use schema version `2026-04-25` with `execution.mode="model_routed"` when the
payload is a candidate list for an RL/model runner rather than a deterministic
broker order. In this mode, omit `entry` and `exit`. The API validates and
persists the instruction in `MODEL_ROUTED_PENDING`; the normal execution worker
does not submit it to IBKR or the virtual adapter.

The envelope still uses the word `instructions` because it shares the validated
submit contract, but model-routed rows are RL candidate names. The runner should
read them from `GET /v1/rl/candidates`, roll each symbol forward bar by bar,
and only create an executable trader instruction after the model emits an
action.

Example instruction:

```json
{
  "instruction_id": "2026-04-28-VIRTUALRL01-long-AXFO-model-routed",
  "account": {
    "account_key": "VIRTUALRL01",
    "book_key": "rl_shared_long_trial_106_virtual_01",
    "book_role": "virtual",
    "book_side": "LONG"
  },
  "instrument": {
    "symbol": "AXFO",
    "security_type": "STK",
    "exchange": "XSTO",
    "currency": "SEK"
  },
  "intent": {
    "side": "BUY",
    "position_side": "LONG"
  },
  "sizing": {
    "mode": "target_notional",
    "target_notional": "6666"
  },
  "execution": {
    "mode": "model_routed",
    "model_id": "long_trial_106_v1",
    "model_family": "canonical_long_live_execution_policy",
    "model_version": "v1",
    "model_artifact_id": "trial_106",
    "window": {
      "start_at": "2026-04-28T09:00:00+02:00",
      "end_at": "2026-04-28T17:30:00+02:00"
    }
  },
  "trace": {
    "reason_code": "rl_model_routed_candidate",
    "trade_date": "2026-04-28",
    "data_cutoff_date": "2026-03-23",
    "metadata": {
      "static_features": {
        "schema_version": "rl_static_features_v1",
        "model_key": "long_trial_106_v1",
        "feature_schema_version": "long_trial_106_static_v1",
        "feature_names": ["rank_score_z", "turnover_z"],
        "values": [0.25, -1.5],
        "normalized": true,
        "source": "upstream_candidate_payload"
      }
    }
  }
}
```

For RL candidates, `trace.metadata.static_features` is the preferred per-name
static feature vector. `feature_names` must be in the exact order expected by
the promoted model's `static_feature_cols.csv`, `values` must be finite numbers,
and `normalized` must be `true`.

### RL Sizing And Capital Allocation

RL bucket-booster sizing is side-exposure based. Long and short candidates may
share the same broker account, but they have separate book-level exposure
budgets and one shared account-level margin guard.

For the current API, every model-routed candidate still carries the final
per-name execution exposure as `sizing.mode="target_notional"`. The sizing
policy and shared guard are also persisted under `trace.metadata.capital_plan`
so the runner, dashboard, and audit path can explain how that per-name amount
was produced.

Long bucket example:

```json
{
  "strategy_key": "bucket_booster_long",
  "sizing_policy": {
    "capital_base": "net_liquidation_value",
    "book_allocation_pct": "0.90",
    "per_name_method": "equal_weight",
    "max_book_gross_account_pct": "0.90",
    "min_order_notional": "1000",
    "rounding": "whole_shares_down"
  }
}
```

Short bucket example:

```json
{
  "strategy_key": "bucket_booster_short",
  "sizing_policy": {
    "capital_base": "net_liquidation_value",
    "book_allocation_pct": "0.80",
    "per_name_method": "equal_weight",
    "max_book_gross_account_pct": "0.80",
    "min_order_notional": "1000",
    "rounding": "whole_shares_down",
    "require_shortable": true,
    "require_borrow_rate_available": true
  }
}
```

Shared account guard:

```json
{
  "account_key": "VIRTUALRL01",
  "allocation_guard": {
    "capital_base": "net_liquidation_value",
    "max_long_gross_account_pct": "0.90",
    "max_short_gross_account_pct": "0.80",
    "max_total_gross_account_pct": "1.70",
    "max_abs_net_exposure_account_pct": "0.25",
    "min_excess_liquidity_buffer_pct": "0.20",
    "block_if_margin_preflight_fails": true,
    "block_if_projected_maintenance_margin_exceeded": true
  }
}
```

Concrete per-candidate payload shape:

```json
{
  "sizing": {
    "mode": "target_notional",
    "target_notional": "6000"
  },
  "trace": {
    "metadata": {
      "capital_plan": {
        "schema_version": "rl_capital_plan_v2",
        "allocation_method": "account_pct_gross_exposure_equal_weight",
        "account_key": "VIRTUALRL01",
        "account_currency": "SEK",
        "account_equity_reference": "100000",
        "capital_base": "net_liquidation_value",
        "strategy_key": "bucket_booster_long",
        "strategy_side": "LONG",
        "book_allocation_pct": "0.90",
        "max_book_gross_account_pct": "0.90",
        "strategy_gross_budget": "90000",
        "candidate_count": 15,
        "per_name_target_notional": "6000",
        "min_order_notional": "1000",
        "rounding": "whole_shares_down",
        "require_shortable": false,
        "require_borrow_rate_available": false,
        "short_sale_proceeds_reinvested": false,
        "allocation_guard": {
          "schema_version": "rl_allocation_guard_v1",
          "account_key": "VIRTUALRL01",
          "capital_base": "net_liquidation_value",
          "max_long_gross_account_pct": "0.90",
          "max_short_gross_account_pct": "0.80",
          "max_total_gross_account_pct": "1.70",
          "max_abs_net_exposure_account_pct": "0.25",
          "min_excess_liquidity_buffer_pct": "0.20",
          "block_if_margin_preflight_fails": true,
          "block_if_projected_maintenance_margin_exceeded": true
        }
      }
    }
  }
}
```

Payload-maker rule for each morning batch:

- read the trader API account snapshot and use `net_liquidation_value` as the
  capital base
- choose one book allocation for the long strategy and one for the short
  strategy
- long `book_allocation_pct` means long gross exposure as a percentage of NLV
- short `book_allocation_pct` means short gross exposure as a percentage of
  NLV, not cash spent
- divide each strategy's gross exposure budget by that strategy's actual
  candidate count for the day
- write the resulting amount into each candidate's `sizing.target_notional`
- cap the amount by deployment risk limits such as `max_notional_per_name_sek`
- omit or mark candidates whose notional would round below one share or below
  `min_order_notional`

Example with `100000 SEK` NLV:

- long allocation `0.90` gives `90000 SEK` long gross exposure
- short allocation `0.80` gives `80000 SEK` short gross exposure
- 15 long candidates get `6000 SEK` per candidate
- 20 short candidates get `4000 SEK` short exposure per candidate
- total gross may reach `170000 SEK` only if both sides fill

Short candidates are gross exposure allocations. Do not add expected short-sale
proceeds back into account cash, long buying power, or the next candidate's
budget. Shorts still consume margin capacity, require shortability/borrow
checks, and must pass the shared account margin guard before live submission.

`GET /v1/rl/candidates` is the preferred RL-agent view. `GET /v1/instructions`
and `GET /v1/instructions/{instruction_id}` still include the persisted payload
for auditing and debugging.

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

### Timed follow-up exits

When the strategy needs a later follow-up order anchored to the market price at
that later time, use `exit.delayed_limit`.

Example:

```json
{
  "exit": {
    "delayed_limit": {
      "submit_at": "2026-04-20T10:30:00+02:00",
      "limit_offset_pct": "0.05",
      "reference": "MARKET_AT_TRIGGER"
    }
  }
}
```

Meaning:

- wait until `exit.delayed_limit.submit_at`
- observe the live market using the latest available IBKR market price at that time
- place the exit limit order `5%` above that observed market for a long position
- place the exit limit order `5%` below that observed market for a short position

`exit.delayed_limit` is meant for timed market-anchored exits, so do not combine
it with `exit.take_profit_pct`, which is a fill-anchored immediate protective exit.

## Strong opinion on sizing

The execution contract should carry **one** sizing mechanism per instruction.

Allowed:

- fraction of account NAV
- target notional
- target quantity

For long `BUY` instructions, account-based sizing is cash-backed by default.
That means the execution service uses account cash, not margin, unless the
instruction explicitly opts into leveraged sizing:

- `sizing.funding_basis = "cash"`:
  use cash balance for account-based sizing
- `sizing.funding_basis = "account_nav"` plus `sizing.allow_leverage = true`:
  allow NAV/margin-backed sizing for a long entry

Short entries may still size off account NAV without this extra flag.

Not allowed:

- multiple sizing targets in the same instruction
- account-level and book-level sizing targets that disagree

For model-routed RL candidates, `trace.metadata.capital_plan` may explain the
book-level allocation math, but `sizing.target_notional` is still the resolved
per-name exposure the runner will copy into any generated execution
instruction. If the portfolio construction layer needs richer intermediate
math, keep that upstream and emit one final execution sizing decision.

## Currency semantics

To avoid ambiguity, the contract uses two currencies with different roles:

- `instrument.currency`: the native price currency of the tradable contract
- broker account currency: used for NAV, buying power, and account-level controls

Examples:

- `SIVE` on Stockholm should use `SEK` bars, `SEK` limit prices, `SEK` fills, and `SEK` direct notionals
- `AAPL` on NASDAQ should use `USD` bars, `USD` limit prices, `USD` fills, and `USD` direct notionals

If an instruction uses account-based sizing, the execution service may convert
the chosen account funding value into `instrument.currency` for sizing. After
that conversion, the downstream order preview and execution remain in
`instrument.currency`.
