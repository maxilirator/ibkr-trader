# RL Setup Guide

This guide is the plain version of the RL architecture.

The short version:

- the research or bucket repo trains the policy and produces promoted model artifacts
- this repo registers that promoted model, binds it to an account, stores action and heartbeat logs, and shows it in the dashboard
- real or virtual orders should still go through the normal instruction and runtime path
- safe long/short entry actions are translated into the normal instruction contract
- cancels and exits execute only when the API can prove durable RL ownership of
  the generated instruction or exit order

## Mental Model

Think of the system as five boxes.

1. **Training and artifact source**
   - Lives outside this repo.
   - Produces a promoted checkpoint, workflow path, action space, and observation contract.
   - May also run the inference loop.

2. **RL registry**
   - Lives in this repo.
   - Stores model metadata in `trader_model`.
   - Stores account-bound deployments in `trader_deployment`.

3. **RL runtime or runner**
   - Usually outside this repo for now.
   - Reads market data, runs the model, emits actions, and updates heartbeat.
   - Calls this repo over HTTP.

4. **Execution translator**
   - Converts safe RL entry actions into this repo's normal instruction contract.
   - Executes owned cancels and exits against the durable generated instruction.
   - Can return a payload only, or submit and log through `POST /v1/rl/actions/translate`.

5. **Execution runtime**
   - Lives in this repo.
   - Submits validated instructions through IBKR or through the virtual adapter.
   - Owns fills, exits, reconciliation, ledger rows, and operator visibility.

## What Is Implemented Now

Current RL-related pieces in this repo:

- database tables:
  - `trader_model`
  - `trader_deployment`
  - `trader_action`
  - `trader_heartbeat`
- API endpoints:
  - `POST /v1/rl/models/register`
  - `POST /v1/rl/models/upsert`
  - `PUT /v1/rl/models/{model_key}`
  - `POST /v1/rl/deployments`
  - `PATCH /v1/rl/deployments/{deployment_key}`
  - `POST /v1/rl/observations/build`
  - `POST /v1/rl/actions/log`
  - `POST /v1/rl/actions/translate`
  - `POST /v1/rl/deployments/{deployment_key}/heartbeat`
  - `GET /v1/read/rl-dashboard`
- dashboard page:
  - `/rl`
- virtual execution support:
  - virtual accounts whose key starts with `virtual`
  - virtual market-watch quotes
  - virtual orders, fills, positions, account snapshots, and dashboard badges

Current important limits:

- `POST /v1/rl/actions/log` only logs an action
- `POST /v1/rl/actions/translate` can submit side-aware entries and can now
  execute `cancel_entry`, `exit_market`, `exit_tp_180bp`, `exit_tp_200bp`, and
  `clear_exit` when they match exactly one durable RL-generated instruction
- broker or virtual execution still starts from `POST /v1/instructions/submit` and the runtime cycle

## What You Need From The Bucket Repo

I do not need the bucket repo to explain this repo's current setup. I do need it
to make the setup match the real promoted RL system.

Useful bucket-repo inputs are:

- promoted model key, display name, strategy family, and side
- source workflow path or URI
- promoted checkpoint path or URI
- artifact checksum or promotion manifest, if one exists
- exact action space and action-space version
- observation contract:
  - bar family
  - bar size
  - required series
  - lookback length
  - feature schema version
  - adjusted versus raw price convention
  - timezone and session alignment rules
- inference runner command or service definition
- where the runner reads bars or features from
- how the runner maps symbols to IBKR-tradable symbols, `conId`, exchange, and currency
- intended account, mode, book key, allowed symbols, and risk limits
- idempotency convention for action decisions and generated instructions

Without those details, this repo can still be set up for registry, dashboard,
and virtual smoke testing. It just cannot prove that the live runner is reading
the right features or loading the exact promoted artifact.

## Setup Checklist

### 1. Install Runtime Dependencies

Use Python 3.12.

With `uv`:

```bash
uv sync --extra server --extra db --extra dev --extra rl
```

With `pip`:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[server,db,dev,rl]"
```

The promoted DQN runner also needs PyTorch. Use the q-training environment if
it already has the right Torch build, or install a host-appropriate CPU/GPU
Torch wheel separately. The API-only server does not need Torch.

### 2. Configure `.env`

Start from the repo template:

```bash
cp .env.example .env
```

For local virtual RL testing, the important settings are:

```bash
APP_TIMEZONE=Europe/Stockholm
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/ibkr_trader
SESSION_CALENDAR_PATH=../q-data/xsto/calendars/day_sessions.parquet
XSTO_INSTRUMENTS_PATH=../q-data/xsto/instruments/all.txt
XSTO_IDENTITY_PATH=../q-data/xsto/meta/instrument_identity.parquet
API_HOST=0.0.0.0
API_PORT=8000
API_REQUIRE_LOOPBACK_ONLY=false
EXECUTION_RUNTIME_ENABLED=false
```

For real IBKR execution, also set:

```bash
IBKR_HOST=127.0.0.1
IBKR_PORT=4002
IBKR_CLIENT_ID=0
IBKR_DIAGNOSTIC_CLIENT_ID=7
IBKR_STREAMING_CLIENT_ID=9
IBKR_ACCOUNT_ID=UXXXXXXX
IBKR_ACCOUNT_IDS=UXXXXXXX
```

Turn on the long-running execution loop only when the account, Gateway, and
operator controls are ready:

```bash
EXECUTION_RUNTIME_ENABLED=true
EXECUTION_RUNTIME_INTERVAL_SECONDS=5
EXECUTION_RUNTIME_SUBMISSION_LEAD_SECONDS=60
```

### 3. Create The Database Schema

```bash
source .venv/bin/activate
python -m ibkr_trader.db.init_schema
```

### 4. Start The API

```bash
source .venv/bin/activate
python -m ibkr_trader.api.server
```

Check it:

```bash
curl -sS http://127.0.0.1:8000/healthz
```

### 5. Create A Virtual Account First

Use virtual mode before touching a real IBKR account.

```bash
API=http://127.0.0.1:8000

curl -sS -X POST "$API/v1/virtual/accounts" \
  -H "Content-Type: application/json" \
  -d '{
    "account_key": "virtualrl01",
    "base_currency": "SEK",
    "account_label": "RL virtual sandbox",
    "cash_balance": "200000"
  }'
```

Publish a first virtual quote:

```bash
curl -sS -X POST "$API/v1/virtual/market-watch" \
  -H "Content-Type: application/json" \
  -d '{
    "account_key": "virtualrl01",
    "observed_at": "2026-04-27T09:01:00Z",
    "symbol": "SIVE",
    "security_type": "STK",
    "exchange": "XSTO",
    "currency": "SEK",
    "bid_price": "10.00",
    "ask_price": "10.00",
    "last_price": "10.00",
    "source": "rl_virtual_market_watch"
  }'
```

### 6. Register Or Update The Promoted Model

This creates or replaces durable model metadata. It does not load the model into
memory.

Use `POST /v1/rl/models/register` only when the model key must be new. Use
`POST /v1/rl/models/upsert` when a promoted artifact or observation contract
needs to refresh an existing model key.

```bash
curl -sS -X POST "$API/v1/rl/models/upsert" \
  -H "Content-Type: application/json" \
  -d '{
    "model_key": "short_trial36_v1",
    "display_name": "Short Trial 36 V1",
    "strategy_family": "canonical_short_live_execution_policy",
    "side": "SHORT",
    "source_workflow_path": "/path/from/bucket/repo/workflow.yaml",
    "promoted_checkpoint_path": "/path/from/bucket/repo/best_checkpoint.pt",
    "action_space": [
      "skip",
      "wait",
      "market_entry",
      "cancel_entry",
      "exit_market",
      "clear_exit",
      "entry_prevclose_88bp",
      "exit_tp_180bp"
    ],
    "observation_contract": {
      "bar_family": "phase1_intraday_ohlc_v1",
      "bar_interval": "5m",
      "intraday_fetch_config": "/home/mattias/dev/q-training-bucket-booster/configs/intraday_fetch.yaml",
      "session_timezone": "Europe/Stockholm",
      "session_open_local": "09:00",
      "session_close_local": "17:30",
      "price_inputs": ["open", "high", "low", "close"],
      "growing_day_prefix": true,
      "include_market_context": true,
      "include_vol_normalized_intraday_state": true,
      "vol_normalization_floor": 0.000001,
      "feature_schema_version": "short_live_v1",
      "source_market_data_contract": {
        "bar_family": "stockholm_intraday_1m_v1",
        "required_series": ["TRADES"],
        "adapter": "ibkr_1m_trades_to_phase1_5m_ohlc_v1"
      }
    },
    "execution_mapping_version": "short_actions_v1"
  }'
```

Long-side promoted models use the same base contract, but with the long action
space and mapping version:

```json
{
  "action_space": [
    "skip",
    "wait",
    "market_entry",
    "cancel_entry",
    "exit_market",
    "clear_exit",
    "entry_prevclose_-50bp",
    "exit_tp_200bp"
  ],
  "execution_mapping_version": "long_actions_v1"
}
```

### 7. Create A Deployment

A deployment binds one registered model to one account and one internal book.
For a paired long/short RL setup that shares capital or short-sale proceeds,
use the same `account_key` for both deployments and keep separate `book_key`
values for attribution, limits, and dashboard filtering.

Virtual deployment:

```bash
curl -sS -X POST "$API/v1/rl/deployments" \
  -H "Content-Type: application/json" \
  -d '{
    "deployment_key": "short_trial36_virtual_01",
    "model_key": "short_trial36_v1",
    "account_key": "virtualrl01",
    "book_key": "rl_short_trial36_virtual_01",
    "mode": "virtual",
    "status": "running",
    "allowed_symbols": ["SIVE", "VOLV-B"],
    "risk_limits": {
      "max_open_positions": 2,
      "max_notional_per_name_sek": 1000
    },
    "action_constraints": {
      "position_side": "SHORT",
      "state_machine_version": "short_symbol_state_v1",
      "execution_mapping_version": "short_actions_v1"
    }
  }'
```

Live deployment shape:

```json
{
  "deployment_key": "short_trial36_live_01",
  "model_key": "short_trial36_v1",
  "account_key": "U25245596",
  "book_key": "rl_short_trial36_live_01",
  "mode": "live",
  "status": "running",
  "allowed_symbols": ["SIVE", "VOLV-B"]
}
```

Practical rule: a long/short RL pair that is meant to share buying power should
run under the same real IBKR account. Split the pair by `book_key`, side-aware
action constraints, and deployment-level risk limits instead of separate broker
accounts.

To edit deployment fields after creation, patch only the fields that changed:

```bash
curl -sS -X PATCH "$API/v1/rl/deployments/short_trial36_virtual_01" \
  -H "Content-Type: application/json" \
  -d '{
    "allowed_symbols": ["SIVE", "VOLV-B", "ERIC-B"],
    "risk_limits": {
      "max_open_positions": 3,
      "max_notional_per_name_sek": 1000
    },
    "metadata": {
      "operator_note": "controlled first virtual run"
    }
  }'
```

### 8. Build Model Observations

The model-facing input is **5-minute OHLC**. The API may refresh the observation
payload every minute, but the promoted RL policy should only make a new decision
on the completed 5-minute bar cadence. Do not feed the model raw 1-minute bars
as features. The 1-minute IBKR TRADES bars are only a source stream that this
repo can aggregate into the bucket-trained contract:

- bar family: `phase1_intraday_ohlc_v1`
- model bar interval: `5m`
- observation refresh cadence: `1m`
- model decision cadence: `5m`
- decision policy: completed 5-minute bars only
- session: `Europe/Stockholm`, `09:00` to `17:30`
- current-day policy: growing day prefix with the current incomplete 5-minute
  bar included for monitoring only
- full-day denominator: 102 expected XSTO 5-minute bars, even at 09:05
- history features: previous-session shape plus trailing intraday realized
  volatility over up to 20 prior sessions
- optional JSON override: the runner may send precomputed history/volatility if
  it only wants this API to build the live day prefix
- static name features: the upstream selected-name payload must provide the
  promoted model's normalized static candidate feature vector; the DQN
  checkpoint expects that vector before the live intraday state. Put it on each
  model-routed instruction at `trace.metadata.static_features`.

If the runner already has source bars, send them. The example is shortened, but
the real request should include every available 1-minute source bar for each
symbol up to `as_of`:

```bash
curl -sS -X POST "$API/v1/rl/observations/build" \
  -H "Content-Type: application/json" \
  -d '{
    "deployment_key": "long_trial_106_virtual_shared_01",
    "symbols": ["AXFO"],
    "as_of": "2026-04-28T09:07:30+02:00",
    "source_bars": {
      "AXFO": [
        {
          "timestamp": "20260428 09:00:00",
          "open": "250.00",
          "high": "251.00",
          "low": "249.50",
          "close": "250.50",
          "volume": "12000"
        }
      ]
    },
    "history_overrides": {
      "AXFO": {
        "prev_close": "248.00",
        "history_features": {
          "prev_open_rel_close": "0.0010",
          "prev_high_rel_close": "0.0120",
          "prev_low_rel_close": "-0.0080",
          "prev_close_rel_open": "0.0040",
          "prev_high_rel_low": "0.0200",
          "trailing_intraday_realized_vol": "0.0180",
          "trailing_session_count_norm": "1.0"
        }
      }
    },
    "static_features": {
      "AXFO": {
        "feature_names": ["rank_score_z", "turnover_z"],
        "values": ["0.25", "-1.50"],
        "normalized": true,
        "source": "upstream_candidate_payload"
      }
    }
  }'
```

For production, subscribe once to the live market-data stream and let this repo
maintain the 1-minute source-bar buffer:

```bash
curl -sS -X POST "$API/v1/market-data/stream/subscribe" \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["AXFO", "AZN", "TELIA"],
    "exchange": "SMART",
    "primary_exchange": "SFB",
    "currency": "SEK",
    "market_data_type": "LIVE",
    "replace": true
  }'
```

Then call `POST /v1/rl/observations/build` every minute without `source_bars`.
The endpoint reads the in-memory stream buffer, aggregates 1-minute source bars
into 5-minute model bars, and returns a model payload. This is the intended path
for 40-name morning runs. Do not poll IBKR historical bars every minute for the
full candidate list.

Historical fetching is still available as an explicit smoke-test fallback with
`"fetch": {"mode": "historical_bars"}`. It is not the live RL path.

The response contains the exact model-facing data:

- `phase1_bars`: 5-minute OHLC bars, including whether the latest bar is complete
- `model_decision`: whether a completed 5-minute decision step is ready, how
  many bars the runner may use, and a per-symbol `decision_id`
- `history_features`: previous-session and trailing-volatility vector
- `static_features`: upstream normalized candidate feature vector, if supplied
- `base_dynamic`: intraday state features
- `extra_dynamic`: volatility-normalized and market-context features
- `path_feature_stack`: own and market OHLC paths relative to previous close

At `09:07`, for example, the payload may include the incomplete `09:05-09:10`
bar, but `model_decision.usable_bar_count` should still point only to completed
bars through `09:05`. The runner should ignore the trailing incomplete bar for
model inference and should not emit another action for the same `decision_id`.

This observation builder is account-mode neutral. It is the same for virtual,
paper, and live deployments; switching to live should only change deployment
account/mode and risk controls, not the model input contract.

Do not run the DQN if `features.static_features_ready` is false. The API can
build live bars, history, volatility, and market context, but it cannot recreate
the upstream lockbox/static ranker row unless the runner sends it.

### 9. Send Heartbeats

The runner should update heartbeat on every loop or every few loops.

```bash
curl -sS -X POST "$API/v1/rl/deployments/short_trial36_virtual_01/heartbeat" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "running",
    "last_seen_at": "2026-04-27T09:01:05Z",
    "last_bar_at": "2026-04-27T09:01:00Z",
    "metrics": {
      "bar_lag_seconds": 5,
      "symbols_seen": 2
    }
  }'
```

The dashboard treats old heartbeats as stale.

### 10. Log RL Actions

Every model decision should be append-only logged, including skipped or rejected
decisions.

```bash
curl -sS -X POST "$API/v1/rl/actions/log" \
  -H "Content-Type: application/json" \
  -d '{
    "deployment_key": "short_trial36_virtual_01",
    "symbol": "SIVE",
    "action_name": "market_entry",
    "observed_at": "2026-04-27T09:01:00Z",
    "state_before": "FLAT",
    "state_after": "ENTRY_PENDING",
    "action_status": "logged",
    "payload": {
      "policy_score": "0.73",
      "feature_schema_version": "short_live_v1"
    }
  }'
```

Important: logging this action does not execute it yet.

## Sending Candidate Lists To RL Agents

When another agent wants to hand names to an RL model, use the model-routed
candidate contract instead of a deterministic `entry` and `exit`. The payload
field is still named `instructions` for API compatibility, but these rows are
candidate names, not broker orders.

The API stores these rows as `MODEL_ROUTED_PENDING`. The normal runtime ignores
them. The RL runner reads them through `GET /v1/rl/candidates`, subscribes their
symbols to the market-data stream, rolls every candidate through bars during the
execution window, and only creates a real trader instruction after the model
emits an action.

Use:

```json
{
  "schema_version": "2026-04-25",
  "instructions": [
    {
      "account": {
        "account_key": "VIRTUALRL01",
        "book_key": "rl_shared_long_trial_106_virtual_01",
        "book_role": "virtual",
        "book_side": "LONG"
      },
      "execution": {
        "mode": "model_routed",
        "model_id": "long_trial_106_v1",
        "window": {
          "start_at": "2026-04-28T09:00:00+02:00",
          "end_at": "2026-04-28T17:30:00+02:00"
        }
      }
    }
  ]
}
```

Each instruction still needs the normal `instruction_id`, `instrument`,
`intent`, `sizing`, and `trace` fields. The full model-routed example is in
[instruction-contract.md](instruction-contract.md).

## How Actions Become Orders

The runner should map actions like this after a completed model decision bar:

- `skip`: log the decision only
- `wait`: log the decision only
- `market_entry`: translate to a side-aware market-entry instruction
- `entry_prevclose_88bp`: translate a short limit entry at `previous_close * 1.0088`
- `entry_prevclose_-50bp`: translate a long limit entry at `previous_close * 0.9950`
- `cancel_entry`: cancel the pending entry instruction owned by this deployment and symbol
- `exit_market`: flatten the open position at market and cancel conflicting exits first
- `exit_tp_180bp`: create or replace the short take-profit exit at a `1.80%` favorable move
- `exit_tp_200bp`: create or replace the long take-profit exit at a `2.00%` favorable move
- `clear_exit`: cancel a pending exit while keeping the position open

Use `POST /v1/rl/actions/translate` for entry, cancel, and exit actions. It
reads a persisted `MODEL_ROUTED_PENDING` source instruction, validates that it
matches the deployment account, book, model, and allowed symbol, and then either
returns a normal deterministic entry payload or executes an owned cancel/exit
mutation. With `"submit": true`, it persists or executes the action. With
`"log_action": true`, it logs the RL action.

```bash
curl -sS -X POST "$API/v1/rl/actions/translate" \
  -H "Content-Type: application/json" \
  -d '{
    "deployment_key": "long_trial_106_virtual_shared_01",
    "source_instruction_id": "2026-04-28-long-AXFO-model-routed-01",
    "action_name": "entry_prevclose_-50bp",
    "state_before": "FLAT",
    "observed_at": "2026-04-28T09:05:00+02:00",
    "previous_close": "248.00",
    "decision_id": "long_trial_106_virtual_shared_01:AXFO:2026-04-28T09:05:00+02:00",
    "submit": true,
    "log_action": true
  }'
```

The translated payload for that long action uses `BUY/LONG`, `LIMIT`, and
`246.7600` as the entry price. The equivalent short action
`entry_prevclose_88bp` uses `SELL/SHORT`, `LIMIT`, and previous close plus
`0.88%`.

Actions that require existing order ownership use the durable RL-generated
instruction created by an earlier entry action:

- `cancel_entry` cancels the pending entry before broker submission, or cancels
  the submitted entry order through the normal broker/virtual cancel path
- `exit_market` cancels any owned pending exit first, then submits a market
  exit on the opposite side of the entry
- `exit_tp_180bp` is valid for short models and submits/replaces a BUY limit
  exit `1.80%` below the entry fill price
- `exit_tp_200bp` is valid for long models and submits/replaces a SELL limit
  exit `2.00%` above the entry fill price
- `clear_exit` cancels the owned pending exit and keeps the position open

If the API cannot find exactly one active RL-generated instruction for the
deployment, source instruction, account/book, and symbol, it returns a conflict
instead of touching broker state.

For short entries, the generated instruction must use:

```json
{
  "intent": {
    "side": "SELL",
    "position_side": "SHORT"
  }
}
```

For long entries, the generated instruction must use:

```json
{
  "intent": {
    "side": "BUY",
    "position_side": "LONG"
  }
}
```

The generated instruction account must match the deployment:

```json
{
  "account": {
    "account_key": "virtualrl01",
    "book_key": "rl_short_trial36_virtual_01",
    "book_role": "virtual",
    "book_side": "SHORT"
  }
}
```

Then the normal execution path is:

1. `POST /v1/instructions/submit`
2. `POST /v1/runtime/run-once` for manual cycles, or `EXECUTION_RUNTIME_ENABLED=true` for the long-running loop
3. fills, exits, ledger rows, and dashboard state update through the shared runtime

The full instruction payload contract is in [instruction-contract.md](instruction-contract.md).

## Safe State Machine

Use one state per deployment and symbol:

- `FLAT`
- `ENTRY_PENDING`
- `SHORT_OPEN`
- `LONG_OPEN`
- `EXIT_PENDING`
- `BLOCKED`

Allowed actions:

- `FLAT`: `skip`, `wait`, `market_entry`, `entry_prevclose_88bp`
- `FLAT` for long models: `skip`, `wait`, `market_entry`, `entry_prevclose_-50bp`
- `ENTRY_PENDING`: `wait`, `cancel_entry`
- `SHORT_OPEN`: `wait`, `exit_market`, `exit_tp_180bp`
- `LONG_OPEN`: `wait`, `exit_market`, `exit_tp_200bp`
- `EXIT_PENDING`: `wait`, `clear_exit`, `exit_market`

Invalid actions should be logged with `action_status="invalid_action"` and no
instruction should be submitted.

## Dashboard

Run the dashboard:

```bash
cd dashboard
npm install
IBKR_TRADER_API_BASE_URL=http://127.0.0.1:8000 npm run dev -- --host 127.0.0.1 --port 4173
```

Open:

```text
http://127.0.0.1:4173/rl
```

The dashboard expects the API and read models to exist. It should fail visibly
instead of inventing fake model, deployment, action, or heartbeat data.

## Minimal Virtual Smoke Test

Use this sequence to prove the wiring before live deployment:

1. start Postgres
2. run `python -m ibkr_trader.db.init_schema`
3. start `python -m ibkr_trader.api.server`
4. create `virtualrl01`
5. register the model
6. create the virtual deployment
7. publish a virtual quote
8. build `POST /v1/rl/observations/build` and verify 5-minute bars/features
9. log a heartbeat with `last_bar_at`
10. log an RL action
11. check `GET /v1/read/rl-dashboard`
12. submit a normal virtual instruction generated from that action
13. run `POST /v1/runtime/run-once`
14. publish later virtual quotes until entry or exit prices cross
15. run the runtime again and inspect the ledger/dashboard

The detailed virtual execution contract is in [virtual-trading.md](virtual-trading.md).
The morning readiness checklist is in
[rl-operational-readiness.md](rl-operational-readiness.md).

## Live Deployment Checklist

Do not move from virtual to live until these are true:

- IB Gateway is stable and reachable on `127.0.0.1`
- the correct real `IBKR_ACCOUNT_ID` or `IBKR_ACCOUNT_IDS` is configured
- the shared RL account is financially isolated enough for autonomous RL risk
- the daily model-routed candidate payload is the explicit active universe; do
  not rely on stale deployment-level allow-lists for dynamic boosters
- Stockholm shortability is current for short-side models
- risk limits are configured at deployment level
- the kill switch and operator dashboard are visible
- the runner can prove its feature contract matches the promoted model
- the runner logs every action and heartbeat
- the generated instructions pass `/v1/instructions/validate`
- the first live run uses tiny size

## Current Runner Implementation

The repo now has a promoted-model runner path:

- `src/ibkr_trader/rl/model_artifacts.py` is the single source of truth for the
  q-training long and short artifacts
- `scripts/bootstrap_rl_registry.py` creates or updates the shared virtual
  account, model registry rows, and the two shared virtual deployments
- `scripts/submit_rl_candidate_lists.py` submits the promoted lockbox names as
  `MODEL_ROUTED_PENDING` candidates
- the number of submitted RL candidates is not fixed; each overnight booster
  run submits every row marked `selected=true` for that side and trade date
- `scripts/run_rl_agents.py` loads the promoted DQN checkpoints, polls
  `/v1/rl/candidates`, subscribes active symbols to the market stream, hydrates
  static features from the q-training candidate tapes, backfills and caches
  prior-session volatility/history overrides, runs inference only on new
  completed 5-minute bars, logs actions, and submits translated virtual
  entries/cancels/exits when `--execute-virtual` is set

Runner start shape:

```bash
uv run python scripts/run_rl_agents.py \
  --api-base http://127.0.0.1:8000 \
  --trade-date 2026-04-29 \
  --execute-virtual
```

Durable action ownership:

- entries are deterministic instructions tagged with `rl_deployment_key`,
  `rl_source_instruction_id`, and `rl_decision_id`
- cancel/exit actions use those tags to find exactly one active generated
  instruction before mutating broker state
- the same code path is used for virtual and live accounts; switching from
  virtual to live should only change the account/deployment mode and broker
  connectivity, not action semantics
