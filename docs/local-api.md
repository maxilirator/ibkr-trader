# Trader API

This repository now includes a small FastAPI control plane intended to be reachable on the **trusted LAN**, while IB Gateway itself remains local to the host.

Current runtime scope:

- Stockholm equities first
- `Europe/Stockholm` as the runtime timezone
- q-data Stockholm session calendar as the next-session scheduler input

## Why this shape

For this system, using the official IBKR Python API directly inside a local service is a good fit:

- we keep IBKR session management in one process
- AI and orchestration layers can call a simple HTTP API
- we avoid exposing the raw broker API on the network
- we retain full control over validation, scheduling, and audit behavior

This is especially useful once multiple internal components need to submit instructions, validate intents, or request broker state without each one opening its own IBKR connection.

The runtime scheduler uses `Europe/Stockholm` as the default local timezone and reads the Stockholm session calendar from `SESSION_CALENDAR_PATH`.

The Stockholm shortability snapshot endpoint reads its default symbol universe from `XSTO_INSTRUMENTS_PATH` and enriches Stockholm names with `XSTO_IDENTITY_PATH` when available.

If the configured parquet file cannot be read directly, the scheduler will use the sibling `.csv` file when it exists.

## Security posture

The intended deployment now is:

- IB Gateway stays on `127.0.0.1`
- the trader API is exposed on the trusted LAN
- agents and operator tools call the trader API, not IB Gateway directly

This is a strong starting point, but it is not the final security model for a production quant system. Later we should add:

- process-level auth between LAN clients and the trader API
- OS firewall rules
- separate live and paper environments
- stricter instruction authorization and audit controls

## Endpoints

### `GET /healthz`

Returns basic process status, broker session state, broker heartbeat freshness,
snapshot-refresh freshness, and execution-runtime lease state. By default the
endpoint may start one non-blocking broker-monitor refresh if the cached broker
evidence is older than `BROKER_STATUS_REFRESH_MIN_INTERVAL_SECONDS`.

Use `GET /healthz?refresh_broker_status=false` for a pure cached read.

### `POST /v1/ibkr/probe`

Runs the current broker probe and returns:

- host
- port
- client ID
- broker-reported current time
- next valid order ID

This is the fastest end-to-end check that the official IBKR API path is healthy.

### `POST /v1/contracts/resolve`

Runs a read-only IBKR contract lookup using `reqContractDetails`.

Use it to verify:

- whether a symbol resolves uniquely
- the IBKR `conId`
- primary exchange and currency
- valid exchanges and trading class
- basic market metadata before any order-preview logic exists

Example request body:

```json
{
  "symbol": "SIVE",
  "security_type": "STK",
  "exchange": "XSTO",
  "currency": "SEK",
  "primary_exchange": "XSTO",
  "isin": "SE0003917798"
}
```

### `POST /v1/accounts/summary`

Runs a read-only IBKR account summary query through the diagnostic client ID.

Use it to fetch the fields we need for sizing and risk checks, especially:

- `NetLiquidation`
- `BuyingPower`
- `AvailableFunds`
- `ExcessLiquidity`

Example request body:

```json
{
  "account_id": "DU1234567",
  "tags": ["NetLiquidation", "BuyingPower", "AvailableFunds", "ExcessLiquidity"]
}
```

### Virtual Trading Endpoints

Virtual trading is the local execution path for accounts whose key starts with
`virtual`, for example `virtual0001`. The API normalizes the key to
`VIRTUAL0001`, writes ledger rows with `is_virtual=true`, and does not send
virtual orders or market-data reads to IB Gateway.

Implemented endpoints:

- `POST /v1/virtual/accounts`
- `POST /v1/virtual/market-watch`
- `GET /v1/virtual/market-watch`

Create or refresh a virtual account:

```json
{
  "account_key": "virtual0001",
  "base_currency": "SEK",
  "account_label": "RL virtual sandbox"
}
```

Publish a virtual market-watch quote:

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
  "source": "rl_virtual_market_watch"
}
```

`POST /v1/virtual/market-watch` publishes prices into the virtual adapter. It is
not an instruction preflight endpoint. Use `POST /v1/instructions/validate`
before `POST /v1/instructions/submit` when checking candidate orders.

Quote submission also evaluates open virtual orders for the same account and
instrument. Matching orders fill locally, use `quantity="1"`, record a fixed
`15 SEK` commission, and update virtual account and position snapshots.

Use the normal instruction endpoints with `account.account_key="virtual0001"` to
schedule and run virtual trades:

- `POST /v1/instructions/submit`
- `POST /v1/instructions/{instruction_id}/submit-entry`
- `POST /v1/instructions/{instruction_id}/cancel-entry`
- `POST /v1/runtime/run-once`

See [Virtual Trading](virtual-trading.md) for the full contract and an end-to-end
smoke sequence.

### `GET /v1/broker/runtime-snapshot`

Returns the current broker-visible runtime snapshot from the primary IBKR session.

Use it to see:

- live broker open orders
- executions already reported by IBKR
- the current gap between broker-live state and local control-plane state

Important visibility limit:

- this endpoint shows what IBKR currently exposes as broker-live
- it does **not** show TWS-local orders that remain untransmitted
- untransmitted rows that still show a `Transmit` button in TWS must be captured from the submit-time `openOrder` handoff, not rediscovered later through the open-order API

### `POST /v1/market-data/historical-bars`

Runs a read-only IBKR historical-bars request through the historical/backfill
client ID.

Use it to:

- fetch historical bars in the instrument's native trading currency
- verify the resolved contract before building data ingestion jobs
- build the first native-currency bar ingestion path for the internal data backend

Example request body:

```json
{
  "symbol": "SIVE",
  "security_type": "STK",
  "exchange": "SMART",
  "currency": "SEK",
  "primary_exchange": "SFB",
  "duration": "2 D",
  "bar_size": "5 mins",
  "what_to_show": "TRADES",
  "use_rth": true
}
```

### `POST /v1/market-data/stockholm-intraday-backfill`

Collects one paged batch of Stockholm intraday bars through the
historical/backfill IBKR session.

Use it to:

- nightly pull `1 min` Stockholm intraday data from this repo
- page through the IBKR-tradable Stockholm stock universe from another repo or job runner
- fetch multiple series for the same symbol set, such as `TRADES`, `MIDPOINT`, `BID`, `ASK`, and `ADJUSTED_LAST`

Important current behavior:

- this endpoint is a collector only; it does **not** persist bars in this repo
- it returns one page at a time, with `next_cursor` for the caller to continue the nightly batch
- by default it uses the current Stockholm universe from `XSTO_INSTRUMENTS_PATH`
- it enriches names with `XSTO_IDENTITY_PATH`
- it classifies each result as:
  - `resolves_cleanly`
  - `resolves_suspiciously_remapped`
  - `lookup_error` / `timeout` / `error`
- suspicious remaps are skipped by default unless `include_remapped=true`

Recommended production request body for the fast nightly page:

```json
{
  "as_of_date": "2026-04-24",
  "bar_size": "1 min",
  "what_to_show": ["TRADES"],
  "use_rth": true,
  "max_symbols": 25,
  "sleep_seconds": 0.05,
  "max_runtime_seconds": 55
}
```

Useful paging fields:

- `max_symbols`: how many Stockholm names to fetch in this batch
- `start_after`: optional cursor from the previous response
- `symbols`: optional explicit slug list instead of paging the whole universe
- `max_runtime_seconds`: optional wall-clock budget for the HTTP response. The
  endpoint returns a partial page with `budget_exhausted=true` before holding a
  caller open indefinitely.

For optional quote series such as `MIDPOINT`, `BID`, and `ASK`, use a smaller
page or run a second pass after the raw trade bars have landed. `ADJUSTED_LAST`
is not requested by this dated intraday endpoint because IBKR rejects explicit
end-date intraday requests for that series; apply adjustment factors downstream
from a separate corporate-action or adjusted-close source.

Example response shape:

```json
{
  "accepted": true,
  "session_client_id": 7,
  "market": "stockholm",
  "series_mode": "paged_batch",
  "query": {
    "as_of_date": "2026-04-24",
    "bar_size": "1 min",
    "what_to_show": ["TRADES"],
    "use_rth": true,
    "max_symbols": 25,
    "start_after": null,
    "symbols": null,
    "include_remapped": false,
    "sleep_seconds": 0.05,
    "max_runtime_seconds": 55
  },
  "universe": {
    "current_universe_size": 955,
    "page_size": 25,
    "next_cursor": "volcar-b",
    "requested_page_next_cursor": "volcar-b"
  },
  "summary": {
    "requested_symbol_count": 25,
    "processed_symbol_count": 25,
    "ok_count": 24,
    "lookup_error_count": 1,
    "timeout_count": 0,
    "error_count": 0,
    "partial_count": 0,
    "skipped_remapped_count": 0,
    "unsupported_series_count": 0,
    "not_requested_series_count": 0,
    "resolves_cleanly_count": 24,
    "resolves_suspiciously_remapped_count": 0,
    "budget_exhausted": false,
    "elapsed_seconds": 18.42
  },
  "entries": [
    {
      "slug": "sive",
      "status": "ok",
      "classification": "resolves_cleanly",
      "resolved_contract": {
        "con_id": 123456789,
        "symbol": "SIVE",
        "local_symbol": "SIVE",
        "primary_exchange": "SFB"
      },
      "series": {
        "TRADES": {
          "status": "ok",
          "bar_count": 510,
          "currency": "SEK",
          "bars": []
        }
      }
    }
  ]
}
```

### `POST /v1/market-data/tick-stream-sample`

Collects a short live sample from IBKR's tick-by-tick streaming API through the dedicated streaming client session.

Use it to:

- verify streaming connectivity separately from execution and diagnostic sessions
- collect raw live ticks for a short sampling window
- validate which tick-by-tick streams are entitled for the current IBKR session

It currently supports:

- `Last`
- `AllLast`
- `BidAsk`
- `MidPoint`

Important current behavior:

- this is a timed sample endpoint, not a long-lived socket relay yet
- it is intended as the first raw-data primitive for the future parquet ingestion service
- if IBKR rejects the requested tick-by-tick stream, the endpoint returns the broker error directly

### `POST /v1/market-data/stream/desired`

Publishes the desired persistent market-data stream set used by the RL runner.
This is the normal production path for active candidate names: the runner writes
desired symbols, the API stream owner applies broker subscription diffs, keeps
one socket open, and builds observations from the in-memory 1-minute bar buffer.

Example:

```bash
curl -sS -X POST "$API/v1/market-data/stream/desired" \
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

Use `replace=true` for a fresh morning candidate list. Use `replace=false` to
add names without dropping existing desired symbols. The endpoint does not open
a broker socket itself; the API stream owner reconciles the desired set.

### `POST /v1/market-data/stream/subscribe`

Operator/API-owner endpoint that applies the requested set to the persistent
IBKR market-data stream immediately. Prefer `/desired` for automated runners.
The service keeps top of book, last price, and 1-minute OHLC bars from live
last-price ticks.
For Stockholm symbols, the API enriches canonical dash symbols such as `ERIC-B`
with ticker alias and ISIN from `XSTO_IDENTITY_PATH` before opening the IBKR
subscription, while snapshots still use the canonical dash symbol keys.

### `GET /v1/market-data/stream/snapshot`

Returns the current stream state without touching IBKR.

```bash
curl -sS "$API/v1/market-data/stream/snapshot?symbols=AXFO,AZN&bar_limit=20"
```

`POST /v1/rl/observations/build` reads this same buffer by default when
`source_bars` is omitted. For 40-name RL runs, use this stream path instead of
polling historical bars every minute.

### `POST /v1/market-data/stream/stop`

Cancels active stream subscriptions and disconnects the dedicated streaming
client session.

### `POST /v1/market-data/shortability-snapshot`

Collects a current Stockholm shortability snapshot.

Use it to:

- scan the configured Stockholm universe and return the names that are currently shortable
- classify the full Stockholm universe cleanly as `shortable` or `not_shortable` from IBKR's official Sweden shortable list
- persist a dated daily snapshot into our own data backend

Important current behavior:

- the default source is `OFFICIAL_IBKR_PAGE`, which fetches IBKR's public Sweden shortable list directly
- the official page is the current authoritative path for the Stockholm shortable universe in this repo
- the older `BROKER_TICKS` path is still available as a diagnostic source, but it depends on generic tick `236` and the live Gateway session being healthy
- calling it with no body uses the latest completed listing date from `XSTO_INSTRUMENTS_PATH`
- set `as_of_date` to query the Stockholm listed universe for a specific day
- the official page already includes symbols such as `VOLV.B` and `SIVE`; the API normalizes Swedish share-class dots into our canonical dash form, such as `VOLV-B`
- `VOLV-B` is still a practical Stockholm canary for smoke checks
- `only_shortable=true` is the default, so the response is already filtered to `shortable` and `locate_required`
- set `only_shortable=false` when you want the full scan, including `not_shortable`
- full-universe scans persist by default; smaller samples or explicit symbol requests persist only when `persist=true`
- persisted artifacts are written to:
  `../q-data/xsto/instruments/shortable.txt`
  `../q-data/xsto/instruments/shortable_or_locate.txt`
  `../q-data/xsto/meta/shortability/shortability_snapshot_<date>.json`
  `../q-data/xsto/meta/shortability/shortability_latest.json`
- the persisted JSON keeps the full evaluated universe metadata even when the HTTP response is filtered to shortable names
- the response includes `source`, `source_url`, `source_updated_text`, `snapshot_at`, `universe_as_of_date`, `status_counts`, and the universe source used for the scan
- IBKR does not expose historical shortability snapshots through this path, so daily history still needs to be stored by us after each run

Current status vocabulary:

- `shortable`: the name appears on IBKR's official Sweden shortable list
- `not_shortable`: the name does not appear on IBKR's official Sweden shortable list for the current snapshot
- `locate_required`, `not_found`, `timeout`, `error`, `unknown_status`: these remain possible when the source is `BROKER_TICKS`

Example request body for a small sample:

```json
{
  "as_of_date": "2026-04-14",
  "max_symbols": 50
}
```

Example request body for explicit symbols:

```json
{
  "symbols": ["ABB", "SIVE", "VOLV-B"],
  "only_shortable": false,
  "market_data_type": "LIVE",
  "persist": true
}
```

For a deliberate full-universe refresh outside the API server, use:

```bash
source .venv/bin/activate
PYTHONPATH=src python -m ibkr_trader.ibkr.shortability_refresh
```

That command always requests the full Stockholm universe and persists the results into the same `q-data` files listed above.

### `POST /v1/orders/preview`

Builds a read-only broker preview for an instruction batch using one diagnostic IBKR session.

Currency rule:

- market prices stay in the instrument's native trading currency
- order prices stay in the instrument's native trading currency
- fills and future execution events should be reported in the instrument's native trading currency
- account-level sizing may start from account currency and convert explicitly into instrument currency

It currently:

- validates the instruction batch
- reads account summary
- resolves the broker contract
- constructs the broker order fields we would send at execution time
- normalizes dynamic stock sizing down to whole shares before execution
- reports unresolved cases explicitly instead of guessing

Current safe behavior:

- ready for `target_quantity`
- ready for `target_notional` when `limit_price` is present
- ready for `fraction_of_account_nav`, including cross-currency sizing via IBKR historical FX midpoint data when the pair is available
- explicit `target_quantity` must already be whole shares for stock orders
- dynamic stock sizing from notional or NAV is rounded down to whole shares for execution
- unresolved when IBKR cannot provide a usable FX conversion for the requested currency pair

This is intentional. We should not guess FX sizing in a production trading system, so the preview only uses broker-derived FX data.

### `POST /v1/orders/submit`

Submits a **manual broker order** immediately through the primary IBKR client session.

This endpoint is intentionally narrower than the durable instruction flow:

- exactly one instruction per request
- stock orders only
- `LIMIT` orders only
- whole-share quantities only
- uses the canonical instruction contract, but submits immediately instead of scheduling

It currently:

- validates the canonical instruction batch
- resolves the broker account
- resolves the IBKR contract
- computes quantity using the same sizing logic as preview
- places the broker order through the primary IBKR client session
- returns the broker order status payload
- returns the immediate TWS `openOrder` handoff when available, including `orderState.status`, `warning_text`, and related TWS-local diagnostics

Important current behavior:

- this is an operator/manual broker path
- it is not wired to persisted `ENTRY_PENDING` instructions yet
- it does not add a paper-safety wrapper; live or paper behavior depends entirely on the configured IBKR session
- use it only for explicit operator-driven submits

### `POST /v1/orders/{order_id}/cancel`

Cancels a broker order by IBKR order ID through the primary client session.

This endpoint is the cleanup companion to `POST /v1/orders/submit`.

### RL Trader Registry

The API now includes an early RL trader control surface.

Current registry endpoints:

- `POST /v1/rl/models/register`
- `POST /v1/rl/models/upsert`
- `PUT /v1/rl/models/{model_key}`
- `POST /v1/rl/deployments`
- `PATCH /v1/rl/deployments/{deployment_key}`
- `GET /v1/rl/candidates`
- `POST /v1/rl/observations/build`
- `POST /v1/rl/actions/translate`
- `POST /v1/rl/actions/log`
- `POST /v1/rl/deployments/{deployment_key}/heartbeat`
- `GET /v1/read/rl-dashboard`

Purpose:

- register promoted RL model metadata and artifact lineage
- update promoted RL model metadata when the upstream promotion contract changes
- bind one deployment to one real broker account and one internal book
- expose model-routed candidate names for bar-by-bar RL evaluation
- build model-facing observations from the market-data stream
- translate safe RL actions into normal execution instructions
- log append-only model actions before or after execution translation
- expose heartbeat and recent action visibility to the dashboard

Important current behavior:

- registry endpoints do not submit broker orders directly
- `/v1/rl/actions/translate` can persist translated entry instructions and can
  execute owned RL cancel/exit actions when `submit=true`
- the RL layer is a durable registry, operator view, and side-aware action
  bridge into the normal execution path
- action logging already validates that an action belongs to the registered model action space
- deployments are explicitly account-bound through `account_key`
- deployments may use `mode="virtual"` with a `virtual...` account key for local
  RL execution testing
- registry payloads must be explicit; the API does not infer model side,
  deployment mode/status, action status, action timestamp, or heartbeat timestamp
- `/v1/rl/models/register` is create-only and returns conflict for existing
  model keys
- `/v1/rl/models/upsert` and `PUT /v1/rl/models/{model_key}` create or replace
  the registered metadata for a model key
- `PATCH /v1/rl/deployments/{deployment_key}` updates editable deployment
  fields such as `allowed_symbols`, `status`, `risk_limits`,
  `action_constraints`, and `metadata` without recreating the deployment

Example model registration body:

```json
{
  "model_key": "short_trial36_v1",
  "display_name": "Short Trial 36 V1",
  "strategy_family": "canonical_short_live_execution_policy",
  "side": "SHORT",
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
    "bar_family": "stockholm_intraday_1m_v1",
    "required_series": ["TRADES", "MIDPOINT", "BID", "ASK", "ADJUSTED_LAST"],
    "feature_schema_version": "short_live_v1"
  }
}
```

Example deployment body:

```json
{
  "deployment_key": "short_trial36_live_01",
  "model_key": "short_trial36_v1",
  "account_key": "U25245596",
  "book_key": "rl_short_trial36_live_01",
  "mode": "live",
  "status": "running",
  "allowed_symbols": ["SIVE", "VOLV-B"],
  "risk_limits": {
    "max_open_positions": 8,
    "max_notional_per_name_sek": 25000
  },
  "action_constraints": {
    "position_side": "SHORT",
    "state_machine_version": "short_symbol_state_v1"
  }
}
```

Virtual deployment example:

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

Deployment update example:

```json
{
  "allowed_symbols": ["SIVE", "VOLV-B", "ERIC-B"],
  "risk_limits": {
    "max_open_positions": 3
  },
  "metadata": {
    "operator_note": "controlled first virtual run"
  }
}
```

Example RL dashboard response shape:

```json
{
  "accepted": true,
  "rl_dashboard": {
    "summary": {
      "model_count": 1,
      "deployment_count": 1,
      "live_deployment_count": 1,
      "virtual_deployment_count": 0,
      "running_deployment_count": 1,
      "stale_heartbeat_count": 0,
      "recent_action_count": 5
    },
    "models": [],
    "deployments": [],
    "recent_actions": []
  }
}
```

### `POST /v1/instructions/submit`

Accepts the canonical instruction batch, validates it, computes the Stockholm runtime schedule, and persists the instruction into Postgres.

It currently:

- validates the instruction batch
- cancels older active entry instructions in the same intent group before
  accepting a replacement
- computes runtime schedule metadata, including next-session-open for Stockholm instruments
- persists the instruction in `ENTRY_PENDING` state
- writes an initial `instruction_submitted` lifecycle event
- returns the stored instruction state back to the caller

Important current behavior:

- this endpoint does **not** place a broker order yet
- it is a durable control-plane submit, not a live execution submit
- if an open position already owns the same account/book/side/symbol group, the
  endpoint rejects the fresh entry rather than adding or crossing risk
- virtual account instructions are persisted with `is_virtual=true` and are
  later submitted through the virtual adapter

### `POST /v1/instructions/intent-cleanup`

Plans or applies cleanup for active instructions competing in the same intent
group:

- `account_key`
- `book_key`
- `book_side`
- `symbol`
- `exchange`
- `currency`

At least one selector is required. With `"apply": false`, the endpoint returns
the cancellation plan without mutating state. With `"apply": true`, it cancels
stale `ENTRY_PENDING` rows locally and stale `ENTRY_SUBMITTED` rows through the
normal broker/virtual cancellation path. It never cancels `POSITION_OPEN` or
`EXIT_PENDING` rows; those are returned as blockers so the runtime can manage
position exits with broker-position checks.

Example dry run:

```bash
curl -sS -X POST "$API/v1/instructions/intent-cleanup" \
  -H "Content-Type: application/json" \
  -d '{
    "requested_by": "operator",
    "reason": "Clean duplicate NIBE entries before tomorrow.",
    "apply": false,
    "account_key": "U25245596",
    "symbol": "NIBE B",
    "exchange": "XSTO",
    "currency": "SEK"
  }'
```

### `GET /v1/instructions`

Lists persisted instruction statuses. Archived rows are hidden by default.

Useful query parameters:

- `limit=100`
- `state=ENTRY_PENDING`
- `model_routed=true|false`
- `include_archived=true|false`

### `POST /v1/instructions/archive-set`

Archives matching instruction rows from the default dashboard view without
deleting audit history or lifecycle events.

Useful selectors include:

- `instruction_ids`
- `states`
- `batch_id`
- `account_key`
- `book_key`
- `source_system`
- `model_routed`
- `expire_before`

Active execution states are skipped unless `include_active=true`.

### `GET /v1/instructions/{instruction_id}`

Returns the persisted control-plane status for one instruction.

It currently includes:

- current execution state
- submit/expire timestamps
- entry broker IDs and broker status
- entry/exit fill fields already reconciled into Postgres
- the instruction event trail by default

Optional query parameter:

- `include_events=true|false`

### `POST /v1/instructions/{instruction_id}/submit-entry`

Submits a persisted `ENTRY_PENDING` instruction to the broker through the primary IBKR client session.

It currently:

- loads the persisted canonical instruction from Postgres
- requires current state `ENTRY_PENDING`
- resolves and submits the broker order using the same sizing and contract logic as manual submit
- stores `broker_order_id`, `broker_perm_id`, `broker_client_id`, and `broker_order_status` on the instruction record
- writes an `entry_order_submitted` event
- moves instruction state to `ENTRY_SUBMITTED`

This is the first DB-backed execution bridge between durable instructions and IBKR.

### `POST /v1/instructions/{instruction_id}/cancel-entry`

Cancels a persisted `ENTRY_SUBMITTED` entry order through the primary IBKR client session.

It currently:

- looks up the persisted instruction and its `broker_order_id`
- cancels the broker order
- updates `broker_order_status`
- writes an `entry_order_cancelled` event
- moves instruction state to `ENTRY_CANCELLED`

### `POST /v1/runtime/run-once`

Runs one MVP execution-runtime cycle against persisted instructions.

It currently:

- submits any due `ENTRY_PENDING` instructions
- fetches IBKR open orders and executions
- reconciles entry fills into persisted state
- submits a take-profit exit after a full entry fill when `exit.take_profit_pct` is present
- submits stop-loss and catastrophic stop-loss exits after a full entry fill when configured
- submits a forced market exit when `force_exit_next_session_open` is due from the Stockholm session calendar
- marks instructions `COMPLETED` after exit fills are reconciled

Example request body:

```json
{
  "now_at": "2026-04-13T09:00:00+02:00",
  "instruction_ids": ["2026-04-13-GTW05-long_risk_book-AAPL-long-01"],
  "timeout": 10
}
```

Important current behavior:

- this is a polling-style MVP runtime cycle, not the final long-lived broker process
- it uses the primary IBKR client session
- it is safe for manual operator-driven testing when pointed at a non-production broker session
- when `instruction_ids` is provided, the cycle only touches that selected set
- it now retries transient IBKR client-id reuse / reconnect churn a small number of times before failing the cycle
- a persistent runtime-owned IBKR connection is still the next step
- when the selected work is virtual-only, the cycle skips IBKR snapshot fetches
  and performs order and fill evaluation against virtual market-watch quotes

### `POST /v1/instructions/schedule-preview`

Accepts the canonical instruction batch and returns a read-only runtime schedule view.

It currently:

- projects `entry.submit_at` and `entry.expire_at` into both UTC and the configured runtime timezone
- makes the active entry window explicit before we wire order submission to it
- resolves `force_exit_next_session_open` for Stockholm instruments from the shared q-data session calendar

Current scheduling rule:

- the runtime default timezone is `Europe/Stockholm`
- Stockholm next-session exits come from the local q-data session calendar
- next-session exits are not guessed from wall-clock dates

This matches the current project scope: Stockholm first.

### `POST /v1/instructions/validate`

Accepts a JSON instruction batch payload and validates it against the execution contract.

The canonical request format is defined in [instruction-contract.md](instruction-contract.md).

## Running it

Important config is read from the repo-root `.env` file automatically.

Install the server dependencies in your environment, then run:

```bash
source .venv/bin/activate
python3 -m ibkr_trader.api.server
```

Optional development reload:

```bash
source .venv/bin/activate
python3 -m ibkr_trader.api.server --reload
```

## Next server steps

The natural next endpoints are:

1. keep a long-lived runtime-owned IBKR connection instead of reconnecting per cycle
2. persist broker callbacks and fills beyond polling-only reconciliation
3. add restart reconciliation against IBKR open orders, executions, and positions
4. turn tick-stream sampling into a long-lived local streaming service
