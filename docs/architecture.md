# Architecture Draft

## System goal

Build a production-capable execution platform for Interactive Brokers that separates:

- research and AI instruction generation
- instruction validation and risk controls
- execution orchestration
- broker integration
- data ingestion and storage

## Current implementation scope

The runtime is currently being built for **Stockholm equities first**.

- operator timezone: `Europe/Stockholm`
- next-session scheduling source: shared q-data Stockholm session calendar
- current broker-facing work: validation, preview, and scheduling before submit

## Core services

### 1. Instruction Ingest

Receives strategy output from AI or research services and converts it into a strict internal schema.

Responsibilities:

- reject ambiguous instructions
- normalize timestamps and timezones
- resolve portfolio and account targets
- attach strategy metadata and provenance

### 2. Execution Orchestrator

Owns the state machine for every instruction.

Responsibilities:

- schedule future actions
- respond to fills, partial fills, cancels, and rejects
- create follow-up orders
- perform next-session transitions
- recover safely after restarts

The orchestrator is where multi-day logic belongs.

Scheduling rule:

- actionable timestamps should be normalized into UTC for execution and into `Europe/Stockholm` for operator-facing runtime views
- Stockholm session transitions should use the shared q-data session calendar as the primary source
- "next session open" must be resolved from the Stockholm session calendar, not guessed from wall-clock dates

### 3. IBKR Broker Adapter

Wraps the chosen IBKR API and exposes a clean internal interface.

Responsibilities:

- connect and reconnect to IB Gateway
- resolve contracts and exchange routing
- place, modify, and cancel orders
- subscribe to order status, execution, account, and market-data callbacks
- translate broker events into internal events

### 3A. Trader Control API

Thin FastAPI service in front of the execution runtime, exposed to the trusted LAN.

Responsibilities:

- accept validated local HTTP calls from internal services
- accept validated LAN HTTP calls from agents and operator tools
- keep the official IBKR Python API isolated in one process boundary
- expose probe, validation, and later order-management endpoints
- keep broker access isolated even when the trader API itself is LAN-visible

### 3B. Virtual Execution Adapter

Local execution adapter for accounts whose key starts with `virtual`, such as
`virtual0001`.

Responsibilities:

- create virtual broker-account records and snapshots
- accept virtual market-watch quotes through the Trader Control API
- route virtual entry, exit, cancel, and market-price reads away from IBKR
- fill virtual orders when the virtual quote crosses the order condition
- write broker orders, fills, account snapshots, and position snapshots into the
  same ledger tables as real broker execution
- mark every virtual row with `is_virtual=true`

Current simulation rules:

- requested quantity is intentionally ignored for now and every fill uses
  `quantity="1"`
- every fill records a fixed `15 SEK` commission
- virtual-only runtime cycles do not fetch a broker runtime snapshot from IBKR
- the operator, ledger, and RL dashboards display virtual accounts and rows with
  a `Virtual` badge

### 4. Risk and Controls

Independent guardrail layer between instruction intake and broker execution.

Responsibilities:

- max notional and max position checks
- duplicate-order prevention
- stale signal rejection
- allowed symbol universe
- trading-hours policy
- kill switch and circuit breakers

### 5. Data Backend

Internal market and execution data store.

Suggested responsibilities:

- top-of-book snapshots
- intraday bars
- persistent live tick/market streams for active intraday agents
- shortability and borrow metadata
- fills, order events, and account snapshots

For RL candidate lists, the live path is a persistent market-data stream:
subscribe the active names once, maintain an in-memory 1-minute OHLC buffer from
last-price ticks, and let the RL observation builder aggregate those source bars
into the 5-minute `phase1_intraday_ohlc_v1` contract. The active name count is
dynamic per booster run, so a 5-name day and a 30-name day use the same stream
path. Do not implement this as one historical-bar request per name every minute.

### 5A. Stockholm Intraday Backfill

For Stockholm intraday bars, the right replacement shape for EODHD is:

- treat IBKR-resolved Stockholm stocks as the canonical nightly universe
- maintain a broker-resolved contract master instead of relying on raw q-data slugs
- classify names into:
  - resolves cleanly
  - resolves suspiciously/remapped
  - does not resolve at IBKR

Operational rule:

- the nightly intraday collector should use only:
  - cleanly resolved names by default
  - explicitly approved remapped names when we have validated the broker symbol lineage
- unresolved names should stay out of the nightly IBKR backfill and remain on the exception list

Current live evidence from `quant`:

- current Stockholm universe scanned: `955`
- resolves cleanly at IBKR: `705`
- resolves suspiciously/remapped: `21`
- does not resolve at IBKR: `229`
- live Friday `2026-04-24` `5 min` sample across `40` cleanly resolved names:
  - `40/40` historical-bar requests succeeded
  - `29/40` returned the full `102` Stockholm session bars
  - the remaining `11/40` returned fewer bars, but still succeeded
  - average request latency was about `0.309s`

Interpretation:

- for the names IBKR can actually trade and resolve, nightly intraday retrieval is strong enough to replace EODHD for this slice
- sparse `TRADES` bars are expected for thinner names and should not be treated as failed retrievals
- if we need a dense price-only grid for thin names, a controlled `MIDPOINT` fallback can be added later, but `TRADES` should remain the primary series
- the right first collector shape is nightly `1 min` data capture, then downstream rollups into `5 min` and higher bars

Currency rule for stored market and execution data:

- bars, ticks, quotes, order prices, and fills should be stored in the instrument's native trading currency
- every stored market or execution record should carry an explicit currency code
- account-NAV and portfolio-risk views may also keep account-currency projections as separate derived fields
- account-currency projections must not replace the native instrument-currency record

## Storage

Suggested first pass:

- Postgres for instructions, events, positions, and reconciled broker state
- object storage or partitioned tables for larger raw market-data payloads

For control-plane tables, start with SQLAlchemy ORM models so the execution and metadata schema stays close to the Python domain model. Use `create_all()` only for the initial bootstrap phase; once the schema is moving, add migrations.

If intraday storage grows quickly, evaluate TimescaleDB or a dedicated columnar store later.

## Execution lifecycle

1. AI platform submits an instruction.
2. Instruction is validated and assigned an internal ID.
3. Orchestrator schedules entry behavior.
4. Broker adapter sends order to IBKR at the correct time.
5. Broker callbacks update execution state.
6. Exit policies are attached after fill.
7. Next-session logic runs off exchange calendar events.
8. Final outcome is written to the audit trail.

## Operational requirements

- one source of truth for instruction state
- idempotent action dispatch
- restart-safe reconciliation on boot
- clear separation between paper and live trading
- exchange calendar awareness with Europe/Stockholm as the default runtime timezone
- complete audit history for compliance and debugging

## Nightly Collector Endpoint

Implemented control-plane endpoint:

- `POST /v1/market-data/stockholm-intraday-backfill`

Purpose:

- collect one trading day of Stockholm intraday bars every night from IBKR
- return them page by page to the calling repo or batch runner
- make the run restart-safe and resumable through cursor paging

Current request shape:

```json
{
  "as_of_date": "2026-04-24",
  "bar_size": "1 min",
  "what_to_show": ["TRADES"],
  "use_rth": true,
  "max_symbols": 25,
  "start_after": null,
  "include_remapped": false,
  "sleep_seconds": 0.05,
  "max_runtime_seconds": 55
}
```

Current behavior:

1. Load the broker-resolved Stockholm contract master.
2. Select:
   - all `resolves_cleanly` names
   - optionally `resolves_suspiciously_remapped` names only when explicitly enabled or approved
3. For each selected contract:
   - request `1 D` of `1 min` raw trade bars ending at Stockholm close for `as_of_date`
   - return bars in native trading currency
   - keep the resolved IBKR identifiers with the payload:
     - `conId`
     - `symbol`
     - `localSymbol`
     - `primaryExchange`
     - `ISIN`
4. Return a batch summary with:
   - requested symbol count
   - completed symbol count
   - failed symbol count
   - skipped remap count
   - resolution classification counts
   - `next_cursor` for the next page

Current response shape:

```json
{
  "accepted": true,
  "market": "stockholm",
  "series_mode": "paged_batch",
  "summary": {
    "requested_symbol_count": 25,
    "ok_count": 24,
    "lookup_error_count": 1
  },
  "universe": {
    "next_cursor": "volcar-b"
  },
  "entries": []
}
```

Implementation notes:

- use one dedicated diagnostic client ID for the collector
- pace requests gently and keep the job single-session
- use `max_runtime_seconds` so a nightly page returns partial results with a
  resumable cursor instead of timing out at the HTTP client
- collect optional `MIDPOINT`, `BID`, and `ASK` series in smaller second-pass
  pages if they are needed
- do not request `ADJUSTED_LAST` through this dated intraday endpoint; IBKR
  rejects explicit end dates for that series, so adjusted prices should come
  from downstream adjustment factors or a separate adjusted-close source
- the current endpoint still resolves via the Stockholm identity map on each run; the next hardening step is to move it to a durable broker-resolved master
- contract-master refresh should be a separate slower job, not part of the nightly bar backfill
- the calling repo should handle persistence and keep the nightly job idempotent at the `(date, conId, bar_size, what_to_show)` level

## RL Trader Execution Layer

For the operational setup version of this section, see [RL Setup Guide](rl-setup.md).

The next execution layer should let a promoted RL policy trade through the same
durable instruction and broker-order path as every other strategy in this repo.

Strong boundary:

- the RL agent should emit **strategy actions**, not raw IBKR orders
- this trader should translate those actions into our normal validated
  instruction contract
- the ledger, reconciliation, and operator controls should stay shared

### Account model

Recommended real-broker deployment model:

- use a real IBKR account as the top-level execution boundary
- keep `account_key` equal to that real broker account
- use `book_key` as the internal strategy sleeve, such as
  `rl_short_trial36_live_01`

Reasoning:

- shortability, margin, minimum-equity rules, cash, and statements are all real
  broker-account concerns
- autonomous RL strategies deserve hard financial separation when possible
- internal virtual books are still useful, but only **inside** a real account

Practical rule:

- start with one live RL deployment per real IBKR account
- only allow multiple autonomous RL deployments inside the same account later,
  once we add explicit cross-model position arbitration

Virtual exception:

- for local RL testing without paper-account live data, bind the deployment to a
  virtual account such as `virtual0001`
- use `mode="virtual"`
- publish quotes through `/v1/virtual/market-watch`
- let the normal instruction runtime submit and reconcile the virtual orders
- keep virtual and real rows distinguishable through `is_virtual=true`

### Core registry objects

Suggested durable objects:

- `trader_model`
  - immutable model identity and artifact lineage
  - source workflow path
  - promoted checkpoint path
  - observation contract
  - action-space version
  - default execution mapping version
- `trader_deployment`
  - one live, paper, or virtual binding of a model
  - immutable `account_key`
  - immutable `book_key`
  - mode: `paper`, `live`, or `virtual`
  - status: `draft`, `paused`, `running`, `degraded`, `stopped`
  - approved symbol universe
  - deployment-level risk limits
- `trader_action`
  - append-only action log emitted by the RL runtime
  - observed-at timestamp
  - symbol
  - action name
  - decision metadata
  - resulting instruction ID or rejection detail
- `trader_heartbeat`
  - runtime liveness, last bar seen, last policy step, and degradation reason

For the first pass, the observation contract and action-space metadata can live
as JSON on `trader_model`; we can normalize later if we need multiple schemas.

### Short RL action space

The promoted short-side action set from the current research line is:

- `skip`
- `wait`
- `market_entry`
- `cancel_entry`
- `exit_market`
- `clear_exit`
- `entry_prevclose_88bp`
- `exit_tp_180bp`

These should map to trader behavior like this:

- `skip`
  - do nothing for the symbol and mark the decision as intentionally idle
- `wait`
  - advance one observation step with no execution change
- `market_entry`
  - create a short `SELL` market entry instruction on the deployment account
- `cancel_entry`
  - cancel an existing pending short entry instruction for that deployment and symbol
- `exit_market`
  - flatten an open short position at market and cancel conflicting pending exits first
- `clear_exit`
  - cancel a pending take-profit or other exit order while leaving the short position open
- `entry_prevclose_88bp`
  - create a short limit entry using the prior close as reference and a `+0.88%`
    offset
- `exit_tp_180bp`
  - create or replace the take-profit exit at `1.80%` favorable move from fill for
    the short

### Symbol state machine

To keep the action set safe, every deployment-symbol pair should have an explicit
state machine:

- `FLAT`
- `ENTRY_PENDING`
- `SHORT_OPEN`
- `EXIT_PENDING`
- `BLOCKED`

Valid actions by state should be constrained:

- `FLAT`
  - `skip`
  - `wait`
  - `market_entry`
  - `entry_prevclose_88bp`
- `ENTRY_PENDING`
  - `wait`
  - `cancel_entry`
- `SHORT_OPEN`
  - `wait`
  - `exit_market`
  - `exit_tp_180bp`
- `EXIT_PENDING`
  - `wait`
  - `clear_exit`
  - `exit_market`

Any invalid action should be:

- rejected before execution
- written to the append-only `trader_action` log
- surfaced in the RL dashboard as `invalid_action`

### Observation contract

Every promoted RL model should declare the bar and feature contract it expects.

For the current short research line, the model metadata should carry at least:

- the promoted workflow and checkpoint path
- model bar family, for example `phase1_intraday_ohlc_v1`
- target model bar interval, currently `5m`
- source adapter, currently `ibkr_live_market_stream_1m_to_phase1_5m_ohlc_v1`
- required series, such as:
  - `TRADES`
  - optional `MIDPOINT`, `BID`, `ASK`, `ADJUSTED_LAST`
- lookback or sequence length
- whether market context is required
- whether vol-normalized state is required
- feature-schema version

The execution layer should reject a deployment start if the market-data backend
cannot satisfy the declared observation contract.

### Execution contract translation

The RL runtime should not invent its own broker protocol. It should translate
actions into the existing execution contract:

- short entries must use:
  - `intent.side = SELL`
  - `intent.position_side = SHORT`
- deployment sizing rules should be explicit and bounded
- take-profit exits should reuse `exit.take_profit_pct`
- next-session forced flattening should remain available through
  `exit.force_exit_next_session_open`

This preserves:

- short-sale validation
- account and margin validation
- duplicate-order prevention
- restart-safe reconciliation

### Operator UX

Add a dedicated RL dashboard view, not just more rows in the general operator page.

Safety rule:

- dashboards must fail visibly when the API base URL, health response, read
  model, or required registry payload field is missing
- do not render synthetic account, model, deployment, action, or heartbeat data
  as a fallback

Suggested sections:

- `Deployments`
  - model
  - deployment ID
  - account
  - book
  - mode
  - status
  - last heartbeat
- `Action Stream`
  - continuous append-only list of model actions
  - symbol
  - state before
  - action
  - execution result
  - linked instruction ID
- `Risk`
  - live notional
  - open shorts
  - pending entries
  - rejected/invalid action count
  - kill-switch exposure
- `Health`
  - market-data freshness
  - feature freshness
  - last runtime error
  - broker account preflight status

### Immediate implementation slice

The safest first slice is:

1. add `trader_model`, `trader_deployment`, `trader_action`, and
   `trader_heartbeat`
2. add a deployment-bound action intake API that writes append-only action rows
3. translate valid actions into the existing instruction contract
4. add a read model and dashboard page for RL deployments and action history
5. allow virtual deployments for local simulation, while real broker
   deployments still bind to one real `account_key`

## Recommended near-term roadmap

1. Keep the configured IB Gateway connection healthy and stable.
2. Add Stockholm broker order placement and cancel flow on top of persisted `ENTRY_PENDING` instructions.
3. Add execution-event persistence and replay.
4. Add Stockholm market-data ingestion for a small symbol set.
5. Add shortability collection and nightly snapshots.
