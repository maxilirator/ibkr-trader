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
- optional tick streams where justified
- shortability and borrow metadata
- fills, order events, and account snapshots

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
  "what_to_show": ["TRADES", "MIDPOINT", "BID", "ASK", "ADJUSTED_LAST"],
  "use_rth": true,
  "max_symbols": 25,
  "start_after": null,
  "include_remapped": false,
  "sleep_seconds": 0.05
}
```

Current behavior:

1. Load the broker-resolved Stockholm contract master.
2. Select:
   - all `resolves_cleanly` names
   - optionally `resolves_suspiciously_remapped` names only when explicitly enabled or approved
3. For each selected contract:
   - request `1 D` of `1 min` bars ending at Stockholm close for `as_of_date`
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
- the current endpoint still resolves via the Stockholm identity map on each run; the next hardening step is to move it to a durable broker-resolved master
- contract-master refresh should be a separate slower job, not part of the nightly bar backfill
- the calling repo should handle persistence and keep the nightly job idempotent at the `(date, conId, bar_size, what_to_show)` level

## Recommended near-term roadmap

1. Keep the configured IB Gateway connection healthy and stable.
2. Add Stockholm broker order placement and cancel flow on top of persisted `ENTRY_PENDING` instructions.
3. Add execution-event persistence and replay.
4. Add Stockholm market-data ingestion for a small symbol set.
5. Add shortability collection and nightly snapshots.
