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

### 3A. Local Control API

Thin local-only FastAPI service in front of the execution runtime.

Responsibilities:

- accept validated local HTTP calls from internal services
- keep the official IBKR Python API isolated in one process boundary
- expose probe, validation, and later order-management endpoints
- enforce loopback-only access by default

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

## Recommended near-term roadmap

1. Keep the configured IB Gateway connection healthy and stable.
2. Add Stockholm broker order placement and cancel flow on top of persisted `ENTRY_PENDING` instructions.
3. Add execution-event persistence and replay.
4. Add Stockholm market-data ingestion for a small symbol set.
5. Add shortability collection and nightly snapshots.
