# Architecture Draft

## System goal

Build a production-capable execution platform for Interactive Brokers that separates:

- research and AI instruction generation
- instruction validation and risk controls
- execution orchestration
- broker integration
- data ingestion and storage

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

### 3. IBKR Broker Adapter

Wraps the chosen IBKR API and exposes a clean internal interface.

Responsibilities:

- connect and reconnect to IB Gateway
- resolve contracts and exchange routing
- place, modify, and cancel orders
- subscribe to order status, execution, account, and market-data callbacks
- translate broker events into internal events

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

## Storage

Suggested first pass:

- Postgres for instructions, events, positions, and reconciled broker state
- object storage or partitioned tables for larger raw market-data payloads

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
- New York market calendar awareness
- complete audit history for compliance and debugging

## Recommended near-term roadmap

1. Build a paper-trading TWS API connection through IB Gateway.
2. Implement contract lookup and order placement for US equities.
3. Add execution-event persistence and replay.
4. Add scheduled-entry and next-open-exit flows.
5. Add market-data ingestion for a small symbol set.
6. Add shortability collection and nightly snapshots.

