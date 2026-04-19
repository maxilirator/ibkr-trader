# Plan

This document is the current working plan for turning `ibkr-trader` from a solid MVP into a production-grade trading platform.

The main adjustment from the earlier discussion is important:

- agents do **not** only need instruction-state events
- agents need **live market data streams**
- agents should react to those streams independently
- agents should still submit orders through **our trading API**, not directly to brokers

So the target system is not one service. It is a small platform.

## Core Principles

### 1. Execution is separate from market data

The broker-owning execution runtime and the live market-data runtime are different concerns.

- execution is stateful, conservative, and durable
- market data is high-frequency, fan-out oriented, and subscription-driven

They can share broker adapters, but they should not be the same service.

### 2. Agents consume our streams and submit to our API

Agents should:

- subscribe to our normalized live data
- subscribe to our operational events when relevant
- submit trading instructions to our API

Agents should not open direct IBKR sessions.

### 3. The ledger is the durable truth

Instructions are intent.

The ledger must hold:

- broker orders
- order lifecycle events
- fills
- fees
- cash movements
- account snapshots
- position snapshots
- reconciliation results

The UI should read projections built from the ledger, not trigger fresh broker calls on page load.

### 4. Broker support must stay generic

IBKR is first, but the schema and internal contracts should leave room for:

- other traditional brokers
- crypto exchanges
- broker-specific market-data adapters

We should not make the core DB model IBKR-shaped.

### 5. UI is decoupled from the execution API

The operator UI should not be coupled to submit/cancel internals.

The clean shape is:

- write API for agents and operator actions
- read API or DB-backed projections for the UI

## Target System

### A. Market Data Runtime

Purpose:

- maintain live subscriptions
- normalize incoming ticks
- publish them to downstream consumers
- persist raw and bar data

Responsibilities:

- broker/exchange market-data adapters
- symbol subscription management
- tick normalization
- local publish-subscribe fan-out
- parquet persistence
- bar building
- feed health tracking

Initial target:

- IBKR tick streams for the instruments we trade
- native-currency ticks and bars
- Stockholm first

Likely interfaces:

- internal stream transport: WebSocket and/or SSE for immediate downstream use
- durable sink: parquet datasets in `q-data`
- optional future bus: Redis Streams or NATS if fan-out becomes heavier

### B. Trading API

Purpose:

- accept instructions from multiple agents
- validate and persist them
- expose safe operator actions

Responsibilities:

- authenticated instruction ingestion
- idempotency
- account and broker routing
- instruction validation
- instruction-set cancellation
- kill switch actions

The trading API should not be the component that owns long-lived execution state transitions by itself. It should hand work to the runtime.

### C. Execution Runtime

Purpose:

- own the live broker session
- schedule and submit orders
- process broker callbacks
- reconcile state after restart

Responsibilities:

- persistent broker connection
- submit/cancel/update orders
- bracket and protective exit logic
- next-session forced exits
- fill reconciliation
- broker callback capture
- startup reconciliation
- kill switch enforcement

This is the critical operational core.

### D. Ledger and Read Models

Purpose:

- provide durable operational truth
- provide fast UI queries
- support reconciliation and audit

Core record families:

- instruction
- instruction_event
- broker_order
- broker_order_event
- execution_fill
- commission_fee
- account_snapshot
- position_snapshot
- cash_ledger_entry
- reconciliation_run
- alert

Read models should then support:

- current accounts
- current positions
- current open orders
- current live instructions
- recent fills
- broker rejects and warnings
- feed and runtime health

### E. Operator UI

Purpose:

- give a reliable operational view
- give a very small set of safe controls

Initial controls:

- kill switch
- cancel instruction set

Initial visibility:

- all broker accounts
- account status
- open orders
- positions
- recent fills
- recent instructions
- warnings
- broker rejects
- runtime health
- feed health

Explicitly deferred for now:

- paper/live switch as a first-class UI feature
- flatten account button
- pause new entries button

## Information We Need in the System

This is the minimum useful operational information set.

### Brokers and Accounts

- all configured brokers
- all broker accounts
- account labels and ownership
- account base currency
- account current status
- account net liquidation
- account cash
- account buying power
- account margin fields

### Instructions and Orders

- all active instructions
- instruction source agent
- instruction timestamps
- instruction state
- broker order IDs
- broker perm IDs
- broker status
- instruction-set membership

### Execution and Holdings

- recent fills
- current positions
- average prices
- realized and unrealized PnL
- current holdings by account
- cash movements
- fees and commissions

### Broker and Runtime Health

- broker connection status
- current client IDs in use
- callback health
- restart reconciliation result
- recent broker errors
- recent broker rejects
- warnings requiring operator attention

### Market Data

- active subscriptions
- feed health per source
- symbol subscription coverage
- latest tick timestamps
- bar-ingestion health
- shortability snapshots

## DB Direction

The DB should be high-level enough for multiple brokers.

Every execution-related core row should be able to answer:

- which broker?
- which broker account?
- which instrument?
- which external broker identifier?
- which internal instruction/order identifier?

Recommended generic columns:

- `broker_kind`
- `broker_account_key`
- `external_order_id`
- `external_perm_id`
- `external_execution_id`
- `raw_payload`

The current `instruction` table is fine as an MVP anchor, but it should stop carrying too much derived state once ledger tables land.

## Agent Interaction Model

### Agents need two paths

#### 1. Live data subscription path

Agents subscribe to:

- live ticks
- live bars if we publish them
- operational alerts if useful

This is for reaction logic.

#### 2. Instruction submission path

Agents submit:

- structured instructions
- cancel requests if needed

This is for execution.

That split is important. Reaction happens on the data side. Trading authority stays on the execution side.

## Minimal Production Controls

For the first production-grade iteration, the control bar can stay intentionally low.

Must have:

- kill switch
- cancel instruction set
- warnings
- broker rejects visible

Can wait:

- broad hard risk controls
- flatten account
- pause-new-entries
- rich paper/live UX

Even with a lighter control bar, we still need durable audit and reconciliation. Those are not optional.

## Phases

## Phase 1: Harden the Core Runtime

Goal:

- make the existing trader trustworthy before expanding surface area

Build:

- one long-lived execution runtime
- broker callback persistence
- startup reconciliation
- proper ledger tables for orders, order events, and fills
- account snapshot persistence
- position snapshot persistence

Exit condition:

- restart does not lose operational truth
- fills and orders are persisted independently of instruction summary fields

## Phase 2: Build the Live Data Runtime

Goal:

- let agents react to normalized live data in real time

Build:

- long-lived market-data service
- subscription manager
- normalized tick stream
- downstream subscription API
- raw parquet persistence
- bar builder
- feed health metrics

Exit condition:

- agents can subscribe to our live stream without touching IBKR directly

## Phase 3: Split Write API and Read Models

Goal:

- decouple operator visibility from broker traffic

Build:

- write API for instruction submission and operator controls
- read API backed by Postgres projections
- account overview projections
- open-order projections
- fill and position projections
- warnings and reject projections

Exit condition:

- dashboard loads entirely from local state and not direct broker pulls

## Phase 4: Production UI

Goal:

- provide the minimum operator cockpit needed to run the system cleanly

Build:

- accounts screen
- live instructions screen
- open orders screen
- fills screen
- positions screen
- warnings and broker rejects
- kill switch
- cancel instruction set

Exit condition:

- a human can understand current operational state in a few seconds

## Phase 5: Multi-Broker Expansion

Goal:

- make the platform ready for non-IBKR execution and market data

Build:

- broker adapter interface cleanup
- broker-agnostic order and fill model
- market-data adapter interface
- account and venue routing

Exit condition:

- adding a new broker does not require rethinking the core schema

## Immediate Next Build Slice

The best next implementation slice is:

1. ledger tables for broker orders, order events, executions, account snapshots, and position snapshots
2. long-lived execution runtime with callback persistence
3. startup reconciliation
4. read models for accounts, orders, positions, and fills

That gives us the base needed for both:

- a production-grade operator UI
- a future agent-facing live data platform

## Non-Goals Right Now

- broad enterprise auth system
- complex paper/live environment productization
- full hard risk framework
- generic multi-market abstractions everywhere before we have the ledger right

We should stay disciplined and build the durable center first.
