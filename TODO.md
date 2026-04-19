# TODO

This file is the active implementation tracker for the production-grade trader plan.

## In Progress

- [ ] Long-lived execution runtime that owns one persistent broker session:
  the canonical session plumbing and startup boot gate are in place, but the
  always-on service still needs clearer supervision and runtime lifecycle semantics

## Next

- [ ] Operator controls for reconciliation warnings and broker attention
- [ ] Operator write actions for reconciliation issues and broker-attention items
- [ ] Replace the temporary dashboard `vite preview` process with a clearer long-lived production service step

## Done

- [x] Runtime snapshots no longer depend on IBKR account-summary subscriptions:
  the background broker monitor now uses per-account account updates for the
  configured accounts, and the sync wrapper always unsubscribes account-summary
  and account-update requests even when IBKR errors or times out
- [x] Background broker heartbeat and snapshot refresh inside the API server:
  keep a durable IB Gateway heartbeat running, persist fresh runtime snapshots
  into the ledger, and expose monitor status through healthz for the UI
- [x] Ledger dashboard page backed by the durable ledger:
  add a separate operator-facing page for append-only instruction events,
  broker order events, fills, control events, cancellation requests, and
  reconciliation issues, with optional focus on a single instruction
- [x] Window-aware instruction management in the operator dashboard:
  stale instructions no longer show misleading submit buttons after expiry,
  and rows now explain whether they are scheduled, open, or expired with
  cancellation and ledger review paths when appropriate

- [x] Phase 1 ledger foundation:
  add broker accounts, broker orders, broker order events, execution fills,
  account snapshots, and position snapshots to the database
- [x] Persist live broker runtime snapshots into the ledger:
  account snapshots, position snapshots, open orders, and executions now
  write durable rows instead of staying in memory only
- [x] Persist broker submissions and cancellations into the ledger:
  entry submits, protective exits, forced exits, and persisted entry
  cancellations now write durable broker-order rows and events immediately
- [x] Persist callback-driven broker order status updates, rejects, and
  completions directly into the ledger so we do not wait for the next
  snapshot or reconciliation pass
- [x] Persist durable reconciliation-run audit rows for every runtime cycle:
  runtime passes now write a real ledger history with issue rows and
  action summaries instead of leaving reconciliation outcomes in memory only
- [x] Startup reconciliation is now a runtime boot gate:
  the persistent runtime performs startup reconciliation before entering
  normal trading cycles and blocks on issues unless explicitly overridden
- [x] Read models for accounts, positions, open orders, fills, warnings,
  and reconciliation history, exposed through the operator snapshot API
- [x] Dashboard moved onto the durable operator read model instead of the
  live broker runtime snapshot, so the UI reflects persisted truth first
- [x] Kill switch and instruction-set cancellation wired through durable
  state, enforced by the API and runtime, and surfaced in the dashboard
- [x] Dashboard write actions for kill switch and instruction-set
  cancellation, backed by the real local trading API and real durable state
- [x] Row-level operator actions for recent instructions and open broker
  orders through the dashboard, backed by the existing local trading API

## Later

- [ ] Long-lived live market-data runtime for agent subscriptions
- [ ] Parquet persistence for raw ticks and bar builds
- [ ] Decoupled read API for the operator UI
- [ ] Multi-broker adapter cleanup after the ledger center is stable
