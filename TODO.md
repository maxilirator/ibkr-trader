# TODO

This file is the active implementation tracker for the production-grade trader plan.

## Next

- [ ] Operator controls for reconciliation warnings and broker attention
- [ ] Operator write actions for reconciliation issues and broker-attention items

## Done

- [x] Timed follow-up exits anchored to live market at activation time:
  instructions can now support "buy now, then at 10:30 place a sell limit
  5% above the market price observed at 10:30", and the live SIVE smoke
  test on 2026-04-20 bought 1 share and submitted the delayed 5% exit
- [x] Long-lived execution runtime that owns one persistent broker session:
  the API host can now run the execution loop continuously, with a durable
  runtime lease, startup reconciliation gate, persisted heartbeat/status,
  and operator-visible lifecycle state for whether execution is running,
  degraded, blocked, stopped, or failed
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
- [x] Order preview and live broker submission no longer depend on IBKR
  account-summary subscriptions:
  broker account selection and account-based sizing now use per-account
  account updates, and the operator dashboard no longer presents scheduled
  runtime entries as a manual submit workflow
- [x] Order prices are normalized against IBKR market rules before submit:
  live entry and exit order prices now snap to valid broker tick increments
  instead of sending raw model prices that IBKR rejects with error 110
- [x] Dashboard now runs as a long-lived Node service instead of a temporary
  `vite preview` process:
  the SvelteKit build is started through a dedicated `systemd --user` unit on
  the server so the site survives disconnects and reboot like the API host

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
