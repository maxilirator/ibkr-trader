# Current Status

This document is the operational snapshot of where the repo is right now.

## Scope

- Current execution scope is Stockholm equities first.
- Current runtime timezone is `Europe/Stockholm`.
- Current session-calendar source is the shared q-data Stockholm session calendar.
- Current broker boundary is the official IBKR Python API behind a local-only FastAPI service.

## Working now

- [x] Repo-local `.env` loading
- [x] Official IBKR Python client installed in `.venv`
- [x] Local-only FastAPI wrapper
- [x] IBKR connectivity probe
- [x] Read-only contract resolution
- [x] Read-only account summary
- [x] Historical bars endpoint
- [x] Tick-stream sample endpoint
- [x] Read-only order preview
- [x] Manual paper order submit/cancel endpoints
- [x] Durable instruction submit endpoint
- [x] Instruction persistence in Postgres
- [x] Persisted entry submit/cancel flow
- [x] MVP runtime cycle for due-entry submit, fill reconciliation, protective exit submit, and next-session forced exit
- [x] FX-aware sizing for `fraction_of_account_nav`
- [x] Whole-share normalization for dynamic stock sizing
- [x] Stockholm-first schedule preview
- [x] Next-session-open resolution from q-data Stockholm session calendar
- [x] Initial SQLAlchemy ORM control-plane schema

## Verified behavior

- TWS API connection works from this repo through the local IBKR desktop session.
- Contract resolution works against live broker metadata.
- Account summary works through the diagnostic client.
- Historical bars work for entitled symbols.
- Tick-stream sampling is wired, and currently surfaces broker entitlement errors directly.
- Order preview keeps prices and notionals in instrument currency.
- Dynamic stock sizing from notional or NAV is rounded down to whole shares before execution.
- Manual paper submit/cancel works on NY paper symbols through the local API.
- Submit persists instructions and an initial `instruction_submitted` event in Postgres.
- Persisted instructions can move through `ENTRY_PENDING -> ENTRY_SUBMITTED -> ENTRY_CANCELLED` with broker IDs stored on the instruction record.
- The runtime can now:
  - auto-submit due `ENTRY_PENDING` instructions
  - reconcile entry fills from IBKR executions
  - submit a take-profit exit after a full entry fill
  - submit stop-loss and catastrophic stop-loss exits after a full entry fill when configured
  - submit a forced market exit at the resolved next Stockholm session open
  - mark an instruction `COMPLETED` after exit fill reconciliation
  - run against a selected `instruction_ids` set for safer operator-driven paper testing
  - retry transient IBKR client-id reuse / reconnect churn within the MVP cycle
- Stockholm schedule preview resolves the next session open from the local q-data calendar.

## Not built yet

- [ ] Broker callback persistence
- [ ] Restart reconciliation
- [ ] Parquet market-data ingestion worker
- [ ] Shortability ingestion
- [ ] Live-trading controls and kill switch

## Current API surface

- `GET /healthz`
- `POST /v1/ibkr/probe`
- `POST /v1/contracts/resolve`
- `POST /v1/accounts/summary`
- `POST /v1/market-data/historical-bars`
- `POST /v1/market-data/tick-stream-sample`
- `POST /v1/orders/preview`
- `POST /v1/orders/submit`
- `POST /v1/orders/{order_id}/cancel`
- `POST /v1/instructions/submit`
- `GET /v1/instructions/{instruction_id}`
- `POST /v1/instructions/{instruction_id}/submit-entry`
- `POST /v1/instructions/{instruction_id}/cancel-entry`
- `POST /v1/instructions/schedule-preview`
- `POST /v1/instructions/validate`
- `POST /v1/runtime/run-once`

## Current design decisions

- Stockholm comes first; we are not designing the runtime around generic multi-market support yet.
- Market data and execution prices stay in instrument currency.
- Account-currency conversion is explicit and mainly used for sizing and controls.
- The execution runtime owns scheduling and multi-day logic.
- The Stockholm session calendar comes from the shared q-data dataset, not from ad hoc wall-clock rules.
- Postgres is for control-plane state; bars and raw market data will live in parquet datasets.

## Next implementation step

Harden the runtime into a long-lived broker-owning process:

- keep one persistent IBKR runtime connection instead of reconnecting per action
- persist broker callbacks directly instead of polling only
- add restart reconciliation against IBKR open orders, executions, and positions
- then promote tick-stream sampling into a long-lived local stream for parquet ingestion
