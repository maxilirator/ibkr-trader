# IBKR Trader

Professional quant trading system scaffold for Interactive Brokers.

This repository is the starting point for a stateful execution platform that can:

- accept AI-generated trading instructions
- validate and normalize them into a strict internal format
- execute through Interactive Brokers
- react to fills, cancels, market open/close events, and risk events
- collect intraday market data and broker-side metadata such as shortability

## Current scope

This repo is currently scoped to **Stockholm equities first**.

- runtime timezone: `Europe/Stockholm`
- session calendar: shared q-data Stockholm session calendar
- initial broker integration: official IBKR Python API through local TWS / IB Gateway
- current work is focused on broker-safe validation, durable submit, and order preview before live broker submit
- manual NY paper order submit/cancel is now available for broker-path smoke testing

## Current direction

We are optimizing for a production-style architecture rather than a simple script.

The recommended first broker integration is the IBKR TWS API through IB Gateway. Per IBKR's current API overview, TWS API is their TCP socket API intended for fast paced, data intensive, and complex trading, with near-full parity with Trader Workstation and support for large numbers of streaming market data lines. Web API is useful, but for this repo the best starting point is TWS API plus our own orchestration layer.

Relevant official sources:

- https://ibkrcampus.com/campus/ibkr-api-page/getting-started/
- https://ibkrcampus.com/campus/ibkr-api-page/twsapi-doc/
- https://ibkrcampus.com/campus/ibkr-api-page/cpapi-v1/
- https://ibkrcampus.com/campus/ibkr-api-page/market-data-subscriptions/
- https://interactivebrokers.github.io/tws-api/tick_types.html

## Current status

See [docs/current-status.md](docs/current-status.md) for the working checklist, verified pieces, and the next implementation steps.

## First runnable broker step

We now include a small broker probe that is meant to validate the official TWS API connection path before we build order placement on top.

Expected paper-trading defaults:

- `IBKR_HOST=127.0.0.1`
- `IBKR_PORT=7497`
- `IBKR_CLIENT_ID=0`
- `IBKR_DIAGNOSTIC_CLIENT_ID=7`
- `IBKR_STREAMING_CLIENT_ID=8`

The `0` client ID is intentional. IBKR's current TWS API docs recommend connecting with `client_id=0` for optimal order-management functionality. In this repo we reserve `0` for the future long-lived trading runtime and use a separate diagnostic client ID for probe and read-only resolution calls.

See [docs/ib-gateway-setup.md](docs/ib-gateway-setup.md) for setup notes.

## Environment config

Important runtime settings now live in a repo-root `.env` file.

- the app auto-loads `.env` before building config
- real environment variables still win over `.env` values
- `.env` is gitignored, while `.env.example` remains the template

Current important settings include:

- app mode and timezone
- session calendar path
- database URL
- local API bind host and port
- IBKR host, port, primary client ID, diagnostic client ID, streaming client ID, and account ID

## Local API wrapper

The recommended service shape is:

- keep the official IBKR Python API as the broker core
- expose a small FastAPI control plane around it
- bind the API only to loopback, not to public or LAN interfaces

This gives the AI and orchestration layers a clean local HTTP interface without exposing the raw broker session to the network.

The initial FastAPI wrapper includes:

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
- `POST /v1/instructions/{instruction_id}/submit-entry`
- `POST /v1/instructions/{instruction_id}/cancel-entry`
- `POST /v1/instructions/schedule-preview`
- `POST /v1/instructions/validate`

See [docs/local-api.md](docs/local-api.md) for endpoint behavior and [docs/instruction-contract.md](docs/instruction-contract.md) for the upstream payload contract.

## Database foundation

The control-plane database is now set up to start with SQLAlchemy ORM models first.

- SQLAlchemy ORM models define the initial control-plane tables.
- `create_all()` is acceptable for this first bootstrap stage.
- Once the schema starts changing in a non-trivial way, we should add Alembic migrations rather than keep evolving tables ad hoc.

The first ORM tables are intentionally small:

- `instrument`
- `instruction`
- `instruction_event`

Create the initial schema with:

```bash
source .venv/bin/activate
python3 -m ibkr_trader.db.init_schema
```

## Scheduling default

The runtime scheduler is Stockholm-first.

- `APP_TIMEZONE` defaults to `Europe/Stockholm`
- `SESSION_CALENDAR_PATH` defaults to `../q-data/xsto/calendars/day_sessions.parquet`
- schedule preview converts every instruction into both UTC and Stockholm-local times
- Stockholm next-session exits resolve from the shared q-data session calendar when the instrument maps to Stockholm exchange codes

## What belongs in this system

- Instruction API: receives strategy output from AI or research systems.
- Execution orchestrator: owns multi-step and multi-day workflows.
- Broker adapter: translates internal actions into IBKR order and data calls.
- Risk engine: account guards, symbol guards, price band checks, kill switch.
- Event store: durable audit trail for every decision, callback, fill, and cancel.
- Data backend: captures intraday bars, ticks, account snapshots, and shortability.
- Currency rule: market data and execution prices stay in the instrument's native trading currency; account-currency views are derived separately.

## Why the orchestrator matters

An instruction such as:

`buy limit at 09:25, then after fill place take profit at +2%, stop loss at -15%, and if still open next morning sell at the open`

should not be treated as a single broker order. Some pieces can be expressed with native IBKR order features like bracket orders, OCA groups, `GoodAfterTime`, `GoodTillDate`, and order conditions, but the full lifecycle is better modeled as our own state machine with persistent scheduling and reconciliation.

## Repository layout

```text
.
├── docs/
├── src/ibkr_trader/
│   ├── brokers/
│   ├── domain/
│   ├── ibkr/
│   └── orchestration/
└── tests/
```

## First milestones

1. Persist broker callbacks and fills beyond submit/cancel.
2. Add execution reconciliation on startup.
3. Add Stockholm market-data ingestion into the parquet data backend.
4. Add shortability and fee-rate collection where IBKR exposes it.
5. Add live/paper environment separation and operator controls.

## Running the gateway probe

After installing the official TWS API Python client and starting TWS or IB Gateway paper trading:

```bash
source .venv/bin/activate
PYTHONPATH=src python3 -m ibkr_trader.ibkr.probe
```

This probe attempts to connect through the official IBKR Python API and returns:

- connection target
- the broker-reported current time
- the next valid order ID

Those are enough to prove the basic API path is healthy before we add live order workflows.

## Running the local API

After installing the server dependencies in your environment:

```bash
source .venv/bin/activate
python3 -m ibkr_trader.api.server
```

Expected local defaults:

- `API_HOST=127.0.0.1`
- `API_PORT=8000`
- `API_REQUIRE_LOOPBACK_ONLY=true`

Even if the server is started incorrectly, the app refuses non-loopback bind targets when loopback-only mode is enabled.
