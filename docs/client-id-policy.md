# Client ID Policy

This document is the canonical client-ID policy for this repository.

The goal is simple:

- long-lived services get stable reserved client IDs
- we do not rotate or generate fresh client IDs as the normal operating model
- if a fixed client ID is already in use, we treat that as a real ownership / process problem to resolve

## Reserved client IDs

- `0`: main long-lived trading runtime
- `7`: diagnostics, probes, and controlled read-only lookup
- `8`: historical bars and controlled backfill
- `9`: streaming and market-data sampling

The broker usage rewrite keeps those IDs stable but narrows their roles over
time. Client `7` must not become the live loop's catch-all for probes,
historical data, contract lookup, and RL backfills. Historical/backfill now uses
client `8`. See
[IBKR Broker Usage Rewrite](ibkr-broker-usage-rewrite.md).

These values are the canonical defaults used by the repo config layer.

Code source of truth:

- [`src/ibkr_trader/ibkr/client_ids.py`](/home/mattias/dev/ibkr-trader/src/ibkr_trader/ibkr/client_ids.py)

## Operational rules

- The trading runtime should keep `client_id=0`.
- Read-only diagnostics should default to `client_id=7`.
- Historical bars and controlled backfills should default to `client_id=8`.
- Streaming jobs should default to `client_id=9`.
- RL runners should publish desired stream state through the API and should not
  open their own IBKR market-data client.
- The FastAPI server now keeps long-lived primary and diagnostic broker sessions open and reuses them across requests.
- The runtime worker keeps a long-lived primary broker session open across cycles instead of reconnecting per broker action.
- Operator checks should normally go through the local FastAPI service instead of opening side broker sessions directly.
- If Gateway reports `client id is already in use`, identify the owning process or session and recover that path instead of sidestepping it with a new ID.
- We should keep moving toward long-lived API-owned broker sessions rather than per-request reconnect churn.

## Current environment variables

- `IBKR_CLIENT_ID`
- `IBKR_DIAGNOSTIC_CLIENT_ID`
- `IBKR_HISTORICAL_CLIENT_ID`
- `IBKR_STREAMING_CLIENT_ID`
- `IBKR_API_STARTUP_FAILURE_SLOW_PROBE_SECONDS`
- `IBKR_API_MAX_REQUESTS_PER_SECOND`
- `IBKR_MARKET_DATA_LINE_LIMIT`
- `IBKR_HISTORICAL_REQUESTS_PER_10_MINUTES`

The repo template in [`.env.example`](/home/mattias/dev/ibkr-trader/.env.example) uses the canonical reserved values.
