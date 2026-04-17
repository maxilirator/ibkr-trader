# Client ID Policy

This document is the canonical client-ID policy for this repository.

The goal is simple:

- long-lived services get stable reserved client IDs
- we do not rotate or generate fresh client IDs as the normal operating model
- if a fixed client ID is already in use, we treat that as a real ownership / process problem to resolve

## Reserved client IDs

- `0`: main long-lived trading runtime
- `7`: diagnostic and read-only broker access
- `9`: streaming and market-data sampling

These values are the canonical defaults used by the repo config layer.

Code source of truth:

- [`src/ibkr_trader/ibkr/client_ids.py`](/home/mattias/dev/ibkr-trader/src/ibkr_trader/ibkr/client_ids.py)

## Operational rules

- The trading runtime should keep `client_id=0`.
- Read-only diagnostics should default to `client_id=7`.
- Streaming jobs should default to `client_id=9`.
- The FastAPI server now keeps long-lived primary and diagnostic broker sessions open and reuses them across requests.
- The runtime worker keeps a long-lived primary broker session open across cycles instead of reconnecting per broker action.
- Operator checks should normally go through the local FastAPI service instead of opening side broker sessions directly.
- If Gateway reports `client id is already in use`, identify the owning process or session and recover that path instead of sidestepping it with a new ID.
- We should keep moving toward long-lived API-owned broker sessions rather than per-request reconnect churn.

## Current environment variables

- `IBKR_CLIENT_ID`
- `IBKR_DIAGNOSTIC_CLIENT_ID`
- `IBKR_STREAMING_CLIENT_ID`

The repo template in [`.env.example`](/home/mattias/dev/ibkr-trader/.env.example) uses the canonical reserved values.
