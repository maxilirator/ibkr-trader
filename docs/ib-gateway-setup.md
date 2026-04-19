# IB Gateway Setup

This document keeps the repo aligned with the current official IBKR setup path for the TWS API.

## Official direction

IBKR's current documentation says:

- TWS API requires either Trader Workstation or IB Gateway to be installed.
- IB Gateway is effectively synonymous with TWS from the API perspective, but is smaller and more resource efficient because it is API-focused.
- The TWS API is a TCP socket API.
- For connections on the same machine, use `localhost` or `127.0.0.1`.
- The documented default ports are:
  - TWS live: `7496`
  - TWS paper: `7497`
  - IB Gateway live: `4001`
  - IB Gateway paper: `4002`
- IBKR currently recommends `client_id=0` for optimal order-management functionality.

Official sources:

- https://ibkrcampus.com/campus/ibkr-api-page/twsapi-doc/
- https://ibkrcampus.com/campus/trading-lessons/configuring-ibs-trader-workstation/
- https://ibkrcampus.com/campus/trading-lessons/accessing-the-tws-python-api-source-code/

## Installation notes

For the Python API, IBKR's lesson directs users to:

1. Download the current Stable or Latest TWS API release.
2. Enter the extracted `source/pythonclient/` directory.
3. Install the Python client from that official package into your chosen environment.

The lesson explicitly notes that the Python client source lives under `source/pythonclient/` and includes `setup.py`.

## TWS / Gateway settings

IBKR's setup lessons call out these API settings:

1. Open TWS or IB Gateway configuration.
2. Go to `API-Settings`.
3. Enable socket/API clients.
4. Enable API message logging.
5. Optionally include market data in the API log file for troubleshooting.
6. Confirm the socket port matches the port used by this repo.

## Repo defaults

This repository's current config template targets the dedicated live **IB Gateway** host:

```dotenv
IBKR_HOST=127.0.0.1
IBKR_PORT=4002
IBKR_CLIENT_ID=0
IBKR_DIAGNOSTIC_CLIENT_ID=7
IBKR_STREAMING_CLIENT_ID=9
IBKR_ACCOUNT_IDS=U25245595,U25245596
BROKER_MONITOR_ENABLED=true
BROKER_HEARTBEAT_INTERVAL_SECONDS=30
BROKER_SNAPSHOT_REFRESH_INTERVAL_SECONDS=60
```

Recommended repo usage:

- reserve `IBKR_CLIENT_ID=0` for the main long-lived trading runtime
- reserve `IBKR_DIAGNOSTIC_CLIENT_ID=7` for probe and contract-resolution calls
- reserve `IBKR_STREAMING_CLIENT_ID=9` for streaming and market-data sampling
- set `IBKR_ACCOUNT_IDS` when the colocated runtime should refresh balances and portfolio data for multiple visible accounts without using the more fragile account-summary subscription path
- do not work around ownership problems by generating fresh client IDs during normal operation

See [docs/client-id-policy.md](/home/mattias/dev/ibkr-trader/docs/client-id-policy.md) for the canonical policy.

## First live validation

Once the remote IB Gateway is running and the official Python API client is installed:

```bash
PYTHONPATH=src python3 -m ibkr_trader.ibkr.probe
```

The probe validates the official connection path by requesting:

- current broker time
- next valid order ID

If those requests succeed, we know the basic socket session is working and can safely move on to contract lookup and order placement.
