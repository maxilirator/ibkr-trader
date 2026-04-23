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
EXECUTION_RUNTIME_ENABLED=true
EXECUTION_RUNTIME_INTERVAL_SECONDS=5
EXECUTION_RUNTIME_SUBMISSION_LEAD_SECONDS=60
```

Recommended repo usage:

- reserve `IBKR_CLIENT_ID=0` for the main long-lived trading runtime
- reserve `IBKR_DIAGNOSTIC_CLIENT_ID=7` for probe and contract-resolution calls
- reserve `IBKR_STREAMING_CLIENT_ID=9` for streaming and market-data sampling
- set `IBKR_ACCOUNT_IDS` when the colocated runtime should refresh balances and portfolio data for multiple visible accounts without using the more fragile account-summary subscription path
- do not work around ownership problems by generating fresh client IDs during normal operation

When the API host is running with `EXECUTION_RUNTIME_ENABLED=true`, that same colocated process now hosts the long-lived execution loop. The runtime takes a durable Postgres lease before it starts cycling, and the dashboard can show whether the execution loop is running, degraded, blocked on startup reconciliation, or stopped.

For Stockholm session-bound orders, the execution runtime now pre-submits exact open/close instructions ahead of the boundary. With the default `EXECUTION_RUNTIME_SUBMISSION_LEAD_SECONDS=60`, next-session-open forced exits and exact open/close scheduled orders are sent one minute early so the broker already has them before the auction starts.

See [docs/client-id-policy.md](/home/mattias/dev/ibkr-trader/docs/client-id-policy.md) for the canonical policy.

## Session-bound Gateway service

For the current `quant.geisler.se` operating model, the safest first service
shape is a **session-bound** IB Gateway service:

- `ibgateway` logs in once via RDP
- XRDP keeps that desktop session alive across disconnects
- a `systemd --user` service starts IBC and IB Gateway on that same display
- reconnecting via RDP returns to the same visible Gateway screen

This is intentionally different from a headless `Xvfb` service. A headless
service is useful later, but it does not satisfy the operator requirement of
"disconnect and reconnect to the same Gateway screen."

The repo now includes:

- [ibgateway-ibc.service](/home/mattias/dev/ibkr-trader/ops/systemd/ibgateway-ibc.service)
- [ibgateway-ibc-system.service](/home/mattias/dev/ibkr-trader/ops/systemd/ibgateway-ibc-system.service)
- [run_ibgateway_ibc.sh](/home/mattias/dev/ibkr-trader/ops/scripts/run_ibgateway_ibc.sh)
- [write_ibgateway_session_env.sh](/home/mattias/dev/ibkr-trader/ops/scripts/write_ibgateway_session_env.sh)
- [ibgateway-ibc.env.example](/home/mattias/dev/ibkr-trader/ops/examples/ibgateway-ibc.env.example)

### Install flow for `ibgateway`

Run these steps as the `ibgateway` user on the server after the XRDP desktop is
stable and reconnecting properly.

1. Create the persistent Gateway config directory:

```bash
mkdir -p ~/.config/ibgateway ~/.config/systemd/user
```

2. Copy the repo templates into the user config location:

```bash
cp ~/ibkr-trader/ops/systemd/ibgateway-ibc.service ~/.config/systemd/user/
cp ~/ibkr-trader/ops/examples/ibgateway-ibc.env.example ~/.config/ibgateway/ibgateway.env
```

3. Edit `~/.config/ibgateway/ibgateway.env` so the paths match the actual IBC
   and Gateway installation for `ibgateway`.

4. Capture the active desktop session. If you are doing this from an XRDP
   terminal inside the live desktop, the current `DISPLAY` is normally enough:

```bash
~/ibkr-trader/ops/scripts/write_ibgateway_session_env.sh
```

If you are doing it from SSH, specify the live display explicitly:

```bash
~/ibkr-trader/ops/scripts/write_ibgateway_session_env.sh \
  --display :11 \
  --xauthority /home/ibgateway/.Xauthority
```

5. Reload and start the user service:

```bash
systemctl --user daemon-reload
systemctl --user enable --now ibgateway-ibc.service
```

6. Verify status and logs:

```bash
systemctl --user status ibgateway-ibc.service
journalctl --user -u ibgateway-ibc.service -n 200 --no-pager
```

### If `systemctl --user` has no session bus

Some XRDP and SSH combinations do not provide a usable `systemd --user` bus for
the `ibgateway` account. If you see:

```text
Failed to connect to bus: No medium found
```

use the root-managed system service instead. It still runs the Gateway as the
`ibgateway` user and still consumes the same `session.env` file, but it is
managed by the host's system `systemd`.

Install it as `root`:

```bash
cp /home/ibgateway/ibkr-trader/ops/systemd/ibgateway-ibc-system.service \
  /etc/systemd/system/ibgateway-ibc.service
systemctl daemon-reload
systemctl enable --now ibgateway-ibc.service
systemctl status ibgateway-ibc.service --no-pager
journalctl -u ibgateway-ibc.service -n 200 --no-pager
```

This is the recommended path on `quant.geisler.se` if `systemctl --user` is not
reliably available.

### Operational notes

- If the XRDP display number changes later, rerun
  `write_ibgateway_session_env.sh` and restart the service.
- This service will fail closed if `DISPLAY`, `XAUTHORITY`, IBC, or the Gateway
  config path are missing.
- This service does not solve second-factor authentication by itself. It only
  makes the launch path durable and explicit.
- For XRDP reconnect behavior, the server should use `KillDisconnected=false`
  and a reconnect policy such as `Policy=UBI` in `/etc/xrdp/sesman.ini`.

## First live validation

Once the remote IB Gateway is running and the official Python API client is installed:

```bash
PYTHONPATH=src python3 -m ibkr_trader.ibkr.probe
```

The probe validates the official connection path by requesting:

- current broker time
- next valid order ID

If those requests succeed, we know the basic socket session is working and can safely move on to contract lookup and order placement.
