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
BROKER_CONNECT_BACKOFF_INITIAL_SECONDS=5
BROKER_CONNECT_BACKOFF_MAX_SECONDS=300
BROKER_HEARTBEAT_INTERVAL_SECONDS=30
BROKER_SNAPSHOT_REFRESH_INTERVAL_SECONDS=60
BROKER_STATUS_REFRESH_MIN_INTERVAL_SECONDS=30
MARKET_STREAM_AUTO_RECONNECT_ENABLED=true
MARKET_STREAM_RECONNECT_INTERVAL_SECONDS=15
MARKET_STREAM_MAX_SUBSCRIPTIONS=120
EXECUTION_RUNTIME_ENABLED=true
EXECUTION_RUNTIME_INTERVAL_SECONDS=5
EXECUTION_RUNTIME_SUBMISSION_LEAD_SECONDS=60
EXECUTION_RUNTIME_RESTART_BACKOFF_INITIAL_SECONDS=30
EXECUTION_RUNTIME_RESTART_BACKOFF_MAX_SECONDS=300
```

Recommended repo usage:

- reserve `IBKR_CLIENT_ID=0` for the main long-lived trading runtime
- reserve `IBKR_DIAGNOSTIC_CLIENT_ID=7` for probe and contract-resolution calls
- reserve `IBKR_STREAMING_CLIENT_ID=9` for streaming and market-data sampling
- set `IBKR_ACCOUNT_IDS` when the colocated runtime should refresh balances and portfolio data for multiple visible accounts without using the more fragile account-summary subscription path
- do not work around ownership problems by generating fresh client IDs during normal operation
- keep broker backoff enabled so failed Gateway connects cool down instead of looping
- keep market-stream auto reconnect enabled so existing subscribed symbols are restored after a Gateway recovery
- keep the market-stream subscription cap explicit; the current default is 120
  symbols, with the runner reporting overflow instead of silently dropping names
- keep dashboard-triggered broker status refresh throttled so operator pages can notice stale state without creating extra Gateway pressure

When the API host is running with `EXECUTION_RUNTIME_ENABLED=true`, that same colocated process now hosts the long-lived execution loop. The runtime takes a durable Postgres lease before it starts cycling, and the dashboard can show whether the execution loop is running, degraded, stale, blocked on startup reconciliation, or stopped.

For Stockholm session-bound orders, the execution runtime now pre-submits exact open/close instructions ahead of the boundary. With the default `EXECUTION_RUNTIME_SUBMISSION_LEAD_SECONDS=60`, next-session-open forced exits and exact open/close scheduled orders are sent one minute early so the broker already has them before the auction starts.

If IB Gateway stops answering, the API should stay up. Managed broker sessions
enter exponential cooldown after connection failures, the monitor skips snapshot
refreshes when the heartbeat is already down, and the background execution
runtime retries after a broker exception instead of silently dying.

If the Gateway JVM stays alive but the API socket stops completing the
`nextValidId` handshake, systemd will not see a process failure. Use the
watchdog below to restart Gateway automatically after repeated failed broker
probes.

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

### Automatic API-deadlock recovery

The failure mode to recover is:

- IB Gateway is visibly open
- port `4002` accepts TCP connections
- the trader API is alive
- `POST /v1/ibkr/probe` fails repeatedly because Gateway does not return
  `currentTime` or `nextValidId`

That means the Gateway API server is wedged even though systemd still sees the
Java process as running. Install the root-owned watchdog timer:

```bash
install -m 0755 /home/mattias/ibkr-trader/ops/scripts/ibgateway_api_watchdog.sh \
  /usr/local/sbin/ibgateway_api_watchdog.sh
install -m 0755 /home/mattias/ibkr-trader/ops/scripts/ibgateway_relogin_restart.sh \
  /usr/local/sbin/ibgateway_relogin_restart.sh
install -m 0644 /home/mattias/ibkr-trader/ops/needrestart/99-ibgateway.conf \
  /etc/needrestart/conf.d/99-ibgateway.conf
cp /home/mattias/ibkr-trader/ops/systemd/ibgateway-api-watchdog.service \
  /etc/systemd/system/
cp /home/mattias/ibkr-trader/ops/systemd/ibgateway-api-watchdog.timer \
  /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now ibgateway-api-watchdog.timer
```

The watchdog is intentionally conservative:

- it first verifies the trader API health endpoint is reachable
- it uses the same official probe endpoint the dashboard trusts
- it restarts `ibgateway-ibc.service` only after twelve consecutive probe
  failures
- it allows a startup grace period after a Gateway restart before counting
  more failures
- it uses `/usr/local/sbin/ibgateway_relogin_restart.sh`, which first tries
  the IBC CommandServer `RESTART` command. IBC documents this as the auto
  restart path that reuses the current session credentials, so it normally
  avoids fresh two-factor authentication during the trading week.
- it does not fall back to `systemctl restart` by default. A hard service
  restart starts a fresh login and may require operator 2FA; if the command
  server path is unavailable, the watchdog alerts instead of cycling Gateway
  repeatedly.
- it only performs automatic restarts Monday through Friday by default. Sunday
  restarts usually require a fresh weekly authentication and should be handled
  while the operator is available.
- it will not restart again immediately if the probe is still failing after a
  recent restart; that condition usually means Gateway is waiting for manual
  login or second-factor confirmation
- unattended package upgrades must not restart `ibgateway-ibc.service`
  directly; the `needrestart` override keeps those hard restarts out of the
  automatic maintenance path

IBC's own user guide documents the CommandServer `RESTART` behavior: for
Gateway, IBC sets the next auto-restart time and reuses the current session
credentials. That command cannot bypass IBKR's full Sunday authentication
requirement, so the weekly restart should be planned for a time when the
operator can approve it.

- it writes recent Gateway log context to
  `/run/ibgateway-api-watchdog.last-journal`
- it resets its failure counter after a successful probe or restart

Tune the timer or threshold by editing
`/etc/systemd/system/ibgateway-api-watchdog.service`:

```ini
Environment=TRADER_API_BASE_URL=http://127.0.0.1:8000
Environment=GATEWAY_SERVICE=ibgateway-ibc.service
Environment=GATEWAY_RESTART_COMMAND=/usr/local/sbin/ibgateway_relogin_restart.sh
Environment=FAILURE_THRESHOLD=12
Environment=CURL_TIMEOUT_SECONDS=20
Environment=RESTART_ALLOWED_WINDOWS=06:00-23:00
Environment=RESTART_WINDOW_TZ=Europe/Stockholm
Environment=RESTART_ALLOWED_DAYS=Mon,Tue,Wed,Thu,Fri
Environment=STARTUP_GRACE_SECONDS=900
Environment=RESTART_COOLDOWN_SECONDS=3600
# Optional generic JSON webhook: {"text": "..."}
# Environment=OPERATOR_ALERT_WEBHOOK_URL=https://example.invalid/webhook
# Optional mobile push through ntfy.
# Environment=OPERATOR_ALERT_NTFY_TOPIC=your-secret-topic
# Optional mobile push through Pushover.
# Environment=OPERATOR_ALERT_PUSHOVER_APP_TOKEN=...
# Environment=OPERATOR_ALERT_PUSHOVER_USER_KEY=...
# Environment=OPERATOR_ALERT_COOLDOWN_SECONDS=1800
```

Install the restart wrapper alongside the watchdog:

```bash
install -m 0755 /home/mattias/ibkr-trader/ops/scripts/ibgateway_relogin_restart.sh \
  /usr/local/sbin/ibgateway_relogin_restart.sh
```

Useful checks:

```bash
systemctl list-timers ibgateway-api-watchdog.timer
systemctl status ibgateway-api-watchdog.service --no-pager
journalctl -u ibgateway-api-watchdog.service -n 100 --no-pager
cat /run/ibgateway-api-watchdog.last-journal
```

The restart window is intentional. A Gateway restart can still require manual
second-factor confirmation, so the watchdog should restart Gateway only during
hours when an operator can answer the IBKR prompt. Outside the configured window
it records the failure and leaves the trader API degraded instead of creating a
hidden 2FA problem.

If one of the operator alert settings is set, the watchdog sends one
cooldown-protected human-action alert when a restart is outside the allowed
window, when the probe is still failing shortly after a restart, or when the
restart command fails. For mobile push, the simplest path is `ntfy`: install the
mobile app, choose a private topic, then set `OPERATOR_ALERT_NTFY_TOPIC`.

### Operational notes

- If the XRDP display number changes later, rerun
  `write_ibgateway_session_env.sh` and restart the service.
- This service will fail closed if `DISPLAY`, `XAUTHORITY`, IBC, or the Gateway
  config path are missing.
- IBC 3.14+ uses `ReloginAfterSecondFactorAuthenticationTimeout=yes` for
  modern IBKR Mobile 2FA handling. The repo start script and restart wrapper
  enforce that row, and the launch argument now defaults to
  `--on2fatimeout=restart`.
- This service cannot bypass IBKR's mandatory weekly or risk-triggered
  authentication. It can keep unattended restarts on the relogin path; if IBKR
  requires a fresh approval, the watchdog should notify the operator.
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
