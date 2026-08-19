# Bootstrap configuration

How the trader resolves its environment, and why production refuses to start
without it.

## Why this exists

The service used to read `<checkout>/.env`. That has two problems in production:

1. **It is source-dependent.** Configuration travels with whichever source tree
   the process started from. A second checkout or a stale working copy can
   silently supply production values.
2. **It fails open.** When a value is absent, the defaults in `config.py` decide
   what happens, and two of those defaults are dangerous live:
   - `DATABASE_URL` defaults to `postgresql://postgres:postgres@localhost:5432/ibkr_trader`,
     a local throwaway database. Trading against it would write the ledger
     somewhere nobody is reading.
   - `IBKR_PORT` defaults to `7497`, which is the **paper-trading** port.

Production now resolves configuration from one protected absolute path and
refuses to start if it cannot be trusted.

## Layout

| Concern | Location | Notes |
| --- | --- | --- |
| Secrets and bootstrap values | `/etc/ibkr-trader/bootstrap.env` | Outside the source tree. Not in git. |
| Non-secret, audited settings | PostgreSQL settings registry | Read-only in the dashboard at `/settings`. |

Secrets never go in PostgreSQL. Non-secret operational settings never go in
`bootstrap.env`, so that changing one does not require touching a secrets file.

## Where `APP_ENV` must be set

**In the systemd unit, never in `.env`.**

```ini
[Service]
Environment=APP_ENV=production
```

A checkout-local `.env` that sets `APP_ENV=production` is **refused** at startup.
This is not a style preference. Production is decided before `.env` is read, so
a `.env` that declared production would escalate *after* the gate had already
declined to engage: the protected file would never be opened, and every value —
including the throwaway `DATABASE_URL` and the paper `IBKR_PORT` — would come
from the source tree while the process reported `environment=production`. That is
exactly the failure this mechanism exists to prevent, so declaring production is
a decision a checkout is not allowed to make.

No unit in `ops/systemd/` sets `APP_ENV` today. Adding it is a deliberate step of
the approved cutover, not a default.

## Behaviour

Selected by `APP_ENV`.

### Production (`APP_ENV=production` or `prod`)

- `/etc/ibkr-trader/bootstrap.env` is **required**.
- The checkout-local `.env` is **never read**.
- The file is **authoritative**: it overrides ambient environment variables. A
  protected file that any inherited variable could silently replace would not be
  protected. Overridden key *names* are recorded in the load result for audit.
- The file cannot set `APP_ENV` to a non-production value; it must not be able
  to disable the enforcement that just validated it.

Startup fails with `BootstrapConfigurationError` when the file is missing, is
not a regular file, is unreadable, is world-accessible, or is missing a required
key. Every missing key is reported at once, so a production outage is not fixed
one restart-and-discover-the-next-missing-key at a time.

Required keys:

- `DATABASE_URL`
- `IBKR_HOST`
- `IBKR_PORT`
- at least one of `IBKR_ACCOUNT_ID` or `IBKR_ACCOUNT_IDS`

### Development and test (any other `APP_ENV`)

- `bootstrap.env` is optional; if present it is applied with `setdefault`.
- The checkout-local `.env` is then applied, also with `setdefault`.
- An explicitly exported variable still wins.
- A world-accessible bootstrap file is reported as a warning, not an error.

`APP_ENV=live` is **not** production. `live` already names an RL *deployment
mode* (`virtual`/`paper`/`live`) in this codebase, and the trigger for refusing
to start must be unambiguous.

## Creating the file

The service runs as an unprivileged user (`systemctl --user`), so it must be
able to **traverse the directory** as well as read the file. Set the group on
both; a `root:root 0750` directory is not traversable by the service user and
produces a permission error at startup.

```bash
SERVICE_GROUP=<the group the trader service runs as>
sudo install -d -m 0750 -o root -g "$SERVICE_GROUP" /etc/ibkr-trader
sudo install -m 0640 -o root -g "$SERVICE_GROUP" /dev/null /etc/ibkr-trader/bootstrap.env
sudo "$EDITOR" /etc/ibkr-trader/bootstrap.env
```

Contents:

```sh
DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/ibkr_trader
IBKR_HOST=127.0.0.1
IBKR_PORT=4001
IBKR_ACCOUNT_IDS=U1234567
```

Permissions: the file must not be world-accessible, and must not be
group-writable — anyone with group write could redirect the ledger database or
the Gateway port. `0640 root:<service group>` is the intended shape. Verify both
the directory and the file, and confirm the service user can actually read it:

```bash
stat -c '%A %U:%G %n' /etc/ibkr-trader /etc/ibkr-trader/bootstrap.env
sudo -u <service user> head -c1 /etc/ibkr-trader/bootstrap.env >/dev/null && echo readable
```

Set `IBKR_TRADER_BOOTSTRAP_ENV` to use a different location (tests, staging).

## Before cutover

`APP_ENV` is not set by anything in this repository, so the value the live host
uses must be confirmed directly on that host. Fail-closed startup engages **only**
for `production`/`prod`. If the live unit sets something else, the protections
above are not active, and the value must be changed deliberately as part of
cutover — not assumed.

Checklist:

1. Confirm the live `APP_ENV` value and where it is currently set.
2. Create `/etc/ibkr-trader/bootstrap.env` with correct ownership, mode, and
   every required key; verify the service user can read it.
3. Confirm `IBKR_PORT` is the live Gateway port. `7497`/`7496` are refused
   unless `IBKR_ALLOW_PAPER_PORT_IN_PRODUCTION=1` records the decision.
4. Confirm `DATABASE_URL` points at the production database.
5. Remove any `APP_ENV` from the checkout `.env` — it would now be refused.
6. Only then add `Environment=APP_ENV=production` to the systemd unit and
   restart the application services.

Steps 1-6 change application startup behaviour and require explicit operator
approval before being applied to the live host. Restart only the application
services; do not restart IB Gateway.
