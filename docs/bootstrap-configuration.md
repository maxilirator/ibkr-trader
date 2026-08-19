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

```bash
sudo install -d -m 0750 /etc/ibkr-trader
sudo install -m 0640 /dev/null /etc/ibkr-trader/bootstrap.env
sudo "$EDITOR" /etc/ibkr-trader/bootstrap.env
```

Contents:

```sh
DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/ibkr_trader
IBKR_HOST=127.0.0.1
IBKR_PORT=4001
IBKR_ACCOUNT_IDS=U1234567
```

Permissions: the file must not be world-readable, writable, or executable.
`0640` owned by `root:<service group>` is the intended shape. Verify with:

```bash
stat -c '%A %U:%G %n' /etc/ibkr-trader/bootstrap.env
```

Set `IBKR_TRADER_BOOTSTRAP_ENV` to use a different location (tests, staging).

## Before cutover

`APP_ENV` is not set by anything in this repository, so the value the live host
uses must be confirmed directly on that host. Fail-closed startup engages **only**
for `production`/`prod`. If the live unit sets something else, the protections
above are not active, and the value must be changed deliberately as part of
cutover — not assumed.

Checklist:

1. Confirm the live `APP_ENV` value.
2. Create `/etc/ibkr-trader/bootstrap.env` with correct permissions and every
   required key.
3. Confirm `IBKR_PORT` is the live Gateway port, not `7497`.
4. Confirm `DATABASE_URL` points at the production database.
5. Only then set `APP_ENV=production`.

Steps 1-5 change application startup behaviour and require explicit operator
approval before being applied to the live host.
