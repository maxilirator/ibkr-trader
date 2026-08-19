# Runtime settings registry

Typed, non-secret operational settings, resolved from PostgreSQL and shown
read-only in the existing operator dashboard at `/settings`.

## Why

Operational knobs previously existed only as environment variables read through
`config.py`. Nothing showed what the running process had actually resolved, so
answering "what is the stale-data threshold right now?" meant reading the systemd
unit, the protected bootstrap file and the checkout `.env`, and reconciling three
sources by hand.

The registry declares those knobs, resolves them, and reports the effective value
together with **where it came from**.

## The secret / non-secret split

| Kind | Home | Visible in dashboard |
| --- | --- | --- |
| Secrets, connection strings, credentials | `/etc/ibkr-trader/bootstrap.env` | No |
| Non-secret operational settings | `runtime_setting` table | Yes, read-only |

This split is **enforced in code**, not just documented. `assert_not_secret_key`
runs on every definition at import time and rejects any key matching
`PASSWORD`, `SECRET`, `TOKEN`, `CREDENTIAL`, `PRIVATE_KEY`, `API_KEY`,
`DATABASE_URL` or `DSN`. Documenting the split is not enough — without a check it
erodes the first time someone adds a convenient "just one" credential.

## Read-only, deliberately

There is no `POST /v1/settings` and no application code path that writes a
setting. The registry **reports** what the runtime resolved; it must not become a
second way to change trading behaviour alongside the operator controls. A test
asserts that no write method is registered on the route.

Populating or changing a stored value is therefore an administrative action
against the database, recorded in `runtime_setting_event`. Adding a governed
write path is out of scope for this phase and would require explicit approval,
since several of these settings (for example `EXECUTION_RUNTIME_ENABLED`) change
trading behaviour.

## Resolution rules

- A setting with **no stored row** resolves to its declared default and reports
  `source: "default"`. That is a real answer, not a guess: the runtime genuinely
  uses the default.
- A setting **with a row** reports `source: "database"`, plus who changed it and
  when.
- A stored value that **does not parse** as its declared type is reported as an
  error against that setting. The effective value falls back to the default and
  says so. An operator must never be shown a value the runtime is not using.
- A stored row with **no matching definition** is surfaced under
  `undeclared_keys`. Such a row affects nothing — it is either a typo or a
  setting removed from the code — and hiding it would make the page misleading.

## Adding a setting

Add a `_define(...)` entry to `SETTING_DEFINITIONS` in
`src/ibkr_trader/settings_registry.py`. The key should match the environment
variable `config.py` reads, so a dashboard row maps to the variable that sets it.
Tests assert that every default matches its declared type and that every
definition carries a description and category.

## Endpoint

`GET /v1/settings` →

```json
{
  "accepted": true,
  "read_only": true,
  "settings": [
    {
      "key": "MARKET_STREAM_STALE_AFTER_SECONDS",
      "value_type": "float",
      "effective_value": 45.0,
      "default_value": 180.0,
      "source": "database",
      "updated_by": "mattias",
      "error": null
    }
  ],
  "categories": ["broker", "execution", "market-data", "market-stream", "rl"],
  "error_count": 0,
  "undeclared_keys": []
}
```
