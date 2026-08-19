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

## Runtime value vs stored value

**Nothing in the runtime reads this table.** `config.py` resolves configuration
from the process environment. A row here is *recorded intent*, not an applied
setting.

This distinction is the whole design. Reporting a stored row as "the effective
value" would assert that the runtime is using a value it never reads — a
fabricated success state of exactly the kind the repository rules prohibit. So
the registry reports both, separately:

| Field | Meaning |
| --- | --- |
| `runtime_value` | What the running process actually resolved. **The operative value.** |
| `runtime_source` | `environment` or `default` — where `runtime_value` came from |
| `stored_value` | What the database records. Not consumed by the runtime. |
| `drifted` | True when a stored value exists and disagrees with `runtime_value` |

`drifted` is the actionable signal: it means someone recorded an intent that the
running process is not honouring.

The payload also carries `stored_values_are_applied: false`, so no consumer can
mistake one for the other.

## Resolution rules

- A setting with **no stored row** reports `stored_value: null` and
  `has_stored_value: false`. `runtime_value` still reports what the process uses.
- A value that **does not parse** as its declared type is reported as an error
  against that setting. An invalid *environment* value means the runtime fell
  back to the default, and that is stated. An invalid *stored* value is shown as
  unset rather than as a number.
- A stored row with **no matching definition** is surfaced under
  `undeclared_keys`. Such a row affects nothing — it is either a typo or a
  setting removed from the code — and hiding it would make the page misleading.

If a future change wires this table into `config.py`, the wording above stops
being true. `test_no_declared_setting_is_read_from_the_database_by_config` fails
in that case, deliberately, so the claim gets revisited rather than silently
becoming false.

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
      "runtime_value": 180.0,
      "runtime_source": "default",
      "stored_value": 45.0,
      "has_stored_value": true,
      "drifted": true,
      "default_value": 180.0,
      "updated_by": "mattias",
      "error": null
    }
  ],
  "categories": ["broker", "execution", "market-data", "market-stream", "rl"],
  "error_count": 0,
  "drift_count": 1,
  "undeclared_keys": [],
  "stored_values_are_applied": false
}
```
