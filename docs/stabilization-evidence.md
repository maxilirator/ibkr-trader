# Stabilization program evidence

Acceptance evidence for the phases in `docs/approved-stabilization-goal.md`.
Append-only: each phase records what was built, how it was verified, and what
review found. Claims here must be reproducible from the recorded commits.

Branch: `stabilization/phases-1-4`
Baseline commit: `b7a2973` ("Resolve shared q-data through the catalog only")

## Standing constraints (unchanged by this work)

| Constraint | State | Evidence |
| --- | --- | --- |
| Global kill switch enabled | **Now enforced** (`59dfdd9`, approved by Mattias): an absent record reads as *enabled*. Live row verified `enabled=True` before the change, so live impact is nil. | `tests/test_operator_controls.py::KillSwitchFailClosedTests` |
| One dedicated Gateway; legacy `ibgateway.service` disabled | Untouched | No change to `ops/systemd/*`; no Gateway restart or config change performed |
| Watchdog restart authority disabled | Untouched | `ops/systemd/ibgateway-api-watchdog.service` still sets `WATCHDOG_RESTART_ENABLED=no` |
| RL runner virtual | Untouched | `ops/systemd/ibkr-trader-rl-runner.service` still passes `--execute-virtual` only |
| Canonical dashboard extended, not replaced | No parallel dashboard created | No new dashboard app; Phase 3 will extend `dashboard/src/routes/` |
| No live RL trading enabled | Untouched | No change to `rl/runner_loop.py` mode gating |

Nothing in Phases 1-2 was deployed. No live host was contacted. No Gateway,
service restart, or configuration change was performed.

## Test baseline

The suite is **not** green on the baseline commit. Recorded here because Phase 4
gates a release on matched tests and cannot honestly claim "all tests pass".

Baseline `b7a2973`: **3 failed, 538 passed, 1 skipped**.

| Failing test | Cause | Product defect? |
| --- | --- | --- |
| `test_sync_wrapper.py::test_send_msg_records_raw_order_protocol_payloads` | Locally installed `ibapi`'s `sendMsg` never reaches the stubbed `conn`; version skew in `.venv` | No — environment |
| `test_order_preview.py::test_preview_flags_invalid_stockholm_short_before_submit` | `tests/conftest.py::write_catalog` never creates `shortability/shortability_latest.json`; `FileNotFoundError` | No — test fixture gap |
| `test_order_execution_01.py::test_submit_order_from_batch_rejects_explicit_short_on_non_shortable_stockholm_account` | Same fixture gap | No — test fixture gap |

> **Superseded.** All three were fixed in `e7a8ca5`; none was a product defect.
> See "Test baseline — now green" below. The suite is green, which is what
> Phase 4's matched-tests gate requires.

## Phase 1 — Recovery and stream policy

Commit `ddb394f`. Status: **implemented, reviewed, changes applied**.

Added `src/ibkr_trader/ibkr/recovery_policy.py`: named `BrokerRecoveryState`
(`healthy`/`degraded`/`recovering`/`blocked`/`down`/`maintenance`/`unknown`) and
`StreamState` (`streaming`/`idle`/`stale`/`reconnecting`/`blocked`/`stopped`/
`unknown`). Each broker state carries a `RecoveryAuthority`. Classifiers are
pure functions of a snapshot, so transitions are directly testable.

Surfaced read-only at `/healthz` under `recovery_policy`. No trading, broker or
reconnect behaviour changed.

### Independent review

Verdict: **APPROVE-WITH-CHANGES**, 3 blocking findings. All 3 were independently
re-verified against the source before fixing, and all 3 were confirmed real.

| # | Finding | Verified | Resolution |
| --- | --- | --- | --- |
| 1 | Stream state read `stale_reconnect_enabled`, a different setting from the one gating the reconnect supervisor (`market_stream_auto_reconnect_enabled`, `server.py:763`). A stream that would never recover reported `reconnecting`. | Confirmed: the auto-reconnect flag was not in the snapshot at all | Snapshot now exposes `auto_reconnect_active` (supervisor thread liveness — the honest observable). Absent ⇒ `unknown`, never optimistic. |
| 2 | `status(blocking=False)` returns a synthetic `connected=False` when the session lock is busy (`session_manager.py:503-531`). A connected, actively-trading session classified as `recovering`/`down`. | Confirmed | `ManagedSessionStatus.status_available` added; classifies `unknown`. |
| 3 | `_is_stream_stale_locked` returns `False` both for "fresh" and for "staleness detection disabled" (`market_stream.py:752`). A six-hour-stale feed could classify `streaming`, `is_usable: true`. | Confirmed | Service verdict is now corroborating, not authoritative; undetectable staleness ⇒ `unknown`. |

Non-blocking findings also applied: per-session circuit breaker now blocks even
when the shared circuit is closed (a successful market-stream connect clears the
shared circuit out from under a circuit-broken primary session); negative failure
counts classify `unknown` instead of clamping to healthy; `maintenance_mode`
reports `null` rather than `false` when unreadable; stream errors surface in the
payload rather than only in a log line; `health_snapshot()` acquires the stream
lock non-blockingly so `/healthz` cannot stall the operator watchdog during
reconnect churn.

Review **refuted** one suspected defect: the `snapshot()` extraction refactor was
verified key-for-key equivalent at AST level (45 keys, no drift). An exact-key-set
test (`test_snapshot_returns_exactly_the_contracted_keys`) now pins the contract,
since a subset check could not have caught key loss.

### Verification

- `tests/test_recovery_policy.py`: 45 passed
- Full suite: **3 failed (the pre-existing three), 609 passed, 1 skipped**
- `ruff check` clean on new modules; `market_stream.py`'s 56 pre-existing findings unchanged

## Phase 2 — Fail-closed source-independent bootstrap

Commits `73bc591` (initial), `a4f9011` (review fixes), `402ad33` (re-verification
fixes). Status: **implemented; reviewed twice; rejected twice; fixed. A third
independent verification is required before this phase may be called done.**

This phase was rejected on first review and found incomplete on re-verification.
Both times the finding was the same class of defect: a production process could
start without the protected file being read and validated. That history is the
reason the phase is not treated as settled after two rounds.

Added `src/ibkr_trader/bootstrap.py`. With `APP_ENV=production`/`prod`:

- `/etc/ibkr-trader/bootstrap.env` is required; the checkout-local `.env` is
  never read (source-independence).
- Missing / non-regular / unreadable / world-accessible / incomplete file raises
  `BootstrapConfigurationError` and the process does not start. All missing keys
  are reported at once.
- Required: `DATABASE_URL`, `IBKR_HOST`, `IBKR_PORT`, and one of
  `IBKR_ACCOUNT_ID` / `IBKR_ACCOUNT_IDS`. These previously fell back to a local
  throwaway database and to port `7497` — the **paper** port.
- The file is authoritative over ambient environment; overridden key names are
  recorded for audit. It cannot downgrade `APP_ENV` and so cannot disable the
  enforcement that validated it.
- Secret values never reach logs, error messages or the audit payload.

Outside production: file optional, `.env` still applies, explicit exports still win.

`APP_ENV=live` is deliberately **not** a production alias — `live` already names
an RL deployment mode, and the trigger for refusing to start must be unambiguous.

### Independent review

Verdict on `73bc591`: **REJECT**, 2 blockers. Both were reproduced before being
fixed, and the reproduction is recorded below because it is the strongest
evidence that the phase's headline guarantee was not initially delivered.

**Blocker 1 (CRITICAL) — a checkout `.env` could declare production.**
`APP_ENV` was read twice: once before the `.env` load, deciding whether to
engage the gate, and again after it, deciding whether values were required.
With `APP_ENV` unset the gate declined to engage, `.env` then set
`APP_ENV=production`, and the app ran as production against source-tree values.
Reproduced with the shipped `.env.example` values:

```
load source      = dotenv | is_production = False   <- gate declined to engage
APP_ENV after    = production                       <- .env raised it afterwards
DATABASE_URL     = postgresql://postgres:postgres@localhost:5432/ibkr_trader
IBKR host/port   = 127.0.0.1 7497                   <- the PAPER port
```

The documented cutover step said "set `APP_ENV=production`" without saying
where, and no systemd unit sets it — so following the checklist literally
produced exactly this. Fixed by removing the second read: `.env` is inspected
before it is applied, a file declaring production is refused, and
`AppConfig.from_env()` takes the environment from the load result. Verified
refused after the fix, with `os.environ` left unpolluted.

**Blocker 2 (HIGH) — raw `PermissionError` escaped both branches.**
`Path.exists()` propagates `EACCES` rather than returning `False`. The
documented install produced `root:root 0750 /etc/ibkr-trader`, which the
unprivileged service user cannot traverse. Confirmed on this host:
`exists() RAISED PermissionError [Errno 13]`. It also broke *non-production*
startup, where the file is documented as optional. Access failure is now a
distinct third answer from "absent": concrete error in production, warning
outside it.

Non-blocking findings also applied: paper ports refused rather than merely
defaulted away from (overridable via `IBKR_ALLOW_PAPER_PORT_IN_PRODUCTION=1`);
`IBKR_PORT` validated as an integer inside the all-problems-at-once report;
group-writable files rejected; the path override must be absolute in
production; the load is logged (key names only), as the audit record was
previously built and discarded.

The review also identified that the test suite would read the real
`/etc/ibkr-trader/bootstrap.env` — on the live host, pulling production secrets
into the test process. A `conftest.py` fixture now redirects it.

### Re-verification of the fix (commit `402ad33`)

The fix to blocker 1 was independently re-verified and found **INCOMPLETE**. The
specific exploit was closed, but three further routes reached production without
the protected file being read and validated. All were reproduced before fixing.

| Route | Effect | Resolution |
| --- | --- | --- |
| Duplicate `APP_ENV` key in `.env` | `_parse_env_text` resolved duplicates last-wins, `load_dotenv_file` first-wins. A file inspected as `dev` was applied as `production` — the full original bug restored. | Parse is first-wins; each file parsed once and that dict applied, so the value inspected is the value used. Also removes a TOCTOU double read. |
| Bootstrap file containing `APP_ENV=production` while the unit had not set it | Non-production branch applied every key including `APP_ENV`, with no validation. Split brain: `AppConfig.environment=dev` (skipping the `DATABASE_URL` requirement, using the throwaway default) while `IbkrConnectionConfig` took the production branch and got port 7497. **This is the documented cutover's own intermediate state.** | Neither file may declare production when the gate has not engaged; both are vetted before anything is applied. |
| `IBKR_TRADER_BOOTSTRAP_ENV=~/x.env` | `expanduser()` ran before the absolute-path check, so the home-relative form that check exists to reject was made absolute first. | Check runs on the raw value. |
| `APP_ENV=production # live` | Matched no alias, silently degraded to dev: operator believes production is on while the gate disengages. | Refused. |

The invariant behind the whole class is now explicit and tested: after
`load_runtime_environment()`, `os.environ["APP_ENV"]` always agrees with the
returned environment.

Re-verification also found the `conftest` isolation fixture ineffective — it used
`os.environ.setdefault`, which `patch.dict(clear=True)` strips, so six tests
still resolved the real `/etc/ibkr-trader/bootstrap.env`. On the live host that
would have read production secrets into the test process and produced spurious
failures in the release-evidence run. It now patches the module constant.

### Verification

- `tests/test_bootstrap.py`: 45 passed, including a regression test per route,
  one test per unsafe default proving it is unreachable in production, one
  proving no secret value appears in the error message, and one pinning the
  `APP_ENV` agreement invariant
- All four exploits re-run against the fixed code: **all refused**, with
  `os.environ` left unpolluted
- Instrumented probe across the full suite: **0 resolutions** of the real
  protected path
- Full suite: **3 failed (pre-existing), 628 passed, 1 skipped**
- `ruff check` clean on changed files
- The pre-existing `test_config.py::test_real_environment_overrides_dotenv`
  passes **unmodified**, evidence that non-production semantics are unchanged

### Blocking pre-cutover actions

1. Nothing in this repository sets `APP_ENV`, and fail-closed startup engages
   **only** for `production`/`prod`. The value the live host uses must be
   confirmed on that host; if it is anything else, these protections are
   inactive.
2. `APP_ENV=production` must be set in the **systemd unit**. Setting it in the
   checkout `.env` is now refused at startup, by design.
3. If the live `.env` currently contains `APP_ENV`, it must be removed, or the
   application will refuse to start after this change is deployed.
4. `/etc/ibkr-trader` must be traversable and the file readable by the
   unprivileged service user. Verify with the commands in
   `docs/bootstrap-configuration.md`, not by assumption.

All four change application startup behaviour and require explicit operator
approval before being applied to the live host.

## Open findings requiring a decision

### 1. The global kill switch is fail-open on a fresh database (HIGH)

`orchestration/operator_controls.py:117-129` — `_build_kill_switch_status`
returns `enabled=False` when no `operator_control` row exists. `read_kill_switch_state`
→ `assert_kill_switch_inactive` therefore **permits new entries** when the
control row is absent.

Verified directly in source. This contradicts the non-negotiable rule "the
global kill switch remains enabled throughout this goal": the rule holds only
because a row happens to exist in the live database. A fresh database, a failed
schema migration, or a deleted row silently authorises trading.

Not changed unilaterally: it alters kill-switch semantics, and while making it
fail *closed* is strictly safer than the present behaviour, it is a material
change to a control the approval gates name explicitly.

Proposed fix, pending approval:
1. Treat an absent control row as **enabled** (blocked) in
   `_build_kill_switch_status`, and
2. seed the row as enabled in `db/init_schema.py`, with an audit event
   recording that it was seeded rather than operator-set.

Impact to confirm before applying: any environment without the row would begin
blocking new entries. The RL runner is virtual and `execution_runtime_enabled`
defaults to `false`, so the expected live impact is nil — but this must be
confirmed against the live database rather than assumed.

## Phases not started

- **Phase 3** — non-secret typed settings registry + existing dashboard `/settings`.
  No `/settings` route exists today; `operator_control` is the closest existing
  pattern to reuse. No migration tool in the repo (schema is `create_all`).
- **Phase 4** — verifiable release pipeline. No CI exists (`.github/` absent) and
  no provenance/version stamping exists. Blocked on the 3 red baseline tests.
- **Phase 5** — cutover. Requires explicit Mattias approval. Not started.
- **Phase 6** — stability observation. Depends on Phase 5.

## Phase 2 — final state

Commits `73bc591`, `a4f9011`, `402ad33`, `1ef68b6`, `aafd1b8`, `0a337e5`.

Rejected on first review, found incomplete on re-verification, and then subjected
to a red-team exploit hunt and a deployment-safety review in parallel. **Seven**
distinct ways to reach production without a validated protected file were found
and closed. Six of the seven were the same defect class: *the value checked
differed from the value used*, or *state already decided was read a second time*.

| # | Route | Found by |
| --- | --- | --- |
| 1 | `.env` declared production after the gate declined to engage | review |
| 2 | Duplicate `APP_ENV` key: inspected last-wins, applied first-wins | re-verification |
| 3 | Bootstrap file self-promoted to production when the unit had not — the documented cutover's own intermediate state | re-verification |
| 4 | `expanduser()` ran before the absolute-path check, so `~/x.env` passed | re-verification |
| 5 | Non-production branch returned an environment re-read from ambient, disagreeing with the branch it took | red team |
| 6 | Unkeyed per-process cache served a stale `dev` result after `APP_ENV` became production | self-found while auditing the cache |
| 7 | `_looks_like_production` substring match refused legitimate names (`nonprod`, `preprod`, `prod-eu`) — the inverse failure: a self-inflicted startup outage | self-found, confirmed by deployment review |

Two further correctness defects were found by the deployment review:

- **`PAPER_TRADING_PORTS` was inverted for this deployment.** It listed
  `{7497, 7496}`; `7496` is TWS **live**, and `4002` is IB Gateway **paper**.
  This host runs IB Gateway (`.env.example` and `docs/ib-gateway-setup.md` both
  use `4002`), so the guard refused a live port while waving the paper port
  through. Now `{7497, 4002}`.
- **`Q_DATA_CATALOG_PATH` was not required.** It has no default and fails closed,
  and production no longer reads `.env`, so the documented cutover produced a
  guaranteed outage with an error that reads like a data problem.

The runtime environment is now loaded once per process. `AppConfig.from_env()` is
reached from the live order-submission path, so every order was re-reading and
re-validating the protected file — a `chmod` would have broken order validation
on a running service.

### Cutover risk that cannot be fixed in code

Production stops reading `.env` entirely, so every key it supplied that the
bootstrap file does not silently reverts to a code default — **over 40 keys** for
this repository's `.env.example`. Combined with the kill switch reading
*disabled* when its row is absent, a mistyped `DATABASE_URL` yields a service
that starts cleanly, creates an empty schema, reports healthy, and has no kill
switch on a live account. That is worse than an outage because nothing looks wrong.

`scripts/preflight_bootstrap_cutover.py` blocks on exactly that, plus paper
ports, missing keys, bad permissions, and `APP_ENV` in `.env`. It is read-only.
Verified against a database with no kill-switch row.

### Verification

- `tests/test_bootstrap.py`: 60 passed, with a regression test per route
- All seven routes re-run against the fixed code: **all refused**, `os.environ` unpolluted
- Instrumented probe: **0** resolutions of the real protected path across the suite
- Full suite: **675 passed, 1 skipped, 0 failed**

## Test baseline — now green

The three pre-existing failures are fixed (commit `e7a8ca5`). None was a product
defect.

- The two Stockholm short-sale tests: the shared q-data fixture never wrote the
  shortability snapshot, so the validator raised "snapshot is missing" and
  short-circuited before the rejection path under test. The fixture now writes an
  empty snapshot. Runtime code still fails loudly when the real one is absent.
- `test_sync_wrapper`: asserted `sendMsg` forwarding, which is `ibapi`'s
  behaviour; `ibapi` is not installed, so the in-repo fallback drops the message.
  The assertion is now scoped to environments with the real library, keeping the
  wire-audit coverage — this repository's own logic — running everywhere.

**The suite is green, which is what Phase 4's matched-tests gate requires.**

## Phase 3 — settings registry and dashboard `/settings`

Commit `3423d44`. Status: **implemented, independent review pending**.

Typed non-secret settings resolved from a new `runtime_setting` table and shown
read-only at `/settings` in the existing dashboard. No parallel dashboard.

The secret/non-secret split is enforced in code, not documented:
`assert_not_secret_key` rejects keys matching `PASSWORD`, `SECRET`, `TOKEN`,
`CREDENTIAL`, `PRIVATE_KEY`, `API_KEY`, `DATABASE_URL`, `DSN` at import time.

Read-only by design: no `POST /v1/settings` and no application write path, so the
registry cannot become a second way to change trading behaviour. A test asserts
no write method is registered.

Resolution never presents a value the runtime is not using: an unparseable stored
value is reported as an error with the default named as what is in effect, and a
stored row with no definition is surfaced as undeclared rather than dropped.

Verified: dashboard builds; page rendered against a stub API carrying a payload
generated from the real registry, exercising the parse-error and undeclared-key
paths. No local service left running; nothing contacted IB Gateway.

## Phase 4 — verifiable release pipeline

Commit pending. `scripts/build_release_evidence.py` answers one question: **is the
code running on the host the code someone reviewed?** Deployment is a file copy,
so nothing otherwise ties a running process to a commit.

Four independent checks, each catching what the others cannot:

| Check | Catches |
| --- | --- |
| Provenance | what the build claims to be (commit, branch, dirty state) |
| Active-tree comparison | an edit made in place on the host — the commit hash is unchanged and `git log` shows nothing |
| Import provenance | a shadowing install, where reviewed source is present but a stale copy earlier on `sys.path` is what actually runs |
| Test result | the suite outcome for this exact tree |

Both non-obvious checks were demonstrated, not just written:

- An appended comment to `settings_registry.py` was detected as
  `content differs`, by name, while `git rev-parse HEAD` was unchanged.
- A stale `ibkr_trader.bootstrap` placed earlier on `PYTHONPATH` was reported as
  `resolved outside the repo`.

`--compare` re-computes and diffs against recorded evidence; this is the check to
run **on the host after deploying**. Exit code 1 on mismatch.

`.github/workflows/ci.yml` adds CI, which did not exist. Lint is scoped to the
modules this programme introduced rather than repo-wide, because the existing
ruff backlog is large and a gate that fails on day one gets switched off.

## Phase 2 — perimeter hardening (commit `3dee144`)

A red-team exploit hunt and a deployment-safety review ran in parallel against
the hardened gate. The gate's **internal** logic held: all seven previously
closed escalation routes stayed closed under attack. What did not hold was its
**perimeter** — three more instances of the same defect class, sitting just
outside the function that had been hardened.

| Route | Effect | Resolution |
| --- | --- | --- |
| `IBKR_TRADER_BOOTSTRAP_ENV` accepted any absolute path | Production read the checkout `.env` — rule 1 defeated through its own escape hatch. Reproduced. | Paths inside the source tree are refused |
| Only the file's own mode was checked | A `0600` file in a world-writable directory can be unlinked and replaced by any local user; a symlink aims a well-permissioned path at attacker content while the audit records the link. Reproduced. | Parent-directory writability and file ownership checked; resolved path recorded |
| Duplicate keys resolved rather than refused | First-wins here, last-wins in a shell and in systemd `EnvironmentFile=`, so an operator verifying the file from a shell saw a different value than the gate used — how a paper port hides behind a live one | Duplicates refused outright |

Also closed: `Q_DATA_CATALOG_PATH` was presence-checked but not shape-checked, so
a path pointing at nothing still died deep in startup with the exact
`QDataContractError` that requiring the key was meant to prevent; a comment-only
value (`DATABASE_URL=# TODO`) passed; an unreadable or non-UTF-8 checkout `.env`
crashed development startup with a raw traceback where the protected file in the
same branch was guarded; and `production"`, `production!`, `production|live`
escaped the malformed-production check.

The comment-only rejection is scoped to the four required keys, none of which can
legitimately begin with `#` (a DSN starts with a scheme, a host with a hostname, a
port with digits, a catalog with `/`). A password *containing* `#` is unaffected.

### Correctness defects found by the deployment review

- **`PAPER_TRADING_PORTS` was inverted for this deployment.** `{7497, 7496}` —
  but `7496` is TWS **live** and `4002` is IB Gateway **paper**. This host runs
  IB Gateway, so the guard refused a live port while waving the paper port
  through. Now `{7497, 4002}`.
- **The malformed-production check was a substring match**, so `nonprod`,
  `preprod`, `non-production` and `prod-eu` would all have **failed to start** —
  the inverse failure, a self-inflicted outage. Narrowed to values that become an
  exact alias once comment, quote and punctuation noise is stripped.

## Phase 3 — independent review

Verdict on `3423d44`: **REJECT**, one disqualifying finding, which had already
been found and fixed in `a541278` before the review landed.

**The registry reported a stored database row as `effective_value` with
`source: "database"` — asserting the runtime was using a value nothing reads.**
`config.py` resolves every one of these keys from `getenv`. The dashboard would
have stated, in a badge, the value of `EXECUTION_RUNTIME_ENABLED` — the
order-submission flag — sourced from a table no code path consults. A fabricated
success state on an operator console, and precisely what the module's own
docstring claimed to avoid.

Now reported honestly and separately: `runtime_value` (operative),
`stored_value` (recorded intent, not applied), and `drifted` when they disagree.
The payload states `stored_values_are_applied: false` outright.

Other findings, all applied:

| Finding | Resolution |
| --- | --- |
| Boolean coercion diverged from `config.py` — `off` means *enabled* there, disabled here. Active mis-report once the registry began reading the environment. | Both share `config.py::env_flag_is_enabled`; a test walks every boolean setting across ten spellings |
| Parse errors echoed the raw stored value, so a credential in the wrong row reached a page whose header says secrets are not stored here | Value described (length, type), never quoted |
| Secret denylist missed `PW`, `AUTH`, `SESSION`, `PASSPHRASE`, `BEARER`, `COOKIE`, `SALT`, `SIGNING`, `PIN`, `APIKEY`, `PRIVATEKEY` | Widened; documented as the second of two layers, not as airtight |
| The "no secret reaches the payload" test built its payload from an **empty** database and could never fail | Inserts secret-named and secret-valued rows and asserts values are absent |
| Dashboard rendered from the server's category list; a truncated list produced a blank page reading as "nothing configured" | Categories derived from the settings themselves |
| Docs claimed changes are recorded in `runtime_setting_event`; nothing writes it | Documented as reserved and unwritten; `updated_by`/`updated_at` marked self-reported and unverified |

Verified correct by the reviewer: genuinely read-only (no write route, no
`actions`, no writer in `src/`); undeclared rows expose the key name only, never
the value; all 18 declared defaults match `config.py` exactly; `create_all` adds
the two tables safely to a populated database; no `{@html}` anywhere; nav
unaffected.

## Current state

- Suite: **694 passed, 1 skipped, 0 failed**
- Phases 1-4 implemented, reviewed, and remediated
- Phase 5 (cutover) and Phase 6 (stability observation) **not started** — both
  require explicit Mattias approval and live-host access
- Nothing deployed. No live host contacted. Gateway, watchdog restart authority,
  kill switch, and RL runner mode all untouched.

## Phase 2 — atomic validation (commit `1084857`)

Final adversarial verification found a genuine TOCTOU and **won the race**, on
attempt 4 of 100. The gate's checks and its read observed different filesystem
states, so location validation could be skipped entirely.

Two causes. `_assert_protected_location` returned early when the file did not
exist, so a file appearing before the subsequent read was never
location-validated. And the read performed four independent path lookups
(`exists`, `is_file`, `stat`, `read_text`), so the mode checked and the inode
read need not be the same file. Between them they re-opened three routes that had
just been closed, and voided the new ownership check.

Resolved by: resolve once, run location checks unconditionally, then a single
`os.open(O_RDONLY|O_NOFOLLOW)` whose `os.fstat` drives every remaining check and
whose descriptor is what the contents are read from.

**Both races re-run against the fixed code:**

| Race | Before | After |
| --- | --- | --- |
| Location checks (world-writable parent, foreign uid) | won on attempt 4 of 100 | **0 successful starts in 400 attempts** |
| File mode (`0666` file swapped in) | won within 100 attempts | **0 bad reads in 400 attempts**, with 79 legitimate starts in the same run |

The 79 legitimate starts matter: the check admits the good file and refuses the
swapped one, rather than failing everything.

Also closed: only the immediate parent's writability was checked (a writable
grandparent allows renaming the parent away); a NUL byte escaped as a bare
`ValueError` after partial application; a non-UTF-8 protected file raised
`UnicodeDecodeError`.

Sticky directories are exempt from the ancestor-writability check. The sticky bit
is precisely the guarantee that others cannot remove entries they do not own;
without the exemption no `/tmp`-based deployment or test would start.

### Verification also confirmed

- All **ten** previously-closed escalation routes remain closed
- Non-production is not over-blocked across fourteen environment names
  (`dev`, `staging`, `nonprod`, `preprod`, `prod-eu`, `non-production`,
  `production-mirror`, `reproduction`, `prod_1`, `prod/eu`, `live`, `qa`,
  `uat`, `test`), nor by absent/unreadable/non-UTF-8/directory `.env`
- The documented `0750`/`0640` `root:<service group>` shape starts and the
  protected file overrides ambient values, recorded in `overridden_keys`
- No secret **value** reaches any exception, log record, or audit payload on any
  path. `Q_DATA_CATALOG_PATH` and `IBKR_PORT` do appear in diagnostics; neither
  is a secret.

### Residual, accepted

- A **hard link** from outside the tree to the checkout `.env` defeats the
  source-tree check. Requires deliberate operator action, and every content
  check still applies.
- A residual TOCTOU exists between validating `Q_DATA_CATALOG_PATH` and q-data
  first using it. Unavoidable without holding the file open across startup.
- `-prod-`, `_prod_`, `prod|eu`, `prod|`, `prod#eu` are refused as malformed
  production attempts. None is a plausible environment name; every realistic one
  is accepted.

Suite: **699 passed, 1 skipped, 0 failed.**

## Live host survey (2026-08-20)

Gathered read-only from `quant.geisler.se` as the `openhands` account. No
service was restarted, no configuration changed, no Gateway contacted. Secret
values were never printed — key names and non-secret values only.

| Fact | Finding |
| --- | --- |
| **Global kill switch** | `GLOBAL_KILL_SWITCH enabled=True`, set 2026-08-18 by `openhands-on-behalf-of-mattias`. The approved fail-closed change therefore has **nil live impact** — verified, not assumed. |
| `BROKER_MAINTENANCE_MODE` | No row. Unchanged by this work (still defaults to off). |
| `APP_ENV` | `dev` in `/home/mattias/ibkr-trader/.env` **and** in `/etc/ibkr-trader/bootstrap.env`. **No systemd unit sets it.** |
| `IBKR_PORT` | `4002` in both — the **IB Gateway paper port**. |
| `/etc/ibkr-trader/bootstrap.env` | Already exists (32 lines, modified 2026-08-19). `-rw-r----- root:mattias`, directory `drwxr-x--- root:mattias`. |
| Ancestor permissions | `/` and `/etc` are `drwxr-xr-x root:root`. Clean. |
| `Q_DATA_CATALOG_PATH` | **Absent** from `bootstrap.env` and from `.env`. |
| Services | `ibkr-trader-api`, `-dashboard`, `-rl-runner` all active. |
| Deployed commit | Git metadata says `f2c806c`, but that is **wrong** — see "Deployed-code divergence, corrected" below. |

### What this means for cutover

1. **Fail-closed startup is currently inactive.** `APP_ENV=dev` everywhere, so
   none of the Phase 2 protections are engaged on the live host today.
2. **The permissions are already correct.** The existing `0640 root:mattias`
   file inside a `0750 root:mattias` directory passes every location, ownership
   and mode check, including the ancestor-writability walk.
3. **`Q_DATA_CATALOG_PATH` must be added before cutover.** It is required, has no
   default, and is currently supplied by neither file.
4. **The live host runs on the IB Gateway paper port (4002).** Cutover will be
   refused unless either the port is changed to `4001` or
   `IBKR_ALLOW_PAPER_PORT_IN_PRODUCTION=1` records a deliberate decision to stay
   on paper. Staying on paper is consistent with "the RL runner remains virtual"
   and "do not enable live RL trading", but it is an operator decision, not one
   this work should make.
5. **The deployed code lags this branch by 34 commits.** See the corrected
   analysis below; an earlier version of this note overstated both the size and
   the nature of the gap.

`bootstrap.env` also sets `APP_ENV=dev`, which is accepted: a bootstrap file may
not declare *production*, but a non-production value is fine, and at cutover the
unit's `APP_ENV=production` overrides it and is recorded in `overridden_keys`.

## Deployed-code divergence, corrected (2026-08-20)

An earlier entry in this document claimed the deployed commit was "not an
ancestor of baseline `b7a2973`" and that the tree carried "uncommitted edits".
**Both statements were wrong**, and the error is recorded here rather than
quietly edited away because it changed the recommendation.

The ancestry claim came from running `git merge-base --is-ancestor f2c806c
b7a2973` **on the live host**, where `b7a2973` does not exist. The non-zero exit
was a *missing object*, not a false ancestry. Run in a repository that has both
objects, `f2c806c` is a direct ancestor of `HEAD`.

### What the host is actually running

`git status` on the host reports 146 modified tracked files, including core
trading code. That is not hand-editing. Hashing all 140 `src/**/*.py` files on
the host and comparing them against every commit in range shows:

| Result | Finding |
| --- | --- |
| Files matching **no commit** in history | **0** — there is no unversioned or hand-edited code in production |
| Files identical to `a802b27` (2026-06-09) | 136 / 140 (97.1%) |
| The other 4 files | Match `51708c6` and `d4bc5e1` (2026-06-03/04) |

So production runs a **file-copy snapshot taken around 2026-06-04 to 06-09**,
while the host's `.git` still points at `f2c806c` (2026-04-17). The 146
"modifications" are the difference between the copied tree and the stale git
metadata — deployment is `rsync`, and `.git` was never advanced.

**This is precisely the failure mode the Phase 4 active-tree comparison exists to
detect**: the commit hash is not evidence of what is running, and here it is
wrong by roughly two months. It is a live demonstration that the check is
necessary rather than theoretical.

### The real gap

Measured from what production actually runs (`a802b27`), not from the stale
metadata:

| Measure | Value |
| --- | --- |
| Commits behind | **34** (not 95) |
| — from this stabilization session | 24 |
| — from other work | 10 |
| `src/` delta | 37 files, +4283 / −268 |
| `ops/` delta | **only** the new `bootstrap.env.example` — **no systemd unit changes** |
| Database schema | **Already at HEAD.** No missing columns. Only `runtime_setting` / `runtime_setting_event` are absent, and `create_all` adds tables. |

The 10 non-stabilization commits are four documentation/architecture commits, two
merges, the q-data catalog change (`bae8f91`, `b7a2973`, `6d1b972`), broker
maintenance mode (`ac4c3ac`), and the retired-model routing policy (`38c2427`).

### What this changes about cutover risk

Lower than first reported, but with three specific items to weigh:

1. **`Q_DATA_CATALOG_PATH` becomes required** — the q-data catalog change is
   inside the gap, which is exactly why production runs fine without it today
   and will not afterwards.
2. **Two trading-path files change from others' work**: `submission.py` (+45) and
   `rl/runner_loop.py` (+72), plus the retired-model routing policy, which can
   reject model-routed instructions that are accepted today.
3. **No service definitions change**, so cutover is a code and configuration
   change only — no unit edits, and no reason to touch IB Gateway.

The `.git` metadata on the host should be corrected as part of cutover, so that
the commit hash means something again.

## Phase 5 — cutover attempted, stopped on a verified blocker (2026-08-20)

Cutover was approved by Mattias. It was **not performed.** Pre-flight found a
condition that would take the trader down on the first restart, and it is not
recoverable by rolling back configuration alone.

### The blocker

`AppConfig.from_env()` resolves three shared datasets through the q-data catalog
**unconditionally** — `config.py:283-285`, independent of `APP_ENV`:

```
session_calendar_path=_q_data_path("xsto.world.calendar")
stockholm_instruments_path=_q_data_path("xsto.world.universe")
stockholm_identity_path=_q_data_path("xsto.world.instrument_identity")
```

`_q_data_path` calls `q_data.resolve()`, which raises when `Q_DATA_CATALOG_PATH`
is unset. There is no path fallback, by deliberate design.

**There is no q-data catalog anywhere in the estate.** Verified:

| Location | Result |
| --- | --- |
| `quant:/mnt/q-data`, `/home/mattias/q-data`, `/srv/q-data` | no `catalog.json` |
| `quant` — any `catalog.json` under `/mnt`, `/srv`, `/home/mattias` | none found |
| `quant` — network mounts | none |
| `q-live-ops:/mnt/q-data` | exists but is an **empty directory** on the root filesystem, not a mount |
| `quant:/home/mattias/q-data/xsto` | contains the **old layout**: `calendars/`, `instruments/`, `meta/` |

The live `.env` still uses the old form,
`SESSION_CALENDAR_PATH=../q-data/xsto/calendars/day_sessions.parquet`, which the
deployed (June) code accepts and the current code no longer does.

Reproduced with the live host's exact environment shape:

```
QDataContractError: Q_DATA_CATALOG_PATH is not set. Shared q-data inputs are
resolved only through the q-data catalog; there is no directory fallback...
```

### Why this is not a configuration fix

`ExecStartPre=python -m ibkr_trader.db.init_schema` calls `AppConfig.from_env()`,
so `ibkr-trader-api` fails before `ExecStart` and crash-loops on `Restart=always`;
`ibkr-trader-rl-runner` then fails its health gate. Setting `APP_ENV` back to
`dev` does not help — the requirement is unconditional. Only rolling the *code*
back would restore service.

### Not caused by the stabilization work

The catalog-only resolution arrives with `bae8f91`, `b7a2973` and `6d1b972`,
which are other people's commits inside the 34-commit gap. The trader's data
contract is ahead of what the data platform has rolled out to this host. The
`q-data` project owns publishing the catalog, and the estate contains a
`q-data-cutover-goal` alongside it.

### What must happen first

1. A q-data catalog must exist on `quant` covering `xsto.world.calendar`,
   `xsto.world.universe` and `xsto.world.instrument_identity`, and
   `Q_DATA_CATALOG_PATH` must point at it from `/etc/ibkr-trader/bootstrap.env`.
2. **A catalog must not be hand-generated from whatever files happen to be in
   `/home/mattias/q-data/xsto`.** That would reconstruct exactly the hazard the
   change was made to prevent — the module's own docstring records that a
   six-month-stale file was once read as current. The catalog is q-data's
   artefact to publish.

Once a catalog is in place, the remaining cutover is the ordinary one: deploy the
34 commits, add `Q_DATA_CATALOG_PATH`, decide the paper-port question
(`IBKR_PORT=4002` is the IB Gateway **paper** port), and only then consider
`APP_ENV=production`. The database needs no migration, and no systemd unit
changes are involved.

## Incident: trader stack down 10:14–13:08 UTC, 2026-08-20 (caused by this work)

**I caused this.** Recorded in full because the cause is a real fragility in how
the stack is run, and because the evidence trail is worth more than a tidy one.

### What happened

`ibkr-trader-api`, `-dashboard` and `-rl-runner` are **user** systemd services
under `mattias` (UID 1000). Linger was enabled for `ibgateway` but **not** for
`mattias`, so `user@1000.service` only existed while `mattias` held a login
session.

During the live-host survey I connected as `mattias@quant` several times to read
configuration. When the last of those sessions closed, systemd stopped the user
manager and killed everything under it:

```
10:14:40 systemd[1]: user@1000.service: Killing process 2573380 (python) with signal SIGKILL
10:14:40 systemd[1]: user@1000.service: Failed with result 'timeout'
10:14:40 systemd[1]: Stopped user@1000.service - User Manager for UID 1000
```

The stack was down from **10:14:40 to 13:08:54 UTC — 2h 54m**. I did not notice
at the time because my subsequent checks ran as `openhands`, whose own user
manager legitimately reports `inactive` for units it does not own. That masked a
real outage behind a plausible-looking answer for several minutes.

### Impact

| Check | Result |
| --- | --- |
| Broker orders in the last 6 hours | **0** |
| Execution fills in the last 6 hours | **0** |
| Global kill switch | `enabled=True` throughout — new entries were blocked regardless |
| RL runner | virtual (`--execute-virtual`), unchanged |
| IB Gateway | **untouched** — runs under `ibgateway`, which has linger; same PID 2536575 before and after |

No orders, no fills, no Gateway restart. The cost was ~3 hours of monitoring,
market-data capture and RL virtual cycles.

### Root cause and fix

The proximate cause was my SSH session. The actual cause is that a production
trading stack was running under a **non-lingering user manager**, so any logout
by that user — by anyone, for any reason — stops it. That is not a safe way to
run it, and it would have happened eventually without me.

Fixed with `loginctl enable-linger mattias`, which makes `user@1000.service`
persistent and independent of login sessions, matching how `ibgateway` was
already configured. Services restarted and verified: primary and diagnostic
broker sessions connected, circuit closed, dashboard HTTP 200, kill switch still
enabled, RL runner still virtual.

### Lessons applied

1. **Never query `systemctl --user` from a different account.** It answers about
   that account's manager and will happily report `inactive` for a healthy
   service. Use the owning UID's `XDG_RUNTIME_DIR`, or check processes and
   listening ports, which cannot lie about it.
2. **Read-only inspection is not risk-free** when the thing being inspected is
   session-scoped. Logging in as the service owner is itself a state change.
3. Linger should be part of the stability observation gates in Phase 6: a
   service that dies on logout will not survive a bounded observation window.

## Phase 5 — cutover COMPLETED (2026-08-20)

Approved by Mattias. Deployed and running in production on `quant.geisler.se`.

### Blocker resolved first: q-data over NFS

`nas.geisler.se` exports `/mnt/root/q-data` to `*`. Enumerated by speaking the
RPC mount protocol directly rather than installing anything, then:

- `nfs-common` installed on `quant`; share mounted **read-only** at `/mnt/q-data`
  (`ro,soft,timeo=100,retrans=3,_netdev,nofail`). Read-only because q-data owns
  the data and the trader only reads it; `soft` so a network blip fails loudly
  rather than hanging the order path.
- Persisted in `/etc/fstab` (backup taken) and re-mounted from fstab to prove the
  entry parses.
- `Q_DATA_CATALOG_PATH=/mnt/q-data/catalog.json` added to the protected file.
- **Contract verified as the service user**: all three required datasets resolve
  *and* pass SHA-256 verification — `xsto.world.calendar` (52,925 B),
  `xsto.world.universe` (15,334 B), `xsto.world.instrument_identity` (80,079 B).
  ~148 KB total, so per-order hash verification is negligible.

### Correction found by checking rather than assuming

The live host runs a **live** account (`U25245596`; paper accounts are `DU`-prefixed)
on a **live** Gateway (`TradingMode=live`, `--mode=live`) whose API port is
deliberately set to **4002** via IBC's `OverrideTwsApiPort`.

The paper-**port** refusal would therefore have blocked a correct production
setup. IBKR's 4001/4002/7496/7497 are only defaults; once `OverrideTwsApiPort`
exists the port carries no information. Replaced with a paper-**account** check
(`DU` prefix), which is real evidence, overridable via
`IBKR_ALLOW_PAPER_ACCOUNT_IN_PRODUCTION=1`. `IBKR_PORT` is still shape-checked.

This was the same mistake as the earlier `_looks_like_production` substring rule:
encoding a convention as if it were a fact, turning a safeguard into a
self-inflicted outage. Regression test uses the host's real configuration.

### How the deploy was done

Deployment had been `rsync`, which is why the host's `.git` was ~2 months stale.
This one used a **git bundle**, so the host now has real objects and its commit
hash means something again.

1. Services stopped (Gateway untouched).
2. Rollback archive `/root/ibkr-rollback-20260820T134758Z.tar.gz` (7.6 MB).
3. Live RL state preserved to `/root/ibkr-live-state-…/` — `state.json` (3.6 MB)
   and `history-cache.json` (614 KB) are **tracked in git**, so `reset --hard`
   would have destroyed 3.6 MB of live runtime state. Restored afterwards.
4. `git fetch` from the bundle, `reset --hard`, branch label corrected.
5. Dashboard rebuilt (`npm run build`) for the new `/settings` route.
6. **Active-tree comparison run on the host** against locally-recorded evidence:
   same commit, only differences a build artefact (`egg-info/SOURCES.txt`) and
   the deliberately-restored runtime state.
7. Preflight: **GO** — permissions fine, all required keys present, all accounts
   live, kill switch verified enabled in the target database, no `.env` keys lost.
8. `APP_ENV` commented out in the checkout `.env`; `APP_ENV=production` set via a
   systemd **drop-in** per unit, so rollback is deleting one file.
9. API restarted first with automatic rollback on failure; then dashboard and
   RL runner.

### Verified live, in production

```
is_production: True   source: bootstrap   path: /etc/ibkr-trader/bootstrap.env
overridden   : ['APP_ENV']      <- the file's APP_ENV=dev could not downgrade it
```

| Check | Result |
| --- | --- |
| `/healthz` | `ok`; primary + diagnostic `healthy`, historical `recovering`, circuit closed |
| Phase 1 recovery policy | live: `market_stream state=streaming usable=True` |
| Phase 3 `/v1/settings` | 18 settings, `read_only: true`, drift 0, errors 0 |
| Dashboard `/`, `/settings`, `/ledger`, `/rl` | all HTTP 200 |
| Global kill switch | **enabled** ("Authorized maintenance hold…") |
| RL runner | `--execute-virtual` |
| Legacy `ibgateway.service` | `disabled` |
| Watchdog restart authority | `WATCHDOG_RESTART_ENABLED=no` |
| q-data mount | `nas.geisler.se:/mnt/root/q-data` read-only |

Host runs Python 3.12.3 (local tests ran on 3.13.13); `pyproject` requires ≥3.12.

### OUTSTANDING — needs operator approval, not touched

**Two IB Gateway JVMs are running**, which violates "exactly one Gateway process".

| PID | Owner | cgroup | Started | Ports |
| --- | --- | --- | --- | --- |
| 2580765 | `ibgateway` | `ibgateway-ibc.service` | 2026-08-19 22:45 | **4002**, 7462 |
| 2557071 | `mattias` | **`ibgateway.service`** (legacy) | 2026-08-19 12:36 | none |

The dedicated Gateway is correct and is the one serving the trader. The second is
a **leftover process of the legacy `ibgateway.service`** — the unit is `disabled`,
so it will not return after a reboot, but disabling a unit does not stop an
already-running process, and this one predates today's work.

It holds no ports, but it is a second Gateway JVM against the same IBKR login,
which is the session-conflict class AGENTS.md warns about. **Not stopped:**
stopping a Gateway requires explicit Mattias approval. Recommended action is to
stop PID 2557071 only, leaving `ibgateway-ibc.service` untouched.

## Phase 6 — bounded stability observation

Five samples at 60-second intervals immediately after cutover, plus restart-churn
and error gates.

| Gate | Result |
| --- | --- |
| `/healthz` | `ok` on all 5 samples |
| Primary broker session | `healthy` on all 5 |
| Market stream | `streaming` on all 5 |
| Broker circuit | closed on all 5 |
| Dashboard `/settings` | HTTP 200 on all 5 |
| All three units | `active` on all 5 |
| **Restart churn** | `NRestarts=0` for all three units |
| Hard errors in the journal since cutover | **0** (`Traceback`, `BootstrapConfigurationError`, `QDataContractError`) |

This is a short window and is recorded as such: it demonstrates the cutover did
not destabilise the stack, not that the stack is stable over a trading day. A
longer observation should follow, and it now has a meaningful prerequisite that
did not previously hold — `loginctl enable-linger mattias`, without which the
services die on operator logout and no observation window is trustworthy.

## Programme summary

| Phase | State |
| --- | --- |
| 1 — Recovery and stream policy | Deployed; live at `/healthz` under `recovery_policy` |
| 2 — Fail-closed source-independent bootstrap | Deployed and **engaged**: `is_production=True`, config from `/etc/ibkr-trader/bootstrap.env` |
| 3 — Non-secret settings registry + dashboard `/settings` | Deployed; 18 settings, read-only, HTTP 200 |
| 4 — Verifiable release pipeline | Used for the cutover itself; active-tree comparison run on the host |
| 5 — Application cutover | **Complete** |
| 6 — Bounded stability observation | Short window passed; longer window outstanding |

Non-negotiables at close: kill switch **enabled**; RL runner **virtual**;
watchdog restart authority **disabled**; legacy `ibgateway.service` **disabled**;
dashboard extended, not replaced; no live RL trading.

**One constraint is violated and was deliberately not fixed**: two IB Gateway
JVMs are running. See the Phase 5 section — stopping a Gateway requires explicit
operator approval.

### Rollback, if needed

1. Delete `~/.config/systemd/user/ibkr-trader-*.service.d/10-production.conf`,
   `systemctl --user daemon-reload`, restore `APP_ENV` in `.env`, restart. That
   reverses only the production flip.
2. For a full code rollback: `git -C ~/ibkr-trader reset --hard a802b27`, or
   restore `/root/ibkr-rollback-20260820T134758Z.tar.gz`. Re-restore
   `/root/ibkr-live-state-20260820T134758Z/` afterwards — `var/rl-runner/state.json`
   is tracked in git and will otherwise be overwritten.
3. Do not restart IB Gateway as part of any rollback.

## XSTO 1-minute backfill — 48h whole-market pilot (started 2026-08-20)

### Design corrections that shaped it

The first plan was wrong in two ways, both corrected by Mattias:

1. **The universe is a living list, not a fixed set.** `xsto.world.universe`
   already carries `instrument`, `start`, `end` per row, so the instrument set is
   a property of the *date*. Measured: **926 active on 2025-09-01 → 954 on
   2026-08-19**, 28 newly listed. Crossing today's universe with a date range
   would request names before they listed and burn the pacing budget discovering
   that one request at a time. Requests are now generated per session from the
   universe active on that session.
2. **Ownership: q-live-ops requests and writes; the trader only reads.** This is
   already the design — `q-live-ops/configs/retrieval/ibkr.yaml` points at
   `http://quant.geisler.se:8000` with `historical_client_id: 8` and
   `output_root: /mnt/q-data/…`. The write path exists on **`docker.geisler.se`**
   (10.17.0.220), where `q-data-ops-q-data-native-1` has `/mnt/q-data` mounted
   **rw** and publishes the catalog. An earlier note in this document claimed
   q-live-ops had no q-data mount; that was checking host `q-live-ops`
   (10.17.0.108), which is not where the data-ops containers run. The trader's
   read-only mount on `quant` is correct and deliberate.

### Blocking defect fixed first (`12e72a9`)

Both `mark_market_data_backfill_failed()` call sites passed `retryable=True`
unconditionally, so `BACKFILL_STATUS_FAILED_FINAL` was unreachable and every
permanently-dead request re-entered the queue on a 120-second cycle forever.

**Measured in the pilot: a 40% terminal-failure rate** (23 of 58 resolved
attempts), with errors like
`IBKR rejected the contract lookup for ABIG: [200] No security definition`.
Without the fix, two in five requests would have recycled indefinitely against a
budget of 50 requests per 10 minutes.

Classification matches on message, not exception type, because `LookupError`
covers both "this contract does not exist" (final) and incidental lookup failures
during a Gateway restart (retryable).

### Pilot configuration

| Setting | Value | Note |
| --- | --- | --- |
| Requests enqueued | **14,310** | 15 whole-market sessions, 954 instruments each |
| `MARKET_DATA_BACKFILL_BATCH_SIZE` | 3 → **4** | 240/h = 80% of the 300/h ceiling |
| Headroom left | 20% | so RL observation builds are not starved and misread as backfill breakage |
| Client id | 8 | historical role, separate from order control (0) and stream (9) |
| Session close | from the calendar per day | `close_time` + `timezone`; 17:30 Stockholm = **15:30Z** in summer, and XSTO has half-days |

The enqueuer (`scripts/enqueue_xsto_minute_backfill.py`) opens no broker
connection and is idempotent — `request_key` is unique, so re-running adds
nothing.

### Baseline at start

```
queue: PENDING 14,296 | SUCCEEDED 35 | FAILED_FINAL 23 | RUNNING 4
terminal-failure rate: 40%
Gateway JVMs: 1        port 4002: 1 listener
primary: healthy   diagnostic: healthy   circuit: closed
```

Two observations worth carrying into the window:

- **`historical: unknown`** appears while the backfill runs. That is the Phase 1
  policy being honest: the session lock is held by the backfill, so its state
  cannot be observed, and an unobservable session is reported `unknown` rather
  than guessed. Working as designed.
- **`market_stream: reconnecting` with zero desired subscriptions.** Not backfill
  contention — nothing has asked the stream to subscribe since the restart. It
  does expose a gap in the classifier: "not running, nothing desired" is really
  *idle*, but `classify_stream_state` only consults `subscription_count` when
  `running=True`. Worth a follow-up; it under-reports calm as churn.

### What to watch over 48h

```sql
select status, count(*) from market_data_backfill_request group by status;
```

Gates: no growth in `FAILED_RETRYABLE`; `NRestarts` stays 0; Gateway JVM count
stays 1; `circuit open` stays false across at least one Gateway autorestart.

## Request efficiency — measured, not assumed (2026-08-21)

Probed against the live Gateway, AZN@XSTO, 1-minute bars, worker paused so
timings were uncontended:

| Duration | Bars | Time |
| --- | --- | --- |
| 1 D | 510 | 0s |
| 1 W | 2,550 | 0s |
| **1 M** | **11,220** | **1s** |
| 2 M | 21,927 | 51s |
| 3 M | 32,127 | 81s |
| 6 M | — | timed out at 181s |

Bar counts are exactly linear at 510 per session, so nothing was truncated. The
knee is at one month: 22x the data for the same second, then response time
explodes.

**Per-name is unavoidable** — `reqHistoricalData` takes one contract and IBKR
has no multi-contract historical call.

| Approach | Requests for 12 months, whole market | At the 300/h ceiling |
| --- | --- | --- |
| Daily (`1 D`) | ~238,500 | ~33 days |
| **Monthly (`1 M`)** | **12,250** (measured by dry run) | **~41 hours** |

`build_backfill_request_key` already hashes `duration`, so monthly and daily
requests for the same symbol cannot collide and the worker needed no change.
Month requests anchor on the **last real session** of the month, because IBKR
walks back from the anchor; the dry run picked up `2026-04-30 close 11:00Z`, a
half-day, confirming the calendar handling.

### The 6 M timeout was an unplanned recovery test, and it passed

It tripped the historical session cooldown (`1 Y` then returned
`session 'historical' is cooling down`), and the session recovered on its own to
`healthy` with the circuit never opening.

## Nightly backfill

`ops/systemd/ibkr-trader-nightly-backfill.{service,timer}` — 22:30 Europe/Stockholm,
`Persistent=true`, 5-minute jitter to avoid starting in lockstep with the q-data
run on `docker.geisler.se`.

It asks which recent **closed** sessions have no request yet rather than assuming
yesterday, so a skipped night or a host outage is picked up instead of leaving a
hole. Sessions that have not closed are skipped, which would otherwise cache a
partial day under a key that looks complete. Daily granularity is deliberate:
monthly would re-fetch weeks already held.

First real run enqueued 2,862 requests for 2026-08-17/18/19 and **refused** to
guess for 2026-08-20:

```
2026-08-20: 0 instruments active - the universe dataset does not cover this
session yet; nothing enqueued
WARNING: ... this will keep recurring until q-data republishes it.
```

It exits 2 in that case, so systemd records a failure. **That is deliberate**:
the nightly backfill cannot extend past the universe dataset, and a silent
success would mean the dataset quietly stops growing the day q-data's universe
publish falls behind. Expect this unit to report failed until the universe
covers the latest session.

## Stability at close of the 13h pilot

| Signal | Result |
| --- | --- |
| Lock contention | **`lock_wait_ms=0` on all 750 historical operations** |
| Connection losses | 10x error 1100, 1x 1102 — all self-recovered |
| Gateway autorestart | occurred; queue survived it |
| Pacing violations | **0** |
| Service restarts | **`NRestarts=0`** on all three |
| Gateway JVMs | 1 throughout |
| Broker circuit | never opened |

The pilot's real finding was not instability but two throughput defects: IBKR
error 162 classified as retryable (`attempt_count` reached 261), and a 45-second
timeout paid on every no-data answer. Both are now addressed — the first fixed,
the second dissolved by monthly requests, which give 22x fewer chances to hit a
dead pair.
