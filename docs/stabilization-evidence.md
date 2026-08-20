# Stabilization program evidence

Acceptance evidence for the phases in `docs/approved-stabilization-goal.md`.
Append-only: each phase records what was built, how it was verified, and what
review found. Claims here must be reproducible from the recorded commits.

Branch: `stabilization/phases-1-4`
Baseline commit: `b7a2973` ("Resolve shared q-data through the catalog only")

## Standing constraints (unchanged by this work)

| Constraint | State | Evidence |
| --- | --- | --- |
| Global kill switch enabled | Untouched by this work — **but see Open finding 1** | No change to `orchestration/operator_controls.py` in `ddb394f`/`73bc591` |
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

These three remain failing and unmodified. They are a **Phase 4 prerequisite**:
a release gate cannot pass while they are red, so they must be fixed or
explicitly quarantined with justification before Phase 4 completes.

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
| Deployed commit | `f2c806c`, with uncommitted local modifications. **Not an ancestor of baseline `b7a2973`**, and `src/ibkr_trader/q_data.py` is absent. |

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
5. **The deployed code has diverged from this branch.** `f2c806c` is not an
   ancestor of the baseline, the deployed tree carries uncommitted edits, and it
   predates the q-data catalog change. Deploying this branch is therefore not a
   small delta, and the divergence must be reconciled before Phase 5. This is
   exactly what the Phase 4 active-tree comparison exists to make visible.

`bootstrap.env` also sets `APP_ENV=dev`, which is accepted: a bootstrap file may
not declare *production*, but a non-production value is fine, and at cutover the
unit's `APP_ENV=production` overrides it and is recorded in `overridden_keys`.
