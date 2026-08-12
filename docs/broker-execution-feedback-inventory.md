# Broker execution and feedback domain inventory

This is a **read-only static inventory** of the broker execution and feedback domain in
`ibkr-trader`, produced for graph ingestion. Every claim below points at repository
evidence (`path:line`). It is not a readiness statement, not an operating procedure, and
not a description of live host state.

- **Commit inventoried:** `a762f812bb9421861960f96b9e0fb742db647618` (2026-08-10)
- **Branch:** `agent/task-fe903381-e341f229`
- **Method:** static reading of tracked files only. No broker credentials, runtime
  accounts, live hosts, databases, or IB Gateway sessions were accessed.

Companion documents: [`docs/codebase-map.md`](codebase-map.md) (repository-wide
orientation), [`docs/instruction-contract.md`](instruction-contract.md),
[`docs/client-id-policy.md`](client-id-policy.md),
[`docs/ibkr-order-wire-audit-2026-05-24.md`](ibkr-order-wire-audit-2026-05-24.md).

## Operational boundaries for anyone reading this map

- Do **not** start or restart the API, dashboard, RL runner, or IB Gateway. The API
  lifespan starts broker-facing background work
  ([`src/ibkr_trader/api/server.py`](../src/ibkr_trader/api/server.py)).
- Do **not** run order, probe, stream, backfill, or runtime-cycle commands from a
  development checkout. Two API processes against one Gateway is an operational fault
  ([`AGENTS.md`](../AGENTS.md)).
- Do **not** treat the identifiers, endpoints, or client IDs listed here as authority to
  act on a live account.

---

## 1. Model-to-order path

The path from an RL model decision to a broker order is a fixed six-hop chain. Intent is
never sent to IBKR directly; it is always converted into the broker-neutral instruction
contract first.

| Hop | What happens | Evidence |
| --- | --- | --- |
| 1. Candidate exists | A **model-routed** instruction is persisted in state `MODEL_ROUTED_PENDING`. An instruction is model-routed exactly when its payload carries an `execution` block. | [`domain/execution_contract.py:302`](../src/ibkr_trader/domain/execution_contract.py), [`orchestration/submission.py:339-357`](../src/ibkr_trader/orchestration/submission.py) |
| 2. Runner reads candidates | RL runner polls `GET /v1/rl/candidates`, filters by `model_id`, `trace.trade_date`, and `reason_code`. | [`rl/runner_loop.py:89-100`](../src/ibkr_trader/rl/runner_loop.py), [`api/server_routes_rl.py:354`](../src/ibkr_trader/api/server_routes_rl.py) |
| 3. Runner builds state + observation | Authoritative per-symbol state comes from the API (`GET /v1/rl/runtime-state`), observations from `POST /v1/rl/observations/build`. Model inference is local to the runner process. | [`rl/runner_loop.py:255-262`](../src/ibkr_trader/rl/runner_loop.py), [`rl/runner_loop.py:487-491`](../src/ibkr_trader/rl/runner_loop.py), [`rl/runner_loop.py:592-604`](../src/ibkr_trader/rl/runner_loop.py) |
| 4. Action translation | `POST /v1/rl/actions/translate` validates deployment/model/account/symbol binding, then calls `translate_rl_action()`. | [`api/server_routes_rl.py:484-560`](../src/ibkr_trader/api/server_routes_rl.py), [`rl/action_translation.py:45`](../src/ibkr_trader/rl/action_translation.py) |
| 5a. Entry actions | Translation emits a **full instruction batch payload**, which is submitted through the ordinary `submit_execution_batch()` path after intent supersession. | [`rl/action_translation.py:276-377`](../src/ibkr_trader/rl/action_translation.py), [`api/server_routes_rl.py:571-599`](../src/ibkr_trader/api/server_routes_rl.py) |
| 5b. Mutating actions | `cancel_entry`, `clear_exit`, `exit_market`, `exit_tp_*bp` emit **no** instruction payload; they mutate the durable RL-owned instruction via `execute_owned_rl_action()`. | [`rl/action_translation.py:182-243`](../src/ibkr_trader/rl/action_translation.py), [`orchestration/rl_action_execution.py:106`](../src/ibkr_trader/orchestration/rl_action_execution.py) |
| 6. Wire submission | `submit_order_from_instruction()` / `submit_exit_order_from_instruction()` resolve contract + account, size, validate, and place the IBKR order. | [`ibkr/order_execution_submission.py:29`](../src/ibkr_trader/ibkr/order_execution_submission.py), [`ibkr/order_execution_submission.py:317`](../src/ibkr_trader/ibkr/order_execution_submission.py) |

### Action vocabulary and its state gates

`translate_rl_action()` is a strict state machine over runner-side states `FLAT`,
`ENTRY_PENDING`, `LONG_OPEN`, `SHORT_OPEN`, `EXIT_PENDING`
([`rl/action_translation.py:18-22`](../src/ibkr_trader/rl/action_translation.py)):

| Action | Valid from | Result | Emits instruction payload? |
| --- | --- | --- | --- |
| `skip`, `wait` | any of the five | `logged`, state unchanged | no |
| `market_entry` | `FLAT` | `translated` → `ENTRY_PENDING`, `MARKET` order | yes |
| `entry_prevclose_<signed_bp>bp` | `FLAT` | `translated` → `ENTRY_PENDING`, `LIMIT` order priced off `previous_close` | yes |
| `market_entry` / `entry_prevclose_*` | `ENTRY_PENDING` | `logged` — explicitly refuses to duplicate a pending entry | no |
| `cancel_entry` | `ENTRY_PENDING` | `translated` → `FLAT` | no (owned mutation) |
| `exit_market` | open-for-side or `EXIT_PENDING` | `translated` → `EXIT_PENDING` | no (owned mutation) |
| `exit_tp_200bp` (LONG) / `exit_tp_180bp` (SHORT) | open-for-side or `EXIT_PENDING` | `translated` → `EXIT_PENDING` | no (owned mutation) |
| `clear_exit` | `EXIT_PENDING` | `translated` → open-for-side | no (owned mutation) |
| anything else | — | `invalid_action` | no |

Direction guards are hard-coded: a LONG prev-close entry must use a **negative** basis-point
offset, SHORT must use **positive**
([`rl/action_translation.py:123-136`](../src/ibkr_trader/rl/action_translation.py)); take-profit
basis points are pinned to 200 bp long / 180 bp short
([`rl/action_translation.py:211-224`](../src/ibkr_trader/rl/action_translation.py)).

The runner's own view of which actions can be submitted is a separate allowlist in
[`rl/runner_http.py:47-53`](../src/ibkr_trader/rl/runner_http.py).

### Ownership resolution for mutating actions

`_find_owned_instruction()` locates the durable instruction an action may mutate by
`source_system == "rl-runner"` + `account_key` + `book_key` + `symbol` + non-archived +
active state, then filters on payload metadata `rl_deployment_key` and
`rl_source_instruction_id`
([`orchestration/rl_action_execution.py:448-499`](../src/ibkr_trader/orchestration/rl_action_execution.py)).
It uses `SELECT ... FOR UPDATE` and **refuses to mutate broker state** when more than one
active instruction matches, raising `RLActionOwnershipError` → HTTP 409
([`orchestration/rl_action_execution.py:480-485`](../src/ibkr_trader/orchestration/rl_action_execution.py),
[`api/server_routes_rl.py:669-672`](../src/ibkr_trader/api/server_routes_rl.py)).

### Deterministic instruction identity

Translated entries get a deterministic instruction id derived from
`("rl", deployment_key, source_instruction_id, symbol, action_name, decision_id ?? observed_at)`
via a slugified prefix plus a 12-hex SHA-1 suffix; the batch id is derived from that id
([`rl/action_translation.py:296-308`](../src/ibkr_trader/rl/action_translation.py),
[`rl/action_translation.py:423-428`](../src/ibkr_trader/rl/action_translation.py)). This is
what makes re-submission idempotent at the ledger layer
([`orchestration/submission.py:179-258`](../src/ibkr_trader/orchestration/submission.py)).

---

## 2. Order lifecycle

### Instruction states

`ExecutionState` ([`orchestration/state_machine.py:9-20`](../src/ibkr_trader/orchestration/state_machine.py)):
`RECEIVED`, `MODEL_ROUTED_PENDING`, `REENTRY_WAITING_FOR_FLAT`, `ENTRY_PENDING`,
`ENTRY_SUBMITTED`, `ENTRY_CANCELLED`, `NEEDS_REVIEW`, `POSITION_OPEN`, `EXIT_PENDING`,
`COMPLETED`, `FAILED`.

Initial state at submission time is chosen in
[`orchestration/submission.py:339-345`](../src/ibkr_trader/orchestration/submission.py):
model-routed → `MODEL_ROUTED_PENDING`; deferred re-entry → `REENTRY_WAITING_FOR_FLAT`;
otherwise `ENTRY_PENDING`.

The "active/owned" set used for RL ownership and runtime-state resolution is
`{ENTRY_PENDING, ENTRY_SUBMITTED, POSITION_OPEN, EXIT_PENDING}`
([`orchestration/rl_action_execution.py:49-54`](../src/ibkr_trader/orchestration/rl_action_execution.py),
[`api/rl_runtime_state.py:26-31`](../src/ibkr_trader/api/rl_runtime_state.py)).

### Transitions and who performs them

| Transition | Trigger | Evidence |
| --- | --- | --- |
| `ENTRY_PENDING → ENTRY_SUBMITTED` | `submit_persisted_instruction_entry()`, after `assert_kill_switch_inactive()` | [`orchestration/entry_submission.py:114-232`](../src/ibkr_trader/orchestration/entry_submission.py) |
| `ENTRY_SUBMITTED → ENTRY_CANCELLED` | operator/RL cancel, or terminal unfilled broker status | [`orchestration/entry_submission.py:257-356`](../src/ibkr_trader/orchestration/entry_submission.py), [`ledger/instruction_projection.py:111-131`](../src/ibkr_trader/ledger/instruction_projection.py) |
| `ENTRY_PENDING → ENTRY_CANCELLED` (pre-submit) | RL `cancel_entry` before broker submission | [`orchestration/rl_action_execution.py:209-243`](../src/ibkr_trader/orchestration/rl_action_execution.py) |
| `ENTRY_SUBMITTED → POSITION_OPEN` | terminal broker status carrying `filled > 0` | [`ledger/instruction_projection.py:66-109`](../src/ibkr_trader/ledger/instruction_projection.py) |
| `ENTRY_SUBMITTED/ENTRY_CANCELLED → NEEDS_REVIEW` | broker error code 202 whose text matches `risk mitigation` / `trdv` | [`ledger/instruction_projection.py:134-211`](../src/ibkr_trader/ledger/instruction_projection.py) |
| `POSITION_OPEN → EXIT_PENDING` | protective exit, forced exit, or RL exit action | [`orchestration/runtime_protective_exits.py`](../src/ibkr_trader/orchestration/runtime_protective_exits.py), [`orchestration/rl_action_execution.py:307-445`](../src/ibkr_trader/orchestration/rl_action_execution.py) |
| `EXIT_PENDING → POSITION_OPEN` | RL `clear_exit` cancels the pending exit and keeps the position | [`orchestration/rl_action_execution.py:246-304`](../src/ibkr_trader/orchestration/rl_action_execution.py) |
| `EXIT_PENDING → COMPLETED` | exit fill reconciliation in the runtime cycle | [`orchestration/runtime_cycle.py:91`](../src/ibkr_trader/orchestration/runtime_cycle.py), [`orchestration/runtime_exit_cleanup.py`](../src/ibkr_trader/orchestration/runtime_exit_cleanup.py) |
| `* → cancelled by kill switch` | global kill switch active during a runtime cycle | [`orchestration/runtime_cycle.py:509-580`](../src/ibkr_trader/orchestration/runtime_cycle.py) |

Every transition writes an append-only `instruction_event` row with `event_type`,
`source`, `state_before`, `state_after`, and a JSON payload
([`db/models.py:269-295`](../src/ibkr_trader/db/models.py)).

### Broker-side order statuses

Terminal/closed broker statuses are declared as a set in **four** places, all with the
same seven members `{API_CANCELLED, CANCELLED, ERROR, FILLED, INACTIVE,
NOT_FOUND_AT_BROKER, REJECTED}`:
[`ledger/instruction_projection.py:17-25`](../src/ibkr_trader/ledger/instruction_projection.py),
[`orchestration/rl_action_execution.py:40-48`](../src/ibkr_trader/orchestration/rl_action_execution.py),
[`orchestration/runtime_broker_matching.py:16-24`](../src/ibkr_trader/orchestration/runtime_broker_matching.py),
[`orchestration/runtime_audit.py:60-68`](../src/ibkr_trader/orchestration/runtime_audit.py).
Note `NOT_FOUND_AT_BROKER` is a repository-local status, not an IBKR status string.

### Wire-level submission behaviour

`submit_order_from_instruction()` performs, in order
([`ibkr/order_execution_submission.py:29-314`](../src/ibkr_trader/ibkr/order_execution_submission.py)):
account summary read → account selection → contract resolution (must resolve to exactly
one contract) → sizing preview → whole-share normalization → short-sale validation →
tick-increment price normalization → optional WhatIf cash-cushion preflight (up to 5
resizes) → `place_order_sync()` inside a **3-attempt** loop that shrinks quantity on
IBKR error 201 (insufficient funds).

Notable wire rules in [`ibkr/order_execution_common.py`](../src/ibkr_trader/ibkr/order_execution_common.py):
- `order.orderRef` is set to the **instruction id** for entries (`:592-628`, called at
  `order_execution_submission.py:159-168`), and to
  `"<instruction_id>:exit:rl_market"` / `"<instruction_id>:exit:rl_take_profit"` for RL
  exits ([`orchestration/rl_action_execution.py:727-730`](../src/ibkr_trader/orchestration/rl_action_execution.py)).
- `outsideRth = False`, `transmit = True` always (`:614-615`).
- Error code 399 is the only non-fatal order warning (`:37`); a 399 whose text defers
  exchange placement past the entry trade date triggers an automatic cancel and then a
  hard failure (`order_execution_submission.py:213-235`).
- Cancel-specific codes: 10147 not found, 10148 already cancelled (`:35-36`).

---

## 3. Identifiers and correlation keys

| Identifier | Type / owner | Where defined | Notes |
| --- | --- | --- | --- |
| `instruction_id` | string, caller-supplied or derived | `instruction.instruction_id`, unique constraint [`db/models.py:216`](../src/ibkr_trader/db/models.py) | Also used as IBKR `orderRef` for entries. |
| `batch_id` | string, source system | [`db/models.py:230`](../src/ibkr_trader/db/models.py) | Indexed; groups a submitted batch. |
| `account_key` | string | [`db/models.py:231`](../src/ibkr_trader/db/models.py) | Normalized upper-case in RL comparisons ([`api/server_routes_rl.py:530-534`](../src/ibkr_trader/api/server_routes_rl.py)). |
| `book_key` | string | [`db/models.py:232`](../src/ibkr_trader/db/models.py) | Normalized lower-case in RL comparisons. |
| `model_key` | string, RL registry | unique on `trader_model` [`db/models.py:73`](../src/ibkr_trader/db/models.py) | Must equal the candidate's `execution.model_id`. |
| `deployment_key` | string, RL registry | unique on `trader_deployment` [`db/models.py:104`](../src/ibkr_trader/db/models.py) | The model↔account↔book binding. |
| `decision_id` | string, observation builder | consumed at [`rl/runner_loop.py:575`](../src/ibkr_trader/rl/runner_loop.py) | Feeds the deterministic instruction id and the runner dedupe key `deployment:instruction:decision`. |
| `external_order_id` | broker `orderId` | [`db/models.py:473`](../src/ibkr_trader/db/models.py) | Unique per `(broker_kind, account_key, external_order_id)` [`:447-452`](../src/ibkr_trader/db/models.py). Reuse is handled by lineage retirement (§8). |
| `external_perm_id` | broker `permId` | [`db/models.py:474`](../src/ibkr_trader/db/models.py) | Indexed; stable across reconnects. |
| `external_client_id` | IBKR client id | [`db/models.py:475`](../src/ibkr_trader/db/models.py) | See role-based policy below. |
| `order_ref` | IBKR `orderRef` | [`db/models.py:476`](../src/ibkr_trader/db/models.py) | The join key back to the instruction. |
| `order_role` | `ENTRY` / `EXIT` / `BROKER_NATIVE` | [`db/models.py:472`](../src/ibkr_trader/db/models.py), inferred at [`ledger/persistence_order_records.py:406`](../src/ibkr_trader/ledger/persistence_order_records.py) | Drives which instruction summary field a status projects onto. |
| `external_execution_id` | broker execution id | unique per `(broker_kind, account_key, external_execution_id)` [`db/models.py:543-549`](../src/ibkr_trader/db/models.py) | Fill idempotency key. |
| `request_key` | backfill dedupe key | unique [`db/models.py:736`](../src/ibkr_trader/db/models.py) | Coalesces day-level backfill requests. |
| `runtime_key` / `owner_token` | runtime lease | unique [`db/models.py:837`](../src/ibkr_trader/db/models.py), `:847` | Single-writer execution runtime lease. |
| `control_key` | operator control | unique [`db/models.py:304`](../src/ibkr_trader/db/models.py); `GLOBAL_KILL_SWITCH` at [`orchestration/operator_controls.py:29`](../src/ibkr_trader/orchestration/operator_controls.py) | |
| `owner_deployment_key` / `owner_instruction_id` | position attribution | [`db/models.py:655-658`](../src/ibkr_trader/db/models.py) | How a position snapshot is attributed to an RL deployment. |

**Role-based IBKR client IDs** are fixed, not rotated
([`ibkr/client_ids.py:10-13`](../src/ibkr_trader/ibkr/client_ids.py)): `0` primary
runtime/order control, `7` diagnostics, `8` historical/backfill, `9` streaming. Overridable
by `IBKR_CLIENT_ID`, `IBKR_DIAGNOSTIC_CLIENT_ID`, `IBKR_HISTORICAL_CLIENT_ID`,
`IBKR_STREAMING_CLIENT_ID` ([`config.py:104-113`](../src/ibkr_trader/config.py)), and the
session variants are constructed by `primary_session()` / `diagnostic_session()` /
`historical_session()` / `streaming_session()` ([`config.py:81-91`](../src/ibkr_trader/config.py)).

**RL trace metadata** written into every translated instruction payload, and the key that
makes ownership resolution possible
([`rl/action_translation.py:314-326`](../src/ibkr_trader/rl/action_translation.py)):
`rl_action_name`, `rl_deployment_key`, `rl_source_instruction_id`, `rl_source_batch_id`,
`rl_decision_id`, plus optional `previous_close` and `entry_limit_price`.

---

## 4. Interfaces

### HTTP interfaces in the execution/feedback domain

Registered on the FastAPI app; route modules are listed in
[`docs/codebase-map.md`](codebase-map.md).

Order and instruction control — [`api/server_routes_broker_market.py`](../src/ibkr_trader/api/server_routes_broker_market.py):
`POST /v1/orders/preview` (`:786`), `POST /v1/orders/submit` (`:810`),
`POST /v1/orders/{order_id}/cancel` (`:861`), `POST /v1/instructions/validate` (`:927`),
`GET /v1/instructions` (`:940`), `GET /v1/instructions/{instruction_id}` (`:968`),
`GET /v1/broker/runtime-snapshot` (`:513`), `GET /v1/ibkr/telemetry` (`:408`),
`POST /v1/ibkr/probe` (`:421`), `GET /healthz` (`:374`).

Operator/runtime control — [`api/server_routes_operator.py`](../src/ibkr_trader/api/server_routes_operator.py):
`POST /v1/instructions/submit` (`:569`), `/intent-cleanup` (`:628`), `/cancel-set` (`:658`),
`/archive-set` (`:706`), `/{id}/submit-entry` (`:719`), `/{id}/cancel-entry` (`:762`),
`/schedule-preview` (`:803`), `GET|POST /v1/controls/kill-switch` (`:471`, `:480`),
`POST /v1/broker-attention/{event_id}/review` (`:498`),
`POST /v1/reconciliation-issues/{issue_id}/review` (`:522`) and `/archive-open` (`:546`),
`POST /v1/runtime/run-once` (`:822`), `POST /v1/runtime/startup-reconcile` (`:864`),
`GET /v1/read/operator-snapshot` (`:263`), `GET /v1/read/ledger-snapshot` (`:409`).

RL model/feedback — [`api/server_routes_rl.py`](../src/ibkr_trader/api/server_routes_rl.py):
`POST /v1/rl/models/register` (`:256`), `/upsert` (`:271`), `PUT /v1/rl/models/{model_key}` (`:284`),
`POST /v1/rl/deployments` (`:300`), `PATCH /v1/rl/deployments/{deployment_key}` (`:317`),
`POST /v1/rl/actions/log` (`:339`), `GET /v1/rl/candidates` (`:354`),
`POST /v1/rl/candidates/archive-expired` (`:450`), **`POST /v1/rl/actions/translate` (`:484`)**,
`POST /v1/rl/deployments/{deployment_key}/heartbeat` (`:712`),
`POST /v1/rl/observations/build` (`:734`), `GET /v1/read/rl-dashboard` (`:1078`),
**`GET /v1/rl/runtime-state` (`:1190`)**.

The API binds loopback-only by default (`API_REQUIRE_LOOPBACK_ONLY=true`,
[`config.py:131-134`](../src/ibkr_trader/config.py)); the dashboard reaches it through
`IBKR_TRADER_API_BASE_URL=http://127.0.0.1:8000`
([`ops/systemd/ibkr-trader-dashboard.service`](../ops/systemd/ibkr-trader-dashboard.service)).

### Error-code contract of `/v1/rl/actions/translate`

A dense and graph-worthy mapping
([`api/server_routes_rl.py:658-684`](../src/ibkr_trader/api/server_routes_rl.py)):

| Exception | HTTP |
| --- | --- |
| `InstructionStatusNotFoundError`, `TraderDeploymentNotFoundError` | 404 |
| `IntentReplacementConflictError`, `RLActionOwnershipError`, `RLActionStateError`, `SubmissionConflictError` | 409 |
| `IntentCleanupSelectorError`, `LookupError`, `KeyError`, `ValueError` | 400 |
| `IbkrDependencyError` | 503 |
| `ConnectionError` | 502 |
| `TimeoutError` | 504 |

The runner treats 409 as non-retryable and marks the decision processed
([`rl/runner_loop.py:632-653`](../src/ibkr_trader/rl/runner_loop.py) with
[`rl/runner_runtime_state.py`](../src/ibkr_trader/rl/runner_runtime_state.py) `_api_error_is_conflict`).

### Internal seams (broker-neutral boundaries)

- **Broker submission seam:** `OrderExecutionSyncWrapperProtocol`
  ([`ibkr/order_execution_common.py:53-101`](../src/ibkr_trader/ibkr/order_execution_common.py))
  — the injectable IBKR client surface (`place_order_sync`, `cancel_order_sync`,
  `preview_order_sync`, `get_contract_details`, `get_market_rule`, …).
- **Injectable callables** on the runtime cycle: `entry_submitter`, `entry_canceler`,
  `exit_submitter`, `market_price_reader`, `broker_snapshot_fetcher`,
  `broker_callback_fetcher`, `broker_order_canceler`, `virtual_market_sync`
  ([`orchestration/runtime_cycle.py:102-110`](../src/ibkr_trader/orchestration/runtime_cycle.py)).
  This is how tests substitute the broker without fabricating live state.
- **Virtual/real dispatch** is decided by account key, not by a separate process:
  `is_virtual_account_key()` selects virtual submit/cancel implementations
  ([`orchestration/rl_action_execution.py:662-711`](../src/ibkr_trader/orchestration/rl_action_execution.py),
  [`orchestration/entry_submission.py:146-164`](../src/ibkr_trader/orchestration/entry_submission.py)).
- **Ledger write API:** [`ledger/persistence.py`](../src/ibkr_trader/ledger/persistence.py)
  exports exactly `persist_broker_callback_events`, `persist_broker_order_cancellation`,
  `persist_broker_order_cancellation_result`, `persist_broker_order_submission`,
  `persist_broker_runtime_snapshot`, `BROKER_KIND_IBKR`.

---

## 5. Feedback paths

There are **four** distinct broker→system feedback paths, deliberately ranked so the
trading loop never blocks on `reqExecutions`.

| Rank | Path | Mechanism | Evidence |
| --- | --- | --- | --- |
| 1 | Synchronous submit/cancel response | `broker_order_status` + `tws_submission` + `ibkr_wire_audit` returned from the submit call and persisted immediately | [`ibkr/order_execution_submission.py:268-314`](../src/ibkr_trader/ibkr/order_execution_submission.py), [`ledger/persistence_order_records.py:35-109`](../src/ibkr_trader/ledger/persistence_order_records.py) |
| 2 | Broker callback drain | `persist_broker_callback_events()` writes `openOrder`, `orderStatus`, terminal-fill, and `error` callbacks into `broker_order_event` | [`ledger/persistence_callbacks.py:36`, `:185`, `:348`, `:463`, `:627`](../src/ibkr_trader/ledger/persistence_callbacks.py) |
| 3 | Runtime snapshot | `persist_broker_runtime_snapshot()` upserts open orders, positions, account values | [`ledger/persistence_runtime.py`](../src/ibkr_trader/ledger/persistence_runtime.py), [`ledger/persistence_snapshots.py:41`, `:231`, `:385`](../src/ibkr_trader/ledger/persistence_snapshots.py) |
| 4 | Execution history (slow repair) | `include_executions` is hard-coded `False` in the active loop; execution snapshots are an explicit background repair path | [`orchestration/runtime_cycle.py:301-304`](../src/ibkr_trader/orchestration/runtime_cycle.py) |

### Projection back onto intent

`sync_instruction_from_broker_order_terminal_status()` is the single funnel from broker
order truth to instruction state
([`ledger/instruction_projection.py:33-131`](../src/ibkr_trader/ledger/instruction_projection.py)):

- `order_role == "ENTRY"` → sets `instruction.broker_order_status`, then (only from
  `ENTRY_SUBMITTED`) projects `POSITION_OPEN` when filled > 0 or `ENTRY_CANCELLED` otherwise.
- `order_role == "EXIT"` → sets `instruction.exit_order_status` and **returns without
  changing state**. Exit-driven completion is the runtime cycle's job, not the projection's.
- Fill price preference is `avgFillPrice`, falling back to `lastFillPrice`
  ([`:73-75`](../src/ibkr_trader/ledger/instruction_projection.py)).
- Emitted `instruction_event` carries `evidence_source: "broker_order_status"`
  ([`:100`](../src/ibkr_trader/ledger/instruction_projection.py)).

Fill aggregation has three sources, mirroring the ranking above:
`aggregate_executions()`, `aggregate_broker_order_status_fill()`,
`aggregate_persisted_execution_fill()`
([`orchestration/runtime_fills.py:21`, `:85`, `:171`](../src/ibkr_trader/orchestration/runtime_fills.py)).

### Feedback into the model loop

`build_rl_runtime_state_snapshot()` is the authoritative reconstruction of runner state
from ledger truth ([`api/rl_runtime_state.py:34`](../src/ibkr_trader/api/rl_runtime_state.py)).
It joins active RL instructions with the latest owner-attributed position snapshot and
returns, per symbol, either `status: "ready"` with a `runner_state`
(`in_position`, `pending_entry_anchor`, `pending_entry_rel_bp`, `pending_exit_tp_bp`,
`entry_price`, `entry_bar_idx`, `bars_since_entry_order`, `bars_since_exit_order`) or
`status: "blocked"` with `state_before: "INCONSISTENT"` and typed blockers:

`duplicate_active_entries`, `duplicate_active_positions`, `position_side_mismatch`,
`virtual_position_missing_owner`, `virtual_position_owner_mismatch`,
`virtual_position_owner_instruction_mismatch`, `unowned_current_holding`,
`owned_position_missing_current_holding`
([`api/rl_runtime_state.py:199-320`](../src/ibkr_trader/api/rl_runtime_state.py)).

Blocked symbols cause the runner to skip the candidate with
`status: "runtime_state_blocked"` rather than guess
([`rl/runner_loop.py:282-292`](../src/ibkr_trader/rl/runner_loop.py)).

Bar indices are derived arithmetically from the observation contract's session open and
target bar minutes, not from stored counters
([`api/rl_runtime_state.py:517-550`](../src/ibkr_trader/api/rl_runtime_state.py)).

### What is *not* a feedback path

There is **no reward computation, no online learning, and no training loop in this
repository**: `grep -rn 'reward' --include='*.py' src scripts` returns nothing. Execution
feedback reaches the model only as *state* (`runner_state`, `previous_close`,
observations), never as a learning signal. Offline post-hoc analysis is
[`scripts/replay_rl_day.py`](../scripts/replay_rl_day.py), which replays one
deployment/symbol/day through the promoted model against stored `MarketStreamBarRecord`
bars and a `ReplayFill` reconstruction.

---

## 6. Observability

| Surface | What it records | Evidence |
| --- | --- | --- |
| `trader_action` | Append-only per-decision log: `action_name`, `action_status` (`logged` / `translated` / `executed` / `invalid_action`), `state_before`/`state_after`, `instruction_id`, and a payload containing `decision_id`, `model_diagnostics`, `submitted`, `translation_note`, `action_execution` | [`db/models.py:146-178`](../src/ibkr_trader/db/models.py), written at [`api/server_routes_rl.py:618-657`](../src/ibkr_trader/api/server_routes_rl.py) |
| `trader_heartbeat` | One row per deployment: `status`, `last_seen_at`, `last_bar_at`, `last_action_at`, `runtime_error`, `metrics_json` | [`db/models.py:181-208`](../src/ibkr_trader/db/models.py), written from [`rl/runner_loop.py:721-778`](../src/ibkr_trader/rl/runner_loop.py) |
| `reconciliation_run` / `reconciliation_issue` | One row per runtime or startup reconciliation pass, status `CLEAN` / `WARNINGS`, with issue rows keyed by `stage` | [`db/models.py:769-829`](../src/ibkr_trader/db/models.py), [`orchestration/runtime_audit.py:190-286`](../src/ibkr_trader/orchestration/runtime_audit.py) |
| `runtime_service` / `runtime_service_event` | Lease-holder identity (`owner_token`, `hostname`, `pid`, `broker_client_id`), heartbeat and cycle timestamps, `stop_requested`, `last_error` | [`db/models.py:832-899`](../src/ibkr_trader/db/models.py), [`orchestration/runtime_service_state.py`](../src/ibkr_trader/orchestration/runtime_service_state.py) |
| Broker monitor | Separate heartbeat and snapshot-refresh status with `is_stale` computed from configured intervals; skips snapshot refresh when the heartbeat probe fails | [`api/broker_monitor.py:110-354`](../src/ibkr_trader/api/broker_monitor.py); staleness thresholds `:200-204`, `:216-220`; skip at `:252-257` |
| IBKR wire audit | Per-order request/response events captured around every submit and attached to raised exceptions as `exc.ibkr_wire_audit` | [`ibkr/order_execution_common.py:382-438`](../src/ibkr_trader/ibkr/order_execution_common.py) |
| Broker pacing | Governor with configured rate limits and a `BrokerPacingSnapshot` counting permits and rejections | [`ibkr/pacing.py:13-38`](../src/ibkr_trader/ibkr/pacing.py) |
| Operator attention items | `broker_order_event` rows classified into attention items with expected-cancel suppression heuristics | [`orchestration/operator_reviews.py:145`, `:227`](../src/ibkr_trader/orchestration/operator_reviews.py), [`read_models/operator_dashboard_common.py:271-503`](../src/ibkr_trader/read_models/operator_dashboard_common.py), [`read_models/operator_dashboard_builders.py:396-494`](../src/ibkr_trader/read_models/operator_dashboard_builders.py) |
| `operator_review_action` | Append-only operator acknowledgements of attention items and reconciliation issues | [`db/models.py:356-379`](../src/ibkr_trader/db/models.py) |
| Push alerts | `send_operator_alert()` fans out to webhook, ntfy, or Pushover; returns `False` when nothing is configured (no silent success claim) | [`orchestration/operator_alerts.py:15-40`](../src/ibkr_trader/orchestration/operator_alerts.py) |
| Read models | `build_operator_dashboard_snapshot()`, `build_ledger_dashboard_snapshot()`, `build_rl_trader_dashboard_snapshot()` | [`read_models/__init__.py`](../src/ibkr_trader/read_models/__init__.py) |

Runner heartbeat status degradation rules
([`rl/runner_loop.py:705-720`](../src/ibkr_trader/rl/runner_loop.py)), in precedence order:
all-stale decision bars → action-distribution warning → any `translate_error` →
`cadence_over_budget` (processing exceeded the 5-minute decision cadence).

---

## 7. Deployment

Three checked-in application units, all user-scoped (`%h`), all `Restart=always`:

| Unit | Command | Ordering |
| --- | --- | --- |
| [`ibkr-trader-api.service`](../ops/systemd/ibkr-trader-api.service) | `ExecStartPre` runs `python -m ibkr_trader.db.init_schema`, then `python -m ibkr_trader.api.server`; `PYTHONPATH=src` | after `network-online.target` |
| [`ibkr-trader-rl-runner.service`](../ops/systemd/ibkr-trader-rl-runner.service) | `scripts/run_rl_agents.py --api-base http://127.0.0.1:8000 --execute-virtual --state-file var/rl-runner/state.json --history-cache-file var/rl-runner/history-cache.json --allow-metadata-history-fallback --metadata-history-only`; `ExecStartPre` polls `/healthz` for up to 60 s | after `ibkr-trader-api.service` |
| [`ibkr-trader-dashboard.service`](../ops/systemd/ibkr-trader-dashboard.service) | `node dashboard/build/index.js`, `HOST=0.0.0.0`, `PORT=4173`, `ORIGIN=http://quant.geisler.se:4173` | after `ibkr-trader-api.service` |

The checked-in RL runner unit runs **`--execute-virtual` only** — it does not pass
`--execute-broker`, so broker-mode execution is not enabled by the tracked unit
(`execute_actions` gate at [`rl/runner_loop.py:206-211`](../src/ibkr_trader/rl/runner_loop.py)).

Gateway-side units and watchdogs (not application code):
[`ibgateway-ibc.service`](../ops/systemd/ibgateway-ibc.service),
[`ibgateway-ibc-system.service`](../ops/systemd/ibgateway-ibc-system.service),
[`ibgateway-api-watchdog.service`](../ops/systemd/ibgateway-api-watchdog.service) +
[`.timer`](../ops/systemd/ibgateway-api-watchdog.timer) (`OnBootSec=5min`,
`OnUnitActiveSec=2min`),
[`ibkr-disk-watchdog.service`](../ops/systemd/ibkr-disk-watchdog.service) + `.timer`.
The API watchdog defaults to **restart disabled** (`WATCHDOG_RESTART_ENABLED=no`) with a
`FAILURE_THRESHOLD=6`, 900 s restart cooldown, 300 s startup grace, and optional
time-window gating ([`ops/scripts/ibgateway_api_watchdog.sh:4-31`](../ops/scripts/ibgateway_api_watchdog.sh)).

**Execution runtime is off by default in code**: `execution_runtime_enabled` defaults to
`False` / `EXECUTION_RUNTIME_ENABLED=false`
([`config.py:178`](../src/ibkr_trader/config.py), `:313-317`), so whether the embedded
execution loop is running is a host-configuration fact this repository cannot establish.

Other execution-relevant defaults ([`config.py:152-186`](../src/ibkr_trader/config.py)):
broker heartbeat 30 s, snapshot refresh 60 s, execution-runtime interval 5 s / lease 30 s /
submission lead 60 s, connect backoff 5→300 s, IBKR pacing 45 req/s, market-data line
limit 80, historical 50 per 10 min, RL observed-bar minimum coverage ratio 0.8.

---

## 8. Failure modes and guards

Ordered roughly from "refuses to act" to "degrades and reports".

1. **Ambiguous ownership → refuse.** Multiple active RL instructions matching one source
   candidate raises `RLActionOwnershipError` before any broker call
   ([`orchestration/rl_action_execution.py:480-485`](../src/ibkr_trader/orchestration/rl_action_execution.py)).
2. **Wrong state → refuse.** `RLActionStateError` for state mismatch, zero remaining
   quantity, missing entry average price, or `EXIT_PENDING` with no cancellable exit order
   ([`:494-498`, `:344-348`, `:628-632`, `:548-553`](../src/ibkr_trader/orchestration/rl_action_execution.py)).
3. **Inconsistent runtime state → block the symbol.** Eight typed blockers, surfaced as
   `state_before: "INCONSISTENT"` (§5) rather than a guessed state.
4. **Kill switch.** Blocks new entry submission
   ([`orchestration/entry_submission.py:122`](../src/ibkr_trader/orchestration/entry_submission.py),
   [`orchestration/runtime_entries.py:293-297`](../src/ibkr_trader/orchestration/runtime_entries.py))
   and cancels submitted entries during a cycle
   ([`orchestration/runtime_cycle.py:509-580`](../src/ibkr_trader/orchestration/runtime_cycle.py)).
   It is **not** applied to exits, cancels, or `execute_owned_rl_action()`, and
   `submit_execution_batch()` skips the assertion when *every* instruction in the batch is
   model-routed ([`orchestration/submission.py:318-319`](../src/ibkr_trader/orchestration/submission.py))
   — a translated RL entry is not model-routed, so it *is* gated.
5. **Global broker circuit breaker.** `BrokerHealthCircuit.raise_if_open()` raises
   `BrokerCircuitOpen(ConnectionError)` with a retry-at timestamp; default open window
   900 s ([`ibkr/broker_circuit.py:36-104`](../src/ibkr_trader/ibkr/broker_circuit.py)).
6. **Pacing limits.** `BrokerPacingLimitExceeded` for request-rate, market-data-line, and
   historical-window breaches ([`ibkr/pacing.py:13-24`](../src/ibkr_trader/ibkr/pacing.py)).
7. **Insufficient funds.** IBKR error 201 with a parseable `Loan Value … Initial Margin`
   message triggers a proportional quantity reduction and retry, at most 3 attempts, then
   a hard failure ([`ibkr/order_execution_common.py:500-589`](../src/ibkr_trader/ibkr/order_execution_common.py),
   [`ibkr/order_execution_submission.py:158-250`](../src/ibkr_trader/ibkr/order_execution_submission.py)).
8. **Cash cushion.** WhatIf preflight shrinks cash-backed long entries until post-trade
   headroom clears the reserve/commission buffer; up to 5 iterations, then raises. A blank
   202 cancel notice or a WhatIf timeout degrades to cash-reserve sizing with a warning
   instead of failing ([`ibkr/order_execution_common.py:887-984`](../src/ibkr_trader/ibkr/order_execution_common.py)).
9. **Deferred exchange activation.** A 399 warning whose date differs from the entry
   submit date causes an automatic cancel plus a hard `LookupError`; if the cancel itself
   fails, the error explicitly says the order may still be live
   ([`ibkr/order_execution_submission.py:210-235`](../src/ibkr_trader/ibkr/order_execution_submission.py)).
10. **Broker risk-mitigation stop → `NEEDS_REVIEW`.** Error 202 whose text contains
    `risk mitigation` or `trdv`, on an unfilled entry, moves the instruction to
    `NEEDS_REVIEW` and fires an operator alert; resubmission requires human review
    ([`ledger/instruction_projection.py:134-234`](../src/ibkr_trader/ledger/instruction_projection.py)).
11. **Broker snapshot unavailable.** The cycle records a `broker_snapshot` issue, appends a
    Gateway diagnostic hint when the message matches known outage markers, substitutes an
    **empty** snapshot, and disables `close_missing_open_orders` so an empty snapshot is not
    mistaken for "no open orders"
    ([`orchestration/runtime_cycle.py:319-379`](../src/ibkr_trader/orchestration/runtime_cycle.py),
    [`orchestration/runtime_audit.py:71-81`](../src/ibkr_trader/orchestration/runtime_audit.py)).
12. **Repeated-outage audit suppression.** Cycles whose only issues are broker-outage
    issues with zero actions fold into the previous matching reconciliation run within a
    10-minute window, incrementing `suppressed_reconciliation_repeats` instead of creating
    new rows ([`orchestration/runtime_audit.py:31-53`, `:439-526`](../src/ibkr_trader/orchestration/runtime_audit.py)).
13. **Auto-resolving stale warnings.** Forced-exit-cleanup warnings are archived once the
    ledger proves the instruction completed with a nonzero exit fill, or the named broker
    order reached a resolved status
    ([`orchestration/runtime_audit.py:289-360`](../src/ibkr_trader/orchestration/runtime_audit.py)).
14. **Forced exit position guard.** For non-virtual accounts, a forced exit is skipped
    with a typed reason (`missing_broker_position`,
    `insufficient_long_broker_position`, `insufficient_short_broker_position`,
    `unsupported_entry_side`) when the broker snapshot does not confirm enough position
    ([`orchestration/runtime_broker_matching.py:218-290`](../src/ibkr_trader/orchestration/runtime_broker_matching.py)).
15. **Broker order id reuse.** When lineage (perm id, order ref, symbol, local symbol)
    changes under a reused `orderId`, the old row is retired and a fresh
    `broker_order` row is created rather than overwritten
    ([`ledger/persistence_order_records.py:372-389`](../src/ibkr_trader/ledger/persistence_order_records.py)).
16. **Cancellation with no matching order row.** Three lookup strategies are tried, then a
    `broker_order` row is reconstructed from the instruction and tagged
    `reconstructed_from_instruction: True`; if there is no instruction either, it raises
    ([`ledger/persistence_order_records.py:142-256`](../src/ibkr_trader/ledger/persistence_order_records.py)).
17. **Model/route retirement.** `RETIRED_MODEL_ROUTES` hard-blocks
    `long_trial_106_v1` on `VIRTUALSEEDRL01/seedpicker_rl_long_01` with an explanatory
    reason ([`domain/model_routing_policy.py:21-53`](../src/ibkr_trader/domain/model_routing_policy.py)).
18. **Contract-incompatible model → degraded, no inference.** A runtime contract requiring
    market-context universe features blocks live top-1 candidate inference
    ([`rl/runner_loop.py:403-461`](../src/ibkr_trader/rl/runner_loop.py)).
19. **Stale or missing bars → skip, not guess.** Candidates without stream bars, without
    `trace.data_cutoff_date`, or without `trace.trade_date` are skipped with typed reasons;
    decision-bar freshness is classified before any action is taken
    ([`rl/runner_loop.py:271-341`](../src/ibkr_trader/rl/runner_loop.py), `:542-574`).
20. **Duplicate-decision suppression.** Runner dedupe key
    `deployment_key:instruction_id:decision_id`, persisted to a state file
    ([`rl/runner_loop.py:576-589`](../src/ibkr_trader/rl/runner_loop.py),
    [`rl/runner_http.py:56`](../src/ibkr_trader/rl/runner_http.py)).
21. **Duplicate-entry suppression at the contract layer.** An entry action arriving while
    an entry is already pending is logged, not resubmitted
    ([`rl/action_translation.py:167-180`](../src/ibkr_trader/rl/action_translation.py)).
22. **Single-writer runtime.** A durable lease with `owner_token` and `lease_expires_at`
    prevents two execution runtimes from acting
    ([`db/models.py:832-862`](../src/ibkr_trader/db/models.py),
    [`orchestration/runtime_service_state.py`](../src/ibkr_trader/orchestration/runtime_service_state.py)).

---

## 9. Proposed graph facts

Suggested schema for ingestion. Node ids use the identifier columns from §3.

### Node types

| Label | Natural key | Source of truth |
| --- | --- | --- |
| `TraderModel` | `model_key` | `trader_model` |
| `TraderDeployment` | `deployment_key` | `trader_deployment` |
| `BrokerAccount` | `(broker_kind, account_key)` | `broker_account` |
| `Book` | `book_key` | `trader_deployment` / `instruction` |
| `Instrument` | `(symbol, exchange, currency, security_type)` | `instrument` |
| `Instruction` | `instruction_id` | `instruction` |
| `InstructionEvent` | `instruction_event.id` | `instruction_event` |
| `TraderAction` | `trader_action.id` | `trader_action` |
| `BrokerOrder` | `(broker_kind, account_key, external_order_id)` | `broker_order` |
| `BrokerOrderEvent` | `broker_order_event.id` | `broker_order_event` |
| `ExecutionFill` | `(broker_kind, account_key, external_execution_id)` | `execution_fill` |
| `PositionSnapshot` | `position_snapshot.id` | `position_snapshot` |
| `AccountSnapshot` | `account_snapshot.id` | `account_snapshot` |
| `ReconciliationRun` / `ReconciliationIssue` | row id | `reconciliation_run(_issue)` |
| `RuntimeService` | `runtime_key` | `runtime_service` |
| `OperatorControl` | `control_key` | `operator_control` |
| `Endpoint` | HTTP method + path | route modules (§4) |
| `Service` | systemd unit name | `ops/systemd/*.service` |
| `ExecutionState` | state name | `ExecutionState` enum |
| `ActionName` | action string | `translate_rl_action` branches |
| `FailureMode` | stable slug | §8 |

### Edge types

```
(TraderModel)         -[:HAS_DEPLOYMENT]->        (TraderDeployment)
(TraderDeployment)    -[:TRADES_IN_ACCOUNT]->    (BrokerAccount)
(TraderDeployment)    -[:TRADES_BOOK]->          (Book)
(TraderDeployment)    -[:ALLOWS_SYMBOL]->        (Instrument)          // allowed_symbols_json
(TraderDeployment)    -[:EMITTED_ACTION]->       (TraderAction)
(TraderAction)        -[:CHOSE]->                (ActionName)
(TraderAction)        -[:TRANSLATED_TO]->        (Instruction)         // trader_action.instruction_id
(TraderAction)        -[:DERIVED_FROM]->         (Instruction)         // payload.source_instruction_id
(Instruction)         -[:FOR_INSTRUMENT]->       (Instrument)
(Instruction)         -[:IN_STATE]->             (ExecutionState)
(Instruction)         -[:HAS_EVENT]->            (InstructionEvent)
(InstructionEvent)    -[:TRANSITIONED]->         (ExecutionState)      // state_before/state_after
(Instruction)         -[:PRODUCED_ORDER {role}]->(BrokerOrder)
(BrokerOrder)         -[:HAS_EVENT]->            (BrokerOrderEvent)
(BrokerOrder)         -[:FILLED_BY]->            (ExecutionFill)
(BrokerOrder)         -[:RETIRED_BY]->           (BrokerOrder)         // orderId reuse lineage
(ExecutionFill)       -[:SETTLES]->              (Instruction)
(PositionSnapshot)    -[:OWNED_BY_DEPLOYMENT]->  (TraderDeployment)    // owner_deployment_key
(PositionSnapshot)    -[:OWNED_BY_INSTRUCTION]-> (Instruction)         // owner_instruction_id
(ReconciliationRun)   -[:RAISED]->               (ReconciliationIssue)
(ReconciliationIssue) -[:CONCERNS]->             (Instruction)
(Endpoint)            -[:READS|WRITES]->         (<record node>)
(Endpoint)            -[:MAY_RAISE {http}]->     (FailureMode)
(Service)             -[:HOSTS]->                (Endpoint)
(Service)             -[:DEPENDS_ON]->           (Service)
(Service)             -[:USES_CLIENT_ID {role}]->(BrokerAccount)
(FailureMode)         -[:GUARDS]->               (Instruction|BrokerOrder)
```

### Candidate seed facts (all statically verifiable at this commit)

| Subject | Predicate | Object | Evidence |
| --- | --- | --- | --- |
| `POST /v1/rl/actions/translate` | `is_the_only_endpoint_that` | converts a model action into an order intent | `api/server_routes_rl.py:484` |
| `POST /v1/rl/actions/translate` | `may_raise` | `409 RLActionOwnershipError` | `api/server_routes_rl.py:669` |
| `translate_rl_action` | `emits_instruction_payload_for` | `market_entry`, `entry_prevclose_*bp` | `rl/action_translation.py:91-157` |
| `execute_owned_rl_action` | `handles` | `cancel_entry`, `clear_exit`, `exit_market`, `exit_tp_*bp` | `orchestration/rl_action_execution.py:117-161` |
| `exit_tp_200bp` | `valid_only_for_side` | LONG | `rl/action_translation.py:211-216` |
| `exit_tp_180bp` | `valid_only_for_side` | SHORT | `rl/action_translation.py:218-224` |
| `instruction_id` | `is_carried_on_wire_as` | IBKR `orderRef` (entries) | `ibkr/order_execution_submission.py:161`, `ibkr/order_execution_common.py:617` |
| `order_role=EXIT` status projection | `does_not_change` | instruction state | `ledger/instruction_projection.py:54-56` |
| runtime cycle | `does_not_require` | `reqExecutions` | `orchestration/runtime_cycle.py:301-304` |
| `GLOBAL_KILL_SWITCH` | `gates` | entry submission only | `orchestration/entry_submission.py:122`, `orchestration/runtime_entries.py:293` |
| client id `0` | `is_role` | primary runtime / order control | `ibkr/client_ids.py:10` |
| `ibkr-trader-rl-runner.service` | `runs_with_flag` | `--execute-virtual` (not `--execute-broker`) | `ops/systemd/ibkr-trader-rl-runner.service` |
| `execution_runtime_enabled` | `defaults_to` | `false` | `config.py:178` |
| repository | `contains_no` | reward or training feedback loop | `grep -rn 'reward' src scripts` → empty |

---

## 10. Benchmark questions

Questions a graph built from this domain should answer, each with the anchor that makes
the answer checkable. Grouped by difficulty.

### Tier 1 — single-hop lookup

1. Which HTTP endpoint turns an RL action into a broker order intent?
   → `POST /v1/rl/actions/translate` (`api/server_routes_rl.py:484`).
2. What are the eleven `ExecutionState` values? → `orchestration/state_machine.py:9-20`.
3. Which IBKR client id owns order control? → `0` (`ibkr/client_ids.py:10`).
4. What is the uniqueness key for a fill? →
   `(broker_kind, account_key, external_execution_id)` (`db/models.py:543-549`).
5. Which three systemd units run the application stack, and in what order?
   → `ops/systemd/*.service`.

### Tier 2 — path and multi-hop

6. Trace every hop from an RL model decision to an IBKR `placeOrder` for a `market_entry`,
   naming each function. → §1 rows 2→6.
7. Which correlation key lets you join an IBKR `orderStatus` callback back to the RL
   deployment that caused it? → `order_ref` == `instruction_id` → `instruction.payload
   .instruction.trace.metadata.rl_deployment_key` (§3).
8. Which actions mutate broker state without creating a new instruction, and how is the
   target instruction found? → §1 row 5b + `_find_owned_instruction`.
9. What are the four broker→system feedback paths, and which one is deliberately excluded
   from the active trading loop? → §5; executions (`runtime_cycle.py:301-304`).
10. Which state transitions can only be performed by the runtime cycle, not by a
    projection or an API call? → `EXIT_PENDING → COMPLETED`, kill-switch entry cancels (§2).

### Tier 3 — invariants and guards

11. What happens when two active RL-generated instructions match one source candidate?
    → `RLActionOwnershipError` → HTTP 409, no broker call (§8.1).
12. Is the global kill switch able to prevent an RL exit? → No; it gates entries only (§8.4).
13. How does the system avoid interpreting an empty broker snapshot as "no open orders"?
    → `close_missing_open_orders` / `empty_open_orders_authoritative`
    (`orchestration/runtime_cycle.py:365-379`).
14. What prevents a reused IBKR `orderId` from corrupting an existing ledger row?
    → lineage-change detection + `_retire_reused_external_order_id`
    (`ledger/persistence_order_records.py:372-389`).
15. Under what exact condition does an entry become `NEEDS_REVIEW` rather than
    `ENTRY_CANCELLED`? → error 202 + text `risk mitigation`/`trdv` + `order_role=ENTRY` +
    zero entry fill + state in `{ENTRY_SUBMITTED, ENTRY_CANCELLED}`
    (`ledger/instruction_projection.py:190-211`).
16. How many times will a single entry submission retry, and on what error?
    → 3 attempts, IBKR error 201 with a parseable margin message (§8.7).
17. Which eight blocker reasons make `/v1/rl/runtime-state` return
    `state_before: "INCONSISTENT"`, and what does the runner do then? → §5.
18. What stops the reconciliation tables from filling with identical rows during a Gateway
    outage? → 10-minute outage-signature suppression window
    (`orchestration/runtime_audit.py:31`, `:449-526`).
19. Which take-profit basis points are permitted, per side, and where is that pinned?
    → 200 bp LONG / 180 bp SHORT, `rl/action_translation.py:211-224`; recomputed for state
    display at `api/rl_runtime_state.py:395`.
20. Is there any online learning from execution outcomes in this repository?
    → No (§5, "What is *not* a feedback path").

### Tier 4 — cross-cutting / drift detection

21. The closed-broker-order-status set is duplicated in four modules. Name them, and state
    whether they currently agree. → They agree at this commit (§2); a graph edge
    `DUPLICATES_CONSTANT` makes future divergence detectable.
22. Which documented API surface in `docs/current-status.md` no longer matches the code?
    → §11.
23. Which configured intervals determine when broker heartbeat and snapshot data are
    reported `is_stale`? → `broker_heartbeat_interval_seconds` (30 s) and
    `broker_snapshot_refresh_interval_seconds` (60 s), doubled/offset at
    `api/broker_monitor.py:200-220`.
24. If `EXECUTION_RUNTIME_ENABLED` is unset, does the embedded execution loop run?
    → No (`config.py:178`, `:313-317`) — and the repository cannot tell you what the live
    host sets.

---

## 11. Gaps and drift observed while inventorying

These are factual observations about the tracked files, not change requests.

1. **`docs/current-status.md` is stale relative to the code.** It lists
   "Live-trading controls and kill switch", "Broker callback persistence", and "Restart
   reconciliation" under *Not built yet*
   ([`docs/current-status.md:61-67`](current-status.md)), but all three exist:
   `GLOBAL_KILL_SWITCH` ([`orchestration/operator_controls.py:29`](../src/ibkr_trader/orchestration/operator_controls.py)),
   `persist_broker_callback_events()` ([`ledger/persistence_callbacks.py:627`](../src/ibkr_trader/ledger/persistence_callbacks.py)),
   and `POST /v1/runtime/startup-reconcile` ([`api/server_routes_operator.py:864`](../src/ibkr_trader/api/server_routes_operator.py)).
   Its "Current API surface" list also omits the entire `/v1/rl/*` family.
2. **Closed-status set duplicated four times** (§2) with no shared constant.
3. **`ExecutionState.RECEIVED` is never persisted.** `InstructionRuntime` defaults to it in
   memory ([`orchestration/state_machine.py:26`](../src/ibkr_trader/orchestration/state_machine.py)),
   but no code path writes `"RECEIVED"` to `instruction.state`. `FAILED`, by contrast, is
   assigned ([`orchestration/runtime_entries.py:167`](../src/ibkr_trader/orchestration/runtime_entries.py)).
4. **`entry_sessionopen_*` actions are recognized downstream but not translatable.** The
   inference vector masks them ([`rl/inference_vector.py:192`, `:198`](../src/ibkr_trader/rl/inference_vector.py)),
   the runtime-state anchor classifier names them
   ([`api/rl_runtime_state.py:568`](../src/ibkr_trader/api/rl_runtime_state.py)), and the
   runner parses them ([`rl/runner_runtime_state.py:203`](../src/ibkr_trader/rl/runner_runtime_state.py)),
   yet `translate_rl_action()` has no `entry_sessionopen_` branch — such an action would
   fall through to `invalid_action`
   ([`rl/action_translation.py:245-250`](../src/ibkr_trader/rl/action_translation.py)) — and
   `_is_executable_action()` ([`rl/runner_http.py:47-53`](../src/ibkr_trader/rl/runner_http.py))
   does not list it. The translator branch structure and that allowlist are kept in
   agreement by convention, not by construction.
5. **`gateway-exported-logs.txt` (5.2 MB) is tracked at the repository root** and is not
   referenced by any module; it is broker-log material sitting in version control.
6. **Deployment `risk_limits_json` and `action_constraints_json`** exist on
   `trader_deployment` ([`db/models.py:123-128`](../src/ibkr_trader/db/models.py)) but the
   translate endpoint enforces only `allowed_symbols_json`
   ([`api/server_routes_rl.py:539-550`](../src/ibkr_trader/api/server_routes_rl.py)).

---

## 12. Safe local verification

These commands read tracked text or run isolated tests. None starts an application
process, contacts IB Gateway, or touches a live account.

```bash
# Confirm the commit this inventory describes.
git rev-parse HEAD   # expect a762f812bb9421861960f96b9e0fb742db647618

# Model-to-order chain entry points.
rg -n 'def (translate_rl_action|execute_owned_rl_action|submit_order_from_instruction|submit_exit_order_from_instruction|submit_execution_batch|submit_persisted_instruction_entry)' src

# Lifecycle states and the duplicated closed-status set.
rg -n 'class ExecutionState' -A 14 src/ibkr_trader/orchestration/state_machine.py
rg -n 'NOT_FOUND_AT_BROKER' src

# HTTP surface of the execution/feedback domain.
rg -n '@app\.(get|post|put|patch|delete)' src/ibkr_trader/api/server_routes_*.py

# Confirm no reward / training feedback loop exists.
rg -n 'reward' src scripts || echo 'no reward path'

# Focused tests over this domain (in-memory DB, mocked broker boundaries).
uv run --extra dev --extra db --extra rl pytest \
  tests/test_rl_action_translation.py \
  tests/test_rl_action_translation_01.py \
  tests/test_rl_action_translation_02.py \
  tests/test_rl_action_translation_03.py \
  tests/test_order_execution.py \
  tests/test_order_execution_01.py \
  tests/test_entry_submission.py \
  tests/test_instruction_status.py \
  tests/test_ledger_persistence.py \
  tests/test_client_ids.py \
  tests/test_db_schema.py
```

Observed result at this commit in a checkout **without** the shared `../q-data` tree:
`1 failed, 67 passed, 3 skipped`. The single failure is
`tests/test_order_execution_01.py::OrderExecutionTests01::test_submit_order_from_batch_rejects_explicit_short_on_non_shortable_stockholm_account`,
which asserts the "not present on the persisted official IBKR Sweden shortable list"
message but gets "Stockholm shortability snapshot is missing"
([`ibkr/short_sale_validation.py:215`](../src/ibkr_trader/ibkr/short_sale_validation.py)),
because `XSTO_IDENTITY_PATH` / the shortability dataset under `../q-data/xsto`
([`config.py:205-222`](../src/ibkr_trader/config.py)) is absent. This is an environment
dependency of the test, not an execution-path defect.

Do **not** substitute `python -m ibkr_trader.api.server`,
`python -m ibkr_trader.orchestration.runtime_worker`, `python scripts/run_rl_agents.py`,
`npm run dev`, or `npm run start`: those entry points create networked runtime behaviour.
