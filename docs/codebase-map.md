# ibkr-trader codebase map

This is an orientation map of the repository as it exists, not a production-readiness statement or an operating procedure. Every architectural statement below points to repository evidence. Instructions represent intent; broker orders, events, fills, account snapshots, and position snapshots persisted in the ledger are the operational record.

## Operational boundaries

Use this document for static, local orientation only.

- Do **not** start or restart the API, dashboard, RL runner, any other service, or IB Gateway.
- Do **not** run broker probes, runtime cycles, stream or backfill workers, order commands, or any command that connects to or acts on a live broker account. The API lifespan can start broker-facing background work, as shown in [`src/ibkr_trader/api/server.py`](../src/ibkr_trader/api/server.py), and the standalone runtime warms broker sessions, as shown in [`src/ibkr_trader/orchestration/runtime_worker.py`](../src/ibkr_trader/orchestration/runtime_worker.py).
- Do **not** deploy this checkout or use the service declarations below as permission to change a host.
- Do **not** infer or claim production readiness from this map. Readiness has separate checks and open qualifications in [`docs/current-status.md`](current-status.md), [`docs/rl-operational-readiness.md`](rl-operational-readiness.md), and [`scripts/check_operational_readiness.py`](../scripts/check_operational_readiness.py).

## Runtime topology and entry points

### API and control plane

The executable API entry point is [`src/ibkr_trader/api/server.py`](../src/ibkr_trader/api/server.py): `create_app()` constructs the database/session layer, canonical broker sessions, background broker monitor, market stream, backfill worker, and execution runtime; its FastAPI lifespan conditionally starts and stops those background components; `main()` runs Uvicorn. The checked-in service declaration invokes the same module in [`ops/systemd/ibkr-trader-api.service`](../ops/systemd/ibkr-trader-api.service). By contrast, [`src/ibkr_trader/main.py`](../src/ibkr_trader/main.py) only prints selected configuration and is not the API server.

Routes are separated by responsibility:

- Broker state, market data, orders, validation, and virtual-account endpoints are registered from [`src/ibkr_trader/api/server_routes_broker_market.py`](../src/ibkr_trader/api/server_routes_broker_market.py).
- Operator snapshots and controls, instruction submission, and manual runtime-cycle endpoints are registered from [`src/ibkr_trader/api/server_routes_operator.py`](../src/ibkr_trader/api/server_routes_operator.py).
- RL registry, deployment, observation, action, candidate, and dashboard endpoints are registered from [`src/ibkr_trader/api/server_routes_rl.py`](../src/ibkr_trader/api/server_routes_rl.py).

The API serialization and view-building boundary is under [`src/ibkr_trader/api/`](../src/ibkr_trader/api/), including operator payloads in [`operator_payloads.py`](../src/ibkr_trader/api/operator_payloads.py), broker payloads in [`broker_payloads.py`](../src/ibkr_trader/api/broker_payloads.py), and status serializers in [`status_serializers.py`](../src/ibkr_trader/api/status_serializers.py).

### Dashboard

The operator dashboard is a SvelteKit application under [`dashboard/`](../dashboard/). Its root page connects the server-side loader and actions in [`dashboard/src/routes/+page.server.js`](../dashboard/src/routes/+page.server.js), which delegate to [`operator-dashboard.server-load.js`](../dashboard/src/routes/operator-dashboard.server-load.js) and [`operator-dashboard.server-actions.js`](../dashboard/src/routes/operator-dashboard.server-actions.js); client components live in [`dashboard/src/routes/operator-dashboard-client/components/`](../dashboard/src/routes/operator-dashboard-client/components/). Dedicated server-loaded views are rooted at [`dashboard/src/routes/ledger/+page.server.js`](../dashboard/src/routes/ledger/+page.server.js) and [`dashboard/src/routes/rl/+page.server.js`](../dashboard/src/routes/rl/+page.server.js).

Server-side requests to the trader API are centralized in [`dashboard/src/lib/server/trader-api.js`](../dashboard/src/lib/server/trader-api.js). Build and start scripts are declared in [`dashboard/package.json`](../dashboard/package.json), while the checked-in deployed-process shape is documented by [`ops/systemd/ibkr-trader-dashboard.service`](../ops/systemd/ibkr-trader-dashboard.service).

### Orchestration and execution runtime

The persistent execution loop is `run_persistent_execution_runtime()` in [`src/ibkr_trader/orchestration/runtime_worker.py`](../src/ibkr_trader/orchestration/runtime_worker.py). It acquires a durable runtime lease, performs startup reconciliation, runs cycles, and records lifecycle status; the same module supplies an embedded `BackgroundExecutionRuntimeService` and a standalone CLI `main()`. The API embeds that service from [`src/ibkr_trader/api/server.py`](../src/ibkr_trader/api/server.py).

One cycle is coordinated in [`src/ibkr_trader/orchestration/runtime_cycle.py`](../src/ibkr_trader/orchestration/runtime_cycle.py). Work is divided among [`runtime_planning.py`](../src/ibkr_trader/orchestration/runtime_planning.py), [`runtime_entries.py`](../src/ibkr_trader/orchestration/runtime_entries.py), [`runtime_fills.py`](../src/ibkr_trader/orchestration/runtime_fills.py), [`runtime_position_lifecycle.py`](../src/ibkr_trader/orchestration/runtime_position_lifecycle.py), [`runtime_protective_exits.py`](../src/ibkr_trader/orchestration/runtime_protective_exits.py), and [`runtime_exit_cleanup.py`](../src/ibkr_trader/orchestration/runtime_exit_cleanup.py). Durable ownership and status are implemented in [`runtime_service_state.py`](../src/ibkr_trader/orchestration/runtime_service_state.py). Instruction intake and transitions are implemented in [`submission.py`](../src/ibkr_trader/orchestration/submission.py), [`entry_submission.py`](../src/ibkr_trader/orchestration/entry_submission.py), and [`state_machine.py`](../src/ibkr_trader/orchestration/state_machine.py).

### Broker integration

Broker-facing code is under [`src/ibkr_trader/ibkr/`](../src/ibkr_trader/ibkr/). [`session_manager.py`](../src/ibkr_trader/ibkr/session_manager.py) owns canonical persistent sessions and reconnect behavior. Role-based client IDs are defined in [`client_ids.py`](../src/ibkr_trader/ibkr/client_ids.py) and explained in [`docs/client-id-policy.md`](client-id-policy.md): `0` is primary runtime/order control, `7` diagnostics and controlled lookups, `8` historical/backfill, and `9` streaming.

The order boundary is [`order_execution.py`](../src/ibkr_trader/ibkr/order_execution.py), with submission and cancellation details in [`order_execution_submission.py`](../src/ibkr_trader/ibkr/order_execution_submission.py) and [`order_execution_cancel.py`](../src/ibkr_trader/ibkr/order_execution_cancel.py). Broker runtime snapshots are assembled in [`runtime_snapshot.py`](../src/ibkr_trader/ibkr/runtime_snapshot.py). Streaming and historical paths are distinct in [`market_stream.py`](../src/ibkr_trader/ibkr/market_stream.py), [`market_stream_store.py`](../src/ibkr_trader/ibkr/market_stream_store.py), and [`market_data_backfill.py`](../src/ibkr_trader/ibkr/market_data_backfill.py).

### Ledger and read models

SQLAlchemy engine, session, and schema helpers live in [`src/ibkr_trader/db/base.py`](../src/ibkr_trader/db/base.py); first-class persisted records are declared in [`src/ibkr_trader/db/models.py`](../src/ibkr_trader/db/models.py). The schema initializer entry point is [`src/ibkr_trader/db/init_schema.py`](../src/ibkr_trader/db/init_schema.py), also referenced by the API service declaration in [`ops/systemd/ibkr-trader-api.service`](../ops/systemd/ibkr-trader-api.service).

Ledger writes are grouped under [`src/ibkr_trader/ledger/`](../src/ibkr_trader/ledger/): [`persistence.py`](../src/ibkr_trader/ledger/persistence.py) exposes the public persistence operations, while callbacks, order records, runtime snapshots, and snapshot records are split into [`persistence_callbacks.py`](../src/ibkr_trader/ledger/persistence_callbacks.py), [`persistence_order_records.py`](../src/ibkr_trader/ledger/persistence_order_records.py), [`persistence_runtime.py`](../src/ibkr_trader/ledger/persistence_runtime.py), and [`persistence_snapshots.py`](../src/ibkr_trader/ledger/persistence_snapshots.py). [`instruction_projection.py`](../src/ibkr_trader/ledger/instruction_projection.py) projects terminal broker order evidence back onto instruction state. API payload modules and the dashboard server loaders cited above form the local read/view path; they should be preferred for operator views over adding ad hoc live-broker reads.

### RL runner

The executable RL runner is [`scripts/run_rl_agents.py`](../scripts/run_rl_agents.py). Its `main()` parses API, execution-mode, model-history, stream, and state-file options, then delegates the polling work into [`src/ibkr_trader/rl/runner_loop.py`](../src/ibkr_trader/rl/runner_loop.py). The checked-in service invokes that script in virtual execution mode, as shown in [`ops/systemd/ibkr-trader-rl-runner.service`](../ops/systemd/ibkr-trader-rl-runner.service).

Runner responsibilities are split across [`runner_deployments.py`](../src/ibkr_trader/rl/runner_deployments.py), [`runner_model.py`](../src/ibkr_trader/rl/runner_model.py), [`runner_history.py`](../src/ibkr_trader/rl/runner_history.py), [`runner_decisions.py`](../src/ibkr_trader/rl/runner_decisions.py), [`runner_runtime_state.py`](../src/ibkr_trader/rl/runner_runtime_state.py), [`runner_stream.py`](../src/ibkr_trader/rl/runner_stream.py), and [`runner_http.py`](../src/ibkr_trader/rl/runner_http.py). Observation construction, model contracts, and action translation live in [`observations.py`](../src/ibkr_trader/rl/observations.py), [`model_contracts.py`](../src/ibkr_trader/rl/model_contracts.py), and [`action_translation.py`](../src/ibkr_trader/rl/action_translation.py). The runner communicates through API endpoints rather than owning the database directly, evidenced by HTTP reads and writes in [`runner_loop.py`](../src/ibkr_trader/rl/runner_loop.py) and [`runner_http.py`](../src/ibkr_trader/rl/runner_http.py).

## Safe local inspection and tests

Run these from the repository root. They inspect tracked text or execute isolated tests; they do not start an application process or contact IB Gateway:

```bash
# Static inventory and entry-point inspection.
rg --files src/ibkr_trader dashboard/src scripts ops/systemd docs | sort
rg -n '^(def (create_app|main|run_persistent_execution_runtime)|class BackgroundExecutionRuntimeService)' \
  src/ibkr_trader/api/server.py \
  src/ibkr_trader/orchestration/runtime_worker.py \
  scripts/run_rl_agents.py

# Focused tests whose fixtures use local/in-memory state and mocked broker boundaries.
uv run --extra dev --extra db --extra rl pytest \
  tests/test_config.py \
  tests/test_client_ids.py \
  tests/test_db_schema.py \
  tests/test_rl_runner_01.py \
  tests/test_rl_runner_02.py

# Compile the dashboard without starting its development or preview servers.
npm --prefix dashboard ci
npm --prefix dashboard run build
```

Do not substitute server commands such as `python -m ibkr_trader.api.server`, `python -m ibkr_trader.orchestration.runtime_worker`, `python scripts/run_rl_agents.py`, `npm run dev`, or `npm run start`: their entry points can create networked runtime behavior documented above.
