from __future__ import annotations

import argparse
import json
import logging
from contextlib import asynccontextmanager
from dataclasses import asdict
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import select

try:
    from fastapi import Request as FastAPIRequest
except ModuleNotFoundError:  # pragma: no cover - server extra is optional locally.
    FastAPIRequest = Any  # type: ignore[misc,assignment]

from ibkr_trader.api.broker_monitor import BrokerMonitorService
from ibkr_trader.api.broker_monitor import serialize_broker_monitor_status
from ibkr_trader.api.market_stream_payloads import (
    market_stream_contracts_for_current_holdings,
)
from ibkr_trader.api.market_stream_payloads import (
    market_stream_contracts_for_open_orders,
)
from ibkr_trader.api.market_stream_payloads import (
    market_stream_contracts_for_open_virtual_positions,
)
from ibkr_trader.api.market_stream_payloads import (
    market_stream_contracts_for_runtime_holdings,
)
from ibkr_trader.api.market_stream_payloads import parse_market_stream_subscribe_payload
from ibkr_trader.api.market_stream_payloads import parse_market_stream_symbols
from ibkr_trader.api.market_stream_payloads import subscribe_open_order_market_streams
from ibkr_trader.api.operator_stream_overlay import (
    enrich_operator_snapshot_with_market_stream,
)
from ibkr_trader.api.observation_streaming import _completed_rl_bar_as_of
from ibkr_trader.api.observation_streaming import _merge_persisted_stream_bars
from ibkr_trader.api.observation_streaming import _paused_market_stream_observation
from ibkr_trader.api.observation_streaming import _rl_backfill_instrument
from ibkr_trader.api.payloads import enforce_loopback_binding
from ibkr_trader.api.payloads import is_loopback_host
from ibkr_trader.api.payloads import parse_account_summary_payload
from ibkr_trader.api.payloads import parse_contract_resolve_payload
from ibkr_trader.api.payloads import parse_historical_bars_payload
from ibkr_trader.api.payloads import parse_instruction_archive_payload
from ibkr_trader.api.payloads import parse_instruction_set_cancellation_payload
from ibkr_trader.api.payloads import parse_intent_cleanup_payload
from ibkr_trader.api.payloads import parse_kill_switch_payload
from ibkr_trader.api.payloads import parse_operator_review_payload
from ibkr_trader.api.payloads import parse_positive_limit
from ibkr_trader.api.payloads import parse_rl_action_translate_payload
from ibkr_trader.api.payloads import parse_rl_observation_build_payload
from ibkr_trader.api.payloads import parse_runtime_cycle_payload
from ibkr_trader.api.payloads import parse_shortability_snapshot_payload
from ibkr_trader.api.payloads import parse_stockholm_intraday_backfill_payload
from ibkr_trader.api.payloads import parse_tick_stream_payload
from ibkr_trader.api.payloads import parse_trader_action_payload
from ibkr_trader.api.payloads import parse_trader_deployment_payload
from ibkr_trader.api.payloads import parse_trader_deployment_update_payload
from ibkr_trader.api.payloads import parse_trader_heartbeat_payload
from ibkr_trader.api.payloads import parse_trader_model_payload
from ibkr_trader.api.payloads import parse_virtual_account_payload
from ibkr_trader.api.payloads import parse_virtual_market_quote_payload
from ibkr_trader.api.rl_runtime_state import build_rl_runtime_state_snapshot
from ibkr_trader.api.serialization import serialize_execution_batch
from ibkr_trader.api.serialization import serialize_for_json as _serialize_for_json
from ibkr_trader.api.server_routes_broker_market import register_broker_market_routes
from ibkr_trader.api.server_routes_operator import register_operator_routes
from ibkr_trader.api.server_routes_rl import register_rl_routes
from ibkr_trader.api.status_serializers import broker_exception_detail
from ibkr_trader.api.status_serializers import serialize_operator_instruction_status
from ibkr_trader.api.status_serializers import serialize_rl_candidate_status
from ibkr_trader.api.status_serializers import serialize_runtime_schedule_preview
from ibkr_trader.api.status_serializers import serialize_submitted_batch
from ibkr_trader.config import AppConfig
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import TraderDeploymentRecord
from ibkr_trader.db.models import TraderModelRecord
from ibkr_trader.domain.execution_payloads import parse_datetime
from ibkr_trader.domain.execution_payloads import parse_execution_batch_payload
from ibkr_trader.ibkr.account_summary import read_account_summary
from ibkr_trader.ibkr.contracts import (
    resolve_contracts,
    serialize_contract_resolve_result,
)
from ibkr_trader.ibkr.broker_circuit import BrokerHealthCircuit
from ibkr_trader.ibkr.errors import IbkrDependencyError
from ibkr_trader.ibkr.gateway_diagnostics import read_ibgateway_diagnostics
from ibkr_trader.ibkr.historical_bars import HistoricalBarsQuery, read_historical_bars
from ibkr_trader.ibkr.market_data_backfill import (
    BackgroundMarketDataBackfillService,
    enqueue_market_data_backfill_request,
)
from ibkr_trader.ibkr.market_stream import LiveMarketDataStreamService
from ibkr_trader.ibkr.order_execution import cancel_broker_order
from ibkr_trader.ibkr.order_execution import submit_order_from_batch
from ibkr_trader.ibkr.order_execution import submit_order_from_instruction
from ibkr_trader.ibkr.order_execution import submit_exit_order_from_instruction
from ibkr_trader.ibkr.order_preview import preview_execution_batch
from ibkr_trader.ibkr.pacing import BrokerApiPacingGovernor
from ibkr_trader.ibkr.pacing import BrokerPacingConfig
from ibkr_trader.ibkr.pacing import BrokerPacingLimitExceeded
from ibkr_trader.ibkr.probe import probe_gateway
from ibkr_trader.ibkr.runtime_snapshot import (
    fetch_broker_runtime_snapshot,
    serialize_broker_runtime_snapshot,
)
from ibkr_trader.ibkr.shortability import ShortabilitySource
from ibkr_trader.ibkr.shortability import collect_shortability_snapshot
from ibkr_trader.ibkr.shortability import load_stockholm_identity_map
from ibkr_trader.ibkr.shortability import persist_shortability_snapshot
from ibkr_trader.ibkr.stockholm_intraday import collect_stockholm_intraday_backfill
from ibkr_trader.ibkr.tick_stream import collect_tick_stream_sample
from ibkr_trader.ibkr.session_manager import CanonicalSyncSessions
from ibkr_trader.orchestration.entry_submission import PersistedInstructionNotFoundError
from ibkr_trader.orchestration.entry_submission import PersistedInstructionStateError
from ibkr_trader.orchestration.entry_submission import cancel_persisted_instruction_entry
from ibkr_trader.orchestration.entry_submission import serialize_persisted_broker_cancellation
from ibkr_trader.orchestration.entry_submission import serialize_persisted_broker_submission
from ibkr_trader.orchestration.entry_submission import submit_persisted_instruction_entry
from ibkr_trader.orchestration.instruction_archive import (
    InstructionArchiveSelectorError,
    archive_instruction_set,
    serialize_instruction_archive_result,
)
from ibkr_trader.orchestration.instruction_status import InstructionStatusNotFoundError
from ibkr_trader.orchestration.instruction_status import list_instruction_statuses
from ibkr_trader.orchestration.instruction_status import read_instruction_status
from ibkr_trader.orchestration.instruction_status import serialize_instruction_status
from ibkr_trader.orchestration.intent_replacement import (
    IntentCleanupSelectorError,
    IntentReplacementConflictError,
    cleanup_intent_groups,
    serialize_intent_cleanup_result,
    supersede_batch_intent_entries,
)
from ibkr_trader.orchestration.operator_controls import (
    InstructionSetCancellationNotFoundError,
    InstructionSetCancellationSelectorError,
    KillSwitchActiveError,
    cancel_instruction_set,
    read_kill_switch_state,
    serialize_instruction_set_cancellation_result,
    serialize_kill_switch_status,
    set_kill_switch_state,
)
from ibkr_trader.orchestration.operator_reviews import (
    OperatorReviewTargetNotFoundError,
    archive_open_reconciliation_issues,
    record_broker_attention_review_action,
    record_reconciliation_issue_review_action,
    serialize_reconciliation_issue_archive_result,
    serialize_operator_review_status,
)
from ibkr_trader.orchestration.rl_candidate_lifecycle import (
    retire_completed_rl_candidates,
)
from ibkr_trader.orchestration.rl_candidate_rollover import (
    archive_expired_rl_candidates,
    serialize_rl_candidate_rollover_result,
)
from ibkr_trader.orchestration.runtime_service_state import (
    EXECUTION_RUNTIME_KEY,
    mark_runtime_service_disabled,
    read_runtime_service_status,
    serialize_runtime_service_status,
)
from ibkr_trader.orchestration.runtime_worker import BackgroundExecutionRuntimeService
from ibkr_trader.orchestration.runtime_worker import run_runtime_cycle
from ibkr_trader.orchestration.runtime_worker import run_startup_reconciliation
from ibkr_trader.orchestration.runtime_worker import serialize_runtime_cycle_result
from ibkr_trader.orchestration.scheduling import build_batch_runtime_schedule
from ibkr_trader.orchestration.state_machine import ExecutionState
from ibkr_trader.orchestration.submission import SubmissionConflictError
from ibkr_trader.orchestration.submission import submit_execution_batch
from ibkr_trader.orchestration.rl_action_execution import (
    RLActionOwnershipError,
    RLActionStateError,
    execute_owned_rl_action,
    serialize_rl_owned_action_execution,
)
from ibkr_trader.orchestration.trader_registry import (
    TraderDeploymentConflictError,
    TraderDeploymentNotFoundError,
    TraderModelConflictError,
    TraderModelNotFoundError,
    create_trader_deployment,
    log_trader_action,
    register_trader_model,
    update_trader_deployment,
    upsert_trader_model,
    upsert_trader_heartbeat,
)
from ibkr_trader.ledger.persistence import BROKER_KIND_IBKR
from ibkr_trader.ledger.persistence import persist_broker_runtime_snapshot
from ibkr_trader.ledger.persistence import persist_broker_order_cancellation_result
from ibkr_trader.read_models import build_operator_dashboard_snapshot
from ibkr_trader.read_models import build_ledger_dashboard_snapshot
from ibkr_trader.read_models import build_rl_trader_dashboard_snapshot
from ibkr_trader.read_models import serialize_ledger_dashboard_snapshot
from ibkr_trader.read_models import serialize_operator_dashboard_snapshot
from ibkr_trader.read_models import serialize_rl_trader_dashboard_snapshot
from ibkr_trader.rl.action_translation import ACTION_STATUS_EXECUTED
from ibkr_trader.rl.action_translation import ACTION_STATUS_TRANSLATED
from ibkr_trader.rl.action_translation import translate_rl_action
from ibkr_trader.rl.observations import build_phase1_observation_payload
from ibkr_trader.virtual.accounts import BROKER_KIND_VIRTUAL
from ibkr_trader.virtual.accounts import is_virtual_account_key
from ibkr_trader.virtual.accounts import normalize_virtual_account_key
from ibkr_trader.virtual.execution import cancel_virtual_order
from ibkr_trader.virtual.execution import ensure_virtual_account_record
from ibkr_trader.virtual.execution import list_virtual_market_quotes
from ibkr_trader.virtual.execution import record_virtual_market_quote
from ibkr_trader.virtual.execution import record_virtual_market_quotes_from_stream_snapshot
from ibkr_trader.virtual.execution import submit_virtual_entry_order
from ibkr_trader.virtual.execution import submit_virtual_exit_order


LOGGER = logging.getLogger(__name__)


class ApiDependencyError(RuntimeError):
    """Raised when optional API server dependencies are unavailable."""


_BACKGROUND_RECOVERY_INSTRUCTION_STATES = {
    ExecutionState.ENTRY_SUBMITTED.value,
    ExecutionState.POSITION_OPEN.value,
    ExecutionState.EXIT_PENDING.value,
}
_OPERATOR_TERMINAL_EXECUTION_STATES = {
    ExecutionState.ENTRY_CANCELLED.value,
    ExecutionState.COMPLETED.value,
    ExecutionState.FAILED.value,
}
_BACKGROUND_RECOVERY_CLOSED_ORDER_STATUSES = {
    "API_CANCELLED",
    "CANCELLED",
    "ERROR",
    "FILLED",
    "INACTIVE",
    "NOT_FOUND_AT_BROKER",
    "REJECTED",
}



def should_include_background_execution_recovery(
    session_factory: Any,
) -> bool:
    with session_scope(session_factory) as session:
        active_instruction = session.execute(
            select(InstructionRecord.id)
            .where(
                InstructionRecord.state.in_(
                    tuple(_BACKGROUND_RECOVERY_INSTRUCTION_STATES)
                ),
                InstructionRecord.is_virtual.is_(False),
            )
            .limit(1)
        ).first()
        if active_instruction is not None:
            return True

        unsettled_order = session.execute(
            select(BrokerOrderRecord.id)
            .where(
                BrokerOrderRecord.is_virtual.is_(False),
                or_(
                    BrokerOrderRecord.status.is_(None),
                    func.upper(BrokerOrderRecord.status).not_in(
                        _BACKGROUND_RECOVERY_CLOSED_ORDER_STATUSES
                    ),
                )
            )
            .limit(1)
        ).first()
        return unsettled_order is not None





def _load_fastapi_runtime() -> tuple[Any, Any, Any, Any]:
    try:
        from fastapi import FastAPI, HTTPException, Request
        from fastapi.responses import JSONResponse
    except ModuleNotFoundError as exc:
        raise ApiDependencyError(
            "FastAPI server dependencies are not installed. "
            "Install the optional `server` dependencies for this project."
        ) from exc

    return FastAPI, HTTPException, Request, JSONResponse


def create_app(config: AppConfig | None = None) -> Any:
    app_config = config or AppConfig.from_env()
    engine = build_engine(app_config.database_url)
    session_factory = create_session_factory(engine)
    enforce_loopback_binding(
        app_config.api.host,
        require_loopback_only=app_config.api.require_loopback_only,
    )
    FastAPI, HTTPException, Request, JSONResponse = _load_fastapi_runtime()
    broker_pacing_governor = BrokerApiPacingGovernor(
        BrokerPacingConfig(
            max_requests_per_second=app_config.ibkr_api_max_requests_per_second,
            request_acquire_timeout_seconds=app_config.ibkr_api_pacing_timeout_seconds,
            max_market_data_lines=app_config.ibkr_market_data_line_limit,
            max_historical_requests_per_10_minutes=(
                app_config.ibkr_historical_requests_per_10_minutes
            ),
        )
    )
    broker_circuit = BrokerHealthCircuit(
        default_open_seconds=app_config.broker_api_startup_failure_slow_probe_seconds
    )
    broker_sessions = CanonicalSyncSessions(
        app_config.ibkr,
        initial_connect_backoff_seconds=app_config.broker_connect_backoff_initial_seconds,
        max_connect_backoff_seconds=app_config.broker_connect_backoff_max_seconds,
        api_startup_failure_slow_probe_seconds=(
            app_config.broker_api_startup_failure_slow_probe_seconds
        ),
        pacing_governor=broker_pacing_governor,
        broker_circuit=broker_circuit,
    )

    def with_primary_session(
        operation_name: str,
        operation: Any,
        *,
        ignore_cooldown: bool = False,
    ) -> Any:
        return broker_sessions.primary.execute(
            operation_name,
            operation,
            ignore_cooldown=ignore_cooldown,
        )

    def with_diagnostic_session(
        operation_name: str,
        operation: Any,
        *,
        ignore_cooldown: bool = False,
    ) -> Any:
        return broker_sessions.diagnostic.execute(
            operation_name,
            operation,
            ignore_cooldown=ignore_cooldown,
        )

    def with_historical_session(
        operation_name: str,
        operation: Any,
        *,
        ignore_cooldown: bool = False,
    ) -> Any:
        return broker_sessions.historical.execute(
            operation_name,
            operation,
            ignore_cooldown=ignore_cooldown,
        )

    def submit_order_with_primary(
        broker_config: Any,
        instruction: Any,
        *,
        timeout: int = 10,
    ) -> dict[str, Any]:
        if is_virtual_account_key(instruction.account.account_key):
            return submit_virtual_entry_order(
                session_factory,
                broker_config,
                instruction,
                timeout=timeout,
            )
        return with_primary_session(
            "persisted_entry_submit",
            lambda broker_app: submit_order_from_instruction(
                broker_config,
                instruction,
                timeout=timeout,
                app=broker_app,
            )
        )

    def submit_exit_with_primary(
        broker_config: Any,
        instruction: Any,
        *,
        quantity: Decimal,
        order_type: Any,
        order_ref: str,
        timeout: int = 10,
        limit_price: Decimal | None = None,
        stop_price: Decimal | None = None,
        oca_group: str | None = None,
        oca_type: int | None = None,
    ) -> dict[str, Any]:
        if is_virtual_account_key(instruction.account.account_key):
            return submit_virtual_exit_order(
                session_factory,
                broker_config,
                instruction,
                quantity=quantity,
                order_type=order_type,
                order_ref=order_ref,
                timeout=timeout,
                limit_price=limit_price,
                stop_price=stop_price,
                oca_group=oca_group,
                oca_type=oca_type,
            )
        return with_primary_session(
            "runtime_exit_submit",
            lambda broker_app: submit_exit_order_from_instruction(
                broker_config,
                instruction,
                quantity=quantity,
                order_type=order_type,
                order_ref=order_ref,
                timeout=timeout,
                limit_price=limit_price,
                stop_price=stop_price,
                oca_group=oca_group,
                oca_type=oca_type,
                app=broker_app,
            )
        )

    def cancel_order_with_primary(
        broker_config: Any,
        order_id: int,
        *,
        timeout: int = 10,
    ) -> dict[str, Any]:
        with session_scope(session_factory) as session:
            is_virtual_order = bool(
                session.execute(
                    select(BrokerOrderRecord.is_virtual).where(
                        BrokerOrderRecord.external_order_id == str(order_id)
                    )
                ).scalar_one_or_none()
            )
        if is_virtual_order:
            return cancel_virtual_order(
                session_factory,
                broker_config,
                order_id,
                timeout=timeout,
            )
        return with_primary_session(
            "broker_cancel",
            lambda broker_app: cancel_broker_order(
                broker_config,
                order_id,
                timeout=timeout,
                app=broker_app,
            )
        )

    def fetch_runtime_snapshot_with_primary(
        broker_config: Any,
        *,
        timeout: int = 10,
        include_open_orders: bool = True,
        include_executions: bool = True,
        include_account_updates: bool = True,
        include_positions: bool = True,
    ) -> Any:
        return with_primary_session(
            "broker_runtime_snapshot",
            lambda broker_app: fetch_broker_runtime_snapshot(
                broker_config,
                timeout=timeout,
                include_open_orders=include_open_orders,
                include_executions=include_executions,
                include_account_updates=include_account_updates,
                include_positions=include_positions,
                app=broker_app,
            )
        )

    def fetch_runtime_snapshot_with_diagnostic(
        broker_config: Any,
        *,
        timeout: int = 10,
        include_open_orders: bool = True,
        include_executions: bool = True,
        include_account_updates: bool = True,
        include_positions: bool = True,
    ) -> Any:
        return with_diagnostic_session(
            "broker_runtime_snapshot",
            lambda broker_app: fetch_broker_runtime_snapshot(
                broker_config,
                timeout=timeout,
                include_open_orders=include_open_orders,
                include_executions=include_executions,
                include_account_updates=include_account_updates,
                include_positions=include_positions,
                app=broker_app,
            )
        )

    def fetch_reconciliation_runtime_snapshot_with_primary(
        broker_config: Any,
        *,
        timeout: int = 10,
        include_open_orders: bool = True,
        include_executions: bool = True,
        include_account_updates: bool = False,
        include_positions: bool = True,
    ) -> Any:
        return fetch_runtime_snapshot_with_primary(
            broker_config,
            timeout=timeout,
            include_open_orders=include_open_orders,
            include_executions=include_executions,
            include_account_updates=include_account_updates,
            include_positions=include_positions,
        )

    def drain_broker_callbacks_with_primary() -> list[dict[str, Any]]:
        return broker_sessions.primary.drain_broker_callback_events(
            connect_if_needed=False,
        )

    def run_diagnostic_heartbeat_probe() -> Any:
        return with_diagnostic_session(
            "heartbeat_probe",
            lambda broker_app: probe_gateway(
                app_config.ibkr.diagnostic_session(),
                timeout=app_config.broker_heartbeat_timeout_seconds,
                app=broker_app,
            ),
        )

    def fetch_background_runtime_snapshot() -> Any:
        account_id = app_config.ibkr.account_id.strip()
        diagnostic_config = app_config.ibkr.diagnostic_session()
        if account_id:
            diagnostic_config = replace(
                diagnostic_config,
                account_id=account_id,
                account_ids=(account_id,),
            )
        include_execution_recovery = should_include_background_execution_recovery(
            session_factory
        )
        return fetch_runtime_snapshot_with_diagnostic(
            diagnostic_config,
            timeout=app_config.broker_snapshot_refresh_timeout_seconds,
            include_open_orders=True,
            include_executions=include_execution_recovery,
            include_positions=False,
        )

    def persist_background_runtime_snapshot(snapshot: Any, captured_at: datetime) -> None:
        persist_broker_runtime_snapshot(
            session_factory,
            snapshot,
            broker_kind="IBKR",
            captured_at=captured_at,
            default_account_key=app_config.ibkr.account_id or None,
            close_missing_open_orders=True,
        )
        try:
            subscribe_open_order_market_streams(
                market_stream_service,
                snapshot,
                session_factory=session_factory,
            )
        except Exception:
            LOGGER.warning(
                "Failed to sync market stream subscriptions for open broker orders.",
                exc_info=True,
            )

    broker_monitor = BrokerMonitorService(
        heartbeat_probe=run_diagnostic_heartbeat_probe,
        snapshot_fetcher=fetch_background_runtime_snapshot,
        snapshot_persister=persist_background_runtime_snapshot,
        heartbeat_interval_seconds=app_config.broker_heartbeat_interval_seconds,
        snapshot_refresh_interval_seconds=app_config.broker_snapshot_refresh_interval_seconds,
    )
    market_stream_service = LiveMarketDataStreamService(
        app_config.ibkr.streaming_session(),
        initial_connect_backoff_seconds=app_config.broker_connect_backoff_initial_seconds,
        max_connect_backoff_seconds=app_config.broker_connect_backoff_max_seconds,
        stale_data_after_seconds=app_config.market_stream_stale_after_seconds,
        stale_reconnect_enabled=app_config.market_stream_stale_reconnect_enabled,
        stale_reconnect_timezone=app_config.timezone,
        pacing_governor=broker_pacing_governor,
        broker_circuit=broker_circuit,
    )

    def sync_virtual_market_watch_from_stream(cycle_at: datetime) -> dict[str, Any]:
        return record_virtual_market_quotes_from_stream_snapshot(
            session_factory,
            stream_snapshot=market_stream_service.snapshot(bar_limit=1),
            observed_at=cycle_at,
        )

    execution_runtime = BackgroundExecutionRuntimeService(
        session_factory,
        app_config,
        broker_sessions,
        virtual_market_sync=sync_virtual_market_watch_from_stream,
    )
    market_data_backfill_worker = BackgroundMarketDataBackfillService(
        session_factory,
        broker_config=app_config.ibkr.historical_session(),
        execute_historical=with_historical_session,
        interval_seconds=app_config.market_data_backfill_interval_seconds,
        batch_size=app_config.market_data_backfill_batch_size,
        timeout_seconds=app_config.market_data_backfill_timeout_seconds,
    )
    market_stream_identity_map = load_stockholm_identity_map(
        app_config.stockholm_identity_path,
    )

    @asynccontextmanager
    async def lifespan(_: Any) -> Any:
        if app_config.broker_warmup_enabled:
            broker_sessions.warmup()
        if app_config.broker_monitor_enabled and app_config.environment != "test":
            broker_monitor.start()
        if (
            app_config.market_stream_auto_reconnect_enabled
            and app_config.environment != "test"
        ):
            market_stream_service.start_auto_reconnect(
                interval_seconds=app_config.market_stream_reconnect_interval_seconds
            )
        if app_config.execution_runtime_enabled and app_config.environment != "test":
            execution_runtime.start()
        elif app_config.environment != "test":
            mark_runtime_service_disabled(
                session_factory,
                runtime_key=EXECUTION_RUNTIME_KEY,
                note="Execution runtime disabled by EXECUTION_RUNTIME_ENABLED=false.",
            )
        if (
            app_config.market_data_backfill_worker_enabled
            and app_config.environment != "test"
        ):
            market_data_backfill_worker.start()
        try:
            yield
        finally:
            market_data_backfill_worker.stop()
            market_stream_service.stop()
            execution_runtime.stop()
            broker_monitor.stop()
            broker_sessions.shutdown()

    app = FastAPI(
        title="IBKR Trader Local API",
        version="0.1.0",
        summary="Local-only control plane for the IBKR Trader runtime.",
        lifespan=lifespan,
    )
    app.state.broker_sessions = broker_sessions
    app.state.broker_pacing_governor = broker_pacing_governor
    app.state.broker_circuit = broker_circuit
    app.state.broker_monitor = broker_monitor
    app.state.execution_runtime = execution_runtime
    app.state.market_data_backfill_worker = market_data_backfill_worker
    app.state.market_stream_service = market_stream_service
    app.state.market_stream_identity_map = market_stream_identity_map

    @app.exception_handler(BrokerPacingLimitExceeded)
    async def broker_pacing_limit_handler(
        _: FastAPIRequest,
        exc: BrokerPacingLimitExceeded,
    ) -> Any:
        return JSONResponse(status_code=429, content={"detail": str(exc)})

    @app.middleware("http")
    async def require_local_client(request: FastAPIRequest, call_next: Any) -> Any:
        client_host = request.client.host if request.client else None
        if (
            app_config.api.require_loopback_only
            and not is_loopback_host(client_host)
        ):
            return JSONResponse(
                status_code=403,
                content={"detail": "This API accepts loopback clients only."},
            )
        return await call_next(request)

    route_context = SimpleNamespace(
        app_config=app_config,
        session_factory=session_factory,
        broker_sessions=broker_sessions,
        broker_monitor=broker_monitor,
        market_stream_service=market_stream_service,
        market_data_backfill_worker=market_data_backfill_worker,
        market_stream_identity_map=market_stream_identity_map,
        execution_runtime=execution_runtime,
        with_primary_session=with_primary_session,
        with_diagnostic_session=with_diagnostic_session,
        with_historical_session=with_historical_session,
        submit_order_with_primary=submit_order_with_primary,
        submit_exit_with_primary=submit_exit_with_primary,
        cancel_order_with_primary=cancel_order_with_primary,
        fetch_runtime_snapshot_with_primary=fetch_runtime_snapshot_with_primary,
        fetch_reconciliation_runtime_snapshot_with_primary=(
            fetch_reconciliation_runtime_snapshot_with_primary
        ),
        drain_broker_callbacks_with_primary=drain_broker_callbacks_with_primary,
        sync_virtual_market_watch_from_stream=sync_virtual_market_watch_from_stream,
        collect_stockholm_intraday_backfill=(
            lambda *args, **kwargs: collect_stockholm_intraday_backfill(*args, **kwargs)
        ),
        collect_tick_stream_sample=(
            lambda *args, **kwargs: collect_tick_stream_sample(*args, **kwargs)
        ),
        HTTPException=HTTPException,
        Request=Request,
    )
    register_broker_market_routes(app, route_context)
    register_rl_routes(app, route_context)
    register_operator_routes(app, route_context)

    return app


def run_server(config: AppConfig | None = None, *, reload: bool = False) -> None:
    app_config = config or AppConfig.from_env()
    enforce_loopback_binding(
        app_config.api.host,
        require_loopback_only=app_config.api.require_loopback_only,
    )

    try:
        import uvicorn
    except ModuleNotFoundError as exc:
        raise ApiDependencyError(
            "Uvicorn is not installed. Install the optional `server` dependencies "
            "for this project."
        ) from exc

    uvicorn.run(
        create_app(app_config),
        host=app_config.api.host,
        port=app_config.api.port,
        reload=reload,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the local-only FastAPI server for IBKR Trader."
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable development reload mode.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    run_server(reload=args.reload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
