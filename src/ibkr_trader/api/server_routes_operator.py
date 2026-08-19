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
from ibkr_trader.orchestration.entry_submission import (
    cancel_persisted_instruction_entry,
)
from ibkr_trader.orchestration.entry_submission import (
    serialize_persisted_broker_cancellation,
)
from ibkr_trader.orchestration.entry_submission import (
    serialize_persisted_broker_submission,
)
from ibkr_trader.orchestration.entry_submission import (
    submit_persisted_instruction_entry,
)
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
    deferred_reentry_instruction_ids_for_cleanup,
    serialize_intent_cleanup_result,
    supersede_batch_intent_entries,
)
from ibkr_trader.orchestration.operator_controls import (
    InstructionSetCancellationNotFoundError,
    InstructionSetCancellationSelectorError,
    KillSwitchActiveError,
    cancel_instruction_set,
    read_broker_maintenance_mode_state,
    read_kill_switch_state,
    set_broker_maintenance_mode_state,
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
from ibkr_trader.settings_registry import read_settings_registry
from ibkr_trader.settings_registry import serialize_settings_registry
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
from ibkr_trader.virtual.execution import (
    record_virtual_market_quotes_from_stream_snapshot,
)
from ibkr_trader.virtual.execution import submit_virtual_entry_order
from ibkr_trader.virtual.execution import submit_virtual_exit_order


LOGGER = logging.getLogger(__name__)


_OPERATOR_TERMINAL_EXECUTION_STATES = {
    ExecutionState.ENTRY_CANCELLED.value,
    ExecutionState.COMPLETED.value,
    ExecutionState.FAILED.value,
}


def register_operator_routes(app: Any, context: Any) -> None:
    app_config = context.app_config
    session_factory = context.session_factory
    broker_sessions = context.broker_sessions
    broker_monitor = context.broker_monitor
    market_stream_service = context.market_stream_service
    market_data_backfill_worker = context.market_data_backfill_worker
    market_stream_identity_map = context.market_stream_identity_map
    execution_runtime = context.execution_runtime
    with_primary_session = context.with_primary_session
    with_diagnostic_session = context.with_diagnostic_session
    with_historical_session = context.with_historical_session
    submit_order_with_primary = context.submit_order_with_primary
    submit_exit_with_primary = context.submit_exit_with_primary
    cancel_order_with_primary = context.cancel_order_with_primary
    fetch_runtime_snapshot_with_primary = context.fetch_runtime_snapshot_with_primary
    fetch_reconciliation_runtime_snapshot_with_primary = (
        context.fetch_reconciliation_runtime_snapshot_with_primary
    )
    drain_broker_callbacks_with_primary = context.drain_broker_callbacks_with_primary
    sync_virtual_market_watch_from_stream = (
        context.sync_virtual_market_watch_from_stream
    )
    market_data_readiness_checker = context.market_data_readiness_checker
    HTTPException = context.HTTPException
    Request = context.Request

    @app.get("/v1/read/operator-snapshot")
    def get_operator_snapshot(
        request: FastAPIRequest,
        instruction_limit: int = 500,
        candidate_limit: int = 20,
        candidate_reason_code: str | None = None,
        order_limit: int = 50,
        fill_limit: int = 50,
        attention_limit: int = 25,
        reconciliation_run_limit: int = 20,
        include_flat_positions: bool = False,
        include_expired_candidates: bool = False,
        include_terminal_instructions: bool = False,
    ) -> dict[str, Any]:
        try:
            validated_instruction_limit = parse_positive_limit(
                instruction_limit,
                field_name="instruction_limit",
                maximum=500,
            )
            validated_candidate_limit = parse_positive_limit(
                candidate_limit,
                field_name="candidate_limit",
                maximum=500,
            )
            validated_order_limit = parse_positive_limit(
                order_limit,
                field_name="order_limit",
                maximum=500,
            )
            validated_fill_limit = parse_positive_limit(
                fill_limit,
                field_name="fill_limit",
                maximum=500,
            )
            validated_attention_limit = parse_positive_limit(
                attention_limit,
                field_name="attention_limit",
                maximum=200,
            )
            validated_reconciliation_run_limit = parse_positive_limit(
                reconciliation_run_limit,
                field_name="reconciliation_run_limit",
                maximum=200,
            )
            if not include_expired_candidates:
                archive_expired_rl_candidates(session_factory)
                retire_completed_rl_candidates(session_factory)
            operator_snapshot = build_operator_dashboard_snapshot(
                session_factory,
                include_flat_positions=include_flat_positions,
                order_limit=validated_order_limit,
                fill_limit=validated_fill_limit,
                attention_limit=validated_attention_limit,
                reconciliation_run_limit=validated_reconciliation_run_limit,
            )
            instructions = list_instruction_statuses(
                session_factory,
                limit=validated_instruction_limit,
                model_routed=False,
            )
            if not include_terminal_instructions:
                instructions = tuple(
                    instruction
                    for instruction in instructions
                    if instruction.state not in _OPERATOR_TERMINAL_EXECUTION_STATES
                )
            rl_candidates = list_instruction_statuses(
                session_factory,
                limit=500,
                state=ExecutionState.MODEL_ROUTED_PENDING.value,
                model_routed=True,
                expire_after=None if include_expired_candidates else utc_now(),
            )
            normalized_candidate_reason_code = (
                candidate_reason_code.strip()
                if candidate_reason_code is not None and candidate_reason_code.strip()
                else None
            )
            if normalized_candidate_reason_code is not None:

                def candidate_reason_code(candidate: Any) -> Any:
                    instruction_payload = candidate.payload.get("instruction", {})
                    if not isinstance(instruction_payload, dict):
                        return None
                    trace_payload = instruction_payload.get("trace", {})
                    if not isinstance(trace_payload, dict):
                        return None
                    return trace_payload.get("reason_code")

                rl_candidates = tuple(
                    candidate
                    for candidate in rl_candidates
                    if candidate_reason_code(candidate)
                    == normalized_candidate_reason_code
                )
            rl_candidates = rl_candidates[:validated_candidate_limit]
            operator_snapshot_payload = serialize_operator_dashboard_snapshot(
                operator_snapshot,
            )
            stream_symbols = sorted(
                {
                    str(row.get("symbol") or row.get("local_symbol") or "")
                    .strip()
                    .upper()
                    for collection_name in ("positions", "open_orders")
                    for row in operator_snapshot_payload.get(collection_name, [])
                    if isinstance(row, dict)
                    and str(row.get("symbol") or row.get("local_symbol") or "").strip()
                }
            )
            if stream_symbols:
                try:
                    operator_stream_snapshot = (
                        request.app.state.market_stream_service.snapshot(
                            symbols=stream_symbols,
                            bar_limit=2,
                        )
                    )
                except Exception as exc:  # pragma: no cover - defensive UI fallback.
                    operator_snapshot_payload["market_stream_overlay"] = {
                        "applied": False,
                        "error": str(exc),
                    }
                else:
                    enrich_operator_snapshot_with_market_stream(
                        operator_snapshot_payload,
                        operator_stream_snapshot,
                    )
            else:
                operator_snapshot_payload["market_stream_overlay"] = {
                    "applied": False,
                    "reason": "no positions or open orders",
                }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "operator_snapshot": {
                **operator_snapshot_payload,
                "instructions": [
                    serialize_operator_instruction_status(instruction, app_config)
                    for instruction in (*rl_candidates, *instructions)
                ],
            },
        }

    @app.get("/v1/read/ledger-snapshot")
    def get_ledger_snapshot(
        focus_instruction_id: str | None = None,
        instruction_event_limit: int = 100,
        order_event_limit: int = 100,
        fill_limit: int = 100,
        control_event_limit: int = 50,
        cancellation_limit: int = 50,
        reconciliation_issue_limit: int = 50,
    ) -> dict[str, Any]:
        try:
            validated_instruction_event_limit = parse_positive_limit(
                instruction_event_limit,
                field_name="instruction_event_limit",
                maximum=500,
            )
            validated_order_event_limit = parse_positive_limit(
                order_event_limit,
                field_name="order_event_limit",
                maximum=500,
            )
            validated_fill_limit = parse_positive_limit(
                fill_limit,
                field_name="fill_limit",
                maximum=500,
            )
            validated_control_event_limit = parse_positive_limit(
                control_event_limit,
                field_name="control_event_limit",
                maximum=200,
            )
            validated_cancellation_limit = parse_positive_limit(
                cancellation_limit,
                field_name="cancellation_limit",
                maximum=200,
            )
            validated_reconciliation_issue_limit = parse_positive_limit(
                reconciliation_issue_limit,
                field_name="reconciliation_issue_limit",
                maximum=200,
            )
            normalized_focus_instruction_id = (
                focus_instruction_id.strip() if focus_instruction_id else None
            )
            ledger_snapshot = build_ledger_dashboard_snapshot(
                session_factory,
                focus_instruction_id=normalized_focus_instruction_id,
                instruction_event_limit=validated_instruction_event_limit,
                order_event_limit=validated_order_event_limit,
                fill_limit=validated_fill_limit,
                control_event_limit=validated_control_event_limit,
                cancellation_limit=validated_cancellation_limit,
                reconciliation_issue_limit=validated_reconciliation_issue_limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "ledger_snapshot": serialize_ledger_dashboard_snapshot(ledger_snapshot),
        }

    @app.get("/v1/settings")
    def get_settings_registry() -> dict[str, Any]:
        """Read-only view of declared non-secret runtime settings.

        There is deliberately no POST counterpart: the registry reports what the
        runtime resolved, and must not become a second way to change trading
        behaviour. Secrets are structurally excluded from the registry, so this
        response cannot carry one.
        """
        return {
            "accepted": True,
            **serialize_settings_registry(read_settings_registry(session_factory)),
        }

    @app.get("/v1/controls/kill-switch")
    def get_kill_switch() -> dict[str, Any]:
        return {
            "accepted": True,
            "kill_switch": serialize_kill_switch_status(
                read_kill_switch_state(session_factory)
            ),
        }

    @app.post("/v1/controls/kill-switch")
    def update_kill_switch(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            enabled, reason, updated_by = parse_kill_switch_payload(payload)
            result = set_kill_switch_state(
                session_factory,
                enabled=enabled,
                reason=reason,
                updated_by=updated_by,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "kill_switch": serialize_kill_switch_status(result),
        }

    @app.get("/v1/controls/broker-maintenance-mode")
    def get_broker_maintenance_mode() -> dict[str, Any]:
        return {
            "accepted": True,
            "broker_maintenance_mode": _serialize_for_json(
                read_broker_maintenance_mode_state(session_factory)
            ),
        }

    @app.post("/v1/controls/broker-maintenance-mode")
    def update_broker_maintenance_mode(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            enabled, reason, updated_by = parse_kill_switch_payload(payload)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "accepted": True,
            "broker_maintenance_mode": _serialize_for_json(
                set_broker_maintenance_mode_state(
                    session_factory,
                    enabled=enabled,
                    reason=reason,
                    updated_by=updated_by,
                )
            ),
        }

    @app.post("/v1/broker-attention/{event_id}/review")
    def review_broker_attention(
        event_id: int,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            action, updated_by, note = parse_operator_review_payload(payload)
            result = record_broker_attention_review_action(
                session_factory,
                event_id=event_id,
                action_type=action,
                updated_by=updated_by,
                note=note,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except OperatorReviewTargetNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        return {
            "accepted": True,
            "operator_review": serialize_operator_review_status(result),
        }

    @app.post("/v1/reconciliation-issues/{issue_id}/review")
    def review_reconciliation_issue(
        issue_id: int,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            action, updated_by, note = parse_operator_review_payload(payload)
            result = record_reconciliation_issue_review_action(
                session_factory,
                issue_id=issue_id,
                action_type=action,
                updated_by=updated_by,
                note=note,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except OperatorReviewTargetNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        return {
            "accepted": True,
            "operator_review": serialize_operator_review_status(result),
        }

    @app.post("/v1/reconciliation-issues/archive-open")
    def archive_open_reconciliation_issue_rows(
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            action, updated_by, note = parse_operator_review_payload(
                {**payload, "action": payload.get("action", "ARCHIVE")}
            )
            if action != "ARCHIVE":
                raise ValueError("action must be ARCHIVE for archive-open")
            result = archive_open_reconciliation_issues(
                session_factory,
                updated_by=updated_by,
                note=note,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "reconciliation_issue_archive": serialize_reconciliation_issue_archive_result(
                result
            ),
        }

    @app.post("/v1/instructions/submit")
    def submit_instruction(payload: dict[str, Any]) -> dict[str, Any]:
        intent_cleanup = None
        try:
            batch = parse_execution_batch_payload(payload)
            intent_cleanup = supersede_batch_intent_entries(
                session_factory,
                app_config.ibkr.primary_session(),
                batch,
                requested_by="instruction_submit",
                reason="Incoming instruction batch superseded older active entries.",
                timeout=10,
                canceler=cancel_order_with_primary,
                defer_blocked_positions=True,
            )
            result = submit_execution_batch(
                session_factory,
                batch,
                runtime_timezone=app_config.timezone,
                session_calendar_path=app_config.session_calendar_path,
                deferred_reentry_instruction_ids=(
                    deferred_reentry_instruction_ids_for_cleanup(batch, intent_cleanup)
                ),
            )
        except IntentReplacementConflictError as exc:
            detail: dict[str, Any] = {"message": str(exc)}
            if exc.result is not None:
                detail["intent_cleanup"] = serialize_intent_cleanup_result(exc.result)
            raise HTTPException(status_code=409, detail=detail) from exc
        except IntentCleanupSelectorError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except SubmissionConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except KillSwitchActiveError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "instruction_count": result.instruction_count,
            "runtime_timezone": app_config.timezone,
            "session_calendar_path": str(app_config.session_calendar_path),
            "submitted": serialize_submitted_batch(result),
            "intent_cleanup": (
                serialize_intent_cleanup_result(intent_cleanup)
                if intent_cleanup is not None
                else None
            ),
        }

    @app.post("/v1/instructions/intent-cleanup")
    def cleanup_instruction_intents(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            parsed = parse_intent_cleanup_payload(payload)
            result = cleanup_intent_groups(
                session_factory,
                app_config.ibkr.primary_session(),
                canceler=cancel_order_with_primary,
                **parsed,
            )
        except IntentCleanupSelectorError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except PersistedInstructionStateError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

        return {
            "accepted": True,
            "intent_cleanup": serialize_intent_cleanup_result(result),
        }

    @app.post("/v1/instructions/cancel-set")
    def cancel_instruction_batch(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            (
                requested_by,
                reason,
                batch_id,
                account_key,
                book_key,
                instruction_ids,
                timeout,
            ) = parse_instruction_set_cancellation_payload(payload)
            result = cancel_instruction_set(
                session_factory,
                app_config.ibkr.primary_session(),
                requested_by=requested_by,
                reason=reason,
                batch_id=batch_id,
                account_key=account_key,
                book_key=book_key,
                instruction_ids=instruction_ids,
                timeout=timeout,
                canceler=cancel_order_with_primary,
            )
        except InstructionSetCancellationSelectorError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except InstructionSetCancellationNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except PersistedInstructionStateError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

        return {
            "accepted": True,
            "cancelled_instruction_set": serialize_instruction_set_cancellation_result(
                result
            ),
        }

    @app.post("/v1/instructions/archive-set")
    def archive_instruction_batch(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            parsed = parse_instruction_archive_payload(payload)
            result = archive_instruction_set(session_factory, **parsed)
        except (InstructionArchiveSelectorError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "archived_instruction_set": serialize_instruction_archive_result(result),
        }

    @app.post("/v1/instructions/{instruction_id}/submit-entry")
    def submit_instruction_entry(
        instruction_id: str, timeout: int = 10
    ) -> dict[str, Any]:
        try:
            result = submit_persisted_instruction_entry(
                session_factory,
                app_config.ibkr.primary_session(),
                instruction_id,
                timeout=timeout,
                submitter=submit_order_with_primary,
            )
        except PersistedInstructionNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except PersistedInstructionStateError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except KillSwitchActiveError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(
                status_code=400, detail=broker_exception_detail(exc)
            ) from exc
        except TimeoutError as exc:
            raise HTTPException(
                status_code=504, detail=broker_exception_detail(exc)
            ) from exc

        return {
            "accepted": True,
            "mode": (
                "persisted_virtual_entry_submit"
                if result.broker_submission.get("broker_kind") == BROKER_KIND_VIRTUAL
                else "persisted_entry_submit"
            ),
            "runtime_timezone": app_config.timezone,
            "session_client_id": (
                None
                if result.broker_submission.get("broker_kind") == BROKER_KIND_VIRTUAL
                else app_config.ibkr.client_id
            ),
            "submitted_entry": serialize_persisted_broker_submission(result),
        }

    @app.post("/v1/instructions/{instruction_id}/cancel-entry")
    def cancel_instruction_entry(
        instruction_id: str, timeout: int = 10
    ) -> dict[str, Any]:
        try:
            result = cancel_persisted_instruction_entry(
                session_factory,
                app_config.ibkr.primary_session(),
                instruction_id,
                timeout=timeout,
                canceler=cancel_order_with_primary,
            )
        except PersistedInstructionNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except PersistedInstructionStateError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(
                status_code=400, detail=broker_exception_detail(exc)
            ) from exc
        except TimeoutError as exc:
            raise HTTPException(
                status_code=504, detail=broker_exception_detail(exc)
            ) from exc

        return {
            "accepted": True,
            "mode": (
                "persisted_virtual_entry_cancel"
                if result.broker_cancellation.get("broker_kind") == BROKER_KIND_VIRTUAL
                else "persisted_entry_cancel"
            ),
            "runtime_timezone": app_config.timezone,
            "session_client_id": (
                None
                if result.broker_cancellation.get("broker_kind") == BROKER_KIND_VIRTUAL
                else app_config.ibkr.client_id
            ),
            "cancelled_entry": serialize_persisted_broker_cancellation(result),
        }

    @app.post("/v1/instructions/schedule-preview")
    def preview_instruction_schedule(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            batch = parse_execution_batch_payload(payload)
            schedule = build_batch_runtime_schedule(
                batch,
                runtime_timezone=app_config.timezone,
                session_calendar_path=app_config.session_calendar_path,
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "runtime_timezone": app_config.timezone,
            "session_calendar_path": str(app_config.session_calendar_path),
            "schedule": serialize_runtime_schedule_preview(schedule),
        }

    @app.post("/v1/runtime/run-once")
    def run_runtime_cycle_once(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        request_payload = payload or {}
        try:
            now_at, timeout, instruction_ids = parse_runtime_cycle_payload(
                request_payload
            )
            result = run_runtime_cycle(
                session_factory,
                app_config.ibkr.primary_session(),
                runtime_timezone=app_config.timezone,
                session_calendar_path=app_config.session_calendar_path,
                now=now_at,
                timeout=timeout,
                instruction_ids=instruction_ids,
                entry_submitter=submit_order_with_primary,
                exit_submitter=submit_exit_with_primary,
                broker_snapshot_fetcher=fetch_reconciliation_runtime_snapshot_with_primary,
                broker_callback_fetcher=drain_broker_callbacks_with_primary,
                broker_order_canceler=cancel_order_with_primary,
                virtual_market_sync=sync_virtual_market_watch_from_stream,
                market_data_readiness_checker=market_data_readiness_checker,
                submission_lead_time=timedelta(
                    seconds=app_config.execution_runtime_submission_lead_seconds
                ),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

        return {
            "accepted": True,
            "runtime_timezone": app_config.timezone,
            "session_calendar_path": str(app_config.session_calendar_path),
            "runtime_cycle": serialize_runtime_cycle_result(result),
        }

    @app.post("/v1/runtime/startup-reconcile")
    def run_startup_reconciliation_once(
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        request_payload = payload or {}
        try:
            now_at, timeout, instruction_ids = parse_runtime_cycle_payload(
                request_payload
            )
            result = run_startup_reconciliation(
                session_factory,
                app_config.ibkr.primary_session(),
                runtime_timezone=app_config.timezone,
                session_calendar_path=app_config.session_calendar_path,
                now=now_at,
                timeout=timeout,
                instruction_ids=instruction_ids,
                exit_submitter=submit_exit_with_primary,
                broker_snapshot_fetcher=fetch_reconciliation_runtime_snapshot_with_primary,
                broker_callback_fetcher=drain_broker_callbacks_with_primary,
                broker_order_canceler=cancel_order_with_primary,
                virtual_market_sync=sync_virtual_market_watch_from_stream,
                submission_lead_time=timedelta(
                    seconds=app_config.execution_runtime_submission_lead_seconds
                ),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

        return {
            "accepted": True,
            "runtime_timezone": app_config.timezone,
            "session_calendar_path": str(app_config.session_calendar_path),
            "startup_reconciliation": serialize_runtime_cycle_result(result),
        }
