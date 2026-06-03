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



def register_rl_routes(app: Any, context: Any) -> None:
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
    fetch_reconciliation_runtime_snapshot_with_primary = context.fetch_reconciliation_runtime_snapshot_with_primary
    drain_broker_callbacks_with_primary = context.drain_broker_callbacks_with_primary
    sync_virtual_market_watch_from_stream = context.sync_virtual_market_watch_from_stream
    HTTPException = context.HTTPException
    Request = context.Request

    @app.post("/v1/rl/models/register")
    def create_trader_model(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            parsed = parse_trader_model_payload(payload)
            result = register_trader_model(session_factory, **parsed)
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TraderModelConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        return {
            "accepted": True,
            "trader_model": _serialize_for_json(asdict(result)),
        }

    @app.post("/v1/rl/models/upsert")
    def upsert_rl_trader_model(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            parsed = parse_trader_model_payload(payload)
            result = upsert_trader_model(session_factory, **parsed)
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "trader_model": _serialize_for_json(asdict(result)),
        }

    @app.put("/v1/rl/models/{model_key}")
    def update_rl_trader_model(
        model_key: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            parsed = parse_trader_model_payload({**payload, "model_key": model_key})
            result = upsert_trader_model(session_factory, **parsed)
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "trader_model": _serialize_for_json(asdict(result)),
        }

    @app.post("/v1/rl/deployments")
    def create_rl_deployment(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            parsed = parse_trader_deployment_payload(payload)
            result = create_trader_deployment(session_factory, **parsed)
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TraderModelNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except TraderDeploymentConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        return {
            "accepted": True,
            "trader_deployment": _serialize_for_json(asdict(result)),
        }

    @app.patch("/v1/rl/deployments/{deployment_key}")
    def update_rl_deployment(
        deployment_key: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            parsed = parse_trader_deployment_update_payload(payload)
            result = update_trader_deployment(
                session_factory,
                deployment_key=deployment_key,
                **parsed,
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TraderDeploymentNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        return {
            "accepted": True,
            "trader_deployment": _serialize_for_json(asdict(result)),
        }

    @app.post("/v1/rl/actions/log")
    def log_rl_action(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            parsed = parse_trader_action_payload(payload)
            result = log_trader_action(session_factory, **parsed)
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TraderDeploymentNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        return {
            "accepted": True,
            "trader_action": _serialize_for_json(asdict(result)),
        }

    @app.get("/v1/rl/candidates")
    def list_rl_candidates(
        limit: int = 100,
        deployment_key: str | None = None,
        model_key: str | None = None,
        include_expired: bool = False,
    ) -> dict[str, Any]:
        try:
            validated_limit = parse_positive_limit(
                limit,
                field_name="limit",
                maximum=500,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        normalized_model_key = (
            model_key.strip().lower() if model_key is not None else None
        )
        normalized_account_key = None
        normalized_book_key = None
        normalized_deployment_key = (
            deployment_key.strip().lower() if deployment_key is not None else None
        )
        if normalized_deployment_key:
            with session_scope(session_factory) as session:
                deployment_row = session.execute(
                    select(
                        TraderDeploymentRecord.account_key,
                        TraderDeploymentRecord.book_key,
                        TraderModelRecord.model_key,
                    )
                    .join(TraderModelRecord)
                    .where(
                        TraderDeploymentRecord.deployment_key
                        == normalized_deployment_key
                    )
                ).one_or_none()
            if deployment_row is None:
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"Trader deployment '{normalized_deployment_key}' was not found."
                    ),
                )
            normalized_account_key = deployment_row.account_key.upper()
            normalized_book_key = deployment_row.book_key.lower()
            normalized_model_key = deployment_row.model_key.lower()

        if not include_expired:
            archive_expired_rl_candidates(session_factory)
            retire_completed_rl_candidates(session_factory)

        candidates = list_instruction_statuses(
            session_factory,
            limit=500,
            state=ExecutionState.MODEL_ROUTED_PENDING.value,
            expire_after=None if include_expired else utc_now(),
        )

        def candidate_matches(candidate: Any) -> bool:
            stored_instruction = candidate.payload.get("instruction", {})
            if not isinstance(stored_instruction, dict):
                return False
            execution = stored_instruction.get("execution", {})
            if not isinstance(execution, dict):
                return False
            candidate_model_key = str(
                execution.get("model_id") or stored_instruction.get("model") or ""
            ).lower()
            if normalized_model_key and candidate_model_key != normalized_model_key:
                return False
            if (
                normalized_account_key
                and candidate.account_key.upper() != normalized_account_key
            ):
                return False
            if (
                normalized_book_key
                and candidate.book_key.lower() != normalized_book_key
            ):
                return False
            return True

        matched_candidates = tuple(
            candidate for candidate in candidates if candidate_matches(candidate)
        )[:validated_limit]
        return {
            "accepted": True,
            "candidate_count": len(matched_candidates),
            "candidates": [
                serialize_rl_candidate_status(candidate)
                for candidate in matched_candidates
            ],
        }

    @app.post("/v1/rl/candidates/archive-expired")
    def archive_expired_rl_candidate_rows(
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = payload or {}
        try:
            requested_by = str(payload.get("requested_by", "api")).strip()
            if not requested_by:
                raise ValueError("requested_by must be a non-empty string")
            reason = payload.get("reason")
            if reason is not None:
                reason = str(reason).strip() or None
            cutoff = (
                parse_datetime(payload["cutoff"], "cutoff")
                if payload.get("cutoff") is not None
                else None
            )
            limit = int(payload.get("limit", 1000))
            result = archive_expired_rl_candidates(
                session_factory,
                cutoff=cutoff,
                requested_by=requested_by,
                reason=reason
                or "Expired model-routed source candidate archived by API request.",
                limit=limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "archived_rl_candidates": serialize_rl_candidate_rollover_result(result),
        }

    @app.post("/v1/rl/actions/translate")
    def translate_rl_action_endpoint(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            parsed = parse_rl_action_translate_payload(payload)
            source_status = read_instruction_status(
                session_factory,
                parsed["source_instruction_id"],
                include_events=False,
            )
            if source_status.archived_at is not None:
                raise ValueError("source_instruction_id references an archived candidate")
            if source_status.state != ExecutionState.MODEL_ROUTED_PENDING.value:
                raise ValueError(
                    "source_instruction_id must reference a MODEL_ROUTED_PENDING instruction"
                )
            stored_payload = source_status.payload
            source_batch = parse_execution_batch_payload(
                {
                    "schema_version": stored_payload["schema_version"],
                    "source": stored_payload["source"],
                    "instructions": [stored_payload["instruction"]],
                }
            )
            source_instruction = source_batch.instructions[0]
            with session_scope(session_factory) as session:
                deployment = session.execute(
                    select(TraderDeploymentRecord)
                    .join(TraderModelRecord)
                    .where(
                        TraderDeploymentRecord.deployment_key
                        == parsed["deployment_key"]
                    )
                ).scalar_one_or_none()
                if deployment is None:
                    raise TraderDeploymentNotFoundError(
                        f"Trader deployment '{parsed['deployment_key']}' was not found."
                    )
                if (
                    source_instruction.execution is None
                    or source_instruction.execution.model_id
                    != deployment.trader_model.model_key
                ):
                    raise ValueError(
                        "source instruction model_id must match the deployment model"
                    )
                if (
                    source_instruction.account.account_key.upper()
                    != deployment.account_key.upper()
                    or source_instruction.account.book_key.lower()
                    != deployment.book_key.lower()
                ):
                    raise ValueError(
                        "source instruction account_key and book_key must match "
                        "the deployment"
                    )
                allowed_symbols = {
                    str(symbol).strip().upper()
                    for symbol in deployment.allowed_symbols_json
                }
                if (
                    allowed_symbols
                    and source_instruction.instrument.symbol.upper()
                    not in allowed_symbols
                ):
                    raise ValueError(
                        "source instruction symbol is outside deployment allowed_symbols"
                    )
            translation = translate_rl_action(
                source_batch,
                source_instruction,
                deployment_key=parsed["deployment_key"],
                action_name=parsed["action_name"],
                state_before=parsed["state_before"],
                observed_at=parsed["observed_at"],
                previous_close=parsed["previous_close"],
                decision_id=parsed["decision_id"],
            )

            submitted_batch = None
            generated_instruction_id = None
            action_execution = None
            intent_cleanup = None
            if translation.instruction_payload is not None:
                generated_instruction_id = str(
                    translation.instruction_payload["instructions"][0]["instruction_id"]
                )

            if parsed["submit"] and translation.instruction_payload is not None:
                deterministic_batch = parse_execution_batch_payload(
                    translation.instruction_payload
                )
                intent_cleanup = supersede_batch_intent_entries(
                    session_factory,
                    app_config.ibkr.primary_session(),
                    deterministic_batch,
                    requested_by="rl_action_translate",
                    reason=(
                        "RL translated entry superseded older active entries for "
                        "the same intent group."
                    ),
                    timeout=10,
                    canceler=cancel_order_with_primary,
                )
                submitted_batch = submit_execution_batch(
                    session_factory,
                    deterministic_batch,
                    runtime_timezone=app_config.timezone,
                    session_calendar_path=app_config.session_calendar_path,
                )

            if (
                parsed["submit"]
                and translation.instruction_payload is None
                and translation.action_status == ACTION_STATUS_TRANSLATED
            ):
                action_execution = execute_owned_rl_action(
                    session_factory,
                    app_config.ibkr.primary_session(),
                    source_instruction,
                    deployment_key=parsed["deployment_key"],
                    action_name=parsed["action_name"],
                    timeout=10,
                    canceler=cancel_order_with_primary,
                    exit_submitter=submit_exit_with_primary,
                )
                generated_instruction_id = action_execution.instruction_id

            action_log = None
            if parsed["log_action"]:
                action_execution_payload = (
                    serialize_rl_owned_action_execution(action_execution)
                    if action_execution is not None
                    else None
                )
                action_log = log_trader_action(
                    session_factory,
                    deployment_key=parsed["deployment_key"],
                    symbol=source_status.symbol,
                    action_name=parsed["action_name"],
                    observed_at=parsed["observed_at"],
                    state_before=parsed["state_before"],
                    state_after=(
                        action_execution.state_after
                        if action_execution is not None
                        else translation.state_after
                    ),
                    action_status=(
                        ACTION_STATUS_EXECUTED
                        if action_execution is not None
                        else translation.action_status
                    ),
                    instruction_id=generated_instruction_id,
                    payload={
                        "source_instruction_id": parsed["source_instruction_id"],
                        "source_account_key": source_instruction.account.account_key,
                        "source_book_key": source_instruction.account.book_key,
                        "decision_id": parsed["decision_id"],
                        "model_diagnostics": parsed["model_diagnostics"],
                        "submitted": (
                            submitted_batch is not None
                            or action_execution is not None
                        ),
                        "translation_note": translation.note,
                        "action_execution": action_execution_payload,
                    },
                    note=translation.note,
                )
        except InstructionStatusNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except TraderDeploymentNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except IntentReplacementConflictError as exc:
            detail: dict[str, Any] = {"message": str(exc)}
            if exc.result is not None:
                detail["intent_cleanup"] = serialize_intent_cleanup_result(exc.result)
            raise HTTPException(status_code=409, detail=detail) from exc
        except IntentCleanupSelectorError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RLActionOwnershipError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except RLActionStateError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except SubmissionConflictError as exc:
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
            "submitted": submitted_batch is not None or action_execution is not None,
            "translation": _serialize_for_json(asdict(translation)),
            "submitted_batch": (
                serialize_submitted_batch(submitted_batch)
                if submitted_batch is not None
                else None
            ),
            "intent_cleanup": (
                serialize_intent_cleanup_result(intent_cleanup)
                if intent_cleanup is not None
                else None
            ),
            "action_execution": (
                serialize_rl_owned_action_execution(action_execution)
                if action_execution is not None
                else None
            ),
            "trader_action": (
                _serialize_for_json(asdict(action_log))
                if action_log is not None
                else None
            ),
        }

    @app.post("/v1/rl/deployments/{deployment_key}/heartbeat")
    def update_rl_heartbeat(
        deployment_key: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        try:
            parsed = parse_trader_heartbeat_payload(payload)
            result = upsert_trader_heartbeat(
                session_factory,
                deployment_key=deployment_key,
                **parsed,
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TraderDeploymentNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        return {
            "accepted": True,
            "trader_heartbeat": _serialize_for_json(asdict(result)),
        }

    @app.post("/v1/rl/observations/build")
    def build_rl_observation(
        payload: dict[str, Any],
        request: FastAPIRequest,
        timeout: int = 20,
    ) -> dict[str, Any]:
        try:
            parsed = parse_rl_observation_build_payload(payload)
            with session_scope(session_factory) as session:
                deployment = session.execute(
                    select(TraderDeploymentRecord)
                    .join(TraderModelRecord)
                    .where(
                        TraderDeploymentRecord.deployment_key
                        == parsed["deployment_key"]
                    )
                ).scalar_one_or_none()
                if deployment is None:
                    raise TraderDeploymentNotFoundError(
                        f"Trader deployment '{parsed['deployment_key']}' was not found."
                    )
                model = deployment.trader_model
                deployment_snapshot = {
                    "deployment_key": deployment.deployment_key,
                    "model_key": model.model_key,
                    "model_side": model.side,
                    "action_space": list(model.action_space_json),
                    "observation_contract": dict(model.observation_contract_json),
                    "account_key": deployment.account_key,
                    "book_key": deployment.book_key,
                    "mode": deployment.mode,
                    "allowed_symbols": list(deployment.allowed_symbols_json),
                }

            source_bars = dict(parsed["source_bars"])
            requested_symbols = list(parsed["symbols"])
            if not requested_symbols:
                if source_bars:
                    requested_symbols = sorted(
                        str(symbol).strip().upper() for symbol in source_bars
                    )
                elif deployment_snapshot["allowed_symbols"]:
                    requested_symbols = list(deployment_snapshot["allowed_symbols"])
                else:
                    raise ValueError(
                        "symbols are required when the deployment has no allowed_symbols "
                        "and source_bars are not provided"
                    )
            allowed_symbols = {
                str(symbol).strip().upper()
                for symbol in deployment_snapshot["allowed_symbols"]
            }
            if allowed_symbols:
                disallowed = [
                    symbol for symbol in requested_symbols if symbol not in allowed_symbols
                ]
                if disallowed:
                    raise ValueError(
                        f"symbols are outside deployment allowed_symbols: {disallowed}"
                    )

            fetched_symbols: list[str] = []
            streamed_symbols: list[str] = []
            backfill_requests: list[dict[str, Any]] = []
            source_mode = "provided"
            config_overrides = dict(parsed["config_overrides"])
            config_overrides.setdefault(
                "min_observed_bar_coverage_ratio",
                app_config.rl_observed_bar_min_coverage_ratio,
            )
            backfill_requested_until = _completed_rl_bar_as_of(
                as_of=parsed["as_of"],
                observation_contract=deployment_snapshot["observation_contract"],
                config_overrides=config_overrides,
                fallback_timezone=app_config.timezone,
            )
            if not source_bars:
                fetch = dict(parsed["fetch"])
                fetch_mode = (
                    str(fetch.get("mode", fetch.get("source", "market_stream")))
                    .strip()
                    .lower()
                    .replace("-", "_")
                )
                if fetch_mode in {"stream", "market_stream", "live_stream"}:
                    bar_limit = int(fetch.get("bar_limit", 390))
                    if bar_limit <= 0:
                        raise ValueError("fetch.bar_limit must be positive")
                    stream_snapshot = request.app.state.market_stream_service.snapshot(
                        symbols=requested_symbols,
                        bar_limit=bar_limit,
                    )
                    stream_snapshot, _stored_bars = _merge_persisted_stream_bars(
                        session_factory,
                        stream_snapshot=stream_snapshot,
                        symbols=requested_symbols,
                        bar_limit=bar_limit,
                        as_of=parsed["as_of"],
                        timezone_name=app_config.timezone,
                    )
                    stream_bars = {
                        str(symbol).strip().upper(): bars
                        for symbol, bars in stream_snapshot["bars_by_symbol"].items()
                        if bars
                    }
                    missing_stream_bars = [
                        symbol for symbol in requested_symbols if symbol not in stream_bars
                    ]
                    instruments = (
                        fetch.get("instruments")
                        if isinstance(fetch.get("instruments"), Mapping)
                        else {}
                    )
                    backfill_missing = bool(fetch.get("backfill_missing", True))
                    session_date = parsed["as_of"].astimezone(
                        ZoneInfo(app_config.timezone)
                    ).date()
                    if missing_stream_bars and backfill_missing:
                        for symbol in missing_stream_bars:
                            backfill_requests.append(
                                enqueue_market_data_backfill_request(
                                    session_factory,
                                    symbol=symbol,
                                    trade_date=session_date.isoformat(),
                                    requested_until=backfill_requested_until,
                                    instrument=_rl_backfill_instrument(
                                        symbol=symbol,
                                        fetch=fetch,
                                        instruments=instruments,
                                    ),
                                    reason="rl_market_stream_missing_symbol_bars",
                                    duration=str(fetch.get("backfill_duration", "2 D")),
                                    bar_size=str(fetch.get("backfill_bar_size", "1 min")),
                                    what_to_show=str(
                                        fetch.get("what_to_show", "TRADES")
                                    ).upper(),
                                    use_rth=bool(fetch.get("use_rth", True)),
                                )
                            )
                    source_bars = stream_bars
                    streamed_symbols = sorted(stream_bars)
                    source_mode = "market_stream"
                    missing_request_by_symbol = {
                        str(request_payload["symbol"]).upper(): request_payload
                        for request_payload in backfill_requests
                    }
                    build_symbols = [
                        symbol for symbol in requested_symbols if symbol in source_bars
                    ]
                    if build_symbols:
                        observation = build_phase1_observation_payload(
                            deployment_key=deployment_snapshot["deployment_key"],
                            model_key=deployment_snapshot["model_key"],
                            model_side=deployment_snapshot["model_side"],
                            observation_contract=deployment_snapshot[
                                "observation_contract"
                            ],
                            action_space=deployment_snapshot["action_space"],
                            as_of=parsed["as_of"],
                            source_bars_by_symbol=source_bars,
                            symbols=build_symbols,
                            history_overrides=parsed["history_overrides"],
                            static_features_by_symbol=parsed["static_features"],
                            config_overrides=config_overrides,
                            include_source_bars=parsed["include_source_bars"],
                        )
                    else:
                        observation = {
                            "deployment_key": deployment_snapshot["deployment_key"],
                            "model_key": deployment_snapshot["model_key"],
                            "model_side": str(
                                deployment_snapshot["model_side"]
                            ).upper(),
                            "action_space": [
                                str(action).lower()
                                for action in deployment_snapshot["action_space"]
                            ],
                            "as_of": parsed["as_of"].astimezone(
                                ZoneInfo(app_config.timezone)
                            ).isoformat(),
                            "symbols": [],
                            "input_contract": {
                                "bar_family": "phase1_intraday_ohlc_v1",
                                "bar_interval": "5m",
                                "refresh_cadence": "1m",
                                "update_cadence": "1m",
                                "decision_cadence": "5m",
                                "decision_policy": "completed_5m_bar_only",
                                "bar_sequence_policy": "observed_provider_bars_only",
                                "source_adapter": (
                                    "ibkr_1m_trades_to_phase1_5m_ohlc_v1"
                                ),
                                "source_bar_interval": "1m",
                                "session_timezone": app_config.timezone,
                                "min_observed_bar_coverage_ratio": (
                                    config_overrides[
                                        "min_observed_bar_coverage_ratio"
                                    ]
                                ),
                                "growing_day_prefix": True,
                            },
                            "feature_schema": {},
                            "market_context": None,
                            "observations": {},
                        }
                    for symbol in missing_stream_bars:
                        observation["observations"][symbol] = (
                            _paused_market_stream_observation(
                                symbol=symbol,
                                as_of=parsed["as_of"].astimezone(
                                    ZoneInfo(app_config.timezone)
                                ),
                                session_date=session_date,
                                reason="paused_market_stream_bars_missing_backfill_pending",
                                backfill_request=missing_request_by_symbol.get(symbol),
                                min_coverage_ratio=float(
                                    config_overrides[
                                        "min_observed_bar_coverage_ratio"
                                    ]
                                ),
                            )
                        )
                    observation["symbols"] = requested_symbols
                    for symbol, symbol_observation in observation[
                        "observations"
                    ].items():
                        decision = symbol_observation.get("model_decision", {})
                        if (
                            isinstance(decision, Mapping)
                            and decision.get("reason")
                            == "paused_observed_bar_coverage_below_threshold"
                            and backfill_missing
                        ):
                            backfill_request = enqueue_market_data_backfill_request(
                                session_factory,
                                symbol=symbol,
                                trade_date=session_date.isoformat(),
                                requested_until=backfill_requested_until,
                                instrument=_rl_backfill_instrument(
                                    symbol=symbol,
                                    fetch=fetch,
                                    instruments=instruments,
                                ),
                                reason="rl_observed_bar_coverage_below_threshold",
                                duration=str(fetch.get("backfill_duration", "2 D")),
                                bar_size=str(fetch.get("backfill_bar_size", "1 min")),
                                what_to_show=str(
                                    fetch.get("what_to_show", "TRADES")
                                ).upper(),
                                use_rth=bool(fetch.get("use_rth", True)),
                            )
                            backfill_requests.append(backfill_request)
                            data_quality = symbol_observation.setdefault(
                                "data_quality",
                                {},
                            )
                            data_quality["backfill_request"] = backfill_request
                            decision["backfill_status"] = backfill_request["status"]
                            decision["backfill_request"] = backfill_request
                            if isinstance(decision.get("data_quality"), Mapping):
                                decision["data_quality"] = {
                                    **decision["data_quality"],
                                    "backfill_request": backfill_request,
                                }
                elif fetch_mode in {"historical", "historical_bars", "ibkr_historical_bars"}:
                    for symbol in requested_symbols:
                        query = HistoricalBarsQuery(
                            symbol=symbol,
                            security_type=str(fetch.get("security_type", "STK")).upper(),
                            exchange=str(fetch.get("exchange", "SMART")).upper(),
                            currency=str(fetch.get("currency", "SEK")).upper(),
                            primary_exchange=(
                                str(fetch["primary_exchange"]).upper()
                                if fetch.get("primary_exchange") is not None
                                else "SFB"
                            ),
                            isin=(
                                str(fetch["isin"])
                                if fetch.get("isin") is not None
                                else None
                            ),
                            duration=str(fetch.get("duration", "25 D")),
                            bar_size=str(fetch.get("bar_size", "1 min")),
                            what_to_show=str(fetch.get("what_to_show", "TRADES")).upper(),
                            use_rth=bool(fetch.get("use_rth", True)),
                            end_at=parsed["as_of"],
                        )
                        result = with_historical_session(
                            "rl_observation_historical_bars",
                            lambda broker_app, query=query: read_historical_bars(
                                app_config.ibkr.historical_session(),
                                query,
                                timeout=timeout,
                                app=broker_app,
                            ),
                        )
                        source_bars[symbol] = result["bars"]
                        fetched_symbols.append(symbol)
                    source_mode = "ibkr_historical_bars"
                else:
                    raise ValueError(
                        "fetch.mode must be market_stream or historical_bars"
                    )
            if source_mode != "market_stream":
                observation = build_phase1_observation_payload(
                    deployment_key=deployment_snapshot["deployment_key"],
                    model_key=deployment_snapshot["model_key"],
                    model_side=deployment_snapshot["model_side"],
                    observation_contract=deployment_snapshot["observation_contract"],
                    action_space=deployment_snapshot["action_space"],
                    as_of=parsed["as_of"],
                    source_bars_by_symbol=source_bars,
                    symbols=requested_symbols,
                    history_overrides=parsed["history_overrides"],
                    static_features_by_symbol=parsed["static_features"],
                    config_overrides=config_overrides,
                    include_source_bars=parsed["include_source_bars"],
                )
        except TraderDeploymentNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except (KeyError, ValueError) as exc:
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
            "source_mode": source_mode,
            "fetched_symbols": fetched_symbols,
            "streamed_symbols": streamed_symbols,
            "backfill_request_count": len(backfill_requests),
            "backfill_requests": _serialize_for_json(backfill_requests),
            "account_key": deployment_snapshot["account_key"],
            "book_key": deployment_snapshot["book_key"],
            "mode": deployment_snapshot["mode"],
            "rl_observation": _serialize_for_json(observation),
        }

    @app.get("/v1/read/rl-dashboard")
    def get_rl_dashboard(
        model_limit: int = 40,
        deployment_limit: int = 40,
        action_limit: int = 120,
        candidate_limit: int = 40,
        heartbeat_stale_after_seconds: int = 120,
        include_expired_candidates: bool = False,
    ) -> dict[str, Any]:
        try:
            validated_model_limit = parse_positive_limit(
                model_limit,
                field_name="model_limit",
                maximum=500,
            )
            validated_deployment_limit = parse_positive_limit(
                deployment_limit,
                field_name="deployment_limit",
                maximum=500,
            )
            validated_action_limit = parse_positive_limit(
                action_limit,
                field_name="action_limit",
                maximum=1000,
            )
            validated_candidate_limit = parse_positive_limit(
                candidate_limit,
                field_name="candidate_limit",
                maximum=500,
            )
            validated_stale_after_seconds = parse_positive_limit(
                heartbeat_stale_after_seconds,
                field_name="heartbeat_stale_after_seconds",
                maximum=86400,
            )
            if not include_expired_candidates:
                archive_expired_rl_candidates(session_factory)
                retire_completed_rl_candidates(session_factory)
            snapshot = build_rl_trader_dashboard_snapshot(
                session_factory,
                model_limit=validated_model_limit,
                deployment_limit=validated_deployment_limit,
                action_limit=validated_action_limit,
                heartbeat_stale_after_seconds=validated_stale_after_seconds,
            )
            rl_candidates = list_instruction_statuses(
                session_factory,
                limit=validated_candidate_limit,
                state=ExecutionState.MODEL_ROUTED_PENDING.value,
                model_routed=True,
                expire_after=None if include_expired_candidates else utc_now(),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        dashboard_payload = serialize_rl_trader_dashboard_snapshot(snapshot)
        dashboard_payload["candidates"] = [
            serialize_rl_candidate_status(candidate)
            for candidate in rl_candidates
        ]
        dashboard_payload["summary"]["candidate_count"] = len(rl_candidates)
        candidate_runtime = {
            "queued_candidate_count": len(rl_candidates),
            "active_candidate_count": 0,
            "bar_ready_candidate_count": 0,
            "stream_any_bar_candidate_count": 0,
            "fresh_decision_bar_candidate_count": 0,
            "stale_decision_bar_candidate_count": 0,
            "not_ready_candidate_count": 0,
            "already_processed_candidate_count": 0,
            "evaluated_candidate_count": 0,
            "backfilled_symbol_count": 0,
        }
        for deployment in dashboard_payload.get("deployments", []):
            heartbeat = deployment.get("heartbeat") if isinstance(deployment, dict) else None
            metrics = heartbeat.get("metrics") if isinstance(heartbeat, dict) else None
            if not isinstance(metrics, dict):
                continue
            candidate_runtime["active_candidate_count"] += int(
                metrics.get("active_candidate_count") or 0
            )
            candidate_runtime["bar_ready_candidate_count"] += int(
                metrics.get("stream_bar_ready_candidate_count") or 0
            )
            candidate_runtime["stream_any_bar_candidate_count"] += int(
                metrics.get("stream_any_bar_candidate_count") or 0
            )
            candidate_runtime["fresh_decision_bar_candidate_count"] += int(
                metrics.get("fresh_decision_bar_candidate_count") or 0
            )
            candidate_runtime["stale_decision_bar_candidate_count"] += int(
                metrics.get("stale_decision_bar_candidate_count") or 0
            )
            candidate_runtime["not_ready_candidate_count"] += int(
                metrics.get("not_ready_candidate_count") or 0
            )
            candidate_runtime["already_processed_candidate_count"] += int(
                metrics.get("already_processed_candidate_count") or 0
            )
            candidate_runtime["evaluated_candidate_count"] += int(
                metrics.get("evaluated_candidate_count") or 0
            )
            candidate_runtime["backfilled_symbol_count"] += int(
                metrics.get("backfilled_symbol_count") or 0
            )
        dashboard_payload["summary"].update(candidate_runtime)

        return {
            "accepted": True,
            "rl_dashboard": dashboard_payload,
        }

    @app.get("/v1/rl/runtime-state")
    def get_rl_runtime_state(
        deployment_key: str,
        symbols: str | None = None,
    ) -> dict[str, Any]:
        try:
            runtime_state = build_rl_runtime_state_snapshot(
                session_factory,
                deployment_key=deployment_key,
                symbols=parse_market_stream_symbols(symbols) or None,
            )
        except TraderDeploymentNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "runtime_state": runtime_state,
        }


