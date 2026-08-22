from __future__ import annotations

import argparse
import json
import logging
from contextlib import asynccontextmanager
from dataclasses import asdict
from dataclasses import replace
from datetime import datetime
from datetime import time
from datetime import timedelta
from datetime import timezone
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
from ibkr_trader.db.models import MarketStreamBarRecord
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
from ibkr_trader.ibkr.market_stream import LiveMarketDataStreamService
from ibkr_trader.ibkr.recovery_policy import (
    assess_broker_session_payload,
    assess_market_stream_payload,
)
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
from ibkr_trader.ibkr.shortability import shortability_meta_dir
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
    broker_maintenance_mode_is_enabled,
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


def _parse_stream_bar_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    raw_value = str(value).strip()
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _stream_quote_for_symbol(stream_snapshot: dict[str, Any], symbol: str) -> dict[str, Any] | None:
    quotes = stream_snapshot.get("quotes")
    if not isinstance(quotes, list):
        return None
    normalized_symbol = symbol.strip().upper()
    for quote in quotes:
        if not isinstance(quote, dict):
            continue
        if str(quote.get("symbol") or "").strip().upper() == normalized_symbol:
            return quote
    return None


def _latest_stream_bar_for_symbol(
    stream_snapshot: dict[str, Any],
    symbol: str,
) -> dict[str, Any] | None:
    bars_by_symbol = stream_snapshot.get("bars_by_symbol")
    if not isinstance(bars_by_symbol, dict):
        return None
    bars = bars_by_symbol.get(symbol)
    if not isinstance(bars, list) or not bars:
        return None
    for bar in reversed(bars):
        if isinstance(bar, dict):
            return bar
    return None


def _enrich_omxs30_snapshot_previous_close(
    session_factory: Any,
    *,
    stream_snapshot: dict[str, Any],
    symbols: list[str] | None,
    timezone_name: str,
) -> None:
    requested_symbols = {str(symbol).strip().upper() for symbol in symbols or []}
    if requested_symbols and "OMXS30" not in requested_symbols:
        return

    quote = _stream_quote_for_symbol(stream_snapshot, "OMXS30")
    if quote is not None and quote.get("close_price") not in (None, ""):
        return

    latest_bar = _latest_stream_bar_for_symbol(stream_snapshot, "OMXS30")
    if latest_bar is None:
        return
    latest_timestamp = _parse_stream_bar_timestamp(latest_bar.get("timestamp"))
    if latest_timestamp is None:
        return

    local_zone = ZoneInfo(timezone_name)
    local_latest = latest_timestamp.astimezone(local_zone)
    session_open = datetime.combine(
        local_latest.date(),
        time(9, 0),
        tzinfo=local_zone,
    )

    with session_scope(session_factory) as session:
        previous_bar = session.execute(
            select(MarketStreamBarRecord)
            .where(
                MarketStreamBarRecord.symbol == "OMXS30",
                MarketStreamBarRecord.started_at < session_open,
            )
            .order_by(MarketStreamBarRecord.started_at.desc())
            .limit(1)
        ).scalar_one_or_none()

    if previous_bar is None:
        return
    previous_bar_started_at = previous_bar.started_at
    if previous_bar_started_at.tzinfo is None:
        previous_bar_started_at = previous_bar_started_at.replace(tzinfo=timezone.utc)
    previous_local = previous_bar_started_at.astimezone(local_zone)
    previous_age_days = (local_latest.date() - previous_local.date()).days
    if previous_age_days <= 0 or previous_age_days > 7:
        return

    quotes = stream_snapshot.get("quotes")
    if not isinstance(quotes, list):
        quotes = []
        stream_snapshot["quotes"] = quotes
    if quote is None:
        quote = {
            "symbol": "OMXS30",
            "exchange": latest_bar.get("exchange") or "OMS",
            "currency": latest_bar.get("currency") or "SEK",
            "security_type": "IND",
            "primary_exchange": "",
        }
        quotes.append(quote)

    quote.setdefault("last_price", latest_bar.get("close"))
    quote["close_price"] = previous_bar.close_price
    quote.setdefault("updated_at", latest_bar.get("timestamp"))
    quote.setdefault("last_trade_at", latest_bar.get("timestamp"))


def _build_recovery_policy_payload(
    *,
    session_statuses: dict[str, Any],
    circuit_snapshot: dict[str, Any],
    market_stream_service: Any,
    session_factory: Any,
    maintenance_reader: Any = broker_maintenance_mode_is_enabled,
) -> dict[str, Any]:
    """Derive the named recovery and stream states for ``/healthz``.

    Read-only reporting: this classifies state that already exists, and never
    changes broker, stream or runtime behaviour.

    If broker maintenance mode cannot be read, the broker states are reported as
    ``unknown`` (which withholds all authority) and the read error is surfaced
    rather than swallowed. Health reporting must not take the whole endpoint down,
    but it also must not report a state it could not actually establish.
    """
    # Left as None when it could not be read. Emitting False would be a default
    # that looks valid: a caller reading only this field would conclude "not in
    # maintenance" for a state that was never established.
    maintenance_mode: bool | None = None
    maintenance_error: str | None = None
    try:
        maintenance_mode = maintenance_reader(session_factory)
    except Exception as exc:  # noqa: BLE001 - reported, not silently defaulted
        maintenance_error = f"{type(exc).__name__}: {exc}"
        LOGGER.warning("Broker maintenance mode could not be read: %s", exc)

    sessions: dict[str, Any] = {}
    for role, status_payload in (session_statuses or {}).items():
        if maintenance_error is not None:
            sessions[role] = assess_broker_session_payload(None).to_payload()
            sessions[role]["reason"] = (
                "Broker maintenance mode could not be read, so recovery state "
                f"could not be established: {maintenance_error}"
            )
            continue
        sessions[role] = assess_broker_session_payload(
            status_payload,
            circuit_payload=circuit_snapshot,
            maintenance_mode=bool(maintenance_mode),
        ).to_payload()

    stream_error: str | None = None
    try:
        stream_health = market_stream_service.health_snapshot()
    except Exception as exc:  # noqa: BLE001 - reported, not silently defaulted
        stream_health = None
        stream_error = f"{type(exc).__name__}: {exc}"
        LOGGER.warning("Market stream health snapshot failed: %s", exc)

    return {
        "broker_sessions": sessions,
        "market_stream": assess_market_stream_payload(stream_health).to_payload(),
        "market_stream_error": stream_error,
        "maintenance_mode": maintenance_mode,
        "maintenance_mode_error": maintenance_error,
    }


def register_broker_market_routes(app: Any, context: Any) -> None:
    app_config = context.app_config
    session_factory = context.session_factory
    broker_sessions = context.broker_sessions
    broker_circuit = context.broker_circuit
    broker_pacing_governor = context.broker_pacing_governor
    broker_monitor = context.broker_monitor
    market_stream_service = context.market_stream_service
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
    collect_stockholm_intraday_backfill = context.collect_stockholm_intraday_backfill
    collect_tick_stream_sample = context.collect_tick_stream_sample
    HTTPException = context.HTTPException
    Request = context.Request

    @app.get("/healthz")
    def healthz(refresh_broker_status: bool = False) -> dict[str, Any]:
        if (
            refresh_broker_status
            and app_config.broker_monitor_enabled
            and app_config.environment != "test"
        ):
            broker_monitor.request_cycle_if_due(
                min_interval_seconds=app_config.broker_status_refresh_min_interval_seconds
            )
        session_statuses = broker_sessions.status_snapshot(blocking=False)
        circuit_snapshot = broker_circuit.snapshot()
        return {
            "status": "ok",
            "local_only": app_config.api.require_loopback_only,
            "api_host": app_config.api.host,
            "api_port": app_config.api.port,
            "runtime_timezone": app_config.timezone,
            "session_calendar_path": str(app_config.session_calendar_path),
            "recovery_policy": _build_recovery_policy_payload(
                session_statuses=session_statuses,
                circuit_snapshot=circuit_snapshot,
                market_stream_service=market_stream_service,
                session_factory=session_factory,
            ),
            "broker_sessions": session_statuses,
            "broker_circuit": circuit_snapshot,
            "broker_pacing": broker_pacing_governor.snapshot(),
            "ibgateway": read_ibgateway_diagnostics(),
            "broker_operations": broker_sessions.activity_tracker.snapshot(recent_limit=10),
            "broker_monitor": serialize_broker_monitor_status(broker_monitor.status()),
            "execution_runtime": (
                execution_runtime.status()
                or serialize_runtime_service_status(
                    read_runtime_service_status(
                        session_factory,
                        runtime_key=EXECUTION_RUNTIME_KEY,
                    )
                )
            ),
        }

    @app.get("/v1/ibkr/telemetry")
    def get_ibkr_telemetry(recent_limit: int = 50) -> dict[str, Any]:
        if recent_limit <= 0:
            raise HTTPException(status_code=400, detail="recent_limit must be positive")
        if recent_limit > 200:
            raise HTTPException(status_code=400, detail="recent_limit must be at most 200")
        return {
            "accepted": True,
            "telemetry": broker_sessions.telemetry_snapshot(recent_limit=recent_limit),
            "circuit": broker_circuit.snapshot(),
            "pacing": broker_pacing_governor.snapshot(),
        }

    @app.post("/v1/ibkr/probe")
    def run_ibkr_probe(timeout: int = 5, force: bool = False) -> dict[str, Any]:
        try:
            result = with_diagnostic_session(
                "probe",
                lambda broker_app: probe_gateway(
                    app_config.ibkr.diagnostic_session(),
                    timeout=timeout,
                    app=broker_app,
                ),
                ignore_cooldown=force,
            )
        except IbkrDependencyError as exc:
            LOGGER.warning(
                "IBKR probe failed: timeout=%s force=%s error=%s",
                timeout,
                force,
                exc,
            )
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            LOGGER.warning(
                "IBKR probe failed: timeout=%s force=%s error=%s",
                timeout,
                force,
                exc,
            )
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except TimeoutError as exc:
            LOGGER.warning(
                "IBKR probe failed: timeout=%s force=%s error=%s",
                timeout,
                force,
                exc,
            )
            raise HTTPException(status_code=504, detail=str(exc)) from exc

        return json.loads(result.to_json())

    @app.post("/v1/contracts/resolve")
    def resolve_ibkr_contract(payload: dict[str, Any], timeout: int = 10) -> dict[str, Any]:
        try:
            query = parse_contract_resolve_payload(payload)
            result = with_diagnostic_session(
                "contract_resolve",
                lambda broker_app: resolve_contracts(
                    app_config.ibkr.diagnostic_session(),
                    query,
                    timeout=timeout,
                    app=broker_app,
                )
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=broker_exception_detail(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=broker_exception_detail(exc)) from exc

        return serialize_contract_resolve_result(result)

    @app.post("/v1/accounts/summary")
    def get_account_summary(payload: dict[str, Any] | None = None, timeout: int = 10) -> dict[str, Any]:
        request_payload = payload or {}
        try:
            tags, group, account_id = parse_account_summary_payload(request_payload)
            return with_diagnostic_session(
                "account_summary",
                lambda broker_app: read_account_summary(
                    app_config.ibkr.diagnostic_session(),
                    tags=tags,
                    group=group,
                    account_id=account_id,
                    timeout=timeout,
                    app=broker_app,
                )
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=broker_exception_detail(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=broker_exception_detail(exc)) from exc

    @app.get("/v1/broker/runtime-snapshot")
    def get_broker_runtime_snapshot(timeout: int = 20) -> dict[str, Any]:
        try:
            snapshot = with_primary_session(
                "broker_runtime_snapshot",
                lambda broker_app: fetch_broker_runtime_snapshot(
                    app_config.ibkr.primary_session(),
                    timeout=timeout,
                    app=broker_app,
                )
            )
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=broker_exception_detail(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=broker_exception_detail(exc)) from exc

        return {
            "accepted": True,
            "session_client_id": app_config.ibkr.client_id,
            "visibility_limits": {
                "live_broker_open_orders_only": True,
                "untransmitted_tws_orders_visible_via_api": False,
                "note": (
                    "IBKR does not expose untransmitted TWS-local orders through the "
                    "normal open-order API path while they remain untransmitted."
                ),
            },
            "broker_runtime": serialize_broker_runtime_snapshot(snapshot),
        }

    @app.post("/v1/market-data/historical-bars")
    def get_historical_bars(payload: dict[str, Any], timeout: int = 20) -> dict[str, Any]:
        try:
            query = parse_historical_bars_payload(payload)
            return with_historical_session(
                "historical_bars",
                lambda broker_app: read_historical_bars(
                    app_config.ibkr.historical_session(),
                    query,
                    timeout=timeout,
                    app=broker_app,
                    negative_contract_cache_ttl_seconds=(
                        app_config.ibkr_negative_contract_cache_ttl_seconds
                    ),
                )
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=broker_exception_detail(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=broker_exception_detail(exc)) from exc

    @app.post("/v1/market-data/stockholm-intraday-backfill")
    def get_stockholm_intraday_backfill(
        payload: dict[str, Any],
        timeout: int = 20,
    ) -> dict[str, Any]:
        try:
            query = parse_stockholm_intraday_backfill_payload(payload)
            result = with_historical_session(
                "stockholm_intraday_backfill",
                lambda broker_app: collect_stockholm_intraday_backfill(
                    app_config.ibkr.historical_session(),
                    query,
                    instruments_path=app_config.stockholm_instruments_path,
                    identity_path=app_config.stockholm_identity_path,
                    timeout=timeout,
                    app=broker_app,
                    negative_contract_cache_ttl_seconds=(
                        app_config.ibkr_negative_contract_cache_ttl_seconds
                    ),
                ),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

        return {
            "accepted": True,
            "session_client_id": app_config.ibkr.diagnostic_client_id,
            "market": "stockholm",
            "series_mode": "paged_batch",
            **result,
        }

    @app.post("/v1/market-data/tick-stream-sample")
    def get_tick_stream_sample(payload: dict[str, Any], timeout: int = 15) -> dict[str, Any]:
        try:
            query = parse_tick_stream_payload(payload)
            return collect_tick_stream_sample(
                app_config.ibkr.streaming_session(),
                query,
                timeout=timeout,
            )
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

    @app.post("/v1/market-data/stream/subscribe")
    def subscribe_market_data_stream(
        payload: dict[str, Any],
        request: FastAPIRequest,
    ) -> dict[str, Any]:
        try:
            parsed = parse_market_stream_subscribe_payload(
                payload,
                stockholm_identity_map=request.app.state.market_stream_identity_map,
                max_contracts=app_config.effective_market_stream_max_subscriptions,
            )
            snapshot = request.app.state.market_stream_service.subscribe_many(
                parsed["contracts"],
                replace=parsed["replace"],
                market_data_type=parsed["market_data_type"],
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        return {
            "accepted": True,
            "mode": "streaming_market_data",
            "session_client_id": app_config.ibkr.streaming_client_id,
            "stream": snapshot,
        }

    @app.post("/v1/market-data/stream/desired")
    def set_market_data_stream_desired(
        payload: dict[str, Any],
        request: FastAPIRequest,
    ) -> dict[str, Any]:
        try:
            parsed = parse_market_stream_subscribe_payload(
                payload,
                stockholm_identity_map=request.app.state.market_stream_identity_map,
                max_contracts=app_config.effective_market_stream_max_subscriptions,
            )
            snapshot = request.app.state.market_stream_service.set_desired_many(
                parsed["contracts"],
                replace=parsed["replace"],
                market_data_type=parsed["market_data_type"],
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        return {
            "accepted": True,
            "mode": "streaming_market_data_desired",
            "session_client_id": app_config.ibkr.streaming_client_id,
            "stream": snapshot,
        }

    @app.get("/v1/market-data/stream/snapshot")
    def get_market_data_stream_snapshot(
        request: FastAPIRequest,
        symbols: str | None = None,
        bar_limit: int = 390,
    ) -> dict[str, Any]:
        if bar_limit <= 0:
            raise HTTPException(status_code=400, detail="bar_limit must be positive")
        if bar_limit > 2000:
            raise HTTPException(status_code=400, detail="bar_limit must be at most 2000")
        snapshot = request.app.state.market_stream_service.snapshot(
            symbols=parse_market_stream_symbols(symbols),
            bar_limit=bar_limit,
        )
        snapshot, _stored_bars = _merge_persisted_stream_bars(
            session_factory,
            stream_snapshot=snapshot,
            symbols=parse_market_stream_symbols(symbols),
            bar_limit=bar_limit,
            as_of=utc_now(),
            timezone_name=app_config.timezone,
        )
        _enrich_omxs30_snapshot_previous_close(
            session_factory,
            stream_snapshot=snapshot,
            symbols=parse_market_stream_symbols(symbols),
            timezone_name=app_config.timezone,
        )
        return {
            "accepted": True,
            "mode": "streaming_market_data",
            "stream": snapshot,
        }

    @app.post("/v1/market-data/stream/stop")
    def stop_market_data_stream(request: FastAPIRequest) -> dict[str, Any]:
        request.app.state.market_stream_service.stop()
        return {
            "accepted": True,
            "mode": "streaming_market_data_stopped",
        }

    @app.post("/v1/market-data/shortability-snapshot")
    def get_shortability_snapshot(
        payload: dict[str, Any] | None = None,
        timeout: int = 120,
    ) -> dict[str, Any]:
        request_payload = payload or {}
        try:
            query = parse_shortability_snapshot_payload(request_payload)
            snapshot = collect_shortability_snapshot(
                app_config.ibkr.streaming_session(),
                query,
                instruments_path=app_config.stockholm_instruments_path,
                identity_path=app_config.stockholm_identity_path,
                timeout=timeout,
            )
            persist_requested = bool(
                request_payload.get(
                    "persist",
                    query.symbols is None and query.max_symbols is None,
                )
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

        persisted_artifacts = None
        if persist_requested:
            persisted_artifacts = persist_shortability_snapshot(
                snapshot,
                # This service's own outputs go under its own root. Deriving
                # them from a resolved q-data path would write into an
                # immutable, versioned dataset directory q-data owns.
                instruments_dir=app_config.output_root,
                meta_dir=shortability_meta_dir(app_config.output_root),
            )

        return {
            "accepted": True,
            "session_client_id": (
                app_config.ibkr.streaming_client_id
                if query.source == ShortabilitySource.BROKER_TICKS
                else None
            ),
            "stockholm_instruments_path": str(app_config.stockholm_instruments_path),
            "persisted_artifacts": persisted_artifacts,
            "shortability_snapshot": snapshot,
        }

    @app.post("/v1/orders/preview")
    def preview_orders(payload: dict[str, Any], timeout: int = 10) -> dict[str, Any]:
        try:
            batch = parse_execution_batch_payload(payload)
            return with_diagnostic_session(
                "order_preview",
                lambda broker_app: preview_execution_batch(
                    app_config.ibkr.diagnostic_session(),
                    batch,
                    timeout=timeout,
                    app=broker_app,
                )
            )
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

    @app.post("/v1/orders/submit")
    def submit_order(payload: dict[str, Any], timeout: int = 10) -> dict[str, Any]:
        try:
            batch = parse_execution_batch_payload(payload)
            if (
                len(batch.instructions) == 1
                and is_virtual_account_key(batch.instructions[0].account.account_key)
            ):
                result = submit_virtual_entry_order(
                    session_factory,
                    app_config.ibkr.primary_session(),
                    batch.instructions[0],
                    timeout=timeout,
                )
            else:
                result = with_primary_session(
                    "order_submit",
                    lambda broker_app: submit_order_from_batch(
                        app_config.ibkr.primary_session(),
                        batch,
                        timeout=timeout,
                        app=broker_app,
                    )
                )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=broker_exception_detail(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=broker_exception_detail(exc)) from exc

        return {
            "accepted": True,
            "mode": (
                "manual_virtual_submit"
                if result.get("broker_kind") == BROKER_KIND_VIRTUAL
                else "manual_broker_submit"
            ),
            "runtime_timezone": app_config.timezone,
            "session_client_id": (
                None
                if result.get("broker_kind") == BROKER_KIND_VIRTUAL
                else app_config.ibkr.client_id
            ),
            "submitted_order": result,
        }

    @app.post("/v1/orders/{order_id}/cancel")
    def cancel_order(order_id: int, timeout: int = 10) -> dict[str, Any]:
        ledger_warning: str | None = None
        try:
            result = cancel_order_with_primary(
                app_config.ibkr.primary_session(),
                order_id,
                timeout=timeout,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=broker_exception_detail(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=broker_exception_detail(exc)) from exc

        broker_status = result.get("broker_order_status")
        if isinstance(broker_status, dict):
            try:
                persist_broker_order_cancellation_result(
                    session_factory,
                    broker_kind=str(result.get("broker_kind") or BROKER_KIND_IBKR),
                    broker_cancellation=result,
                    observed_at=utc_now(),
                    fallback_account_key=(
                        str(result["account"])
                        if result.get("account") not in (None, "")
                        else app_config.ibkr.account_id or None
                    ),
                    event_type=(
                        "manual_virtual_order_cancelled"
                        if result.get("broker_kind") == BROKER_KIND_VIRTUAL
                        else "manual_broker_order_cancelled"
                    ),
                    note=(
                        "Persisted manual virtual-order cancellation from the API."
                        if result.get("broker_kind") == BROKER_KIND_VIRTUAL
                        else "Persisted manual broker-order cancellation from the API."
                    ),
                )
            except ValueError as exc:
                ledger_warning = str(exc)

        response = {
            "accepted": True,
            "mode": (
                "manual_virtual_cancel"
                if result.get("broker_kind") == BROKER_KIND_VIRTUAL
                else "manual_broker_cancel"
            ),
            "session_client_id": (
                None
                if result.get("broker_kind") == BROKER_KIND_VIRTUAL
                else app_config.ibkr.client_id
            ),
            "order_id": order_id,
            "cancelled_order": result,
        }
        if ledger_warning is not None:
            response["ledger_warning"] = ledger_warning
        return response

    @app.post("/v1/instructions/validate")
    def validate_instruction(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            batch = parse_execution_batch_payload(payload)
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "instruction_count": len(batch.instructions),
            "batch": serialize_execution_batch(batch),
        }

    @app.get("/v1/instructions")
    def list_instructions(
        limit: int = 100,
        state: str | None = None,
        include_archived: bool = False,
        model_routed: bool | None = None,
    ) -> dict[str, Any]:
        if limit <= 0:
            raise HTTPException(status_code=400, detail="limit must be positive")
        if limit > 500:
            raise HTTPException(status_code=400, detail="limit must be at most 500")

        normalized_state = state.strip().upper() if state is not None else None
        instructions = list_instruction_statuses(
            session_factory,
            limit=limit,
            state=normalized_state,
            include_archived=include_archived,
            model_routed=model_routed,
        )
        return {
            "accepted": True,
            "instruction_count": len(instructions),
            "instructions": [
                serialize_instruction_status(instruction) for instruction in instructions
            ],
        }

    @app.get("/v1/instructions/{instruction_id}")
    def get_instruction_status(
        instruction_id: str,
        include_events: bool = True,
    ) -> dict[str, Any]:
        try:
            result = read_instruction_status(
                session_factory,
                instruction_id,
                include_events=include_events,
            )
        except InstructionStatusNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        return {
            "accepted": True,
            "instruction": serialize_instruction_status(result),
        }

    @app.post("/v1/virtual/accounts")
    def create_virtual_account(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            parsed = parse_virtual_account_payload(payload)
            result = ensure_virtual_account_record(session_factory, **parsed)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "virtual_account": result,
        }

    @app.post("/v1/virtual/market-watch")
    def update_virtual_market_watch(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            parsed = parse_virtual_market_quote_payload(payload)
            result = record_virtual_market_quote(session_factory, **parsed)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "virtual_market_watch": result,
        }

    @app.get("/v1/virtual/market-watch")
    def get_virtual_market_watch(
        account_key: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        try:
            validated_limit = parse_positive_limit(
                limit,
                field_name="limit",
                maximum=1000,
            )
            normalized_account_key = (
                normalize_virtual_account_key(account_key)
                if account_key is not None
                else None
            )
            quotes = list_virtual_market_quotes(
                session_factory,
                account_key=normalized_account_key,
                limit=validated_limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "quote_count": len(quotes),
            "quotes": list(quotes),
        }
