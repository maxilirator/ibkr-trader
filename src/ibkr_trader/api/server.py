from __future__ import annotations

import argparse
import ipaddress
import json
from contextlib import asynccontextmanager
from dataclasses import asdict
from dataclasses import replace
from datetime import date, datetime
from datetime import timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping

from sqlalchemy import or_
from sqlalchemy import select

try:
    from fastapi import Request as FastAPIRequest
except ModuleNotFoundError:  # pragma: no cover - server extra is optional locally.
    FastAPIRequest = Any  # type: ignore[misc,assignment]

from ibkr_trader.api.broker_monitor import BrokerMonitorService
from ibkr_trader.api.broker_monitor import serialize_broker_monitor_status
from ibkr_trader.config import AppConfig
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import TraderDeploymentRecord
from ibkr_trader.db.models import TraderModelRecord
from ibkr_trader.domain.contract_resolution import ContractResolveQuery
from ibkr_trader.domain.execution_contract import (
    ExecutionInstructionBatch,
)
from ibkr_trader.domain.execution_payloads import parse_datetime
from ibkr_trader.domain.execution_payloads import parse_decimal
from ibkr_trader.domain.execution_payloads import parse_date
from ibkr_trader.domain.execution_payloads import parse_execution_batch_payload
from ibkr_trader.ibkr.account_summary import (
    DEFAULT_ACCOUNT_SUMMARY_TAGS,
    read_account_summary,
)
from ibkr_trader.ibkr.contracts import (
    resolve_contracts,
    serialize_contract_resolve_result,
)
from ibkr_trader.ibkr.errors import IbkrDependencyError
from ibkr_trader.ibkr.historical_bars import HistoricalBarsQuery, read_historical_bars
from ibkr_trader.ibkr.market_stream import LiveMarketDataStreamService
from ibkr_trader.ibkr.market_stream import MarketStreamContract
from ibkr_trader.ibkr.order_execution import cancel_broker_order
from ibkr_trader.ibkr.order_execution import submit_order_from_batch
from ibkr_trader.ibkr.order_execution import submit_order_from_instruction
from ibkr_trader.ibkr.order_execution import submit_exit_order_from_instruction
from ibkr_trader.ibkr.order_preview import preview_execution_batch
from ibkr_trader.ibkr.probe import probe_gateway
from ibkr_trader.ibkr.runtime_snapshot import (
    fetch_broker_runtime_snapshot,
    serialize_broker_runtime_snapshot,
)
from ibkr_trader.ibkr.shortability import (
    ShortabilityMarketDataType,
    ShortabilitySource,
    ShortabilitySnapshotQuery,
    collect_shortability_snapshot,
    load_stockholm_identity_map,
    persist_shortability_snapshot,
)
from ibkr_trader.ibkr.stockholm_intraday import (
    DEFAULT_STOCKHOLM_INTRADAY_TYPES,
    StockholmIntradayBackfillQuery,
    collect_stockholm_intraday_backfill,
)
from ibkr_trader.ibkr.tick_stream import TickStreamQuery
from ibkr_trader.ibkr.tick_stream import _normalize_tick_type
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
    record_broker_attention_review_action,
    record_reconciliation_issue_review_action,
    serialize_operator_review_status,
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
from ibkr_trader.virtual.execution import submit_virtual_entry_order
from ibkr_trader.virtual.execution import submit_virtual_exit_order


class ApiDependencyError(RuntimeError):
    """Raised when optional API server dependencies are unavailable."""


_BACKGROUND_RECOVERY_INSTRUCTION_STATES = {
    ExecutionState.ENTRY_SUBMITTED.value,
    ExecutionState.POSITION_OPEN.value,
    ExecutionState.EXIT_PENDING.value,
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


def is_loopback_host(host: str | None) -> bool:
    if not host:
        return False
    if host == "localhost":
        return True

    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def enforce_loopback_binding(host: str, *, require_loopback_only: bool) -> None:
    if require_loopback_only and not is_loopback_host(host):
        raise ValueError(
            "API host must be loopback when API_REQUIRE_LOOPBACK_ONLY is enabled."
        )


def parse_positive_limit(
    value: int,
    *,
    field_name: str,
    maximum: int,
) -> int:
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")
    if value > maximum:
        raise ValueError(f"{field_name} must be at most {maximum}")
    return value


def parse_contract_resolve_payload(payload: Mapping[str, Any]) -> ContractResolveQuery:
    query = ContractResolveQuery(
        symbol=str(payload["symbol"]).upper(),
        security_type=str(payload.get("security_type", "STK")).upper(),
        exchange=str(payload["exchange"]).upper(),
        currency=str(payload["currency"]).upper(),
        primary_exchange=(
            str(payload["primary_exchange"]).upper()
            if payload.get("primary_exchange") is not None
            else None
        ),
        local_symbol=(
            str(payload["local_symbol"])
            if payload.get("local_symbol") is not None
            else None
        ),
        include_expired=bool(payload.get("include_expired", False)),
        isin=str(payload["isin"]) if payload.get("isin") is not None else None,
    )
    query.validate()
    return query


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
                    BrokerOrderRecord.status.not_in(
                        tuple(_BACKGROUND_RECOVERY_CLOSED_ORDER_STATUSES)
                    ),
                )
            )
            .limit(1)
        ).first()
        return unsettled_order is not None


def parse_account_summary_payload(payload: Mapping[str, Any]) -> tuple[tuple[str, ...], str, str | None]:
    raw_tags = payload.get("tags")
    if raw_tags is None:
        tags = DEFAULT_ACCOUNT_SUMMARY_TAGS
    else:
        if not isinstance(raw_tags, list) or not raw_tags:
            raise ValueError("tags must be a non-empty array of strings")
        tags = tuple(str(tag) for tag in raw_tags)
        if not all(tag for tag in tags):
            raise ValueError("tags must contain only non-empty strings")

    group = str(payload.get("group", "All"))
    account_id = (
        str(payload["account_id"])
        if payload.get("account_id") is not None
        else None
    )
    return tags, group, account_id


def parse_historical_bars_payload(payload: Mapping[str, Any]) -> HistoricalBarsQuery:
    end_at = (
        parse_datetime(payload["end_at"], "end_at")
        if payload.get("end_at") is not None
        else None
    )
    query = HistoricalBarsQuery(
        symbol=str(payload["symbol"]).upper(),
        security_type=str(payload.get("security_type", "STK")).upper(),
        exchange=str(payload["exchange"]).upper(),
        currency=str(payload["currency"]).upper(),
        primary_exchange=(
            str(payload["primary_exchange"]).upper()
            if payload.get("primary_exchange") is not None
            else None
        ),
        local_symbol=(
            str(payload["local_symbol"])
            if payload.get("local_symbol") is not None
            else None
        ),
        isin=str(payload["isin"]) if payload.get("isin") is not None else None,
        duration=str(payload["duration"]),
        bar_size=str(payload["bar_size"]),
        what_to_show=str(payload.get("what_to_show", "TRADES")).upper(),
        use_rth=bool(payload.get("use_rth", True)),
        end_at=end_at,
    )
    query.validate()
    return query


def parse_stockholm_intraday_backfill_payload(
    payload: Mapping[str, Any],
) -> StockholmIntradayBackfillQuery:
    if payload.get("as_of_date") is None:
        raise ValueError("as_of_date is required")

    as_of_date = parse_date(payload["as_of_date"], "as_of_date")

    raw_what_to_show = payload.get("what_to_show")
    if raw_what_to_show is None:
        what_to_show = DEFAULT_STOCKHOLM_INTRADAY_TYPES
    else:
        if not isinstance(raw_what_to_show, list) or not raw_what_to_show:
            raise ValueError("what_to_show must be a non-empty array of strings")
        what_to_show = tuple(str(item).strip().upper() for item in raw_what_to_show)
        if not all(what_to_show):
            raise ValueError("what_to_show must contain only non-empty strings")
        if len(set(what_to_show)) != len(what_to_show):
            raise ValueError("what_to_show must not contain duplicates")

    raw_symbols = payload.get("symbols")
    symbols: tuple[str, ...] | None = None
    if raw_symbols is not None:
        if not isinstance(raw_symbols, list) or not raw_symbols:
            raise ValueError("symbols must be a non-empty array of strings")
        parsed_symbols = tuple(str(item).strip().lower() for item in raw_symbols)
        if not all(parsed_symbols):
            raise ValueError("symbols must contain only non-empty strings")
        if len(set(parsed_symbols)) != len(parsed_symbols):
            raise ValueError("symbols must not contain duplicates")
        symbols = parsed_symbols

    raw_max_runtime_seconds = payload.get("max_runtime_seconds", 55.0)
    query = StockholmIntradayBackfillQuery(
        as_of_date=as_of_date,
        bar_size=str(payload.get("bar_size", "1 min")),
        what_to_show=what_to_show,
        use_rth=bool(payload.get("use_rth", True)),
        max_symbols=int(payload.get("max_symbols", 25)),
        start_after=(
            str(payload["start_after"]).strip().lower()
            if payload.get("start_after") is not None
            else None
        ),
        symbols=symbols,
        include_remapped=bool(payload.get("include_remapped", False)),
        sleep_seconds=float(payload.get("sleep_seconds", 0.05)),
        max_runtime_seconds=(
            None
            if raw_max_runtime_seconds is None
            else float(raw_max_runtime_seconds)
        ),
    )
    query.validate()
    return query


def _parse_string_list(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    required: bool = False,
    normalize: Any | None = None,
) -> tuple[str, ...]:
    raw_value = payload.get(field_name)
    if raw_value is None:
        if required:
            raise ValueError(f"{field_name} is required")
        return ()
    if not isinstance(raw_value, list) or not raw_value:
        raise ValueError(f"{field_name} must be a non-empty array of strings")
    values: list[str] = []
    seen: set[str] = set()
    for item in raw_value:
        value = str(item).strip()
        if normalize is not None:
            value = normalize(value)
        if not value:
            raise ValueError(f"{field_name} must contain only non-empty strings")
        if value in seen:
            continue
        seen.add(value)
        values.append(value)
    return tuple(values)


def _parse_json_object_field(
    payload: Mapping[str, Any],
    field_name: str,
) -> dict[str, Any]:
    raw_value = payload.get(field_name)
    if raw_value is None:
        return {}
    if not isinstance(raw_value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return dict(raw_value)


def _parse_required_string(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    normalize: Any | None = None,
) -> str:
    value = str(payload.get(field_name, "")).strip()
    if normalize is not None:
        value = normalize(value)
    if not value:
        raise ValueError(f"{field_name} is required")
    return value


def parse_trader_model_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    model_key = _parse_required_string(
        payload,
        "model_key",
        normalize=lambda value: value.lower(),
    )
    display_name = _parse_required_string(payload, "display_name")
    strategy_family = _parse_required_string(payload, "strategy_family")
    side = _parse_required_string(
        payload,
        "side",
        normalize=lambda value: value.upper(),
    )
    action_space = _parse_string_list(
        payload,
        "action_space",
        required=True,
        normalize=lambda value: value.lower(),
    )
    return {
        "model_key": model_key,
        "display_name": display_name,
        "strategy_family": strategy_family,
        "side": side,
        "source_workflow_path": (
            str(payload["source_workflow_path"]).strip()
            if payload.get("source_workflow_path") is not None
            else None
        ),
        "promoted_checkpoint_path": (
            str(payload["promoted_checkpoint_path"]).strip()
            if payload.get("promoted_checkpoint_path") is not None
            else None
        ),
        "action_space": action_space,
        "observation_contract": _parse_json_object_field(
            payload,
            "observation_contract",
        ),
        "execution_mapping_version": (
            str(payload["execution_mapping_version"]).strip()
            if payload.get("execution_mapping_version") is not None
            else None
        ),
        "metadata": _parse_json_object_field(payload, "metadata"),
    }


def parse_trader_deployment_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    account_key = _parse_required_string(
        payload,
        "account_key",
        normalize=lambda value: value.upper(),
    )
    return {
        "deployment_key": _parse_required_string(
            payload,
            "deployment_key",
            normalize=lambda value: value.lower(),
        ),
        "model_key": _parse_required_string(
            payload,
            "model_key",
            normalize=lambda value: value.lower(),
        ),
        "account_key": account_key,
        "book_key": _parse_required_string(
            payload,
            "book_key",
            normalize=lambda value: value.lower(),
        ),
        "mode": _parse_required_string(
            payload,
            "mode",
            normalize=lambda value: value.lower(),
        ),
        "status": _parse_required_string(
            payload,
            "status",
            normalize=lambda value: value.lower(),
        ),
        "allowed_symbols": _parse_string_list(
            payload,
            "allowed_symbols",
            normalize=lambda value: value.upper(),
        ),
        "risk_limits": _parse_json_object_field(payload, "risk_limits"),
        "action_constraints": _parse_json_object_field(payload, "action_constraints"),
        "metadata": _parse_json_object_field(payload, "metadata"),
    }


def _parse_optional_string_list_update(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    normalize: Any | None = None,
) -> tuple[str, ...]:
    raw_value = payload.get(field_name)
    if raw_value is None:
        return ()
    if not isinstance(raw_value, list):
        raise ValueError(f"{field_name} must be an array of strings")
    values: list[str] = []
    seen: set[str] = set()
    for item in raw_value:
        value = str(item).strip()
        if normalize is not None:
            value = normalize(value)
        if not value:
            raise ValueError(f"{field_name} must contain only non-empty strings")
        if value in seen:
            continue
        seen.add(value)
        values.append(value)
    return tuple(values)


def parse_trader_deployment_update_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    if "account_key" in payload:
        parsed["account_key"] = _parse_required_string(
            payload,
            "account_key",
            normalize=lambda value: value.upper(),
        )
    if "book_key" in payload:
        parsed["book_key"] = _parse_required_string(
            payload,
            "book_key",
            normalize=lambda value: value.lower(),
        )
    if "mode" in payload:
        parsed["mode"] = _parse_required_string(
            payload,
            "mode",
            normalize=lambda value: value.lower(),
        )
    if "status" in payload:
        parsed["status"] = _parse_required_string(
            payload,
            "status",
            normalize=lambda value: value.lower(),
        )
    if "allowed_symbols" in payload:
        parsed["allowed_symbols"] = _parse_optional_string_list_update(
            payload,
            "allowed_symbols",
            normalize=lambda value: value.upper(),
        )
    if "risk_limits" in payload:
        parsed["risk_limits"] = _parse_json_object_field(payload, "risk_limits")
    if "action_constraints" in payload:
        parsed["action_constraints"] = _parse_json_object_field(
            payload,
            "action_constraints",
        )
    if "metadata" in payload:
        parsed["metadata"] = _parse_json_object_field(payload, "metadata")
    if not parsed:
        raise ValueError("at least one deployment field is required")
    return parsed


def parse_trader_action_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    observed_at = parse_datetime(
        _parse_required_string(payload, "observed_at"),
        "observed_at",
    )
    return {
        "deployment_key": _parse_required_string(
            payload,
            "deployment_key",
            normalize=lambda value: value.lower(),
        ),
        "symbol": _parse_required_string(
            payload,
            "symbol",
            normalize=lambda value: value.upper(),
        ),
        "action_name": _parse_required_string(
            payload,
            "action_name",
            normalize=lambda value: value.lower(),
        ),
        "observed_at": observed_at,
        "state_before": (
            str(payload["state_before"]).strip().upper()
            if payload.get("state_before") is not None
            else None
        ),
        "state_after": (
            str(payload["state_after"]).strip().upper()
            if payload.get("state_after") is not None
            else None
        ),
        "action_status": _parse_required_string(
            payload,
            "action_status",
            normalize=lambda value: value.lower(),
        ),
        "instruction_id": (
            str(payload["instruction_id"]).strip()
            if payload.get("instruction_id") is not None
            else None
        ),
        "payload": _parse_json_object_field(payload, "payload"),
        "note": str(payload["note"]).strip() if payload.get("note") is not None else None,
    }


def parse_rl_action_translate_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    observed_at = (
        parse_datetime(payload["observed_at"], "observed_at")
        if payload.get("observed_at") is not None
        else utc_now()
    )
    previous_close = (
        parse_decimal(payload["previous_close"], "previous_close")
        if payload.get("previous_close") is not None
        else None
    )
    decision_id = (
        str(payload["decision_id"]).strip()
        if payload.get("decision_id") is not None
        else None
    )
    if decision_id == "":
        decision_id = None
    return {
        "deployment_key": _parse_required_string(
            payload,
            "deployment_key",
            normalize=lambda value: value.lower(),
        ),
        "source_instruction_id": _parse_required_string(
            payload,
            "source_instruction_id",
        ),
        "action_name": _parse_required_string(
            payload,
            "action_name",
            normalize=lambda value: value.lower(),
        ),
        "state_before": str(payload.get("state_before", "FLAT")).strip().upper(),
        "observed_at": observed_at,
        "previous_close": previous_close,
        "decision_id": decision_id,
        "submit": bool(payload.get("submit", False)),
        "log_action": bool(payload.get("log_action", False)),
    }


def parse_trader_heartbeat_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    last_seen_at = parse_datetime(
        _parse_required_string(payload, "last_seen_at"),
        "last_seen_at",
    )
    last_bar_at = (
        parse_datetime(payload["last_bar_at"], "last_bar_at")
        if payload.get("last_bar_at") is not None
        else None
    )
    last_action_at = (
        parse_datetime(payload["last_action_at"], "last_action_at")
        if payload.get("last_action_at") is not None
        else None
    )
    return {
        "status": _parse_required_string(
            payload,
            "status",
            normalize=lambda value: value.lower(),
        ),
        "last_seen_at": last_seen_at,
        "last_bar_at": last_bar_at,
        "last_action_at": last_action_at,
        "runtime_error": (
            str(payload["runtime_error"]).strip()
            if payload.get("runtime_error") is not None
            else None
        ),
        "metrics": _parse_json_object_field(payload, "metrics"),
    }


def parse_rl_observation_build_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw_source_bars = payload.get("source_bars")
    if raw_source_bars is None:
        source_bars: dict[str, Any] = {}
    elif isinstance(raw_source_bars, Mapping):
        source_bars = dict(raw_source_bars)
    else:
        raise ValueError("source_bars must be an object keyed by symbol")

    raw_history_overrides = (
        payload.get("history_overrides")
        if payload.get("history_overrides") is not None
        else payload.get("history_features")
    )
    if raw_history_overrides is None:
        history_overrides: dict[str, Any] = {}
    elif isinstance(raw_history_overrides, Mapping):
        history_overrides = dict(raw_history_overrides)
    else:
        raise ValueError("history_overrides must be an object keyed by symbol")

    raw_static_features = (
        payload.get("static_features")
        if payload.get("static_features") is not None
        else payload.get("static_features_by_symbol")
    )
    if raw_static_features is None:
        static_features: dict[str, Any] = {}
    elif isinstance(raw_static_features, Mapping):
        static_features = dict(raw_static_features)
    else:
        raise ValueError("static_features must be an object keyed by symbol")

    raw_fetch = payload.get("fetch", {})
    if not isinstance(raw_fetch, Mapping):
        raise ValueError("fetch must be an object")

    return {
        "deployment_key": _parse_required_string(
            payload,
            "deployment_key",
            normalize=lambda value: value.lower(),
        ),
        "symbols": _parse_string_list(
            payload,
            "symbols",
            normalize=lambda value: value.upper(),
        ),
        "as_of": (
            parse_datetime(payload["as_of"], "as_of")
            if payload.get("as_of") is not None
            else utc_now()
        ),
        "source_bars": source_bars,
        "history_overrides": history_overrides,
        "static_features": static_features,
        "config_overrides": _parse_json_object_field(payload, "observation"),
        "include_source_bars": bool(payload.get("include_source_bars", False)),
        "fetch": dict(raw_fetch),
    }


def parse_virtual_account_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    account_key = normalize_virtual_account_key(str(payload.get("account_key", "")))
    base_currency = _parse_required_string(
        payload,
        "base_currency",
        normalize=lambda value: value.upper(),
    )
    return {
        "account_key": account_key,
        "base_currency": base_currency,
        "account_label": (
            str(payload["account_label"]).strip()
            if payload.get("account_label") is not None
            else None
        ),
        "cash_balance": (
            parse_decimal(payload["cash_balance"], "cash_balance")
            if payload.get("cash_balance") is not None
            else None
        ),
    }


def parse_virtual_market_quote_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    observed_at = parse_datetime(
        _parse_required_string(payload, "observed_at"),
        "observed_at",
    )
    parsed_prices = {
        "bid_price": (
            parse_decimal(payload["bid_price"], "bid_price")
            if payload.get("bid_price") is not None
            else None
        ),
        "ask_price": (
            parse_decimal(payload["ask_price"], "ask_price")
            if payload.get("ask_price") is not None
            else None
        ),
        "last_price": (
            parse_decimal(payload["last_price"], "last_price")
            if payload.get("last_price") is not None
            else None
        ),
        "midpoint_price": (
            parse_decimal(payload["midpoint_price"], "midpoint_price")
            if payload.get("midpoint_price") is not None
            else None
        ),
    }
    if all(value is None for value in parsed_prices.values()):
        raise ValueError(
            "At least one of bid_price, ask_price, last_price, or midpoint_price is required"
        )
    symbol = _parse_required_string(
        payload,
        "symbol",
        normalize=lambda value: value.upper(),
    )
    exchange = _parse_required_string(
        payload,
        "exchange",
        normalize=lambda value: value.upper(),
    )
    currency = _parse_required_string(
        payload,
        "currency",
        normalize=lambda value: value.upper(),
    )
    security_type = _parse_required_string(
        payload,
        "security_type",
        normalize=lambda value: value.upper(),
    )

    return {
        "account_key": normalize_virtual_account_key(
            str(payload.get("account_key", ""))
        ),
        "symbol": symbol,
        "exchange": exchange,
        "currency": currency,
        "security_type": security_type,
        "observed_at": observed_at,
        "primary_exchange": (
            str(payload["primary_exchange"]).strip().upper()
            if payload.get("primary_exchange") is not None
            else None
        ),
        "local_symbol": (
            str(payload["local_symbol"]).strip()
            if payload.get("local_symbol") is not None
            else None
        ),
        **parsed_prices,
        "source": str(payload["source"]).strip() if payload.get("source") is not None else None,
        "raw_payload": dict(payload),
        "metadata": _parse_json_object_field(payload, "metadata"),
    }


def parse_runtime_cycle_payload(
    payload: Mapping[str, Any],
) -> tuple[datetime | None, int, tuple[str, ...] | None]:
    now_at = (
        parse_datetime(payload["now_at"], "now_at")
        if payload.get("now_at") is not None
        else None
    )
    timeout = int(payload.get("timeout", 10))
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    raw_instruction_ids = payload.get("instruction_ids")
    instruction_ids: tuple[str, ...] | None = None
    if raw_instruction_ids is not None:
        if not isinstance(raw_instruction_ids, list) or not raw_instruction_ids:
            raise ValueError("instruction_ids must be a non-empty array of strings")
        parsed_instruction_ids = tuple(str(item).strip() for item in raw_instruction_ids)
        if not all(parsed_instruction_ids):
            raise ValueError("instruction_ids must contain only non-empty strings")
        if len(set(parsed_instruction_ids)) != len(parsed_instruction_ids):
            raise ValueError("instruction_ids must not contain duplicates")
        instruction_ids = parsed_instruction_ids
    return now_at, timeout, instruction_ids


def parse_kill_switch_payload(payload: Mapping[str, Any]) -> tuple[bool, str | None, str]:
    if "enabled" not in payload:
        raise ValueError("enabled is required")
    enabled = payload["enabled"]
    if not isinstance(enabled, bool):
        raise ValueError("enabled must be a boolean")

    reason = payload.get("reason")
    if reason is not None:
        reason = str(reason).strip()
        if not reason:
            reason = None

    updated_by = str(payload.get("updated_by", "api")).strip()
    if not updated_by:
        raise ValueError("updated_by must be a non-empty string")

    return enabled, reason, updated_by


def parse_operator_review_payload(
    payload: Mapping[str, Any],
) -> tuple[str, str, str | None]:
    if "action" not in payload:
        raise ValueError("action is required")

    action = str(payload["action"]).strip()
    if not action:
        raise ValueError("action must be a non-empty string")

    updated_by = str(payload.get("updated_by", "api")).strip()
    if not updated_by:
        raise ValueError("updated_by must be a non-empty string")

    note = payload.get("note")
    if note is not None:
        note = str(note).strip()
        if not note:
            note = None

    return action, updated_by, note


def parse_instruction_set_cancellation_payload(
    payload: Mapping[str, Any],
) -> tuple[str, str | None, str | None, str | None, str | None, tuple[str, ...] | None, int]:
    requested_by = str(payload.get("requested_by", "api")).strip()
    if not requested_by:
        raise ValueError("requested_by must be a non-empty string")

    reason = payload.get("reason")
    if reason is not None:
        reason = str(reason).strip()
        if not reason:
            reason = None

    batch_id = str(payload["batch_id"]).strip() if payload.get("batch_id") is not None else None
    account_key = (
        str(payload["account_key"]).strip() if payload.get("account_key") is not None else None
    )
    book_key = str(payload["book_key"]).strip() if payload.get("book_key") is not None else None

    raw_instruction_ids = payload.get("instruction_ids")
    instruction_ids: tuple[str, ...] | None = None
    if raw_instruction_ids is not None:
        if not isinstance(raw_instruction_ids, list) or not raw_instruction_ids:
            raise ValueError("instruction_ids must be a non-empty array of strings")
        normalized_instruction_ids = tuple(str(item).strip() for item in raw_instruction_ids)
        if not all(normalized_instruction_ids):
            raise ValueError("instruction_ids must contain only non-empty strings")
        if len(set(normalized_instruction_ids)) != len(normalized_instruction_ids):
            raise ValueError("instruction_ids must not contain duplicates")
        instruction_ids = normalized_instruction_ids

    timeout = int(payload.get("timeout", 10))
    if timeout <= 0:
        raise ValueError("timeout must be positive")

    return requested_by, reason, batch_id, account_key, book_key, instruction_ids, timeout


def parse_instruction_archive_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    requested_by = str(payload.get("requested_by", "api")).strip()
    if not requested_by:
        raise ValueError("requested_by must be a non-empty string")

    reason = payload.get("reason")
    if reason is not None:
        reason = str(reason).strip() or None

    raw_instruction_ids = payload.get("instruction_ids")
    instruction_ids: tuple[str, ...] | None = None
    if raw_instruction_ids is not None:
        if not isinstance(raw_instruction_ids, list) or not raw_instruction_ids:
            raise ValueError("instruction_ids must be a non-empty array of strings")
        parsed_instruction_ids = tuple(str(item).strip() for item in raw_instruction_ids)
        if not all(parsed_instruction_ids):
            raise ValueError("instruction_ids must contain only non-empty strings")
        if len(set(parsed_instruction_ids)) != len(parsed_instruction_ids):
            raise ValueError("instruction_ids must not contain duplicates")
        instruction_ids = parsed_instruction_ids

    raw_states = payload.get("states")
    states: tuple[str, ...] | None = None
    if raw_states is not None:
        if not isinstance(raw_states, list) or not raw_states:
            raise ValueError("states must be a non-empty array of strings")
        states = tuple(str(item).strip().upper() for item in raw_states)
        if not all(states):
            raise ValueError("states must contain only non-empty strings")

    expire_before = (
        parse_datetime(payload["expire_before"], "expire_before")
        if payload.get("expire_before") is not None
        else None
    )
    limit = int(payload.get("limit", 500))
    include_active = bool(payload.get("include_active", False))
    model_routed = payload.get("model_routed")
    if model_routed is not None and not isinstance(model_routed, bool):
        raise ValueError("model_routed must be a boolean when provided")

    return {
        "requested_by": requested_by,
        "reason": reason,
        "instruction_ids": instruction_ids,
        "states": states,
        "batch_id": (
            str(payload["batch_id"]).strip()
            if payload.get("batch_id") is not None
            else None
        ),
        "account_key": (
            str(payload["account_key"]).strip()
            if payload.get("account_key") is not None
            else None
        ),
        "book_key": (
            str(payload["book_key"]).strip()
            if payload.get("book_key") is not None
            else None
        ),
        "source_system": (
            str(payload["source_system"]).strip()
            if payload.get("source_system") is not None
            else None
        ),
        "model_routed": model_routed,
        "expire_before": expire_before,
        "include_active": include_active,
        "limit": limit,
    }


def parse_tick_stream_payload(payload: Mapping[str, Any]) -> TickStreamQuery:
    raw_tick_types = payload.get("tick_types", ["Last", "BidAsk"])
    if not isinstance(raw_tick_types, list) or not raw_tick_types:
        raise ValueError("tick_types must be a non-empty array of strings")

    query = TickStreamQuery(
        symbol=str(payload["symbol"]).upper(),
        security_type=str(payload.get("security_type", "STK")).upper(),
        exchange=str(payload["exchange"]).upper(),
        currency=str(payload["currency"]).upper(),
        primary_exchange=(
            str(payload["primary_exchange"]).upper()
            if payload.get("primary_exchange") is not None
            else None
        ),
        local_symbol=(
            str(payload["local_symbol"])
            if payload.get("local_symbol") is not None
            else None
        ),
        isin=str(payload["isin"]) if payload.get("isin") is not None else None,
        tick_types=tuple(_normalize_tick_type(item) for item in raw_tick_types),
        duration_seconds=float(payload.get("duration_seconds", 5.0)),
        max_events=int(payload.get("max_events", 500)),
        ignore_size=bool(payload.get("ignore_size", False)),
    )
    query.validate()
    return query


def _identity_lookup_key(symbol: str) -> str:
    return symbol.strip().upper()


def _identity_value(identity: Any, field_name: str) -> str | None:
    if identity is None:
        return None
    raw_value = getattr(identity, field_name, None)
    if raw_value is None:
        return None
    value = str(raw_value).strip()
    return value or None


def _market_stream_contract_for_symbol(
    *,
    symbol: str,
    security_type: str,
    exchange: str,
    currency: str,
    primary_exchange: str | None,
    local_symbol: str | None,
    isin: str | None,
    stockholm_identity_map: Mapping[str, Any] | None,
) -> MarketStreamContract:
    normalized_symbol = symbol.strip().upper()
    identity = (stockholm_identity_map or {}).get(_identity_lookup_key(normalized_symbol))
    enriched_local_symbol = local_symbol or _identity_value(identity, "ticker_alias")
    enriched_isin = isin or _identity_value(identity, "isin")
    return MarketStreamContract(
        symbol=normalized_symbol,
        security_type=security_type,
        exchange=exchange,
        currency=currency,
        primary_exchange=primary_exchange,
        local_symbol=enriched_local_symbol,
        isin=enriched_isin,
    )


def parse_market_stream_subscribe_payload(
    payload: Mapping[str, Any],
    *,
    stockholm_identity_map: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    raw_contracts = payload.get("contracts") or payload.get("instruments")
    raw_symbols = payload.get("symbols")
    contracts: list[MarketStreamContract] = []

    if raw_contracts is not None:
        if not isinstance(raw_contracts, list) or not raw_contracts:
            raise ValueError("contracts must be a non-empty array")
        for item in raw_contracts:
            if not isinstance(item, Mapping):
                raise ValueError("contracts entries must be objects")
            local_symbol = (
                str(item["local_symbol"]).strip()
                if item.get("local_symbol") is not None
                else None
            )
            isin = str(item["isin"]).strip() if item.get("isin") is not None else None
            contract = _market_stream_contract_for_symbol(
                symbol=str(item.get("symbol", "")).strip().upper(),
                security_type=str(item.get("security_type", "STK")).strip().upper(),
                exchange=str(item.get("exchange", payload.get("exchange", "SMART"))).strip().upper(),
                currency=str(item.get("currency", payload.get("currency", "SEK"))).strip().upper(),
                primary_exchange=(
                    str(item["primary_exchange"]).strip().upper()
                    if item.get("primary_exchange") is not None
                    else (
                        str(payload["primary_exchange"]).strip().upper()
                        if payload.get("primary_exchange") is not None
                        else "SFB"
                    )
                ),
                local_symbol=local_symbol,
                isin=isin,
                stockholm_identity_map=stockholm_identity_map,
            )
            contract.validate()
            contracts.append(contract)
    else:
        if not isinstance(raw_symbols, list) or not raw_symbols:
            raise ValueError("symbols must be a non-empty array of strings")
        symbols = tuple(str(symbol).strip().upper() for symbol in raw_symbols)
        if not all(symbols):
            raise ValueError("symbols must contain only non-empty strings")
        if len(set(symbols)) != len(symbols):
            raise ValueError("symbols must not contain duplicates")
        contracts = [
            _market_stream_contract_for_symbol(
                symbol=symbol,
                security_type=str(payload.get("security_type", "STK")).strip().upper(),
                exchange=str(payload.get("exchange", "SMART")).strip().upper(),
                currency=str(payload.get("currency", "SEK")).strip().upper(),
                primary_exchange=(
                    str(payload["primary_exchange"]).strip().upper()
                    if payload.get("primary_exchange") is not None
                    else "SFB"
                ),
                local_symbol=None,
                isin=None,
                stockholm_identity_map=stockholm_identity_map,
            )
            for symbol in symbols
        ]

    if len(contracts) > 100:
        raise ValueError("market stream subscriptions are limited to 100 symbols")

    market_data_type = (
        str(payload["market_data_type"]).strip().upper()
        if payload.get("market_data_type") is not None
        else None
    )
    return {
        "contracts": contracts,
        "replace": bool(payload.get("replace", True)),
        "market_data_type": market_data_type,
    }


def parse_market_stream_symbols(raw_value: str | None) -> list[str] | None:
    if raw_value is None or not raw_value.strip():
        return None
    symbols = [
        item.strip().upper()
        for item in raw_value.replace("\n", ",").split(",")
        if item.strip()
    ]
    return sorted(set(symbols)) or None


def parse_shortability_snapshot_payload(
    payload: Mapping[str, Any],
) -> ShortabilitySnapshotQuery:
    raw_symbols = payload.get("symbols")
    symbols: tuple[str, ...] | None = None
    if raw_symbols is not None:
        if not isinstance(raw_symbols, list) or not raw_symbols:
            raise ValueError("symbols must be a non-empty array of strings")
        symbols = tuple(str(symbol).strip().upper() for symbol in raw_symbols)
        if not all(symbols):
            raise ValueError("symbols must contain only non-empty strings")
        if len(set(symbols)) != len(symbols):
            raise ValueError("symbols must not contain duplicates")

    raw_market_data_type = str(payload.get("market_data_type", "LIVE")).strip().upper()
    normalized_market_data_type = raw_market_data_type.replace("-", "_").replace(" ", "_")
    try:
        market_data_type = ShortabilityMarketDataType(normalized_market_data_type)
    except ValueError as exc:
        raise ValueError(
            "market_data_type must be one of LIVE, FROZEN, DELAYED, DELAYED_FROZEN"
        ) from exc

    raw_source = str(
        payload.get("source", ShortabilitySource.OFFICIAL_IBKR_PAGE.value)
    ).strip()
    normalized_source = raw_source.upper().replace("-", "_").replace(" ", "_")
    source_aliases = {
        "OFFICIAL": ShortabilitySource.OFFICIAL_IBKR_PAGE,
        "OFFICIAL_PAGE": ShortabilitySource.OFFICIAL_IBKR_PAGE,
        "OFFICIAL_IBKR_PAGE": ShortabilitySource.OFFICIAL_IBKR_PAGE,
        "BROKER": ShortabilitySource.BROKER_TICKS,
        "BROKER_TICK": ShortabilitySource.BROKER_TICKS,
        "BROKER_TICKS": ShortabilitySource.BROKER_TICKS,
    }
    source = source_aliases.get(normalized_source)
    if source is None:
        raise ValueError("source must be OFFICIAL_IBKR_PAGE or BROKER_TICKS")

    query = ShortabilitySnapshotQuery(
        symbols=symbols,
        as_of_date=(
            parse_date(payload["as_of_date"], "as_of_date")
            if payload.get("as_of_date") is not None
            else None
        ),
        exchange=str(payload.get("exchange", "SMART")).upper(),
        primary_exchange=str(payload.get("primary_exchange", "SFB")).upper(),
        currency=str(payload.get("currency", "SEK")).upper(),
        security_type=str(payload.get("security_type", "STK")).upper(),
        source=source,
        only_shortable=bool(payload.get("only_shortable", True)),
        market_data_type=market_data_type,
        per_symbol_timeout_seconds=float(payload.get("per_symbol_timeout_seconds", 2.0)),
        max_concurrent=int(payload.get("max_concurrent", 25)),
        max_symbols=(
            int(payload["max_symbols"])
            if payload.get("max_symbols") is not None
            else None
        ),
    )
    query.validate()
    return query


def _serialize_for_json(payload: Any) -> Any:
    if isinstance(payload, Enum):
        return payload.value
    if isinstance(payload, Decimal):
        return str(payload)
    if isinstance(payload, datetime):
        return payload.isoformat()
    if isinstance(payload, date):
        return payload.isoformat()
    if isinstance(payload, list):
        return [_serialize_for_json(item) for item in payload]
    if isinstance(payload, tuple):
        return [_serialize_for_json(item) for item in payload]
    if isinstance(payload, dict):
        return {key: _serialize_for_json(value) for key, value in payload.items()}
    return payload


def serialize_execution_batch(batch: ExecutionInstructionBatch) -> dict[str, Any]:
    payload = asdict(batch)
    payload = _serialize_for_json(payload)
    return payload


def serialize_rl_candidate_status(payload: Any) -> dict[str, Any]:
    serialized_instruction = serialize_instruction_status(payload)
    stored_payload = serialized_instruction.get("payload", {})
    stored_instruction = (
        stored_payload.get("instruction", {})
        if isinstance(stored_payload, dict)
        else {}
    )
    execution = (
        stored_instruction.get("execution", {})
        if isinstance(stored_instruction, dict)
        else {}
    )
    model_id = None
    if isinstance(execution, dict):
        model_id = execution.get("model_id")
    if model_id is None and isinstance(stored_instruction, dict):
        model_id = stored_instruction.get("model")

    return _serialize_for_json({
        "candidate_id": payload.instruction_id,
        "instruction_id": payload.instruction_id,
        "state": payload.state,
        "account_key": payload.account_key,
        "book_key": payload.book_key,
        "is_virtual": payload.is_virtual,
        "symbol": payload.symbol,
        "exchange": payload.exchange,
        "currency": payload.currency,
        "side": payload.side,
        "model_id": model_id,
        "model_family": (
            execution.get("model_family") if isinstance(execution, dict) else None
        ),
        "model_version": (
            execution.get("model_version") if isinstance(execution, dict) else None
        ),
        "model_artifact_id": (
            execution.get("model_artifact_id") if isinstance(execution, dict) else None
        ),
        "execution_window": (
            execution.get("window") if isinstance(execution, dict) else None
        ),
        "sizing": (
            stored_instruction.get("sizing", {})
            if isinstance(stored_instruction, dict)
            else {}
        ),
        "trace": (
            stored_instruction.get("trace", {})
            if isinstance(stored_instruction, dict)
            else {}
        ),
        "source": (
            stored_payload.get("source", {})
            if isinstance(stored_payload, dict)
            else {}
        ),
        "updated_at": payload.updated_at,
        "candidate": serialized_instruction,
    })


def serialize_runtime_schedule_preview(payload: Any) -> dict[str, Any]:
    serialized = asdict(payload)
    return _serialize_for_json(serialized)


def serialize_submitted_batch(payload: Any) -> dict[str, Any]:
    serialized = asdict(payload)
    return _serialize_for_json(serialized)


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
    broker_sessions = CanonicalSyncSessions(
        app_config.ibkr,
        initial_connect_backoff_seconds=app_config.broker_connect_backoff_initial_seconds,
        max_connect_backoff_seconds=app_config.broker_connect_backoff_max_seconds,
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
    ) -> Any:
        return fetch_runtime_snapshot_with_primary(
            broker_config,
            timeout=timeout,
            include_open_orders=True,
            include_executions=True,
            include_account_updates=False,
            include_positions=True,
        )

    def drain_broker_callbacks_with_primary() -> list[dict[str, Any]]:
        return broker_sessions.primary.drain_broker_callback_events()

    def run_diagnostic_heartbeat_probe() -> Any:
        return with_diagnostic_session(
            "heartbeat_probe",
            lambda broker_app: probe_gateway(
                app_config.ibkr.diagnostic_session(),
                timeout=app_config.broker_heartbeat_timeout_seconds,
                app=broker_app,
            ),
            ignore_cooldown=True,
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
            include_open_orders=False,
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
        )

    broker_monitor = BrokerMonitorService(
        heartbeat_probe=run_diagnostic_heartbeat_probe,
        snapshot_fetcher=fetch_background_runtime_snapshot,
        snapshot_persister=persist_background_runtime_snapshot,
        heartbeat_interval_seconds=app_config.broker_heartbeat_interval_seconds,
        snapshot_refresh_interval_seconds=app_config.broker_snapshot_refresh_interval_seconds,
    )
    execution_runtime = BackgroundExecutionRuntimeService(
        session_factory,
        app_config,
        broker_sessions,
    )
    market_stream_service = LiveMarketDataStreamService(
        app_config.ibkr.streaming_session(),
        initial_connect_backoff_seconds=app_config.broker_connect_backoff_initial_seconds,
        max_connect_backoff_seconds=app_config.broker_connect_backoff_max_seconds,
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
        try:
            yield
        finally:
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
    app.state.broker_monitor = broker_monitor
    app.state.execution_runtime = execution_runtime
    app.state.market_stream_service = market_stream_service
    app.state.market_stream_identity_map = market_stream_identity_map

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

    @app.get("/healthz")
    def healthz(refresh_broker_status: bool = True) -> dict[str, Any]:
        if (
            refresh_broker_status
            and app_config.broker_monitor_enabled
            and app_config.environment != "test"
        ):
            broker_monitor.request_cycle_if_due(
                min_interval_seconds=app_config.broker_status_refresh_min_interval_seconds
            )
        return {
            "status": "ok",
            "local_only": app_config.api.require_loopback_only,
            "api_host": app_config.api.host,
            "api_port": app_config.api.port,
            "runtime_timezone": app_config.timezone,
            "session_calendar_path": str(app_config.session_calendar_path),
            "broker_sessions": broker_sessions.status_snapshot(blocking=False),
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
        }

    @app.post("/v1/ibkr/probe")
    def run_ibkr_probe(timeout: int = 5) -> dict[str, Any]:
        try:
            result = with_diagnostic_session(
                "probe",
                lambda broker_app: probe_gateway(
                    app_config.ibkr.diagnostic_session(),
                    timeout=timeout,
                    app=broker_app,
                ),
                ignore_cooldown=True,
            )
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except TimeoutError as exc:
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
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

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
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

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
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

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
            return with_diagnostic_session(
                "historical_bars",
                lambda broker_app: read_historical_bars(
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
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

    @app.post("/v1/market-data/stockholm-intraday-backfill")
    def get_stockholm_intraday_backfill(
        payload: dict[str, Any],
        timeout: int = 20,
    ) -> dict[str, Any]:
        try:
            query = parse_stockholm_intraday_backfill_payload(payload)
            result = with_diagnostic_session(
                "stockholm_intraday_backfill",
                lambda broker_app: collect_stockholm_intraday_backfill(
                    app_config.ibkr.diagnostic_session(),
                    query,
                    instruments_path=app_config.stockholm_instruments_path,
                    identity_path=app_config.stockholm_identity_path,
                    timeout=timeout,
                    app=broker_app,
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
                instruments_dir=app_config.stockholm_instruments_path.parent,
                meta_dir=app_config.stockholm_identity_path.parent / "shortability",
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
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

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
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

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

        candidates = list_instruction_statuses(
            session_factory,
            limit=500,
            state=ExecutionState.MODEL_ROUTED_PENDING.value,
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
            if normalized_account_key and candidate.account_key.upper() != normalized_account_key:
                return False
            if normalized_book_key and candidate.book_key.lower() != normalized_book_key:
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

    @app.post("/v1/rl/actions/translate")
    def translate_rl_action_endpoint(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            parsed = parse_rl_action_translate_payload(payload)
            source_status = read_instruction_status(
                session_factory,
                parsed["source_instruction_id"],
                include_events=False,
            )
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
            if translation.instruction_payload is not None:
                generated_instruction_id = str(
                    translation.instruction_payload["instructions"][0]["instruction_id"]
                )

            if parsed["submit"] and translation.instruction_payload is not None:
                deterministic_batch = parse_execution_batch_payload(
                    translation.instruction_payload
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
                        "decision_id": parsed["decision_id"],
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
            source_mode = "provided"
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
                    stream_bars = {
                        str(symbol).strip().upper(): bars
                        for symbol, bars in stream_snapshot["bars_by_symbol"].items()
                        if bars
                    }
                    missing_stream_bars = [
                        symbol for symbol in requested_symbols if symbol not in stream_bars
                    ]
                    if missing_stream_bars:
                        raise ValueError(
                            "market stream has no 1-minute bars for symbols: "
                            f"{missing_stream_bars}. Subscribe first with "
                            "POST /v1/market-data/stream/subscribe and wait for live ticks."
                        )
                    source_bars = stream_bars
                    streamed_symbols = sorted(stream_bars)
                    source_mode = "market_stream"
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
                        result = with_diagnostic_session(
                            "rl_observation_historical_bars",
                            lambda broker_app, query=query: read_historical_bars(
                                app_config.ibkr.diagnostic_session(),
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
                config_overrides=parsed["config_overrides"],
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
        heartbeat_stale_after_seconds: int = 120,
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
            validated_stale_after_seconds = parse_positive_limit(
                heartbeat_stale_after_seconds,
                field_name="heartbeat_stale_after_seconds",
                maximum=86400,
            )
            snapshot = build_rl_trader_dashboard_snapshot(
                session_factory,
                model_limit=validated_model_limit,
                deployment_limit=validated_deployment_limit,
                action_limit=validated_action_limit,
                heartbeat_stale_after_seconds=validated_stale_after_seconds,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "rl_dashboard": serialize_rl_trader_dashboard_snapshot(snapshot),
        }

    @app.get("/v1/read/operator-snapshot")
    def get_operator_snapshot(
        instruction_limit: int = 50,
        candidate_limit: int = 20,
        candidate_reason_code: str | None = None,
        order_limit: int = 50,
        fill_limit: int = 50,
        attention_limit: int = 25,
        reconciliation_run_limit: int = 20,
        include_flat_positions: bool = False,
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
            rl_candidates = list_instruction_statuses(
                session_factory,
                limit=500,
                state=ExecutionState.MODEL_ROUTED_PENDING.value,
                model_routed=True,
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
                    if candidate_reason_code(candidate) == normalized_candidate_reason_code
                )
            rl_candidates = rl_candidates[:validated_candidate_limit]
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "operator_snapshot": {
                **serialize_operator_dashboard_snapshot(operator_snapshot),
                "instructions": [
                    serialize_instruction_status(instruction)
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

    @app.post("/v1/instructions/submit")
    def submit_instruction(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            batch = parse_execution_batch_payload(payload)
            result = submit_execution_batch(
                session_factory,
                batch,
                runtime_timezone=app_config.timezone,
                session_calendar_path=app_config.session_calendar_path,
            )
        except SubmissionConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except KillSwitchActiveError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "instruction_count": result.instruction_count,
            "runtime_timezone": app_config.timezone,
            "session_calendar_path": str(app_config.session_calendar_path),
            "submitted": serialize_submitted_batch(result),
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
    def submit_instruction_entry(instruction_id: str, timeout: int = 10) -> dict[str, Any]:
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
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

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
    def cancel_instruction_entry(instruction_id: str, timeout: int = 10) -> dict[str, Any]:
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
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

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
            now_at, timeout, instruction_ids = parse_runtime_cycle_payload(request_payload)
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
    def run_startup_reconciliation_once(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        request_payload = payload or {}
        try:
            now_at, timeout, instruction_ids = parse_runtime_cycle_payload(request_payload)
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
