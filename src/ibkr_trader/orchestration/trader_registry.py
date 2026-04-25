from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import TraderActionRecord
from ibkr_trader.db.models import TraderDeploymentRecord
from ibkr_trader.db.models import TraderHeartbeatRecord
from ibkr_trader.db.models import TraderModelRecord


RL_SHORT_ACTION_SPACE = (
    "skip",
    "wait",
    "market_entry",
    "cancel_entry",
    "exit_market",
    "clear_exit",
    "entry_prevclose_88bp",
    "exit_tp_180bp",
)


class TraderModelConflictError(RuntimeError):
    """Raised when a trader model key already exists."""


class TraderDeploymentConflictError(RuntimeError):
    """Raised when a trader deployment key already exists."""


class TraderModelNotFoundError(RuntimeError):
    """Raised when a referenced trader model is missing."""


class TraderDeploymentNotFoundError(RuntimeError):
    """Raised when a referenced trader deployment is missing."""


@dataclass(slots=True)
class TraderModelRegistration:
    model_key: str
    display_name: str
    strategy_family: str
    side: str
    source_workflow_path: str | None
    promoted_checkpoint_path: str | None
    action_space: tuple[str, ...]
    observation_contract: dict[str, Any]
    execution_mapping_version: str | None
    metadata: dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class TraderDeploymentRegistration:
    deployment_key: str
    model_key: str
    display_name: str
    account_key: str
    book_key: str
    mode: str
    status: str
    allowed_symbols: tuple[str, ...]
    risk_limits: dict[str, Any]
    action_constraints: dict[str, Any]
    metadata: dict[str, Any]
    started_at: datetime | None
    paused_at: datetime | None
    stopped_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class TraderActionLogEntry:
    action_id: int
    deployment_key: str
    model_key: str
    account_key: str
    book_key: str
    symbol: str
    action_name: str
    state_before: str | None
    state_after: str | None
    action_status: str
    observed_at: datetime
    action_at: datetime
    instruction_id: str | None
    payload: dict[str, Any]
    note: str | None


@dataclass(slots=True)
class TraderHeartbeatStatus:
    deployment_key: str
    model_key: str
    status: str
    last_seen_at: datetime
    last_bar_at: datetime | None
    last_action_at: datetime | None
    runtime_error: str | None
    metrics: dict[str, Any]
    updated_at: datetime


def _serialize_dataclass(payload: Any) -> dict[str, Any]:
    return asdict(payload)


def _normalize_symbol_list(symbols: list[str] | tuple[str, ...] | None) -> list[str]:
    if not symbols:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        symbol = str(raw_symbol).strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    return normalized


def _normalize_action_space(action_space: list[str] | tuple[str, ...]) -> list[str]:
    if not action_space:
        raise ValueError("action_space must not be empty")
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_action in action_space:
        action_name = str(raw_action).strip().lower()
        if not action_name:
            raise ValueError("action_space must contain only non-empty strings")
        if action_name in seen:
            continue
        seen.add(action_name)
        normalized.append(action_name)
    return normalized


def register_trader_model(
    session_factory: sessionmaker,
    *,
    model_key: str,
    display_name: str,
    strategy_family: str,
    side: str,
    source_workflow_path: str | None,
    promoted_checkpoint_path: str | None,
    action_space: list[str] | tuple[str, ...],
    observation_contract: dict[str, Any] | None = None,
    execution_mapping_version: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> TraderModelRegistration:
    normalized_model_key = model_key.strip().lower()
    if not normalized_model_key:
        raise ValueError("model_key is required")

    normalized_display_name = display_name.strip()
    if not normalized_display_name:
        raise ValueError("display_name is required")

    normalized_strategy_family = strategy_family.strip()
    if not normalized_strategy_family:
        raise ValueError("strategy_family is required")

    normalized_side = side.strip().upper()
    if normalized_side not in {"LONG", "SHORT", "MIXED"}:
        raise ValueError("side must be LONG, SHORT, or MIXED")

    normalized_action_space = _normalize_action_space(action_space)

    with session_scope(session_factory) as session:
        existing = session.execute(
            select(TraderModelRecord).where(
                TraderModelRecord.model_key == normalized_model_key
            )
        ).scalar_one_or_none()
        if existing is not None:
            raise TraderModelConflictError(
                f"Trader model '{normalized_model_key}' already exists."
            )

        record = TraderModelRecord(
            model_key=normalized_model_key,
            display_name=normalized_display_name,
            strategy_family=normalized_strategy_family,
            side=normalized_side,
            source_workflow_path=source_workflow_path.strip()
            if source_workflow_path
            else None,
            promoted_checkpoint_path=promoted_checkpoint_path.strip()
            if promoted_checkpoint_path
            else None,
            action_space_json=normalized_action_space,
            observation_contract_json=dict(observation_contract or {}),
            execution_mapping_version=execution_mapping_version.strip()
            if execution_mapping_version
            else None,
            metadata_json=dict(metadata or {}),
        )
        session.add(record)
        session.flush()
        session.refresh(record)

        return TraderModelRegistration(
            model_key=record.model_key,
            display_name=record.display_name,
            strategy_family=record.strategy_family,
            side=record.side,
            source_workflow_path=record.source_workflow_path,
            promoted_checkpoint_path=record.promoted_checkpoint_path,
            action_space=tuple(record.action_space_json),
            observation_contract=dict(record.observation_contract_json),
            execution_mapping_version=record.execution_mapping_version,
            metadata=dict(record.metadata_json),
            created_at=record.created_at,
            updated_at=record.updated_at,
        )


def create_trader_deployment(
    session_factory: sessionmaker,
    *,
    deployment_key: str,
    model_key: str,
    account_key: str,
    book_key: str,
    mode: str,
    status: str,
    allowed_symbols: list[str] | tuple[str, ...] | None = None,
    risk_limits: dict[str, Any] | None = None,
    action_constraints: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> TraderDeploymentRegistration:
    normalized_deployment_key = deployment_key.strip().lower()
    if not normalized_deployment_key:
        raise ValueError("deployment_key is required")

    normalized_model_key = model_key.strip().lower()
    if not normalized_model_key:
        raise ValueError("model_key is required")

    normalized_account_key = account_key.strip().upper()
    if not normalized_account_key:
        raise ValueError("account_key is required")

    normalized_book_key = book_key.strip().lower()
    if not normalized_book_key:
        raise ValueError("book_key is required")

    normalized_mode = mode.strip().lower()
    if normalized_mode not in {"paper", "live"}:
        raise ValueError("mode must be paper or live")

    normalized_status = status.strip().lower()
    if normalized_status not in {"draft", "paused", "running", "degraded", "stopped"}:
        raise ValueError(
            "status must be draft, paused, running, degraded, or stopped"
        )

    normalized_allowed_symbols = _normalize_symbol_list(allowed_symbols)
    started_at = utc_now() if normalized_status == "running" else None
    paused_at = utc_now() if normalized_status == "paused" else None
    stopped_at = utc_now() if normalized_status == "stopped" else None

    with session_scope(session_factory) as session:
        model_record = session.execute(
            select(TraderModelRecord).where(
                TraderModelRecord.model_key == normalized_model_key
            )
        ).scalar_one_or_none()
        if model_record is None:
            raise TraderModelNotFoundError(
                f"Trader model '{normalized_model_key}' was not found."
            )

        existing = session.execute(
            select(TraderDeploymentRecord).where(
                TraderDeploymentRecord.deployment_key == normalized_deployment_key
            )
        ).scalar_one_or_none()
        if existing is not None:
            raise TraderDeploymentConflictError(
                f"Trader deployment '{normalized_deployment_key}' already exists."
            )

        record = TraderDeploymentRecord(
            trader_model_id=model_record.id,
            deployment_key=normalized_deployment_key,
            account_key=normalized_account_key,
            book_key=normalized_book_key,
            mode=normalized_mode,
            status=normalized_status,
            allowed_symbols_json=normalized_allowed_symbols,
            risk_limits_json=dict(risk_limits or {}),
            action_constraints_json=dict(action_constraints or {}),
            metadata_json=dict(metadata or {}),
            started_at=started_at,
            paused_at=paused_at,
            stopped_at=stopped_at,
        )
        session.add(record)
        session.flush()
        session.refresh(record)

        return TraderDeploymentRegistration(
            deployment_key=record.deployment_key,
            model_key=model_record.model_key,
            display_name=model_record.display_name,
            account_key=record.account_key,
            book_key=record.book_key,
            mode=record.mode,
            status=record.status,
            allowed_symbols=tuple(record.allowed_symbols_json),
            risk_limits=dict(record.risk_limits_json),
            action_constraints=dict(record.action_constraints_json),
            metadata=dict(record.metadata_json),
            started_at=record.started_at,
            paused_at=record.paused_at,
            stopped_at=record.stopped_at,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )


def log_trader_action(
    session_factory: sessionmaker,
    *,
    deployment_key: str,
    symbol: str,
    action_name: str,
    observed_at: datetime,
    state_before: str | None = None,
    state_after: str | None = None,
    action_status: str = "logged",
    instruction_id: str | None = None,
    payload: dict[str, Any] | None = None,
    note: str | None = None,
) -> TraderActionLogEntry:
    normalized_deployment_key = deployment_key.strip().lower()
    if not normalized_deployment_key:
        raise ValueError("deployment_key is required")
    normalized_symbol = symbol.strip().upper()
    if not normalized_symbol:
        raise ValueError("symbol is required")
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must include timezone information")

    normalized_action_name = action_name.strip().lower()
    if not normalized_action_name:
        raise ValueError("action_name is required")

    normalized_action_status = action_status.strip().lower()
    if not normalized_action_status:
        raise ValueError("action_status is required")

    with session_scope(session_factory) as session:
        deployment_record = session.execute(
            select(TraderDeploymentRecord)
            .join(TraderModelRecord)
            .where(TraderDeploymentRecord.deployment_key == normalized_deployment_key)
        ).scalar_one_or_none()
        if deployment_record is None:
            raise TraderDeploymentNotFoundError(
                f"Trader deployment '{normalized_deployment_key}' was not found."
            )

        model_record = deployment_record.trader_model
        allowed_actions = {
            str(item).strip().lower() for item in model_record.action_space_json
        }
        if normalized_action_name not in allowed_actions:
            raise ValueError(
                f"Action '{normalized_action_name}' is not in model '{model_record.model_key}' action_space."
            )

        record = TraderActionRecord(
            trader_deployment_id=deployment_record.id,
            observed_at=observed_at,
            symbol=normalized_symbol,
            action_name=normalized_action_name,
            state_before=state_before.strip().upper() if state_before else None,
            state_after=state_after.strip().upper() if state_after else None,
            action_status=normalized_action_status,
            instruction_id=instruction_id.strip() if instruction_id else None,
            payload=dict(payload or {}),
            note=note.strip() if note else None,
        )
        session.add(record)
        session.flush()
        session.refresh(record)

        return TraderActionLogEntry(
            action_id=record.id,
            deployment_key=deployment_record.deployment_key,
            model_key=model_record.model_key,
            account_key=deployment_record.account_key,
            book_key=deployment_record.book_key,
            symbol=record.symbol,
            action_name=record.action_name,
            state_before=record.state_before,
            state_after=record.state_after,
            action_status=record.action_status,
            observed_at=record.observed_at,
            action_at=record.action_at,
            instruction_id=record.instruction_id,
            payload=dict(record.payload),
            note=record.note,
        )


def upsert_trader_heartbeat(
    session_factory: sessionmaker,
    *,
    deployment_key: str,
    status: str,
    last_seen_at: datetime,
    last_bar_at: datetime | None = None,
    last_action_at: datetime | None = None,
    runtime_error: str | None = None,
    metrics: dict[str, Any] | None = None,
) -> TraderHeartbeatStatus:
    normalized_deployment_key = deployment_key.strip().lower()
    if not normalized_deployment_key:
        raise ValueError("deployment_key is required")
    normalized_status = status.strip().lower()
    if not normalized_status:
        raise ValueError("status is required")
    if last_seen_at.tzinfo is None:
        raise ValueError("last_seen_at must include timezone information")

    with session_scope(session_factory) as session:
        deployment_record = session.execute(
            select(TraderDeploymentRecord)
            .join(TraderModelRecord)
            .where(TraderDeploymentRecord.deployment_key == normalized_deployment_key)
        ).scalar_one_or_none()
        if deployment_record is None:
            raise TraderDeploymentNotFoundError(
                f"Trader deployment '{normalized_deployment_key}' was not found."
            )

        record = session.execute(
            select(TraderHeartbeatRecord).where(
                TraderHeartbeatRecord.trader_deployment_id == deployment_record.id
            )
        ).scalar_one_or_none()
        if record is None:
            record = TraderHeartbeatRecord(
                trader_deployment_id=deployment_record.id,
                status=normalized_status,
                last_seen_at=last_seen_at,
                last_bar_at=last_bar_at,
                last_action_at=last_action_at,
                runtime_error=runtime_error.strip() if runtime_error else None,
                metrics_json=dict(metrics or {}),
            )
            session.add(record)
        else:
            record.status = normalized_status
            record.last_seen_at = last_seen_at
            record.last_bar_at = last_bar_at
            record.last_action_at = last_action_at
            record.runtime_error = runtime_error.strip() if runtime_error else None
            record.metrics_json = dict(metrics or {})
        session.flush()
        session.refresh(record)

        return TraderHeartbeatStatus(
            deployment_key=deployment_record.deployment_key,
            model_key=deployment_record.trader_model.model_key,
            status=record.status,
            last_seen_at=record.last_seen_at,
            last_bar_at=record.last_bar_at,
            last_action_at=record.last_action_at,
            runtime_error=record.runtime_error,
            metrics=dict(record.metrics_json),
            updated_at=record.updated_at,
        )


__all__ = [
    "RL_SHORT_ACTION_SPACE",
    "TraderActionLogEntry",
    "TraderDeploymentConflictError",
    "TraderDeploymentNotFoundError",
    "TraderDeploymentRegistration",
    "TraderHeartbeatStatus",
    "TraderModelConflictError",
    "TraderModelNotFoundError",
    "TraderModelRegistration",
    "_serialize_dataclass",
    "create_trader_deployment",
    "log_trader_action",
    "register_trader_model",
    "upsert_trader_heartbeat",
]
