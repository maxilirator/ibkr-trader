from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import TraderActionRecord
from ibkr_trader.db.models import TraderDeploymentRecord
from ibkr_trader.db.models import TraderModelRecord


@dataclass(slots=True)
class RLDeploymentHeartbeat:
    status: str
    last_seen_at: datetime
    last_bar_at: datetime | None
    last_action_at: datetime | None
    runtime_error: str | None
    metrics: dict[str, Any]
    updated_at: datetime
    is_stale: bool


@dataclass(slots=True)
class RLTraderModel:
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
class RLTraderDeployment:
    deployment_key: str
    model_key: str
    model_display_name: str
    account_key: str
    book_key: str
    mode: str
    status: str
    is_virtual: bool
    allowed_symbols: tuple[str, ...]
    risk_limits: dict[str, Any]
    action_constraints: dict[str, Any]
    metadata: dict[str, Any]
    started_at: datetime | None
    paused_at: datetime | None
    stopped_at: datetime | None
    created_at: datetime
    updated_at: datetime
    heartbeat: RLDeploymentHeartbeat | None


@dataclass(slots=True)
class RLTraderAction:
    action_id: int
    deployment_key: str
    model_key: str
    model_display_name: str
    account_key: str
    book_key: str
    is_virtual: bool
    symbol: str
    action_name: str
    state_before: str | None
    state_after: str | None
    action_status: str
    observed_at: datetime
    action_at: datetime
    instruction_id: str | None
    note: str | None
    payload: dict[str, Any]


@dataclass(slots=True)
class RLTraderSummary:
    model_count: int
    deployment_count: int
    live_deployment_count: int
    virtual_deployment_count: int
    running_deployment_count: int
    stale_heartbeat_count: int
    recent_action_count: int


@dataclass(slots=True)
class RLTraderDashboardSnapshot:
    generated_at: datetime
    summary: RLTraderSummary
    models: tuple[RLTraderModel, ...]
    deployments: tuple[RLTraderDeployment, ...]
    recent_actions: tuple[RLTraderAction, ...]


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


def serialize_rl_trader_dashboard_snapshot(
    snapshot: RLTraderDashboardSnapshot,
) -> dict[str, Any]:
    return _serialize_for_json(asdict(snapshot))


def _normalize_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def build_rl_trader_dashboard_snapshot(
    session_factory: sessionmaker,
    *,
    model_limit: int = 50,
    deployment_limit: int = 50,
    action_limit: int = 100,
    heartbeat_stale_after_seconds: int = 120,
) -> RLTraderDashboardSnapshot:
    generated_at = utc_now()
    stale_cutoff = generated_at.timestamp() - heartbeat_stale_after_seconds

    with session_scope(session_factory) as session:
        model_records = session.execute(
            select(TraderModelRecord)
            .order_by(TraderModelRecord.updated_at.desc(), TraderModelRecord.id.desc())
            .limit(model_limit)
        ).scalars().all()

        deployment_records = session.execute(
            select(TraderDeploymentRecord)
            .order_by(
                TraderDeploymentRecord.updated_at.desc(),
                TraderDeploymentRecord.id.desc(),
            )
            .limit(deployment_limit)
        ).scalars().all()

        action_records = session.execute(
            select(TraderActionRecord)
            .order_by(TraderActionRecord.action_at.desc(), TraderActionRecord.id.desc())
            .limit(action_limit)
        ).scalars().all()

        models = tuple(
            RLTraderModel(
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
            for record in model_records
        )

        deployments: list[RLTraderDeployment] = []
        stale_heartbeat_count = 0
        for record in deployment_records:
            heartbeat_record = record.heartbeat
            heartbeat: RLDeploymentHeartbeat | None = None
            if heartbeat_record is not None:
                normalized_last_seen_at = _normalize_timestamp(
                    heartbeat_record.last_seen_at
                )
                is_stale = (
                    record.status in {"running", "degraded"}
                    and normalized_last_seen_at.timestamp() < stale_cutoff
                )
                if is_stale:
                    stale_heartbeat_count += 1
                heartbeat = RLDeploymentHeartbeat(
                    status=heartbeat_record.status,
                    last_seen_at=normalized_last_seen_at,
                    last_bar_at=(
                        _normalize_timestamp(heartbeat_record.last_bar_at)
                        if heartbeat_record.last_bar_at is not None
                        else None
                    ),
                    last_action_at=(
                        _normalize_timestamp(heartbeat_record.last_action_at)
                        if heartbeat_record.last_action_at is not None
                        else None
                    ),
                    runtime_error=heartbeat_record.runtime_error,
                    metrics=dict(heartbeat_record.metrics_json),
                    updated_at=heartbeat_record.updated_at,
                    is_stale=is_stale,
                )

            deployments.append(
                RLTraderDeployment(
                    deployment_key=record.deployment_key,
                    model_key=record.trader_model.model_key,
                    model_display_name=record.trader_model.display_name,
                    account_key=record.account_key,
                    book_key=record.book_key,
                    mode=record.mode,
                    status=record.status,
                    is_virtual=record.is_virtual,
                    allowed_symbols=tuple(record.allowed_symbols_json),
                    risk_limits=dict(record.risk_limits_json),
                    action_constraints=dict(record.action_constraints_json),
                    metadata=dict(record.metadata_json),
                    started_at=record.started_at,
                    paused_at=record.paused_at,
                    stopped_at=record.stopped_at,
                    created_at=record.created_at,
                    updated_at=record.updated_at,
                    heartbeat=heartbeat,
                )
            )

        recent_actions = tuple(
            RLTraderAction(
                action_id=record.id,
                deployment_key=record.trader_deployment.deployment_key,
                model_key=record.trader_deployment.trader_model.model_key,
                model_display_name=record.trader_deployment.trader_model.display_name,
                account_key=record.trader_deployment.account_key,
                book_key=record.trader_deployment.book_key,
                is_virtual=record.trader_deployment.is_virtual,
                symbol=record.symbol,
                action_name=record.action_name,
                state_before=record.state_before,
                state_after=record.state_after,
                action_status=record.action_status,
                observed_at=record.observed_at,
                action_at=record.action_at,
                instruction_id=record.instruction_id,
                note=record.note,
                payload=dict(record.payload),
            )
            for record in action_records
        )

        deployment_count = session.execute(
            select(TraderDeploymentRecord.id)
        ).scalars().all()
        live_deployment_count = session.execute(
            select(TraderDeploymentRecord.id).where(TraderDeploymentRecord.mode == "live")
        ).scalars().all()
        virtual_deployment_count = session.execute(
            select(TraderDeploymentRecord.id).where(
                TraderDeploymentRecord.is_virtual.is_(True)
            )
        ).scalars().all()
        running_deployment_count = session.execute(
            select(TraderDeploymentRecord.id).where(
                TraderDeploymentRecord.status == "running"
            )
        ).scalars().all()
        model_count = session.execute(select(TraderModelRecord.id)).scalars().all()

    return RLTraderDashboardSnapshot(
        generated_at=generated_at,
        summary=RLTraderSummary(
            model_count=len(model_count),
            deployment_count=len(deployment_count),
            live_deployment_count=len(live_deployment_count),
            virtual_deployment_count=len(virtual_deployment_count),
            running_deployment_count=len(running_deployment_count),
            stale_heartbeat_count=stale_heartbeat_count,
            recent_action_count=len(recent_actions),
        ),
        models=models,
        deployments=tuple(deployments),
        recent_actions=recent_actions,
    )


__all__ = [
    "RLTraderDashboardSnapshot",
    "build_rl_trader_dashboard_snapshot",
    "serialize_rl_trader_dashboard_snapshot",
]
