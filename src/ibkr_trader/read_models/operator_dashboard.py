from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import AccountSnapshotRecord
from ibkr_trader.db.models import BrokerAccountRecord
from ibkr_trader.db.models import BrokerOrderEventRecord
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import ExecutionFillRecord
from ibkr_trader.db.models import OperatorControlEventRecord
from ibkr_trader.db.models import OperatorControlRecord
from ibkr_trader.db.models import PositionSnapshotRecord
from ibkr_trader.db.models import ReconciliationIssueRecord
from ibkr_trader.db.models import ReconciliationRunRecord
from ibkr_trader.orchestration.operator_controls import KILL_SWITCH_CONTROL_KEY

_CLOSED_ORDER_STATUSES = {
    "API_CANCELLED",
    "CANCELLED",
    "FILLED",
    "INACTIVE",
}


@dataclass(slots=True)
class OperatorAccountSnapshot:
    broker_kind: str
    account_key: str
    account_label: str | None
    base_currency: str | None
    snapshot_at: datetime
    source: str
    currency: str | None
    net_liquidation: str | None
    total_cash_value: str | None
    buying_power: str | None
    available_funds: str | None
    excess_liquidity: str | None
    cushion: str | None


@dataclass(slots=True)
class OperatorPositionSnapshot:
    broker_kind: str
    account_key: str
    account_label: str | None
    snapshot_at: datetime
    source: str
    symbol: str
    exchange: str
    currency: str
    security_type: str
    primary_exchange: str | None
    local_symbol: str | None
    quantity: str
    average_cost: str | None
    market_price: str | None
    market_value: str | None
    unrealized_pnl: str | None
    realized_pnl: str | None


@dataclass(slots=True)
class OperatorOpenOrder:
    broker_order_id: int
    instruction_record_id: int | None
    broker_kind: str
    account_key: str
    account_label: str | None
    order_role: str
    external_order_id: str | None
    external_perm_id: str | None
    external_client_id: str | None
    order_ref: str | None
    symbol: str
    exchange: str
    currency: str
    security_type: str
    primary_exchange: str | None
    local_symbol: str | None
    side: str
    order_type: str
    time_in_force: str | None
    status: str
    total_quantity: str | None
    limit_price: str | None
    stop_price: str | None
    submitted_at: datetime | None
    last_status_at: datetime | None
    warning_text: str | None
    reject_reason: str | None


@dataclass(slots=True)
class OperatorExecutionFill:
    fill_id: int
    broker_order_id: int | None
    instruction_record_id: int | None
    broker_kind: str
    account_key: str
    account_label: str | None
    executed_at: datetime
    symbol: str
    exchange: str | None
    currency: str
    security_type: str
    side: str | None
    quantity: str
    price: str
    commission: str | None
    commission_currency: str | None
    order_ref: str | None
    external_execution_id: str
    external_order_id: str | None
    external_perm_id: str | None


@dataclass(slots=True)
class OperatorBrokerAttention:
    event_id: int
    broker_order_id: int
    account_key: str
    account_label: str | None
    symbol: str
    order_ref: str | None
    event_type: str
    status_after: str | None
    event_at: datetime
    message: str
    note: str | None


@dataclass(slots=True)
class OperatorReconciliationIssue:
    issue_id: int
    instruction_id: str | None
    stage: str
    severity: str
    message: str
    observed_at: datetime
    payload: dict[str, Any]


@dataclass(slots=True)
class OperatorReconciliationRun:
    run_id: int
    run_kind: str
    broker_kind: str
    account_key: str | None
    runtime_timezone: str | None
    started_at: datetime
    completed_at: datetime
    status: str
    issue_count: int
    action_count: int
    metadata_json: dict[str, Any]
    issues: tuple[OperatorReconciliationIssue, ...]


@dataclass(slots=True)
class OperatorKillSwitch:
    enabled: bool
    reason: str | None
    updated_by: str | None
    last_changed_at: datetime | None
    latest_event_at: datetime | None


@dataclass(slots=True)
class OperatorDashboardSnapshot:
    generated_at: datetime
    kill_switch: OperatorKillSwitch
    accounts: tuple[OperatorAccountSnapshot, ...]
    positions: tuple[OperatorPositionSnapshot, ...]
    open_orders: tuple[OperatorOpenOrder, ...]
    recent_fills: tuple[OperatorExecutionFill, ...]
    recent_broker_attention: tuple[OperatorBrokerAttention, ...]
    recent_reconciliation_runs: tuple[OperatorReconciliationRun, ...]


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


def serialize_operator_dashboard_snapshot(
    snapshot: OperatorDashboardSnapshot,
) -> dict[str, Any]:
    return _serialize_for_json(asdict(snapshot))


def _normalize_order_status(status: str | None) -> str | None:
    if status is None:
        return None
    normalized = status.strip()
    if not normalized:
        return None
    return normalized.upper()


def _is_non_zero_quantity(value: str | None) -> bool:
    if value in (None, ""):
        return False
    return Decimal(str(value)) != Decimal("0")


def _build_account_snapshots(
    session: Session,
) -> tuple[OperatorAccountSnapshot, ...]:
    rows = session.execute(
        select(AccountSnapshotRecord, BrokerAccountRecord)
        .join(
            BrokerAccountRecord,
            BrokerAccountRecord.id == AccountSnapshotRecord.broker_account_id,
        )
        .order_by(
            AccountSnapshotRecord.snapshot_at.desc(),
            AccountSnapshotRecord.id.desc(),
        )
    ).all()

    latest_by_account_id: dict[int, OperatorAccountSnapshot] = {}
    for account_snapshot, broker_account in rows:
        if broker_account.id in latest_by_account_id:
            continue
        latest_by_account_id[broker_account.id] = OperatorAccountSnapshot(
            broker_kind=broker_account.broker_kind,
            account_key=broker_account.account_key,
            account_label=broker_account.account_label,
            base_currency=broker_account.base_currency,
            snapshot_at=account_snapshot.snapshot_at,
            source=account_snapshot.source,
            currency=account_snapshot.currency,
            net_liquidation=account_snapshot.net_liquidation,
            total_cash_value=account_snapshot.total_cash_value,
            buying_power=account_snapshot.buying_power,
            available_funds=account_snapshot.available_funds,
            excess_liquidity=account_snapshot.excess_liquidity,
            cushion=account_snapshot.cushion,
        )

    return tuple(
        sorted(
            latest_by_account_id.values(),
            key=lambda row: (row.account_key, row.snapshot_at),
            reverse=False,
        )
    )


def _build_kill_switch(session: Session) -> OperatorKillSwitch:
    record = session.execute(
        select(OperatorControlRecord).where(
            OperatorControlRecord.control_key == KILL_SWITCH_CONTROL_KEY
        )
    ).scalar_one_or_none()
    if record is None:
        return OperatorKillSwitch(
            enabled=False,
            reason=None,
            updated_by=None,
            last_changed_at=None,
            latest_event_at=None,
        )

    latest_event = session.execute(
        select(OperatorControlEventRecord)
        .where(OperatorControlEventRecord.operator_control_id == record.id)
        .order_by(OperatorControlEventRecord.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    return OperatorKillSwitch(
        enabled=record.enabled,
        reason=record.reason,
        updated_by=record.updated_by,
        last_changed_at=record.last_changed_at,
        latest_event_at=latest_event.event_at if latest_event is not None else None,
    )


def _build_position_snapshots(
    session: Session,
    *,
    include_flat_positions: bool,
) -> tuple[OperatorPositionSnapshot, ...]:
    rows = session.execute(
        select(PositionSnapshotRecord, BrokerAccountRecord)
        .join(
            BrokerAccountRecord,
            BrokerAccountRecord.id == PositionSnapshotRecord.broker_account_id,
        )
        .order_by(
            PositionSnapshotRecord.snapshot_at.desc(),
            PositionSnapshotRecord.id.desc(),
        )
    ).all()

    latest_by_identity: dict[
        tuple[int, str, str, str, str, str | None],
        OperatorPositionSnapshot,
    ] = {}
    for position_snapshot, broker_account in rows:
        identity = (
            broker_account.id,
            position_snapshot.symbol,
            position_snapshot.exchange,
            position_snapshot.currency,
            position_snapshot.security_type,
            position_snapshot.local_symbol,
        )
        if identity in latest_by_identity:
            continue
        if not include_flat_positions and not _is_non_zero_quantity(position_snapshot.quantity):
            continue
        latest_by_identity[identity] = OperatorPositionSnapshot(
            broker_kind=broker_account.broker_kind,
            account_key=broker_account.account_key,
            account_label=broker_account.account_label,
            snapshot_at=position_snapshot.snapshot_at,
            source=position_snapshot.source,
            symbol=position_snapshot.symbol,
            exchange=position_snapshot.exchange,
            currency=position_snapshot.currency,
            security_type=position_snapshot.security_type,
            primary_exchange=position_snapshot.primary_exchange,
            local_symbol=position_snapshot.local_symbol,
            quantity=position_snapshot.quantity,
            average_cost=position_snapshot.average_cost,
            market_price=position_snapshot.market_price,
            market_value=position_snapshot.market_value,
            unrealized_pnl=position_snapshot.unrealized_pnl,
            realized_pnl=position_snapshot.realized_pnl,
        )

    return tuple(
        sorted(
            latest_by_identity.values(),
            key=lambda row: (
                row.account_key,
                row.symbol,
                row.exchange,
                row.snapshot_at,
            ),
            reverse=False,
        )
    )


def _build_open_orders(
    session: Session,
    *,
    limit: int,
) -> tuple[OperatorOpenOrder, ...]:
    rows = session.execute(
        select(BrokerOrderRecord, BrokerAccountRecord)
        .join(
            BrokerAccountRecord,
            BrokerAccountRecord.id == BrokerOrderRecord.broker_account_id,
        )
        .order_by(
            BrokerOrderRecord.last_status_at.desc(),
            BrokerOrderRecord.updated_at.desc(),
            BrokerOrderRecord.id.desc(),
        )
        .limit(max(limit * 4, limit))
    ).all()

    open_orders: list[OperatorOpenOrder] = []
    for broker_order, broker_account in rows:
        if _normalize_order_status(broker_order.status) in _CLOSED_ORDER_STATUSES:
            continue
        metadata_json = broker_order.metadata_json or {}
        open_orders.append(
            OperatorOpenOrder(
                broker_order_id=broker_order.id,
                instruction_record_id=broker_order.instruction_id,
                broker_kind=broker_order.broker_kind,
                account_key=broker_order.account_key,
                account_label=broker_account.account_label,
                order_role=broker_order.order_role,
                external_order_id=broker_order.external_order_id,
                external_perm_id=broker_order.external_perm_id,
                external_client_id=broker_order.external_client_id,
                order_ref=broker_order.order_ref,
                symbol=broker_order.symbol,
                exchange=broker_order.exchange,
                currency=broker_order.currency,
                security_type=broker_order.security_type,
                primary_exchange=broker_order.primary_exchange,
                local_symbol=broker_order.local_symbol,
                side=broker_order.side,
                order_type=broker_order.order_type,
                time_in_force=broker_order.time_in_force,
                status=broker_order.status,
                total_quantity=broker_order.total_quantity,
                limit_price=broker_order.limit_price,
                stop_price=broker_order.stop_price,
                submitted_at=broker_order.submitted_at,
                last_status_at=broker_order.last_status_at,
                warning_text=(
                    str(metadata_json.get("warning_text"))
                    if metadata_json.get("warning_text") not in (None, "")
                    else None
                ),
                reject_reason=(
                    str(metadata_json.get("reject_reason"))
                    if metadata_json.get("reject_reason") not in (None, "")
                    else None
                ),
            )
        )
        if len(open_orders) >= limit:
            break
    return tuple(open_orders)


def _build_recent_fills(
    session: Session,
    *,
    limit: int,
) -> tuple[OperatorExecutionFill, ...]:
    rows = session.execute(
        select(ExecutionFillRecord, BrokerAccountRecord)
        .join(
            BrokerAccountRecord,
            BrokerAccountRecord.id == ExecutionFillRecord.broker_account_id,
        )
        .order_by(
            ExecutionFillRecord.executed_at.desc(),
            ExecutionFillRecord.id.desc(),
        )
        .limit(limit)
    ).all()
    return tuple(
        OperatorExecutionFill(
            fill_id=fill.id,
            broker_order_id=fill.broker_order_id,
            instruction_record_id=fill.instruction_id,
            broker_kind=fill.broker_kind,
            account_key=fill.account_key,
            account_label=broker_account.account_label,
            executed_at=fill.executed_at,
            symbol=fill.symbol,
            exchange=fill.exchange,
            currency=fill.currency,
            security_type=fill.security_type,
            side=fill.side,
            quantity=fill.quantity,
            price=fill.price,
            commission=fill.commission,
            commission_currency=fill.commission_currency,
            order_ref=fill.order_ref,
            external_execution_id=fill.external_execution_id,
            external_order_id=fill.external_order_id,
            external_perm_id=fill.external_perm_id,
        )
        for fill, broker_account in rows
    )


def _extract_broker_attention_message(
    broker_order_event: BrokerOrderEventRecord,
    broker_order: BrokerOrderRecord,
) -> str | None:
    payload = broker_order_event.payload or {}
    if not isinstance(payload, dict):
        payload = {}

    if broker_order_event.event_type == "order_error_callback":
        error_code = payload.get("errorCode")
        error_message = payload.get("errorMsg") or payload.get("message")
        if error_message in (None, ""):
            return broker_order_event.note
        if error_code in (None, ""):
            return str(error_message)
        return f"[{error_code}] {error_message}"

    for key in ("reject_reason", "warning_text"):
        raw_value = payload.get(key)
        if raw_value not in (None, ""):
            return str(raw_value)

    metadata_json = broker_order.metadata_json or {}
    for key in ("reject_reason", "warning_text"):
        raw_value = metadata_json.get(key)
        if raw_value not in (None, ""):
            return str(raw_value)

    if broker_order_event.note not in (None, ""):
        lowered_type = broker_order_event.event_type.lower()
        lowered_note = broker_order_event.note.lower()
        if "error" in lowered_type or "reject" in lowered_note or "warning" in lowered_note:
            return broker_order_event.note
    return None


def _build_recent_broker_attention(
    session: Session,
    *,
    limit: int,
) -> tuple[OperatorBrokerAttention, ...]:
    rows = session.execute(
        select(BrokerOrderEventRecord, BrokerOrderRecord, BrokerAccountRecord)
        .join(
            BrokerOrderRecord,
            BrokerOrderRecord.id == BrokerOrderEventRecord.broker_order_id,
        )
        .join(
            BrokerAccountRecord,
            BrokerAccountRecord.id == BrokerOrderRecord.broker_account_id,
        )
        .order_by(
            BrokerOrderEventRecord.event_at.desc(),
            BrokerOrderEventRecord.id.desc(),
        )
        .limit(max(limit * 6, limit))
    ).all()

    attention_rows: list[OperatorBrokerAttention] = []
    for broker_order_event, broker_order, broker_account in rows:
        message = _extract_broker_attention_message(broker_order_event, broker_order)
        if message is None:
            continue
        attention_rows.append(
            OperatorBrokerAttention(
                event_id=broker_order_event.id,
                broker_order_id=broker_order.id,
                account_key=broker_order.account_key,
                account_label=broker_account.account_label,
                symbol=broker_order.symbol,
                order_ref=broker_order.order_ref,
                event_type=broker_order_event.event_type,
                status_after=broker_order_event.status_after,
                event_at=broker_order_event.event_at,
                message=message,
                note=broker_order_event.note,
            )
        )
        if len(attention_rows) >= limit:
            break
    return tuple(attention_rows)


def _build_recent_reconciliation_runs(
    session: Session,
    *,
    limit: int,
) -> tuple[OperatorReconciliationRun, ...]:
    reconciliation_runs = list(
        session.execute(
            select(ReconciliationRunRecord)
            .order_by(
                ReconciliationRunRecord.started_at.desc(),
                ReconciliationRunRecord.id.desc(),
            )
            .limit(limit)
        ).scalars()
    )
    if not reconciliation_runs:
        return ()

    issues = list(
        session.execute(
            select(ReconciliationIssueRecord)
            .where(
                ReconciliationIssueRecord.reconciliation_run_id.in_(
                    [run.id for run in reconciliation_runs]
                )
            )
            .order_by(
                ReconciliationIssueRecord.observed_at.desc(),
                ReconciliationIssueRecord.id.desc(),
            )
        ).scalars()
    )
    issues_by_run_id: dict[int, list[OperatorReconciliationIssue]] = {}
    for issue in issues:
        issues_by_run_id.setdefault(issue.reconciliation_run_id, []).append(
            OperatorReconciliationIssue(
                issue_id=issue.id,
                instruction_id=issue.instruction_id,
                stage=issue.stage,
                severity=issue.severity,
                message=issue.message,
                observed_at=issue.observed_at,
                payload=issue.payload,
            )
        )

    return tuple(
        OperatorReconciliationRun(
            run_id=run.id,
            run_kind=run.run_kind,
            broker_kind=run.broker_kind,
            account_key=run.account_key,
            runtime_timezone=run.runtime_timezone,
            started_at=run.started_at,
            completed_at=run.completed_at,
            status=run.status,
            issue_count=run.issue_count,
            action_count=run.action_count,
            metadata_json=run.metadata_json,
            issues=tuple(issues_by_run_id.get(run.id, ())),
        )
        for run in reconciliation_runs
    )


def build_operator_dashboard_snapshot(
    session_factory: sessionmaker[Session],
    *,
    include_flat_positions: bool = False,
    order_limit: int = 50,
    fill_limit: int = 50,
    attention_limit: int = 25,
    reconciliation_run_limit: int = 20,
) -> OperatorDashboardSnapshot:
    """Return a durable operator-facing snapshot built only from persisted ledger rows."""

    with session_scope(session_factory) as session:
        return OperatorDashboardSnapshot(
            generated_at=utc_now(),
            kill_switch=_build_kill_switch(session),
            accounts=_build_account_snapshots(session),
            positions=_build_position_snapshots(
                session,
                include_flat_positions=include_flat_positions,
            ),
            open_orders=_build_open_orders(session, limit=order_limit),
            recent_fills=_build_recent_fills(session, limit=fill_limit),
            recent_broker_attention=_build_recent_broker_attention(
                session,
                limit=attention_limit,
            ),
            recent_reconciliation_runs=_build_recent_reconciliation_runs(
                session,
                limit=reconciliation_run_limit,
            ),
        )
