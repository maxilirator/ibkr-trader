from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import BrokerOrderEventRecord
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import ExecutionFillRecord
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import InstructionSetCancellationRecord
from ibkr_trader.db.models import OperatorControlEventRecord
from ibkr_trader.db.models import OperatorControlRecord
from ibkr_trader.db.models import ReconciliationIssueRecord
from ibkr_trader.db.models import ReconciliationRunRecord


@dataclass(slots=True)
class LedgerFocusInstruction:
    instruction_id: str
    state: str
    account_key: str
    book_key: str
    symbol: str
    exchange: str
    currency: str
    submit_at: datetime
    expire_at: datetime
    updated_at: datetime
    broker_order_id: int | None
    broker_order_status: str | None
    exit_order_id: int | None
    exit_order_status: str | None


@dataclass(slots=True)
class LedgerSummaryCounts:
    instruction_count: int
    instruction_event_count: int
    broker_order_count: int
    broker_order_event_count: int
    execution_fill_count: int
    control_event_count: int
    instruction_set_cancellation_count: int
    reconciliation_issue_count: int


@dataclass(slots=True)
class LedgerInstructionEvent:
    event_id: int
    instruction_id: str
    symbol: str
    account_key: str
    batch_id: str
    event_type: str
    source: str
    event_at: datetime
    state_before: str | None
    state_after: str | None
    note: str | None
    payload: dict[str, Any]


@dataclass(slots=True)
class LedgerBrokerOrderEvent:
    event_id: int
    broker_order_id: int
    instruction_id: str | None
    account_key: str
    symbol: str
    order_role: str
    external_order_id: str | None
    order_ref: str | None
    event_type: str
    event_at: datetime
    status_before: str | None
    status_after: str | None
    message: str | None
    note: str | None
    payload: dict[str, Any]


@dataclass(slots=True)
class LedgerExecutionFill:
    fill_id: int
    instruction_id: str | None
    broker_order_id: int | None
    account_key: str
    symbol: str
    side: str | None
    quantity: str
    price: str
    commission: str | None
    commission_currency: str | None
    executed_at: datetime
    external_execution_id: str
    external_order_id: str | None
    order_ref: str | None


@dataclass(slots=True)
class LedgerControlEvent:
    event_id: int
    control_key: str
    event_type: str
    source: str
    event_at: datetime
    enabled: bool
    updated_by: str | None
    reason: str | None
    note: str | None
    payload: dict[str, Any]


@dataclass(slots=True)
class LedgerInstructionSetCancellation:
    request_id: int
    requested_at: datetime
    requested_by: str
    reason: str | None
    status: str
    matched_instruction_count: int
    cancelled_pending_count: int
    cancelled_submitted_count: int
    skipped_count: int
    failed_count: int
    selectors: dict[str, Any]
    result_payload: dict[str, Any]


@dataclass(slots=True)
class LedgerReconciliationIssue:
    issue_id: int
    reconciliation_run_id: int
    run_kind: str
    instruction_id: str | None
    stage: str
    severity: str
    message: str
    observed_at: datetime
    payload: dict[str, Any]


@dataclass(slots=True)
class LedgerDashboardSnapshot:
    generated_at: datetime
    focus_instruction: LedgerFocusInstruction | None
    summary: LedgerSummaryCounts
    instruction_events: tuple[LedgerInstructionEvent, ...]
    broker_order_events: tuple[LedgerBrokerOrderEvent, ...]
    recent_fills: tuple[LedgerExecutionFill, ...]
    control_events: tuple[LedgerControlEvent, ...]
    instruction_set_cancellations: tuple[LedgerInstructionSetCancellation, ...]
    reconciliation_issues: tuple[LedgerReconciliationIssue, ...]


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


def serialize_ledger_dashboard_snapshot(
    snapshot: LedgerDashboardSnapshot,
) -> dict[str, Any]:
    return _serialize_for_json(asdict(snapshot))


def _extract_broker_order_event_message(
    event: BrokerOrderEventRecord,
    order: BrokerOrderRecord,
) -> str | None:
    payload = event.payload if isinstance(event.payload, dict) else {}
    if event.event_type == "order_error_callback":
        error_code = payload.get("errorCode")
        error_message = payload.get("errorMsg") or payload.get("message")
        if error_message not in (None, ""):
            if error_code in (None, ""):
                return str(error_message)
            return f"[{error_code}] {error_message}"

    for key in ("message", "warning_text", "reject_reason"):
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)

    metadata_json = order.metadata_json if isinstance(order.metadata_json, dict) else {}
    for key in ("warning_text", "reject_reason"):
        value = metadata_json.get(key)
        if value not in (None, ""):
            return str(value)

    if event.note not in (None, ""):
        return event.note
    return None


def _read_focus_instruction(
    session: Session,
    *,
    instruction_id: str | None,
) -> tuple[InstructionRecord | None, LedgerFocusInstruction | None]:
    if instruction_id is None:
        return None, None

    record = session.execute(
        select(InstructionRecord).where(InstructionRecord.instruction_id == instruction_id)
    ).scalar_one_or_none()
    if record is None:
        return None, None

    return record, LedgerFocusInstruction(
        instruction_id=record.instruction_id,
        state=record.state,
        account_key=record.account_key,
        book_key=record.book_key,
        symbol=record.symbol,
        exchange=record.exchange,
        currency=record.currency,
        submit_at=record.submit_at,
        expire_at=record.expire_at,
        updated_at=record.updated_at,
        broker_order_id=record.broker_order_id,
        broker_order_status=record.broker_order_status,
        exit_order_id=record.exit_order_id,
        exit_order_status=record.exit_order_status,
    )


def _count(session: Session, model: type[Any], where_clause: Any | None = None) -> int:
    statement = select(func.count()).select_from(model)
    if where_clause is not None:
        statement = statement.where(where_clause)
    return int(session.execute(statement).scalar_one())


def _build_summary(
    session: Session,
    *,
    focus_record: InstructionRecord | None,
) -> LedgerSummaryCounts:
    instruction_clause = (
        InstructionRecord.id == focus_record.id if focus_record is not None else None
    )
    instruction_event_clause = (
        InstructionEventRecord.instruction_id == focus_record.id
        if focus_record is not None
        else None
    )
    broker_order_clause = (
        BrokerOrderRecord.instruction_id == focus_record.id
        if focus_record is not None
        else None
    )
    broker_order_event_clause = (
        BrokerOrderRecord.instruction_id == focus_record.id
        if focus_record is not None
        else None
    )
    fill_clause = (
        ExecutionFillRecord.instruction_id == focus_record.id
        if focus_record is not None
        else None
    )
    reconciliation_clause = (
        ReconciliationIssueRecord.instruction_id == focus_record.instruction_id
        if focus_record is not None
        else None
    )

    broker_order_event_statement = select(func.count()).select_from(BrokerOrderEventRecord).join(
        BrokerOrderRecord,
        BrokerOrderRecord.id == BrokerOrderEventRecord.broker_order_id,
    )
    if broker_order_event_clause is not None:
        broker_order_event_statement = broker_order_event_statement.where(
            broker_order_event_clause
        )

    return LedgerSummaryCounts(
        instruction_count=_count(session, InstructionRecord, instruction_clause),
        instruction_event_count=_count(
            session,
            InstructionEventRecord,
            instruction_event_clause,
        ),
        broker_order_count=_count(session, BrokerOrderRecord, broker_order_clause),
        broker_order_event_count=int(
            session.execute(broker_order_event_statement).scalar_one()
        ),
        execution_fill_count=_count(session, ExecutionFillRecord, fill_clause),
        control_event_count=_count(session, OperatorControlEventRecord),
        instruction_set_cancellation_count=_count(session, InstructionSetCancellationRecord),
        reconciliation_issue_count=_count(
            session,
            ReconciliationIssueRecord,
            reconciliation_clause,
        ),
    )


def _build_instruction_events(
    session: Session,
    *,
    focus_record: InstructionRecord | None,
    limit: int,
) -> tuple[LedgerInstructionEvent, ...]:
    statement = (
        select(InstructionEventRecord, InstructionRecord)
        .join(InstructionRecord, InstructionRecord.id == InstructionEventRecord.instruction_id)
        .order_by(InstructionEventRecord.event_at.desc(), InstructionEventRecord.id.desc())
        .limit(limit)
    )
    if focus_record is not None:
        statement = statement.where(InstructionEventRecord.instruction_id == focus_record.id)

    rows = session.execute(statement).all()
    return tuple(
        LedgerInstructionEvent(
            event_id=event.id,
            instruction_id=instruction.instruction_id,
            symbol=instruction.symbol,
            account_key=instruction.account_key,
            batch_id=instruction.batch_id,
            event_type=event.event_type,
            source=event.source,
            event_at=event.event_at,
            state_before=event.state_before,
            state_after=event.state_after,
            note=event.note,
            payload=event.payload if isinstance(event.payload, dict) else {},
        )
        for event, instruction in rows
    )


def _build_broker_order_events(
    session: Session,
    *,
    focus_record: InstructionRecord | None,
    limit: int,
) -> tuple[LedgerBrokerOrderEvent, ...]:
    statement = (
        select(BrokerOrderEventRecord, BrokerOrderRecord, InstructionRecord)
        .join(BrokerOrderRecord, BrokerOrderRecord.id == BrokerOrderEventRecord.broker_order_id)
        .outerjoin(InstructionRecord, InstructionRecord.id == BrokerOrderRecord.instruction_id)
        .order_by(BrokerOrderEventRecord.event_at.desc(), BrokerOrderEventRecord.id.desc())
        .limit(limit)
    )
    if focus_record is not None:
        statement = statement.where(BrokerOrderRecord.instruction_id == focus_record.id)

    rows = session.execute(statement).all()
    return tuple(
        LedgerBrokerOrderEvent(
            event_id=event.id,
            broker_order_id=order.id,
            instruction_id=instruction.instruction_id if instruction is not None else None,
            account_key=order.account_key,
            symbol=order.symbol,
            order_role=order.order_role,
            external_order_id=order.external_order_id,
            order_ref=order.order_ref,
            event_type=event.event_type,
            event_at=event.event_at,
            status_before=event.status_before,
            status_after=event.status_after,
            message=_extract_broker_order_event_message(event, order),
            note=event.note,
            payload=event.payload if isinstance(event.payload, dict) else {},
        )
        for event, order, instruction in rows
    )


def _build_recent_fills(
    session: Session,
    *,
    focus_record: InstructionRecord | None,
    limit: int,
) -> tuple[LedgerExecutionFill, ...]:
    statement = (
        select(ExecutionFillRecord, InstructionRecord)
        .outerjoin(InstructionRecord, InstructionRecord.id == ExecutionFillRecord.instruction_id)
        .order_by(ExecutionFillRecord.executed_at.desc(), ExecutionFillRecord.id.desc())
        .limit(limit)
    )
    if focus_record is not None:
        statement = statement.where(ExecutionFillRecord.instruction_id == focus_record.id)

    rows = session.execute(statement).all()
    return tuple(
        LedgerExecutionFill(
            fill_id=fill.id,
            instruction_id=instruction.instruction_id if instruction is not None else None,
            broker_order_id=fill.broker_order_id,
            account_key=fill.account_key,
            symbol=fill.symbol,
            side=fill.side,
            quantity=fill.quantity,
            price=fill.price,
            commission=fill.commission,
            commission_currency=fill.commission_currency,
            executed_at=fill.executed_at,
            external_execution_id=fill.external_execution_id,
            external_order_id=fill.external_order_id,
            order_ref=fill.order_ref,
        )
        for fill, instruction in rows
    )


def _build_control_events(
    session: Session,
    *,
    limit: int,
) -> tuple[LedgerControlEvent, ...]:
    rows = session.execute(
        select(OperatorControlEventRecord, OperatorControlRecord)
        .join(
            OperatorControlRecord,
            OperatorControlRecord.id == OperatorControlEventRecord.operator_control_id,
        )
        .order_by(
            OperatorControlEventRecord.event_at.desc(),
            OperatorControlEventRecord.id.desc(),
        )
        .limit(limit)
    ).all()
    return tuple(
        LedgerControlEvent(
            event_id=event.id,
            control_key=control.control_key,
            event_type=event.event_type,
            source=event.source,
            event_at=event.event_at,
            enabled=event.enabled,
            updated_by=event.updated_by,
            reason=event.reason,
            note=event.note,
            payload=event.payload if isinstance(event.payload, dict) else {},
        )
        for event, control in rows
    )


def _cancellation_matches_instruction(
    cancellation: InstructionSetCancellationRecord,
    *,
    instruction_id: str,
) -> bool:
    selectors = cancellation.selectors if isinstance(cancellation.selectors, dict) else {}
    selector_instruction_ids = selectors.get("instruction_ids")
    if isinstance(selector_instruction_ids, list) and instruction_id in selector_instruction_ids:
        return True

    result_payload = (
        cancellation.result_payload if isinstance(cancellation.result_payload, dict) else {}
    )
    results = result_payload.get("results")
    if isinstance(results, list):
        for item in results:
            if isinstance(item, dict) and item.get("instruction_id") == instruction_id:
                return True
    return False


def _build_instruction_set_cancellations(
    session: Session,
    *,
    focus_instruction_id: str | None,
    limit: int,
) -> tuple[LedgerInstructionSetCancellation, ...]:
    rows = list(
        session.execute(
            select(InstructionSetCancellationRecord)
            .order_by(
                InstructionSetCancellationRecord.requested_at.desc(),
                InstructionSetCancellationRecord.id.desc(),
            )
            .limit(limit * 8 if focus_instruction_id is not None else limit)
        ).scalars()
    )
    if focus_instruction_id is not None:
        rows = [
            row
            for row in rows
            if _cancellation_matches_instruction(row, instruction_id=focus_instruction_id)
        ]

    return tuple(
        LedgerInstructionSetCancellation(
            request_id=row.id,
            requested_at=row.requested_at,
            requested_by=row.requested_by,
            reason=row.reason,
            status=row.status,
            matched_instruction_count=row.matched_instruction_count,
            cancelled_pending_count=row.cancelled_pending_count,
            cancelled_submitted_count=row.cancelled_submitted_count,
            skipped_count=row.skipped_count,
            failed_count=row.failed_count,
            selectors=row.selectors if isinstance(row.selectors, dict) else {},
            result_payload=(
                row.result_payload if isinstance(row.result_payload, dict) else {}
            ),
        )
        for row in rows[:limit]
    )


def _build_reconciliation_issues(
    session: Session,
    *,
    focus_instruction_id: str | None,
    limit: int,
) -> tuple[LedgerReconciliationIssue, ...]:
    statement = (
        select(ReconciliationIssueRecord, ReconciliationRunRecord)
        .join(
            ReconciliationRunRecord,
            ReconciliationRunRecord.id == ReconciliationIssueRecord.reconciliation_run_id,
        )
        .order_by(
            ReconciliationIssueRecord.observed_at.desc(),
            ReconciliationIssueRecord.id.desc(),
        )
        .limit(limit)
    )
    if focus_instruction_id is not None:
        statement = statement.where(
            ReconciliationIssueRecord.instruction_id == focus_instruction_id
        )

    rows = session.execute(statement).all()
    return tuple(
        LedgerReconciliationIssue(
            issue_id=issue.id,
            reconciliation_run_id=issue.reconciliation_run_id,
            run_kind=run.run_kind,
            instruction_id=issue.instruction_id,
            stage=issue.stage,
            severity=issue.severity,
            message=issue.message,
            observed_at=issue.observed_at,
            payload=issue.payload if isinstance(issue.payload, dict) else {},
        )
        for issue, run in rows
    )


def build_ledger_dashboard_snapshot(
    session_factory: sessionmaker[Session],
    *,
    focus_instruction_id: str | None = None,
    instruction_event_limit: int = 100,
    order_event_limit: int = 100,
    fill_limit: int = 100,
    control_event_limit: int = 50,
    cancellation_limit: int = 50,
    reconciliation_issue_limit: int = 50,
) -> LedgerDashboardSnapshot:
    """Return a durable ledger-centric dashboard snapshot."""

    with session_scope(session_factory) as session:
        focus_record, focus_instruction = _read_focus_instruction(
            session,
            instruction_id=focus_instruction_id,
        )
        return LedgerDashboardSnapshot(
            generated_at=utc_now(),
            focus_instruction=focus_instruction,
            summary=_build_summary(session, focus_record=focus_record),
            instruction_events=_build_instruction_events(
                session,
                focus_record=focus_record,
                limit=instruction_event_limit,
            ),
            broker_order_events=_build_broker_order_events(
                session,
                focus_record=focus_record,
                limit=order_event_limit,
            ),
            recent_fills=_build_recent_fills(
                session,
                focus_record=focus_record,
                limit=fill_limit,
            ),
            control_events=_build_control_events(
                session,
                limit=control_event_limit,
            ),
            instruction_set_cancellations=_build_instruction_set_cancellations(
                session,
                focus_instruction_id=focus_instruction_id,
                limit=cancellation_limit,
            ),
            reconciliation_issues=_build_reconciliation_issues(
                session,
                focus_instruction_id=focus_instruction_id,
                limit=reconciliation_issue_limit,
            ),
        )
