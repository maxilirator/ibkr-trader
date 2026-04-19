from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from typing import Callable

from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import (
    InstructionEventRecord,
    InstructionRecord,
    InstructionSetCancellationRecord,
    OperatorControlEventRecord,
    OperatorControlRecord,
)
from ibkr_trader.orchestration.state_machine import ExecutionState

KILL_SWITCH_CONTROL_KEY = "GLOBAL_KILL_SWITCH"


class KillSwitchActiveError(RuntimeError):
    """Raised when a new entry action is blocked by the global kill switch."""


class InstructionSetCancellationSelectorError(ValueError):
    """Raised when an instruction-set cancellation request has no selectors."""


class InstructionSetCancellationNotFoundError(LookupError):
    """Raised when an instruction-set cancellation request matches no instructions."""


@dataclass(slots=True)
class OperatorControlEventStatus:
    event_id: int
    event_type: str
    source: str
    event_at: datetime
    enabled: bool
    reason: str | None
    updated_by: str | None
    payload: dict[str, Any]
    note: str | None


@dataclass(slots=True)
class KillSwitchStatus:
    record_id: int | None
    control_key: str
    enabled: bool
    reason: str | None
    updated_by: str | None
    last_changed_at: datetime | None
    created_at: datetime | None
    updated_at: datetime | None
    latest_event: OperatorControlEventStatus | None


@dataclass(slots=True)
class InstructionSetCancellationItemResult:
    instruction_id: str
    state_before: str
    state_after: str
    action: str
    error: str | None


@dataclass(slots=True)
class InstructionSetCancellationResult:
    request_id: int
    requested_at: datetime
    requested_by: str
    reason: str | None
    selectors: dict[str, Any]
    status: str
    matched_instruction_count: int
    cancelled_pending_count: int
    cancelled_submitted_count: int
    skipped_count: int
    failed_count: int
    results: tuple[InstructionSetCancellationItemResult, ...]


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


def serialize_kill_switch_status(payload: KillSwitchStatus) -> dict[str, Any]:
    return _serialize_for_json(asdict(payload))


def serialize_instruction_set_cancellation_result(
    payload: InstructionSetCancellationResult,
) -> dict[str, Any]:
    return _serialize_for_json(asdict(payload))


def _build_kill_switch_status(
    record: OperatorControlRecord | None,
    latest_event: OperatorControlEventRecord | None,
) -> KillSwitchStatus:
    if record is None:
        return KillSwitchStatus(
            record_id=None,
            control_key=KILL_SWITCH_CONTROL_KEY,
            enabled=False,
            reason=None,
            updated_by=None,
            last_changed_at=None,
            created_at=None,
            updated_at=None,
            latest_event=None,
        )

    event_status: OperatorControlEventStatus | None = None
    if latest_event is not None:
        event_status = OperatorControlEventStatus(
            event_id=latest_event.id,
            event_type=latest_event.event_type,
            source=latest_event.source,
            event_at=latest_event.event_at,
            enabled=latest_event.enabled,
            reason=latest_event.reason,
            updated_by=latest_event.updated_by,
            payload=latest_event.payload,
            note=latest_event.note,
        )

    return KillSwitchStatus(
        record_id=record.id,
        control_key=record.control_key,
        enabled=record.enabled,
        reason=record.reason,
        updated_by=record.updated_by,
        last_changed_at=record.last_changed_at,
        created_at=record.created_at,
        updated_at=record.updated_at,
        latest_event=event_status,
    )


def _read_kill_switch_record(
    session: Session,
) -> tuple[OperatorControlRecord | None, OperatorControlEventRecord | None]:
    record = session.execute(
        select(OperatorControlRecord).where(
            OperatorControlRecord.control_key == KILL_SWITCH_CONTROL_KEY
        )
    ).scalar_one_or_none()
    if record is None:
        return None, None

    latest_event = session.execute(
        select(OperatorControlEventRecord)
        .where(OperatorControlEventRecord.operator_control_id == record.id)
        .order_by(OperatorControlEventRecord.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    return record, latest_event


def read_kill_switch_state(
    session_factory: sessionmaker[Session],
) -> KillSwitchStatus:
    with session_scope(session_factory) as session:
        record, latest_event = _read_kill_switch_record(session)
        return _build_kill_switch_status(record, latest_event)


def kill_switch_is_enabled(session_factory: sessionmaker[Session]) -> bool:
    return read_kill_switch_state(session_factory).enabled


def assert_kill_switch_inactive(session_factory: sessionmaker[Session]) -> None:
    state = read_kill_switch_state(session_factory)
    if state.enabled:
        suffix = f" Reason: {state.reason}" if state.reason else ""
        raise KillSwitchActiveError(
            f"The global kill switch is enabled. New entry actions are blocked.{suffix}"
        )


def set_kill_switch_state(
    session_factory: sessionmaker[Session],
    *,
    enabled: bool,
    reason: str | None,
    updated_by: str,
    source: str = "api",
) -> KillSwitchStatus:
    event_at = utc_now()
    normalized_reason = reason.strip() if isinstance(reason, str) and reason.strip() else None

    with session_scope(session_factory) as session:
        record = session.execute(
            select(OperatorControlRecord)
            .where(OperatorControlRecord.control_key == KILL_SWITCH_CONTROL_KEY)
            .with_for_update()
        ).scalar_one_or_none()

        previous_enabled = False
        previous_reason = None
        previous_updated_by = None
        if record is None:
            record = OperatorControlRecord(
                control_key=KILL_SWITCH_CONTROL_KEY,
                enabled=enabled,
                reason=normalized_reason,
                updated_by=updated_by,
                last_changed_at=event_at,
            )
            session.add(record)
            session.flush()
        else:
            previous_enabled = record.enabled
            previous_reason = record.reason
            previous_updated_by = record.updated_by
            record.enabled = enabled
            record.reason = normalized_reason
            record.updated_by = updated_by
            record.last_changed_at = event_at
            session.flush()

        event = OperatorControlEventRecord(
            operator_control_id=record.id,
            event_type="kill_switch_enabled" if enabled else "kill_switch_disabled",
            source=source,
            event_at=event_at,
            enabled=enabled,
            reason=normalized_reason,
            updated_by=updated_by,
            payload={
                "previous_enabled": previous_enabled,
                "previous_reason": previous_reason,
                "previous_updated_by": previous_updated_by,
            },
            note="Operator updated the durable global kill switch.",
        )
        session.add(event)
        session.flush()

        return _build_kill_switch_status(record, event)


def _build_instruction_set_selector_statement(
    *,
    batch_id: str | None,
    account_key: str | None,
    book_key: str | None,
    instruction_ids: tuple[str, ...] | None,
) -> Select[tuple[InstructionRecord]]:
    statement: Select[tuple[InstructionRecord]] = select(InstructionRecord)
    if batch_id is not None:
        statement = statement.where(InstructionRecord.batch_id == batch_id)
    if account_key is not None:
        statement = statement.where(InstructionRecord.account_key == account_key)
    if book_key is not None:
        statement = statement.where(InstructionRecord.book_key == book_key)
    if instruction_ids is not None:
        statement = statement.where(InstructionRecord.instruction_id.in_(instruction_ids))
    return statement.order_by(InstructionRecord.id)


def _cancel_pending_instruction(
    session_factory: sessionmaker[Session],
    instruction_id: str,
    *,
    request_id: int,
    requested_by: str,
    reason: str | None,
) -> InstructionSetCancellationItemResult:
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == instruction_id)
            .with_for_update()
        ).scalar_one_or_none()
        if record is None:
            return InstructionSetCancellationItemResult(
                instruction_id=instruction_id,
                state_before="MISSING",
                state_after="MISSING",
                action="failed_missing_instruction",
                error=f"Persisted instruction '{instruction_id}' was not found.",
            )

        if record.state != ExecutionState.ENTRY_PENDING.value:
            return InstructionSetCancellationItemResult(
                instruction_id=instruction_id,
                state_before=record.state,
                state_after=record.state,
                action="skipped_not_entry_pending",
                error=None,
            )

        previous_state = record.state
        record.state = ExecutionState.ENTRY_CANCELLED.value
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type="instruction_set_cancelled",
                source="operator_control",
                event_at=utc_now(),
                state_before=previous_state,
                state_after=record.state,
                payload={
                    "request_id": request_id,
                    "requested_by": requested_by,
                    "reason": reason,
                },
                note="Operator cancelled the pending instruction through instruction-set control.",
            )
        )
        session.flush()

        return InstructionSetCancellationItemResult(
            instruction_id=instruction_id,
            state_before=previous_state,
            state_after=record.state,
            action="cancelled_pending_entry",
            error=None,
        )


def cancel_instruction_set(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    *,
    requested_by: str,
    reason: str | None = None,
    batch_id: str | None = None,
    account_key: str | None = None,
    book_key: str | None = None,
    instruction_ids: tuple[str, ...] | None = None,
    timeout: int = 10,
    canceler: Callable[..., dict[str, Any]] | None = None,
) -> InstructionSetCancellationResult:
    selectors = {
        key: value
        for key, value in {
            "batch_id": batch_id,
            "account_key": account_key,
            "book_key": book_key,
            "instruction_ids": list(instruction_ids) if instruction_ids is not None else None,
        }.items()
        if value is not None
    }
    if not selectors:
        raise InstructionSetCancellationSelectorError(
            "At least one selector is required to cancel an instruction set."
        )

    normalized_reason = reason.strip() if isinstance(reason, str) and reason.strip() else None

    with session_scope(session_factory) as session:
        matches = session.execute(
            _build_instruction_set_selector_statement(
                batch_id=batch_id,
                account_key=account_key,
                book_key=book_key,
                instruction_ids=instruction_ids,
            )
        ).scalars().all()
        if not matches:
            raise InstructionSetCancellationNotFoundError(
                "No persisted instructions matched the requested cancellation selectors."
            )

        request = InstructionSetCancellationRecord(
            requested_at=utc_now(),
            requested_by=requested_by,
            reason=normalized_reason,
            selectors=_serialize_for_json(selectors),
            status="RUNNING",
        )
        session.add(request)
        session.flush()
        request_id = request.id
        requested_at = request.requested_at
        matched_instruction_ids = tuple(record.instruction_id for record in matches)

    item_results: list[InstructionSetCancellationItemResult] = []

    for instruction_id in matched_instruction_ids:
        with session_scope(session_factory) as session:
            record = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == instruction_id
                )
            ).scalar_one_or_none()

        if record is None:
            item_results.append(
                InstructionSetCancellationItemResult(
                    instruction_id=instruction_id,
                    state_before="MISSING",
                    state_after="MISSING",
                    action="failed_missing_instruction",
                    error=f"Persisted instruction '{instruction_id}' was not found.",
                )
            )
            continue

        if record.state == ExecutionState.ENTRY_PENDING.value:
            item_results.append(
                _cancel_pending_instruction(
                    session_factory,
                    instruction_id,
                    request_id=request_id,
                    requested_by=requested_by,
                    reason=normalized_reason,
                )
            )
            continue

        if record.state == ExecutionState.ENTRY_SUBMITTED.value:
            try:
                from ibkr_trader.orchestration.entry_submission import (
                    cancel_persisted_instruction_entry,
                )

                cancellation = cancel_persisted_instruction_entry(
                    session_factory,
                    broker_config,
                    instruction_id,
                    timeout=timeout,
                    canceler=canceler,
                )
                item_results.append(
                    InstructionSetCancellationItemResult(
                        instruction_id=instruction_id,
                        state_before=ExecutionState.ENTRY_SUBMITTED.value,
                        state_after=cancellation.state,
                        action="cancelled_submitted_entry",
                        error=None,
                    )
                )
            except Exception as exc:  # pragma: no cover - runtime-side error propagation
                item_results.append(
                    InstructionSetCancellationItemResult(
                        instruction_id=instruction_id,
                        state_before=record.state,
                        state_after=record.state,
                        action="failed_submitted_entry_cancellation",
                        error=str(exc),
                    )
                )
            continue

        item_results.append(
            InstructionSetCancellationItemResult(
                instruction_id=instruction_id,
                state_before=record.state,
                state_after=record.state,
                action="skipped_non_entry_state",
                error=None,
            )
        )

    cancelled_pending_count = sum(
        1 for item in item_results if item.action == "cancelled_pending_entry"
    )
    cancelled_submitted_count = sum(
        1 for item in item_results if item.action == "cancelled_submitted_entry"
    )
    failed_count = sum(1 for item in item_results if item.error is not None)
    skipped_count = sum(
        1
        for item in item_results
        if item.error is None
        and item.action in {"skipped_not_entry_pending", "skipped_non_entry_state"}
    )
    if failed_count == 0:
        status = "COMPLETED"
    elif failed_count == len(item_results):
        status = "FAILED"
    else:
        status = "PARTIAL_FAILURE"

    with session_scope(session_factory) as session:
        request = session.execute(
            select(InstructionSetCancellationRecord)
            .where(InstructionSetCancellationRecord.id == request_id)
            .with_for_update()
        ).scalar_one()
        request.status = status
        request.matched_instruction_count = len(item_results)
        request.cancelled_pending_count = cancelled_pending_count
        request.cancelled_submitted_count = cancelled_submitted_count
        request.skipped_count = skipped_count
        request.failed_count = failed_count
        request.result_payload = {
            "results": [_serialize_for_json(asdict(item)) for item in item_results]
        }
        session.flush()

    return InstructionSetCancellationResult(
        request_id=request_id,
        requested_at=requested_at,
        requested_by=requested_by,
        reason=normalized_reason,
        selectors=_serialize_for_json(selectors),
        status=status,
        matched_instruction_count=len(item_results),
        cancelled_pending_count=cancelled_pending_count,
        cancelled_submitted_count=cancelled_submitted_count,
        skipped_count=skipped_count,
        failed_count=failed_count,
        results=tuple(item_results),
    )
