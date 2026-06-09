from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.domain.execution_payloads import parse_execution_instruction_payload
from ibkr_trader.orchestration.intent_replacement import intent_group_key_for_record
from ibkr_trader.orchestration.scheduling import (
    resolve_effective_entry_expire_at_for_schedule,
)
from ibkr_trader.orchestration.scheduling import resolve_scheduled_submission_due_at
from ibkr_trader.orchestration.session_calendar import find_session_for_date
from ibkr_trader.orchestration.state_machine import ExecutionState
from ibkr_trader.orchestration.runtime_types import ensure_utc
from ibkr_trader.virtual.execution import has_real_broker_work


BROKER_RUNTIME_SESSION_OPEN_GRACE = timedelta(minutes=10)
BROKER_RUNTIME_SESSION_CLOSE_GRACE = timedelta(minutes=10)
SESSION_GATED_RECONCILIATION_RUN_KINDS = {
    "runtime_cycle",
    "startup_reconciliation",
}


def fetch_instruction_ids(
    session_factory: sessionmaker[Session],
    *,
    states: tuple[str, ...],
    submit_before: datetime | None = None,
    instruction_ids: tuple[str, ...] | None = None,
) -> list[str]:
    """Fetch instruction ids in deterministic runtime order."""

    with session_scope(session_factory) as session:
        query = select(InstructionRecord.instruction_id).where(
            InstructionRecord.state.in_(states)
        )
        if instruction_ids:
            query = query.where(InstructionRecord.instruction_id.in_(instruction_ids))
        if submit_before is not None:
            query = query.where(InstructionRecord.submit_at <= submit_before)
        return list(
            session.execute(
                query.order_by(InstructionRecord.submit_at, InstructionRecord.id)
            ).scalars()
        )


def fetch_instruction_account_keys(
    session_factory: sessionmaker[Session],
    instruction_ids: list[str],
) -> dict[str, str]:
    """Return account keys for a set of instruction ids without changing state."""

    if not instruction_ids:
        return {}
    with session_scope(session_factory) as session:
        rows = session.execute(
            select(InstructionRecord.instruction_id, InstructionRecord.account_key).where(
                InstructionRecord.instruction_id.in_(instruction_ids)
            )
        ).all()
    return {instruction_id: account_key for instruction_id, account_key in rows}


def split_instruction_ids_by_virtual(
    session_factory: sessionmaker[Session],
    instruction_ids: list[str],
) -> tuple[list[str], list[str]]:
    """Partition instruction ids into virtual and real-broker work."""

    if not instruction_ids:
        return [], []
    with session_scope(session_factory) as session:
        rows = session.execute(
            select(InstructionRecord.instruction_id, InstructionRecord.is_virtual).where(
                InstructionRecord.instruction_id.in_(instruction_ids)
            )
        ).all()
    virtual_ids: list[str] = []
    real_ids: list[str] = []
    virtual_by_instruction = {
        instruction_id: bool(is_virtual)
        for instruction_id, is_virtual in rows
    }
    for instruction_id in instruction_ids:
        if virtual_by_instruction.get(instruction_id):
            virtual_ids.append(instruction_id)
        else:
            real_ids.append(instruction_id)
    return virtual_ids, real_ids


def should_touch_real_broker_for_runtime_cycle(
    session_factory: sessionmaker[Session],
    *,
    run_kind: str,
    cycle_at: datetime,
    runtime_timezone: str,
    session_calendar_path: Path,
    instruction_ids: tuple[str, ...] | None,
    due_instruction_ids: list[str],
    expired_submitted_entry_instruction_ids: list[str],
) -> bool:
    """Decide whether the cycle has real broker work worth opening an IBKR session for."""

    if run_kind not in SESSION_GATED_RECONCILIATION_RUN_KINDS:
        return has_real_broker_work(session_factory, instruction_ids=instruction_ids)

    _, real_due_instruction_ids = split_instruction_ids_by_virtual(
        session_factory,
        due_instruction_ids,
    )
    if real_due_instruction_ids:
        return True
    _, real_expired_entry_instruction_ids = split_instruction_ids_by_virtual(
        session_factory,
        expired_submitted_entry_instruction_ids,
    )
    if real_expired_entry_instruction_ids:
        return True

    if not has_real_broker_work(session_factory, instruction_ids=instruction_ids):
        return False

    return _is_runtime_broker_session_window(
        cycle_at=cycle_at,
        runtime_timezone=runtime_timezone,
        session_calendar_path=session_calendar_path,
    )


def should_reconcile_active_runtime_instructions(
    *,
    run_kind: str,
    cycle_at: datetime,
    runtime_timezone: str,
    session_calendar_path: Path,
    due_instruction_ids: list[str],
    expired_submitted_entry_instruction_ids: list[str],
) -> bool:
    """Allow active reconciliation inside the trading-session window or when work is due."""

    if run_kind not in SESSION_GATED_RECONCILIATION_RUN_KINDS:
        return True
    if due_instruction_ids:
        return True
    if expired_submitted_entry_instruction_ids:
        return True
    return _is_runtime_broker_session_window(
        cycle_at=cycle_at,
        runtime_timezone=runtime_timezone,
        session_calendar_path=session_calendar_path,
    )


def has_virtual_runtime_work(
    session_factory: sessionmaker[Session],
) -> bool:
    """Return whether any virtual instruction still needs runtime attention."""

    with session_scope(session_factory) as session:
        row = session.execute(
            select(InstructionRecord.id)
            .where(
                InstructionRecord.is_virtual.is_(True),
                InstructionRecord.state.in_(
                    (
                        ExecutionState.ENTRY_PENDING.value,
                        ExecutionState.ENTRY_SUBMITTED.value,
                        ExecutionState.POSITION_OPEN.value,
                        ExecutionState.EXIT_PENDING.value,
                    )
                ),
            )
            .limit(1)
        ).first()
    return row is not None


def fetch_due_entry_instruction_ids(
    session_factory: sessionmaker[Session],
    *,
    cycle_at: datetime,
    session_calendar_path: Path,
    submission_lead_time: timedelta,
    instruction_ids: tuple[str, ...] | None = None,
) -> list[str]:
    """Fetch pending entries whose submission window is active for this cycle."""

    candidate_cutoff = cycle_at + submission_lead_time
    with session_scope(session_factory) as session:
        query = select(InstructionRecord).where(
            InstructionRecord.state == ExecutionState.ENTRY_PENDING.value,
            InstructionRecord.submit_at <= candidate_cutoff,
        )
        if instruction_ids:
            query = query.where(InstructionRecord.instruction_id.in_(instruction_ids))
        records = list(
            session.execute(
                query.order_by(InstructionRecord.submit_at, InstructionRecord.id)
            ).scalars()
        )

    return [
        record.instruction_id
        for record in records
        if _is_entry_submission_due(
            record,
            cycle_at=cycle_at,
            session_calendar_path=session_calendar_path,
            submission_lead_time=submission_lead_time,
        )
    ]


def promote_due_reentry_waiting_for_flat(
    session_factory: sessionmaker[Session],
    *,
    cycle_at: datetime,
    session_calendar_path: Path,
    submission_lead_time: timedelta,
    instruction_ids: tuple[str, ...] | None = None,
) -> list[str]:
    """Promote deferred same-symbol entries once their previous position is flat."""

    cycle_at = ensure_utc(cycle_at) or cycle_at
    candidate_cutoff = cycle_at + submission_lead_time
    promoted_instruction_ids: list[str] = []
    with session_scope(session_factory) as session:
        query = (
            select(InstructionRecord)
            .where(
                InstructionRecord.state
                == ExecutionState.REENTRY_WAITING_FOR_FLAT.value,
                InstructionRecord.submit_at <= candidate_cutoff,
                InstructionRecord.archived_at.is_(None),
            )
            .order_by(InstructionRecord.submit_at, InstructionRecord.id)
            .with_for_update()
        )
        if instruction_ids:
            query = query.where(InstructionRecord.instruction_id.in_(instruction_ids))
        records = list(session.execute(query).scalars())

        for record in records:
            expired = _is_entry_record_expired(
                record,
                cycle_at=cycle_at,
                session_calendar_path=session_calendar_path,
            )
            due = _is_entry_submission_due(
                record,
                cycle_at=cycle_at,
                session_calendar_path=session_calendar_path,
                submission_lead_time=submission_lead_time,
            )
            if not expired and not due:
                continue

            blocking_instruction_ids = _same_group_active_position_instruction_ids(
                session,
                record,
            )
            if blocking_instruction_ids:
                if expired:
                    _transition_reentry_waiting_for_flat(
                        session,
                        record,
                        cycle_at=cycle_at,
                        state_after=ExecutionState.ENTRY_CANCELLED.value,
                        event_type="reentry_waiting_for_flat_expired",
                        note=(
                            "Deferred re-entry expired before the previous same-group "
                            "position reached flat."
                        ),
                        payload={"blocked_by_instruction_ids": blocking_instruction_ids},
                    )
                continue

            if expired:
                _transition_reentry_waiting_for_flat(
                    session,
                    record,
                    cycle_at=cycle_at,
                    state_after=ExecutionState.ENTRY_CANCELLED.value,
                    event_type="reentry_waiting_for_flat_expired",
                    note=(
                        "Deferred re-entry was flat-ready only after its entry window "
                        "had expired."
                    ),
                    payload={"blocked_by_instruction_ids": []},
                )
                continue

            _transition_reentry_waiting_for_flat(
                session,
                record,
                cycle_at=cycle_at,
                state_after=ExecutionState.ENTRY_PENDING.value,
                event_type="reentry_waiting_for_flat_promoted",
                note=(
                    "Deferred re-entry promoted after no active same-group position "
                    "remained in the durable lifecycle."
                ),
                payload={"blocked_by_instruction_ids": []},
            )
            promoted_instruction_ids.append(record.instruction_id)

    return promoted_instruction_ids


def fetch_expired_submitted_entry_instruction_ids(
    session_factory: sessionmaker[Session],
    *,
    cycle_at: datetime,
    session_calendar_path: Path,
    instruction_ids: tuple[str, ...] | None = None,
) -> list[str]:
    """Find submitted entry orders that should be cancelled because the window expired."""

    with session_scope(session_factory) as session:
        query = select(InstructionRecord).where(
            InstructionRecord.state == ExecutionState.ENTRY_SUBMITTED.value,
            InstructionRecord.expire_at <= cycle_at,
            InstructionRecord.archived_at.is_(None),
        )
        if instruction_ids:
            query = query.where(InstructionRecord.instruction_id.in_(instruction_ids))
        records = list(
            session.execute(
                query.order_by(InstructionRecord.expire_at, InstructionRecord.id)
            ).scalars()
        )

    return [
        record.instruction_id
        for record in records
        if _should_cancel_submitted_entry_at_expiry(record)
        and _is_entry_record_expired(
            record,
            cycle_at=cycle_at,
            session_calendar_path=session_calendar_path,
        )
    ]


def _same_group_active_position_instruction_ids(
    session: Session,
    record: InstructionRecord,
) -> list[str]:
    group_key = intent_group_key_for_record(record)
    candidates = session.execute(
        select(InstructionRecord).where(
            InstructionRecord.id != record.id,
            InstructionRecord.account_key == record.account_key,
            InstructionRecord.book_key == record.book_key,
            InstructionRecord.symbol == record.symbol,
            InstructionRecord.exchange == record.exchange,
            InstructionRecord.currency == record.currency,
            InstructionRecord.state.in_(
                (
                    ExecutionState.POSITION_OPEN.value,
                    ExecutionState.EXIT_PENDING.value,
                )
            ),
            InstructionRecord.archived_at.is_(None),
        )
    ).scalars()
    return [
        candidate.instruction_id
        for candidate in candidates
        if intent_group_key_for_record(candidate) == group_key
    ]


def _transition_reentry_waiting_for_flat(
    session: Session,
    record: InstructionRecord,
    *,
    cycle_at: datetime,
    state_after: str,
    event_type: str,
    note: str,
    payload: dict[str, object],
) -> None:
    state_before = record.state
    record.state = state_after
    record.updated_at = cycle_at
    session.add(
        InstructionEventRecord(
            instruction_id=record.id,
            event_type=event_type,
            source="runtime",
            event_at=cycle_at,
            state_before=state_before,
            state_after=state_after,
            payload=payload,
            note=note,
        )
    )


def is_pending_entry_expired(
    session_factory: sessionmaker[Session],
    *,
    instruction_id: str,
    cycle_at: datetime,
    session_calendar_path: Path,
) -> bool:
    """Check whether a pending entry expired before a submit attempt."""

    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord).where(
                InstructionRecord.instruction_id == instruction_id
            )
        ).scalar_one_or_none()
    if record is None:
        return False
    return _is_entry_record_expired(
        record,
        cycle_at=cycle_at,
        session_calendar_path=session_calendar_path,
    )


def _is_runtime_broker_session_window(
    *,
    cycle_at: datetime,
    runtime_timezone: str,
    session_calendar_path: Path,
) -> bool:
    try:
        runtime_zone = ZoneInfo(runtime_timezone)
        local_cycle_at = cycle_at.astimezone(runtime_zone)
        session = find_session_for_date(
            local_cycle_at.date(),
            session_calendar_path=session_calendar_path,
        )
    except (FileNotFoundError, ValueError):
        return True

    if session is None:
        return False

    open_at = session.open_at.astimezone(timezone.utc) - BROKER_RUNTIME_SESSION_OPEN_GRACE
    close_at = session.close_at.astimezone(timezone.utc) + BROKER_RUNTIME_SESSION_CLOSE_GRACE
    return open_at <= cycle_at.astimezone(timezone.utc) <= close_at


def _is_entry_submission_due(
    record: InstructionRecord,
    *,
    cycle_at: datetime,
    session_calendar_path: Path,
    submission_lead_time: timedelta,
) -> bool:
    submit_at = ensure_utc(record.submit_at)
    if submit_at is None:
        return False
    if submit_at <= cycle_at:
        return True
    if submit_at > cycle_at + submission_lead_time:
        return False

    try:
        instruction = _instruction_payload(record)
    except Exception:
        return False

    due_at = resolve_scheduled_submission_due_at(
        instruction,
        scheduled_at=instruction.entry.submit_at,
        session_calendar_path=session_calendar_path,
        submission_lead_time=submission_lead_time,
    )
    return due_at <= cycle_at


def _effective_entry_expire_at(
    record: InstructionRecord,
    *,
    session_calendar_path: Path,
) -> datetime:
    try:
        instruction = _instruction_payload(record)
        return resolve_effective_entry_expire_at_for_schedule(
            instruction,
            submit_at=ensure_utc(record.submit_at) or record.submit_at,
            expire_at=ensure_utc(record.expire_at) or record.expire_at,
            session_calendar_path=session_calendar_path,
        )
    except Exception:
        return record.expire_at


def _is_entry_record_expired(
    record: InstructionRecord,
    *,
    cycle_at: datetime,
    session_calendar_path: Path,
) -> bool:
    normalized_expire_at = ensure_utc(
        _effective_entry_expire_at(
            record,
            session_calendar_path=session_calendar_path,
        )
    )
    if normalized_expire_at is None:
        return False
    return normalized_expire_at <= cycle_at


def _should_cancel_submitted_entry_at_expiry(record: InstructionRecord) -> bool:
    try:
        instruction = _instruction_payload(record)
    except Exception:
        return True
    return bool(instruction.entry.cancel_unfilled_at_expiry)


def _instruction_payload(record: InstructionRecord) -> ExecutionInstruction:
    raw_instruction_payload = record.payload.get("instruction")
    if not isinstance(raw_instruction_payload, dict):
        raise ValueError(
            f"Instruction '{record.instruction_id}' does not contain a valid persisted payload."
        )
    return parse_execution_instruction_payload(raw_instruction_payload)
