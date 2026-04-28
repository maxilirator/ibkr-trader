from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import InstrumentRecord
from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.domain.execution_contract import ExecutionInstructionBatch
from ibkr_trader.orchestration.operator_controls import assert_kill_switch_inactive
from ibkr_trader.orchestration.scheduling import BatchRuntimeSchedule
from ibkr_trader.orchestration.scheduling import InstructionRuntimeSchedule
from ibkr_trader.orchestration.scheduling import build_batch_runtime_schedule
from ibkr_trader.orchestration.state_machine import ExecutionState
from ibkr_trader.virtual.accounts import is_virtual_account_key


class SubmissionConflictError(ValueError):
    """Raised when submitted instructions would violate uniqueness constraints."""


@dataclass(slots=True)
class SubmittedInstructionEvent:
    event_id: int
    event_type: str
    source: str
    event_at: datetime
    state_before: str | None
    state_after: str | None
    payload: dict[str, Any]
    note: str | None


@dataclass(slots=True)
class SubmittedInstruction:
    record_id: int
    instruction_id: str
    state: str
    created_at: datetime
    updated_at: datetime
    submit_at: datetime
    expire_at: datetime
    account_key: str
    book_key: str
    symbol: str
    exchange: str
    currency: str
    order_type: str
    side: str
    runtime_schedule: dict[str, Any]
    initial_event: SubmittedInstructionEvent


@dataclass(slots=True)
class SubmittedBatch:
    schema_version: str
    batch_id: str
    source_system: str
    runtime_timezone: str
    instruction_count: int
    instructions: tuple[SubmittedInstruction, ...]


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


def _serialize_instruction_payload(
    batch: ExecutionInstructionBatch,
    instruction: ExecutionInstruction,
) -> dict[str, Any]:
    return _serialize_for_json(
        {
            "schema_version": batch.schema_version,
            "source": asdict(batch.source),
            "instruction": asdict(instruction),
        }
    )


def _serialize_runtime_schedule(schedule: InstructionRuntimeSchedule) -> dict[str, Any]:
    return _serialize_for_json(asdict(schedule))


def _ensure_unique_instruction_ids(batch: ExecutionInstructionBatch) -> None:
    seen: set[str] = set()
    duplicates: list[str] = []
    for instruction in batch.instructions:
        if instruction.instruction_id in seen:
            duplicates.append(instruction.instruction_id)
        seen.add(instruction.instruction_id)

    if duplicates:
        duplicate_list = ", ".join(sorted(set(duplicates)))
        raise SubmissionConflictError(
            f"Duplicate instruction_id values in batch: {duplicate_list}"
        )


def _event_from_record(event: InstructionEventRecord) -> SubmittedInstructionEvent:
    return SubmittedInstructionEvent(
        event_id=event.id,
        event_type=event.event_type,
        source=event.source,
        event_at=event.event_at,
        state_before=event.state_before,
        state_after=event.state_after,
        payload=event.payload,
        note=event.note,
    )


def _fallback_initial_event(
    record: InstructionRecord,
    runtime_schedule: InstructionRuntimeSchedule,
) -> SubmittedInstructionEvent:
    return SubmittedInstructionEvent(
        event_id=0,
        event_type="instruction_submitted",
        source="api",
        event_at=record.created_at,
        state_before=None,
        state_after=record.state,
        payload={"runtime_schedule": _serialize_runtime_schedule(runtime_schedule)},
        note="Existing instruction replayed idempotently.",
    )


def _instruction_from_record(
    record: InstructionRecord,
    runtime_schedule: InstructionRuntimeSchedule,
    initial_event: SubmittedInstructionEvent,
) -> SubmittedInstruction:
    return SubmittedInstruction(
        record_id=record.id,
        instruction_id=record.instruction_id,
        state=record.state,
        created_at=record.created_at,
        updated_at=record.updated_at,
        submit_at=record.submit_at,
        expire_at=record.expire_at,
        account_key=record.account_key,
        book_key=record.book_key,
        symbol=record.symbol,
        exchange=record.exchange,
        currency=record.currency,
        order_type=record.order_type,
        side=record.side,
        runtime_schedule=_serialize_runtime_schedule(runtime_schedule),
        initial_event=initial_event,
    )


def _build_idempotent_replay_batch(
    session: Session,
    batch: ExecutionInstructionBatch,
    schedule: BatchRuntimeSchedule,
    *,
    runtime_timezone: str,
) -> SubmittedBatch | None:
    instruction_ids = [instruction.instruction_id for instruction in batch.instructions]
    records = session.execute(
        select(InstructionRecord).where(
            InstructionRecord.instruction_id.in_(instruction_ids)
        )
    ).scalars().all()
    if not records:
        return None

    records_by_instruction_id = {record.instruction_id: record for record in records}
    missing = [
        instruction_id
        for instruction_id in instruction_ids
        if instruction_id not in records_by_instruction_id
    ]
    if missing:
        existing_list = ", ".join(sorted(records_by_instruction_id))
        missing_list = ", ".join(sorted(missing))
        raise SubmissionConflictError(
            "instruction_id already exists for part of the batch: "
            f"existing={existing_list}; missing={missing_list}"
        )

    mismatched = [
        instruction.instruction_id
        for instruction in batch.instructions
        if records_by_instruction_id[instruction.instruction_id].payload
        != _serialize_instruction_payload(batch, instruction)
    ]
    if mismatched:
        mismatch_list = ", ".join(sorted(mismatched))
        raise SubmissionConflictError(
            f"instruction_id already exists with different payload: {mismatch_list}"
        )

    record_ids = [record.id for record in records_by_instruction_id.values()]
    initial_events = session.execute(
        select(InstructionEventRecord)
        .where(
            InstructionEventRecord.instruction_id.in_(record_ids),
            InstructionEventRecord.event_type == "instruction_submitted",
        )
        .order_by(InstructionEventRecord.id)
    ).scalars().all()
    events_by_record_id: dict[int, InstructionEventRecord] = {}
    for event in initial_events:
        events_by_record_id.setdefault(event.instruction_id, event)

    replayed: list[SubmittedInstruction] = []
    for instruction, runtime_schedule in zip(
        batch.instructions,
        schedule.instructions,
        strict=True,
    ):
        record = records_by_instruction_id[instruction.instruction_id]
        event = events_by_record_id.get(record.id)
        initial_event = (
            _event_from_record(event)
            if event is not None
            else _fallback_initial_event(record, runtime_schedule)
        )
        replayed.append(
            _instruction_from_record(record, runtime_schedule, initial_event)
        )

    return SubmittedBatch(
        schema_version=batch.schema_version,
        batch_id=batch.source.batch_id,
        source_system=batch.source.system,
        runtime_timezone=runtime_timezone,
        instruction_count=len(replayed),
        instructions=tuple(replayed),
    )


def _upsert_instrument(
    session: Session,
    instruction: ExecutionInstruction,
) -> InstrumentRecord:
    instrument = session.execute(
        select(InstrumentRecord).where(
            InstrumentRecord.symbol == instruction.instrument.symbol,
            InstrumentRecord.exchange == instruction.instrument.exchange,
            InstrumentRecord.currency == instruction.instrument.currency,
            InstrumentRecord.security_type == instruction.instrument.security_type.value,
        )
    ).scalar_one_or_none()

    if instrument is None:
        instrument = InstrumentRecord(
            symbol=instruction.instrument.symbol,
            exchange=instruction.instrument.exchange,
            currency=instruction.instrument.currency,
            security_type=instruction.instrument.security_type.value,
        )
        session.add(instrument)

    instrument.primary_exchange = instruction.instrument.primary_exchange
    instrument.company_name = instruction.trace.company_name
    instrument.isin = instruction.instrument.isin
    instrument.aliases = list(instruction.instrument.aliases)
    return instrument


def submit_execution_batch(
    session_factory: sessionmaker[Session],
    batch: ExecutionInstructionBatch,
    *,
    runtime_timezone: str,
    session_calendar_path: Path,
) -> SubmittedBatch:
    batch.validate()
    _ensure_unique_instruction_ids(batch)

    schedule = build_batch_runtime_schedule(
        batch,
        runtime_timezone=runtime_timezone,
        session_calendar_path=session_calendar_path,
    )

    with session_scope(session_factory) as session:
        replayed_batch = _build_idempotent_replay_batch(
            session,
            batch,
            schedule,
            runtime_timezone=runtime_timezone,
        )
        if replayed_batch is not None:
            return replayed_batch

    if any(not instruction.is_model_routed for instruction in batch.instructions):
        assert_kill_switch_inactive(session_factory)

    persisted_instructions: list[SubmittedInstruction] = []

    with session_scope(session_factory) as session:
        replayed_batch = _build_idempotent_replay_batch(
            session,
            batch,
            schedule,
            runtime_timezone=runtime_timezone,
        )
        if replayed_batch is not None:
            return replayed_batch

        for instruction, runtime_schedule in zip(
            batch.instructions,
            schedule.instructions,
            strict=True,
        ):
            _upsert_instrument(session, instruction)
            is_model_routed = instruction.is_model_routed
            initial_state = (
                ExecutionState.MODEL_ROUTED_PENDING.value
                if is_model_routed
                else ExecutionState.ENTRY_PENDING.value
            )
            if is_model_routed:
                if instruction.execution is None:
                    raise ValueError("execution is required for model-routed instructions")
                submit_at = instruction.execution.window.start_at
                expire_at = instruction.execution.window.end_at
                order_type = "MODEL_ROUTED"
            else:
                if instruction.entry is None:
                    raise ValueError("entry must be an object")
                submit_at = instruction.entry.submit_at
                expire_at = instruction.entry.expire_at
                order_type = instruction.entry.order_type.value

            instruction_record = InstructionRecord(
                instruction_id=instruction.instruction_id,
                schema_version=batch.schema_version,
                source_system=batch.source.system,
                batch_id=batch.source.batch_id,
                account_key=instruction.account.account_key,
                book_key=instruction.account.book_key,
                is_virtual=is_virtual_account_key(instruction.account.account_key),
                symbol=instruction.instrument.symbol,
                exchange=instruction.instrument.exchange,
                currency=instruction.instrument.currency,
                state=initial_state,
                submit_at=submit_at,
                expire_at=expire_at,
                order_type=order_type,
                side=instruction.intent.side,
                payload=_serialize_instruction_payload(batch, instruction),
            )
            session.add(instruction_record)
            session.flush()

            initial_event = InstructionEventRecord(
                instruction_id=instruction_record.id,
                event_type="instruction_submitted",
                source="api",
                state_before=None,
                state_after=initial_state,
                payload={"runtime_schedule": _serialize_runtime_schedule(runtime_schedule)},
                note=(
                    "Model-routed instruction validated and persisted for RL agent pickup."
                    if is_model_routed
                    else "Instruction validated and persisted for scheduled execution."
                ),
            )
            session.add(initial_event)
            session.flush()

            persisted_instructions.append(
                _instruction_from_record(
                    instruction_record,
                    runtime_schedule,
                    _event_from_record(initial_event),
                )
            )

    return SubmittedBatch(
        schema_version=batch.schema_version,
        batch_id=batch.source.batch_id,
        source_system=batch.source.system,
        runtime_timezone=runtime_timezone,
        instruction_count=len(persisted_instructions),
        instructions=tuple(persisted_instructions),
    )
