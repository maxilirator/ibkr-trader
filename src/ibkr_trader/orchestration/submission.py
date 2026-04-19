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
from ibkr_trader.orchestration.scheduling import InstructionRuntimeSchedule
from ibkr_trader.orchestration.scheduling import build_batch_runtime_schedule
from ibkr_trader.orchestration.state_machine import ExecutionState


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


def _ensure_no_existing_instruction_ids(
    session: Session,
    batch: ExecutionInstructionBatch,
) -> None:
    instruction_ids = [instruction.instruction_id for instruction in batch.instructions]
    existing = session.execute(
        select(InstructionRecord.instruction_id).where(
            InstructionRecord.instruction_id.in_(instruction_ids)
        )
    ).scalars().all()
    if existing:
        duplicate_list = ", ".join(sorted(existing))
        raise SubmissionConflictError(
            f"instruction_id already exists: {duplicate_list}"
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
    assert_kill_switch_inactive(session_factory)
    batch.validate()
    _ensure_unique_instruction_ids(batch)

    schedule = build_batch_runtime_schedule(
        batch,
        runtime_timezone=runtime_timezone,
        session_calendar_path=session_calendar_path,
    )

    persisted_instructions: list[SubmittedInstruction] = []
    initial_state = ExecutionState.ENTRY_PENDING.value

    with session_scope(session_factory) as session:
        _ensure_no_existing_instruction_ids(session, batch)

        for instruction, runtime_schedule in zip(
            batch.instructions,
            schedule.instructions,
            strict=True,
        ):
            _upsert_instrument(session, instruction)

            instruction_record = InstructionRecord(
                instruction_id=instruction.instruction_id,
                schema_version=batch.schema_version,
                source_system=batch.source.system,
                batch_id=batch.source.batch_id,
                account_key=instruction.account.account_key,
                book_key=instruction.account.book_key,
                symbol=instruction.instrument.symbol,
                exchange=instruction.instrument.exchange,
                currency=instruction.instrument.currency,
                state=initial_state,
                submit_at=instruction.entry.submit_at,
                expire_at=instruction.entry.expire_at,
                order_type=instruction.entry.order_type.value,
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
                note="Instruction validated and persisted for scheduled execution.",
            )
            session.add(initial_event)
            session.flush()

            persisted_instructions.append(
                SubmittedInstruction(
                    record_id=instruction_record.id,
                    instruction_id=instruction_record.instruction_id,
                    state=instruction_record.state,
                    created_at=instruction_record.created_at,
                    updated_at=instruction_record.updated_at,
                    submit_at=instruction_record.submit_at,
                    expire_at=instruction_record.expire_at,
                    account_key=instruction_record.account_key,
                    book_key=instruction_record.book_key,
                    symbol=instruction_record.symbol,
                    exchange=instruction_record.exchange,
                    currency=instruction_record.currency,
                    order_type=instruction_record.order_type,
                    side=instruction_record.side,
                    runtime_schedule=_serialize_runtime_schedule(runtime_schedule),
                    initial_event=SubmittedInstructionEvent(
                        event_id=initial_event.id,
                        event_type=initial_event.event_type,
                        source=initial_event.source,
                        event_at=initial_event.event_at,
                        state_before=initial_event.state_before,
                        state_after=initial_event.state_after,
                        payload=initial_event.payload,
                        note=initial_event.note,
                    ),
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
