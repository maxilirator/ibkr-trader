from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from typing import Callable

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.domain.execution_payloads import parse_execution_instruction_payload
from ibkr_trader.ibkr.order_execution import cancel_broker_order
from ibkr_trader.ibkr.order_execution import submit_order_from_instruction
from ibkr_trader.orchestration.state_machine import ExecutionState


class PersistedInstructionNotFoundError(LookupError):
    """Raised when a persisted instruction record cannot be found."""


class PersistedInstructionStateError(ValueError):
    """Raised when a persisted instruction is not in a submit-ready state."""


@dataclass(slots=True)
class BrokerSubmissionEvent:
    event_id: int
    event_type: str
    source: str
    event_at: datetime
    state_before: str | None
    state_after: str | None
    payload: dict[str, Any]
    note: str | None


@dataclass(slots=True)
class PersistedBrokerSubmission:
    record_id: int
    instruction_id: str
    state: str
    broker_order_id: int | None
    broker_perm_id: int | None
    broker_client_id: int | None
    broker_order_status: str | None
    broker_submission: dict[str, Any]
    submission_event: BrokerSubmissionEvent


@dataclass(slots=True)
class PersistedBrokerCancellation:
    record_id: int
    instruction_id: str
    state: str
    broker_order_id: int | None
    broker_perm_id: int | None
    broker_client_id: int | None
    broker_order_status: str | None
    broker_cancellation: dict[str, Any]
    cancellation_event: BrokerSubmissionEvent


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


def serialize_persisted_broker_submission(payload: PersistedBrokerSubmission) -> dict[str, Any]:
    return _serialize_for_json(asdict(payload))


def serialize_persisted_broker_cancellation(
    payload: PersistedBrokerCancellation,
) -> dict[str, Any]:
    return _serialize_for_json(asdict(payload))


def _coerce_optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def submit_persisted_instruction_entry(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    instruction_id: str,
    *,
    timeout: int = 10,
    submitter: Callable[..., dict[str, Any]] | None = None,
) -> PersistedBrokerSubmission:
    with session_scope(session_factory) as session:
        instruction_record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == instruction_id)
            .with_for_update()
        ).scalar_one_or_none()
        if instruction_record is None:
            raise PersistedInstructionNotFoundError(
                f"Persisted instruction '{instruction_id}' was not found."
            )
        if instruction_record.state != ExecutionState.ENTRY_PENDING.value:
            raise PersistedInstructionStateError(
                f"Instruction '{instruction_id}' is in state '{instruction_record.state}', "
                "expected ENTRY_PENDING."
            )

        raw_instruction_payload = instruction_record.payload.get("instruction")
        if not isinstance(raw_instruction_payload, dict):
            raise ValueError(
                f"Instruction '{instruction_id}' does not contain a valid persisted payload."
            )

        instruction = parse_execution_instruction_payload(raw_instruction_payload)
        runtime_submitter = submitter or submit_order_from_instruction
        broker_submission = runtime_submitter(
            broker_config,
            instruction,
            timeout=timeout,
        )
        broker_status = broker_submission["broker_order_status"]

        previous_state = instruction_record.state
        instruction_record.state = ExecutionState.ENTRY_SUBMITTED.value
        instruction_record.broker_order_id = _coerce_optional_int(
            broker_status.get("orderId")
        )
        instruction_record.broker_perm_id = _coerce_optional_int(
            broker_status.get("permId")
        )
        instruction_record.broker_client_id = _coerce_optional_int(
            broker_status.get("clientId")
        )
        instruction_record.broker_order_status = (
            str(broker_status["status"])
            if broker_status.get("status") is not None
            else None
        )
        submitted_order = broker_submission.get("order", {})
        if isinstance(submitted_order, dict):
            total_quantity = submitted_order.get("total_quantity")
            instruction_record.entry_submitted_quantity = (
                str(total_quantity) if total_quantity not in (None, "") else None
            )

        event = InstructionEventRecord(
            instruction_id=instruction_record.id,
            event_type="entry_order_submitted",
            source="broker_submit",
            state_before=previous_state,
            state_after=instruction_record.state,
            payload={"broker_submission": broker_submission},
            note="Persisted instruction entry order submitted to IBKR.",
        )
        session.add(event)
        session.flush()

        return PersistedBrokerSubmission(
            record_id=instruction_record.id,
            instruction_id=instruction_record.instruction_id,
            state=instruction_record.state,
            broker_order_id=instruction_record.broker_order_id,
            broker_perm_id=instruction_record.broker_perm_id,
            broker_client_id=instruction_record.broker_client_id,
            broker_order_status=instruction_record.broker_order_status,
            broker_submission=broker_submission,
            submission_event=BrokerSubmissionEvent(
                event_id=event.id,
                event_type=event.event_type,
                source=event.source,
                event_at=event.event_at,
                state_before=event.state_before,
                state_after=event.state_after,
                payload=event.payload,
                note=event.note,
            ),
        )


def cancel_persisted_instruction_entry(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    instruction_id: str,
    *,
    timeout: int = 10,
    canceler: Callable[..., dict[str, Any]] | None = None,
) -> PersistedBrokerCancellation:
    with session_scope(session_factory) as session:
        instruction_record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == instruction_id)
            .with_for_update()
        ).scalar_one_or_none()
        if instruction_record is None:
            raise PersistedInstructionNotFoundError(
                f"Persisted instruction '{instruction_id}' was not found."
            )
        if instruction_record.state != ExecutionState.ENTRY_SUBMITTED.value:
            raise PersistedInstructionStateError(
                f"Instruction '{instruction_id}' is in state '{instruction_record.state}', "
                "expected ENTRY_SUBMITTED."
            )
        if instruction_record.broker_order_id is None:
            raise ValueError(
                f"Instruction '{instruction_id}' does not have a broker_order_id to cancel."
            )

        runtime_canceler = canceler or cancel_broker_order
        broker_cancellation = runtime_canceler(
            broker_config,
            instruction_record.broker_order_id,
            timeout=timeout,
        )
        broker_status = broker_cancellation["broker_order_status"]

        previous_state = instruction_record.state
        instruction_record.state = ExecutionState.ENTRY_CANCELLED.value
        instruction_record.broker_order_status = (
            str(broker_status["status"])
            if broker_status.get("status") is not None
            else instruction_record.broker_order_status
        )

        event = InstructionEventRecord(
            instruction_id=instruction_record.id,
            event_type="entry_order_cancelled",
            source="broker_cancel",
            state_before=previous_state,
            state_after=instruction_record.state,
            payload={"broker_cancellation": broker_cancellation},
            note="Persisted instruction entry order cancelled at IBKR.",
        )
        session.add(event)
        session.flush()

        return PersistedBrokerCancellation(
            record_id=instruction_record.id,
            instruction_id=instruction_record.instruction_id,
            state=instruction_record.state,
            broker_order_id=instruction_record.broker_order_id,
            broker_perm_id=instruction_record.broker_perm_id,
            broker_client_id=instruction_record.broker_client_id,
            broker_order_status=instruction_record.broker_order_status,
            broker_cancellation=broker_cancellation,
            cancellation_event=BrokerSubmissionEvent(
                event_id=event.id,
                event_type=event.event_type,
                source=event.source,
                event_at=event.event_at,
                state_before=event.state_before,
                state_after=event.state_after,
                payload=event.payload,
                note=event.note,
            ),
        )
