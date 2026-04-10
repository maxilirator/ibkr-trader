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
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord


class InstructionStatusNotFoundError(LookupError):
    """Raised when a persisted instruction record cannot be found."""


@dataclass(slots=True)
class InstructionEventStatus:
    event_id: int
    event_type: str
    source: str
    event_at: datetime
    state_before: str | None
    state_after: str | None
    payload: dict[str, Any]
    note: str | None


@dataclass(slots=True)
class InstructionStatus:
    record_id: int
    instruction_id: str
    schema_version: str
    source_system: str
    batch_id: str
    account_key: str
    book_key: str
    symbol: str
    exchange: str
    currency: str
    state: str
    submit_at: datetime
    expire_at: datetime
    order_type: str
    side: str
    created_at: datetime
    updated_at: datetime
    broker_order_id: int | None
    broker_perm_id: int | None
    broker_client_id: int | None
    broker_order_status: str | None
    entry_submitted_quantity: str | None
    entry_filled_quantity: str | None
    entry_avg_fill_price: str | None
    entry_filled_at: datetime | None
    exit_order_id: int | None
    exit_perm_id: int | None
    exit_client_id: int | None
    exit_order_status: str | None
    exit_submitted_quantity: str | None
    exit_filled_quantity: str | None
    exit_avg_fill_price: str | None
    exit_filled_at: datetime | None
    events: tuple[InstructionEventStatus, ...]


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


def serialize_instruction_status(payload: InstructionStatus) -> dict[str, Any]:
    return _serialize_for_json(asdict(payload))


def read_instruction_status(
    session_factory: sessionmaker[Session],
    instruction_id: str,
    *,
    include_events: bool = True,
) -> InstructionStatus:
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord).where(
                InstructionRecord.instruction_id == instruction_id
            )
        ).scalar_one_or_none()
        if record is None:
            raise InstructionStatusNotFoundError(
                f"Persisted instruction '{instruction_id}' was not found."
            )

        events: tuple[InstructionEventStatus, ...] = ()
        if include_events:
            raw_events = session.execute(
                select(InstructionEventRecord)
                .where(InstructionEventRecord.instruction_id == record.id)
                .order_by(InstructionEventRecord.id)
            ).scalars()
            events = tuple(
                InstructionEventStatus(
                    event_id=event.id,
                    event_type=event.event_type,
                    source=event.source,
                    event_at=event.event_at,
                    state_before=event.state_before,
                    state_after=event.state_after,
                    payload=event.payload,
                    note=event.note,
                )
                for event in raw_events
            )

        return InstructionStatus(
            record_id=record.id,
            instruction_id=record.instruction_id,
            schema_version=record.schema_version,
            source_system=record.source_system,
            batch_id=record.batch_id,
            account_key=record.account_key,
            book_key=record.book_key,
            symbol=record.symbol,
            exchange=record.exchange,
            currency=record.currency,
            state=record.state,
            submit_at=record.submit_at,
            expire_at=record.expire_at,
            order_type=record.order_type,
            side=record.side,
            created_at=record.created_at,
            updated_at=record.updated_at,
            broker_order_id=record.broker_order_id,
            broker_perm_id=record.broker_perm_id,
            broker_client_id=record.broker_client_id,
            broker_order_status=record.broker_order_status,
            entry_submitted_quantity=record.entry_submitted_quantity,
            entry_filled_quantity=record.entry_filled_quantity,
            entry_avg_fill_price=record.entry_avg_fill_price,
            entry_filled_at=record.entry_filled_at,
            exit_order_id=record.exit_order_id,
            exit_perm_id=record.exit_perm_id,
            exit_client_id=record.exit_client_id,
            exit_order_status=record.exit_order_status,
            exit_submitted_quantity=record.exit_submitted_quantity,
            exit_filled_quantity=record.exit_filled_quantity,
            exit_avg_fill_price=record.exit_avg_fill_price,
            exit_filled_at=record.exit_filled_at,
            events=events,
        )
