from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import ExecutionFillRecord
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
    is_virtual: bool
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
    archived_at: datetime | None
    archived_by: str | None
    archive_reason: str | None
    activity_at: datetime
    entry_order_display: str | None
    exit_order_display: str | None
    payload: dict[str, Any]
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


_TERMINAL_BROKER_ORDER_STATUSES = {
    "API_CANCELLED",
    "CANCELLED",
    "ERROR",
    "FILLED",
    "INACTIVE",
    "NOT_FOUND_AT_BROKER",
    "REJECTED",
}


def _normalize_status(status: str | None) -> str | None:
    if status is None:
        return None
    normalized = status.strip()
    if not normalized:
        return None
    return normalized.upper()


def _broker_order_activity_at(order: BrokerOrderRecord) -> datetime:
    return (
        order.last_status_at
        or order.submitted_at
        or order.updated_at
        or order.created_at
    )


def _broker_order_sort_key(order: BrokerOrderRecord) -> tuple[datetime, int]:
    return (_broker_order_activity_at(order), order.id)


def _execution_fill_sort_key(fill: ExecutionFillRecord) -> tuple[datetime, int]:
    return (fill.executed_at, fill.id)


def _int_or_none(value: str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    normalized = value.strip()
    if not normalized or not normalized.isdigit():
        return None
    return int(normalized)


def _latest_matching_order(
    broker_orders: tuple[BrokerOrderRecord, ...],
    *,
    order_role: str,
) -> BrokerOrderRecord | None:
    matching_orders = tuple(
        order
        for order in broker_orders
        if (order.order_role or "").strip().upper() == order_role
    )
    if not matching_orders:
        return None
    return max(matching_orders, key=_broker_order_sort_key)


def _dedupe_order_lineages(
    broker_orders: tuple[BrokerOrderRecord, ...],
) -> tuple[BrokerOrderRecord, ...]:
    if not broker_orders:
        return ()

    ordered = tuple(
        sorted(
            broker_orders,
            key=_broker_order_sort_key,
            reverse=True,
        )
    )
    deduped: list[BrokerOrderRecord] = []
    seen_lineages: set[tuple[str, str]] = set()
    for order in ordered:
        lineage_key = (
            str(order.external_perm_id or "").strip(),
            str(order.order_ref or order.external_order_id or order.id).strip(),
        )
        if lineage_key in seen_lineages:
            continue
        deduped.append(order)
        seen_lineages.add(lineage_key)
    return tuple(deduped)


def _matching_fills_for_role(
    record: InstructionRecord,
    *,
    execution_fills: tuple[ExecutionFillRecord, ...],
    broker_orders: tuple[BrokerOrderRecord, ...],
    order_role: str,
) -> tuple[ExecutionFillRecord, ...]:
    if not execution_fills:
        return ()

    order_role = order_role.strip().upper()
    matching_order_ids = {
        order.id
        for order in broker_orders
        if (order.order_role or "").strip().upper() == order_role
    }
    matching_order_refs = {
        str(order.order_ref).strip()
        for order in broker_orders
        if (order.order_role or "").strip().upper() == order_role
        and order.order_ref is not None
        and str(order.order_ref).strip()
    }
    fill_order_ref_prefix = f"{record.instruction_id}:{order_role.lower()}:"

    matching_fills: list[ExecutionFillRecord] = []
    for fill in execution_fills:
        fill_order_ref = str(fill.order_ref or "").strip()
        if fill.broker_order_id is not None and fill.broker_order_id in matching_order_ids:
            matching_fills.append(fill)
            continue
        if fill_order_ref and fill_order_ref in matching_order_refs:
            matching_fills.append(fill)
            continue
        if fill_order_ref.startswith(fill_order_ref_prefix):
            matching_fills.append(fill)
    return tuple(matching_fills)


def _latest_matching_fill(
    record: InstructionRecord,
    *,
    execution_fills: tuple[ExecutionFillRecord, ...],
    broker_orders: tuple[BrokerOrderRecord, ...],
    order_role: str,
) -> ExecutionFillRecord | None:
    matching_fills = _matching_fills_for_role(
        record,
        execution_fills=execution_fills,
        broker_orders=broker_orders,
        order_role=order_role,
    )
    if not matching_fills:
        return None
    return max(matching_fills, key=_execution_fill_sort_key)


def _display_for_orders(
    broker_orders: tuple[BrokerOrderRecord, ...],
    *,
    order_role: str,
) -> str | None:
    matching_orders = tuple(
        order
        for order in broker_orders
        if (order.order_role or "").strip().upper() == order_role
    )
    matching_orders = _dedupe_order_lineages(matching_orders)
    if not matching_orders:
        return None

    if order_role == "EXIT":
        active_orders = tuple(
            order
            for order in matching_orders
            if _normalize_status(order.status) not in _TERMINAL_BROKER_ORDER_STATUSES
        )
        if active_orders:
            matching_orders = tuple(sorted(active_orders, key=_broker_order_sort_key))
        else:
            matching_orders = (
                max(matching_orders, key=_broker_order_sort_key),
            )
    else:
        matching_orders = (
            max(matching_orders, key=_broker_order_sort_key),
        )

    if len(matching_orders) == 1:
        order = matching_orders[0]
        order_id = order.external_order_id or str(order.id)
        return f"{order_id} / {order.status}"

    order_ids = ", ".join(order.external_order_id or str(order.id) for order in matching_orders)
    statuses = ", ".join(order.status for order in matching_orders)
    return f"{order_ids} / {statuses}"


def _activity_at(
    record: InstructionRecord,
    *,
    events: tuple[InstructionEventStatus, ...],
    broker_orders: tuple[BrokerOrderRecord, ...],
    execution_fills: tuple[ExecutionFillRecord, ...],
) -> datetime:
    candidates = [record.updated_at]
    candidates.extend(event.event_at for event in events)
    candidates.extend(_broker_order_activity_at(order) for order in broker_orders)
    candidates.extend(fill.executed_at for fill in execution_fills)
    return max(candidates)


def _build_instruction_status(
    record: InstructionRecord,
    *,
    broker_orders: tuple[BrokerOrderRecord, ...] = (),
    execution_fills: tuple[ExecutionFillRecord, ...] = (),
    events: tuple[InstructionEventStatus, ...] = (),
) -> InstructionStatus:
    latest_entry_order = _latest_matching_order(broker_orders, order_role="ENTRY")
    latest_exit_order = _latest_matching_order(broker_orders, order_role="EXIT")
    latest_entry_fill = _latest_matching_fill(
        record,
        execution_fills=execution_fills,
        broker_orders=broker_orders,
        order_role="ENTRY",
    )
    latest_exit_fill = _latest_matching_fill(
        record,
        execution_fills=execution_fills,
        broker_orders=broker_orders,
        order_role="EXIT",
    )
    activity_at = _activity_at(
        record,
        events=events,
        broker_orders=broker_orders,
        execution_fills=execution_fills,
    )
    entry_fill_completed = record.entry_filled_quantity not in (None, "", "0")
    exit_fill_completed = record.exit_filled_quantity not in (None, "", "0")
    derived_entry_order_status = (
        "Filled"
        if entry_fill_completed and latest_entry_fill is not None
        else (
            latest_entry_order.status
            if latest_entry_order is not None
            else record.broker_order_status
        )
    )
    derived_exit_order_status = (
        "Filled"
        if exit_fill_completed and latest_exit_fill is not None
        else (
            latest_exit_order.status
            if latest_exit_order is not None
            else record.exit_order_status
        )
    )
    derived_entry_order_display = (
        f"{latest_entry_fill.external_order_id} / Filled"
        if latest_entry_fill is not None and latest_entry_fill.external_order_id is not None
        else None
    )
    derived_exit_order_display = (
        f"{latest_exit_fill.external_order_id} / Filled"
        if latest_exit_fill is not None and latest_exit_fill.external_order_id is not None
        else None
    )
    resolved_exit_order_display = (
        derived_exit_order_display
        or _display_for_orders(
            broker_orders,
            order_role="EXIT",
        )
        or (
            f"{record.exit_order_id} / {derived_exit_order_status}"
            if record.exit_order_id is not None or derived_exit_order_status is not None
            else None
        )
    )
    return InstructionStatus(
        record_id=record.id,
        instruction_id=record.instruction_id,
        schema_version=record.schema_version,
        source_system=record.source_system,
        batch_id=record.batch_id,
        account_key=record.account_key,
        book_key=record.book_key,
        is_virtual=record.is_virtual,
        symbol=record.symbol,
        exchange=record.exchange,
        currency=record.currency,
        state=record.state,
        submit_at=record.submit_at,
        expire_at=record.expire_at,
        order_type=record.order_type,
        side=record.side,
        created_at=record.created_at,
        updated_at=activity_at,
        broker_order_id=(
            _int_or_none(latest_entry_fill.external_order_id)
            if latest_entry_fill is not None
            and _int_or_none(latest_entry_fill.external_order_id) is not None
            else _int_or_none(latest_entry_order.external_order_id)
            if latest_entry_order is not None
            and _int_or_none(latest_entry_order.external_order_id) is not None
            else record.broker_order_id
        ),
        broker_perm_id=(
            _int_or_none(latest_entry_fill.external_perm_id)
            if latest_entry_fill is not None
            and _int_or_none(latest_entry_fill.external_perm_id) is not None
            else _int_or_none(latest_entry_order.external_perm_id)
            if latest_entry_order is not None
            and _int_or_none(latest_entry_order.external_perm_id) is not None
            else record.broker_perm_id
        ),
        broker_client_id=(
            int(latest_entry_order.external_client_id)
            if latest_entry_order is not None
            and latest_entry_order.external_client_id is not None
            and latest_entry_order.external_client_id.isdigit()
            else record.broker_client_id
        ),
        broker_order_status=derived_entry_order_status,
        entry_submitted_quantity=record.entry_submitted_quantity,
        entry_filled_quantity=record.entry_filled_quantity,
        entry_avg_fill_price=record.entry_avg_fill_price,
        entry_filled_at=record.entry_filled_at,
        exit_order_id=(
            _int_or_none(latest_exit_fill.external_order_id)
            if latest_exit_fill is not None
            and _int_or_none(latest_exit_fill.external_order_id) is not None
            else _int_or_none(latest_exit_order.external_order_id)
            if latest_exit_order is not None
            and _int_or_none(latest_exit_order.external_order_id) is not None
            else record.exit_order_id
        ),
        exit_perm_id=(
            _int_or_none(latest_exit_fill.external_perm_id)
            if latest_exit_fill is not None
            and _int_or_none(latest_exit_fill.external_perm_id) is not None
            else _int_or_none(latest_exit_order.external_perm_id)
            if latest_exit_order is not None
            and _int_or_none(latest_exit_order.external_perm_id) is not None
            else record.exit_perm_id
        ),
        exit_client_id=(
            int(latest_exit_order.external_client_id)
            if latest_exit_order is not None
            and latest_exit_order.external_client_id is not None
            and latest_exit_order.external_client_id.isdigit()
            else record.exit_client_id
        ),
        exit_order_status=(
            derived_exit_order_status
        ),
        exit_submitted_quantity=record.exit_submitted_quantity,
        exit_filled_quantity=record.exit_filled_quantity,
        exit_avg_fill_price=record.exit_avg_fill_price,
        exit_filled_at=record.exit_filled_at,
        archived_at=record.archived_at,
        archived_by=record.archived_by,
        archive_reason=record.archive_reason,
        activity_at=activity_at,
        entry_order_display=derived_entry_order_display
        or _display_for_orders(
            broker_orders,
            order_role="ENTRY",
        )
        or (
            f"{record.broker_order_id} / {derived_entry_order_status}"
            if record.broker_order_id is not None or derived_entry_order_status is not None
            else None
        ),
        exit_order_display=resolved_exit_order_display,
        payload=record.payload,
        events=events,
    )


def list_instruction_statuses(
    session_factory: sessionmaker[Session],
    *,
    limit: int = 100,
    state: str | None = None,
    include_archived: bool = False,
    model_routed: bool | None = None,
) -> tuple[InstructionStatus, ...]:
    statement = select(InstructionRecord)
    if state is not None:
        statement = statement.where(InstructionRecord.state == state)
    if not include_archived:
        statement = statement.where(InstructionRecord.archived_at.is_(None))
    if model_routed is True:
        statement = statement.where(
            or_(
                InstructionRecord.state == "MODEL_ROUTED_PENDING",
                InstructionRecord.order_type == "MODEL_ROUTED",
            )
        )
    elif model_routed is False:
        statement = statement.where(
            InstructionRecord.state != "MODEL_ROUTED_PENDING",
            InstructionRecord.order_type != "MODEL_ROUTED",
        )
    statement = statement.order_by(
        InstructionRecord.id.desc(),
    )

    with session_scope(session_factory) as session:
        records = tuple(session.execute(statement).scalars())
        if not records:
            return ()

        broker_orders = tuple(
            session.execute(
                select(BrokerOrderRecord).where(
                    BrokerOrderRecord.instruction_id.in_([record.id for record in records])
                )
            ).scalars()
        )
        execution_fills = tuple(
            session.execute(
                select(ExecutionFillRecord).where(
                    ExecutionFillRecord.instruction_id.in_([record.id for record in records])
                )
            ).scalars()
        )
        broker_orders_by_instruction_id: dict[int, list[BrokerOrderRecord]] = {}
        for broker_order in broker_orders:
            if broker_order.instruction_id is None:
                continue
            broker_orders_by_instruction_id.setdefault(
                broker_order.instruction_id,
                [],
            ).append(broker_order)
        execution_fills_by_instruction_id: dict[int, list[ExecutionFillRecord]] = {}
        for execution_fill in execution_fills:
            if execution_fill.instruction_id is None:
                continue
            execution_fills_by_instruction_id.setdefault(
                execution_fill.instruction_id,
                [],
            ).append(execution_fill)

        statuses = tuple(
            _build_instruction_status(
                record,
                broker_orders=tuple(broker_orders_by_instruction_id.get(record.id, ())),
                execution_fills=tuple(execution_fills_by_instruction_id.get(record.id, ())),
            )
            for record in records
        )
        return tuple(
            sorted(
                statuses,
                key=lambda instruction: (
                    instruction.activity_at,
                    instruction.record_id,
                ),
                reverse=True,
            )[:limit]
        )


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

        broker_orders = tuple(
            session.execute(
                select(BrokerOrderRecord).where(BrokerOrderRecord.instruction_id == record.id)
            ).scalars()
        )
        execution_fills = tuple(
            session.execute(
                select(ExecutionFillRecord).where(ExecutionFillRecord.instruction_id == record.id)
            ).scalars()
        )

        return _build_instruction_status(
            record,
            broker_orders=broker_orders,
            execution_fills=execution_fills,
            events=events,
        )
