from __future__ import annotations

import json
from dataclasses import asdict
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timezone
from decimal import Decimal
from decimal import InvalidOperation
from enum import Enum
from typing import Any


@dataclass(slots=True)
class RuntimeCycleIssue:
    """A single operator-visible runtime problem captured during one cycle."""

    instruction_id: str | None
    stage: str
    message: str


@dataclass(slots=True)
class RuntimeCycleAction:
    """A durable action the runtime took for one instruction."""

    instruction_id: str
    action: str
    state: str
    detail: dict[str, Any]


@dataclass(slots=True)
class EntryBrokerOrderSnapshot:
    """Small immutable view of the latest entry broker-order lineage."""

    broker_order_id: int | None
    external_order_id: str | None
    external_perm_id: str | None
    status: str | None
    order_ref: str | None
    raw_payload: dict[str, Any]
    metadata_json: dict[str, Any]


@dataclass(slots=True)
class RuntimeCycleResult:
    """Full structured result emitted by runtime and startup reconciliation cycles."""

    cycle_started_at: datetime
    cycle_completed_at: datetime
    runtime_timezone: str
    submitted_entries: tuple[RuntimeCycleAction, ...]
    cancelled_entries: tuple[RuntimeCycleAction, ...]
    filled_entries: tuple[RuntimeCycleAction, ...]
    submitted_exits: tuple[RuntimeCycleAction, ...]
    completed_instructions: tuple[RuntimeCycleAction, ...]
    issues: tuple[RuntimeCycleIssue, ...]


@dataclass(slots=True)
class ExecutionAggregate:
    """Normalized fill evidence from executions, fill rows, or order-status callbacks."""

    quantity: Decimal = Decimal("0")
    average_price: Decimal | None = None
    executed_at: datetime | None = None
    execution_count: int = 0

    @property
    def has_fill(self) -> bool:
        return self.quantity > 0


def serialize_for_json(payload: Any) -> Any:
    """Recursively serialize runtime dataclasses and scalar types for JSON columns."""

    if isinstance(payload, Enum):
        return payload.value
    if isinstance(payload, Decimal):
        return str(payload)
    if isinstance(payload, datetime):
        return payload.isoformat()
    if isinstance(payload, date):
        return payload.isoformat()
    if isinstance(payload, list):
        return [serialize_for_json(item) for item in payload]
    if isinstance(payload, tuple):
        return [serialize_for_json(item) for item in payload]
    if isinstance(payload, dict):
        return {key: serialize_for_json(value) for key, value in payload.items()}
    return payload


def serialize_runtime_cycle_result(result: RuntimeCycleResult) -> dict[str, Any]:
    return serialize_for_json(asdict(result))


def emit_runtime_cycle_result(result: RuntimeCycleResult) -> None:
    print(json.dumps(serialize_runtime_cycle_result(result), indent=2))


def parse_decimal(value: str | None) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid decimal payload value: {value}") from exc


def ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
