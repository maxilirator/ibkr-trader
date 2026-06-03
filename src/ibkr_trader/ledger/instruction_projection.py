from __future__ import annotations

from datetime import datetime
from decimal import Decimal
import socket
from typing import Any

from sqlalchemy.orm import Session

from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.orchestration.operator_alerts import send_operator_alert
from ibkr_trader.orchestration.state_machine import ExecutionState


OPEN_ORDER_CLOSED_STATUSES = {
    "API_CANCELLED",
    "CANCELLED",
    "ERROR",
    "FILLED",
    "INACTIVE",
    "NOT_FOUND_AT_BROKER",
    "REJECTED",
}
NEEDS_REVIEW_ERROR_CODES = {202}
NEEDS_REVIEW_ERROR_TEXT_FRAGMENTS = (
    "risk mitigation",
    "trdv",
)


def sync_instruction_from_broker_order_terminal_status(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
    event_at: datetime,
    event_source: str,
    status_payload: dict[str, Any] | None = None,
    note: str | None = None,
) -> None:
    """Project terminal broker entry states back onto the instruction row."""

    if broker_order.instruction_id is None:
        return

    instruction_record = session.get(InstructionRecord, broker_order.instruction_id)
    if instruction_record is None:
        return

    order_role = _normalize_text(broker_order.order_role)
    if order_role == "ENTRY":
        instruction_record.broker_order_status = broker_order.status
    elif order_role == "EXIT":
        instruction_record.exit_order_status = broker_order.status
        return
    else:
        return

    normalized_status = _normalize_text(str(broker_order.status).upper())
    if normalized_status not in OPEN_ORDER_CLOSED_STATUSES:
        return
    if instruction_record.state != ExecutionState.ENTRY_SUBMITTED.value:
        return

    filled_quantity = _to_decimal(
        status_payload.get("filled") if isinstance(status_payload, dict) else None
    ) or Decimal("0")
    if filled_quantity > 0:
        average_price = None
        if isinstance(status_payload, dict):
            average_price = _to_decimal(status_payload.get("avgFillPrice"))
            if average_price is None or average_price <= 0:
                average_price = _to_decimal(status_payload.get("lastFillPrice"))

        previous_state = instruction_record.state
        instruction_record.entry_filled_quantity = str(filled_quantity)
        if average_price is not None and average_price > 0:
            instruction_record.entry_avg_fill_price = str(average_price)
        instruction_record.entry_filled_at = event_at
        instruction_record.state = ExecutionState.POSITION_OPEN.value
        session.add(
            InstructionEventRecord(
                instruction_id=instruction_record.id,
                event_type="entry_order_filled",
                source=event_source,
                event_at=event_at,
                state_before=previous_state,
                state_after=instruction_record.state,
                payload={
                    "broker_order_id": broker_order.external_order_id,
                    "broker_perm_id": broker_order.external_perm_id,
                    "broker_order_status": _serialize_for_json(status_payload)
                    if isinstance(status_payload, dict)
                    else broker_order.status,
                    "filled_quantity": str(filled_quantity),
                    "average_price": str(average_price)
                    if average_price is not None
                    else None,
                    "evidence_source": "broker_order_status",
                },
                note=note
                or (
                    "Broker order-status callback reported an entry fill before "
                    "executions were available."
                ),
            )
        )
        return

    previous_state = instruction_record.state
    instruction_record.state = ExecutionState.ENTRY_CANCELLED.value
    session.add(
        InstructionEventRecord(
            instruction_id=instruction_record.id,
            event_type="entry_order_cancelled",
            source=event_source,
            event_at=event_at,
            state_before=previous_state,
            state_after=instruction_record.state,
            payload={
                "broker_order_id": broker_order.external_order_id,
                "broker_perm_id": broker_order.external_perm_id,
                "broker_order_status": _serialize_for_json(status_payload)
                if isinstance(status_payload, dict)
                else broker_order.status,
            },
            note=note
            or "Broker reported the unfilled entry order as terminal before expiry.",
        )
    )


def mark_instruction_needs_review_from_order_error(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
    event_at: datetime,
    error_payload: dict[str, Any],
) -> None:
    """Flag broker-stopped entries that require operator review before resubmission."""

    if broker_order.instruction_id is None:
        return

    instruction_record = session.get(InstructionRecord, broker_order.instruction_id)
    if instruction_record is None:
        return
    if instruction_record.state == ExecutionState.NEEDS_REVIEW.value:
        return
    if not _should_mark_instruction_needs_review(
        broker_order=broker_order,
        instruction_record=instruction_record,
        error_payload=error_payload,
    ):
        return

    previous_state = instruction_record.state
    instruction_record.state = ExecutionState.NEEDS_REVIEW.value
    instruction_record.broker_order_status = broker_order.status
    session.add(
        InstructionEventRecord(
            instruction_id=instruction_record.id,
            event_type="entry_order_needs_review",
            source="broker_callback",
            event_at=event_at,
            state_before=previous_state,
            state_after=instruction_record.state,
            payload={
                "broker_order_id": broker_order.external_order_id,
                "broker_perm_id": broker_order.external_perm_id,
                "broker_order_status": broker_order.status,
                "order_error_callback": _serialize_for_json(error_payload),
            },
            note=(
                "Broker cancelled the entry order for risk mitigation. "
                "Operator review is required before any resubmission."
            ),
        )
    )
    send_operator_alert(
        _build_needs_review_alert_message(
            broker_order=broker_order,
            instruction_record=instruction_record,
            error_payload=error_payload,
        )
    )


def _should_mark_instruction_needs_review(
    *,
    broker_order: BrokerOrderRecord,
    instruction_record: InstructionRecord,
    error_payload: dict[str, Any],
) -> bool:
    if _normalize_text(broker_order.order_role) != "ENTRY":
        return False
    if instruction_record.state not in {
        ExecutionState.ENTRY_SUBMITTED.value,
        ExecutionState.ENTRY_CANCELLED.value,
    }:
        return False
    if (_to_decimal(instruction_record.entry_filled_quantity) or Decimal("0")) > 0:
        return False

    error_code = error_payload.get("errorCode")
    if error_code not in NEEDS_REVIEW_ERROR_CODES:
        return False

    message = (_normalize_broker_error_message(error_payload) or "").lower()
    return any(fragment in message for fragment in NEEDS_REVIEW_ERROR_TEXT_FRAGMENTS)


def _build_needs_review_alert_message(
    *,
    broker_order: BrokerOrderRecord,
    instruction_record: InstructionRecord,
    error_payload: dict[str, Any],
) -> str:
    host = socket.gethostname()
    error_code = error_payload.get("errorCode")
    error_message = (
        _normalize_broker_error_message(error_payload)
        or "Broker requested operator review."
    )
    symbol = instruction_record.symbol or broker_order.symbol or "UNKNOWN"
    account_key = instruction_record.account_key or broker_order.account_key or "UNKNOWN"
    order_id = broker_order.external_order_id or "UNKNOWN"
    error_prefix = f"[{error_code}] " if error_code not in (None, "") else ""
    return (
        f"Instruction needs review on {host}: {instruction_record.instruction_id} "
        f"({symbol}, account {account_key}) had entry order {order_id} stopped by IBKR. "
        f"{error_prefix}{error_message}. Review before any resubmission."
    )


def _normalize_broker_error_message(error_payload: dict[str, Any]) -> str | None:
    for key in ("errorString", "errorMsg", "message"):
        raw_value = error_payload.get(key)
        if raw_value in (None, ""):
            continue
        normalized = _normalize_text(str(raw_value))
        if normalized is not None:
            return " ".join(normalized.split())
    return None


def _serialize_for_json(payload: Any) -> Any:
    if isinstance(payload, Decimal):
        return str(payload)
    if isinstance(payload, datetime):
        return payload.isoformat()
    if isinstance(payload, dict):
        return {key: _serialize_for_json(value) for key, value in payload.items()}
    if isinstance(payload, list):
        return [_serialize_for_json(value) for value in payload]
    if isinstance(payload, tuple):
        return [_serialize_for_json(value) for value in payload]
    return payload


def _normalize_text(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))
