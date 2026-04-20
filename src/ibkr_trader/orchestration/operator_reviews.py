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
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import BrokerOrderEventRecord
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import OperatorReviewActionRecord
from ibkr_trader.db.models import ReconciliationIssueRecord

BROKER_ATTENTION_TARGET_KIND = "BROKER_ATTENTION"
RECONCILIATION_ISSUE_TARGET_KIND = "RECONCILIATION_ISSUE"

ACKNOWLEDGE_ACTION = "ACKNOWLEDGE"
RESOLVE_ACTION = "RESOLVE"
REOPEN_ACTION = "REOPEN"

OPEN_REVIEW_STATUS = "OPEN"
ACKNOWLEDGED_REVIEW_STATUS = "ACKNOWLEDGED"
RESOLVED_REVIEW_STATUS = "RESOLVED"


class OperatorReviewTargetNotFoundError(LookupError):
    """Raised when an operator review action targets a row that does not exist."""


@dataclass(slots=True)
class OperatorReviewStatus:
    status: str
    latest_action_type: str | None
    latest_action_at: datetime | None
    latest_action_by: str | None
    latest_action_note: str | None


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


def serialize_operator_review_status(payload: OperatorReviewStatus) -> dict[str, Any]:
    return _serialize_for_json(asdict(payload))


def normalize_operator_review_action_type(raw_value: str) -> str:
    normalized = raw_value.strip().upper()
    if normalized not in {ACKNOWLEDGE_ACTION, RESOLVE_ACTION, REOPEN_ACTION}:
        raise ValueError(
            "action must be one of ACKNOWLEDGE, RESOLVE, or REOPEN"
        )
    return normalized


def operator_review_status_from_action_type(action_type: str | None) -> str:
    if action_type == ACKNOWLEDGE_ACTION:
        return ACKNOWLEDGED_REVIEW_STATUS
    if action_type == RESOLVE_ACTION:
        return RESOLVED_REVIEW_STATUS
    return OPEN_REVIEW_STATUS


def build_operator_review_status(
    latest_action: OperatorReviewActionRecord | None,
) -> OperatorReviewStatus:
    if latest_action is None:
        return OperatorReviewStatus(
            status=OPEN_REVIEW_STATUS,
            latest_action_type=None,
            latest_action_at=None,
            latest_action_by=None,
            latest_action_note=None,
        )

    return OperatorReviewStatus(
        status=operator_review_status_from_action_type(latest_action.action_type),
        latest_action_type=latest_action.action_type,
        latest_action_at=latest_action.event_at,
        latest_action_by=latest_action.updated_by,
        latest_action_note=latest_action.note,
    )


def extract_broker_attention_message(
    broker_order_event: BrokerOrderEventRecord,
    broker_order: BrokerOrderRecord,
) -> str | None:
    """Return the operator-facing warning or reject text for a broker-order event."""

    payload = broker_order_event.payload or {}
    if not isinstance(payload, dict):
        payload = {}

    if broker_order_event.event_type == "order_error_callback":
        error_code = payload.get("errorCode")
        error_message = payload.get("errorMsg") or payload.get("message")
        if error_message in (None, ""):
            return broker_order_event.note
        if error_code in (None, ""):
            return str(error_message)
        return f"[{error_code}] {error_message}"

    for key in ("reject_reason", "warning_text"):
        raw_value = payload.get(key)
        if raw_value not in (None, ""):
            return str(raw_value)

    metadata_json = broker_order.metadata_json or {}
    for key in ("reject_reason", "warning_text"):
        raw_value = metadata_json.get(key)
        if raw_value not in (None, ""):
            return str(raw_value)

    if broker_order_event.note not in (None, ""):
        lowered_type = broker_order_event.event_type.lower()
        lowered_note = broker_order_event.note.lower()
        if "error" in lowered_type or "reject" in lowered_note or "warning" in lowered_note:
            return broker_order_event.note
    return None


def _normalize_note(note: str | None) -> str | None:
    if note is None:
        return None
    normalized = note.strip()
    return normalized or None


def _normalize_updated_by(updated_by: str) -> str:
    normalized = updated_by.strip()
    if not normalized:
        raise ValueError("updated_by must be a non-empty string")
    return normalized


def _record_review_action(
    session: Session,
    *,
    target_kind: str,
    target_id: int,
    action_type: str,
    updated_by: str,
    note: str | None,
    source: str,
    payload: dict[str, Any],
) -> OperatorReviewStatus:
    event = OperatorReviewActionRecord(
        target_kind=target_kind,
        target_id=target_id,
        action_type=action_type,
        source=source,
        event_at=utc_now(),
        updated_by=updated_by,
        note=note,
        payload=payload,
    )
    session.add(event)
    session.flush()
    return build_operator_review_status(event)


def record_broker_attention_review_action(
    session_factory: sessionmaker[Session],
    *,
    event_id: int,
    action_type: str,
    updated_by: str,
    note: str | None = None,
    source: str = "api",
) -> OperatorReviewStatus:
    normalized_action_type = normalize_operator_review_action_type(action_type)
    normalized_updated_by = _normalize_updated_by(updated_by)
    normalized_note = _normalize_note(note)

    with session_scope(session_factory) as session:
        row = session.execute(
            select(BrokerOrderEventRecord, BrokerOrderRecord)
            .join(
                BrokerOrderRecord,
                BrokerOrderRecord.id == BrokerOrderEventRecord.broker_order_id,
            )
            .where(BrokerOrderEventRecord.id == event_id)
        ).one_or_none()
        if row is None:
            raise OperatorReviewTargetNotFoundError(
                f"Broker attention item {event_id} was not found."
            )

        broker_order_event, broker_order = row
        message = extract_broker_attention_message(broker_order_event, broker_order)
        if message is None:
            raise OperatorReviewTargetNotFoundError(
                f"Broker order event {event_id} is not an operator attention item."
            )

        return _record_review_action(
            session,
            target_kind=BROKER_ATTENTION_TARGET_KIND,
            target_id=event_id,
            action_type=normalized_action_type,
            updated_by=normalized_updated_by,
            note=normalized_note,
            source=source,
            payload={
                "broker_order_id": broker_order.id,
                "account_key": broker_order.account_key,
                "symbol": broker_order.symbol,
                "event_type": broker_order_event.event_type,
                "message": message,
            },
        )


def record_reconciliation_issue_review_action(
    session_factory: sessionmaker[Session],
    *,
    issue_id: int,
    action_type: str,
    updated_by: str,
    note: str | None = None,
    source: str = "api",
) -> OperatorReviewStatus:
    normalized_action_type = normalize_operator_review_action_type(action_type)
    normalized_updated_by = _normalize_updated_by(updated_by)
    normalized_note = _normalize_note(note)

    with session_scope(session_factory) as session:
        issue = session.execute(
            select(ReconciliationIssueRecord).where(ReconciliationIssueRecord.id == issue_id)
        ).scalar_one_or_none()
        if issue is None:
            raise OperatorReviewTargetNotFoundError(
                f"Reconciliation issue {issue_id} was not found."
            )

        return _record_review_action(
            session,
            target_kind=RECONCILIATION_ISSUE_TARGET_KIND,
            target_id=issue_id,
            action_type=normalized_action_type,
            updated_by=normalized_updated_by,
            note=normalized_note,
            source=source,
            payload={
                "instruction_id": issue.instruction_id,
                "stage": issue.stage,
                "severity": issue.severity,
                "message": issue.message,
            },
        )
