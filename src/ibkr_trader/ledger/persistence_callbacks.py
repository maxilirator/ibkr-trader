from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import ExecutionFillRecord
from ibkr_trader.ibkr.runtime_snapshot import BrokerOpenOrder
from ibkr_trader.ledger.persistence_order_records import _upsert_open_order
from ibkr_trader.ledger.persistence_order_reconstruction import _matches_instruction_exit_identity
from ibkr_trader.ledger.persistence_order_reconstruction import _reconstruct_entry_broker_order_from_instruction
from ibkr_trader.ledger.persistence_order_reconstruction import _reconstruct_exit_broker_order_from_instruction
from ibkr_trader.ledger.persistence_shared import _decimal_to_string
from ibkr_trader.ledger.persistence_shared import _find_broker_order
from ibkr_trader.ledger.persistence_shared import _find_broker_order_any_account
from ibkr_trader.ledger.persistence_shared import _find_instruction_record_for_order
from ibkr_trader.ledger.persistence_shared import _get_or_create_broker_account
from ibkr_trader.ledger.persistence_shared import _is_order_status_synthetic_fill
from ibkr_trader.ledger.persistence_shared import _is_virtual_ledger_identity
from ibkr_trader.ledger.persistence_shared import _mark_instruction_needs_review_from_order_error
from ibkr_trader.ledger.persistence_shared import _normalize_text
from ibkr_trader.ledger.persistence_shared import _order_status_fill_execution_id
from ibkr_trader.ledger.persistence_shared import _record_broker_order_event
from ibkr_trader.ledger.persistence_shared import _require_text
from ibkr_trader.ledger.persistence_shared import _resolve_account_key
from ibkr_trader.ledger.persistence_shared import _serialize_for_json
from ibkr_trader.ledger.persistence_shared import _sync_instruction_from_broker_order_terminal_status
from ibkr_trader.ledger.persistence_shared import _to_decimal

def _persist_open_order_callback_event(
    session: Session,
    *,
    broker_kind: str,
    event_payload: dict[str, Any],
    default_account_key: str | None,
) -> None:
    order_payload = event_payload.get("order")
    if not isinstance(order_payload, dict):
        raise ValueError("Open-order callback payload was missing the serialized order body.")
    event_at = event_payload.get("event_at")
    if not isinstance(event_at, datetime):
        raise ValueError("Open-order callback payload was missing a valid event_at timestamp.")
    raw_order_id = order_payload.get("order_id")
    if raw_order_id in (None, ""):
        raise ValueError("Open-order callback payload was missing order.order_id.")

    open_order = BrokerOpenOrder(
        order_id=int(raw_order_id),
        perm_id=(
            int(order_payload["perm_id"])
            if order_payload.get("perm_id") not in (None, "")
            else None
        ),
        client_id=(
            int(order_payload["client_id"])
            if order_payload.get("client_id") not in (None, "")
            else None
        ),
        status=(
            str(order_payload["status"])
            if order_payload.get("status") not in (None, "")
            else None
        ),
        order_ref=(
            str(order_payload["order_ref"])
            if order_payload.get("order_ref") not in (None, "")
            else None
        ),
        action=(
            str(order_payload["action"])
            if order_payload.get("action") not in (None, "")
            else None
        ),
        total_quantity=_to_decimal(order_payload.get("total_quantity")),
        symbol=(
            str(order_payload["symbol"])
            if order_payload.get("symbol") not in (None, "")
            else None
        ),
        account=(
            str(order_payload["account"])
            if order_payload.get("account") not in (None, "")
            else None
        ),
        security_type=(
            str(order_payload["security_type"])
            if order_payload.get("security_type") not in (None, "")
            else None
        ),
        exchange=(
            str(order_payload["exchange"])
            if order_payload.get("exchange") not in (None, "")
            else None
        ),
        primary_exchange=(
            str(order_payload["primary_exchange"])
            if order_payload.get("primary_exchange") not in (None, "")
            else None
        ),
        currency=(
            str(order_payload["currency"])
            if order_payload.get("currency") not in (None, "")
            else None
        ),
        local_symbol=(
            str(order_payload["local_symbol"])
            if order_payload.get("local_symbol") not in (None, "")
            else None
        ),
        order_type=(
            str(order_payload["order_type"])
            if order_payload.get("order_type") not in (None, "")
            else None
        ),
        limit_price=_to_decimal(order_payload.get("limit_price")),
        aux_price=_to_decimal(order_payload.get("aux_price")),
        outside_rth=(
            bool(order_payload["outside_rth"])
            if order_payload.get("outside_rth") is not None
            else None
        ),
        oca_group=(
            str(order_payload["oca_group"])
            if order_payload.get("oca_group") not in (None, "")
            else None
        ),
        oca_type=(
            int(order_payload["oca_type"])
            if order_payload.get("oca_type") not in (None, "")
            else None
        ),
        transmit=(
            bool(order_payload["transmit"])
            if order_payload.get("transmit") is not None
            else None
        ),
        warning_text=(
            str(order_payload["warning_text"])
            if order_payload.get("warning_text") not in (None, "")
            else None
        ),
        reject_reason=(
            str(order_payload["reject_reason"])
            if order_payload.get("reject_reason") not in (None, "")
            else None
        ),
        completed_status=(
            str(order_payload["completed_status"])
            if order_payload.get("completed_status") not in (None, "")
            else None
        ),
        completed_time=(
            str(order_payload["completed_time"])
            if order_payload.get("completed_time") not in (None, "")
            else None
        ),
    )
    account_key = _resolve_account_key(
        open_order.account,
        default_account_key=default_account_key,
        context=f"Open-order callback {open_order.order_id}",
    )
    broker_account = _get_or_create_broker_account(
        session,
        broker_kind=broker_kind,
        account_key=account_key,
        base_currency=_normalize_text(open_order.currency),
    )
    _upsert_open_order(
        session,
        broker_kind=broker_kind,
        broker_account=broker_account,
        open_order=open_order,
        observed_at=event_at,
        default_account_key=default_account_key,
    )


def _persist_order_status_callback_event(
    session: Session,
    *,
    broker_kind: str,
    event_payload: dict[str, Any],
    default_account_key: str | None,
) -> None:
    status_payload = event_payload.get("order_status")
    if not isinstance(status_payload, dict):
        raise ValueError("Order-status callback payload was missing the serialized status body.")
    event_at = event_payload.get("event_at")
    if not isinstance(event_at, datetime):
        raise ValueError("Order-status callback payload was missing a valid event_at timestamp.")

    external_order_id = (
        str(status_payload["orderId"])
        if status_payload.get("orderId") not in (None, "")
        else None
    )
    external_perm_id = (
        str(status_payload["permId"])
        if status_payload.get("permId") not in (None, "")
        else None
    )
    broker_order = None
    if default_account_key is not None:
        broker_order = _find_broker_order(
            session,
            broker_kind=broker_kind,
            account_key=default_account_key,
            external_order_id=external_order_id,
            external_perm_id=external_perm_id,
            order_ref=None,
        )
    if broker_order is None:
        broker_order = _find_broker_order_any_account(
            session,
            broker_kind=broker_kind,
            external_order_id=external_order_id,
            external_perm_id=external_perm_id,
        )

    if broker_order is None:
        instruction_record = _find_instruction_record_for_order(
            session,
            order_ref=None,
            external_order_id=external_order_id,
            external_perm_id=external_perm_id,
        )
        if instruction_record is None:
            raise ValueError(
                f"Order-status callback for order '{external_order_id or external_perm_id}' "
                "could not be matched to a durable broker_order or instruction row."
            )
        reconstruction_account_key = _resolve_account_key(
            None,
            default_account_key=default_account_key,
            context=(
                f"Order-status callback for order '{external_order_id or external_perm_id}'"
            ),
        )
        reconstruction_status = _require_text(
            (
                str(status_payload["status"])
                if status_payload.get("status") not in (None, "")
                else None
            ),
            context=f"Order-status callback for order '{external_order_id or external_perm_id}'",
        )
        if _matches_instruction_exit_identity(
            instruction_record,
            external_order_id=external_order_id,
            external_perm_id=external_perm_id,
        ):
            broker_order = _reconstruct_exit_broker_order_from_instruction(
                session,
                broker_kind=broker_kind,
                instruction_record=instruction_record,
                account_key=reconstruction_account_key,
                external_order_id=external_order_id,
                external_perm_id=external_perm_id,
                external_client_id=(
                    str(status_payload["clientId"])
                    if status_payload.get("clientId") not in (None, "")
                    else None
                ),
                status=reconstruction_status,
                observed_at=event_at,
                raw_payload={"order_status_callback": _serialize_for_json(status_payload)},
                metadata_json={"reconstructed_from_instruction": True},
            )
        else:
            broker_order = _reconstruct_entry_broker_order_from_instruction(
                session,
                broker_kind=broker_kind,
                instruction_record=instruction_record,
                account_key=reconstruction_account_key,
                external_order_id=external_order_id,
                external_perm_id=external_perm_id,
                external_client_id=(
                    str(status_payload["clientId"])
                    if status_payload.get("clientId") not in (None, "")
                    else None
                ),
                status=reconstruction_status,
                observed_at=event_at,
                raw_payload={"order_status_callback": _serialize_for_json(status_payload)},
                metadata_json={"reconstructed_from_instruction": True},
            )

    status_after = _require_text(
        (
            str(status_payload["status"])
            if status_payload.get("status") not in (None, "")
            else None
        ),
        context=f"Order-status callback for order '{external_order_id or external_perm_id}'",
    )
    status_before = broker_order.status
    broker_order.status = status_after
    broker_order.external_perm_id = external_perm_id or broker_order.external_perm_id
    broker_order.external_client_id = (
        str(status_payload["clientId"])
        if status_payload.get("clientId") not in (None, "")
        else broker_order.external_client_id
    )
    broker_order.last_status_at = event_at
    broker_order.raw_payload = {
        **broker_order.raw_payload,
        "last_order_status_callback": _serialize_for_json(status_payload),
    }
    metadata = dict(broker_order.metadata_json)
    metadata["last_order_status_callback"] = _serialize_for_json(status_payload)
    broker_order.metadata_json = metadata
    _record_broker_order_event(
        session,
        broker_order=broker_order,
        event_type="order_status_callback",
        event_at=event_at,
        status_before=status_before,
        status_after=status_after,
        payload=_serialize_for_json(status_payload),
        note="Persisted broker order-status callback directly from the live session.",
    )
    _persist_order_status_fill_if_terminal(
        session,
        broker_order=broker_order,
        status_payload=status_payload,
        event_at=event_at,
    )
    _sync_instruction_from_broker_order_terminal_status(
        session,
        broker_order=broker_order,
        event_at=event_at,
        event_source="broker_callback",
        status_payload=status_payload,
        note=(
            "Broker callback marked the unfilled entry order as cancelled before "
            "expiry."
        ),
    )


def _persist_order_status_fill_if_terminal(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
    status_payload: dict[str, Any],
    event_at: datetime,
) -> None:
    status = _normalize_text(
        str(status_payload["status"])
        if status_payload.get("status") not in (None, "")
        else None
    )
    if status is None or status.upper() != "FILLED":
        return

    filled_quantity = _to_decimal(status_payload.get("filled")) or Decimal("0")
    if filled_quantity <= 0:
        return
    remaining_quantity = _to_decimal(status_payload.get("remaining"))
    if remaining_quantity not in (None, Decimal("0")):
        return

    fill_price = _to_decimal(status_payload.get("avgFillPrice"))
    if fill_price is None or fill_price <= 0:
        fill_price = _to_decimal(status_payload.get("lastFillPrice"))
    if fill_price is None or fill_price <= 0:
        return

    existing_fills = list(
        session.execute(
            select(ExecutionFillRecord).where(
                ExecutionFillRecord.broker_order_id == broker_order.id,
            )
        ).scalars()
    )
    if any(not _is_order_status_synthetic_fill(fill) for fill in existing_fills):
        return

    external_execution_id = _order_status_fill_execution_id(broker_order)
    existing_fill = next(
        (
            fill
            for fill in existing_fills
            if fill.external_execution_id == external_execution_id
        ),
        None,
    )
    raw_payload = {
        "synthetic_from_order_status_callback": True,
        "evidence_source": "broker_order_status",
        "order_status_callback": _serialize_for_json(status_payload),
        "order_status_callback_at": event_at.isoformat(),
    }
    if existing_fill is not None:
        existing_fill.instruction_id = broker_order.instruction_id
        existing_fill.external_perm_id = broker_order.external_perm_id
        existing_fill.order_ref = broker_order.order_ref
        existing_fill.side = broker_order.side
        existing_fill.quantity = _require_text(
            _decimal_to_string(filled_quantity),
            context="Order-status fill quantity",
        )
        existing_fill.price = _require_text(
            _decimal_to_string(fill_price),
            context="Order-status fill price",
        )
        existing_fill.executed_at = event_at
        existing_fill.raw_payload = raw_payload
        return

    session.add(
        ExecutionFillRecord(
            broker_order_id=broker_order.id,
            instruction_id=broker_order.instruction_id,
            broker_account_id=broker_order.broker_account_id,
            broker_kind=broker_order.broker_kind,
            account_key=broker_order.account_key,
            is_virtual=_is_virtual_ledger_identity(
                broker_kind=broker_order.broker_kind,
                account_key=broker_order.account_key,
            ),
            external_execution_id=external_execution_id,
            external_order_id=broker_order.external_order_id,
            external_perm_id=broker_order.external_perm_id,
            order_ref=broker_order.order_ref,
            symbol=_require_text(
                broker_order.symbol,
                context="Order-status fill symbol",
            ),
            exchange=broker_order.exchange,
            currency=_require_text(
                broker_order.currency,
                context="Order-status fill currency",
            ),
            security_type=_require_text(
                broker_order.security_type,
                context="Order-status fill security type",
            ),
            side=broker_order.side,
            quantity=_require_text(
                _decimal_to_string(filled_quantity),
                context="Order-status fill quantity",
            ),
            price=_require_text(
                _decimal_to_string(fill_price),
                context="Order-status fill price",
            ),
            commission=None,
            commission_currency=None,
            executed_at=event_at,
            raw_payload=raw_payload,
        )
    )


def _persist_order_error_callback_event(
    session: Session,
    *,
    broker_kind: str,
    event_payload: dict[str, Any],
    default_account_key: str | None,
) -> None:
    error_payload = event_payload.get("error")
    if not isinstance(error_payload, dict):
        raise ValueError("Order-error callback payload was missing the serialized error body.")
    event_at = event_payload.get("event_at")
    if not isinstance(event_at, datetime):
        raise ValueError("Order-error callback payload was missing a valid event_at timestamp.")

    external_order_id = (
        str(error_payload["orderId"])
        if error_payload.get("orderId") not in (None, "")
        else None
    )
    broker_order = None
    if default_account_key is not None:
        broker_order = _find_broker_order(
            session,
            broker_kind=broker_kind,
            account_key=default_account_key,
            external_order_id=external_order_id,
            external_perm_id=None,
            order_ref=None,
        )
    if broker_order is None:
        broker_order = _find_broker_order_any_account(
            session,
            broker_kind=broker_kind,
            external_order_id=external_order_id,
            external_perm_id=None,
        )

    if broker_order is None:
        instruction_record = _find_instruction_record_for_order(
            session,
            order_ref=None,
            external_order_id=external_order_id,
            external_perm_id=None,
        )
        if instruction_record is None:
            reconstruction_account_key = _resolve_account_key(
                None,
                default_account_key=default_account_key,
                context=f"Order-error callback for order '{external_order_id}'",
            )
            broker_account = _get_or_create_broker_account(
                session,
                broker_kind=broker_kind,
                account_key=reconstruction_account_key,
            )
            broker_order = BrokerOrderRecord(
                broker_account_id=broker_account.id,
                broker_kind=broker_kind,
                account_key=reconstruction_account_key,
                order_role="BROKER_NATIVE",
                external_order_id=external_order_id,
                external_perm_id=None,
                external_client_id=None,
                order_ref=None,
                symbol="UNKNOWN",
                exchange="UNKNOWN",
                currency="UNKNOWN",
                security_type="UNKNOWN",
                side="UNKNOWN",
                order_type="UNKNOWN",
                status="ERROR",
                submitted_at=None,
                last_status_at=event_at,
                raw_payload={
                    "order_error_callback": _serialize_for_json(error_payload)
                },
                metadata_json={
                    "unmatched_callback": True,
                    "reconstructed_from_broker_error": True,
                },
            )
            session.add(broker_order)
            session.flush()
        else:
            reconstruction_account_key = _resolve_account_key(
                None,
                default_account_key=default_account_key,
                context=f"Order-error callback for order '{external_order_id}'",
            )
            if _matches_instruction_exit_identity(
                instruction_record,
                external_order_id=external_order_id,
                external_perm_id=None,
            ):
                broker_order = _reconstruct_exit_broker_order_from_instruction(
                    session,
                    broker_kind=broker_kind,
                    instruction_record=instruction_record,
                    account_key=reconstruction_account_key,
                    external_order_id=external_order_id,
                    external_perm_id=None,
                    external_client_id=None,
                    status="ERROR",
                    observed_at=event_at,
                    raw_payload={
                        "order_error_callback": _serialize_for_json(error_payload)
                    },
                    metadata_json={"reconstructed_from_instruction": True},
                )
            else:
                broker_order = _reconstruct_entry_broker_order_from_instruction(
                    session,
                    broker_kind=broker_kind,
                    instruction_record=instruction_record,
                    account_key=reconstruction_account_key,
                    external_order_id=external_order_id,
                    external_perm_id=None,
                    external_client_id=None,
                    status="ERROR",
                    observed_at=event_at,
                    raw_payload={
                        "order_error_callback": _serialize_for_json(error_payload)
                    },
                    metadata_json={"reconstructed_from_instruction": True},
                )

    metadata = dict(broker_order.metadata_json)
    metadata["last_order_error_callback"] = _serialize_for_json(error_payload)
    broker_order.metadata_json = metadata
    status_before = broker_order.status
    error_code = error_payload.get("errorCode")
    if error_code == 10147:
        broker_order.status = "NOT_FOUND_AT_BROKER"
        broker_order.last_status_at = event_at
    status_after = broker_order.status
    _record_broker_order_event(
        session,
        broker_order=broker_order,
        event_type="order_error_callback",
        event_at=event_at,
        status_before=status_before,
        status_after=status_after,
        payload=_serialize_for_json(error_payload),
        note="Persisted broker order error callback directly from the live session.",
    )
    _mark_instruction_needs_review_from_order_error(
        session,
        broker_order=broker_order,
        event_at=event_at,
        error_payload=error_payload,
    )
    _sync_instruction_from_broker_order_terminal_status(
        session,
        broker_order=broker_order,
        event_at=event_at,
        event_source="broker_callback",
        status_payload=None,
        note=(
            "Broker callback marked the unfilled entry order as terminal before "
            "expiry."
        ),
    )


def persist_broker_callback_events(
    session_factory: sessionmaker[Session],
    callback_events: list[dict[str, Any]],
    *,
    broker_kind: str,
    default_account_key: str | None = None,
) -> None:
    """Persist live broker callback events from the long-lived session into the ledger."""

    if not callback_events:
        return

    with session_scope(session_factory) as session:
        for event_payload in callback_events:
            if not isinstance(event_payload, dict):
                raise ValueError("Broker callback event payload must be a mapping.")
            event_type = event_payload.get("event_type")
            if event_type == "open_order":
                _persist_open_order_callback_event(
                    session,
                    broker_kind=broker_kind,
                    event_payload=event_payload,
                    default_account_key=default_account_key,
                )
                continue
            if event_type == "order_status":
                _persist_order_status_callback_event(
                    session,
                    broker_kind=broker_kind,
                    event_payload=event_payload,
                    default_account_key=default_account_key,
                )
                continue
            if event_type == "order_error":
                _persist_order_error_callback_event(
                    session,
                    broker_kind=broker_kind,
                    event_payload=event_payload,
                    default_account_key=default_account_key,
                )
                continue
            raise ValueError(f"Unsupported broker callback event type: {event_type!r}")
