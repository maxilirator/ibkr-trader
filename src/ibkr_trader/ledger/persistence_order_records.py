from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.models import BrokerAccountRecord
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.ibkr.runtime_snapshot import BrokerOpenOrder
from ibkr_trader.ledger.persistence_order_reconstruction import _build_open_order_from_submission
from ibkr_trader.ledger.persistence_shared import _broker_order_lineage_changed
from ibkr_trader.ledger.persistence_shared import _decimal_to_string
from ibkr_trader.ledger.persistence_shared import _find_broker_order
from ibkr_trader.ledger.persistence_shared import _find_instruction_record_for_order
from ibkr_trader.ledger.persistence_shared import _get_or_create_broker_account
from ibkr_trader.ledger.persistence_shared import _infer_order_role
from ibkr_trader.ledger.persistence_shared import _instruction_instrument_field
from ibkr_trader.ledger.persistence_shared import _instruction_payload
from ibkr_trader.ledger.persistence_shared import _is_open_order_status
from ibkr_trader.ledger.persistence_shared import _is_virtual_ledger_identity
from ibkr_trader.ledger.persistence_shared import _normalize_text
from ibkr_trader.ledger.persistence_shared import _record_broker_order_event
from ibkr_trader.ledger.persistence_shared import _require_text
from ibkr_trader.ledger.persistence_shared import _resolve_account_key
from ibkr_trader.ledger.persistence_shared import _resolve_order_role
from ibkr_trader.ledger.persistence_shared import _retire_reused_external_order_id
from ibkr_trader.ledger.persistence_shared import _serialize_for_json
from ibkr_trader.ledger.persistence_shared import _submission_field

def persist_broker_order_submission(
    session: Session,
    *,
    broker_kind: str,
    instruction_record: InstructionRecord | None,
    broker_submission: dict[str, Any],
    observed_at: datetime,
    fallback_account_key: str | None = None,
    order_role: str | None = None,
    event_type: str = "broker_order_submitted",
    note: str | None = "Persisted broker order submission into the ledger.",
) -> BrokerOrderRecord:
    """Persist a broker submit response into the durable order ledger immediately."""

    synthesized_open_order = _build_open_order_from_submission(
        broker_submission=broker_submission,
        instruction_record=instruction_record,
        fallback_account_key=fallback_account_key,
        fallback_order_role=order_role,
    )
    broker_account = _get_or_create_broker_account(
        session,
        broker_kind=broker_kind,
        account_key=_resolve_account_key(
            synthesized_open_order.account,
            default_account_key=fallback_account_key,
            context=f"Broker submission order {synthesized_open_order.order_id}",
        ),
        base_currency=_normalize_text(synthesized_open_order.currency),
        is_virtual=bool(broker_submission.get("is_virtual")),
    )
    broker_order = _upsert_open_order(
        session,
        broker_kind=broker_kind,
        broker_account=broker_account,
        open_order=synthesized_open_order,
        observed_at=observed_at,
        default_account_key=fallback_account_key,
    )
    broker_order.order_role = _resolve_order_role(
        order_ref=synthesized_open_order.order_ref,
        explicit_order_role=order_role,
    )
    broker_order.time_in_force = (
        _normalize_text(str(_submission_field(broker_submission, "order", "time_in_force")))
        if _submission_field(broker_submission, "order", "time_in_force") not in (None, "")
        else broker_order.time_in_force
    )
    metadata = dict(broker_order.metadata_json)
    metadata["broker_submission"] = _serialize_for_json(broker_submission)
    broker_order.metadata_json = metadata
    _record_broker_order_event(
        session,
        broker_order=broker_order,
        event_type=event_type,
        event_at=observed_at,
        status_before=None,
        status_after=broker_order.status,
        payload=_serialize_for_json(broker_submission),
        note=note,
    )
    if _is_virtual_ledger_identity(
        broker_kind=broker_kind,
        account_key=broker_order.account_key,
    ):
        from ibkr_trader.virtual.execution import persist_virtual_execution_from_submission

        persist_virtual_execution_from_submission(
            session,
            broker_order=broker_order,
            instruction_record=instruction_record,
            broker_submission=broker_submission,
            observed_at=observed_at,
        )
    return broker_order


def persist_broker_order_cancellation(
    session: Session,
    *,
    broker_kind: str,
    broker_cancellation: dict[str, Any],
    observed_at: datetime,
    instruction_record: InstructionRecord | None = None,
    fallback_account_key: str | None = None,
    event_type: str = "broker_order_cancelled",
    note: str | None = "Persisted broker order cancellation into the ledger.",
) -> BrokerOrderRecord:
    """Persist a broker cancel response against an existing durable order row."""

    broker_status = broker_cancellation.get("broker_order_status")
    if not isinstance(broker_status, dict):
        raise ValueError("Broker cancellation payload is missing broker_order_status.")

    external_order_id = (
        str(broker_status["orderId"])
        if broker_status.get("orderId") not in (None, "")
        else None
    )
    external_perm_id = (
        str(broker_status["permId"])
        if broker_status.get("permId") not in (None, "")
        else None
    )
    if external_order_id is None and external_perm_id is None:
        raise ValueError("Broker cancellation payload did not include an order identifier.")

    account_key = _normalize_text(fallback_account_key)
    broker_order = None
    if account_key is not None:
        broker_order = _find_broker_order(
            session,
            broker_kind=broker_kind,
            account_key=account_key,
            external_order_id=external_order_id,
            external_perm_id=external_perm_id,
            order_ref=None,
        )
    if broker_order is None and instruction_record is not None:
        inferred_instruction_account = _normalize_text(
            _instruction_payload(instruction_record).get("account", {}).get("account_key")
            if isinstance(_instruction_payload(instruction_record).get("account"), dict)
            else None
        )
        if inferred_instruction_account is not None:
            broker_order = _find_broker_order(
                session,
                broker_kind=broker_kind,
                account_key=inferred_instruction_account,
                external_order_id=external_order_id,
                external_perm_id=external_perm_id,
                order_ref=None,
            )
    if broker_order is None and instruction_record is not None:
        broker_order = _find_broker_order(
            session,
            broker_kind=broker_kind,
            account_key=instruction_record.account_key,
            external_order_id=external_order_id,
            external_perm_id=external_perm_id,
            order_ref=instruction_record.instruction_id,
        )
    status_after = _require_text(
        (
            str(broker_status["status"])
            if broker_status.get("status") not in (None, "")
            else None
        ),
        context=f"Broker cancellation for order {external_order_id or external_perm_id}",
    )
    reconstructed_from_instruction = False
    if broker_order is None:
        if instruction_record is None:
            raise ValueError(
                "Broker cancellation could not be matched to a durable broker_order row."
            )
        instruction_payload = _instruction_payload(instruction_record)
        entry_payload = instruction_payload.get("entry")
        if entry_payload is not None and not isinstance(entry_payload, dict):
            raise ValueError(
                f"Instruction '{instruction_record.instruction_id}' entry payload was not a mapping."
            )
        entry_payload = entry_payload or {}
        account_key = _resolve_account_key(
            None,
            default_account_key=fallback_account_key,
            context=f"Broker cancellation for order {external_order_id or external_perm_id}",
        )
        broker_account = _get_or_create_broker_account(
            session,
            broker_kind=broker_kind,
            account_key=account_key,
            base_currency=instruction_record.currency,
        )
        broker_order = BrokerOrderRecord(
            instruction_id=instruction_record.id,
            broker_account_id=broker_account.id,
            broker_kind=broker_kind,
            account_key=account_key,
            is_virtual=_is_virtual_ledger_identity(
                broker_kind=broker_kind,
                account_key=account_key,
            ),
            order_role=_infer_order_role(instruction_record.instruction_id),
            external_order_id=external_order_id,
            external_perm_id=external_perm_id,
            external_client_id=(
                str(broker_status["clientId"])
                if broker_status.get("clientId") not in (None, "")
                else None
            ),
            order_ref=instruction_record.instruction_id,
            symbol=instruction_record.symbol,
            exchange=instruction_record.exchange,
            currency=instruction_record.currency,
            security_type=_require_text(
                _instruction_instrument_field(instruction_record, "security_type"),
                context=f"Instruction security type for {instruction_record.instruction_id}",
            ),
            primary_exchange=_instruction_instrument_field(
                instruction_record, "primary_exchange"
            ),
            local_symbol=_instruction_instrument_field(instruction_record, "local_symbol"),
            side=instruction_record.side,
            order_type=instruction_record.order_type,
            time_in_force=None,
            status=status_after,
            total_quantity=instruction_record.entry_submitted_quantity,
            limit_price=_normalize_text(
                str(entry_payload.get("limit_price"))
            )
            if entry_payload.get("limit_price") not in (None, "")
            else None,
            stop_price=None,
            submitted_at=instruction_record.submit_at,
            last_status_at=observed_at,
            raw_payload={"broker_cancellation": _serialize_for_json(broker_cancellation)},
            metadata_json={"reconstructed_from_instruction": True},
        )
        session.add(broker_order)
        session.flush()
        reconstructed_from_instruction = True

    status_before = None if reconstructed_from_instruction else broker_order.status
    broker_order.status = status_after
    broker_order.external_perm_id = external_perm_id or broker_order.external_perm_id
    broker_order.external_client_id = (
        str(broker_status["clientId"])
        if broker_status.get("clientId") not in (None, "")
        else broker_order.external_client_id
    )
    broker_order.last_status_at = observed_at
    metadata = dict(broker_order.metadata_json)
    metadata["broker_cancellation"] = _serialize_for_json(broker_cancellation)
    broker_order.metadata_json = metadata
    _record_broker_order_event(
        session,
        broker_order=broker_order,
        event_type=event_type,
        event_at=observed_at,
        status_before=status_before,
        status_after=status_after,
        payload=_serialize_for_json(broker_cancellation),
        note=note,
    )
    return broker_order


def persist_broker_order_cancellation_result(
    session_factory: sessionmaker[Session],
    *,
    broker_kind: str,
    broker_cancellation: dict[str, Any],
    observed_at: datetime,
    instruction_record: InstructionRecord | None = None,
    fallback_account_key: str | None = None,
    event_type: str = "broker_order_cancelled",
    note: str | None = "Persisted broker order cancellation into the ledger.",
) -> None:
    """Persist a broker cancel response into the durable ledger."""

    with session_scope(session_factory) as session:
        persist_broker_order_cancellation(
            session,
            broker_kind=broker_kind,
            broker_cancellation=broker_cancellation,
            observed_at=observed_at,
            instruction_record=instruction_record,
            fallback_account_key=fallback_account_key,
            event_type=event_type,
            note=note,
        )


def _upsert_open_order(
    session: Session,
    *,
    broker_kind: str,
    broker_account: BrokerAccountRecord,
    open_order: BrokerOpenOrder,
    observed_at: datetime,
    default_account_key: str | None,
) -> BrokerOrderRecord:
    account_key = _resolve_account_key(
        open_order.account,
        default_account_key=default_account_key,
        context=f"Open order {open_order.order_id}",
    )
    external_order_id = str(open_order.order_id)
    external_perm_id = (
        str(open_order.perm_id) if open_order.perm_id is not None else None
    )
    status = _require_text(
        open_order.status,
        context=f"Open order {open_order.order_id} status",
    )
    symbol = _require_text(
        open_order.symbol,
        context=f"Open order {open_order.order_id} symbol",
    )
    exchange = _require_text(
        open_order.exchange,
        context=f"Open order {open_order.order_id} exchange",
    )
    currency = _require_text(
        open_order.currency,
        context=f"Open order {open_order.order_id} currency",
    )
    security_type = _require_text(
        open_order.security_type,
        context=f"Open order {open_order.order_id} security type",
    )
    side = _require_text(
        open_order.action,
        context=f"Open order {open_order.order_id} side",
    )
    order_type = _require_text(
        open_order.order_type,
        context=f"Open order {open_order.order_id} order type",
    )

    instruction_record = _find_instruction_record_for_order(
        session,
        order_ref=open_order.order_ref,
        external_order_id=external_order_id,
        external_perm_id=external_perm_id,
    )
    broker_order = _find_broker_order(
        session,
        broker_kind=broker_kind,
        account_key=account_key,
        external_order_id=external_order_id,
        external_perm_id=external_perm_id,
        order_ref=open_order.order_ref,
    )

    payload = _serialize_for_json(asdict(open_order))
    if broker_order is not None and _broker_order_lineage_changed(
        broker_order,
        external_perm_id=external_perm_id,
        order_ref=open_order.order_ref,
        symbol=symbol,
        local_symbol=open_order.local_symbol,
    ):
        _retire_reused_external_order_id(
            session,
            broker_order=broker_order,
            retired_at=observed_at,
            replacement_external_order_id=external_order_id,
            replacement_external_perm_id=external_perm_id,
            replacement_order_ref=open_order.order_ref,
            replacement_symbol=symbol,
            replacement_local_symbol=open_order.local_symbol,
        )
        broker_order = None

    previous_status = broker_order.status if broker_order is not None else None
    previous_payload = broker_order.raw_payload if broker_order is not None else None
    previous_perm_id = broker_order.external_perm_id if broker_order is not None else None
    is_virtual = _is_virtual_ledger_identity(
        broker_kind=broker_kind,
        account_key=account_key,
    )

    if broker_order is None:
        broker_order = BrokerOrderRecord(
            instruction_id=instruction_record.id if instruction_record is not None else None,
            broker_account_id=broker_account.id,
            broker_kind=broker_kind,
            account_key=account_key,
            is_virtual=is_virtual,
            order_role=_infer_order_role(open_order.order_ref),
            external_order_id=external_order_id,
            external_perm_id=external_perm_id,
            external_client_id=(
                str(open_order.client_id) if open_order.client_id is not None else None
            ),
            order_ref=_normalize_text(open_order.order_ref),
            symbol=symbol,
            exchange=exchange,
            currency=currency,
            security_type=security_type,
            primary_exchange=_normalize_text(open_order.primary_exchange),
            local_symbol=_normalize_text(open_order.local_symbol),
            side=side,
            order_type=order_type,
            time_in_force=None,
            status=status,
            total_quantity=_decimal_to_string(open_order.total_quantity),
            limit_price=_decimal_to_string(open_order.limit_price),
            stop_price=_decimal_to_string(open_order.aux_price),
            submitted_at=observed_at,
            last_status_at=observed_at,
            raw_payload=payload,
            metadata_json={},
        )
        session.add(broker_order)
        session.flush()
    else:
        inferred_order_role = _infer_order_role(open_order.order_ref)
        if instruction_record is not None:
            broker_order.instruction_id = instruction_record.id
        if inferred_order_role != "BROKER_NATIVE":
            broker_order.order_role = inferred_order_role
        elif broker_order.order_role in (None, ""):
            broker_order.order_role = inferred_order_role
        broker_order.broker_account_id = broker_account.id
        broker_order.is_virtual = broker_order.is_virtual or is_virtual
        broker_order.external_order_id = external_order_id or broker_order.external_order_id
        broker_order.external_perm_id = external_perm_id
        broker_order.external_client_id = (
            str(open_order.client_id) if open_order.client_id is not None else None
        )
        broker_order.order_ref = _normalize_text(open_order.order_ref)
        broker_order.symbol = symbol
        broker_order.exchange = exchange
        broker_order.currency = currency
        broker_order.security_type = security_type
        broker_order.primary_exchange = _normalize_text(open_order.primary_exchange)
        broker_order.local_symbol = _normalize_text(open_order.local_symbol)
        broker_order.side = side
        broker_order.order_type = order_type
        broker_order.status = status
        broker_order.total_quantity = _decimal_to_string(open_order.total_quantity)
        broker_order.limit_price = _decimal_to_string(open_order.limit_price)
        broker_order.stop_price = _decimal_to_string(open_order.aux_price)
        broker_order.last_status_at = observed_at
        broker_order.raw_payload = payload

    metadata = dict(broker_order.metadata_json or {})
    for key in (
        "missing_from_runtime_snapshot",
        "missing_from_runtime_snapshot_at",
        "missing_from_runtime_snapshot_account_scope",
        "missing_from_runtime_snapshot_open_order_count",
    ):
        metadata.pop(key, None)
    if previous_perm_id not in (None, external_perm_id) or (
        _is_open_order_status(status)
        and _normalize_text(open_order.reject_reason) is None
    ):
        metadata.pop("last_order_error_callback", None)
    metadata.update(
        {
            "outside_rth": open_order.outside_rth,
            "oca_group": _normalize_text(open_order.oca_group),
            "oca_type": open_order.oca_type,
            "transmit": open_order.transmit,
            "warning_text": _normalize_text(open_order.warning_text),
            "reject_reason": _normalize_text(open_order.reject_reason),
            "completed_status": _normalize_text(open_order.completed_status),
            "completed_time": _normalize_text(open_order.completed_time),
        }
    )
    broker_order.metadata_json = metadata

    should_record_event = (
        previous_status is None
        or previous_status != status
        or previous_payload != payload
    )
    if should_record_event:
        _record_broker_order_event(
            session,
            broker_order=broker_order,
            event_type=(
                "open_order_observed"
                if previous_status is None
                else "open_order_updated"
            ),
            event_at=observed_at,
            status_before=previous_status,
            status_after=status,
            payload=payload,
            note=(
                "Observed open order in broker runtime snapshot."
                if previous_status is None
                else "Updated broker order from runtime snapshot."
            ),
        )
    return broker_order


