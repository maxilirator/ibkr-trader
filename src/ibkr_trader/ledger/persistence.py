from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.models import AccountSnapshotRecord
from ibkr_trader.db.models import BrokerAccountRecord
from ibkr_trader.db.models import BrokerOrderEventRecord
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import ExecutionFillRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import PositionSnapshotRecord
from ibkr_trader.ibkr.runtime_snapshot import BrokerExecution
from ibkr_trader.ibkr.runtime_snapshot import BrokerOpenOrder
from ibkr_trader.ibkr.runtime_snapshot import BrokerPortfolioItem
from ibkr_trader.ibkr.runtime_snapshot import BrokerPosition
from ibkr_trader.ibkr.runtime_snapshot import BrokerRuntimeSnapshot

BROKER_KIND_IBKR = "IBKR"


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


def _require_text(value: str | None, *, context: str) -> str:
    normalized = _normalize_text(value)
    if normalized is None:
        raise ValueError(f"{context} is required but was missing in the broker payload.")
    return normalized


def _decimal_to_string(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _resolve_account_key(
    raw_account_key: str | None,
    *,
    default_account_key: str | None,
    context: str,
) -> str:
    normalized = _normalize_text(raw_account_key)
    if normalized is not None:
        return normalized
    normalized_default = _normalize_text(default_account_key)
    if normalized_default is not None:
        return normalized_default
    raise ValueError(
        f"{context} did not include a broker account and no default account was configured."
    )


def _derive_account_base_currency(
    account_values: dict[str, dict[str, str | None]],
) -> str | None:
    for payload in account_values.values():
        if not isinstance(payload, dict):
            continue
        currency = _normalize_text(payload.get("currency"))
        if currency is not None:
            return currency
    return None


def _get_or_create_broker_account(
    session: Session,
    *,
    broker_kind: str,
    account_key: str,
    base_currency: str | None = None,
) -> BrokerAccountRecord:
    broker_account = session.execute(
        select(BrokerAccountRecord).where(
            BrokerAccountRecord.broker_kind == broker_kind,
            BrokerAccountRecord.account_key == account_key,
        )
    ).scalar_one_or_none()
    if broker_account is None:
        broker_account = BrokerAccountRecord(
            broker_kind=broker_kind,
            account_key=account_key,
            base_currency=base_currency,
        )
        session.add(broker_account)
        session.flush()
    elif broker_account.base_currency is None and base_currency is not None:
        broker_account.base_currency = base_currency
    return broker_account


def _instruction_payload(instruction_record: InstructionRecord) -> dict[str, Any]:
    raw_payload = instruction_record.payload.get("instruction")
    if not isinstance(raw_payload, dict):
        raise ValueError(
            f"Instruction '{instruction_record.instruction_id}' does not contain a valid persisted payload."
        )
    return raw_payload


def _instruction_instrument_field(
    instruction_record: InstructionRecord,
    field_name: str,
) -> str | None:
    instrument_payload = _instruction_payload(instruction_record).get("instrument")
    if not isinstance(instrument_payload, dict):
        return None
    raw_value = instrument_payload.get(field_name)
    if raw_value in (None, ""):
        return None
    return str(raw_value)


def _submission_field(
    payload: dict[str, Any],
    *path: str,
) -> Any:
    current: Any = payload
    for component in path:
        if not isinstance(current, dict):
            return None
        current = current.get(component)
    return current


def _infer_order_role(order_ref: str | None) -> str:
    normalized = _normalize_text(order_ref)
    if normalized is None:
        return "BROKER_NATIVE"
    if ":exit:" in normalized:
        return "EXIT"
    return "ENTRY"


def _instruction_id_from_order_ref(order_ref: str | None) -> str | None:
    normalized = _normalize_text(order_ref)
    if normalized is None:
        return None
    if ":exit:" in normalized:
        return normalized.split(":exit:", 1)[0] or None
    return normalized


def _resolve_order_role(
    *,
    order_ref: str | None,
    explicit_order_role: str | None,
) -> str:
    normalized_role = _normalize_text(explicit_order_role)
    if normalized_role is not None:
        return normalized_role
    return _infer_order_role(order_ref)


def _find_instruction_record_for_order(
    session: Session,
    *,
    order_ref: str | None,
    external_order_id: str | None,
    external_perm_id: str | None,
) -> InstructionRecord | None:
    instruction_id = _instruction_id_from_order_ref(order_ref)
    if instruction_id is not None:
        return session.execute(
            select(InstructionRecord).where(
                InstructionRecord.instruction_id == instruction_id
            )
        ).scalar_one_or_none()

    predicates = []
    if external_order_id is not None:
        try:
            order_id = int(external_order_id)
        except ValueError:
            order_id = None
        if order_id is not None:
            predicates.append(InstructionRecord.broker_order_id == order_id)
            predicates.append(InstructionRecord.exit_order_id == order_id)
    if external_perm_id is not None:
        try:
            perm_id = int(external_perm_id)
        except ValueError:
            perm_id = None
        if perm_id is not None:
            predicates.append(InstructionRecord.broker_perm_id == perm_id)
            predicates.append(InstructionRecord.exit_perm_id == perm_id)

    if not predicates:
        return None
    return session.execute(
        select(InstructionRecord).where(or_(*predicates))
    ).scalar_one_or_none()


def _find_broker_order(
    session: Session,
    *,
    broker_kind: str,
    account_key: str,
    external_order_id: str | None,
    external_perm_id: str | None,
    order_ref: str | None,
) -> BrokerOrderRecord | None:
    if external_order_id is not None:
        broker_order = session.execute(
            select(BrokerOrderRecord).where(
                BrokerOrderRecord.broker_kind == broker_kind,
                BrokerOrderRecord.account_key == account_key,
                BrokerOrderRecord.external_order_id == external_order_id,
            )
        ).scalar_one_or_none()
        if broker_order is not None:
            return broker_order

    if external_perm_id is not None:
        broker_order = session.execute(
            select(BrokerOrderRecord).where(
                BrokerOrderRecord.broker_kind == broker_kind,
                BrokerOrderRecord.account_key == account_key,
                BrokerOrderRecord.external_perm_id == external_perm_id,
            )
        ).scalar_one_or_none()
        if broker_order is not None:
            return broker_order

    normalized_ref = _normalize_text(order_ref)
    if normalized_ref is not None:
        return session.execute(
            select(BrokerOrderRecord).where(
                BrokerOrderRecord.broker_kind == broker_kind,
                BrokerOrderRecord.account_key == account_key,
                BrokerOrderRecord.order_ref == normalized_ref,
            )
        ).scalar_one_or_none()
    return None


def _find_broker_order_any_account(
    session: Session,
    *,
    broker_kind: str,
    external_order_id: str | None,
    external_perm_id: str | None,
) -> BrokerOrderRecord | None:
    if external_perm_id is not None:
        matches = session.execute(
            select(BrokerOrderRecord).where(
                BrokerOrderRecord.broker_kind == broker_kind,
                BrokerOrderRecord.external_perm_id == external_perm_id,
            )
        ).scalars().all()
        if len(matches) > 1:
            raise ValueError(
                f"Multiple broker_order rows matched external_perm_id '{external_perm_id}'."
            )
        if matches:
            return matches[0]

    if external_order_id is not None:
        matches = session.execute(
            select(BrokerOrderRecord).where(
                BrokerOrderRecord.broker_kind == broker_kind,
                BrokerOrderRecord.external_order_id == external_order_id,
            )
        ).scalars().all()
        if len(matches) > 1:
            raise ValueError(
                f"Multiple broker_order rows matched external_order_id '{external_order_id}'."
            )
        if matches:
            return matches[0]

    return None


def _record_broker_order_event(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
    event_type: str,
    event_at: datetime,
    status_before: str | None,
    status_after: str,
    payload: dict[str, Any],
    note: str | None,
) -> None:
    session.add(
        BrokerOrderEventRecord(
            broker_order_id=broker_order.id,
            event_type=event_type,
            event_at=event_at,
            status_before=status_before,
            status_after=status_after,
            payload=_serialize_for_json(payload),
            note=note,
        )
    )


def _entry_payload(instruction_record: InstructionRecord) -> dict[str, Any]:
    instruction_payload = _instruction_payload(instruction_record)
    raw_entry_payload = instruction_payload.get("entry")
    if raw_entry_payload is None:
        return {}
    if not isinstance(raw_entry_payload, dict):
        raise ValueError(
            f"Instruction '{instruction_record.instruction_id}' entry payload was not a mapping."
        )
    return raw_entry_payload


def _reconstruct_entry_broker_order_from_instruction(
    session: Session,
    *,
    broker_kind: str,
    instruction_record: InstructionRecord,
    account_key: str,
    external_order_id: str | None,
    external_perm_id: str | None,
    external_client_id: str | None,
    status: str,
    observed_at: datetime,
    raw_payload: dict[str, Any],
    metadata_json: dict[str, Any],
) -> BrokerOrderRecord:
    if instruction_record.exit_order_id is not None and external_order_id is not None:
        if str(instruction_record.exit_order_id) == external_order_id:
            raise ValueError(
                f"Cannot reconstruct exit broker order '{external_order_id}' from entry-only instruction fields."
            )
    if instruction_record.exit_perm_id is not None and external_perm_id is not None:
        if str(instruction_record.exit_perm_id) == external_perm_id:
            raise ValueError(
                f"Cannot reconstruct exit broker order perm id '{external_perm_id}' from entry-only instruction fields."
            )

    broker_account = _get_or_create_broker_account(
        session,
        broker_kind=broker_kind,
        account_key=account_key,
        base_currency=instruction_record.currency,
    )
    entry_payload = _entry_payload(instruction_record)
    broker_order = BrokerOrderRecord(
        instruction_id=instruction_record.id,
        broker_account_id=broker_account.id,
        broker_kind=broker_kind,
        account_key=account_key,
        order_role="ENTRY",
        external_order_id=external_order_id,
        external_perm_id=external_perm_id,
        external_client_id=external_client_id,
        order_ref=instruction_record.instruction_id,
        symbol=instruction_record.symbol,
        exchange=instruction_record.exchange,
        currency=instruction_record.currency,
        security_type=_require_text(
            _instruction_instrument_field(instruction_record, "security_type"),
            context=f"Instruction security type for {instruction_record.instruction_id}",
        ),
        primary_exchange=_instruction_instrument_field(instruction_record, "primary_exchange"),
        local_symbol=_instruction_instrument_field(instruction_record, "local_symbol"),
        side=instruction_record.side,
        order_type=instruction_record.order_type,
        time_in_force=(
            str(entry_payload["time_in_force"])
            if entry_payload.get("time_in_force") not in (None, "")
            else None
        ),
        status=status,
        total_quantity=instruction_record.entry_submitted_quantity,
        limit_price=(
            str(entry_payload["limit_price"])
            if entry_payload.get("limit_price") not in (None, "")
            else None
        ),
        stop_price=None,
        submitted_at=instruction_record.submit_at,
        last_status_at=observed_at,
        raw_payload=_serialize_for_json(raw_payload),
        metadata_json=metadata_json,
    )
    session.add(broker_order)
    session.flush()
    return broker_order


def _build_open_order_from_submission(
    *,
    broker_submission: dict[str, Any],
    instruction_record: InstructionRecord | None,
    fallback_account_key: str | None,
    fallback_order_role: str | None,
) -> BrokerOpenOrder:
    broker_status = broker_submission.get("broker_order_status")
    if not isinstance(broker_status, dict):
        raise ValueError("Broker submission payload is missing broker_order_status.")

    order_payload = broker_submission.get("order")
    if order_payload is not None and not isinstance(order_payload, dict):
        raise ValueError("Broker submission order payload must be a mapping.")
    order_payload = order_payload or {}

    tws_submission = broker_submission.get("tws_submission")
    if tws_submission is not None and not isinstance(tws_submission, dict):
        raise ValueError("Broker submission tws_submission payload must be a mapping.")
    tws_submission = tws_submission or {}

    tws_contract = tws_submission.get("contract")
    if tws_contract is not None and not isinstance(tws_contract, dict):
        raise ValueError("Broker submission tws_submission.contract must be a mapping.")
    tws_contract = tws_contract or {}

    tws_order_state = tws_submission.get("order_state")
    if tws_order_state is not None and not isinstance(tws_order_state, dict):
        raise ValueError("Broker submission tws_submission.order_state must be a mapping.")
    tws_order_state = tws_order_state or {}

    resolved_contract = broker_submission.get("resolved_contract")
    if resolved_contract is not None and not isinstance(resolved_contract, dict):
        raise ValueError("Broker submission resolved_contract payload must be a mapping.")
    resolved_contract = resolved_contract or {}

    order_id = broker_status.get("orderId")
    if order_id in (None, ""):
        raise ValueError("Broker submission did not include broker_order_status.orderId.")

    order_ref = (
        _normalize_text(str(tws_submission.get("order_ref")))
        if tws_submission.get("order_ref") not in (None, "")
        else None
    ) or (
        _normalize_text(str(order_payload.get("order_ref")))
        if order_payload.get("order_ref") not in (None, "")
        else None
    ) or (instruction_record.instruction_id if instruction_record is not None else None)

    status = (
        _normalize_text(str(broker_status.get("status")))
        if broker_status.get("status") not in (None, "")
        else None
    ) or (
        _normalize_text(str(tws_order_state.get("status")))
        if tws_order_state.get("status") not in (None, "")
        else None
    )

    symbol = (
        _normalize_text(str(tws_contract.get("symbol")))
        if tws_contract.get("symbol") not in (None, "")
        else None
    ) or (
        _normalize_text(str(resolved_contract.get("symbol")))
        if resolved_contract.get("symbol") not in (None, "")
        else None
    ) or (instruction_record.symbol if instruction_record is not None else None)

    exchange = (
        _normalize_text(str(tws_contract.get("exchange")))
        if tws_contract.get("exchange") not in (None, "")
        else None
    ) or (
        _normalize_text(str(resolved_contract.get("exchange")))
        if resolved_contract.get("exchange") not in (None, "")
        else None
    ) or (instruction_record.exchange if instruction_record is not None else None)

    currency = (
        _normalize_text(str(tws_contract.get("currency")))
        if tws_contract.get("currency") not in (None, "")
        else None
    ) or (
        _normalize_text(str(resolved_contract.get("currency")))
        if resolved_contract.get("currency") not in (None, "")
        else None
    ) or (instruction_record.currency if instruction_record is not None else None)

    security_type = (
        _normalize_text(str(tws_contract.get("security_type")))
        if tws_contract.get("security_type") not in (None, "")
        else None
    ) or (
        _normalize_text(str(resolved_contract.get("security_type")))
        if resolved_contract.get("security_type") not in (None, "")
        else None
    ) or (
        _instruction_instrument_field(instruction_record, "security_type")
        if instruction_record is not None
        else None
    )

    action = (
        _normalize_text(str(tws_submission.get("action")))
        if tws_submission.get("action") not in (None, "")
        else None
    ) or (
        _normalize_text(str(order_payload.get("action")))
        if order_payload.get("action") not in (None, "")
        else None
    ) or (instruction_record.side if instruction_record is not None else None)

    order_type = (
        _normalize_text(str(tws_submission.get("order_type")))
        if tws_submission.get("order_type") not in (None, "")
        else None
    ) or (
        _normalize_text(str(order_payload.get("order_type")))
        if order_payload.get("order_type") not in (None, "")
        else None
    ) or (instruction_record.order_type if instruction_record is not None else None)

    return BrokerOpenOrder(
        order_id=int(order_id),
        perm_id=(
            int(broker_status["permId"])
            if broker_status.get("permId") not in (None, "")
            else None
        ),
        client_id=(
            int(broker_status["clientId"])
            if broker_status.get("clientId") not in (None, "")
            else None
        ),
        status=status,
        order_ref=order_ref,
        action=action,
        total_quantity=_to_decimal(
            order_payload.get("total_quantity")
            if order_payload.get("total_quantity") not in (None, "")
            else tws_submission.get("total_quantity")
        ),
        symbol=symbol,
        account=_resolve_account_key(
            (
                _normalize_text(str(broker_submission.get("account")))
                if broker_submission.get("account") not in (None, "")
                else None
            ) or (
                _normalize_text(str(tws_submission.get("account")))
                if tws_submission.get("account") not in (None, "")
                else None
            ),
            default_account_key=fallback_account_key,
            context=f"Broker submission order {order_id}",
        ),
        security_type=security_type,
        exchange=exchange,
        primary_exchange=(
            _normalize_text(str(tws_contract.get("primary_exchange")))
            if tws_contract.get("primary_exchange") not in (None, "")
            else None
        ) or (
            _normalize_text(str(resolved_contract.get("primary_exchange")))
            if resolved_contract.get("primary_exchange") not in (None, "")
            else None
        ) or (
            _instruction_instrument_field(instruction_record, "primary_exchange")
            if instruction_record is not None
            else None
        ),
        currency=currency,
        local_symbol=(
            _normalize_text(str(tws_contract.get("local_symbol")))
            if tws_contract.get("local_symbol") not in (None, "")
            else None
        ) or (
            _normalize_text(str(resolved_contract.get("local_symbol")))
            if resolved_contract.get("local_symbol") not in (None, "")
            else None
        ) or (
            _instruction_instrument_field(instruction_record, "local_symbol")
            if instruction_record is not None
            else None
        ),
        order_type=order_type,
        limit_price=_to_decimal(order_payload.get("limit_price")),
        aux_price=_to_decimal(
            order_payload.get("stop_price")
            if order_payload.get("stop_price") not in (None, "")
            else order_payload.get("aux_price")
        ),
        outside_rth=(
            bool(order_payload["outside_rth"])
            if order_payload.get("outside_rth") is not None
            else None
        ),
        oca_group=(
            _normalize_text(str(order_payload.get("oca_group")))
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
            _normalize_text(str(tws_order_state.get("warning_text")))
            if tws_order_state.get("warning_text") not in (None, "")
            else None
        ),
        reject_reason=(
            _normalize_text(str(tws_order_state.get("reject_reason")))
            if tws_order_state.get("reject_reason") not in (None, "")
            else None
        ),
        completed_status=(
            _normalize_text(str(tws_order_state.get("completed_status")))
            if tws_order_state.get("completed_status") not in (None, "")
            else None
        ),
        completed_time=(
            _normalize_text(str(tws_order_state.get("completed_time")))
            if tws_order_state.get("completed_time") not in (None, "")
            else None
        ),
    )


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
    previous_status = broker_order.status if broker_order is not None else None
    previous_payload = broker_order.raw_payload if broker_order is not None else None

    if broker_order is None:
        broker_order = BrokerOrderRecord(
            instruction_id=instruction_record.id if instruction_record is not None else None,
            broker_account_id=broker_account.id,
            broker_kind=broker_kind,
            account_key=account_key,
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
        if broker_order.instruction_id is None and instruction_record is not None:
            broker_order.instruction_id = instruction_record.id
        broker_order.broker_account_id = broker_account.id
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

    metadata = dict(broker_order.metadata_json)
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


def _persist_account_snapshots(
    session: Session,
    *,
    broker_kind: str,
    snapshot: BrokerRuntimeSnapshot,
    captured_at: datetime,
    default_account_key: str | None,
) -> dict[str, BrokerAccountRecord]:
    broker_accounts: dict[str, BrokerAccountRecord] = {}
    for account_key, values in snapshot.account_values.items():
        normalized_account_key = _resolve_account_key(
            account_key,
            default_account_key=default_account_key,
            context="Account snapshot",
        )
        if not isinstance(values, dict):
            raise ValueError(
                f"Account snapshot for '{normalized_account_key}' was not a mapping."
            )
        broker_account = _get_or_create_broker_account(
            session,
            broker_kind=broker_kind,
            account_key=normalized_account_key,
            base_currency=_derive_account_base_currency(values),
        )
        broker_accounts[normalized_account_key] = broker_account

        def read_value(tag: str) -> str | None:
            payload = values.get(tag)
            if not isinstance(payload, dict):
                return None
            value = payload.get("value")
            return str(value) if value not in (None, "") else None

        currency = None
        for payload in values.values():
            if isinstance(payload, dict) and payload.get("currency") not in (None, ""):
                currency = str(payload["currency"])
                break

        session.add(
            AccountSnapshotRecord(
                broker_account_id=broker_account.id,
                snapshot_at=captured_at,
                source="runtime_snapshot",
                net_liquidation=read_value("NetLiquidation"),
                total_cash_value=read_value("TotalCashValue"),
                buying_power=read_value("BuyingPower"),
                available_funds=read_value("AvailableFunds"),
                excess_liquidity=read_value("ExcessLiquidity"),
                cushion=read_value("Cushion"),
                currency=currency,
                raw_payload=_serialize_for_json(values),
            )
        )
    return broker_accounts


def _build_position_union(
    snapshot: BrokerRuntimeSnapshot,
    *,
    default_account_key: str | None,
) -> dict[
    tuple[str, str, str, str, str | None, str | None],
    tuple[BrokerPosition | None, BrokerPortfolioItem | None],
]:
    positions_by_key: dict[
        tuple[str, str, str, str, str | None, str | None],
        tuple[BrokerPosition | None, BrokerPortfolioItem | None],
    ] = {}

    for position in snapshot.positions:
        account_key = _resolve_account_key(
            position.account,
            default_account_key=default_account_key,
            context=f"Position for symbol {position.symbol or '<missing>'}",
        )
        key = (
            account_key,
            _require_text(position.symbol, context="Position symbol"),
            _require_text(position.exchange, context="Position exchange"),
            _require_text(position.currency, context="Position currency"),
            _normalize_text(position.security_type),
            _normalize_text(position.local_symbol),
        )
        if key in positions_by_key and positions_by_key[key][0] is not None:
            raise ValueError(
                f"Duplicate broker position snapshot for {account_key}:{key[1]}:{key[2]}."
            )
        previous_portfolio = positions_by_key.get(key, (None, None))[1]
        positions_by_key[key] = (position, previous_portfolio)

    for portfolio_item in snapshot.portfolio:
        account_key = _resolve_account_key(
            portfolio_item.account,
            default_account_key=default_account_key,
            context=f"Portfolio item for symbol {portfolio_item.symbol or '<missing>'}",
        )
        key = (
            account_key,
            _require_text(portfolio_item.symbol, context="Portfolio symbol"),
            _require_text(portfolio_item.exchange, context="Portfolio exchange"),
            _require_text(portfolio_item.currency, context="Portfolio currency"),
            _normalize_text(portfolio_item.security_type),
            _normalize_text(portfolio_item.local_symbol),
        )
        if key in positions_by_key and positions_by_key[key][1] is not None:
            raise ValueError(
                f"Duplicate broker portfolio snapshot for {account_key}:{key[1]}:{key[2]}."
            )
        previous_position = positions_by_key.get(key, (None, None))[0]
        positions_by_key[key] = (previous_position, portfolio_item)

    return positions_by_key


def _persist_position_snapshots(
    session: Session,
    *,
    broker_kind: str,
    snapshot: BrokerRuntimeSnapshot,
    captured_at: datetime,
    default_account_key: str | None,
    broker_accounts: dict[str, BrokerAccountRecord],
) -> None:
    for key, (position, portfolio_item) in _build_position_union(
        snapshot,
        default_account_key=default_account_key,
    ).items():
        account_key, symbol, exchange, currency, security_type, local_symbol = key
        quantity_candidates = [
            item
            for item in (
                position.position if position is not None else None,
                portfolio_item.position if portfolio_item is not None else None,
            )
            if item is not None
        ]
        if not quantity_candidates:
            raise ValueError(
                f"Position snapshot for {account_key}:{symbol}:{exchange} did not include a quantity."
            )
        if len(quantity_candidates) == 2 and quantity_candidates[0] != quantity_candidates[1]:
            raise ValueError(
                f"Position quantity mismatch for {account_key}:{symbol}:{exchange}: "
                f"{quantity_candidates[0]} != {quantity_candidates[1]}."
            )
        quantity = quantity_candidates[0]

        broker_account = broker_accounts.get(account_key)
        if broker_account is None:
            broker_account = _get_or_create_broker_account(
                session,
                broker_kind=broker_kind,
                account_key=account_key,
                base_currency=currency,
            )
            broker_accounts[account_key] = broker_account

        session.add(
            PositionSnapshotRecord(
                broker_account_id=broker_account.id,
                snapshot_at=captured_at,
                source="runtime_snapshot",
                symbol=symbol,
                exchange=exchange,
                currency=currency,
                security_type=_require_text(
                    security_type,
                    context=f"Position security type for {account_key}:{symbol}:{exchange}",
                ),
                primary_exchange=(
                    _normalize_text(position.primary_exchange)
                    if position is not None
                    else _normalize_text(
                        portfolio_item.primary_exchange if portfolio_item is not None else None
                    )
                ),
                local_symbol=local_symbol,
                quantity=str(quantity),
                average_cost=_decimal_to_string(
                    position.average_cost if position is not None else None
                )
                or _decimal_to_string(
                    portfolio_item.average_cost if portfolio_item is not None else None
                ),
                market_price=_decimal_to_string(
                    portfolio_item.market_price if portfolio_item is not None else None
                ),
                market_value=_decimal_to_string(
                    portfolio_item.market_value if portfolio_item is not None else None
                ),
                unrealized_pnl=_decimal_to_string(
                    portfolio_item.unrealized_pnl if portfolio_item is not None else None
                ),
                realized_pnl=_decimal_to_string(
                    portfolio_item.realized_pnl if portfolio_item is not None else None
                ),
                raw_payload=_serialize_for_json(
                    {
                        "position": asdict(position) if position is not None else None,
                        "portfolio": (
                            asdict(portfolio_item) if portfolio_item is not None else None
                        ),
                    }
                ),
            )
        )


def _persist_executions(
    session: Session,
    *,
    broker_kind: str,
    snapshot: BrokerRuntimeSnapshot,
    captured_at: datetime,
    default_account_key: str | None,
    broker_accounts: dict[str, BrokerAccountRecord],
) -> None:
    for execution in snapshot.executions:
        exec_id = _require_text(
            execution.exec_id,
            context="Execution identifier",
        )
        account_key = _resolve_account_key(
            execution.account,
            default_account_key=default_account_key,
            context=f"Execution {exec_id}",
        )
        broker_account = broker_accounts.get(account_key)
        if broker_account is None:
            broker_account = _get_or_create_broker_account(
                session,
                broker_kind=broker_kind,
                account_key=account_key,
                base_currency=_normalize_text(execution.currency),
            )
            broker_accounts[account_key] = broker_account

        existing_fill = session.execute(
            select(ExecutionFillRecord).where(
                ExecutionFillRecord.broker_kind == broker_kind,
                ExecutionFillRecord.account_key == account_key,
                ExecutionFillRecord.external_execution_id == exec_id,
            )
        ).scalar_one_or_none()
        if existing_fill is not None:
            continue

        external_order_id = (
            str(execution.order_id) if execution.order_id is not None else None
        )
        external_perm_id = (
            str(execution.perm_id) if execution.perm_id is not None else None
        )
        instruction_record = _find_instruction_record_for_order(
            session,
            order_ref=execution.order_ref,
            external_order_id=external_order_id,
            external_perm_id=external_perm_id,
        )
        broker_order = _find_broker_order(
            session,
            broker_kind=broker_kind,
            account_key=account_key,
            external_order_id=external_order_id,
            external_perm_id=external_perm_id,
            order_ref=execution.order_ref,
        )

        symbol = _normalize_text(execution.symbol) or (
            broker_order.symbol if broker_order is not None else None
        )
        currency = _normalize_text(execution.currency) or (
            broker_order.currency if broker_order is not None else None
        )
        security_type = _normalize_text(execution.security_type) or (
            broker_order.security_type if broker_order is not None else None
        )
        exchange = _normalize_text(execution.exchange) or (
            broker_order.exchange if broker_order is not None else None
        )
        local_symbol = _normalize_text(execution.local_symbol) or (
            broker_order.local_symbol if broker_order is not None else None
        )
        primary_exchange = _normalize_text(execution.primary_exchange) or (
            broker_order.primary_exchange if broker_order is not None else None
        )

        if broker_order is None:
            if external_order_id is None:
                raise ValueError(
                    f"Execution {exec_id} could not be linked to a broker order and did not expose an order id."
                )
            broker_order = BrokerOrderRecord(
                instruction_id=instruction_record.id if instruction_record is not None else None,
                broker_account_id=broker_account.id,
                broker_kind=broker_kind,
                account_key=account_key,
                order_role=_infer_order_role(execution.order_ref),
                external_order_id=external_order_id,
                external_perm_id=external_perm_id,
                external_client_id=(
                    str(execution.client_id) if execution.client_id is not None else None
                ),
                order_ref=_normalize_text(execution.order_ref),
                symbol=_require_text(
                    symbol,
                    context=f"Execution {exec_id} symbol",
                ),
                exchange=_require_text(
                    exchange,
                    context=f"Execution {exec_id} exchange",
                ),
                currency=_require_text(
                    currency,
                    context=f"Execution {exec_id} currency",
                ),
                security_type=_require_text(
                    security_type,
                    context=f"Execution {exec_id} security type",
                ),
                primary_exchange=primary_exchange,
                local_symbol=local_symbol,
                side=_normalize_text(execution.side) or "UNKNOWN",
                order_type="UNKNOWN",
                time_in_force=None,
                status="FILLED",
                total_quantity=_decimal_to_string(execution.shares),
                limit_price=None,
                stop_price=None,
                submitted_at=execution.executed_at or captured_at,
                last_status_at=execution.executed_at or captured_at,
                raw_payload=_serialize_for_json(asdict(execution)),
                metadata_json={},
            )
            session.add(broker_order)
            session.flush()
            _record_broker_order_event(
                session,
                broker_order=broker_order,
                event_type="execution_observed_without_open_order",
                event_at=execution.executed_at or captured_at,
                status_before=None,
                status_after="FILLED",
                payload=_serialize_for_json(asdict(execution)),
                note="Created broker order record from execution because no open-order ledger row existed.",
            )
        else:
            if broker_order.instruction_id is None and instruction_record is not None:
                broker_order.instruction_id = instruction_record.id

        executed_at = execution.executed_at
        if executed_at is None:
            raise ValueError(f"Execution {exec_id} did not include an execution timestamp.")

        session.add(
            ExecutionFillRecord(
                broker_order_id=broker_order.id,
                instruction_id=instruction_record.id if instruction_record is not None else None,
                broker_account_id=broker_account.id,
                broker_kind=broker_kind,
                account_key=account_key,
                external_execution_id=exec_id,
                external_order_id=external_order_id,
                external_perm_id=external_perm_id,
                order_ref=_normalize_text(execution.order_ref),
                symbol=_require_text(symbol, context=f"Execution {exec_id} symbol"),
                exchange=exchange,
                currency=_require_text(currency, context=f"Execution {exec_id} currency"),
                security_type=_require_text(
                    security_type,
                    context=f"Execution {exec_id} security type",
                ),
                side=_normalize_text(execution.side),
                quantity=_require_text(
                    _decimal_to_string(execution.shares),
                    context=f"Execution {exec_id} quantity",
                ),
                price=_require_text(
                    _decimal_to_string(execution.price),
                    context=f"Execution {exec_id} price",
                ),
                commission=None,
                commission_currency=None,
                executed_at=executed_at,
                raw_payload=_serialize_for_json(asdict(execution)),
            )
        )


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
        broker_order = _reconstruct_entry_broker_order_from_instruction(
            session,
            broker_kind=broker_kind,
            instruction_record=instruction_record,
            account_key=_resolve_account_key(
                None,
                default_account_key=default_account_key,
                context=(
                    f"Order-status callback for order '{external_order_id or external_perm_id}'"
                ),
            ),
            external_order_id=external_order_id,
            external_perm_id=external_perm_id,
            external_client_id=(
                str(status_payload["clientId"])
                if status_payload.get("clientId") not in (None, "")
                else None
            ),
            status=_require_text(
                (
                    str(status_payload["status"])
                    if status_payload.get("status") not in (None, "")
                    else None
                ),
                context=f"Order-status callback for order '{external_order_id or external_perm_id}'",
            ),
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
            raise ValueError(
                f"Order-error callback for order '{external_order_id}' "
                "could not be matched to a durable broker_order or instruction row."
            )
        broker_order = _reconstruct_entry_broker_order_from_instruction(
            session,
            broker_kind=broker_kind,
            instruction_record=instruction_record,
            account_key=_resolve_account_key(
                None,
                default_account_key=default_account_key,
                context=f"Order-error callback for order '{external_order_id}'",
            ),
            external_order_id=external_order_id,
            external_perm_id=None,
            external_client_id=None,
            status="ERROR",
            observed_at=event_at,
            raw_payload={"order_error_callback": _serialize_for_json(error_payload)},
            metadata_json={"reconstructed_from_instruction": True},
        )

    metadata = dict(broker_order.metadata_json)
    metadata["last_order_error_callback"] = _serialize_for_json(error_payload)
    broker_order.metadata_json = metadata
    _record_broker_order_event(
        session,
        broker_order=broker_order,
        event_type="order_error_callback",
        event_at=event_at,
        status_before=broker_order.status,
        status_after=broker_order.status,
        payload=_serialize_for_json(error_payload),
        note="Persisted broker order error callback directly from the live session.",
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


def persist_broker_runtime_snapshot(
    session_factory: sessionmaker[Session],
    snapshot: BrokerRuntimeSnapshot,
    *,
    broker_kind: str,
    captured_at: datetime,
    default_account_key: str | None = None,
) -> None:
    """Persist a real broker runtime snapshot into durable ledger tables."""

    with session_scope(session_factory) as session:
        broker_accounts = _persist_account_snapshots(
            session,
            broker_kind=broker_kind,
            snapshot=snapshot,
            captured_at=captured_at,
            default_account_key=default_account_key,
        )

        _persist_position_snapshots(
            session,
            broker_kind=broker_kind,
            snapshot=snapshot,
            captured_at=captured_at,
            default_account_key=default_account_key,
            broker_accounts=broker_accounts,
        )

        for open_order in snapshot.open_orders.values():
            account_key = _resolve_account_key(
                open_order.account,
                default_account_key=default_account_key,
                context=f"Open order {open_order.order_id}",
            )
            broker_account = broker_accounts.get(account_key)
            if broker_account is None:
                broker_account = _get_or_create_broker_account(
                    session,
                    broker_kind=broker_kind,
                    account_key=account_key,
                    base_currency=_normalize_text(open_order.currency),
                )
                broker_accounts[account_key] = broker_account
            _upsert_open_order(
                session,
                broker_kind=broker_kind,
                broker_account=broker_account,
                open_order=open_order,
                observed_at=captured_at,
                default_account_key=default_account_key,
            )

        _persist_executions(
            session,
            broker_kind=broker_kind,
            snapshot=snapshot,
            captured_at=captured_at,
            default_account_key=default_account_key,
            broker_accounts=broker_accounts,
        )
