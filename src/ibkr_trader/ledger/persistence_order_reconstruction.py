from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.ibkr.runtime_snapshot import BrokerOpenOrder
from ibkr_trader.ledger.persistence_shared import _decimal_to_string
from ibkr_trader.ledger.persistence_shared import _get_or_create_broker_account
from ibkr_trader.ledger.persistence_shared import _infer_order_role
from ibkr_trader.ledger.persistence_shared import _instruction_instrument_field
from ibkr_trader.ledger.persistence_shared import _instruction_payload
from ibkr_trader.ledger.persistence_shared import _is_virtual_ledger_identity
from ibkr_trader.ledger.persistence_shared import _normalize_text
from ibkr_trader.ledger.persistence_shared import _require_text
from ibkr_trader.ledger.persistence_shared import _resolve_order_role
from ibkr_trader.ledger.persistence_shared import _resolve_account_key
from ibkr_trader.ledger.persistence_shared import _serialize_for_json
from ibkr_trader.ledger.persistence_shared import _to_decimal

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


def _exit_payload(instruction_record: InstructionRecord) -> dict[str, Any]:
    instruction_payload = _instruction_payload(instruction_record)
    raw_exit_payload = instruction_payload.get("exit")
    if raw_exit_payload is None:
        return {}
    if not isinstance(raw_exit_payload, dict):
        raise ValueError(
            f"Instruction '{instruction_record.instruction_id}' exit payload was not a mapping."
        )
    return raw_exit_payload


def _infer_exit_side(entry_side: str) -> str:
    normalized_entry_side = _require_text(entry_side, context="Instruction side").upper()
    if normalized_entry_side == "BUY":
        return "SELL"
    if normalized_entry_side == "SELL":
        return "BUY"
    raise ValueError(f"Unsupported instruction side for exit reconstruction: {entry_side!r}")


def _infer_exit_order_type_from_instruction(instruction_record: InstructionRecord) -> str:
    exit_payload = _exit_payload(instruction_record)
    if exit_payload.get("delayed_limit") is not None:
        return "LMT"
    if exit_payload.get("take_profit_pct") is not None:
        return "LMT"
    if exit_payload.get("stop_loss_pct") is not None:
        return "STP"
    if exit_payload.get("catastrophic_stop_loss_pct") is not None:
        return "STP"
    if bool(exit_payload.get("force_exit_next_session_open", False)):
        return "MKT"
    raise ValueError(
        "Cannot infer the exit order type because the persisted instruction does not "
        "contain a supported exit contract."
    )


def _matches_instruction_exit_identity(
    instruction_record: InstructionRecord,
    *,
    external_order_id: str | None,
    external_perm_id: str | None,
) -> bool:
    if instruction_record.exit_order_id is not None and external_order_id is not None:
        if str(instruction_record.exit_order_id) == external_order_id:
            return True
    if instruction_record.exit_perm_id is not None and external_perm_id is not None:
        if str(instruction_record.exit_perm_id) == external_perm_id:
            return True
    return False


def _find_matching_instruction_submission_event(
    session: Session,
    *,
    instruction_record: InstructionRecord,
    external_order_id: str | None,
    external_perm_id: str | None,
) -> tuple[InstructionEventRecord, dict[str, Any]] | None:
    candidate_events = session.execute(
        select(InstructionEventRecord)
        .where(InstructionEventRecord.instruction_id == instruction_record.id)
        .order_by(InstructionEventRecord.event_at.desc(), InstructionEventRecord.id.desc())
    ).scalars()

    for candidate_event in candidate_events:
        payload = candidate_event.payload or {}
        if not isinstance(payload, dict):
            continue
        broker_submission = payload.get("broker_submission")
        if not isinstance(broker_submission, dict):
            continue
        broker_status = broker_submission.get("broker_order_status")
        if not isinstance(broker_status, dict):
            continue
        candidate_order_id = (
            str(broker_status["orderId"])
            if broker_status.get("orderId") not in (None, "")
            else None
        )
        candidate_perm_id = (
            str(broker_status["permId"])
            if broker_status.get("permId") not in (None, "")
            else None
        )
        if external_order_id is not None and candidate_order_id == external_order_id:
            return candidate_event, broker_submission
        if external_perm_id is not None and candidate_perm_id == external_perm_id:
            return candidate_event, broker_submission
    return None


def _create_reconstructed_broker_order(
    session: Session,
    *,
    broker_kind: str,
    instruction_record: InstructionRecord,
    account_key: str,
    order_role: str,
    external_order_id: str | None,
    external_perm_id: str | None,
    external_client_id: str | None,
    order_ref: str | None,
    symbol: str,
    exchange: str,
    currency: str,
    security_type: str,
    primary_exchange: str | None,
    local_symbol: str | None,
    side: str,
    order_type: str,
    status: str,
    total_quantity: str | None,
    limit_price: str | None,
    stop_price: str | None,
    submitted_at: datetime | None,
    observed_at: datetime,
    raw_payload: dict[str, Any],
    metadata_json: dict[str, Any],
) -> BrokerOrderRecord:
    broker_account = _get_or_create_broker_account(
        session,
        broker_kind=broker_kind,
        account_key=account_key,
        base_currency=currency,
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
        order_role=order_role,
        external_order_id=external_order_id,
        external_perm_id=external_perm_id,
        external_client_id=external_client_id,
        order_ref=order_ref,
        symbol=symbol,
        exchange=exchange,
        currency=currency,
        security_type=security_type,
        primary_exchange=primary_exchange,
        local_symbol=local_symbol,
        side=side,
        order_type=order_type,
        time_in_force=None,
        status=status,
        total_quantity=total_quantity,
        limit_price=limit_price,
        stop_price=stop_price,
        submitted_at=submitted_at,
        last_status_at=observed_at,
        raw_payload=_serialize_for_json(raw_payload),
        metadata_json=metadata_json,
    )
    session.add(broker_order)
    session.flush()
    return broker_order


def _reconstruct_broker_order_from_instruction_submission_event(
    session: Session,
    *,
    broker_kind: str,
    instruction_record: InstructionRecord,
    account_key: str,
    external_order_id: str | None,
    external_perm_id: str | None,
    observed_at: datetime,
) -> BrokerOrderRecord | None:
    matched_submission = _find_matching_instruction_submission_event(
        session,
        instruction_record=instruction_record,
        external_order_id=external_order_id,
        external_perm_id=external_perm_id,
    )
    if matched_submission is None:
        return None

    submission_event, broker_submission = matched_submission
    synthesized_open_order = _build_open_order_from_submission(
        broker_submission=broker_submission,
        instruction_record=instruction_record,
        fallback_account_key=account_key,
        fallback_order_role=None,
    )
    return _create_reconstructed_broker_order(
        session,
        broker_kind=broker_kind,
        instruction_record=instruction_record,
        account_key=_resolve_account_key(
            synthesized_open_order.account,
            default_account_key=account_key,
            context=(
                f"Broker submission reconstruction for instruction "
                f"'{instruction_record.instruction_id}'"
            ),
        ),
        order_role=_resolve_order_role(
            order_ref=synthesized_open_order.order_ref,
            explicit_order_role=None,
        ),
        external_order_id=str(synthesized_open_order.order_id),
        external_perm_id=(
            str(synthesized_open_order.perm_id)
            if synthesized_open_order.perm_id is not None
            else None
        ),
        external_client_id=(
            str(synthesized_open_order.client_id)
            if synthesized_open_order.client_id is not None
            else None
        ),
        order_ref=_normalize_text(synthesized_open_order.order_ref),
        symbol=_require_text(
            synthesized_open_order.symbol,
            context=(
                f"Reconstructed broker order symbol for "
                f"'{instruction_record.instruction_id}'"
            ),
        ),
        exchange=_require_text(
            synthesized_open_order.exchange,
            context=(
                f"Reconstructed broker order exchange for "
                f"'{instruction_record.instruction_id}'"
            ),
        ),
        currency=_require_text(
            synthesized_open_order.currency,
            context=(
                f"Reconstructed broker order currency for "
                f"'{instruction_record.instruction_id}'"
            ),
        ),
        security_type=_require_text(
            synthesized_open_order.security_type,
            context=(
                f"Reconstructed broker order security type for "
                f"'{instruction_record.instruction_id}'"
            ),
        ),
        primary_exchange=_normalize_text(synthesized_open_order.primary_exchange),
        local_symbol=_normalize_text(synthesized_open_order.local_symbol),
        side=_require_text(
            synthesized_open_order.action,
            context=(
                f"Reconstructed broker order side for '{instruction_record.instruction_id}'"
            ),
        ),
        order_type=_require_text(
            synthesized_open_order.order_type,
            context=(
                f"Reconstructed broker order type for '{instruction_record.instruction_id}'"
            ),
        ),
        status=_require_text(
            synthesized_open_order.status,
            context=(
                f"Reconstructed broker order status for '{instruction_record.instruction_id}'"
            ),
        ),
        total_quantity=_decimal_to_string(synthesized_open_order.total_quantity),
        limit_price=_decimal_to_string(synthesized_open_order.limit_price),
        stop_price=_decimal_to_string(synthesized_open_order.aux_price),
        submitted_at=submission_event.event_at,
        observed_at=observed_at,
        raw_payload={
            "reconstructed_from_instruction_event": True,
            "instruction_event_id": submission_event.id,
            "instruction_event_type": submission_event.event_type,
            "broker_submission": broker_submission,
        },
        metadata_json={
            "reconstructed_from_instruction_event": True,
            "instruction_event_id": submission_event.id,
            "instruction_event_type": submission_event.event_type,
        },
    )


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
    if _matches_instruction_exit_identity(
        instruction_record,
        external_order_id=external_order_id,
        external_perm_id=external_perm_id,
    ):
        raise ValueError(
            "Entry broker-order reconstruction was invoked for an exit-tracked order. "
            "Use the exit reconstruction path instead."
        )

    entry_payload = _entry_payload(instruction_record)
    return _create_reconstructed_broker_order(
        session,
        broker_kind=broker_kind,
        instruction_record=instruction_record,
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
        status=status,
        total_quantity=instruction_record.entry_submitted_quantity,
        limit_price=(
            str(entry_payload["limit_price"])
            if entry_payload.get("limit_price") not in (None, "")
            else None
        ),
        stop_price=None,
        submitted_at=instruction_record.submit_at,
        observed_at=observed_at,
        raw_payload=raw_payload,
        metadata_json=metadata_json,
    )


def _reconstruct_exit_broker_order_from_instruction(
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
    matched_submission_reconstruction = _reconstruct_broker_order_from_instruction_submission_event(
        session,
        broker_kind=broker_kind,
        instruction_record=instruction_record,
        account_key=account_key,
        external_order_id=external_order_id,
        external_perm_id=external_perm_id,
        observed_at=observed_at,
    )
    if matched_submission_reconstruction is not None:
        return matched_submission_reconstruction

    return _create_reconstructed_broker_order(
        session,
        broker_kind=broker_kind,
        instruction_record=instruction_record,
        account_key=account_key,
        order_role="EXIT",
        external_order_id=external_order_id,
        external_perm_id=external_perm_id,
        external_client_id=external_client_id,
        order_ref=f"{instruction_record.instruction_id}:exit:reconstructed",
        symbol=instruction_record.symbol,
        exchange=instruction_record.exchange,
        currency=instruction_record.currency,
        security_type=_require_text(
            _instruction_instrument_field(instruction_record, "security_type"),
            context=f"Instruction security type for {instruction_record.instruction_id}",
        ),
        primary_exchange=_instruction_instrument_field(instruction_record, "primary_exchange"),
        local_symbol=_instruction_instrument_field(instruction_record, "local_symbol"),
        side=_infer_exit_side(instruction_record.side),
        order_type=_infer_exit_order_type_from_instruction(instruction_record),
        status=status,
        total_quantity=(
            instruction_record.exit_submitted_quantity
            or instruction_record.entry_filled_quantity
            or instruction_record.entry_submitted_quantity
        ),
        limit_price=None,
        stop_price=None,
        submitted_at=instruction_record.entry_filled_at or instruction_record.submit_at,
        observed_at=observed_at,
        raw_payload=raw_payload,
        metadata_json={
            **metadata_json,
            "reconstructed_exit_order": True,
            "reconstruction_source": "instruction_fields",
        },
    )


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


