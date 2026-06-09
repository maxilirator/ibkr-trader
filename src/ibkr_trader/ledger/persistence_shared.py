from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import func
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
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import PositionSnapshotRecord
from ibkr_trader.ibkr.runtime_snapshot import BrokerOpenOrder
from ibkr_trader.ibkr.runtime_snapshot import BrokerPortfolioItem
from ibkr_trader.ibkr.runtime_snapshot import BrokerPosition
from ibkr_trader.ibkr.runtime_snapshot import BrokerRuntimeSnapshot
from ibkr_trader.ledger.instruction_projection import (
    mark_instruction_needs_review_from_order_error as _mark_instruction_needs_review_from_order_error,
)
from ibkr_trader.ledger.instruction_projection import (
    sync_instruction_from_broker_order_terminal_status as _sync_instruction_from_broker_order_terminal_status,
)
from ibkr_trader.virtual.accounts import BROKER_KIND_VIRTUAL
from ibkr_trader.virtual.accounts import is_virtual_account_key

BROKER_KIND_IBKR = "IBKR"
ORDER_STATUS_FILL_EXECUTION_ID_PREFIX = "order-status-fill:"

_OPEN_ORDER_CLOSED_STATUSES = {
    "API_CANCELLED",
    "CANCELLED",
    "ERROR",
    "FILLED",
    "INACTIVE",
    "NOT_FOUND_AT_BROKER",
    "REJECTED",
}
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


def _is_virtual_ledger_identity(*, broker_kind: str, account_key: str | None) -> bool:
    return broker_kind == BROKER_KIND_VIRTUAL or is_virtual_account_key(account_key)


def _order_status_fill_execution_id(broker_order: BrokerOrderRecord) -> str:
    order_id = _normalize_text(broker_order.external_order_id) or "no-order-id"
    perm_id = _normalize_text(broker_order.external_perm_id) or "no-perm-id"
    return (
        f"{ORDER_STATUS_FILL_EXECUTION_ID_PREFIX}"
        f"{broker_order.broker_kind}:{broker_order.account_key}:"
        f"{broker_order.id}:{order_id}:{perm_id}"
    )


def _is_order_status_synthetic_fill(fill: ExecutionFillRecord) -> bool:
    if fill.external_execution_id.startswith(ORDER_STATUS_FILL_EXECUTION_ID_PREFIX):
        return True
    raw_payload = fill.raw_payload if isinstance(fill.raw_payload, dict) else {}
    return bool(raw_payload.get("synthetic_from_order_status_callback"))


def _delete_order_status_synthetic_fills_for_broker_order(
    session: Session,
    broker_order: BrokerOrderRecord,
) -> None:
    rows = session.execute(
        select(ExecutionFillRecord).where(
            ExecutionFillRecord.broker_order_id == broker_order.id,
        )
    ).scalars()
    for fill in rows:
        if _is_order_status_synthetic_fill(fill):
            session.delete(fill)


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
    is_virtual: bool | None = None,
) -> BrokerAccountRecord:
    resolved_is_virtual = (
        bool(is_virtual)
        or broker_kind == BROKER_KIND_VIRTUAL
        or is_virtual_account_key(account_key)
    )
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
            is_virtual=resolved_is_virtual,
        )
        session.add(broker_account)
        session.flush()
    elif broker_account.base_currency is None and base_currency is not None:
        broker_account.base_currency = base_currency
    if resolved_is_virtual and not broker_account.is_virtual:
        broker_account.is_virtual = True
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


def _normalize_symbol_key(value: str | None) -> str | None:
    normalized = _normalize_text(value)
    if normalized is None:
        return None
    return (
        normalized.upper()
        .replace(".", "-")
        .replace("_", "-")
        .replace(" ", "-")
    )


def _broker_order_lineage_changed(
    broker_order: BrokerOrderRecord,
    *,
    external_perm_id: str | None,
    order_ref: str | None,
    symbol: str | None,
    local_symbol: str | None,
) -> bool:
    previous_perm_id = broker_order.external_perm_id
    previous_order_ref = broker_order.order_ref
    previous_symbol_key = (
        _normalize_symbol_key(broker_order.local_symbol)
        or _normalize_symbol_key(broker_order.symbol)
    )
    incoming_order_ref = _normalize_text(order_ref)
    incoming_symbol_key = _normalize_symbol_key(local_symbol) or _normalize_symbol_key(symbol)
    return (
        (external_perm_id is not None and previous_perm_id not in (None, external_perm_id))
        or (
            previous_order_ref not in (None, incoming_order_ref)
            and incoming_order_ref is not None
        )
        or (
            previous_symbol_key is not None
            and incoming_symbol_key is not None
            and previous_symbol_key != incoming_symbol_key
        )
    )


def _retire_reused_external_order_id(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
    retired_at: datetime,
    replacement_external_order_id: str | None,
    replacement_external_perm_id: str | None,
    replacement_order_ref: str | None,
    replacement_symbol: str | None,
    replacement_local_symbol: str | None,
) -> None:
    retired_external_order_id = broker_order.external_order_id
    if retired_external_order_id is None:
        return

    metadata = dict(broker_order.metadata_json or {})
    retired_history = metadata.get("retired_reused_external_order_ids")
    if not isinstance(retired_history, list):
        retired_history = []
    retired_history.append(
        _serialize_for_json(
            {
                "retired_at": retired_at,
                "external_order_id": retired_external_order_id,
                "external_perm_id": broker_order.external_perm_id,
                "order_ref": broker_order.order_ref,
                "symbol": broker_order.symbol,
                "local_symbol": broker_order.local_symbol,
                "replacement_external_order_id": replacement_external_order_id,
                "replacement_external_perm_id": replacement_external_perm_id,
                "replacement_order_ref": _normalize_text(replacement_order_ref),
                "replacement_symbol": _normalize_text(replacement_symbol),
                "replacement_local_symbol": _normalize_text(replacement_local_symbol),
            }
        )
    )
    metadata["retired_reused_external_order_ids"] = retired_history
    broker_order.metadata_json = metadata

    broker_order.external_order_id = None
    _record_broker_order_event(
        session,
        broker_order=broker_order,
        event_type="external_order_id_reused",
        event_at=retired_at,
        status_before=broker_order.status,
        status_after=broker_order.status,
        payload={
            "retired_external_order_id": retired_external_order_id,
            "replacement_external_order_id": replacement_external_order_id,
            "replacement_external_perm_id": replacement_external_perm_id,
            "replacement_order_ref": _normalize_text(replacement_order_ref),
            "replacement_symbol": _normalize_text(replacement_symbol),
            "replacement_local_symbol": _normalize_text(replacement_local_symbol),
        },
        note="Retired reused broker order id so a new durable lineage could be created.",
    )


def _instruction_symbol_keys(instruction_record: InstructionRecord) -> set[str]:
    keys = {_normalize_symbol_key(instruction_record.symbol)}
    payload = instruction_record.payload if isinstance(instruction_record.payload, dict) else {}
    instruction_payload = payload.get("instruction", {})
    if isinstance(instruction_payload, dict):
        instrument_payload = instruction_payload.get("instrument", {})
        if isinstance(instrument_payload, dict):
            keys.add(_normalize_symbol_key(instrument_payload.get("symbol")))
            keys.add(_normalize_symbol_key(instrument_payload.get("local_symbol")))
            aliases = instrument_payload.get("aliases")
            if isinstance(aliases, list):
                keys.update(_normalize_symbol_key(str(alias)) for alias in aliases)
    return {key for key in keys if key is not None}


def _execution_symbol_keys(
    *,
    symbol: str | None,
    local_symbol: str | None,
) -> set[str]:
    return {
        key
        for key in (
            _normalize_symbol_key(symbol),
            _normalize_symbol_key(local_symbol),
        )
        if key is not None
    }


def _execution_side_is_exit_for_instruction(
    execution_side: str | None,
    instruction_record: InstructionRecord,
) -> bool:
    normalized_execution_side = (_normalize_text(execution_side) or "").upper()
    normalized_entry_side = (_normalize_text(instruction_record.side) or "").upper()
    if normalized_entry_side == "BUY":
        return normalized_execution_side in {"SELL", "SLD"}
    if normalized_entry_side == "SELL":
        return normalized_execution_side in {"BUY", "BOT"}
    return False


def _positive_decimal(value: str | None) -> Decimal:
    parsed = _to_decimal(value)
    if parsed is None:
        return Decimal("0")
    return abs(parsed)


def _instruction_remaining_exit_quantity(instruction_record: InstructionRecord) -> Decimal:
    entry_quantity = _positive_decimal(instruction_record.entry_filled_quantity)
    exit_quantity = _positive_decimal(instruction_record.exit_filled_quantity)
    remaining = entry_quantity - exit_quantity
    return remaining if remaining > 0 else Decimal("0")


def _find_active_exit_instruction_for_execution(
    session: Session,
    *,
    account_key: str,
    symbol: str | None,
    local_symbol: str | None,
    currency: str | None,
    security_type: str | None,
    execution_side: str | None,
) -> InstructionRecord | None:
    execution_keys = _execution_symbol_keys(symbol=symbol, local_symbol=local_symbol)
    if not execution_keys:
        return None

    statement = (
        select(InstructionRecord)
        .where(
            InstructionRecord.account_key == account_key,
            InstructionRecord.state.in_(("POSITION_OPEN", "EXIT_PENDING")),
            InstructionRecord.archived_at.is_(None),
        )
        .order_by(InstructionRecord.updated_at.desc(), InstructionRecord.id.desc())
    )
    normalized_currency = _normalize_text(currency)
    if normalized_currency is not None:
        statement = statement.where(InstructionRecord.currency == normalized_currency)

    candidates: list[InstructionRecord] = []
    for instruction_record in session.execute(statement).scalars():
        if not _execution_side_is_exit_for_instruction(execution_side, instruction_record):
            continue
        if normalized_currency is not None and instruction_record.currency != normalized_currency:
            continue
        normalized_security_type = _normalize_text(security_type)
        instruction_payload = (
            instruction_record.payload.get("instruction", {})
            if isinstance(instruction_record.payload, dict)
            else {}
        )
        instrument_payload = (
            instruction_payload.get("instrument", {})
            if isinstance(instruction_payload, dict)
            else {}
        )
        instruction_security_type = (
            _normalize_text(instrument_payload.get("security_type"))
            if isinstance(instrument_payload, dict)
            else None
        )
        if (
            normalized_security_type is not None
            and instruction_security_type is not None
            and instruction_security_type != normalized_security_type
        ):
            continue
        if _instruction_symbol_keys(instruction_record).isdisjoint(execution_keys):
            continue
        if _instruction_remaining_exit_quantity(instruction_record) <= 0:
            continue
        candidates.append(instruction_record)

    if len(candidates) == 1:
        return candidates[0]
    return None


def _order_role_for_execution(
    *,
    execution_order_ref: str | None,
    execution_side: str | None,
    instruction_record: InstructionRecord | None,
) -> str:
    if (
        instruction_record is not None
        and _execution_side_is_exit_for_instruction(execution_side, instruction_record)
    ):
        return "EXIT"
    return _infer_order_role(execution_order_ref)


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

    if external_perm_id is not None:
        try:
            perm_id = int(external_perm_id)
        except ValueError:
            perm_id = None
        if perm_id is not None:
            matches = session.execute(
                select(InstructionRecord)
                .where(
                    or_(
                        InstructionRecord.broker_perm_id == perm_id,
                        InstructionRecord.exit_perm_id == perm_id,
                    )
                )
                .order_by(InstructionRecord.id.desc())
            ).scalars().all()
            if len(matches) == 1:
                return matches[0]
            if matches:
                return None

    if external_order_id is not None:
        try:
            order_id = int(external_order_id)
        except ValueError:
            order_id = None
        if order_id is not None:
            matches = session.execute(
                select(InstructionRecord)
                .where(
                    or_(
                        InstructionRecord.broker_order_id == order_id,
                        InstructionRecord.exit_order_id == order_id,
                    )
                )
                .order_by(InstructionRecord.id.desc())
            ).scalars().all()
            if len(matches) == 1:
                return matches[0]

    return None


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
            ).order_by(BrokerOrderRecord.id.desc())
        ).scalars().first()
        if broker_order is not None:
            return broker_order

    if external_perm_id is not None:
        broker_order = session.execute(
            select(BrokerOrderRecord).where(
                BrokerOrderRecord.broker_kind == broker_kind,
                BrokerOrderRecord.account_key == account_key,
                BrokerOrderRecord.external_perm_id == external_perm_id,
            ).order_by(BrokerOrderRecord.id.desc())
        ).scalars().first()
        if broker_order is not None:
            return broker_order

    normalized_ref = _normalize_text(order_ref)
    if (
        normalized_ref is not None
        and external_order_id is None
        and external_perm_id is None
    ):
        broker_order = session.execute(
            select(BrokerOrderRecord).where(
                BrokerOrderRecord.broker_kind == broker_kind,
                BrokerOrderRecord.account_key == account_key,
                BrokerOrderRecord.order_ref == normalized_ref,
            ).order_by(BrokerOrderRecord.id.desc())
        ).scalars().first()
        if broker_order is not None:
            return broker_order

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


def _open_order_status_clause():
    return or_(
        BrokerOrderRecord.status.is_(None),
        func.upper(BrokerOrderRecord.status).not_in(_OPEN_ORDER_CLOSED_STATUSES),
    )


def _is_open_order_status(status: str | None) -> bool:
    normalized = _normalize_text(status)
    if normalized is None:
        return True
    return normalized.upper() not in _OPEN_ORDER_CLOSED_STATUSES


def _snapshot_account_scope(
    snapshot: BrokerRuntimeSnapshot,
    *,
    default_account_key: str | None,
) -> set[str]:
    account_keys: set[str] = set()
    for raw_account_key in snapshot.account_values:
        normalized = _normalize_text(raw_account_key)
        if normalized is not None:
            account_keys.add(normalized)
    for open_order in snapshot.open_orders.values():
        account_keys.add(
            _resolve_account_key(
                open_order.account,
                default_account_key=default_account_key,
                context=f"Open order {open_order.order_id}",
            )
        )
    for execution in snapshot.executions:
        normalized = _normalize_text(execution.account)
        if normalized is not None:
            account_keys.add(normalized)
    for portfolio_item in snapshot.portfolio:
        normalized = _normalize_text(portfolio_item.account)
        if normalized is not None:
            account_keys.add(normalized)
    for position in snapshot.positions:
        normalized = _normalize_text(position.account)
        if normalized is not None:
            account_keys.add(normalized)
    default_account = _normalize_text(default_account_key)
    if default_account is not None:
        account_keys.add(default_account)
    return account_keys


def _mark_missing_open_orders_closed(
    session: Session,
    *,
    broker_kind: str,
    snapshot: BrokerRuntimeSnapshot,
    observed_at: datetime,
    default_account_key: str | None,
    empty_open_orders_authoritative: bool = False,
) -> None:
    account_scope = _snapshot_account_scope(
        snapshot,
        default_account_key=default_account_key,
    )
    if not account_scope:
        return

    observed_external_order_ids = {
        str(open_order.order_id)
        for open_order in snapshot.open_orders.values()
        if open_order.order_id not in (None, "")
    }
    observed_external_perm_ids = {
        str(open_order.perm_id)
        for open_order in snapshot.open_orders.values()
        if open_order.perm_id not in (None, "")
    }
    observed_order_refs = {
        str(open_order.order_ref).strip()
        for open_order in snapshot.open_orders.values()
        if str(open_order.order_ref or "").strip()
    }
    if (
        not observed_external_order_ids
        and not observed_external_perm_ids
        and not observed_order_refs
        and not empty_open_orders_authoritative
    ):
        return

    rows = session.execute(
        select(BrokerOrderRecord)
        .where(
            BrokerOrderRecord.broker_kind == broker_kind,
            BrokerOrderRecord.account_key.in_(account_scope),
            _open_order_status_clause(),
        )
        .order_by(BrokerOrderRecord.id.asc())
    ).scalars()

    for broker_order in rows:
        if (
            broker_order.external_order_id
            and broker_order.external_order_id in observed_external_order_ids
        ):
            continue
        if (
            broker_order.external_perm_id
            and broker_order.external_perm_id in observed_external_perm_ids
        ):
            continue
        if (
            not broker_order.external_order_id
            and not broker_order.external_perm_id
            and broker_order.order_ref
            and broker_order.order_ref in observed_order_refs
        ):
            continue

        status_before = broker_order.status
        if status_before == "NOT_FOUND_AT_BROKER":
            continue
        broker_order.status = "NOT_FOUND_AT_BROKER"
        broker_order.last_status_at = observed_at
        metadata = dict(broker_order.metadata_json or {})
        metadata["missing_from_runtime_snapshot"] = True
        metadata["missing_from_runtime_snapshot_at"] = observed_at.isoformat()
        metadata["missing_from_runtime_snapshot_account_scope"] = sorted(account_scope)
        metadata["missing_from_runtime_snapshot_open_order_count"] = len(
            snapshot.open_orders
        )
        broker_order.metadata_json = _serialize_for_json(metadata)
        _record_broker_order_event(
            session,
            broker_order=broker_order,
            event_type="open_order_missing_from_runtime_snapshot",
            event_at=observed_at,
            status_before=status_before,
            status_after=broker_order.status,
            payload={
                "account_scope": sorted(account_scope),
                "observed_external_order_ids": sorted(observed_external_order_ids),
                "observed_external_perm_ids": sorted(observed_external_perm_ids),
                "observed_order_refs": sorted(observed_order_refs),
            },
            note=(
                "A fresh runtime snapshot did not contain this locally-open "
                "broker order, so it was marked terminal in the local ledger."
            ),
        )
        _sync_instruction_from_broker_order_terminal_status(
            session,
            broker_order=broker_order,
            event_at=observed_at,
            event_source="runtime_snapshot",
            note=(
                "A fresh runtime snapshot no longer contained the unfilled entry "
                "broker order, so the instruction was marked cancelled."
            ),
        )

