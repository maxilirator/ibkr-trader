from __future__ import annotations

from datetime import date
from datetime import datetime
from decimal import Decimal
from decimal import InvalidOperation
from enum import Enum
from typing import Any
from uuid import uuid4

from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import AccountSnapshotRecord
from ibkr_trader.db.models import BrokerAccountRecord
from ibkr_trader.db.models import BrokerOrderEventRecord
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import ExecutionFillRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import PositionSnapshotRecord
from ibkr_trader.db.models import VirtualMarketQuoteRecord
from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.domain.execution_contract import OrderType
from ibkr_trader.virtual.accounts import BROKER_KIND_VIRTUAL
from ibkr_trader.virtual.accounts import VIRTUAL_FIXED_COMMISSION_SEK
from ibkr_trader.virtual.accounts import is_virtual_account_key
from ibkr_trader.virtual.accounts import normalize_virtual_account_key

_VIRTUAL_CLOSED_ORDER_STATUSES = {
    "API_CANCELLED",
    "CANCELLED",
    "ERROR",
    "FILLED",
    "INACTIVE",
    "NOT_FOUND_AT_BROKER",
    "REJECTED",
}
_VIRTUAL_CASH_BALANCE_METADATA_KEY = "virtual_cash_balance_sek"


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


def _normalize_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_order_type(order_type: OrderType | str) -> str:
    if order_type is OrderType.LIMIT or order_type == OrderType.LIMIT:
        return "LMT"
    if order_type is OrderType.MARKET or order_type == OrderType.MARKET:
        return "MKT"
    normalized = str(order_type).strip().upper()
    aliases = {
        "LIMIT": "LMT",
        "MARKET": "MKT",
        "STOP": "STP",
        "STOP_LIMIT": "STP LMT",
    }
    return aliases.get(normalized, normalized)


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid virtual decimal value: {value}") from exc


def _decimal_to_string(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _parse_datetime_value(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    raise ValueError(f"Invalid virtual datetime value: {value}")


def _new_virtual_order_id() -> int:
    return 800_000_000 + (uuid4().int % 1_000_000_000)


def _new_virtual_perm_id(order_id: int) -> int:
    return order_id + 1_000_000_000


def _is_closed_status(status: str | None) -> bool:
    return str(status or "").strip().upper() in _VIRTUAL_CLOSED_ORDER_STATUSES


def ensure_virtual_account(
    session: Session,
    *,
    account_key: str,
    base_currency: str = "SEK",
    account_label: str | None = None,
    cash_balance: Decimal | None = None,
) -> BrokerAccountRecord:
    normalized_account_key = normalize_virtual_account_key(account_key)
    broker_account = session.execute(
        select(BrokerAccountRecord).where(
            BrokerAccountRecord.broker_kind == BROKER_KIND_VIRTUAL,
            BrokerAccountRecord.account_key == normalized_account_key,
        )
    ).scalar_one_or_none()
    if broker_account is None:
        broker_account = BrokerAccountRecord(
            broker_kind=BROKER_KIND_VIRTUAL,
            account_key=normalized_account_key,
            account_label=account_label,
            base_currency=base_currency,
            is_virtual=True,
            metadata_json={
                "virtual_account": True,
                **(
                    {_VIRTUAL_CASH_BALANCE_METADATA_KEY: str(cash_balance)}
                    if cash_balance is not None
                    else {}
                ),
            },
        )
        session.add(broker_account)
        session.flush()
    else:
        broker_account.is_virtual = True
        if broker_account.base_currency is None:
            broker_account.base_currency = base_currency
        if account_label is not None:
            broker_account.account_label = account_label
        metadata = dict(broker_account.metadata_json or {})
        metadata["virtual_account"] = True
        if cash_balance is not None:
            metadata[_VIRTUAL_CASH_BALANCE_METADATA_KEY] = str(cash_balance)
        broker_account.metadata_json = metadata
    if cash_balance is not None and broker_account.metadata_json.get(
        _VIRTUAL_CASH_BALANCE_METADATA_KEY
    ) != str(cash_balance):
        metadata = dict(broker_account.metadata_json or {})
        metadata[_VIRTUAL_CASH_BALANCE_METADATA_KEY] = str(cash_balance)
        broker_account.metadata_json = metadata
    return broker_account


def ensure_virtual_account_record(
    session_factory: sessionmaker[Session],
    *,
    account_key: str,
    base_currency: str = "SEK",
    account_label: str | None = None,
    cash_balance: Decimal | None = None,
    snapshot_at: datetime | None = None,
) -> dict[str, Any]:
    with session_scope(session_factory) as session:
        broker_account = ensure_virtual_account(
            session,
            account_key=account_key,
            base_currency=base_currency,
            account_label=account_label,
            cash_balance=cash_balance,
        )
        snapshot = _persist_virtual_account_snapshot(
            session,
            broker_account=broker_account,
            snapshot_at=snapshot_at or utc_now(),
        )
        session.flush()
        return {
            "account_key": broker_account.account_key,
            "broker_kind": broker_account.broker_kind,
            "account_label": broker_account.account_label,
            "base_currency": broker_account.base_currency,
            "is_virtual": broker_account.is_virtual,
            "cash_balance": broker_account.metadata_json.get(
                _VIRTUAL_CASH_BALANCE_METADATA_KEY
            ),
            "snapshot_id": snapshot.id,
        }


def _latest_virtual_quote(
    session: Session,
    *,
    account_key: str,
    symbol: str,
    currency: str,
    security_type: str,
) -> VirtualMarketQuoteRecord | None:
    normalized_account_key = normalize_virtual_account_key(account_key)
    return session.execute(
        select(VirtualMarketQuoteRecord)
        .where(
            VirtualMarketQuoteRecord.account_key == normalized_account_key,
            VirtualMarketQuoteRecord.symbol == symbol.strip().upper(),
            VirtualMarketQuoteRecord.currency == currency.strip().upper(),
            VirtualMarketQuoteRecord.security_type == security_type.strip().upper(),
        )
        .order_by(
            VirtualMarketQuoteRecord.observed_at.desc(),
            VirtualMarketQuoteRecord.id.desc(),
        )
        .limit(1)
    ).scalar_one_or_none()


def serialize_virtual_quote(quote: VirtualMarketQuoteRecord) -> dict[str, Any]:
    return _serialize_for_json(
        {
            "quote_id": quote.id,
            "account_key": quote.account_key,
            "observed_at": quote.observed_at,
            "symbol": quote.symbol,
            "exchange": quote.exchange,
            "currency": quote.currency,
            "security_type": quote.security_type,
            "primary_exchange": quote.primary_exchange,
            "local_symbol": quote.local_symbol,
            "bid_price": quote.bid_price,
            "ask_price": quote.ask_price,
            "last_price": quote.last_price,
            "midpoint_price": quote.midpoint_price,
            "source": quote.source,
            "metadata": quote.metadata_json,
        }
    )


def _quote_price_for_action(
    quote: VirtualMarketQuoteRecord,
    *,
    action: str,
) -> Decimal | None:
    bid = _to_decimal(quote.bid_price)
    ask = _to_decimal(quote.ask_price)
    last = _to_decimal(quote.last_price)
    midpoint = _to_decimal(quote.midpoint_price)
    if midpoint is None and bid is not None and ask is not None:
        midpoint = (bid + ask) / Decimal("2")

    normalized_action = action.strip().upper()
    candidates = (
        (ask, last, midpoint, bid)
        if normalized_action == "BUY"
        else (bid, last, midpoint, ask)
    )
    for candidate in candidates:
        if candidate is not None and candidate > 0:
            return candidate
    return None


def _virtual_price_condition(
    *,
    action: str,
    order_type: str,
    market_price: Decimal | None,
    limit_price: Decimal | None,
    stop_price: Decimal | None,
) -> tuple[bool, str]:
    if market_price is None:
        return False, "NO_MARKET_PRICE"

    normalized_action = action.strip().upper()
    normalized_order_type = order_type.strip().upper()
    if normalized_order_type == "MKT":
        return True, "MARKET_ORDER"
    if normalized_order_type == "LMT":
        if limit_price is None:
            return False, "LIMIT_PRICE_MISSING"
        if normalized_action == "BUY":
            return market_price <= limit_price, "BUY_LIMIT_MET"
        return market_price >= limit_price, "SELL_LIMIT_MET"
    if normalized_order_type.startswith("STP"):
        if stop_price is None:
            return False, "STOP_PRICE_MISSING"
        if normalized_action == "BUY":
            return market_price >= stop_price, "BUY_STOP_MET"
        return market_price <= stop_price, "SELL_STOP_MET"
    return False, f"UNSUPPORTED_ORDER_TYPE:{normalized_order_type}"


def _build_resolved_contract(instruction: ExecutionInstruction) -> dict[str, Any]:
    return {
        "con_id": None,
        "symbol": instruction.instrument.symbol,
        "local_symbol": instruction.instrument.symbol,
        "security_type": instruction.instrument.security_type.value,
        "exchange": instruction.instrument.exchange,
        "primary_exchange": instruction.instrument.primary_exchange,
        "currency": instruction.instrument.currency,
        "virtual_contract": True,
    }


def _build_virtual_fill_payload(
    *,
    order_id: int,
    perm_id: int,
    order_ref: str,
    action: str,
    price: Decimal,
    quote: VirtualMarketQuoteRecord,
    condition_code: str,
) -> dict[str, Any]:
    side = "BOT" if action.strip().upper() == "BUY" else "SLD"
    return _serialize_for_json(
        {
            "external_execution_id": f"virtual-{order_id}-{quote.id}",
            "external_order_id": str(order_id),
            "external_perm_id": str(perm_id),
            "order_ref": order_ref,
            "side": side,
            "quantity": "1",
            "price": price,
            "commission": VIRTUAL_FIXED_COMMISSION_SEK,
            "commission_currency": "SEK",
            "executed_at": quote.observed_at,
            "condition_code": condition_code,
            "market_quote": serialize_virtual_quote(quote),
        }
    )


def _build_virtual_order_submission(
    *,
    instruction: ExecutionInstruction,
    account_key: str,
    action: str,
    order_ref: str,
    order_type: str,
    time_in_force: str,
    limit_price: Decimal | None,
    stop_price: Decimal | None,
    quote: VirtualMarketQuoteRecord | None,
    oca_group: str | None = None,
    oca_type: int | None = None,
) -> dict[str, Any]:
    order_id = _new_virtual_order_id()
    perm_id = _new_virtual_perm_id(order_id)
    market_price = (
        _quote_price_for_action(quote, action=action)
        if quote is not None
        else None
    )
    price_met, condition_code = _virtual_price_condition(
        action=action,
        order_type=order_type,
        market_price=market_price,
        limit_price=limit_price,
        stop_price=stop_price,
    )
    status = "FILLED" if price_met else "Submitted"
    fill_payload = (
        _build_virtual_fill_payload(
            order_id=order_id,
            perm_id=perm_id,
            order_ref=order_ref,
            action=action,
            price=market_price,
            quote=quote,
            condition_code=condition_code,
        )
        if price_met and market_price is not None and quote is not None
        else None
    )
    return _serialize_for_json(
        {
            "broker_kind": BROKER_KIND_VIRTUAL,
            "instruction_id": instruction.instruction_id,
            "account": account_key,
            "is_virtual": True,
            "warnings": [
                "Virtual execution ignores requested quantity and uses quantity=1."
            ],
            "resolved_contract": _build_resolved_contract(instruction),
            "order": {
                "order_ref": order_ref,
                "action": action,
                "order_type": order_type,
                "time_in_force": time_in_force,
                "limit_price": _decimal_to_string(limit_price),
                "stop_price": _decimal_to_string(stop_price),
                "total_quantity": "1",
                "outside_rth": False,
                "oca_group": oca_group,
                "oca_type": oca_type,
                "transmit": False,
                "is_virtual": True,
            },
            "broker_order_status": {
                "orderId": order_id,
                "status": status,
                "filled": "1" if price_met else "0",
                "remaining": "0" if price_met else "1",
                "avgFillPrice": _decimal_to_string(market_price) if price_met else "0",
                "permId": perm_id,
                "parentId": 0,
                "lastFillPrice": _decimal_to_string(market_price) if price_met else "0",
                "clientId": 0,
                "whyHeld": "",
                "mktCapPrice": "0",
            },
            "virtual_execution": {
                "price_met": price_met,
                "condition_code": condition_code,
                "quantity_disregarded": True,
                "fixed_commission": str(VIRTUAL_FIXED_COMMISSION_SEK),
                "fixed_commission_currency": "SEK",
                "market_price": _decimal_to_string(market_price),
                "market_quote": serialize_virtual_quote(quote) if quote is not None else None,
                "fill": fill_payload,
            },
            "tws_submission": None,
        }
    )


def submit_virtual_entry_order(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    instruction: ExecutionInstruction,
    *,
    timeout: int = 10,
) -> dict[str, Any]:
    del broker_config, timeout
    account_key = normalize_virtual_account_key(instruction.account.account_key)
    with session_scope(session_factory) as session:
        ensure_virtual_account(
            session,
            account_key=account_key,
            base_currency="SEK",
        )
        quote = _latest_virtual_quote(
            session,
            account_key=account_key,
            symbol=instruction.instrument.symbol,
            currency=instruction.instrument.currency,
            security_type=instruction.instrument.security_type.value,
        )
        return _build_virtual_order_submission(
            instruction=instruction,
            account_key=account_key,
            action=instruction.intent.side,
            order_ref=instruction.instruction_id,
            order_type=_normalize_order_type(instruction.entry.order_type),
            time_in_force=instruction.entry.time_in_force.value,
            limit_price=instruction.entry.limit_price,
            stop_price=None,
            quote=quote,
        )


def submit_virtual_exit_order(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    instruction: ExecutionInstruction,
    *,
    quantity: Decimal,
    order_type: OrderType | str,
    order_ref: str,
    timeout: int = 10,
    limit_price: Decimal | None = None,
    stop_price: Decimal | None = None,
    oca_group: str | None = None,
    oca_type: int | None = None,
) -> dict[str, Any]:
    del broker_config, quantity, timeout
    account_key = normalize_virtual_account_key(instruction.account.account_key)
    action = "SELL" if instruction.intent.side == "BUY" else "BUY"
    with session_scope(session_factory) as session:
        ensure_virtual_account(
            session,
            account_key=account_key,
            base_currency="SEK",
        )
        quote = _latest_virtual_quote(
            session,
            account_key=account_key,
            symbol=instruction.instrument.symbol,
            currency=instruction.instrument.currency,
            security_type=instruction.instrument.security_type.value,
        )
        return _build_virtual_order_submission(
            instruction=instruction,
            account_key=account_key,
            action=action,
            order_ref=order_ref,
            order_type=_normalize_order_type(order_type),
            time_in_force=instruction.entry.time_in_force.value,
            limit_price=limit_price,
            stop_price=stop_price,
            quote=quote,
            oca_group=oca_group,
            oca_type=oca_type,
        )


def _latest_position_quantity(
    session: Session,
    *,
    broker_account_id: int,
    symbol: str,
    currency: str,
    security_type: str,
) -> Decimal:
    row = session.execute(
        select(PositionSnapshotRecord)
        .where(
            PositionSnapshotRecord.broker_account_id == broker_account_id,
            PositionSnapshotRecord.symbol == symbol,
            PositionSnapshotRecord.currency == currency,
            PositionSnapshotRecord.security_type == security_type,
        )
        .order_by(
            PositionSnapshotRecord.snapshot_at.desc(),
            PositionSnapshotRecord.id.desc(),
        )
        .limit(1)
    ).scalar_one_or_none()
    if row is None or row.quantity in (None, ""):
        return Decimal("0")
    return _to_decimal(row.quantity) or Decimal("0")


def _sum_virtual_commissions(
    session: Session,
    *,
    broker_account_id: int,
    account_key: str,
) -> Decimal:
    fills = session.execute(
        select(ExecutionFillRecord).where(
            ExecutionFillRecord.broker_account_id == broker_account_id,
            ExecutionFillRecord.account_key == account_key,
            ExecutionFillRecord.is_virtual.is_(True),
        )
    ).scalars()
    total = Decimal("0")
    for fill in fills:
        commission = _to_decimal(fill.commission)
        if commission is not None:
            total += commission
    return total


def _virtual_cash_balance(broker_account: BrokerAccountRecord) -> Decimal:
    metadata = broker_account.metadata_json or {}
    raw_value = metadata.get(_VIRTUAL_CASH_BALANCE_METADATA_KEY)
    return _to_decimal(raw_value) or Decimal("0")


def _persist_virtual_account_snapshot(
    session: Session,
    *,
    broker_account: BrokerAccountRecord,
    snapshot_at: datetime,
) -> AccountSnapshotRecord:
    total_commissions = _sum_virtual_commissions(
        session,
        broker_account_id=broker_account.id,
        account_key=broker_account.account_key,
    )
    cash_balance = _virtual_cash_balance(broker_account)
    cash_value = cash_balance - total_commissions
    snapshot = AccountSnapshotRecord(
        broker_account_id=broker_account.id,
        is_virtual=True,
        snapshot_at=snapshot_at,
        source="virtual_execution",
        net_liquidation=str(cash_value),
        total_cash_value=str(cash_value),
        buying_power=str(cash_value),
        available_funds=str(cash_value),
        excess_liquidity=str(cash_value),
        cushion="1" if cash_value >= 0 else "0",
        currency="SEK",
        raw_payload={
            "virtual_account": True,
            "cash_balance_sek": str(cash_balance),
            "total_commissions_sek": str(total_commissions),
        },
    )
    session.add(snapshot)
    return snapshot


def _persist_virtual_position_snapshot(
    session: Session,
    *,
    broker_account: BrokerAccountRecord,
    broker_order: BrokerOrderRecord,
    fill_payload: dict[str, Any],
) -> None:
    fill_quantity = _to_decimal(fill_payload.get("quantity")) or Decimal("1")
    fill_price = _to_decimal(fill_payload.get("price")) or Decimal("0")
    current_quantity = _latest_position_quantity(
        session,
        broker_account_id=broker_account.id,
        symbol=broker_order.symbol,
        currency=broker_order.currency,
        security_type=broker_order.security_type,
    )
    delta = fill_quantity if broker_order.side == "BUY" else -fill_quantity
    new_quantity = current_quantity + delta
    market_value = new_quantity * fill_price
    snapshot = PositionSnapshotRecord(
        broker_account_id=broker_account.id,
        is_virtual=True,
        snapshot_at=_parse_datetime_value(fill_payload["executed_at"]),
        source="virtual_execution",
        symbol=broker_order.symbol,
        exchange=broker_order.exchange,
        currency=broker_order.currency,
        security_type=broker_order.security_type,
        primary_exchange=broker_order.primary_exchange,
        local_symbol=broker_order.local_symbol,
        quantity=str(new_quantity),
        average_cost=str(fill_price) if new_quantity != 0 else None,
        market_price=str(fill_price),
        market_value=str(market_value),
        unrealized_pnl=None,
        realized_pnl=None,
        raw_payload=_serialize_for_json({
            "virtual_execution": fill_payload,
            "previous_quantity": str(current_quantity),
            "delta_quantity": str(delta),
        }),
    )
    session.add(snapshot)


def persist_virtual_execution_fill(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
    instruction_record: InstructionRecord | None,
    fill_payload: dict[str, Any],
    observed_at: datetime,
    event_type: str = "virtual_execution_fill",
    note: str = "Virtual order filled from virtual market-watch price.",
) -> ExecutionFillRecord | None:
    external_execution_id = str(fill_payload["external_execution_id"])
    executed_at = _parse_datetime_value(fill_payload["executed_at"])
    normalized_fill_payload = {**fill_payload, "executed_at": executed_at}
    existing = session.execute(
        select(ExecutionFillRecord).where(
            ExecutionFillRecord.broker_kind == BROKER_KIND_VIRTUAL,
            ExecutionFillRecord.account_key == broker_order.account_key,
            ExecutionFillRecord.external_execution_id == external_execution_id,
        )
    ).scalar_one_or_none()
    if existing is not None:
        return None

    broker_account = broker_order.broker_account
    broker_account.is_virtual = True
    broker_order.is_virtual = True
    previous_status = broker_order.status
    broker_order.status = "FILLED"
    broker_order.last_status_at = observed_at
    metadata = dict(broker_order.metadata_json or {})
    metadata["virtual_execution"] = fill_payload
    metadata["last_order_status_callback"] = {
        "orderId": broker_order.external_order_id,
        "status": "Filled",
        "filled": fill_payload.get("quantity"),
        "remaining": "0",
        "avgFillPrice": fill_payload.get("price"),
        "lastFillPrice": fill_payload.get("price"),
    }
    broker_order.metadata_json = _serialize_for_json(metadata)

    fill = ExecutionFillRecord(
        broker_order_id=broker_order.id,
        instruction_id=instruction_record.id if instruction_record is not None else None,
        broker_account_id=broker_account.id,
        broker_kind=BROKER_KIND_VIRTUAL,
        account_key=broker_order.account_key,
        is_virtual=True,
        external_execution_id=external_execution_id,
        external_order_id=broker_order.external_order_id,
        external_perm_id=broker_order.external_perm_id,
        order_ref=broker_order.order_ref,
        symbol=broker_order.symbol,
        exchange=broker_order.exchange,
        currency=broker_order.currency,
        security_type=broker_order.security_type,
        side=fill_payload.get("side"),
        quantity=str(fill_payload["quantity"]),
        price=str(fill_payload["price"]),
        commission=str(fill_payload["commission"]),
        commission_currency=str(fill_payload["commission_currency"]),
        executed_at=executed_at,
        raw_payload=_serialize_for_json(normalized_fill_payload),
    )
    session.add(fill)
    session.flush()

    session.add(
        BrokerOrderEventRecord(
            broker_order_id=broker_order.id,
            event_type=event_type,
            event_at=observed_at,
            status_before=previous_status,
            status_after="FILLED",
            payload=_serialize_for_json(normalized_fill_payload),
            note=note,
        )
    )
    _persist_virtual_position_snapshot(
        session,
        broker_account=broker_account,
        broker_order=broker_order,
        fill_payload=normalized_fill_payload,
    )
    _persist_virtual_account_snapshot(
        session,
        broker_account=broker_account,
        snapshot_at=executed_at or observed_at,
    )
    return fill


def persist_virtual_execution_from_submission(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
    instruction_record: InstructionRecord | None,
    broker_submission: dict[str, Any],
    observed_at: datetime,
) -> ExecutionFillRecord | None:
    virtual_execution = broker_submission.get("virtual_execution")
    if not isinstance(virtual_execution, dict):
        return None
    fill_payload = virtual_execution.get("fill")
    if not isinstance(fill_payload, dict):
        return None
    return persist_virtual_execution_fill(
        session,
        broker_order=broker_order,
        instruction_record=instruction_record,
        fill_payload=fill_payload,
        observed_at=observed_at,
    )


def _quote_matches_order(
    quote: VirtualMarketQuoteRecord,
    broker_order: BrokerOrderRecord,
) -> bool:
    if quote.account_key != broker_order.account_key:
        return False
    if quote.symbol != broker_order.symbol:
        return False
    if quote.currency != broker_order.currency:
        return False
    if quote.security_type != broker_order.security_type:
        return False
    return True


def _build_fill_from_quote_for_order(
    *,
    quote: VirtualMarketQuoteRecord,
    broker_order: BrokerOrderRecord,
) -> dict[str, Any] | None:
    price = _quote_price_for_action(quote, action=broker_order.side)
    limit_price = _to_decimal(broker_order.limit_price)
    stop_price = _to_decimal(broker_order.stop_price)
    price_met, condition_code = _virtual_price_condition(
        action=broker_order.side,
        order_type=broker_order.order_type,
        market_price=price,
        limit_price=limit_price,
        stop_price=stop_price,
    )
    if not price_met or price is None:
        return None
    order_id = int(str(broker_order.external_order_id))
    perm_id = (
        int(str(broker_order.external_perm_id))
        if broker_order.external_perm_id not in (None, "")
        else _new_virtual_perm_id(order_id)
    )
    return _build_virtual_fill_payload(
        order_id=order_id,
        perm_id=perm_id,
        order_ref=broker_order.order_ref or str(order_id),
        action=broker_order.side,
        price=price,
        quote=quote,
        condition_code=condition_code,
    )


def process_virtual_quote_fills(
    session: Session,
    *,
    quote: VirtualMarketQuoteRecord,
) -> list[dict[str, Any]]:
    broker_orders = session.execute(
        select(BrokerOrderRecord)
        .where(
            BrokerOrderRecord.broker_kind == BROKER_KIND_VIRTUAL,
            BrokerOrderRecord.is_virtual.is_(True),
            BrokerOrderRecord.account_key == quote.account_key,
            BrokerOrderRecord.symbol == quote.symbol,
            BrokerOrderRecord.currency == quote.currency,
            BrokerOrderRecord.security_type == quote.security_type,
            or_(
                BrokerOrderRecord.status.is_(None),
                BrokerOrderRecord.status.not_in(_VIRTUAL_CLOSED_ORDER_STATUSES),
            ),
        )
        .order_by(BrokerOrderRecord.submitted_at.asc(), BrokerOrderRecord.id.asc())
    ).scalars().all()

    filled_orders: list[dict[str, Any]] = []
    for broker_order in broker_orders:
        if _is_closed_status(broker_order.status) or not _quote_matches_order(
            quote,
            broker_order,
        ):
            continue
        fill_payload = _build_fill_from_quote_for_order(
            quote=quote,
            broker_order=broker_order,
        )
        if fill_payload is None:
            continue
        fill = persist_virtual_execution_fill(
            session,
            broker_order=broker_order,
            instruction_record=broker_order.instruction,
            fill_payload=fill_payload,
            observed_at=quote.observed_at,
        )
        if fill is None:
            continue
        filled_orders.append(
            _serialize_for_json(
                {
                    "broker_order_id": broker_order.id,
                    "external_order_id": broker_order.external_order_id,
                    "order_ref": broker_order.order_ref,
                    "symbol": broker_order.symbol,
                    "side": broker_order.side,
                    "order_type": broker_order.order_type,
                    "status": broker_order.status,
                    "fill_id": fill.id,
                    "execution_id": fill.external_execution_id,
                    "price": fill.price,
                    "commission": fill.commission,
                    "commission_currency": fill.commission_currency,
                }
            )
        )
    return filled_orders


def record_virtual_market_quote(
    session_factory: sessionmaker[Session],
    *,
    account_key: str,
    symbol: str,
    exchange: str,
    currency: str,
    security_type: str = "STK",
    observed_at: datetime | None = None,
    primary_exchange: str | None = None,
    local_symbol: str | None = None,
    bid_price: Decimal | None = None,
    ask_price: Decimal | None = None,
    last_price: Decimal | None = None,
    midpoint_price: Decimal | None = None,
    source: str | None = None,
    raw_payload: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_account_key = normalize_virtual_account_key(account_key)
    quote_observed_at = observed_at or utc_now()
    with session_scope(session_factory) as session:
        broker_account = ensure_virtual_account(
            session,
            account_key=normalized_account_key,
            base_currency="SEK",
        )
        quote = VirtualMarketQuoteRecord(
            account_key=normalized_account_key,
            observed_at=quote_observed_at,
            symbol=symbol.strip().upper(),
            exchange=exchange.strip().upper(),
            currency=currency.strip().upper(),
            security_type=security_type.strip().upper(),
            primary_exchange=primary_exchange.strip().upper()
            if primary_exchange
            else None,
            local_symbol=local_symbol.strip() if local_symbol else None,
            bid_price=_decimal_to_string(bid_price),
            ask_price=_decimal_to_string(ask_price),
            last_price=_decimal_to_string(last_price),
            midpoint_price=_decimal_to_string(midpoint_price),
            source=source.strip() if source else None,
            raw_payload=_serialize_for_json(raw_payload or {}),
            metadata_json=_serialize_for_json(metadata or {}),
        )
        session.add(quote)
        session.flush()
        _persist_virtual_account_snapshot(
            session,
            broker_account=broker_account,
            snapshot_at=quote_observed_at,
        )
        filled_orders = process_virtual_quote_fills(session, quote=quote)
        return {
            "quote": serialize_virtual_quote(quote),
            "filled_order_count": len(filled_orders),
            "filled_orders": filled_orders,
        }


def list_virtual_market_quotes(
    session_factory: sessionmaker[Session],
    *,
    account_key: str | None = None,
    limit: int = 100,
) -> tuple[dict[str, Any], ...]:
    with session_scope(session_factory) as session:
        statement = select(VirtualMarketQuoteRecord).order_by(
            VirtualMarketQuoteRecord.observed_at.desc(),
            VirtualMarketQuoteRecord.id.desc(),
        )
        if account_key is not None:
            statement = statement.where(
                VirtualMarketQuoteRecord.account_key == normalize_virtual_account_key(
                    account_key
                )
            )
        rows = session.execute(statement.limit(limit)).scalars().all()
        return tuple(serialize_virtual_quote(row) for row in rows)


def read_virtual_market_price(
    session_factory: sessionmaker[Session],
    instruction: ExecutionInstruction,
) -> dict[str, Any]:
    account_key = normalize_virtual_account_key(instruction.account.account_key)
    with session_scope(session_factory) as session:
        quote = _latest_virtual_quote(
            session,
            account_key=account_key,
            symbol=instruction.instrument.symbol,
            currency=instruction.instrument.currency,
            security_type=instruction.instrument.security_type.value,
        )
        if quote is None:
            raise LookupError(
                f"No virtual market-watch quote is available for {account_key} "
                f"{instruction.instrument.symbol}.{instruction.instrument.currency}."
            )
        action = "BUY" if instruction.intent.side == "SELL" else "SELL"
        price = _quote_price_for_action(quote, action=action)
        if price is None:
            raise LookupError(
                f"Virtual market-watch quote for {instruction.instrument.symbol} "
                "does not contain a usable price."
            )
        return {
            "price": str(price),
            "observed_at": quote.observed_at.isoformat(),
            "source": "virtual_market_watch",
            "quote": serialize_virtual_quote(quote),
        }


def cancel_virtual_order(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    order_id: int,
    *,
    timeout: int = 10,
) -> dict[str, Any]:
    del broker_config, timeout
    with session_scope(session_factory) as session:
        broker_order = session.execute(
            select(BrokerOrderRecord).where(
                BrokerOrderRecord.broker_kind == BROKER_KIND_VIRTUAL,
                BrokerOrderRecord.external_order_id == str(order_id),
            )
        ).scalar_one_or_none()
        if broker_order is None:
            return {
                "broker_kind": BROKER_KIND_VIRTUAL,
                "is_virtual": True,
                "broker_order_status": {
                    "orderId": order_id,
                    "status": "NOT_FOUND_AT_BROKER",
                },
                "warning": "Virtual order was already absent at cancel time.",
            }
        if _is_closed_status(broker_order.status):
            status = broker_order.status
        else:
            previous_status = broker_order.status
            status = "Cancelled"
            broker_order.status = status
            broker_order.last_status_at = utc_now()
            session.add(
                BrokerOrderEventRecord(
                    broker_order_id=broker_order.id,
                    event_type="virtual_order_cancelled",
                    event_at=broker_order.last_status_at,
                    status_before=previous_status,
                    status_after=status,
                    payload={"order_id": order_id, "is_virtual": True},
                    note="Virtual order cancelled without contacting IBKR.",
                )
            )
        return {
            "broker_kind": BROKER_KIND_VIRTUAL,
            "is_virtual": True,
            "account": broker_order.account_key,
            "broker_order_status": {
                "orderId": order_id,
                "status": status,
                "filled": "0",
                "remaining": broker_order.total_quantity or "1",
                "avgFillPrice": "0",
                "permId": (
                    int(broker_order.external_perm_id)
                    if broker_order.external_perm_id not in (None, "")
                    else None
                ),
                "parentId": 0,
                "lastFillPrice": "0",
                "clientId": (
                    int(broker_order.external_client_id)
                    if broker_order.external_client_id not in (None, "")
                    else 0
                ),
                "whyHeld": "",
                "mktCapPrice": "0",
            },
        }


def has_real_broker_work(
    session_factory: sessionmaker[Session],
    *,
    instruction_ids: tuple[str, ...] | None = None,
) -> bool:
    with session_scope(session_factory) as session:
        active_instruction = select(InstructionRecord.id).where(
            InstructionRecord.state.in_(
                ("ENTRY_SUBMITTED", "POSITION_OPEN", "EXIT_PENDING")
            ),
            InstructionRecord.is_virtual.is_(False),
        )
        if instruction_ids:
            active_instruction = active_instruction.where(
                InstructionRecord.instruction_id.in_(instruction_ids)
            )
        if session.execute(active_instruction.limit(1)).first() is not None:
            return True

        unsettled_order = select(BrokerOrderRecord.id).where(
            BrokerOrderRecord.is_virtual.is_(False),
            or_(
                BrokerOrderRecord.status.is_(None),
                BrokerOrderRecord.status.not_in(_VIRTUAL_CLOSED_ORDER_STATUSES),
            ),
        )
        return session.execute(unsettled_order.limit(1)).first() is not None


def is_virtual_instruction(instruction: ExecutionInstruction) -> bool:
    return is_virtual_account_key(instruction.account.account_key)
