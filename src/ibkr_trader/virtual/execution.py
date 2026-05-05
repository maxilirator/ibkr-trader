from __future__ import annotations

from datetime import date
from datetime import datetime
from decimal import Decimal
from decimal import InvalidOperation
from decimal import ROUND_DOWN
from enum import Enum
from typing import Any
from uuid import uuid4

from sqlalchemy import func
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
from ibkr_trader.domain.execution_contract import FundingBasis
from ibkr_trader.domain.execution_contract import OrderType
from ibkr_trader.domain.execution_contract import SizingMode
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
_VIRTUAL_ORDER_ID_BASE = 800_000_000
_VIRTUAL_ORDER_ID_SPAN = 900_000_000
_VIRTUAL_PERM_ID_OFFSET = 100_000_000
_MAX_INT32 = 2_147_483_647
_STREAM_VIRTUAL_QUOTE_SOURCE = "ibkr_live_market_stream_virtual_bridge"
_TRAINING_LIMIT_FILL_PRICE_POLICY = "training_limit_price"


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


def _parse_optional_datetime_value(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return _parse_datetime_value(value)
    except (TypeError, ValueError):
        return None


def _optional_decimal(value: Any) -> Decimal | None:
    try:
        return _to_decimal(value)
    except ValueError:
        return None


def _new_virtual_order_id() -> int:
    return _VIRTUAL_ORDER_ID_BASE + (uuid4().int % _VIRTUAL_ORDER_ID_SPAN)


def _new_virtual_perm_id(order_id: int) -> int:
    perm_id = order_id + _VIRTUAL_PERM_ID_OFFSET
    if perm_id > _MAX_INT32:
        raise ValueError(f"Virtual permId exceeded int32 range: {perm_id}")
    return perm_id


def _is_closed_status(status: str | None) -> bool:
    return str(status or "").strip().upper() in _VIRTUAL_CLOSED_ORDER_STATUSES


def _open_virtual_order_status_clause():
    return or_(
        BrokerOrderRecord.status.is_(None),
        func.upper(BrokerOrderRecord.status).not_in(_VIRTUAL_CLOSED_ORDER_STATUSES),
    )


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


def _quote_fill_price_policy(quote: VirtualMarketQuoteRecord | None) -> str | None:
    if quote is None:
        return None
    metadata = quote.metadata_json or {}
    raw_payload = quote.raw_payload or {}
    raw_policy = metadata.get("fill_price_policy") or raw_payload.get("fill_price_policy")
    if raw_policy in (None, ""):
        return None
    return str(raw_policy).strip().lower()


def _uses_training_limit_fill_price(quote: VirtualMarketQuoteRecord | None) -> bool:
    return _quote_fill_price_policy(quote) == _TRAINING_LIMIT_FILL_PRICE_POLICY


def _stream_bar_range_from_quote(
    quote: VirtualMarketQuoteRecord,
) -> tuple[Decimal | None, Decimal | None]:
    raw_payload = quote.raw_payload or {}
    latest_bar = raw_payload.get("latest_stream_bar")
    if not isinstance(latest_bar, dict):
        return None, None
    low_price = _optional_decimal(latest_bar.get("low"))
    high_price = _optional_decimal(latest_bar.get("high"))
    return low_price, high_price


def _virtual_condition_price_for_order(
    quote: VirtualMarketQuoteRecord,
    *,
    action: str,
    order_type: str,
) -> tuple[Decimal | None, str | None]:
    quote_price = _quote_price_for_action(quote, action=action)
    low_price, high_price = _stream_bar_range_from_quote(quote)
    normalized_action = action.strip().upper()
    normalized_order_type = order_type.strip().upper()

    if normalized_order_type == "LMT":
        if normalized_action == "BUY":
            candidates = [
                (price, source)
                for price, source in (
                    (quote_price, "QUOTE"),
                    (low_price, "STREAM_BAR_LOW"),
                )
                if price is not None
            ]
            if not candidates:
                return None, None
            return min(candidates, key=lambda item: item[0])
        candidates = [
            (price, source)
            for price, source in (
                (quote_price, "QUOTE"),
                (high_price, "STREAM_BAR_HIGH"),
            )
            if price is not None
        ]
        if not candidates:
            return None, None
        return max(candidates, key=lambda item: item[0])

    if normalized_order_type.startswith("STP"):
        if normalized_action == "BUY":
            candidates = [
                (price, source)
                for price, source in (
                    (quote_price, "QUOTE"),
                    (high_price, "STREAM_BAR_HIGH"),
                )
                if price is not None
            ]
            if not candidates:
                return None, None
            return max(candidates, key=lambda item: item[0])
        candidates = [
            (price, source)
            for price, source in (
                (quote_price, "QUOTE"),
                (low_price, "STREAM_BAR_LOW"),
            )
            if price is not None
        ]
        if not candidates:
            return None, None
        return min(candidates, key=lambda item: item[0])

    return quote_price, "QUOTE" if quote_price is not None else None


def _virtual_execution_price_for_order(
    *,
    quote: VirtualMarketQuoteRecord | None,
    action: str,
    order_type: str,
    condition_price: Decimal | None,
    condition_source: str | None,
    limit_price: Decimal | None,
    stop_price: Decimal | None,
) -> Decimal | None:
    if condition_price is None:
        return None
    normalized_action = action.strip().upper()
    normalized_order_type = order_type.strip().upper()
    if normalized_order_type == "LMT" and limit_price is not None:
        if condition_source in {"STREAM_BAR_LOW", "STREAM_BAR_HIGH"} and (
            quote is None or _uses_training_limit_fill_price(quote)
        ):
            return limit_price
        quote_price = _quote_price_for_action(quote, action=action) if quote else None
        reference_price = quote_price or condition_price
        if normalized_action == "BUY":
            return min(reference_price, limit_price)
        return max(reference_price, limit_price)
    if normalized_order_type.startswith("STP") and stop_price is not None:
        if condition_source in {"STREAM_BAR_LOW", "STREAM_BAR_HIGH"} and (
            quote is None or _uses_training_limit_fill_price(quote)
        ):
            return stop_price
        quote_price = _quote_price_for_action(quote, action=action) if quote else None
        if quote_price is not None:
            return quote_price
    return condition_price


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
    quantity: Decimal,
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
            "quantity": quantity,
            "price": price,
            "commission": VIRTUAL_FIXED_COMMISSION_SEK,
            "commission_currency": "SEK",
            "executed_at": quote.observed_at,
            "condition_code": condition_code,
            "market_quote": serialize_virtual_quote(quote),
        }
    )


def _normalize_virtual_stock_quantity(
    quantity: Decimal,
    *,
    allow_round_down: bool,
) -> tuple[Decimal, list[str]]:
    if quantity <= 0:
        raise ValueError("Virtual order quantity must be positive.")
    whole_quantity = quantity.to_integral_value(rounding=ROUND_DOWN)
    if whole_quantity <= 0:
        raise ValueError("Virtual order quantity rounds below one share.")
    if quantity == whole_quantity:
        return whole_quantity, []
    if not allow_round_down:
        raise ValueError("Virtual target_quantity must be a whole-share value.")
    return whole_quantity, [
        f"Virtual stock quantity rounded down from {quantity} to {whole_quantity}."
    ]


def _virtual_sizing_price(
    *,
    order_type: str,
    limit_price: Decimal | None,
    stop_price: Decimal | None,
    market_price: Decimal | None,
) -> Decimal | None:
    normalized_order_type = order_type.strip().upper()
    if normalized_order_type == "LMT" and limit_price is not None:
        return limit_price
    if normalized_order_type.startswith("STP") and stop_price is not None:
        return stop_price
    return market_price


def _virtual_order_quantity(
    *,
    instruction: ExecutionInstruction,
    explicit_quantity: Decimal | None,
    order_type: str,
    limit_price: Decimal | None,
    stop_price: Decimal | None,
    market_price: Decimal | None,
    account_cash_balance: Decimal,
) -> tuple[Decimal, list[str], dict[str, Any]]:
    if explicit_quantity is not None:
        quantity, warnings = _normalize_virtual_stock_quantity(
            explicit_quantity,
            allow_round_down=False,
        )
        return quantity, warnings, {
            "mode": "explicit_exit_quantity",
            "estimated_quantity": str(explicit_quantity),
            "normalized_quantity": str(quantity),
        }

    sizing = instruction.sizing
    sizing_price = _virtual_sizing_price(
        order_type=order_type,
        limit_price=limit_price,
        stop_price=stop_price,
        market_price=market_price,
    )
    target_notional = None
    estimated_quantity = None
    warnings: list[str] = []

    if sizing.mode is SizingMode.TARGET_QUANTITY:
        estimated_quantity = sizing.target_quantity
        allow_round_down = False
    else:
        if sizing.mode is SizingMode.TARGET_NOTIONAL:
            target_notional = sizing.target_notional
        elif sizing.mode is SizingMode.FRACTION_OF_ACCOUNT_NAV:
            funding_basis = sizing.funding_basis or FundingBasis.CASH
            if (
                funding_basis is FundingBasis.ACCOUNT_NAV
                and instruction.intent.position_side.value == "LONG"
                and not sizing.allow_leverage
            ):
                raise ValueError(
                    "Virtual long account_nav sizing requires sizing.allow_leverage=true."
                )
            if account_cash_balance <= 0:
                raise ValueError("Virtual account has no positive cash balance for sizing.")
            if sizing.target_fraction_of_account is None:
                raise ValueError("Virtual account fraction sizing is missing a target fraction.")
            target_notional = account_cash_balance * sizing.target_fraction_of_account
        else:  # pragma: no cover - enum validation should make this unreachable.
            raise ValueError(f"Unsupported virtual sizing mode: {sizing.mode}")

        if target_notional is None or target_notional <= 0:
            raise ValueError("Virtual target notional must be positive.")
        if sizing_price is None or sizing_price <= 0:
            raise ValueError(
                "Virtual target_notional sizing requires a positive limit, stop, or market price."
            )
        estimated_quantity = target_notional / sizing_price
        allow_round_down = True

    if estimated_quantity is None:
        raise ValueError("Virtual order quantity could not be estimated from sizing.")
    quantity, quantity_warnings = _normalize_virtual_stock_quantity(
        estimated_quantity,
        allow_round_down=allow_round_down,
    )
    warnings.extend(quantity_warnings)
    return quantity, warnings, {
        "mode": sizing.mode.value,
        "target_notional": str(target_notional) if target_notional is not None else None,
        "target_quantity": (
            str(sizing.target_quantity) if sizing.target_quantity is not None else None
        ),
        "target_fraction_of_account": (
            str(sizing.target_fraction_of_account)
            if sizing.target_fraction_of_account is not None
            else None
        ),
        "sizing_price": str(sizing_price) if sizing_price is not None else None,
        "estimated_quantity": str(estimated_quantity),
        "normalized_quantity": str(quantity),
        "account_cash_balance": str(account_cash_balance),
    }


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
    account_cash_balance: Decimal,
    quantity: Decimal | None = None,
    oca_group: str | None = None,
    oca_type: int | None = None,
) -> dict[str, Any]:
    order_id = _new_virtual_order_id()
    perm_id = _new_virtual_perm_id(order_id)
    market_price, price_source = (
        _virtual_condition_price_for_order(
            quote,
            action=action,
            order_type=order_type,
        )
        if quote is not None
        else (None, None)
    )
    price_met, condition_code = _virtual_price_condition(
        action=action,
        order_type=order_type,
        market_price=market_price,
        limit_price=limit_price,
        stop_price=stop_price,
    )
    execution_price = (
        _virtual_execution_price_for_order(
            quote=quote,
            action=action,
            order_type=order_type,
            condition_price=market_price,
            condition_source=price_source,
            limit_price=limit_price,
            stop_price=stop_price,
        )
        if price_met
        else None
    )
    total_quantity, sizing_warnings, sizing_payload = _virtual_order_quantity(
        instruction=instruction,
        explicit_quantity=quantity,
        order_type=order_type,
        limit_price=limit_price,
        stop_price=stop_price,
        market_price=market_price,
        account_cash_balance=account_cash_balance,
    )
    if price_met and price_source not in (None, "QUOTE"):
        condition_code = f"{condition_code}:{price_source}"
    status = "FILLED" if price_met else "Submitted"
    fill_payload = (
        _build_virtual_fill_payload(
            order_id=order_id,
            perm_id=perm_id,
            order_ref=order_ref,
            action=action,
            quantity=total_quantity,
            price=execution_price,
            quote=quote,
            condition_code=condition_code,
        )
        if price_met and execution_price is not None and quote is not None
        else None
    )
    return _serialize_for_json(
        {
            "broker_kind": BROKER_KIND_VIRTUAL,
            "instruction_id": instruction.instruction_id,
            "account": account_key,
            "is_virtual": True,
            "warnings": sizing_warnings,
            "resolved_contract": _build_resolved_contract(instruction),
            "order": {
                "order_ref": order_ref,
                "action": action,
                "order_type": order_type,
                "time_in_force": time_in_force,
                "limit_price": _decimal_to_string(limit_price),
                "stop_price": _decimal_to_string(stop_price),
                "total_quantity": str(total_quantity),
                "outside_rth": False,
                "oca_group": oca_group,
                "oca_type": oca_type,
                "transmit": False,
                "is_virtual": True,
            },
            "broker_order_status": {
                "orderId": order_id,
                "status": status,
                "filled": str(total_quantity) if price_met else "0",
                "remaining": "0" if price_met else str(total_quantity),
                "avgFillPrice": _decimal_to_string(execution_price) if price_met else "0",
                "permId": perm_id,
                "parentId": 0,
                "lastFillPrice": _decimal_to_string(execution_price) if price_met else "0",
                "clientId": 0,
                "whyHeld": "",
                "mktCapPrice": "0",
            },
            "virtual_execution": {
                "price_met": price_met,
                "condition_code": condition_code,
                "quantity_disregarded": False,
                "sizing": sizing_payload,
                "fixed_commission": str(VIRTUAL_FIXED_COMMISSION_SEK),
                "fixed_commission_currency": "SEK",
                "market_price": _decimal_to_string(market_price),
                "market_price_source": price_source,
                "execution_price": _decimal_to_string(execution_price),
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
        broker_account = ensure_virtual_account(
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
            account_cash_balance=_virtual_cash_balance(broker_account),
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
    del broker_config, timeout
    account_key = normalize_virtual_account_key(instruction.account.account_key)
    action = "SELL" if instruction.intent.side == "BUY" else "BUY"
    with session_scope(session_factory) as session:
        broker_account = ensure_virtual_account(
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
            account_cash_balance=_virtual_cash_balance(broker_account),
            quantity=quantity,
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


def _latest_position_snapshot(
    session: Session,
    *,
    broker_account_id: int,
    symbol: str,
    currency: str,
    security_type: str,
) -> PositionSnapshotRecord | None:
    return session.execute(
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


def _latest_virtual_position_snapshots_for_account(
    session: Session,
    *,
    broker_account_id: int,
) -> tuple[PositionSnapshotRecord, ...]:
    rows = session.execute(
        select(PositionSnapshotRecord)
        .where(
            PositionSnapshotRecord.broker_account_id == broker_account_id,
            PositionSnapshotRecord.is_virtual.is_(True),
        )
        .order_by(
            PositionSnapshotRecord.symbol.asc(),
            PositionSnapshotRecord.currency.asc(),
            PositionSnapshotRecord.security_type.asc(),
            PositionSnapshotRecord.snapshot_at.desc(),
            PositionSnapshotRecord.id.desc(),
        )
    ).scalars()

    latest_by_identity: dict[tuple[str, str, str, str | None], PositionSnapshotRecord] = {}
    for row in rows:
        identity = (row.symbol, row.currency, row.security_type, row.local_symbol)
        latest_by_identity.setdefault(identity, row)
    return tuple(latest_by_identity.values())


def _position_unrealized_pnl(position: PositionSnapshotRecord) -> Decimal:
    quantity = _to_decimal(position.quantity) or Decimal("0")
    average_cost = _to_decimal(position.average_cost)
    market_price = _to_decimal(position.market_price)
    if quantity == 0 or average_cost is None or market_price is None:
        return Decimal("0")
    return quantity * (market_price - average_cost)


def _fill_signed_quantity(fill: ExecutionFillRecord) -> Decimal:
    quantity = _to_decimal(fill.quantity) or Decimal("0")
    side = str(fill.side or "").strip().upper()
    if side in {"BOT", "BUY"}:
        return quantity
    if side in {"SLD", "SELL"}:
        return -quantity
    return quantity


def _sum_virtual_realized_pnl(
    session: Session,
    *,
    broker_account_id: int,
    account_key: str,
) -> Decimal:
    fills = session.execute(
        select(ExecutionFillRecord)
        .where(
            ExecutionFillRecord.broker_account_id == broker_account_id,
            ExecutionFillRecord.account_key == account_key,
            ExecutionFillRecord.is_virtual.is_(True),
        )
        .order_by(
            ExecutionFillRecord.symbol.asc(),
            ExecutionFillRecord.currency.asc(),
            ExecutionFillRecord.security_type.asc(),
            ExecutionFillRecord.executed_at.asc(),
            ExecutionFillRecord.id.asc(),
        )
    ).scalars()

    positions: dict[tuple[str, str, str], tuple[Decimal, Decimal | None]] = {}
    realized = Decimal("0")
    for fill in fills:
        identity = (fill.symbol, fill.currency, fill.security_type)
        current_quantity, average_cost = positions.get(identity, (Decimal("0"), None))
        fill_quantity = _fill_signed_quantity(fill)
        fill_price = _to_decimal(fill.price)
        if fill_quantity == 0 or fill_price is None:
            continue

        current_sign = 1 if current_quantity > 0 else -1 if current_quantity < 0 else 0
        fill_sign = 1 if fill_quantity > 0 else -1
        if current_sign == 0 or current_sign == fill_sign:
            new_quantity = current_quantity + fill_quantity
            if new_quantity == 0:
                positions[identity] = (Decimal("0"), None)
                continue
            current_notional = abs(current_quantity) * (average_cost or fill_price)
            fill_notional = abs(fill_quantity) * fill_price
            positions[identity] = (
                new_quantity,
                (current_notional + fill_notional) / abs(new_quantity),
            )
            continue

        close_quantity = min(abs(current_quantity), abs(fill_quantity))
        if average_cost is not None:
            realized += (
                close_quantity * (fill_price - average_cost)
                if current_quantity > 0
                else close_quantity * (average_cost - fill_price)
            )

        new_quantity = current_quantity + fill_quantity
        if new_quantity == 0:
            positions[identity] = (Decimal("0"), None)
        elif (new_quantity > 0 and current_quantity > 0) or (
            new_quantity < 0 and current_quantity < 0
        ):
            positions[identity] = (new_quantity, average_cost)
        else:
            positions[identity] = (new_quantity, fill_price)
    return realized


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
    realized_pnl = _sum_virtual_realized_pnl(
        session,
        broker_account_id=broker_account.id,
        account_key=broker_account.account_key,
    )
    position_unrealized_pnl = Decimal("0")
    position_count = 0
    for position in _latest_virtual_position_snapshots_for_account(
        session,
        broker_account_id=broker_account.id,
    ):
        quantity = _to_decimal(position.quantity) or Decimal("0")
        if quantity == 0:
            continue
        position_count += 1
        position_unrealized_pnl += _position_unrealized_pnl(position)

    cash_value = cash_balance + realized_pnl - total_commissions
    net_liquidation = cash_value + position_unrealized_pnl
    snapshot = AccountSnapshotRecord(
        broker_account_id=broker_account.id,
        is_virtual=True,
        snapshot_at=snapshot_at,
        source="virtual_execution",
        net_liquidation=str(net_liquidation),
        total_cash_value=str(cash_value),
        buying_power=str(net_liquidation),
        available_funds=str(cash_value),
        excess_liquidity=str(net_liquidation),
        cushion="1" if net_liquidation >= 0 else "0",
        currency="SEK",
        raw_payload={
            "virtual_account": True,
            "cash_balance_sek": str(cash_balance),
            "realized_pnl_sek": str(realized_pnl),
            "unrealized_pnl_sek": str(position_unrealized_pnl),
            "total_commissions_sek": str(total_commissions),
            "open_position_count": position_count,
        },
    )
    session.add(snapshot)
    return snapshot


def _apply_virtual_fill_to_position(
    *,
    current_quantity: Decimal,
    current_average_cost: Decimal | None,
    current_realized_pnl: Decimal,
    fill_delta: Decimal,
    fill_price: Decimal,
) -> tuple[Decimal, Decimal | None, Decimal]:
    if fill_delta == 0:
        return current_quantity, current_average_cost, current_realized_pnl

    current_sign = 1 if current_quantity > 0 else -1 if current_quantity < 0 else 0
    fill_sign = 1 if fill_delta > 0 else -1
    if current_sign == 0 or current_sign == fill_sign:
        new_quantity = current_quantity + fill_delta
        if new_quantity == 0:
            return Decimal("0"), None, current_realized_pnl
        current_notional = abs(current_quantity) * (current_average_cost or fill_price)
        fill_notional = abs(fill_delta) * fill_price
        return (
            new_quantity,
            (current_notional + fill_notional) / abs(new_quantity),
            current_realized_pnl,
        )

    close_quantity = min(abs(current_quantity), abs(fill_delta))
    if current_average_cost is not None:
        current_realized_pnl += (
            close_quantity * (fill_price - current_average_cost)
            if current_quantity > 0
            else close_quantity * (current_average_cost - fill_price)
        )

    new_quantity = current_quantity + fill_delta
    if new_quantity == 0:
        return Decimal("0"), None, current_realized_pnl
    if (new_quantity > 0 and current_quantity > 0) or (
        new_quantity < 0 and current_quantity < 0
    ):
        return new_quantity, current_average_cost, current_realized_pnl
    return new_quantity, fill_price, current_realized_pnl


def _persist_virtual_position_snapshot(
    session: Session,
    *,
    broker_account: BrokerAccountRecord,
    broker_order: BrokerOrderRecord,
    fill_payload: dict[str, Any],
) -> None:
    fill_quantity = _to_decimal(fill_payload.get("quantity")) or Decimal("1")
    fill_price = _to_decimal(fill_payload.get("price")) or Decimal("0")
    latest_snapshot = _latest_position_snapshot(
        session,
        broker_account_id=broker_account.id,
        symbol=broker_order.symbol,
        currency=broker_order.currency,
        security_type=broker_order.security_type,
    )
    current_quantity = (
        _to_decimal(latest_snapshot.quantity)
        if latest_snapshot is not None
        else None
    ) or Decimal("0")
    current_average_cost = (
        _to_decimal(latest_snapshot.average_cost)
        if latest_snapshot is not None
        else None
    )
    current_realized_pnl = (
        _to_decimal(latest_snapshot.realized_pnl)
        if latest_snapshot is not None
        else None
    ) or Decimal("0")
    delta = fill_quantity if broker_order.side == "BUY" else -fill_quantity
    new_quantity, average_cost, realized_pnl = _apply_virtual_fill_to_position(
        current_quantity=current_quantity,
        current_average_cost=current_average_cost,
        current_realized_pnl=current_realized_pnl,
        fill_delta=delta,
        fill_price=fill_price,
    )
    market_value = new_quantity * fill_price
    unrealized_pnl = (
        new_quantity * (fill_price - average_cost)
        if new_quantity != 0 and average_cost is not None
        else Decimal("0")
    )
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
        average_cost=str(average_cost) if average_cost is not None else None,
        market_price=str(fill_price),
        market_value=str(market_value),
        unrealized_pnl=str(unrealized_pnl),
        realized_pnl=str(realized_pnl),
        raw_payload=_serialize_for_json({
            "virtual_execution": fill_payload,
            "previous_quantity": str(current_quantity),
            "delta_quantity": str(delta),
        }),
    )
    session.add(snapshot)


def _quote_mark_price(quote: VirtualMarketQuoteRecord) -> Decimal | None:
    last = _to_decimal(quote.last_price)
    midpoint = _to_decimal(quote.midpoint_price)
    bid = _to_decimal(quote.bid_price)
    ask = _to_decimal(quote.ask_price)
    if midpoint is None and bid is not None and ask is not None:
        midpoint = (bid + ask) / Decimal("2")
    for candidate in (last, midpoint, bid, ask):
        if candidate is not None and candidate > 0:
            return candidate
    return None


def _persist_virtual_position_mark_snapshot(
    session: Session,
    *,
    broker_account: BrokerAccountRecord,
    quote: VirtualMarketQuoteRecord,
) -> PositionSnapshotRecord | None:
    latest_snapshot = _latest_position_snapshot(
        session,
        broker_account_id=broker_account.id,
        symbol=quote.symbol,
        currency=quote.currency,
        security_type=quote.security_type,
    )
    if latest_snapshot is None:
        return None
    quantity = _to_decimal(latest_snapshot.quantity) or Decimal("0")
    if quantity == 0:
        return None
    mark_price = _quote_mark_price(quote)
    if mark_price is None:
        return None
    average_cost = _to_decimal(latest_snapshot.average_cost)
    market_value = quantity * mark_price
    unrealized_pnl = (
        quantity * (mark_price - average_cost)
        if average_cost is not None
        else Decimal("0")
    )
    snapshot = PositionSnapshotRecord(
        broker_account_id=broker_account.id,
        is_virtual=True,
        snapshot_at=quote.observed_at,
        source="virtual_market_mark",
        symbol=latest_snapshot.symbol,
        exchange=latest_snapshot.exchange,
        currency=latest_snapshot.currency,
        security_type=latest_snapshot.security_type,
        primary_exchange=latest_snapshot.primary_exchange,
        local_symbol=latest_snapshot.local_symbol,
        quantity=str(quantity),
        average_cost=str(average_cost) if average_cost is not None else None,
        market_price=str(mark_price),
        market_value=str(market_value),
        unrealized_pnl=str(unrealized_pnl),
        realized_pnl=latest_snapshot.realized_pnl,
        raw_payload=_serialize_for_json(
            {
                "virtual_market_mark": True,
                "quote": serialize_virtual_quote(quote),
                "previous_snapshot_id": latest_snapshot.id,
            }
        ),
    )
    session.add(snapshot)
    return snapshot


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
    price, price_source = _virtual_condition_price_for_order(
        quote,
        action=broker_order.side,
        order_type=broker_order.order_type,
    )
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
    execution_price = _virtual_execution_price_for_order(
        quote=quote,
        action=broker_order.side,
        order_type=broker_order.order_type,
        condition_price=price,
        condition_source=price_source,
        limit_price=limit_price,
        stop_price=stop_price,
    )
    if execution_price is None:
        return None
    if price_source not in (None, "QUOTE"):
        condition_code = f"{condition_code}:{price_source}"
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
        quantity=_to_decimal(broker_order.total_quantity) or Decimal("1"),
        price=execution_price,
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
            _open_virtual_order_status_clause(),
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
        _persist_virtual_position_mark_snapshot(
            session,
            broker_account=broker_account,
            quote=quote,
        )
        filled_orders = process_virtual_quote_fills(session, quote=quote)
        if not filled_orders:
            _persist_virtual_account_snapshot(
                session,
                broker_account=broker_account,
                snapshot_at=quote_observed_at,
            )
        return {
            "quote": serialize_virtual_quote(quote),
            "filled_order_count": len(filled_orders),
            "filled_orders": filled_orders,
        }


def _normalize_stream_symbol(value: Any) -> str | None:
    normalized = _normalize_text(value)
    if normalized is None:
        return None
    return normalized.upper()


def _stream_payload(stream_snapshot: dict[str, Any]) -> dict[str, Any]:
    nested = stream_snapshot.get("stream")
    if isinstance(nested, dict):
        return nested
    return stream_snapshot


def _latest_stream_bar(
    stream_payload: dict[str, Any],
    *,
    symbol: str,
) -> dict[str, Any] | None:
    bars_by_symbol = stream_payload.get("bars_by_symbol")
    if not isinstance(bars_by_symbol, dict):
        return None
    bars = bars_by_symbol.get(symbol.upper())
    if not bars and "-" in symbol:
        bars = bars_by_symbol.get(symbol.replace("-", " ").upper())
    if not isinstance(bars, list) or not bars:
        return None
    latest = bars[-1]
    return latest if isinstance(latest, dict) else None


def _quote_observed_at_from_stream(
    *,
    quote_payload: dict[str, Any] | None,
    bar_payload: dict[str, Any] | None,
    fallback: datetime,
) -> datetime:
    if quote_payload is not None:
        observed_at = _parse_optional_datetime_value(
            quote_payload.get("last_trade_at") or quote_payload.get("updated_at")
        )
        if observed_at is not None:
            return observed_at
    if bar_payload is not None:
        observed_at = _parse_optional_datetime_value(bar_payload.get("timestamp"))
        if observed_at is not None:
            return observed_at
    return fallback


def record_virtual_market_quotes_from_stream_snapshot(
    session_factory: sessionmaker[Session],
    *,
    stream_snapshot: dict[str, Any],
    observed_at: datetime | None = None,
    account_key: str | None = None,
) -> dict[str, Any]:
    """Mirror live stream prices into the virtual quote tape for open virtual orders."""

    stream = _stream_payload(stream_snapshot)
    quote_payloads = stream.get("quotes")
    quotes_by_symbol: dict[str, dict[str, Any]] = {}
    if isinstance(quote_payloads, list):
        for quote_payload in quote_payloads:
            if not isinstance(quote_payload, dict):
                continue
            symbol = _normalize_stream_symbol(quote_payload.get("symbol"))
            if symbol is not None:
                quotes_by_symbol[symbol] = quote_payload

    fallback_observed_at = observed_at or utc_now()
    normalized_account_filter = (
        normalize_virtual_account_key(account_key) if account_key is not None else None
    )

    with session_scope(session_factory) as session:
        statement = (
            select(BrokerOrderRecord)
            .where(
                BrokerOrderRecord.broker_kind == BROKER_KIND_VIRTUAL,
                BrokerOrderRecord.is_virtual.is_(True),
                _open_virtual_order_status_clause(),
            )
            .order_by(BrokerOrderRecord.account_key.asc(), BrokerOrderRecord.symbol.asc())
        )
        if normalized_account_filter is not None:
            statement = statement.where(
                BrokerOrderRecord.account_key == normalized_account_filter
            )
        broker_orders = session.execute(statement).scalars().all()

        target_by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for broker_order in broker_orders:
            key = (
                broker_order.account_key,
                broker_order.symbol,
                broker_order.currency,
                broker_order.security_type,
            )
            target_by_key[key] = {
                "account_key": broker_order.account_key,
                "symbol": broker_order.symbol,
                "exchange": broker_order.exchange,
                "currency": broker_order.currency,
                "security_type": broker_order.security_type,
                "primary_exchange": broker_order.primary_exchange,
                "local_symbol": broker_order.local_symbol,
            }

        position_statement = (
            select(PositionSnapshotRecord, BrokerAccountRecord)
            .join(
                BrokerAccountRecord,
                BrokerAccountRecord.id == PositionSnapshotRecord.broker_account_id,
            )
            .where(
                PositionSnapshotRecord.is_virtual.is_(True),
                BrokerAccountRecord.broker_kind == BROKER_KIND_VIRTUAL,
            )
            .order_by(
                BrokerAccountRecord.account_key.asc(),
                PositionSnapshotRecord.symbol.asc(),
                PositionSnapshotRecord.currency.asc(),
                PositionSnapshotRecord.security_type.asc(),
                PositionSnapshotRecord.snapshot_at.desc(),
                PositionSnapshotRecord.id.desc(),
            )
        )
        if normalized_account_filter is not None:
            position_statement = position_statement.where(
                BrokerAccountRecord.account_key == normalized_account_filter
            )
        position_rows = session.execute(position_statement).all()

        seen_position_keys: set[tuple[str, str, str, str]] = set()
        for position_snapshot, broker_account in position_rows:
            key = (
                broker_account.account_key,
                position_snapshot.symbol,
                position_snapshot.currency,
                position_snapshot.security_type,
            )
            if key in seen_position_keys:
                continue
            seen_position_keys.add(key)
            quantity = _to_decimal(position_snapshot.quantity) or Decimal("0")
            if quantity == 0:
                continue
            target_by_key.setdefault(
                key,
                {
                    "account_key": broker_account.account_key,
                    "symbol": position_snapshot.symbol,
                    "exchange": position_snapshot.exchange,
                    "currency": position_snapshot.currency,
                    "security_type": position_snapshot.security_type,
                    "primary_exchange": position_snapshot.primary_exchange,
                    "local_symbol": position_snapshot.local_symbol,
                },
            )

        filled_orders: list[dict[str, Any]] = []
        quotes_recorded: list[dict[str, Any]] = []
        skipped_count = 0

        for key, target in target_by_key.items():
            stream_symbol = str(target["symbol"]).upper()
            quote_payload = quotes_by_symbol.get(stream_symbol)
            bar_payload = _latest_stream_bar(stream, symbol=stream_symbol)
            if quote_payload is None and bar_payload is None:
                skipped_count += 1
                continue

            bid_price = (
                _optional_decimal(quote_payload.get("bid_price"))
                if quote_payload is not None
                else None
            )
            ask_price = (
                _optional_decimal(quote_payload.get("ask_price"))
                if quote_payload is not None
                else None
            )
            last_price = (
                _optional_decimal(quote_payload.get("last_price"))
                if quote_payload is not None
                else None
            )
            if last_price is None and bar_payload is not None:
                last_price = _optional_decimal(bar_payload.get("close"))

            if (
                bid_price is None
                and ask_price is None
                and last_price is None
                and bar_payload is None
            ):
                skipped_count += 1
                continue

            broker_account = ensure_virtual_account(
                session,
                account_key=target["account_key"],
                base_currency=target["currency"] or "SEK",
            )
            quote = VirtualMarketQuoteRecord(
                account_key=target["account_key"],
                observed_at=_quote_observed_at_from_stream(
                    quote_payload=quote_payload,
                    bar_payload=bar_payload,
                    fallback=fallback_observed_at,
                ),
                symbol=target["symbol"],
                exchange=(
                    _normalize_text(
                        quote_payload.get("exchange") if quote_payload else None
                    )
                    or target["exchange"]
                ).upper(),
                currency=target["currency"],
                security_type=target["security_type"],
                primary_exchange=(
                    _normalize_text(
                        quote_payload.get("primary_exchange") if quote_payload else None
                    )
                    or target["primary_exchange"]
                ),
                local_symbol=target["local_symbol"],
                bid_price=_decimal_to_string(bid_price),
                ask_price=_decimal_to_string(ask_price),
                last_price=_decimal_to_string(last_price),
                midpoint_price=None,
                source=_STREAM_VIRTUAL_QUOTE_SOURCE,
                raw_payload=_serialize_for_json(
                    {
                        "stream_quote": quote_payload or {},
                        "latest_stream_bar": bar_payload or {},
                    }
                ),
                metadata_json={
                    "virtual_stream_bridge": True,
                    "broker_order_ids_seen": [
                        order.id
                        for order in broker_orders
                        if order.account_key == target["account_key"]
                        and order.symbol == target["symbol"]
                        and order.currency == target["currency"]
                        and order.security_type == target["security_type"]
                    ],
                },
            )
            session.add(quote)
            session.flush()
            _persist_virtual_position_mark_snapshot(
                session,
                broker_account=broker_account,
                quote=quote,
            )
            quote_fills = process_virtual_quote_fills(session, quote=quote)
            filled_orders.extend(quote_fills)
            if not quote_fills:
                _persist_virtual_account_snapshot(
                    session,
                    broker_account=broker_account,
                    snapshot_at=quote.observed_at,
                )
            quotes_recorded.append(serialize_virtual_quote(quote))

        return {
            "source": _STREAM_VIRTUAL_QUOTE_SOURCE,
            "open_virtual_order_count": len(broker_orders),
            "virtual_market_target_count": len(target_by_key),
            "quote_count": len(quotes_recorded),
            "quotes": quotes_recorded,
            "skipped_order_count": skipped_count,
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
            _open_virtual_order_status_clause(),
        )
        return session.execute(unsettled_order.limit(1)).first() is not None


def is_virtual_instruction(instruction: ExecutionInstruction) -> bool:
    return is_virtual_account_key(instruction.account.account_key)
