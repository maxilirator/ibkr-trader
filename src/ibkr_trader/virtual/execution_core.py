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


def _virtual_cash_balance(broker_account: BrokerAccountRecord) -> Decimal:
    metadata = broker_account.metadata_json or {}
    raw_balance = metadata.get(_VIRTUAL_CASH_BALANCE_METADATA_KEY)
    return Decimal(str(raw_balance)) if raw_balance not in (None, "") else Decimal("0")


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



__all__ = [name for name in globals() if not name.startswith("__")]
