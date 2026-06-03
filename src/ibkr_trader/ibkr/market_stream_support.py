from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import time
from datetime import timedelta
from decimal import Decimal
from decimal import InvalidOperation
from threading import Event
from threading import Lock
from threading import RLock
from threading import Thread
from threading import current_thread
from typing import Any
from typing import Mapping
from zoneinfo import ZoneInfo

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.domain.contract_resolution import ContractResolveQuery
from ibkr_trader.ibkr.broker_circuit import BrokerHealthCircuit
from ibkr_trader.ibkr.contracts import build_ibkr_contract
from ibkr_trader.ibkr.errors import IbkrDependencyError
from ibkr_trader.ibkr.pacing import BrokerApiPacingGovernor


BID_PRICE_TICKS = {1, 66}
ASK_PRICE_TICKS = {2, 67}
LAST_PRICE_TICKS = {4, 68}
CLOSE_PRICE_TICKS = {9, 75}
BID_SIZE_TICKS = {0, 69}
ASK_SIZE_TICKS = {3, 70}
LAST_SIZE_TICKS = {5, 71}
MARKET_DATA_TYPE_CODES = {
    "LIVE": 1,
    "FROZEN": 2,
    "DELAYED": 3,
    "DELAYED_FROZEN": 4,
}


def _normalize_ib_error_args(args: tuple[Any, ...]) -> tuple[int | None, int, str, str]:
    if len(args) == 2:
        error_code, error_string = args
        return None, int(error_code), str(error_string), ""
    if len(args) == 3:
        first, second, third = args
        if isinstance(first, int) and isinstance(second, int):
            return int(first), int(second), str(third), ""
        return None, int(first), str(second), str(third or "")
    if len(args) >= 4:
        error_time, error_code, error_string, advanced_json = args[:4]
        return int(error_time), int(error_code), str(error_string), str(advanced_json or "")
    return None, 0, "Unknown IBKR market stream error callback", ""


@dataclass(frozen=True, slots=True)
class MarketStreamContract:
    symbol: str
    exchange: str = "SMART"
    currency: str = "SEK"
    security_type: str = "STK"
    primary_exchange: str | None = "SFB"
    local_symbol: str | None = None
    isin: str | None = None

    @property
    def key(self) -> str:
        return self.symbol.upper()

    def validate(self) -> None:
        if not self.symbol:
            raise ValueError("symbol is required")
        if not self.exchange:
            raise ValueError("exchange is required")
        if not self.currency:
            raise ValueError("currency is required")
        if not self.security_type:
            raise ValueError("security_type is required")


@dataclass(slots=True)
class MarketStreamBar:
    started_at: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    bar_count: int = 0

    def update(self, price: Decimal) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.bar_count += 1


@dataclass(slots=True)
class MarketStreamQuote:
    symbol: str
    exchange: str
    currency: str
    security_type: str
    primary_exchange: str | None
    bid_price: Decimal | None = None
    ask_price: Decimal | None = None
    last_price: Decimal | None = None
    close_price: Decimal | None = None
    bid_size: Decimal | None = None
    ask_size: Decimal | None = None
    last_size: Decimal | None = None
    updated_at: datetime | None = None
    last_trade_at: datetime | None = None
    market_data_type: int | None = None


@dataclass(slots=True)
class MarketStreamSubscription:
    request_id: int
    contract: MarketStreamContract
    subscribed_at: datetime
    status: str = "subscribed"
    last_error: str | None = None
    market_data_type: int | None = None


def _load_market_data_runtime() -> tuple[type[Any], type[Any], type[Any]]:
    try:
        from ibapi.client import EClient
        from ibapi.contract import Contract
        from ibapi.wrapper import EWrapper
    except ModuleNotFoundError as exc:
        raise IbkrDependencyError(
            "The official IBKR Python client is not installed. "
            "Install the current TWS API package from IBKR and make sure "
            "the `ibapi` module is available in this environment."
        ) from exc

    return EClient, EWrapper, Contract


def _parse_decimal(value: Any) -> Decimal | None:
    if value in (None, "", -1, -1.0):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() and parsed > 0 else None


def _serialize_decimal(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _minute_start(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(second=0, microsecond=0)



__all__ = [name for name in globals() if not name.startswith("__")]
