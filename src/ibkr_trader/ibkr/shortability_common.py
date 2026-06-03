from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timezone
from decimal import Decimal
from decimal import InvalidOperation
from enum import StrEnum
from html import unescape
import json
from pathlib import Path
import re
from threading import Event
from threading import Lock
from threading import Thread
from time import monotonic
from time import sleep
from typing import Any
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.request import Request
from urllib.request import urlopen

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.domain.contract_resolution import ContractResolveQuery
from ibkr_trader.ibkr.contracts import build_ibkr_contract
from ibkr_trader.ibkr.errors import IbkrDependencyError


class ShortabilityMarketDataType(StrEnum):
    LIVE = "LIVE"
    FROZEN = "FROZEN"
    DELAYED = "DELAYED"
    DELAYED_FROZEN = "DELAYED_FROZEN"


class ShortabilitySource(StrEnum):
    OFFICIAL_IBKR_PAGE = "OFFICIAL_IBKR_PAGE"
    BROKER_TICKS = "BROKER_TICKS"


class ShortabilityStatus(StrEnum):
    SHORTABLE = "shortable"
    LOCATE_REQUIRED = "locate_required"
    NOT_SHORTABLE = "not_shortable"
    NOT_FOUND = "not_found"
    TIMEOUT = "timeout"
    ERROR = "error"
    UNKNOWN_STATUS = "unknown_status"


MARKET_DATA_TYPE_CODES: dict[ShortabilityMarketDataType, int] = {
    ShortabilityMarketDataType.LIVE: 1,
    ShortabilityMarketDataType.FROZEN: 2,
    ShortabilityMarketDataType.DELAYED: 3,
    ShortabilityMarketDataType.DELAYED_FROZEN: 4,
}


GENERIC_TICK_SHORTABLE = 236
TICK_TYPE_SHORTABLE = 46
TICK_TYPE_SHORTABLE_SHARES = 89
DEFAULT_POST_DATA_GRACE_SECONDS = 0.35
GLOBAL_IBKR_MESSAGE_CODES = {2104, 2106, 2107, 2108, 2119, 2158}
OFFICIAL_IBKR_SHORTABLE_STOCKHOLM_URL = (
    "https://www.interactivebrokers.com/en/index.php"
    "?asset=&cntry=swedish&f=4587&ib_entity=llc&ln=&tag=Sweden"
)
OFFICIAL_IBKR_LAST_UPDATED_RE = re.compile(
    r"Last updated:\s*([^<]+)",
    re.IGNORECASE,
)
OFFICIAL_IBKR_SHORTABLE_ROW_RE = re.compile(
    r"<tr>\s*"
    r"<td class='text-center'>\s*"
    r"<a href=\"javascript:NewWindow\('(?P<details_url>[^']*conid=(?P<conid>\d+)[^']*)'[^>]*>"
    r"(?P<symbol>[^<]+)</a>\s*</td>\s*"
    r"<td class='text-center'>(?P<currency>[^<]+)</td>\s*"
    r"<td>(?P<long_name>[^<]+)</td>",
    re.IGNORECASE | re.DOTALL,
)


@dataclass(slots=True)
class ShortabilitySnapshotQuery:
    symbols: tuple[str, ...] | None = None
    as_of_date: date | None = None
    exchange: str = "SMART"
    primary_exchange: str = "SFB"
    currency: str = "SEK"
    security_type: str = "STK"
    source: ShortabilitySource = ShortabilitySource.OFFICIAL_IBKR_PAGE
    only_shortable: bool = True
    market_data_type: ShortabilityMarketDataType = ShortabilityMarketDataType.LIVE
    per_symbol_timeout_seconds: float = 2.0
    max_concurrent: int = 25
    max_symbols: int | None = None

    def validate(self) -> None:
        if not self.exchange:
            raise ValueError("exchange is required")
        if not self.primary_exchange:
            raise ValueError("primary_exchange is required")
        if not self.currency:
            raise ValueError("currency is required")
        if not self.security_type:
            raise ValueError("security_type is required")
        if self.per_symbol_timeout_seconds <= 0:
            raise ValueError("per_symbol_timeout_seconds must be positive")
        if self.per_symbol_timeout_seconds > 30:
            raise ValueError("per_symbol_timeout_seconds must be at most 30")
        if self.max_concurrent <= 0:
            raise ValueError("max_concurrent must be positive")
        if self.max_concurrent > 100:
            raise ValueError("max_concurrent must be at most 100")
        if self.max_symbols is not None and self.max_symbols <= 0:
            raise ValueError("max_symbols must be positive when provided")
        if self.symbols is not None:
            if not self.symbols:
                raise ValueError("symbols must contain at least one symbol when provided")
            if any(not symbol for symbol in self.symbols):
                raise ValueError("symbols must contain only non-empty symbols")


@dataclass(slots=True)
class ShortabilityEntry:
    symbol: str
    exchange: str
    primary_exchange: str
    currency: str
    security_type: str
    status: ShortabilityStatus
    source_symbol: str | None = None
    long_name: str | None = None
    broker_conid: str | None = None
    shortable_value: Decimal | None = None
    shortable_shares: Decimal | None = None
    market_data_type: str | None = None
    errors: tuple[dict[str, Any], ...] = ()
    completed_reason: str | None = None


@dataclass(slots=True)
class ShortabilitySnapshot:
    snapshot_at: datetime
    source: str
    source_url: str | None
    source_updated_text: str | None
    market_data_type: str
    universe_source: str
    universe_as_of_date: date | None
    requested_symbol_count: int
    evaluated_symbol_count: int
    returned_symbol_count: int
    only_shortable: bool
    status_counts: dict[str, int]
    global_errors: tuple[dict[str, Any], ...]
    entries: tuple[ShortabilityEntry, ...]
    evaluated_entries: tuple[ShortabilityEntry, ...] = ()


@dataclass(frozen=True, slots=True)
class ShortabilityPersistenceResult:
    as_of_date: str
    shortable_count: int
    shortable_or_locate_count: int
    shortable_path: str
    shortable_or_locate_path: str
    snapshot_path: str
    latest_snapshot_path: str


@dataclass(frozen=True, slots=True)
class OfficialIbkrShortableRow:
    symbol: str
    normalized_symbol: str
    currency: str
    long_name: str
    broker_conid: str
    details_url: str


def _load_shortability_runtime() -> tuple[type[Any], type[Any], type[Any]]:
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


def _normalize_symbol(raw_symbol: Any) -> str:
    symbol = str(raw_symbol).strip()
    if not symbol:
        raise ValueError("symbols must contain only non-empty symbols")
    return symbol.upper()


@dataclass(frozen=True, slots=True)
class StockholmListedInstrument:
    symbol: str
    listed_from: date
    listed_to: date


@dataclass(frozen=True, slots=True)
class StockholmInstrumentIdentity:
    symbol: str
    isin: str | None
    ticker_alias: str | None
    yahoo_symbol: str | None


def _normalize_official_ibkr_symbol(raw_symbol: str) -> str:
    return _normalize_symbol(raw_symbol).replace(".", "-")


def parse_official_ibkr_shortable_rows(
    html_text: str,
) -> tuple[str | None, tuple[OfficialIbkrShortableRow, ...]]:
    last_updated_match = OFFICIAL_IBKR_LAST_UPDATED_RE.search(html_text)
    last_updated_text = (
        unescape(last_updated_match.group(1)).strip()
        if last_updated_match is not None
        else None
    )

    parsed_rows: list[OfficialIbkrShortableRow] = []
    seen_symbols: set[str] = set()
    for match in OFFICIAL_IBKR_SHORTABLE_ROW_RE.finditer(html_text):
        symbol = _normalize_symbol(unescape(match.group("symbol")))
        normalized_symbol = _normalize_official_ibkr_symbol(symbol)
        if normalized_symbol in seen_symbols:
            continue
        seen_symbols.add(normalized_symbol)
        parsed_rows.append(
            OfficialIbkrShortableRow(
                symbol=symbol,
                normalized_symbol=normalized_symbol,
                currency=_normalize_symbol(unescape(match.group("currency"))),
                long_name=" ".join(unescape(match.group("long_name")).split()),
                broker_conid=match.group("conid"),
                details_url=unescape(match.group("details_url")),
            )
        )

    if not parsed_rows:
        raise ValueError(
            "IBKR official Sweden shortable page did not contain any parsable rows."
        )

    return last_updated_text, tuple(parsed_rows)


def fetch_official_ibkr_shortable_rows(
    *,
    source_url: str = OFFICIAL_IBKR_SHORTABLE_STOCKHOLM_URL,
    timeout_seconds: float = 30.0,
) -> tuple[str | None, tuple[OfficialIbkrShortableRow, ...]]:
    request = Request(
        source_url,
        headers={"User-Agent": "ibkr-trader/0.1 (+https://github.com/maxilirator/ibkr-trader)"},
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
            html_text = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        raise ConnectionError(
            f"IBKR official shortability page returned HTTP {exc.code}."
        ) from exc
    except URLError as exc:
        reason = getattr(exc, "reason", exc)
        if isinstance(reason, TimeoutError):
            raise TimeoutError(
                "Timed out while fetching the IBKR official shortability page."
            ) from exc
        raise ConnectionError(
            "Failed to fetch the IBKR official shortability page."
        ) from exc
    return parse_official_ibkr_shortable_rows(html_text)


def _coerce_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None

    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _load_stockholm_identity_runtime() -> Any:
    try:
        import pandas as pd
    except ModuleNotFoundError as exc:
        raise IbkrDependencyError(
            "Stockholm identity metadata requires pandas and parquet support in "
            "this environment."
        ) from exc

    return pd


def _filter_shortable_entries(
    entries: tuple[ShortabilityEntry, ...],
    *,
    only_shortable: bool,
) -> tuple[ShortabilityEntry, ...]:
    if not only_shortable:
        return entries

    return tuple(
        entry
        for entry in entries
        if entry.status in {
            ShortabilityStatus.SHORTABLE,
            ShortabilityStatus.LOCATE_REQUIRED,
        }
    )


def _count_entry_statuses(entries: tuple[ShortabilityEntry, ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for entry in entries:
        counts[entry.status.value] = counts.get(entry.status.value, 0) + 1
    return counts


def interpret_shortability_status(
    shortable_value: Decimal | None,
    shortable_shares: Decimal | None = None,
) -> ShortabilityStatus:
    if shortable_value is not None:
        if shortable_value > Decimal("2.5"):
            return ShortabilityStatus.SHORTABLE
        if shortable_value > Decimal("1.5"):
            return ShortabilityStatus.LOCATE_REQUIRED
        return ShortabilityStatus.NOT_SHORTABLE

    if shortable_shares is not None:
        if shortable_shares > 0:
            return ShortabilityStatus.SHORTABLE
        return ShortabilityStatus.NOT_SHORTABLE

    return ShortabilityStatus.UNKNOWN_STATUS


def _classify_request_status(request: "_PendingShortabilityRequest") -> ShortabilityStatus:
    if request.shortable_value is not None or request.shortable_shares is not None:
        return interpret_shortability_status(
            request.shortable_value,
            request.shortable_shares,
        )

    last_error_code = request.errors[-1]["error_code"] if request.errors else None
    if last_error_code == 200:
        return ShortabilityStatus.NOT_FOUND
    if request.completed_reason == "error":
        return ShortabilityStatus.ERROR
    if request.completed_reason in {None, "timeout"}:
        return ShortabilityStatus.TIMEOUT
    return ShortabilityStatus.UNKNOWN_STATUS



__all__ = [name for name in globals() if not name.startswith("__")]
