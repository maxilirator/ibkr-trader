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
GLOBAL_IBKR_MESSAGE_CODES = {2104, 2106, 2107, 2108, 2158}
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


def load_stockholm_symbols_from_instruments_file(
    instruments_path: Path,
    *,
    as_of_date: date | None = None,
    max_symbols: int | None = None,
    today: date | None = None,
) -> tuple[tuple[str, ...], date]:
    if not instruments_path.exists():
        raise FileNotFoundError(
            f"Stockholm instruments path was not found: {instruments_path}"
        )
    if not instruments_path.is_file():
        raise ValueError(f"Stockholm instruments path is not a file: {instruments_path}")

    listed_instruments: list[StockholmListedInstrument] = []
    for line_number, raw_line in enumerate(
        instruments_path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        line = raw_line.strip()
        if not line:
            continue

        parts = line.split("\t")
        if len(parts) != 3:
            raise ValueError(
                f"Invalid Stockholm instruments row at line {line_number}: {raw_line!r}"
            )
        symbol, listed_from_raw, listed_to_raw = parts
        try:
            listed_from = date.fromisoformat(listed_from_raw)
            listed_to = date.fromisoformat(listed_to_raw)
        except ValueError as exc:
            raise ValueError(
                f"Invalid Stockholm instruments date at line {line_number}: {raw_line!r}"
            ) from exc

        listed_instruments.append(
            StockholmListedInstrument(
                symbol=_normalize_symbol(symbol),
                listed_from=listed_from,
                listed_to=listed_to,
            )
        )

    if not listed_instruments:
        raise ValueError(f"No listed instruments were found in {instruments_path}")

    effective_as_of_date = as_of_date
    if effective_as_of_date is None:
        reference_today = today or date.today()
        eligible_listed_to_dates = [
            item.listed_to
            for item in listed_instruments
            if item.listed_to <= reference_today
        ]
        effective_as_of_date = (
            max(eligible_listed_to_dates)
            if eligible_listed_to_dates
            else max(item.listed_to for item in listed_instruments)
        )
    symbols = sorted(
        item.symbol
        for item in listed_instruments
        if item.listed_from <= effective_as_of_date <= item.listed_to
    )

    if not symbols:
        raise ValueError(
            "No listed Stockholm instruments were found for "
            f"{effective_as_of_date.isoformat()} in {instruments_path}"
        )

    if max_symbols is not None:
        symbols = symbols[:max_symbols]

    return tuple(symbols), effective_as_of_date


def load_stockholm_identity_map(
    identity_path: Path,
    *,
    symbols: tuple[str, ...] | None = None,
) -> dict[str, StockholmInstrumentIdentity]:
    if not identity_path.exists():
        return {}
    if not identity_path.is_file():
        raise ValueError(f"Stockholm identity path is not a file: {identity_path}")

    pd = _load_stockholm_identity_runtime()
    frame = pd.read_parquet(
        identity_path,
        columns=["instrument", "isin", "ticker_alias", "yahoo_symbol"],
    )
    if symbols is not None:
        normalized_symbols = {_normalize_symbol(symbol) for symbol in symbols}
        frame = frame[frame["instrument"].str.upper().isin(normalized_symbols)]

    identity_map: dict[str, StockholmInstrumentIdentity] = {}
    for row in frame.itertuples(index=False):
        symbol = _normalize_symbol(row.instrument)
        identity_map[symbol] = StockholmInstrumentIdentity(
            symbol=symbol,
            isin=(str(row.isin).strip() or None),
            ticker_alias=(str(row.ticker_alias).strip() or None),
            yahoo_symbol=(str(row.yahoo_symbol).strip() or None),
        )
    return identity_map


def serialize_shortability_snapshot(snapshot: ShortabilitySnapshot) -> dict[str, Any]:
    payload = asdict(snapshot)
    payload["snapshot_at"] = snapshot.snapshot_at.isoformat()
    payload["universe_as_of_date"] = (
        snapshot.universe_as_of_date.isoformat()
        if snapshot.universe_as_of_date is not None
        else None
    )
    payload["entries"] = [
        {
            **entry,
            "status": entry["status"].value,
            "shortable_value": (
                str(entry["shortable_value"])
                if entry["shortable_value"] is not None
                else None
            ),
            "shortable_shares": (
                str(entry["shortable_shares"])
                if entry["shortable_shares"] is not None
                else None
            ),
        }
        for entry in payload["entries"]
    ]
    if snapshot.only_shortable:
        payload["evaluated_entries"] = [
            {
                **entry,
                "status": entry["status"].value,
                "shortable_value": (
                    str(entry["shortable_value"])
                    if entry["shortable_value"] is not None
                    else None
                ),
                "shortable_shares": (
                    str(entry["shortable_shares"])
                    if entry["shortable_shares"] is not None
                    else None
                ),
            }
            for entry in payload["evaluated_entries"]
        ]
    else:
        payload.pop("evaluated_entries", None)
    return payload


def _write_symbol_list(path: Path, symbols: tuple[str, ...]) -> None:
    content = "".join(f"{symbol.lower()}\n" for symbol in symbols)
    path.write_text(content, encoding="utf-8")


def persist_shortability_snapshot(
    snapshot_payload: dict[str, Any],
    *,
    instruments_dir: Path,
    meta_dir: Path,
) -> dict[str, Any]:
    instruments_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    as_of_date = snapshot_payload.get("universe_as_of_date") or str(
        snapshot_payload["snapshot_at"]
    )[:10]
    entries = tuple(
        snapshot_payload.get("evaluated_entries")
        or snapshot_payload.get("entries", ())
    )
    shortable_symbols = tuple(
        sorted(entry["symbol"] for entry in entries if entry["status"] == "shortable")
    )
    shortable_or_locate_symbols = tuple(
        sorted(
            entry["symbol"]
            for entry in entries
            if entry["status"] in {"shortable", "locate_required"}
        )
    )

    shortable_path = instruments_dir / "shortable.txt"
    shortable_or_locate_path = instruments_dir / "shortable_or_locate.txt"
    snapshot_path = meta_dir / f"shortability_snapshot_{as_of_date}.json"
    latest_snapshot_path = meta_dir / "shortability_latest.json"

    _write_symbol_list(shortable_path, shortable_symbols)
    _write_symbol_list(shortable_or_locate_path, shortable_or_locate_symbols)
    serialized_snapshot = json.dumps(snapshot_payload, indent=2, sort_keys=True) + "\n"
    snapshot_path.write_text(serialized_snapshot, encoding="utf-8")
    latest_snapshot_path.write_text(serialized_snapshot, encoding="utf-8")

    return asdict(
        ShortabilityPersistenceResult(
            as_of_date=as_of_date,
            shortable_count=len(shortable_symbols),
            shortable_or_locate_count=len(shortable_or_locate_symbols),
            shortable_path=str(shortable_path),
            shortable_or_locate_path=str(shortable_or_locate_path),
            snapshot_path=str(snapshot_path),
            latest_snapshot_path=str(latest_snapshot_path),
        )
    )


@dataclass(slots=True)
class _PendingShortabilityRequest:
    req_id: int
    symbol: str
    exchange: str
    primary_exchange: str
    currency: str
    security_type: str
    started_at: float
    market_data_type: str | None = None
    shortable_value: Decimal | None = None
    shortable_shares: Decimal | None = None
    first_data_at: float | None = None
    errors: list[dict[str, Any]] | None = None
    completed_reason: str | None = None
    contract_queries: tuple[ContractResolveQuery, ...] = ()
    attempt_index: int = 0

    def __post_init__(self) -> None:
        if self.errors is None:
            self.errors = []

    @property
    def current_contract_query(self) -> ContractResolveQuery:
        return self.contract_queries[self.attempt_index]

    def can_retry_contract(self) -> bool:
        return self.attempt_index + 1 < len(self.contract_queries)

    def move_to_next_contract(self) -> None:
        self.attempt_index += 1
        self.shortable_value = None
        self.shortable_shares = None
        self.first_data_at = None
        self.completed_reason = None
        self.errors = []
        self.market_data_type = None
        self.started_at = monotonic()


class _ShortabilitySnapshotApp:
    def __init__(self, *, timeout: int = 10) -> None:
        eclient_cls, ewrapper_cls, contract_cls = _load_shortability_runtime()

        class ShortabilityRuntime(ewrapper_cls, eclient_cls):
            def __init__(self, outer: "_ShortabilitySnapshotApp") -> None:
                eclient_cls.__init__(self, self)
                self._outer = outer

            def connectAck(self) -> None:  # noqa: N802
                self._outer.on_connect_ack()

            def nextValidId(self, orderId: int) -> None:  # noqa: N802
                self._outer.on_next_valid_id(orderId)

            def error(  # noqa: N802
                self,
                reqId: int,
                errorTime: int,
                errorCode: int,
                errorString: str,
                advancedOrderRejectJson: str = "",
            ) -> None:
                self._outer.on_error(
                    req_id=reqId,
                    error_time=errorTime,
                    error_code=errorCode,
                    error_string=errorString,
                    advanced_order_reject_json=advancedOrderRejectJson,
                )

            def tickGeneric(self, reqId: int, tickType: int, value: float) -> None:  # noqa: N802
                self._outer.on_tick_generic(req_id=reqId, tick_type=tickType, value=value)

            def tickSize(self, reqId: int, tickType: int, size: Decimal) -> None:  # noqa: N802
                self._outer.on_tick_size(req_id=reqId, tick_type=tickType, size=size)

            def marketDataType(self, reqId: int, marketDataType: int) -> None:  # noqa: N802
                self._outer.on_market_data_type(req_id=reqId, market_data_type=marketDataType)

        self.timeout = timeout
        self.contract_cls = contract_cls
        self.client = ShortabilityRuntime(self)
        self._thread: Thread | None = None
        self._connected_event = Event()
        self._request_id_lock = Lock()
        self._next_request_id: int = 1
        self._requests: dict[int, _PendingShortabilityRequest] = {}
        self._requests_lock = Lock()
        self.global_errors: list[dict[str, Any]] = []

    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool:
        self.client.connect(host, port, client_id)
        self._thread = Thread(target=self.client.run, name="ibkr-shortability", daemon=True)
        self._thread.start()
        connected = self._connected_event.wait(timeout=self.timeout)
        if not connected:
            self.disconnect_and_stop()
        return connected

    def disconnect_and_stop(self) -> None:
        if self.client.isConnected():
            self.client.disconnect()
        if self._thread is not None:
            self._thread.join(timeout=2)

    def on_connect_ack(self) -> None:
        self._connected_event.set()

    def on_next_valid_id(self, order_id: int) -> None:
        self._connected_event.set()

    def next_request_id(self) -> int:
        with self._request_id_lock:
            request_id = self._next_request_id
            self._next_request_id += 1
        return request_id

    def set_market_data_type(self, market_data_type: ShortabilityMarketDataType) -> None:
        self.client.reqMarketDataType(MARKET_DATA_TYPE_CODES[market_data_type])

    def start_request(self, request: _PendingShortabilityRequest, contract: Any) -> None:
        with self._requests_lock:
            self._requests[request.req_id] = request
        self.client.reqMktData(
            request.req_id,
            contract,
            str(GENERIC_TICK_SHORTABLE),
            False,
            False,
            [],
        )

    def cancel_request(self, req_id: int) -> _PendingShortabilityRequest:
        if self.client.isConnected() and self.client.serverVersion() is not None:
            self.client.cancelMktData(req_id)
        with self._requests_lock:
            request = self._requests.pop(req_id)
        return request

    def on_error(
        self,
        *,
        req_id: int,
        error_time: int,
        error_code: int,
        error_string: str,
        advanced_order_reject_json: str,
    ) -> None:
        payload = {
            "req_id": req_id,
            "error_time": error_time,
            "error_code": error_code,
            "error_string": error_string,
            "advanced_order_reject_json": advanced_order_reject_json or None,
        }
        if req_id < 0:
            if error_code not in GLOBAL_IBKR_MESSAGE_CODES:
                self.global_errors.append(payload)
            return

        with self._requests_lock:
            request = self._requests.get(req_id)
            if request is None:
                return
            request.errors.append(payload)
            if error_code not in GLOBAL_IBKR_MESSAGE_CODES:
                request.completed_reason = "error"

    def on_tick_generic(self, *, req_id: int, tick_type: int, value: float) -> None:
        if tick_type != TICK_TYPE_SHORTABLE:
            return

        with self._requests_lock:
            request = self._requests.get(req_id)
            if request is None:
                return
            request.shortable_value = _coerce_decimal(value)
            if request.first_data_at is None:
                request.first_data_at = monotonic()
            request.completed_reason = "shortable_value"

    def on_tick_size(self, *, req_id: int, tick_type: int, size: Decimal) -> None:
        if tick_type != TICK_TYPE_SHORTABLE_SHARES:
            return

        with self._requests_lock:
            request = self._requests.get(req_id)
            if request is None:
                return
            request.shortable_shares = _coerce_decimal(size)
            if request.first_data_at is None:
                request.first_data_at = monotonic()
            if request.completed_reason is None:
                request.completed_reason = "shortable_shares"

    def on_market_data_type(self, *, req_id: int, market_data_type: int) -> None:
        with self._requests_lock:
            request = self._requests.get(req_id)
            if request is None:
                return
            request.market_data_type = str(market_data_type)


def _build_shortability_snapshot_from_official_rows(
    query: ShortabilitySnapshotQuery,
    *,
    all_symbols: tuple[str, ...],
    universe_source: str,
    universe_as_of_date: date | None,
    shortable_rows: tuple[OfficialIbkrShortableRow, ...],
    source_updated_text: str | None,
) -> dict[str, Any]:
    shortable_by_symbol = {
        row.normalized_symbol: row
        for row in shortable_rows
    }

    completed_entries = tuple(
        ShortabilityEntry(
            symbol=symbol,
            exchange=query.exchange,
            primary_exchange=query.primary_exchange,
            currency=(
                shortable_by_symbol[symbol].currency
                if symbol in shortable_by_symbol
                else query.currency
            ),
            security_type=query.security_type,
            status=(
                ShortabilityStatus.SHORTABLE
                if symbol in shortable_by_symbol
                else ShortabilityStatus.NOT_SHORTABLE
            ),
            source_symbol=(
                shortable_by_symbol[symbol].symbol
                if symbol in shortable_by_symbol
                else None
            ),
            long_name=(
                shortable_by_symbol[symbol].long_name
                if symbol in shortable_by_symbol
                else None
            ),
            broker_conid=(
                shortable_by_symbol[symbol].broker_conid
                if symbol in shortable_by_symbol
                else None
            ),
            completed_reason="official_ibkr_page",
        )
        for symbol in all_symbols
    )
    filtered_entries = _filter_shortable_entries(
        completed_entries,
        only_shortable=query.only_shortable,
    )
    snapshot = ShortabilitySnapshot(
        snapshot_at=datetime.now(tz=timezone.utc),
        source=query.source.value,
        source_url=OFFICIAL_IBKR_SHORTABLE_STOCKHOLM_URL,
        source_updated_text=source_updated_text,
        market_data_type=query.market_data_type.value,
        universe_source=universe_source,
        universe_as_of_date=universe_as_of_date,
        requested_symbol_count=len(all_symbols),
        evaluated_symbol_count=len(completed_entries),
        returned_symbol_count=len(filtered_entries),
        only_shortable=query.only_shortable,
        status_counts=_count_entry_statuses(completed_entries),
        global_errors=(),
        entries=filtered_entries,
        evaluated_entries=completed_entries,
    )
    return serialize_shortability_snapshot(snapshot)


def _build_contract_attempt_queries(
    query: ShortabilitySnapshotQuery,
    symbol: str,
    *,
    identity: StockholmInstrumentIdentity | None = None,
) -> tuple[ContractResolveQuery, ...]:
    raw_attempts: list[ContractResolveQuery] = [
        ContractResolveQuery(
            symbol=symbol,
            security_type=query.security_type,
            exchange=query.exchange,
            currency=query.currency,
            primary_exchange=query.primary_exchange,
        )
    ]

    if identity is not None and identity.isin:
        raw_attempts.extend(
            [
                ContractResolveQuery(
                    symbol=symbol,
                    security_type=query.security_type,
                    exchange=query.exchange,
                    currency=query.currency,
                    primary_exchange=query.primary_exchange,
                    isin=identity.isin,
                ),
                ContractResolveQuery(
                    symbol=symbol,
                    security_type=query.security_type,
                    exchange=query.exchange,
                    currency=query.currency,
                    primary_exchange=query.primary_exchange,
                    local_symbol=identity.ticker_alias,
                    isin=identity.isin,
                ),
            ]
        )
        if identity.ticker_alias:
            raw_attempts.append(
                ContractResolveQuery(
                    symbol=identity.ticker_alias,
                    security_type=query.security_type,
                    exchange=query.exchange,
                    currency=query.currency,
                    primary_exchange=query.primary_exchange,
                    local_symbol=identity.ticker_alias,
                    isin=identity.isin,
                )
            )

    if "-" in symbol:
        root, suffix = symbol.split("-", 1)
        local_symbol = f"{root} {suffix}"
        raw_attempts.extend(
            [
                ContractResolveQuery(
                    symbol=root,
                    security_type=query.security_type,
                    exchange=query.exchange,
                    currency=query.currency,
                    primary_exchange=query.primary_exchange,
                    local_symbol=local_symbol,
                ),
                ContractResolveQuery(
                    symbol=local_symbol,
                    security_type=query.security_type,
                    exchange=query.exchange,
                    currency=query.currency,
                    primary_exchange=query.primary_exchange,
                ),
                ContractResolveQuery(
                    symbol=f"{root}{suffix}",
                    security_type=query.security_type,
                    exchange=query.exchange,
                    currency=query.currency,
                    primary_exchange=query.primary_exchange,
                ),
            ]
        )

    unique_attempts: list[ContractResolveQuery] = []
    seen_keys: set[tuple[str, str | None, str | None]] = set()
    for candidate in raw_attempts:
        key = (candidate.symbol, candidate.local_symbol, candidate.isin)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        unique_attempts.append(candidate)
    return tuple(unique_attempts)


def _build_shortability_contract(
    contract_query: ContractResolveQuery,
    *,
    contract_cls: type[Any] | None = None,
) -> Any:
    return build_ibkr_contract(
        contract_query,
        contract_cls=contract_cls,
    )


def _finalize_request(request: _PendingShortabilityRequest) -> ShortabilityEntry:
    return ShortabilityEntry(
        symbol=request.symbol,
        exchange=request.exchange,
        primary_exchange=request.primary_exchange,
        currency=request.currency,
        security_type=request.security_type,
        status=_classify_request_status(request),
        shortable_value=request.shortable_value,
        shortable_shares=request.shortable_shares,
        market_data_type=request.market_data_type,
        errors=tuple(request.errors),
        completed_reason=request.completed_reason or "timeout",
    )


def _collect_shortability_snapshot_from_broker_ticks(
    config: IbkrConnectionConfig,
    query: ShortabilitySnapshotQuery,
    *,
    instruments_path: Path,
    identity_path: Path | None = None,
    timeout: int = 120,
    app_cls: type[_ShortabilitySnapshotApp] | None = None,
) -> dict[str, Any]:
    query.validate()
    universe_as_of_date = query.as_of_date
    if query.symbols is not None:
        all_symbols = tuple(_normalize_symbol(symbol) for symbol in query.symbols)
    else:
        all_symbols, universe_as_of_date = load_stockholm_symbols_from_instruments_file(
            instruments_path,
            as_of_date=query.as_of_date,
            max_symbols=query.max_symbols,
        )
    if query.max_symbols is not None and query.symbols is not None:
        all_symbols = all_symbols[: query.max_symbols]
    identity_map = (
        load_stockholm_identity_map(identity_path, symbols=all_symbols)
        if identity_path is not None
        else {}
    )

    runtime_app_cls = app_cls or _ShortabilitySnapshotApp
    app = runtime_app_cls(timeout=timeout)
    if not app.connect_and_start(
        host=config.host,
        port=config.port,
        client_id=config.client_id,
    ):
        raise ConnectionError(
            f"Failed to connect to IBKR at {config.host}:{config.port} "
            f"with client_id={config.client_id}."
        )

    try:
        app.set_market_data_type(query.market_data_type)
        pending_symbols = list(all_symbols)
        active_requests: dict[int, _PendingShortabilityRequest] = {}
        completed_entries: list[ShortabilityEntry] = []

        while pending_symbols or active_requests:
            if not app.client.isConnected() or app.client.serverVersion() is None:
                raise ConnectionError(
                    "IBKR Gateway disconnected during shortability snapshot collection."
                )
            while pending_symbols and len(active_requests) < query.max_concurrent:
                symbol = pending_symbols.pop(0)
                req_id = app.next_request_id()
                request = _PendingShortabilityRequest(
                    req_id=req_id,
                    symbol=symbol,
                    exchange=query.exchange,
                    primary_exchange=query.primary_exchange,
                    currency=query.currency,
                    security_type=query.security_type,
                    started_at=monotonic(),
                    contract_queries=_build_contract_attempt_queries(
                        query,
                        symbol,
                        identity=identity_map.get(symbol),
                    ),
                )
                contract = _build_shortability_contract(
                    request.current_contract_query,
                    contract_cls=app.contract_cls,
                )
                app.start_request(request, contract)
                active_requests[req_id] = request

            now = monotonic()
            for req_id, request in list(active_requests.items()):
                elapsed = now - request.started_at
                last_error_code = (
                    request.errors[-1]["error_code"] if request.errors else None
                )
                can_retry_contract = (
                    request.completed_reason == "error"
                    and last_error_code == 200
                    and request.can_retry_contract()
                )
                if can_retry_contract:
                    request = app.cancel_request(req_id)
                    request.move_to_next_contract()
                    next_req_id = app.next_request_id()
                    request.req_id = next_req_id
                    contract = _build_shortability_contract(
                        request.current_contract_query,
                        contract_cls=app.contract_cls,
                    )
                    app.start_request(request, contract)
                    active_requests.pop(req_id, None)
                    active_requests[next_req_id] = request
                    continue

                ready_after_data = (
                    request.first_data_at is not None
                    and now - request.first_data_at >= DEFAULT_POST_DATA_GRACE_SECONDS
                )
                has_terminal_error = request.completed_reason == "error"
                timed_out = elapsed >= query.per_symbol_timeout_seconds
                if not (ready_after_data or has_terminal_error or timed_out):
                    continue

                request = app.cancel_request(req_id)
                completed_entries.append(_finalize_request(request))
                active_requests.pop(req_id, None)

            sleep(0.05)
    finally:
        app.disconnect_and_stop()

    all_entries = tuple(sorted(completed_entries, key=lambda item: item.symbol))
    filtered_entries = _filter_shortable_entries(all_entries, only_shortable=query.only_shortable)
    snapshot = ShortabilitySnapshot(
        snapshot_at=datetime.now(tz=timezone.utc),
        source=query.source.value,
        source_url=None,
        source_updated_text=None,
        market_data_type=query.market_data_type.value,
        universe_source=(
            "request.symbols"
            if query.symbols is not None
            else str(instruments_path)
        ),
        universe_as_of_date=universe_as_of_date,
        requested_symbol_count=len(all_symbols),
        evaluated_symbol_count=len(all_entries),
        returned_symbol_count=len(filtered_entries),
        only_shortable=query.only_shortable,
        status_counts=_count_entry_statuses(all_entries),
        global_errors=tuple(app.global_errors),
        entries=filtered_entries,
        evaluated_entries=all_entries,
    )
    return serialize_shortability_snapshot(snapshot)


def collect_shortability_snapshot(
    config: IbkrConnectionConfig,
    query: ShortabilitySnapshotQuery,
    *,
    instruments_path: Path,
    identity_path: Path | None = None,
    timeout: int = 120,
    app_cls: type[_ShortabilitySnapshotApp] | None = None,
) -> dict[str, Any]:
    query.validate()
    if query.source == ShortabilitySource.BROKER_TICKS:
        return _collect_shortability_snapshot_from_broker_ticks(
            config,
            query,
            instruments_path=instruments_path,
            identity_path=identity_path,
            timeout=timeout,
            app_cls=app_cls,
        )

    universe_as_of_date = query.as_of_date
    if query.symbols is not None:
        all_symbols = tuple(_normalize_symbol(symbol) for symbol in query.symbols)
        universe_source = "request.symbols"
    else:
        all_symbols, universe_as_of_date = load_stockholm_symbols_from_instruments_file(
            instruments_path,
            as_of_date=query.as_of_date,
            max_symbols=query.max_symbols,
        )
        universe_source = str(instruments_path)

    if query.max_symbols is not None and query.symbols is not None:
        all_symbols = all_symbols[: query.max_symbols]

    source_updated_text, shortable_rows = fetch_official_ibkr_shortable_rows()
    return _build_shortability_snapshot_from_official_rows(
        query,
        all_symbols=all_symbols,
        universe_source=universe_source,
        universe_as_of_date=universe_as_of_date,
        shortable_rows=shortable_rows,
        source_updated_text=source_updated_text,
    )
