from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from decimal import InvalidOperation
from threading import Event
from threading import Lock
from threading import RLock
from threading import Thread
from threading import current_thread
from typing import Any

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.domain.contract_resolution import ContractResolveQuery
from ibkr_trader.ibkr.contracts import build_ibkr_contract
from ibkr_trader.ibkr.errors import IbkrDependencyError


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


class LiveMarketDataStreamService:
    """Persistent top-of-book/last-price stream for live RL observations."""

    def __init__(
        self,
        config: IbkrConnectionConfig,
        *,
        timeout: int = 10,
        max_bars_per_symbol: int = 780,
        initial_connect_backoff_seconds: float = 5.0,
        max_connect_backoff_seconds: float = 300.0,
    ) -> None:
        self._config = config
        self._timeout = timeout
        self._max_bars_per_symbol = max_bars_per_symbol
        self._initial_connect_backoff_seconds = max(0.0, initial_connect_backoff_seconds)
        self._max_connect_backoff_seconds = max(
            self._initial_connect_backoff_seconds,
            max_connect_backoff_seconds,
        )
        self._lock = RLock()
        self._request_id_lock = Lock()
        self._connected_event = Event()
        self._next_request_id = 10_000
        self._client: Any | None = None
        self._thread: Thread | None = None
        self._contract_cls: type[Any] | None = None
        self._desired_contracts_by_key: dict[str, MarketStreamContract] = {}
        self._desired_market_data_type: str | None = None
        self._subscriptions_by_key: dict[str, MarketStreamSubscription] = {}
        self._subscription_keys_by_req_id: dict[int, str] = {}
        self._quotes_by_key: dict[str, MarketStreamQuote] = {}
        self._bars_by_key: dict[str, dict[datetime, MarketStreamBar]] = {}
        self._errors: list[dict[str, Any]] = []
        self._started_at: datetime | None = None
        self._connect_attempt_count = 0
        self._connect_success_count = 0
        self._consecutive_failures = 0
        self._last_connect_attempt_at: datetime | None = None
        self._last_connect_success_at: datetime | None = None
        self._last_disconnect_observed_at: datetime | None = None
        self._disconnect_failure_recorded_for_success_at: datetime | None = None
        self._last_error: str | None = None
        self._cooldown_until: datetime | None = None
        self._supervisor_stop_event = Event()
        self._supervisor_thread: Thread | None = None

    def _record_connect_attempt_locked(self) -> None:
        self._connect_attempt_count += 1
        self._last_connect_attempt_at = _utc_now()

    def _record_connect_success_locked(self) -> None:
        self._connect_success_count += 1
        self._last_connect_success_at = _utc_now()
        self._disconnect_failure_recorded_for_success_at = None
        self._consecutive_failures = 0
        self._last_error = None
        self._cooldown_until = None

    def _record_connect_failure_locked(self, error: str) -> None:
        self._consecutive_failures += 1
        self._last_error = error
        if self._initial_connect_backoff_seconds <= 0:
            self._cooldown_until = None
            return
        delay = min(
            self._max_connect_backoff_seconds,
            self._initial_connect_backoff_seconds
            * (2 ** max(0, self._consecutive_failures - 1)),
        )
        self._cooldown_until = _utc_now() + timedelta(seconds=delay)

    def _cooldown_seconds_remaining_locked(self) -> int | None:
        if self._cooldown_until is None:
            return None
        remaining = int((self._cooldown_until - _utc_now()).total_seconds())
        return max(0, remaining)

    def _raise_if_cooling_down_locked(self) -> None:
        if self._cooldown_until is None:
            return
        if self._cooldown_until <= _utc_now():
            return
        raise ConnectionError(
            "market stream connection is cooling down after "
            f"{self._consecutive_failures} failed broker attempt(s); next retry at "
            f"{self._cooldown_until.isoformat()}. Last error: {self._last_error}"
        )

    def _record_unexpected_disconnect_locked(self) -> None:
        if self._last_connect_success_at is None:
            return
        if (
            self._disconnect_failure_recorded_for_success_at
            == self._last_connect_success_at
        ):
            return
        self._last_disconnect_observed_at = _utc_now()
        self._disconnect_failure_recorded_for_success_at = self._last_connect_success_at
        self._record_connect_failure_locked(
            "market stream disconnected after a successful broker connection"
        )

    def connect_and_start(self) -> None:
        with self._lock:
            if self._client is not None and self._client.isConnected():
                return
            self._raise_if_cooling_down_locked()
            self._record_connect_attempt_locked()
            self._clear_active_subscriptions_locked()
            eclient_cls, ewrapper_cls, contract_cls = _load_market_data_runtime()
            self._contract_cls = contract_cls

            class MarketDataRuntime(ewrapper_cls, eclient_cls):
                def __init__(self, outer: "LiveMarketDataStreamService") -> None:
                    eclient_cls.__init__(self, self)
                    self._outer = outer

                def connectAck(self) -> None:  # noqa: N802
                    self._outer._on_connected()

                def nextValidId(self, orderId: int) -> None:  # noqa: N802
                    self._outer._on_connected()

                def error(  # noqa: N802
                    self,
                    reqId: int,
                    *args: Any,
                ) -> None:
                    error_time, error_code, error_string, advanced_json = (
                        _normalize_ib_error_args(args)
                    )
                    self._outer._on_error(
                        req_id=reqId,
                        error_time=error_time,
                        error_code=error_code,
                        error_string=error_string,
                        advanced_order_reject_json=advanced_json,
                    )

                def marketDataType(self, reqId: int, marketDataType: int) -> None:  # noqa: N802
                    self._outer._on_market_data_type(
                        req_id=reqId,
                        market_data_type=marketDataType,
                    )

                def tickPrice(self, reqId: int, tickType: int, price: float, attrib: Any) -> None:  # noqa: N802
                    _ = attrib
                    self._outer._on_tick_price(
                        req_id=reqId,
                        tick_type=tickType,
                        price=price,
                    )

                def tickSize(self, reqId: int, tickType: int, size: Decimal) -> None:  # noqa: N802
                    self._outer._on_tick_size(
                        req_id=reqId,
                        tick_type=tickType,
                        size=size,
                    )

            self._connected_event.clear()
            self._client = MarketDataRuntime(self)
            try:
                self._client.connect(
                    self._config.host,
                    self._config.port,
                    self._config.client_id,
                )
            except Exception as exc:
                error = str(exc)
                self._record_connect_failure_locked(error)
                self._client = None
                raise
            self._thread = Thread(
                target=self._client.run,
                name="ibkr-live-market-stream",
                daemon=True,
            )
            self._thread.start()
            self._started_at = _utc_now()

        if not self._connected_event.wait(timeout=self._timeout):
            error = (
                f"Failed to connect to IBKR at {self._config.host}:{self._config.port} "
                f"with client_id={self._config.client_id}."
            )
            with self._lock:
                self._record_connect_failure_locked(error)
            self._disconnect_client()
            raise ConnectionError(error)
        with self._lock:
            self._record_connect_success_locked()

    def start_auto_reconnect(self, *, interval_seconds: float = 15.0) -> None:
        interval_seconds = max(1.0, interval_seconds)
        with self._lock:
            if (
                self._supervisor_thread is not None
                and self._supervisor_thread.is_alive()
            ):
                return
            self._supervisor_stop_event.clear()
            thread = Thread(
                target=self._run_supervisor,
                kwargs={"interval_seconds": interval_seconds},
                name="ibkr-market-stream-supervisor",
                daemon=True,
            )
            self._supervisor_thread = thread
            thread.start()

    def stop(self, *, clear_desired: bool = True) -> None:
        self._supervisor_stop_event.set()
        with self._lock:
            supervisor_thread = self._supervisor_thread
            if clear_desired:
                self._desired_contracts_by_key.clear()
                self._desired_market_data_type = None
        if (
            supervisor_thread is not None
            and supervisor_thread.is_alive()
            and supervisor_thread is not current_thread()
        ):
            supervisor_thread.join(timeout=5)
        with self._lock:
            self._supervisor_thread = None
        self._disconnect_client()

    def _disconnect_client(self) -> None:
        with self._lock:
            client = self._client
            request_ids = [
                subscription.request_id
                for subscription in self._subscriptions_by_key.values()
            ]
            self._clear_active_subscriptions_locked()
            thread = self._thread
            self._client = None
            self._thread = None
            self._connected_event.clear()
        if client is not None and client.isConnected():
            for request_id in request_ids:
                try:
                    client.cancelMktData(request_id)
                except Exception:
                    pass
            client.disconnect()
        if thread is not None and thread is not current_thread():
            thread.join(timeout=2)

    def _clear_active_subscriptions_locked(self) -> None:
        self._subscriptions_by_key.clear()
        self._subscription_keys_by_req_id.clear()

    def _run_supervisor(self, *, interval_seconds: float) -> None:
        while not self._supervisor_stop_event.is_set():
            with self._lock:
                has_desired_contracts = bool(self._desired_contracts_by_key)
                connected = self._client is not None and self._client.isConnected()
                if has_desired_contracts and not connected:
                    self._record_unexpected_disconnect_locked()
            if has_desired_contracts and not connected:
                try:
                    self._restore_desired_subscriptions()
                except Exception:
                    pass
            self._supervisor_stop_event.wait(interval_seconds)

    def _restore_desired_subscriptions(self) -> dict[str, Any]:
        with self._lock:
            contracts = list(self._desired_contracts_by_key.values())
            market_data_type = self._desired_market_data_type
        if not contracts:
            return self.snapshot()
        self.connect_and_start()
        with self._lock:
            if market_data_type is not None and self._client is not None:
                self._client.reqMarketDataType(_market_data_type_code(market_data_type))
            for contract in contracts:
                existing = self._subscriptions_by_key.get(contract.key)
                if existing is not None and (
                    existing.status != "error"
                    and existing.contract == contract
                ):
                    continue
                if existing is not None:
                    self._unsubscribe_key_locked(contract.key)
                self._subscribe_locked(contract)
        return self.snapshot()

    def subscribe_many(
        self,
        contracts: list[MarketStreamContract],
        *,
        replace: bool = False,
        market_data_type: str | None = None,
    ) -> dict[str, Any]:
        if not contracts:
            raise ValueError("symbols are required")
        for contract in contracts:
            contract.validate()
        with self._lock:
            if replace:
                self._desired_contracts_by_key = {
                    contract.key: contract for contract in contracts
                }
            else:
                for contract in contracts:
                    self._desired_contracts_by_key[contract.key] = contract
            if market_data_type is not None:
                self._desired_market_data_type = market_data_type
        self.connect_and_start()
        with self._lock:
            client = self._client
            if client is None:
                raise ConnectionError("market stream client is not connected")
            if market_data_type is not None:
                client.reqMarketDataType(_market_data_type_code(market_data_type))

            target_keys = {contract.key for contract in contracts}
            if replace:
                for key in list(self._subscriptions_by_key):
                    if key not in target_keys:
                        self._unsubscribe_key_locked(key)

            for contract in contracts:
                existing = self._subscriptions_by_key.get(contract.key)
                if existing is not None and (
                    existing.status != "error"
                    and existing.contract == contract
                ):
                    continue
                if existing is not None:
                    self._unsubscribe_key_locked(contract.key)
                self._subscribe_locked(contract)
        return self.snapshot()

    def snapshot(
        self,
        *,
        symbols: list[str] | None = None,
        bar_limit: int = 390,
    ) -> dict[str, Any]:
        requested = {symbol.upper() for symbol in symbols or []}
        with self._lock:
            keys = (
                sorted(requested)
                if requested
                else sorted(self._subscriptions_by_key.keys() | self._quotes_by_key.keys())
            )
            subscriptions = [
                _serialize_subscription(self._subscriptions_by_key[key])
                for key in keys
                if key in self._subscriptions_by_key
            ]
            quotes = [
                _serialize_quote(self._quotes_by_key[key])
                for key in keys
                if key in self._quotes_by_key
            ]
            bars_by_symbol = {}
            for key in keys:
                quote = self._quotes_by_key.get(key)
                currency = quote.currency if quote is not None else None
                bars_by_symbol[key] = [
                    _serialize_bar(bar, currency=currency)
                    for _, bar in sorted(self._bars_by_key.get(key, {}).items())[
                        -bar_limit:
                    ]
                ]
            connected = self._client is not None and self._client.isConnected()
            return {
                "running": connected,
                "started_at": (
                    self._started_at.isoformat() if self._started_at is not None else None
                ),
                "last_error": self._last_error,
                "consecutive_failures": self._consecutive_failures,
                "cooldown_until": (
                    self._cooldown_until.isoformat()
                    if self._cooldown_until is not None
                    else None
                ),
                "cooldown_seconds_remaining": self._cooldown_seconds_remaining_locked(),
                "connect_attempt_count": self._connect_attempt_count,
                "connect_success_count": self._connect_success_count,
                "last_connect_attempt_at": (
                    self._last_connect_attempt_at.isoformat()
                    if self._last_connect_attempt_at is not None
                    else None
                ),
                "last_connect_success_at": (
                    self._last_connect_success_at.isoformat()
                    if self._last_connect_success_at is not None
                    else None
                ),
                "last_disconnect_observed_at": (
                    self._last_disconnect_observed_at.isoformat()
                    if self._last_disconnect_observed_at is not None
                    else None
                ),
                "desired_subscription_count": len(self._desired_contracts_by_key),
                "desired_symbols": sorted(self._desired_contracts_by_key),
                "subscribed_count": len(self._subscriptions_by_key),
                "subscriptions": subscriptions,
                "quote_count": len(quotes),
                "quotes": quotes,
                "bars_by_symbol": bars_by_symbol,
                "errors": list(self._errors[-50:]),
            }

    def _allocate_request_id(self) -> int:
        with self._request_id_lock:
            request_id = self._next_request_id
            self._next_request_id += 1
            return request_id

    def _subscribe_locked(self, contract: MarketStreamContract) -> None:
        client = self._client
        if client is None:
            raise ConnectionError("market stream client is not connected")
        if self._contract_cls is None:
            raise ConnectionError("market stream contract runtime is not loaded")
        request_id = self._allocate_request_id()
        ib_contract = build_ibkr_contract(
            ContractResolveQuery(
                symbol=contract.symbol,
                security_type=contract.security_type,
                exchange=contract.exchange,
                currency=contract.currency,
                primary_exchange=contract.primary_exchange,
                local_symbol=contract.local_symbol,
                isin=contract.isin,
            ),
            contract_cls=self._contract_cls,
        )
        self._subscriptions_by_key[contract.key] = MarketStreamSubscription(
            request_id=request_id,
            contract=contract,
            subscribed_at=_utc_now(),
        )
        self._subscription_keys_by_req_id[request_id] = contract.key
        self._quotes_by_key.setdefault(
            contract.key,
            MarketStreamQuote(
                symbol=contract.key,
                exchange=contract.exchange,
                currency=contract.currency,
                security_type=contract.security_type,
                primary_exchange=contract.primary_exchange,
            ),
        )
        client.reqMktData(request_id, ib_contract, "", False, False, [])

    def _unsubscribe_key_locked(self, key: str) -> None:
        subscription = self._subscriptions_by_key.pop(key, None)
        if subscription is None:
            return
        self._subscription_keys_by_req_id.pop(subscription.request_id, None)
        client = self._client
        if client is not None and client.isConnected():
            client.cancelMktData(subscription.request_id)

    def _on_connected(self) -> None:
        self._connected_event.set()

    def _on_error(
        self,
        *,
        req_id: int,
        error_time: int | None,
        error_code: int,
        error_string: str,
        advanced_order_reject_json: str,
    ) -> None:
        with self._lock:
            key = self._subscription_keys_by_req_id.get(req_id)
            payload = {
                "req_id": req_id,
                "symbol": key,
                "error_time": error_time,
                "error_code": error_code,
                "error_string": error_string,
                "advanced_order_reject_json": advanced_order_reject_json or None,
                "observed_at": _utc_now().isoformat(),
            }
            self._errors.append(payload)
            if key is not None and key in self._subscriptions_by_key:
                self._subscriptions_by_key[key].status = "error"
                self._subscriptions_by_key[key].last_error = (
                    f"[{error_code}] {error_string}"
                )

    def _on_market_data_type(self, *, req_id: int, market_data_type: int) -> None:
        with self._lock:
            key = self._subscription_keys_by_req_id.get(req_id)
            if key is None:
                return
            if key in self._subscriptions_by_key:
                self._subscriptions_by_key[key].market_data_type = market_data_type
            if key in self._quotes_by_key:
                self._quotes_by_key[key].market_data_type = market_data_type

    def _on_tick_price(self, *, req_id: int, tick_type: int, price: Any) -> None:
        decimal_price = _parse_decimal(price)
        if decimal_price is None:
            return
        observed_at = _utc_now()
        with self._lock:
            key = self._subscription_keys_by_req_id.get(req_id)
            if key is None:
                return
            quote = self._quotes_by_key.get(key)
            if quote is None:
                return
            quote.updated_at = observed_at
            if tick_type in BID_PRICE_TICKS:
                quote.bid_price = decimal_price
            elif tick_type in ASK_PRICE_TICKS:
                quote.ask_price = decimal_price
            elif tick_type in LAST_PRICE_TICKS:
                quote.last_price = decimal_price
                quote.last_trade_at = observed_at
                self._record_trade_bar_locked(key, decimal_price, observed_at)
            elif tick_type in CLOSE_PRICE_TICKS:
                quote.close_price = decimal_price

    def _on_tick_size(self, *, req_id: int, tick_type: int, size: Any) -> None:
        decimal_size = _parse_decimal(size)
        if decimal_size is None:
            return
        observed_at = _utc_now()
        with self._lock:
            key = self._subscription_keys_by_req_id.get(req_id)
            if key is None:
                return
            quote = self._quotes_by_key.get(key)
            if quote is None:
                return
            quote.updated_at = observed_at
            if tick_type in BID_SIZE_TICKS:
                quote.bid_size = decimal_size
            elif tick_type in ASK_SIZE_TICKS:
                quote.ask_size = decimal_size
            elif tick_type in LAST_SIZE_TICKS:
                quote.last_size = decimal_size

    def _record_trade_bar_locked(
        self,
        key: str,
        price: Decimal,
        observed_at: datetime,
    ) -> None:
        bucket = _minute_start(observed_at)
        bars = self._bars_by_key.setdefault(key, {})
        bar = bars.get(bucket)
        if bar is None:
            bars[bucket] = MarketStreamBar(
                started_at=bucket,
                open=price,
                high=price,
                low=price,
                close=price,
                bar_count=1,
            )
        else:
            bar.update(price)
        if len(bars) > self._max_bars_per_symbol:
            for old_key in sorted(bars)[: len(bars) - self._max_bars_per_symbol]:
                del bars[old_key]


def _market_data_type_code(value: str) -> int:
    normalized = value.strip().upper().replace("-", "_").replace(" ", "_")
    try:
        return MARKET_DATA_TYPE_CODES[normalized]
    except KeyError as exc:
        raise ValueError(
            "market_data_type must be one of LIVE, FROZEN, DELAYED, DELAYED_FROZEN"
        ) from exc


def _serialize_timestamp(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _serialize_quote(quote: MarketStreamQuote) -> dict[str, Any]:
    return {
        "symbol": quote.symbol,
        "exchange": quote.exchange,
        "currency": quote.currency,
        "security_type": quote.security_type,
        "primary_exchange": quote.primary_exchange,
        "bid_price": _serialize_decimal(quote.bid_price),
        "ask_price": _serialize_decimal(quote.ask_price),
        "last_price": _serialize_decimal(quote.last_price),
        "close_price": _serialize_decimal(quote.close_price),
        "bid_size": _serialize_decimal(quote.bid_size),
        "ask_size": _serialize_decimal(quote.ask_size),
        "last_size": _serialize_decimal(quote.last_size),
        "updated_at": _serialize_timestamp(quote.updated_at),
        "last_trade_at": _serialize_timestamp(quote.last_trade_at),
        "market_data_type": quote.market_data_type,
    }


def _serialize_bar(bar: MarketStreamBar, *, currency: str | None) -> dict[str, Any]:
    return {
        "timestamp": bar.started_at.isoformat(),
        "open": str(bar.open),
        "high": str(bar.high),
        "low": str(bar.low),
        "close": str(bar.close),
        "volume": None,
        "bar_count": str(bar.bar_count),
        "currency": currency,
        "source": "ibkr_live_market_stream_1m",
    }


def _serialize_subscription(subscription: MarketStreamSubscription) -> dict[str, Any]:
    return {
        "request_id": subscription.request_id,
        "contract": asdict(subscription.contract),
        "subscribed_at": subscription.subscribed_at.isoformat(),
        "status": subscription.status,
        "last_error": subscription.last_error,
        "market_data_type": subscription.market_data_type,
    }
