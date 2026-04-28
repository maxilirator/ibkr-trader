from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, MutableMapping, Protocol, runtime_checkable

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.domain.contract_resolution import ContractResolveQuery
from ibkr_trader.ibkr.contracts import (
    _extract_broker_error_message,
    build_ibkr_contract,
    serialize_contract_details,
)
from ibkr_trader.ibkr.sync_wrapper import (
    load_response_timeout_class as _load_response_timeout_runtime_class,
)
from ibkr_trader.ibkr.sync_wrapper import load_sync_wrapper_class as _load_sync_wrapper_class


@dataclass(slots=True)
class HistoricalBarsQuery:
    symbol: str
    exchange: str
    currency: str
    duration: str
    bar_size: str
    security_type: str = "STK"
    primary_exchange: str | None = None
    isin: str | None = None
    local_symbol: str | None = None
    what_to_show: str = "TRADES"
    use_rth: bool = True
    end_at: datetime | None = None

    def validate(self) -> None:
        if not self.symbol:
            raise ValueError("symbol is required")
        if not self.exchange:
            raise ValueError("exchange is required")
        if not self.currency:
            raise ValueError("currency is required")
        if not self.duration:
            raise ValueError("duration is required")
        if not self.bar_size:
            raise ValueError("bar_size is required")
        if not self.security_type:
            raise ValueError("security_type is required")
        if not self.what_to_show:
            raise ValueError("what_to_show is required")
        if self.end_at is not None and self.end_at.tzinfo is None:
            raise ValueError("end_at must include timezone information")


ContractDetailsCacheKey = tuple[
    str,
    str,
    str,
    str,
    str | None,
    str | None,
    str | None,
]


@runtime_checkable
class HistoricalBarsSyncWrapperProtocol(Protocol):
    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool: ...

    def disconnect_and_stop(self) -> None: ...

    def get_contract_details(self, contract: Any, timeout: int | None = None) -> list[Any]: ...

    def get_historical_data(
        self,
        contract: Any,
        end_date_time: str,
        duration_str: str,
        bar_size_setting: str,
        what_to_show: str,
        use_rth: bool = True,
        format_date: int = 1,
        timeout: int | None = None,
    ) -> list[Any]: ...


def _load_response_timeout_class() -> type[Exception]:
    return _load_response_timeout_runtime_class()


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, "", -1, -1.0):
        return None

    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _format_end_date_time(end_at: datetime | None) -> str:
    if end_at is None:
        return ""

    # IBKR historical data accepts UTC timestamps as `YYYYMMDD-HH:MM:SS`.
    return end_at.astimezone(timezone.utc).strftime("%Y%m%d-%H:%M:%S")


def _serialize_bar_value(value: Any) -> str | None:
    decimal_value = _to_decimal(value)
    return str(decimal_value) if decimal_value is not None else None


def _serialize_historical_bar(raw_bar: Any, *, currency: str) -> dict[str, Any]:
    return {
        "timestamp": str(getattr(raw_bar, "date", "")),
        "open": _serialize_bar_value(getattr(raw_bar, "open", None)),
        "high": _serialize_bar_value(getattr(raw_bar, "high", None)),
        "low": _serialize_bar_value(getattr(raw_bar, "low", None)),
        "close": _serialize_bar_value(getattr(raw_bar, "close", None)),
        "volume": _serialize_bar_value(getattr(raw_bar, "volume", None)),
        "wap": _serialize_bar_value(getattr(raw_bar, "wap", None)),
        "bar_count": _serialize_bar_value(getattr(raw_bar, "barCount", None)),
        "currency": currency,
    }


def _contract_cache_key(query: HistoricalBarsQuery) -> ContractDetailsCacheKey:
    return (
        query.symbol.upper(),
        query.security_type.upper(),
        query.exchange.upper(),
        query.currency.upper(),
        query.primary_exchange.upper() if query.primary_exchange else None,
        query.local_symbol.upper() if query.local_symbol else None,
        query.isin.upper() if query.isin else None,
    )


def read_historical_bars(
    config: IbkrConnectionConfig,
    query: HistoricalBarsQuery,
    *,
    timeout: int = 20,
    sync_wrapper_cls: type[HistoricalBarsSyncWrapperProtocol] | None = None,
    response_timeout_cls: type[Exception] | None = None,
    contract_cls: type[Any] | None = None,
    app: HistoricalBarsSyncWrapperProtocol | None = None,
    contract_details_cache: MutableMapping[ContractDetailsCacheKey, list[Any]]
    | None = None,
) -> dict[str, Any]:
    query.validate()
    timeout_cls = response_timeout_cls or _load_response_timeout_class()
    runtime_app = app
    owns_connection = runtime_app is None
    if runtime_app is None:
        wrapper_cls = sync_wrapper_cls or _load_sync_wrapper_class()
        runtime_app = wrapper_cls(timeout=timeout)
        if not runtime_app.connect_and_start(
            host=config.host,
            port=config.port,
            client_id=config.client_id,
        ):
            raise ConnectionError(
                f"Failed to connect to IBKR at {config.host}:{config.port} "
                f"with client_id={config.client_id}."
            )

    try:
        try:
            if hasattr(runtime_app, "contract_details"):
                runtime_app.contract_details.clear()
            contract_query = ContractResolveQuery(
                symbol=query.symbol,
                security_type=query.security_type,
                exchange=query.exchange,
                currency=query.currency,
                primary_exchange=query.primary_exchange,
                local_symbol=query.local_symbol,
                isin=query.isin,
            )
            ib_contract = build_ibkr_contract(contract_query, contract_cls=contract_cls)
            cache_key = _contract_cache_key(query)
            if contract_details_cache is not None and cache_key in contract_details_cache:
                raw_matches = contract_details_cache[cache_key]
            else:
                raw_matches = runtime_app.get_contract_details(
                    ib_contract,
                    timeout=timeout,
                )
                if contract_details_cache is not None:
                    contract_details_cache[cache_key] = raw_matches
        except timeout_cls as exc:
            broker_error = _extract_broker_error_message(runtime_app)
            if broker_error is not None:
                raise LookupError(
                    f"IBKR rejected the contract lookup for {query.symbol}: {broker_error}"
                ) from exc
            raise TimeoutError(
                f"Timed out while resolving {query.symbol} on {query.exchange}."
            ) from exc

        if not raw_matches:
            raise LookupError(
                f"No IBKR contract matched {query.symbol} on {query.exchange} {query.currency}."
            )
        if len(raw_matches) > 1:
            raise LookupError(
                f"IBKR contract lookup for {query.symbol} returned multiple matches."
            )

        resolved_detail = serialize_contract_details(raw_matches[0])
        resolved_contract = raw_matches[0].contract

        try:
            raw_bars = runtime_app.get_historical_data(
                resolved_contract,
                _format_end_date_time(query.end_at),
                query.duration,
                query.bar_size,
                query.what_to_show,
                query.use_rth,
                1,
                timeout,
            )
        except timeout_cls as exc:
            broker_error = _extract_broker_error_message(runtime_app)
            if broker_error is not None:
                raise LookupError(
                    f"IBKR rejected the historical data request for {query.symbol}: {broker_error}"
                ) from exc
            raise TimeoutError(
                f"Timed out while requesting historical bars for {query.symbol}."
            ) from exc
    finally:
        if owns_connection:
            runtime_app.disconnect_and_stop()

    currency = resolved_detail.currency or query.currency
    return {
        "query": {
            "symbol": query.symbol,
            "security_type": query.security_type,
            "exchange": query.exchange,
            "currency": query.currency,
            "primary_exchange": query.primary_exchange,
            "isin": query.isin,
            "duration": query.duration,
            "bar_size": query.bar_size,
            "what_to_show": query.what_to_show,
            "use_rth": query.use_rth,
            "end_at": query.end_at.isoformat() if query.end_at is not None else None,
        },
        "resolved_contract": {
            **asdict(resolved_detail),
            "min_tick": (
                str(resolved_detail.min_tick)
                if resolved_detail.min_tick is not None
                else None
            ),
            "valid_exchanges": list(resolved_detail.valid_exchanges),
            "order_types": list(resolved_detail.order_types),
            "sec_ids": dict(resolved_detail.sec_ids),
        },
        "bar_count": len(raw_bars),
        "currency": currency,
        "bars": [
            _serialize_historical_bar(raw_bar, currency=currency)
            for raw_bar in raw_bars
        ],
    }


def read_latest_trade_price(
    config: IbkrConnectionConfig,
    *,
    symbol: str,
    exchange: str,
    currency: str,
    security_type: str = "STK",
    primary_exchange: str | None = None,
    isin: str | None = None,
    local_symbol: str | None = None,
    end_at: datetime | None = None,
    timeout: int = 20,
    sync_wrapper_cls: type[HistoricalBarsSyncWrapperProtocol] | None = None,
    response_timeout_cls: type[Exception] | None = None,
    contract_cls: type[Any] | None = None,
    app: HistoricalBarsSyncWrapperProtocol | None = None,
) -> dict[str, Any]:
    response = read_historical_bars(
        config,
        HistoricalBarsQuery(
            symbol=symbol,
            exchange=exchange,
            currency=currency,
            security_type=security_type,
            primary_exchange=primary_exchange,
            isin=isin,
            local_symbol=local_symbol,
            duration="1 D",
            bar_size="1 min",
            what_to_show="TRADES",
            use_rth=True,
            end_at=end_at,
        ),
        timeout=timeout,
        sync_wrapper_cls=sync_wrapper_cls,
        response_timeout_cls=response_timeout_cls,
        contract_cls=contract_cls,
        app=app,
    )
    raw_bars = response["bars"]
    if not raw_bars:
        raise LookupError(
            f"IBKR returned no usable trade bars for {symbol} on {exchange}."
        )
    latest_bar = raw_bars[-1]
    close_value = _to_decimal(latest_bar.get("close"))
    if close_value is None or close_value <= 0:
        raise LookupError(
            f"IBKR returned no usable latest close for {symbol} on {exchange}."
        )
    return {
        "price": str(close_value),
        "observed_at": latest_bar.get("timestamp"),
        "currency": response.get("currency"),
        "source": "ibkr_historical_trades_1min_close",
        "bar": latest_bar,
        "resolved_contract": response.get("resolved_contract"),
    }
