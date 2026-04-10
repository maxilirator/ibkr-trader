from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol, runtime_checkable

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.domain.contract_resolution import ContractResolveQuery
from ibkr_trader.ibkr.contracts import (
    _extract_broker_error_message,
    build_ibkr_contract,
    serialize_contract_details,
)
from ibkr_trader.ibkr.probe import IbkrDependencyError


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


def _load_sync_wrapper_class() -> type[HistoricalBarsSyncWrapperProtocol]:
    try:
        from ibapi.sync_wrapper import TWSSyncWrapper
    except ModuleNotFoundError as exc:
        raise IbkrDependencyError(
            "The official IBKR Python client is not installed. "
            "Install the current TWS API package from IBKR and make sure "
            "the `ibapi` module is available in this environment."
        ) from exc

    return TWSSyncWrapper


def _load_response_timeout_class() -> type[Exception]:
    try:
        from ibapi.sync_wrapper import ResponseTimeout
    except ModuleNotFoundError as exc:
        raise IbkrDependencyError(
            "The official IBKR Python client is not installed. "
            "Install the current TWS API package from IBKR and make sure "
            "the `ibapi` module is available in this environment."
        ) from exc

    return ResponseTimeout


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

    return end_at.astimezone(timezone.utc).strftime("%Y%m%d-%H:%M:%S UTC")


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


def read_historical_bars(
    config: IbkrConnectionConfig,
    query: HistoricalBarsQuery,
    *,
    timeout: int = 20,
    sync_wrapper_cls: type[HistoricalBarsSyncWrapperProtocol] | None = None,
    response_timeout_cls: type[Exception] | None = None,
    contract_cls: type[Any] | None = None,
) -> dict[str, Any]:
    query.validate()
    wrapper_cls = sync_wrapper_cls or _load_sync_wrapper_class()
    timeout_cls = response_timeout_cls or _load_response_timeout_class()
    app = wrapper_cls(timeout=timeout)

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
        try:
            if hasattr(app, "contract_details"):
                app.contract_details.clear()
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
            raw_matches = app.get_contract_details(ib_contract, timeout=timeout)
        except timeout_cls as exc:
            broker_error = _extract_broker_error_message(app)
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
            raw_bars = app.get_historical_data(
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
            broker_error = _extract_broker_error_message(app)
            if broker_error is not None:
                raise LookupError(
                    f"IBKR rejected the historical data request for {query.symbol}: {broker_error}"
                ) from exc
            raise TimeoutError(
                f"Timed out while requesting historical bars for {query.symbol}."
            ) from exc
    finally:
        app.disconnect_and_stop()

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
