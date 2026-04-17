from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from decimal import Decimal
from threading import Event
from threading import Lock
from threading import Thread
from time import sleep
from typing import Any

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.domain.contract_resolution import ContractResolveQuery
from ibkr_trader.ibkr.contracts import build_ibkr_contract
from ibkr_trader.ibkr.contracts import serialize_contract_details
from ibkr_trader.ibkr.errors import IbkrDependencyError


ALLOWED_TICK_TYPES = {
    "LAST": "Last",
    "ALLLAST": "AllLast",
    "BIDASK": "BidAsk",
    "MIDPOINT": "MidPoint",
}


@dataclass(slots=True)
class TickStreamQuery:
    symbol: str
    exchange: str
    currency: str
    security_type: str = "STK"
    primary_exchange: str | None = None
    local_symbol: str | None = None
    isin: str | None = None
    tick_types: tuple[str, ...] = ("Last", "BidAsk")
    duration_seconds: float = 5.0
    max_events: int = 500
    ignore_size: bool = False

    def validate(self) -> None:
        if not self.symbol:
            raise ValueError("symbol is required")
        if not self.exchange:
            raise ValueError("exchange is required")
        if not self.currency:
            raise ValueError("currency is required")
        if self.duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive")
        if self.duration_seconds > 60:
            raise ValueError("duration_seconds must be at most 60")
        if self.max_events <= 0:
            raise ValueError("max_events must be positive")
        if not self.tick_types:
            raise ValueError("tick_types must contain at least one stream type")
        invalid_types = [
            tick_type for tick_type in self.tick_types if tick_type not in ALLOWED_TICK_TYPES.values()
        ]
        if invalid_types:
            invalid_list = ", ".join(sorted(set(invalid_types)))
            raise ValueError(f"Unsupported tick_types: {invalid_list}")


def _normalize_tick_type(raw_value: Any) -> str:
    normalized = str(raw_value).replace("_", "").replace("-", "").upper()
    try:
        return ALLOWED_TICK_TYPES[normalized]
    except KeyError as exc:
        raise ValueError(
            "tick_types entries must be one of Last, AllLast, BidAsk, MidPoint"
        ) from exc


def _serialize_decimal(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(Decimal(str(value)))


def _epoch_to_iso(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _load_stream_runtime() -> tuple[type[Any], type[Any], type[Any]]:
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


class _TickStreamApp:
    def __init__(self, *, timeout: int = 10, max_events: int = 500) -> None:
        eclient_cls, ewrapper_cls, contract_cls = _load_stream_runtime()

        class TickStreamRuntime(ewrapper_cls, eclient_cls):
            def __init__(self, outer: "_TickStreamApp") -> None:
                eclient_cls.__init__(self, self)
                self._outer = outer

            def connectAck(self) -> None:  # noqa: N802
                self._outer.on_connect_ack()

            def nextValidId(self, orderId: int) -> None:  # noqa: N802
                self._outer.on_next_valid_id(orderId)

            def error(
                self,
                reqId: int,
                errorTime: int,
                errorCode: int,
                errorString: str,
                advancedOrderRejectJson: str = "",
            ) -> None:  # noqa: N802
                self._outer.on_error(
                    req_id=reqId,
                    error_time=errorTime,
                    error_code=errorCode,
                    error_string=errorString,
                    advanced_order_reject_json=advancedOrderRejectJson,
                )

            def contractDetails(self, reqId: int, contractDetails: Any) -> None:  # noqa: N802
                self._outer.on_contract_details(reqId, contractDetails)

            def contractDetailsEnd(self, reqId: int) -> None:  # noqa: N802
                self._outer.on_contract_details_end(reqId)

            def tickByTickAllLast(  # noqa: N802
                self,
                reqId: int,
                tickType: int,
                time: int,
                price: float,
                size: Decimal,
                tickAttribLast: Any,
                exchange: str,
                specialConditions: str,
            ) -> None:
                self._outer.on_tick_all_last(
                    req_id=reqId,
                    timestamp=time,
                    price=price,
                    size=size,
                    tick_type=tickType,
                    exchange=exchange,
                    special_conditions=specialConditions,
                    past_limit=bool(getattr(tickAttribLast, "pastLimit", False)),
                    unreported=bool(getattr(tickAttribLast, "unreported", False)),
                )

            def tickByTickBidAsk(  # noqa: N802
                self,
                reqId: int,
                time: int,
                bidPrice: float,
                askPrice: float,
                bidSize: Decimal,
                askSize: Decimal,
                tickAttribBidAsk: Any,
            ) -> None:
                self._outer.on_tick_bid_ask(
                    req_id=reqId,
                    timestamp=time,
                    bid_price=bidPrice,
                    ask_price=askPrice,
                    bid_size=bidSize,
                    ask_size=askSize,
                    bid_past_low=bool(getattr(tickAttribBidAsk, "bidPastLow", False)),
                    ask_past_high=bool(getattr(tickAttribBidAsk, "askPastHigh", False)),
                )

            def tickByTickMidPoint(  # noqa: N802
                self,
                reqId: int,
                time: int,
                midPoint: float,
            ) -> None:
                self._outer.on_tick_midpoint(
                    req_id=reqId,
                    timestamp=time,
                    midpoint=midPoint,
                )

        self.timeout = timeout
        self.max_events = max_events
        self.contract_cls = contract_cls
        self.client = TickStreamRuntime(self)
        self._thread: Thread | None = None
        self._connected_event = Event()
        self._next_request_id: int = 1
        self._request_id_lock = Lock()
        self._contract_details: dict[int, list[Any]] = {}
        self._contract_detail_events: dict[int, Event] = {}
        self._stream_req_ids: dict[int, str] = {}
        self._events: list[dict[str, Any]] = []
        self._events_lock = Lock()
        self._stop_collection = Event()
        self.errors: dict[int, list[dict[str, Any]]] = {}

    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool:
        self.client.connect(host, port, client_id)
        self._thread = Thread(target=self.client.run, name="ibkr-tick-stream", daemon=True)
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

    def on_error(
        self,
        *,
        req_id: int,
        error_time: int,
        error_code: int,
        error_string: str,
        advanced_order_reject_json: str,
    ) -> None:
        self.errors.setdefault(req_id, []).append(
            {
                "req_id": req_id,
                "error_time": error_time,
                "error_code": error_code,
                "error_string": error_string,
                "advanced_order_reject_json": advanced_order_reject_json or None,
            }
        )
        event = self._contract_detail_events.get(req_id)
        if req_id >= 0 and event is not None:
            event.set()

    def on_contract_details(self, req_id: int, contract_details: Any) -> None:
        self._contract_details.setdefault(req_id, []).append(contract_details)

    def on_contract_details_end(self, req_id: int) -> None:
        event = self._contract_detail_events.get(req_id)
        if event is not None:
            event.set()

    def on_tick_all_last(
        self,
        *,
        req_id: int,
        timestamp: int,
        price: float,
        size: Decimal,
        tick_type: int,
        exchange: str,
        special_conditions: str,
        past_limit: bool,
        unreported: bool,
    ) -> None:
        stream = self._stream_req_ids.get(req_id)
        if stream is None:
            return
        self._append_event(
            {
                "stream": stream,
                "timestamp": _epoch_to_iso(timestamp),
                "tick_type": tick_type,
                "price": _serialize_decimal(price),
                "size": str(size),
                "exchange": exchange or None,
                "special_conditions": special_conditions or None,
                "past_limit": past_limit,
                "unreported": unreported,
            }
        )

    def on_tick_bid_ask(
        self,
        *,
        req_id: int,
        timestamp: int,
        bid_price: float,
        ask_price: float,
        bid_size: Decimal,
        ask_size: Decimal,
        bid_past_low: bool,
        ask_past_high: bool,
    ) -> None:
        stream = self._stream_req_ids.get(req_id)
        if stream is None:
            return
        self._append_event(
            {
                "stream": stream,
                "timestamp": _epoch_to_iso(timestamp),
                "bid_price": _serialize_decimal(bid_price),
                "ask_price": _serialize_decimal(ask_price),
                "bid_size": str(bid_size),
                "ask_size": str(ask_size),
                "bid_past_low": bid_past_low,
                "ask_past_high": ask_past_high,
            }
        )

    def on_tick_midpoint(
        self,
        *,
        req_id: int,
        timestamp: int,
        midpoint: float,
    ) -> None:
        stream = self._stream_req_ids.get(req_id)
        if stream is None:
            return
        self._append_event(
            {
                "stream": stream,
                "timestamp": _epoch_to_iso(timestamp),
                "midpoint": _serialize_decimal(midpoint),
            }
        )

    def _append_event(self, event: dict[str, Any]) -> None:
        with self._events_lock:
            if len(self._events) >= self.max_events:
                self._stop_collection.set()
                return
            self._events.append(event)
            if len(self._events) >= self.max_events:
                self._stop_collection.set()

    def _allocate_req_id(self) -> int:
        if not self._connected_event.wait(timeout=self.timeout):
            raise TimeoutError("Timed out while waiting for IBKR connection readiness.")
        with self._request_id_lock:
            request_id = self._next_request_id
            self._next_request_id += 1
            return request_id

    def resolve_contract(self, query: TickStreamQuery) -> Any:
        req_id = self._allocate_req_id()
        event = Event()
        self._contract_detail_events[req_id] = event
        self._contract_details[req_id] = []
        contract = build_ibkr_contract(
            ContractResolveQuery(
                symbol=query.symbol,
                security_type=query.security_type,
                exchange=query.exchange,
                currency=query.currency,
                primary_exchange=query.primary_exchange,
                local_symbol=query.local_symbol,
                isin=query.isin,
            ),
            contract_cls=self.contract_cls,
        )
        self.client.reqContractDetails(req_id, contract)
        if not event.wait(timeout=self.timeout):
            raise TimeoutError(f"Timed out while resolving {query.symbol} for tick stream.")

        matches = self._contract_details.pop(req_id, [])
        self._contract_detail_events.pop(req_id, None)
        if len(matches) != 1:
            latest_error = _latest_request_error(self.errors, [req_id])
            if latest_error is not None:
                raise LookupError(
                    f"IBKR rejected tick stream contract lookup: {latest_error}"
                )
            raise LookupError(
                f"Expected exactly one resolved contract for {query.symbol}, got {len(matches)}."
            )
        return matches[0]

    def collect(self, query: TickStreamQuery) -> dict[str, Any]:
        query.validate()
        resolved_contract = self.resolve_contract(query)
        self._events = []
        self._stream_req_ids = {}
        self._stop_collection.clear()

        stream_req_ids: list[int] = []
        for tick_type in query.tick_types:
            req_id = self._allocate_req_id()
            self._stream_req_ids[req_id] = tick_type
            stream_req_ids.append(req_id)
            self.client.reqTickByTickData(
                req_id,
                resolved_contract.contract,
                tick_type,
                0,
                query.ignore_size,
            )

        started_at = datetime.now(timezone.utc)
        self._stop_collection.wait(timeout=query.duration_seconds)
        for req_id in stream_req_ids:
            if self.client.isConnected() and self.client.serverVersion() is not None:
                self.client.cancelTickByTickData(req_id)
        sleep(0.25)
        ended_at = datetime.now(timezone.utc)

        latest_error = _latest_request_error(self.errors, stream_req_ids)
        if latest_error is not None and not self._events:
            raise LookupError(f"IBKR rejected the tick stream request: {latest_error}")

        return {
            "query": asdict(query),
            "resolved_contract": _serialize_resolved_contract_details(resolved_contract),
            "stream_window": {
                "started_at": started_at.isoformat(),
                "ended_at": ended_at.isoformat(),
                "duration_seconds": query.duration_seconds,
                "max_events": query.max_events,
            },
            "event_count": len(self._events),
            "events": list(self._events),
            "errors": _serialize_errors(self.errors, stream_req_ids),
        }


def _latest_request_error(
    raw_errors: dict[int, list[dict[str, Any]]],
    req_ids: list[int],
) -> str | None:
    matched_errors: list[dict[str, Any]] = []
    for req_id in req_ids:
        matched_errors.extend(raw_errors.get(req_id, ()))
    matched_errors = [
        error
        for error in matched_errors
        if error.get("error_code") not in {300}
    ]
    if not matched_errors:
        return None

    latest_error = matched_errors[-1]
    return f"[{latest_error['error_code']}] {latest_error['error_string']}"


def _serialize_resolved_contract_details(raw_detail: Any) -> dict[str, Any]:
    serialized = asdict(serialize_contract_details(raw_detail))
    serialized["min_tick"] = (
        str(serialized["min_tick"]) if serialized["min_tick"] is not None else None
    )
    serialized["valid_exchanges"] = list(serialized["valid_exchanges"])
    serialized["order_types"] = list(serialized["order_types"])
    serialized["sec_ids"] = dict(serialized["sec_ids"])
    return serialized


def _serialize_errors(
    raw_errors: dict[int, list[dict[str, Any]]],
    req_ids: list[int],
) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for req_id in req_ids:
        serialized.extend(raw_errors.get(req_id, ()))
    return serialized


def collect_tick_stream_sample(
    config: IbkrConnectionConfig,
    query: TickStreamQuery,
    *,
    timeout: int = 10,
) -> dict[str, Any]:
    app = _TickStreamApp(timeout=timeout, max_events=query.max_events)
    connected = app.connect_and_start(
        host=config.host,
        port=config.port,
        client_id=config.client_id,
    )
    if not connected:
        raise ConnectionError(
            f"Failed to connect to IBKR at {config.host}:{config.port} "
            f"with client_id={config.client_id}."
        )

    try:
        return app.collect(query)
    finally:
        app.disconnect_and_stop()
