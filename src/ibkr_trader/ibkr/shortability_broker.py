from __future__ import annotations

from ibkr_trader.ibkr.shortability_common import *
from ibkr_trader.ibkr.shortability_files import *


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

__all__ = [name for name in globals() if not name.startswith("__")]
