from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol, runtime_checkable

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.domain.contract_resolution import (
    ContractResolveQuery,
    ContractResolveResult,
    ResolvedContract,
)
from ibkr_trader.ibkr.probe import IbkrDependencyError


@runtime_checkable
class ContractDetailsSyncWrapperProtocol(Protocol):
    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool: ...

    def disconnect_and_stop(self) -> None: ...

    def get_contract_details(self, contract: Any, timeout: int | None = None) -> list[Any]: ...


def _load_ibkr_contract_runtime() -> tuple[type[Any], type[Exception]]:
    try:
        from ibapi.contract import Contract
        from ibapi.sync_wrapper import ResponseTimeout
    except ModuleNotFoundError as exc:
        raise IbkrDependencyError(
            "The official IBKR Python client is not installed. "
            "Install the current TWS API package from IBKR and make sure "
            "the `ibapi` module is available in this environment."
        ) from exc

    return Contract, ResponseTimeout


def _load_sync_wrapper_class() -> type[ContractDetailsSyncWrapperProtocol]:
    try:
        from ibapi.sync_wrapper import TWSSyncWrapper
    except ModuleNotFoundError as exc:
        raise IbkrDependencyError(
            "The official IBKR Python client is not installed. "
            "Install the current TWS API package from IBKR and make sure "
            "the `ibapi` module is available in this environment."
        ) from exc

    return TWSSyncWrapper


def _split_csv(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    return tuple(item.strip() for item in value.split(",") if item.strip())


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, "", 0, 0.0):
        return None

    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _serialize_sec_ids(raw_sec_ids: Any) -> dict[str, str]:
    if not raw_sec_ids:
        return {}

    sec_ids: dict[str, str] = {}
    for item in raw_sec_ids:
        tag = getattr(item, "tag", None)
        value = getattr(item, "value", None)
        if tag and value:
            sec_ids[str(tag)] = str(value)
    return sec_ids


def _extract_broker_error_message(app: Any) -> str | None:
    raw_errors = getattr(app, "errors", {})
    if not raw_errors:
        return None

    positive_req_ids = [req_id for req_id in raw_errors if isinstance(req_id, int) and req_id >= 0]
    ordered_req_ids = sorted(positive_req_ids or raw_errors.keys())
    if not ordered_req_ids:
        return None

    latest_req_id = ordered_req_ids[-1]
    request_errors = raw_errors.get(latest_req_id) or []
    if not request_errors:
        return None

    latest_error = request_errors[-1]
    error_code = latest_error.get("errorCode")
    error_string = latest_error.get("errorString")
    if error_code is None or not error_string:
        return None

    return f"[{error_code}] {error_string}"


def build_ibkr_contract(query: ContractResolveQuery, *, contract_cls: type[Any] | None = None) -> Any:
    query.validate()
    runtime_contract_cls = contract_cls
    if runtime_contract_cls is None:
        runtime_contract_cls, _ = _load_ibkr_contract_runtime()

    contract = runtime_contract_cls()
    contract.symbol = query.symbol
    contract.secType = query.security_type
    contract.exchange = query.exchange
    contract.currency = query.currency
    contract.includeExpired = query.include_expired

    if query.primary_exchange:
        contract.primaryExchange = query.primary_exchange
    if query.local_symbol:
        contract.localSymbol = query.local_symbol
    if query.isin:
        contract.secIdType = "ISIN"
        contract.secId = query.isin

    return contract


def serialize_contract_details(raw_detail: Any) -> ResolvedContract:
    contract = raw_detail.contract
    return ResolvedContract(
        con_id=int(getattr(contract, "conId", 0)),
        symbol=str(getattr(contract, "symbol", "")),
        local_symbol=str(getattr(contract, "localSymbol", "")),
        security_type=str(getattr(contract, "secType", "")),
        exchange=str(getattr(contract, "exchange", "")),
        primary_exchange=str(getattr(contract, "primaryExchange", "")),
        currency=str(getattr(contract, "currency", "")),
        trading_class=str(getattr(contract, "tradingClass", "")),
        market_name=(str(raw_detail.marketName) if getattr(raw_detail, "marketName", "") else None),
        long_name=(str(raw_detail.longName) if getattr(raw_detail, "longName", "") else None),
        min_tick=_to_decimal(getattr(raw_detail, "minTick", None)),
        valid_exchanges=_split_csv(getattr(raw_detail, "validExchanges", "")),
        order_types=_split_csv(getattr(raw_detail, "orderTypes", "")),
        time_zone_id=(
            str(raw_detail.timeZoneId) if getattr(raw_detail, "timeZoneId", "") else None
        ),
        trading_hours=(
            str(raw_detail.tradingHours) if getattr(raw_detail, "tradingHours", "") else None
        ),
        liquid_hours=(
            str(raw_detail.liquidHours) if getattr(raw_detail, "liquidHours", "") else None
        ),
        stock_type=(str(raw_detail.stockType) if getattr(raw_detail, "stockType", "") else None),
        industry=(str(raw_detail.industry) if getattr(raw_detail, "industry", "") else None),
        category=(str(raw_detail.category) if getattr(raw_detail, "category", "") else None),
        subcategory=(
            str(raw_detail.subcategory) if getattr(raw_detail, "subcategory", "") else None
        ),
        sec_ids=_serialize_sec_ids(getattr(raw_detail, "secIdList", None)),
    )


def serialize_contract_resolve_result(result: ContractResolveResult) -> dict[str, Any]:
    payload = asdict(result)
    payload["matches"] = [
        {
            **match,
            "min_tick": str(match["min_tick"]) if match["min_tick"] is not None else None,
        }
        for match in payload["matches"]
    ]
    payload["match_count"] = result.match_count
    payload["is_unique"] = result.is_unique
    return payload


def resolve_contracts(
    config: IbkrConnectionConfig,
    query: ContractResolveQuery,
    *,
    timeout: int = 10,
    sync_wrapper_cls: type[ContractDetailsSyncWrapperProtocol] | None = None,
    contract_cls: type[Any] | None = None,
    response_timeout_cls: type[Exception] | None = None,
) -> ContractResolveResult:
    wrapper_cls = sync_wrapper_cls or _load_sync_wrapper_class()

    if response_timeout_cls is None:
        _, response_timeout_cls = _load_ibkr_contract_runtime()

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
        ib_contract = build_ibkr_contract(query, contract_cls=contract_cls)
        try:
            raw_matches = app.get_contract_details(ib_contract, timeout=timeout)
        except response_timeout_cls as exc:
            broker_error = _extract_broker_error_message(app)
            if broker_error is not None:
                raise LookupError(
                    f"IBKR rejected the contract lookup for {query.symbol}: {broker_error}"
                ) from exc
            raise TimeoutError(
                f"Timed out while resolving {query.symbol} on {query.exchange}."
            ) from exc
    finally:
        app.disconnect_and_stop()

    return ContractResolveResult(
        query=query,
        matches=tuple(serialize_contract_details(item) for item in raw_matches),
    )
