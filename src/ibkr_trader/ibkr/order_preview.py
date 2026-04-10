from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol, runtime_checkable

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.domain.contract_resolution import ContractResolveQuery
from ibkr_trader.domain.execution_contract import (
    ExecutionInstruction,
    ExecutionInstructionBatch,
    OrderType,
    SizingMode,
)
from ibkr_trader.ibkr.account_summary import (
    DEFAULT_ACCOUNT_SUMMARY_TAGS,
    normalize_account_summary_payload,
)
from ibkr_trader.ibkr.contracts import (
    _extract_broker_error_message,
    build_ibkr_contract,
    serialize_contract_details,
)
from ibkr_trader.ibkr.probe import IbkrDependencyError


@runtime_checkable
class PreviewSyncWrapperProtocol(Protocol):
    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool: ...

    def disconnect_and_stop(self) -> None: ...

    def get_account_summary(
        self,
        tags: str,
        group: str = "All",
        timeout: int = 5,
    ) -> dict[str, dict[str, dict[str, str]]]: ...

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


def _load_sync_wrapper_class() -> type[PreviewSyncWrapperProtocol]:
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


def _load_contract_class() -> type[Any]:
    try:
        from ibapi.contract import Contract
    except ModuleNotFoundError as exc:
        raise IbkrDependencyError(
            "The official IBKR Python client is not installed. "
            "Install the current TWS API package from IBKR and make sure "
            "the `ibapi` module is available in this environment."
        ) from exc

    return Contract


@dataclass(slots=True)
class FxConversionDetail:
    source_currency: str
    target_currency: str
    rate: Decimal
    rate_date: str | None
    lookup_symbol: str
    lookup_currency: str
    inverted: bool
    source: str = "ibkr_historical_midpoint"


def _is_placeholder_account_id(account_id: str | None) -> bool:
    if account_id is None:
        return True
    normalized = account_id.strip().upper()
    return normalized in {"", "DUXXXXXXX", "UXXXXXXX"}


def _to_decimal(value: str | None) -> Decimal | None:
    if value in (None, ""):
        return None

    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _select_broker_account_id(
    *,
    configured_account_id: str,
    normalized_summary: dict[str, Any],
) -> tuple[str | None, list[str]]:
    warnings: list[str] = []
    accounts = normalized_summary["accounts"]

    if not accounts:
        return None, ["No IBKR account summary data was returned."]

    if not _is_placeholder_account_id(configured_account_id):
        if configured_account_id in accounts:
            return configured_account_id, warnings
        return None, [
            f"Configured IBKR_ACCOUNT_ID '{configured_account_id}' was not found in account summary."
        ]

    if len(accounts) == 1:
        selected_account_id = next(iter(accounts))
        warnings.append(
            f"Using the only available broker account '{selected_account_id}' because IBKR_ACCOUNT_ID is not set."
        )
        return selected_account_id, warnings

    return None, [
        "Multiple broker accounts are visible and IBKR_ACCOUNT_ID is not configured."
    ]


def _get_account_value(
    normalized_summary: dict[str, Any],
    account_id: str,
    tag: str,
) -> dict[str, str | None] | None:
    return normalized_summary["accounts"].get(account_id, {}).get(tag)


def _map_order_type(order_type: OrderType) -> str:
    if order_type is OrderType.LIMIT:
        return "LMT"
    if order_type is OrderType.MARKET:
        return "MKT"
    raise ValueError(f"Unsupported order type: {order_type}")


def _estimate_quantity_from_notional(
    *,
    target_notional: Decimal,
    limit_price: Decimal | None,
) -> tuple[Decimal | None, list[str]]:
    if limit_price is None:
        return None, ["Notional sizing preview currently requires a limit_price."]
    if limit_price <= 0:
        return None, ["limit_price must be positive for notional sizing preview."]
    return target_notional / limit_price, []


def _build_fx_contract(
    *,
    base_currency: str,
    quote_currency: str,
    contract_cls: type[Any] | None = None,
) -> Any:
    runtime_contract_cls = contract_cls or _load_contract_class()
    contract = runtime_contract_cls()
    contract.symbol = base_currency
    contract.secType = "CASH"
    contract.exchange = "IDEALPRO"
    contract.currency = quote_currency
    return contract


def _extract_latest_bar_close(raw_bars: list[Any]) -> tuple[Decimal | None, str | None]:
    if not raw_bars:
        return None, None

    latest_bar = raw_bars[-1]
    close_value = _to_decimal(getattr(latest_bar, "close", None))
    if close_value is None or close_value <= 0:
        return None, None

    bar_date = getattr(latest_bar, "date", None)
    return close_value, (str(bar_date) if bar_date is not None else None)


def _serialize_fx_conversion(detail: FxConversionDetail | None) -> dict[str, Any] | None:
    if detail is None:
        return None

    return {
        "source_currency": detail.source_currency,
        "target_currency": detail.target_currency,
        "rate": str(detail.rate),
        "rate_date": detail.rate_date,
        "lookup_contract": {
            "symbol": detail.lookup_symbol,
            "security_type": "CASH",
            "exchange": "IDEALPRO",
            "currency": detail.lookup_currency,
        },
        "inverted": detail.inverted,
        "source": detail.source,
    }


def _resolve_fx_conversion(
    *,
    app: PreviewSyncWrapperProtocol,
    source_currency: str,
    target_currency: str,
    timeout: int,
    timeout_cls: type[Exception],
    contract_cls: type[Any] | None,
    fx_rate_cache: dict[tuple[str, str], tuple[FxConversionDetail | None, tuple[str, ...]]],
) -> tuple[FxConversionDetail | None, list[str]]:
    cache_key = (source_currency, target_currency)
    cached_entry = fx_rate_cache.get(cache_key)
    if cached_entry is not None:
        cached_detail, cached_issues = cached_entry
        return cached_detail, list(cached_issues)

    attempt_errors: list[str] = []
    for base_currency, quote_currency, inverted in (
        (source_currency, target_currency, False),
        (target_currency, source_currency, True),
    ):
        fx_contract = _build_fx_contract(
            base_currency=base_currency,
            quote_currency=quote_currency,
            contract_cls=contract_cls,
        )
        try:
            raw_bars = app.get_historical_data(
                fx_contract,
                "",
                "2 D",
                "1 day",
                "MIDPOINT",
                False,
                1,
                timeout,
            )
        except timeout_cls:
            broker_error = _extract_broker_error_message(app)
            attempt_errors.append(
                f"{base_currency}.{quote_currency}: "
                f"{broker_error or 'timed out while requesting historical FX midpoint data'}"
            )
            continue

        raw_rate, rate_date = _extract_latest_bar_close(raw_bars)
        if raw_rate is None:
            attempt_errors.append(
                f"{base_currency}.{quote_currency}: no usable historical FX midpoint close was returned"
            )
            continue

        resolved_rate = Decimal("1") / raw_rate if inverted else raw_rate
        detail = FxConversionDetail(
            source_currency=source_currency,
            target_currency=target_currency,
            rate=resolved_rate,
            rate_date=rate_date,
            lookup_symbol=base_currency,
            lookup_currency=quote_currency,
            inverted=inverted,
        )
        fx_rate_cache[cache_key] = (detail, ())
        return detail, []

    issues = [
        "IBKR could not derive an FX conversion from "
        f"{source_currency} to {target_currency}. "
        f"Attempts: {'; '.join(attempt_errors)}"
    ]
    fx_rate_cache[cache_key] = (None, tuple(issues))
    return None, issues


def _resolve_sizing_preview(
    instruction: ExecutionInstruction,
    *,
    broker_account_id: str | None,
    normalized_summary: dict[str, Any],
    app: PreviewSyncWrapperProtocol,
    timeout: int,
    timeout_cls: type[Exception],
    contract_cls: type[Any] | None,
    fx_rate_cache: dict[tuple[str, str], tuple[FxConversionDetail | None, tuple[str, ...]]],
) -> dict[str, Any]:
    issues: list[str] = []
    account_net_liquidation = None
    account_currency = None

    if broker_account_id is not None:
        account_net_liquidation = _get_account_value(
            normalized_summary,
            broker_account_id,
            "NetLiquidation",
        )
        if account_net_liquidation is not None:
            account_currency = account_net_liquidation.get("currency")

    target_notional = None
    estimated_quantity = None
    fx_conversion = None
    sizing_mode = instruction.sizing.mode

    if sizing_mode is SizingMode.TARGET_QUANTITY:
        estimated_quantity = instruction.sizing.target_quantity
    elif sizing_mode is SizingMode.TARGET_NOTIONAL:
        target_notional = instruction.sizing.target_notional
        if target_notional is not None:
            estimated_quantity, sizing_issues = _estimate_quantity_from_notional(
                target_notional=target_notional,
                limit_price=instruction.entry.limit_price,
            )
            issues.extend(sizing_issues)
    elif sizing_mode is SizingMode.FRACTION_OF_ACCOUNT_NAV:
        if account_net_liquidation is None:
            issues.append("NetLiquidation is unavailable for account-based sizing.")
        else:
            net_liquidation_value = _to_decimal(account_net_liquidation.get("value"))
            if net_liquidation_value is None:
                issues.append("NetLiquidation could not be parsed as a decimal.")
            elif account_currency is None:
                issues.append("NetLiquidation did not include an account currency.")
            else:
                fraction_value = instruction.sizing.target_fraction_of_account
                if fraction_value is not None:
                    target_notional = net_liquidation_value * fraction_value
                    if account_currency != instruction.instrument.currency:
                        fx_conversion, fx_issues = _resolve_fx_conversion(
                            app=app,
                            source_currency=account_currency,
                            target_currency=instruction.instrument.currency,
                            timeout=timeout,
                            timeout_cls=timeout_cls,
                            contract_cls=contract_cls,
                            fx_rate_cache=fx_rate_cache,
                        )
                        issues.extend(fx_issues)
                        if fx_conversion is not None:
                            target_notional = target_notional * fx_conversion.rate
                        else:
                            target_notional = None
                    if target_notional is not None:
                        estimated_quantity, sizing_issues = _estimate_quantity_from_notional(
                            target_notional=target_notional,
                            limit_price=instruction.entry.limit_price,
                        )
                        issues.extend(sizing_issues)

    return {
        "issues": issues,
        "account_net_liquidation": account_net_liquidation,
        "account_currency": account_currency,
        "target_notional": target_notional,
        "estimated_quantity": estimated_quantity,
        "fx_conversion": fx_conversion,
    }


def _build_instruction_preview(
    instruction: ExecutionInstruction,
    *,
    broker_account_id: str | None,
    contract_matches: list[Any] | None,
    contract_error: str | None,
    sizing_preview: dict[str, Any],
) -> dict[str, Any]:
    issues = list(sizing_preview["issues"])
    warnings: list[str] = []

    if broker_account_id is None:
        issues.append("No broker account could be selected for preview.")

    resolved_contract = None
    if contract_error is not None:
        issues.append(contract_error)
    elif contract_matches is None:
        issues.append("Contract lookup did not return a result.")
    elif len(contract_matches) == 0:
        issues.append("Contract lookup returned no matches.")
    elif len(contract_matches) > 1:
        issues.append("Contract lookup returned multiple matches.")
    else:
        resolved_contract = serialize_contract_details(contract_matches[0])

    target_notional = sizing_preview["target_notional"]
    estimated_quantity = sizing_preview["estimated_quantity"]
    account_net_liquidation = sizing_preview["account_net_liquidation"]
    account_currency = sizing_preview["account_currency"]
    sizing_mode = instruction.sizing.mode

    order_preview = {
        "account": broker_account_id,
        "action": instruction.intent.side,
        "order_type": _map_order_type(instruction.entry.order_type),
        "tif": instruction.entry.time_in_force.value,
        "limit_price": (
            str(instruction.entry.limit_price)
            if instruction.entry.limit_price is not None
            else None
        ),
        "total_quantity": str(estimated_quantity) if estimated_quantity is not None else None,
    }

    return {
        "instruction_id": instruction.instruction_id,
        "status": "ready" if not issues else "unresolved",
        "issues": issues,
        "warnings": warnings,
        "execution_window": {
            "submit_at": instruction.entry.submit_at.isoformat(),
            "expire_at": instruction.entry.expire_at.isoformat(),
            "scheduler_required": True,
        },
        "account": {
            "broker_account_id": broker_account_id,
            "net_liquidation": account_net_liquidation,
        },
        "instrument": {
            "requested": {
                "symbol": instruction.instrument.symbol,
                "exchange": instruction.instrument.exchange,
                "currency": instruction.instrument.currency,
                "primary_exchange": instruction.instrument.primary_exchange,
                "isin": instruction.instrument.isin,
            },
            "resolved": (
                {
                    **asdict(resolved_contract),
                    "min_tick": (
                        str(resolved_contract.min_tick)
                        if resolved_contract.min_tick is not None
                        else None
                    ),
                    "valid_exchanges": list(resolved_contract.valid_exchanges),
                    "order_types": list(resolved_contract.order_types),
                    "sec_ids": dict(resolved_contract.sec_ids),
                }
                if resolved_contract is not None
                else None
            ),
        },
        "sizing": {
            "mode": sizing_mode.value,
            "target_fraction_of_account": (
                str(instruction.sizing.target_fraction_of_account)
                if instruction.sizing.target_fraction_of_account is not None
                else None
            ),
            "target_notional": str(target_notional) if target_notional is not None else None,
            "target_quantity": (
                str(instruction.sizing.target_quantity)
                if instruction.sizing.target_quantity is not None
                else None
            ),
            "account_currency": account_currency,
            "instrument_currency": instruction.instrument.currency,
            "fx_conversion": _serialize_fx_conversion(sizing_preview["fx_conversion"]),
            "estimated_quantity": str(estimated_quantity) if estimated_quantity is not None else None,
        },
        "order": order_preview,
    }


def preview_execution_batch(
    config: IbkrConnectionConfig,
    batch: ExecutionInstructionBatch,
    *,
    timeout: int = 10,
    sync_wrapper_cls: type[PreviewSyncWrapperProtocol] | None = None,
    response_timeout_cls: type[Exception] | None = None,
    contract_cls: type[Any] | None = None,
) -> dict[str, Any]:
    wrapper_cls = sync_wrapper_cls or _load_sync_wrapper_class()
    timeout_cls = response_timeout_cls or _load_response_timeout_class()
    app = wrapper_cls(timeout=timeout)
    runtime_contract_cls = contract_cls or _load_contract_class()

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
            raw_summary = app.get_account_summary(
                tags=",".join(DEFAULT_ACCOUNT_SUMMARY_TAGS),
                group="All",
                timeout=timeout,
            )
        except timeout_cls as exc:
            broker_error = _extract_broker_error_message(app)
            if broker_error is not None:
                raise LookupError(
                    f"IBKR rejected the account summary request: {broker_error}"
                ) from exc
            raise TimeoutError("Timed out while requesting IBKR account summary.") from exc

        normalized_summary = normalize_account_summary_payload(
            raw_summary,
            requested_tags=DEFAULT_ACCOUNT_SUMMARY_TAGS,
            account_id=None,
            group="All",
        )
        broker_account_id, account_warnings = _select_broker_account_id(
            configured_account_id=config.account_id,
            normalized_summary=normalized_summary,
        )

        previews: list[dict[str, Any]] = []
        fx_rate_cache: dict[
            tuple[str, str],
            tuple[FxConversionDetail | None, tuple[str, ...]],
        ] = {}
        for instruction in batch.instructions:
            contract_error = None
            contract_matches = None
            try:
                if hasattr(app, "contract_details"):
                    app.contract_details.clear()
                raw_contract = build_ibkr_contract(
                    ContractResolveQuery(
                        symbol=instruction.instrument.symbol,
                        security_type=instruction.instrument.security_type.value,
                        exchange=instruction.instrument.exchange,
                        currency=instruction.instrument.currency,
                        primary_exchange=instruction.instrument.primary_exchange,
                        isin=instruction.instrument.isin,
                    ),
                    contract_cls=runtime_contract_cls,
                )
                contract_matches = app.get_contract_details(raw_contract, timeout=timeout)
            except timeout_cls as exc:
                broker_error = _extract_broker_error_message(app)
                contract_error = (
                    f"IBKR rejected the contract lookup: {broker_error}"
                    if broker_error is not None
                    else f"Timed out while resolving {instruction.instrument.symbol}."
                )
            sizing_preview = _resolve_sizing_preview(
                instruction,
                broker_account_id=broker_account_id,
                normalized_summary=normalized_summary,
                app=app,
                timeout=timeout,
                timeout_cls=timeout_cls,
                contract_cls=runtime_contract_cls,
                fx_rate_cache=fx_rate_cache,
            )
            preview = _build_instruction_preview(
                instruction,
                broker_account_id=broker_account_id,
                contract_matches=contract_matches,
                contract_error=contract_error,
                sizing_preview=sizing_preview,
            )
            preview["warnings"].extend(account_warnings)
            previews.append(preview)
    finally:
        app.disconnect_and_stop()

    return {
        "account_summary": normalized_summary,
        "instruction_count": len(previews),
        "previews": previews,
    }
