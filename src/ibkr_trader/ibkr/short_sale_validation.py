from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from decimal import InvalidOperation
from pathlib import Path
from typing import Any
from typing import Protocol
from typing import runtime_checkable

from ibkr_trader.config import AppConfig
from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.domain.execution_contract import PositionSide
from ibkr_trader.ibkr.errors import IbkrDependencyError

_SHORT_MINIMUM_EQUITY_EUR = Decimal("2000")
_NON_SHORTABLE_ACCOUNT_TYPES = {"CASH", "ISK"}
_STOCKHOLM_PRIMARY_EXCHANGES = {"XSTO", "SFB"}


class ShortSaleValidationError(ValueError):
    """Raised when an instruction would violate the local short-sale guardrails."""


@dataclass(slots=True)
class ShortSaleValidationResult:
    is_short_sale: bool
    current_position_quantity: Decimal | None
    requested_quantity: Decimal | None
    account_type: str | None
    leverage: Decimal | None
    net_liquidation: Decimal | None
    net_liquidation_currency: str | None
    net_liquidation_eur: Decimal | None
    stockholm_shortability_status: str | None
    stockholm_shortability_as_of_date: str | None
    issues: tuple[str, ...]
    warnings: tuple[str, ...]


@dataclass(slots=True)
class _FxConversionDetail:
    rate: Decimal


@runtime_checkable
class ShortSaleValidationSyncWrapperProtocol(Protocol):
    def get_positions(self, timeout: int = 10) -> dict[str, list[Any]]: ...

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


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


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


def _build_fx_contract(
    *,
    base_currency: str,
    quote_currency: str,
    contract_cls: type[Any],
) -> Any:
    contract = contract_cls()
    contract.symbol = base_currency
    contract.secType = "CASH"
    contract.exchange = "IDEALPRO"
    contract.currency = quote_currency
    return contract


def _extract_latest_bar_close(raw_bars: list[Any]) -> Decimal | None:
    if not raw_bars:
        return None
    latest_bar = raw_bars[-1]
    close_value = _to_decimal(getattr(latest_bar, "close", None))
    if close_value is None or close_value <= 0:
        return None
    return close_value


def _resolve_fx_conversion(
    *,
    app: ShortSaleValidationSyncWrapperProtocol,
    source_currency: str,
    target_currency: str,
    timeout: int,
    timeout_cls: type[Exception],
    contract_cls: type[Any],
) -> tuple[_FxConversionDetail | None, tuple[str, ...]]:
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
            attempt_errors.append(
                f"{base_currency}.{quote_currency}: "
                "timed out while requesting historical FX midpoint data"
            )
            continue

        raw_rate = _extract_latest_bar_close(raw_bars)
        if raw_rate is None:
            attempt_errors.append(
                f"{base_currency}.{quote_currency}: no usable historical FX midpoint close was returned"
            )
            continue

        resolved_rate = Decimal("1") / raw_rate if inverted else raw_rate
        return _FxConversionDetail(rate=resolved_rate), ()

    return (
        None,
        (
            "IBKR could not derive an FX conversion from "
            f"{source_currency} to {target_currency}. "
            f"Attempts: {'; '.join(attempt_errors)}",
        ),
    )


def _account_tag_value(
    normalized_summary: dict[str, Any],
    *,
    broker_account_id: str,
    tag: str,
) -> tuple[str | None, str | None]:
    account_payload = normalized_summary.get("accounts", {}).get(broker_account_id, {})
    if not isinstance(account_payload, dict):
        return None, None
    raw_tag_payload = account_payload.get(tag, {})
    if not isinstance(raw_tag_payload, dict):
        return None, None
    value = raw_tag_payload.get("value")
    currency = raw_tag_payload.get("currency")
    return (
        str(value) if value not in (None, "") else None,
        str(currency) if currency not in (None, "") else None,
    )


def _normalize_stockholm_symbol_variants(symbol: str) -> tuple[str, ...]:
    raw = symbol.strip().lower()
    if not raw:
        return ()
    variants = {
        raw,
        raw.replace(" ", "-"),
        raw.replace(".", "-"),
        raw.replace(" ", "-").replace(".", "-"),
        raw.replace(" ", ""),
        raw.replace(".", ""),
    }
    return tuple(sorted(variant for variant in variants if variant))


def _stockholm_shortability_snapshot_path() -> Path:
    app_config = AppConfig.from_env()
    return app_config.stockholm_identity_path.parent / "shortability" / "shortability_latest.json"


def _load_stockholm_shortability_snapshot(
    snapshot_path: Path,
) -> tuple[dict[str, str], str | None, str | None]:
    try:
        snapshot_payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ShortSaleValidationError(
            "Stockholm shortability snapshot is missing. "
            "Refresh the official IBKR Sweden shortable universe before sending short orders."
        ) from exc

    entries = snapshot_payload.get("entries")
    if not isinstance(entries, list):
        raise ShortSaleValidationError(
            f"Stockholm shortability snapshot '{snapshot_path}' is malformed."
        )

    status_by_symbol: dict[str, str] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        raw_symbol = entry.get("symbol")
        raw_status = entry.get("status")
        if raw_symbol in (None, "") or raw_status in (None, ""):
            continue
        normalized_status = str(raw_status).strip().lower()
        for variant in _normalize_stockholm_symbol_variants(str(raw_symbol)):
            status_by_symbol[variant] = normalized_status

    return (
        status_by_symbol,
        (
            str(snapshot_payload.get("universe_as_of_date"))
            if snapshot_payload.get("universe_as_of_date") not in (None, "")
            else None
        ),
        (
            str(snapshot_payload.get("snapshot_at"))
            if snapshot_payload.get("snapshot_at") not in (None, "")
            else None
        ),
    )


def _resolve_current_position_quantity(
    app: ShortSaleValidationSyncWrapperProtocol,
    *,
    broker_account_id: str,
    instruction: ExecutionInstruction,
    timeout: int,
) -> Decimal:
    raw_positions = app.get_positions(timeout=timeout)
    if not isinstance(raw_positions, dict):
        return Decimal("0")

    matched_position = Decimal("0")
    for raw_position in raw_positions.get(broker_account_id, ()) or ():
        contract = getattr(raw_position, "contract", None)
        symbol = (
            getattr(contract, "symbol", None)
            if contract is not None
            else getattr(raw_position, "symbol", None)
        )
        currency = (
            getattr(contract, "currency", None)
            if contract is not None
            else getattr(raw_position, "currency", None)
        )
        security_type = (
            getattr(contract, "secType", None)
            if contract is not None
            else getattr(raw_position, "security_type", None)
        )
        primary_exchange = (
            getattr(contract, "primaryExchange", None)
            if contract is not None
            else getattr(raw_position, "primary_exchange", None)
        )

        if symbol is None or str(symbol).upper() != instruction.instrument.symbol.upper():
            continue
        if currency is None or str(currency).upper() != instruction.instrument.currency.upper():
            continue
        if (
            security_type is not None
            and str(security_type).upper() != instruction.instrument.security_type.value
        ):
            continue
        if (
            instruction.instrument.primary_exchange not in (None, "")
            and primary_exchange not in (None, "")
            and str(primary_exchange).upper()
            != instruction.instrument.primary_exchange.upper()
        ):
            continue

        quantity = _to_decimal(
            getattr(raw_position, "position", None)
            if getattr(raw_position, "position", None) not in (None, "")
            else getattr(raw_position, "pos", None)
        )
        if quantity is not None:
            matched_position += quantity

    return matched_position


def _is_stockholm_equity_instruction(instruction: ExecutionInstruction) -> bool:
    if instruction.instrument.currency.upper() != "SEK":
        return False
    exchange = instruction.instrument.exchange.upper()
    primary_exchange = (
        instruction.instrument.primary_exchange.upper()
        if instruction.instrument.primary_exchange is not None
        else None
    )
    return exchange in _STOCKHOLM_PRIMARY_EXCHANGES or primary_exchange in _STOCKHOLM_PRIMARY_EXCHANGES


def _validate_stockholm_shortability(
    instruction: ExecutionInstruction,
) -> tuple[str | None, str | None, tuple[str, ...], tuple[str, ...]]:
    if not _is_stockholm_equity_instruction(instruction):
        return None, None, (), ()

    snapshot_path = _stockholm_shortability_snapshot_path()
    status_by_symbol, as_of_date, _snapshot_at = _load_stockholm_shortability_snapshot(
        snapshot_path
    )
    matched_status = None
    for symbol_variant in _normalize_stockholm_symbol_variants(instruction.instrument.symbol):
        matched_status = status_by_symbol.get(symbol_variant)
        if matched_status is not None:
            break

    issues: list[str] = []
    warnings: list[str] = []
    if matched_status != "shortable":
        issues.append(
            "Stockholm short-sale validation rejected this instruction because the "
            "instrument is not present on the persisted official IBKR Sweden shortable list."
        )

    if as_of_date is not None:
        today_stockholm = datetime.now(UTC).astimezone().date().isoformat()
        if as_of_date != today_stockholm:
            warnings.append(
                f"Stockholm shortability snapshot is from {as_of_date}; refresh before "
                "relying on it for new short entries."
            )

    return matched_status, as_of_date, tuple(issues), tuple(warnings)


def _resolve_net_liquidation_eur(
    *,
    app: ShortSaleValidationSyncWrapperProtocol,
    normalized_summary: dict[str, Any],
    broker_account_id: str,
    timeout: int,
    timeout_cls: type[Exception],
    contract_cls: type[Any] | None,
) -> tuple[Decimal | None, Decimal | None, str | None, tuple[str, ...]]:
    raw_net_liquidation, net_liquidation_currency = _account_tag_value(
        normalized_summary,
        broker_account_id=broker_account_id,
        tag="NetLiquidation",
    )
    net_liquidation = _to_decimal(raw_net_liquidation)
    if net_liquidation is None:
        return None, None, net_liquidation_currency, (
            "IBKR NetLiquidation is unavailable for short-sale validation.",
        )

    account_currency = net_liquidation_currency
    if account_currency in (None, "", "BASE"):
        raw_currency, _ = _account_tag_value(
            normalized_summary,
            broker_account_id=broker_account_id,
            tag="Currency",
        )
        account_currency = raw_currency

    if account_currency in (None, ""):
        return net_liquidation, None, None, (
            "IBKR NetLiquidation did not include an account currency for short-sale validation.",
        )

    normalized_currency = account_currency.upper()
    if normalized_currency == "EUR":
        return net_liquidation, net_liquidation, normalized_currency, ()

    fx_conversion, fx_issues = _resolve_fx_conversion(
        app=app,
        source_currency=normalized_currency,
        target_currency="EUR",
        timeout=timeout,
        timeout_cls=timeout_cls,
        contract_cls=contract_cls or _load_contract_class(),
    )
    if fx_conversion is None:
        return net_liquidation, None, normalized_currency, tuple(fx_issues)

    return (
        net_liquidation,
        net_liquidation * fx_conversion.rate,
        normalized_currency,
        (),
    )


def validate_short_sale_entry(
    *,
    app: ShortSaleValidationSyncWrapperProtocol,
    instruction: ExecutionInstruction,
    broker_account_id: str,
    normalized_summary: dict[str, Any],
    requested_quantity: Decimal | None,
    timeout: int,
    timeout_cls: type[Exception],
    contract_cls: type[Any] | None = None,
) -> ShortSaleValidationResult:
    issues: list[str] = []
    warnings: list[str] = []

    current_position_quantity = _resolve_current_position_quantity(
        app,
        broker_account_id=broker_account_id,
        instruction=instruction,
        timeout=timeout,
    )

    is_short_sale = False
    if instruction.intent.position_side is PositionSide.SHORT:
        is_short_sale = True
        if instruction.intent.side != "SELL":
            issues.append("Short entries must use intent.side=SELL.")
        if current_position_quantity > 0:
            issues.append(
                "IBKR does not allow the account to hold a long and short position in the "
                "same security at the same time. Flatten the existing long first."
            )
    elif instruction.intent.side == "SELL":
        if current_position_quantity <= 0:
            issues.append(
                "A SELL instruction without an existing long position would open a short "
                "position. Use intent.position_side=SHORT so the trader can validate it."
            )
        elif requested_quantity is not None and requested_quantity > current_position_quantity:
            issues.append(
                "This SELL instruction would oversell the current long position and open a "
                "new short. Split it into a long exit and a separate short entry."
            )

    account_type, _ = _account_tag_value(
        normalized_summary,
        broker_account_id=broker_account_id,
        tag="AccountType",
    )
    raw_leverage, _ = _account_tag_value(
        normalized_summary,
        broker_account_id=broker_account_id,
        tag="Leverage-S",
    )
    leverage = _to_decimal(raw_leverage)

    net_liquidation = None
    net_liquidation_eur = None
    net_liquidation_currency = None
    stockholm_shortability_status = None
    stockholm_shortability_as_of_date = None

    if is_short_sale:
        normalized_account_type = (
            account_type.strip().upper() if account_type not in (None, "") else None
        )
        if normalized_account_type is None:
            issues.append("IBKR account type is unavailable for short-sale validation.")
        elif normalized_account_type in _NON_SHORTABLE_ACCOUNT_TYPES:
            issues.append(
                f"IBKR account type '{normalized_account_type}' is not allowed to open short stock positions."
            )

        if leverage is not None and leverage <= 0:
            issues.append(
                "IBKR account leverage is zero, which indicates the account does not currently "
                "have margin capacity for stock shorting."
            )

        (
            net_liquidation,
            net_liquidation_eur,
            net_liquidation_currency,
            equity_issues,
        ) = _resolve_net_liquidation_eur(
            app=app,
            normalized_summary=normalized_summary,
            broker_account_id=broker_account_id,
            timeout=timeout,
            timeout_cls=timeout_cls,
            contract_cls=contract_cls,
        )
        issues.extend(equity_issues)
        if net_liquidation_eur is not None and net_liquidation_eur < _SHORT_MINIMUM_EQUITY_EUR:
            issues.append(
                "IBKR requires at least EUR 2000, or the equivalent in another currency, "
                "to open a short stock position."
            )

        (
            stockholm_shortability_status,
            stockholm_shortability_as_of_date,
            shortability_issues,
            shortability_warnings,
        ) = _validate_stockholm_shortability(instruction)
        issues.extend(shortability_issues)
        warnings.extend(shortability_warnings)

    return ShortSaleValidationResult(
        is_short_sale=is_short_sale,
        current_position_quantity=current_position_quantity,
        requested_quantity=requested_quantity,
        account_type=account_type,
        leverage=leverage,
        net_liquidation=net_liquidation,
        net_liquidation_currency=net_liquidation_currency,
        net_liquidation_eur=net_liquidation_eur,
        stockholm_shortability_status=stockholm_shortability_status,
        stockholm_shortability_as_of_date=stockholm_shortability_as_of_date,
        issues=tuple(issues),
        warnings=tuple(warnings),
    )
