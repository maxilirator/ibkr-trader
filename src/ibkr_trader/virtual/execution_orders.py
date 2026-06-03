from __future__ import annotations

from ibkr_trader.virtual.execution_core import *
from ibkr_trader.virtual.execution_quotes import *

def _build_resolved_contract(instruction: ExecutionInstruction) -> dict[str, Any]:
    return {
        "con_id": None,
        "symbol": instruction.instrument.symbol,
        "local_symbol": instruction.instrument.symbol,
        "security_type": instruction.instrument.security_type.value,
        "exchange": instruction.instrument.exchange,
        "primary_exchange": instruction.instrument.primary_exchange,
        "currency": instruction.instrument.currency,
        "virtual_contract": True,
    }


def _build_virtual_fill_payload(
    *,
    order_id: int,
    perm_id: int,
    order_ref: str,
    action: str,
    quantity: Decimal,
    price: Decimal,
    quote: VirtualMarketQuoteRecord,
    condition_code: str,
) -> dict[str, Any]:
    side = "BOT" if action.strip().upper() == "BUY" else "SLD"
    return _serialize_for_json(
        {
            "external_execution_id": f"virtual-{order_id}-{quote.id}",
            "external_order_id": str(order_id),
            "external_perm_id": str(perm_id),
            "order_ref": order_ref,
            "side": side,
            "quantity": quantity,
            "price": price,
            "commission": VIRTUAL_FIXED_COMMISSION_SEK,
            "commission_currency": "SEK",
            "executed_at": quote.observed_at,
            "condition_code": condition_code,
            "market_quote": serialize_virtual_quote(quote),
        }
    )


def _normalize_virtual_stock_quantity(
    quantity: Decimal,
    *,
    allow_round_down: bool,
) -> tuple[Decimal, list[str]]:
    if quantity <= 0:
        raise ValueError("Virtual order quantity must be positive.")
    whole_quantity = quantity.to_integral_value(rounding=ROUND_DOWN)
    if whole_quantity <= 0:
        raise ValueError("Virtual order quantity rounds below one share.")
    if quantity == whole_quantity:
        return whole_quantity, []
    if not allow_round_down:
        raise ValueError("Virtual target_quantity must be a whole-share value.")
    return whole_quantity, [
        f"Virtual stock quantity rounded down from {quantity} to {whole_quantity}."
    ]


def _virtual_sizing_price(
    *,
    order_type: str,
    limit_price: Decimal | None,
    stop_price: Decimal | None,
    market_price: Decimal | None,
) -> Decimal | None:
    normalized_order_type = order_type.strip().upper()
    if normalized_order_type == "LMT" and limit_price is not None:
        return limit_price
    if normalized_order_type.startswith("STP") and stop_price is not None:
        return stop_price
    return market_price


def _virtual_order_quantity(
    *,
    instruction: ExecutionInstruction,
    explicit_quantity: Decimal | None,
    order_type: str,
    limit_price: Decimal | None,
    stop_price: Decimal | None,
    market_price: Decimal | None,
    account_cash_balance: Decimal,
) -> tuple[Decimal, list[str], dict[str, Any]]:
    if explicit_quantity is not None:
        quantity, warnings = _normalize_virtual_stock_quantity(
            explicit_quantity,
            allow_round_down=False,
        )
        return quantity, warnings, {
            "mode": "explicit_exit_quantity",
            "estimated_quantity": str(explicit_quantity),
            "normalized_quantity": str(quantity),
        }

    sizing = instruction.sizing
    sizing_price = _virtual_sizing_price(
        order_type=order_type,
        limit_price=limit_price,
        stop_price=stop_price,
        market_price=market_price,
    )
    target_notional = None
    estimated_quantity = None
    warnings: list[str] = []

    if sizing.mode is SizingMode.TARGET_QUANTITY:
        estimated_quantity = sizing.target_quantity
        allow_round_down = False
    else:
        if sizing.mode is SizingMode.TARGET_NOTIONAL:
            target_notional = sizing.target_notional
        elif sizing.mode is SizingMode.FRACTION_OF_ACCOUNT_NAV:
            funding_basis = sizing.funding_basis or FundingBasis.CASH
            if (
                funding_basis is FundingBasis.ACCOUNT_NAV
                and instruction.intent.position_side.value == "LONG"
                and not sizing.allow_leverage
            ):
                raise ValueError(
                    "Virtual long account_nav sizing requires sizing.allow_leverage=true."
                )
            if account_cash_balance <= 0:
                raise ValueError("Virtual account has no positive cash balance for sizing.")
            if sizing.target_fraction_of_account is None:
                raise ValueError("Virtual account fraction sizing is missing a target fraction.")
            target_notional = account_cash_balance * sizing.target_fraction_of_account
        else:  # pragma: no cover - enum validation should make this unreachable.
            raise ValueError(f"Unsupported virtual sizing mode: {sizing.mode}")

        if target_notional is None or target_notional <= 0:
            raise ValueError("Virtual target notional must be positive.")
        if sizing_price is None or sizing_price <= 0:
            raise ValueError(
                "Virtual target_notional sizing requires a positive limit, stop, or market price."
            )
        estimated_quantity = target_notional / sizing_price
        allow_round_down = True

    if estimated_quantity is None:
        raise ValueError("Virtual order quantity could not be estimated from sizing.")
    quantity, quantity_warnings = _normalize_virtual_stock_quantity(
        estimated_quantity,
        allow_round_down=allow_round_down,
    )
    warnings.extend(quantity_warnings)
    return quantity, warnings, {
        "mode": sizing.mode.value,
        "target_notional": str(target_notional) if target_notional is not None else None,
        "target_quantity": (
            str(sizing.target_quantity) if sizing.target_quantity is not None else None
        ),
        "target_fraction_of_account": (
            str(sizing.target_fraction_of_account)
            if sizing.target_fraction_of_account is not None
            else None
        ),
        "sizing_price": str(sizing_price) if sizing_price is not None else None,
        "estimated_quantity": str(estimated_quantity),
        "normalized_quantity": str(quantity),
        "account_cash_balance": str(account_cash_balance),
    }


def _build_virtual_order_submission(
    *,
    instruction: ExecutionInstruction,
    account_key: str,
    action: str,
    order_ref: str,
    order_type: str,
    time_in_force: str,
    limit_price: Decimal | None,
    stop_price: Decimal | None,
    quote: VirtualMarketQuoteRecord | None,
    account_cash_balance: Decimal,
    quantity: Decimal | None = None,
    oca_group: str | None = None,
    oca_type: int | None = None,
) -> dict[str, Any]:
    order_id = _new_virtual_order_id()
    perm_id = _new_virtual_perm_id(order_id)
    market_price, price_source = (
        _virtual_condition_price_for_order(
            quote,
            action=action,
            order_type=order_type,
        )
        if quote is not None
        else (None, None)
    )
    price_met, condition_code = _virtual_price_condition(
        action=action,
        order_type=order_type,
        market_price=market_price,
        limit_price=limit_price,
        stop_price=stop_price,
    )
    execution_price = (
        _virtual_execution_price_for_order(
            quote=quote,
            action=action,
            order_type=order_type,
            condition_price=market_price,
            condition_source=price_source,
            limit_price=limit_price,
            stop_price=stop_price,
        )
        if price_met
        else None
    )
    total_quantity, sizing_warnings, sizing_payload = _virtual_order_quantity(
        instruction=instruction,
        explicit_quantity=quantity,
        order_type=order_type,
        limit_price=limit_price,
        stop_price=stop_price,
        market_price=market_price,
        account_cash_balance=account_cash_balance,
    )
    if price_met and price_source not in (None, "QUOTE"):
        condition_code = f"{condition_code}:{price_source}"
    status = "FILLED" if price_met else "Submitted"
    fill_payload = (
        _build_virtual_fill_payload(
            order_id=order_id,
            perm_id=perm_id,
            order_ref=order_ref,
            action=action,
            quantity=total_quantity,
            price=execution_price,
            quote=quote,
            condition_code=condition_code,
        )
        if price_met and execution_price is not None and quote is not None
        else None
    )
    return _serialize_for_json(
        {
            "broker_kind": BROKER_KIND_VIRTUAL,
            "instruction_id": instruction.instruction_id,
            "account": account_key,
            "is_virtual": True,
            "warnings": sizing_warnings,
            "resolved_contract": _build_resolved_contract(instruction),
            "order": {
                "order_ref": order_ref,
                "action": action,
                "order_type": order_type,
                "time_in_force": time_in_force,
                "limit_price": _decimal_to_string(limit_price),
                "stop_price": _decimal_to_string(stop_price),
                "total_quantity": str(total_quantity),
                "outside_rth": False,
                "oca_group": oca_group,
                "oca_type": oca_type,
                "transmit": False,
                "is_virtual": True,
            },
            "broker_order_status": {
                "orderId": order_id,
                "status": status,
                "filled": str(total_quantity) if price_met else "0",
                "remaining": "0" if price_met else str(total_quantity),
                "avgFillPrice": _decimal_to_string(execution_price) if price_met else "0",
                "permId": perm_id,
                "parentId": 0,
                "lastFillPrice": _decimal_to_string(execution_price) if price_met else "0",
                "clientId": 0,
                "whyHeld": "",
                "mktCapPrice": "0",
            },
            "virtual_execution": {
                "price_met": price_met,
                "condition_code": condition_code,
                "quantity_disregarded": False,
                "sizing": sizing_payload,
                "fixed_commission": str(VIRTUAL_FIXED_COMMISSION_SEK),
                "fixed_commission_currency": "SEK",
                "market_price": _decimal_to_string(market_price),
                "market_price_source": price_source,
                "execution_price": _decimal_to_string(execution_price),
                "market_quote": serialize_virtual_quote(quote) if quote is not None else None,
                "fill": fill_payload,
            },
            "tws_submission": None,
        }
    )


def submit_virtual_entry_order(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    instruction: ExecutionInstruction,
    *,
    timeout: int = 10,
) -> dict[str, Any]:
    del broker_config, timeout
    account_key = normalize_virtual_account_key(instruction.account.account_key)
    with session_scope(session_factory) as session:
        broker_account = ensure_virtual_account(
            session,
            account_key=account_key,
            base_currency="SEK",
        )
        quote = _latest_virtual_quote(
            session,
            account_key=account_key,
            symbol=instruction.instrument.symbol,
            currency=instruction.instrument.currency,
            security_type=instruction.instrument.security_type.value,
            instruction=instruction,
        )
        return _build_virtual_order_submission(
            instruction=instruction,
            account_key=account_key,
            action=instruction.intent.side,
            order_ref=instruction.instruction_id,
            order_type=_normalize_order_type(instruction.entry.order_type),
            time_in_force=instruction.entry.time_in_force.value,
            limit_price=instruction.entry.limit_price,
            stop_price=None,
            quote=quote,
            account_cash_balance=_virtual_cash_balance(broker_account),
        )


def submit_virtual_exit_order(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    instruction: ExecutionInstruction,
    *,
    quantity: Decimal,
    order_type: OrderType | str,
    order_ref: str,
    timeout: int = 10,
    limit_price: Decimal | None = None,
    stop_price: Decimal | None = None,
    oca_group: str | None = None,
    oca_type: int | None = None,
) -> dict[str, Any]:
    del broker_config, timeout
    account_key = normalize_virtual_account_key(instruction.account.account_key)
    action = "SELL" if instruction.intent.side == "BUY" else "BUY"
    with session_scope(session_factory) as session:
        broker_account = ensure_virtual_account(
            session,
            account_key=account_key,
            base_currency="SEK",
        )
        quote = _latest_virtual_quote(
            session,
            account_key=account_key,
            symbol=instruction.instrument.symbol,
            currency=instruction.instrument.currency,
            security_type=instruction.instrument.security_type.value,
            instruction=instruction,
        )
        return _build_virtual_order_submission(
            instruction=instruction,
            account_key=account_key,
            action=action,
            order_ref=order_ref,
            order_type=_normalize_order_type(order_type),
            time_in_force=instruction.entry.time_in_force.value,
            limit_price=limit_price,
            stop_price=stop_price,
            quote=quote,
            account_cash_balance=_virtual_cash_balance(broker_account),
            quantity=quantity,
            oca_group=oca_group,
            oca_type=oca_type,
        )



__all__ = [name for name in globals() if not name.startswith("__")]
