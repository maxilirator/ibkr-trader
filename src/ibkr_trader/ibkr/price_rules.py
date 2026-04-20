from __future__ import annotations

from decimal import Decimal
from decimal import ROUND_CEILING
from decimal import ROUND_FLOOR
from typing import Any
from typing import Protocol
from typing import runtime_checkable

from ibkr_trader.ibkr.contracts import _extract_broker_error_message


@runtime_checkable
class MarketRuleSyncWrapperProtocol(Protocol):
    def get_market_rule(
        self,
        market_rule_id: int,
        timeout: int = 5,
    ) -> list[Any]: ...


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def _parse_csv(value: Any) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    return tuple(part.strip() for part in str(value).split(",") if part.strip())


def _parse_market_rule_ids(raw_detail: Any) -> tuple[int, ...]:
    market_rule_ids: list[int] = []
    for raw_value in _parse_csv(getattr(raw_detail, "marketRuleIds", "")):
        try:
            market_rule_ids.append(int(raw_value))
        except (TypeError, ValueError):
            continue
    return tuple(market_rule_ids)


def _select_market_rule_id(raw_detail: Any, *, exchange: str) -> int | None:
    valid_exchanges = _parse_csv(getattr(raw_detail, "validExchanges", ""))
    market_rule_ids = _parse_market_rule_ids(raw_detail)
    if not market_rule_ids:
        return None

    if valid_exchanges and len(valid_exchanges) == len(market_rule_ids):
        normalized_exchange = exchange.strip().upper()
        for current_exchange, market_rule_id in zip(valid_exchanges, market_rule_ids):
            if current_exchange.strip().upper() == normalized_exchange:
                return market_rule_id

    return market_rule_ids[0]


def _resolve_increment_from_market_rule(
    raw_market_rule: list[Any],
    *,
    price: Decimal,
) -> Decimal | None:
    best_increment: Decimal | None = None
    best_low_edge: Decimal | None = None

    for raw_increment in raw_market_rule:
        low_edge = _to_decimal(getattr(raw_increment, "lowEdge", None))
        increment = _to_decimal(getattr(raw_increment, "increment", None))
        if low_edge is None or increment is None or increment <= 0:
            continue
        if low_edge > price:
            continue
        if best_low_edge is None or low_edge >= best_low_edge:
            best_low_edge = low_edge
            best_increment = increment

    return best_increment


def resolve_price_increment(
    app: MarketRuleSyncWrapperProtocol,
    raw_detail: Any,
    *,
    exchange: str,
    price: Decimal,
    timeout: int,
    timeout_cls: type[Exception],
) -> Decimal | None:
    market_rule_id = _select_market_rule_id(raw_detail, exchange=exchange)
    if market_rule_id is not None:
        try:
            raw_market_rule = app.get_market_rule(market_rule_id, timeout=timeout)
        except timeout_cls:
            raw_market_rule = []
        else:
            resolved_increment = _resolve_increment_from_market_rule(
                raw_market_rule,
                price=price,
            )
            if resolved_increment is not None:
                return resolved_increment

    min_tick = _to_decimal(getattr(raw_detail, "minTick", None))
    if min_tick is not None and min_tick > 0:
        return min_tick
    return None


def normalize_order_price(
    *,
    price: Decimal | None,
    increment: Decimal | None,
    action: str,
    order_type: str,
) -> Decimal | None:
    if price is None or increment is None:
        return price
    if increment <= 0:
        return price

    normalized_action = action.strip().upper()
    normalized_order_type = order_type.strip().upper()

    if normalized_order_type == "LMT":
        rounding = ROUND_FLOOR if normalized_action == "BUY" else ROUND_CEILING
    elif normalized_order_type == "STP":
        rounding = ROUND_CEILING if normalized_action == "SELL" else ROUND_FLOOR
    else:
        return price

    steps = (price / increment).to_integral_value(rounding=rounding)
    return steps * increment


def describe_market_rule_error(app: Any) -> str | None:
    broker_error = _extract_broker_error_message(app)
    if broker_error is None:
        return None
    return f"IBKR rejected the market rule request: {broker_error}"
