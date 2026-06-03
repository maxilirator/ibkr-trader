from __future__ import annotations

from ibkr_trader.virtual.execution_core import *

def _latest_virtual_quote(
    session: Session,
    *,
    account_key: str,
    symbol: str,
    currency: str,
    security_type: str,
    instruction: ExecutionInstruction | None = None,
) -> VirtualMarketQuoteRecord | None:
    normalized_account_key = normalize_virtual_account_key(account_key)
    quotes = session.execute(
        select(VirtualMarketQuoteRecord)
        .where(
            VirtualMarketQuoteRecord.account_key == normalized_account_key,
            VirtualMarketQuoteRecord.symbol == symbol.strip().upper(),
            VirtualMarketQuoteRecord.currency == currency.strip().upper(),
            VirtualMarketQuoteRecord.security_type == security_type.strip().upper(),
        )
        .order_by(
            VirtualMarketQuoteRecord.observed_at.desc(),
            VirtualMarketQuoteRecord.id.desc(),
        )
        .limit(50)
    ).scalars()
    for quote in quotes:
        if _quote_matches_instruction_owner(quote, instruction):
            return quote
    return None


def _quote_owner_scope(
    quote: VirtualMarketQuoteRecord,
) -> dict[str, str | None] | None:
    metadata = quote.metadata_json if isinstance(quote.metadata_json, dict) else {}
    source = str(quote.source or "").strip().lower()
    purpose = str(metadata.get("purpose") or "").strip().lower()
    deployment_key = _normalize_text(metadata.get("deployment_key"))
    source_instruction_id = _normalize_text(metadata.get("source_instruction_id"))
    if deployment_key is None and source_instruction_id is None:
        return None
    if source != "rl_decision_bar" and purpose != "virtual_same_bar_fill_parity":
        return None
    return {
        "deployment_key": deployment_key,
        "source_instruction_id": source_instruction_id,
    }


def _execution_instruction_owner_scope(
    instruction: ExecutionInstruction | None,
) -> dict[str, str | None]:
    metadata = (
        instruction.trace.metadata
        if instruction is not None and instruction.trace is not None
        else {}
    )
    return {
        "deployment_key": _normalize_text(metadata.get("rl_deployment_key")),
        "source_instruction_id": _normalize_text(
            metadata.get("rl_source_instruction_id")
        ),
    }


def _owner_scope_matches_quote_scope(
    owner: dict[str, str | None],
    quote_scope: dict[str, str | None],
) -> bool:
    quote_deployment = quote_scope.get("deployment_key")
    quote_source = quote_scope.get("source_instruction_id")
    if quote_deployment is not None and owner.get("deployment_key") != quote_deployment:
        return False
    if quote_source is not None and owner.get("source_instruction_id") != quote_source:
        return False
    return True


def _quote_matches_instruction_owner(
    quote: VirtualMarketQuoteRecord,
    instruction: ExecutionInstruction | None,
) -> bool:
    quote_scope = _quote_owner_scope(quote)
    if quote_scope is None:
        return True
    return _owner_scope_matches_quote_scope(
        _execution_instruction_owner_scope(instruction),
        quote_scope,
    )


def serialize_virtual_quote(quote: VirtualMarketQuoteRecord) -> dict[str, Any]:
    return _serialize_for_json(
        {
            "quote_id": quote.id,
            "account_key": quote.account_key,
            "observed_at": quote.observed_at,
            "symbol": quote.symbol,
            "exchange": quote.exchange,
            "currency": quote.currency,
            "security_type": quote.security_type,
            "primary_exchange": quote.primary_exchange,
            "local_symbol": quote.local_symbol,
            "bid_price": quote.bid_price,
            "ask_price": quote.ask_price,
            "last_price": quote.last_price,
            "midpoint_price": quote.midpoint_price,
            "source": quote.source,
            "metadata": quote.metadata_json,
        }
    )


def _quote_price_for_action(
    quote: VirtualMarketQuoteRecord,
    *,
    action: str,
) -> Decimal | None:
    bid = _to_decimal(quote.bid_price)
    ask = _to_decimal(quote.ask_price)
    last = _to_decimal(quote.last_price)
    midpoint = _to_decimal(quote.midpoint_price)
    if midpoint is None and bid is not None and ask is not None:
        midpoint = (bid + ask) / Decimal("2")

    normalized_action = action.strip().upper()
    candidates = (
        (ask, last, midpoint, bid)
        if normalized_action == "BUY"
        else (bid, last, midpoint, ask)
    )
    for candidate in candidates:
        if candidate is not None and candidate > 0:
            return candidate
    return None


def _quote_fill_price_policy(quote: VirtualMarketQuoteRecord | None) -> str | None:
    if quote is None:
        return None
    metadata = quote.metadata_json or {}
    raw_payload = quote.raw_payload or {}
    raw_policy = metadata.get("fill_price_policy") or raw_payload.get("fill_price_policy")
    if raw_policy in (None, ""):
        return None
    return str(raw_policy).strip().lower()


def _uses_training_limit_fill_price(quote: VirtualMarketQuoteRecord | None) -> bool:
    return _quote_fill_price_policy(quote) == _TRAINING_LIMIT_FILL_PRICE_POLICY


def _stream_bar_range_from_quote(
    quote: VirtualMarketQuoteRecord,
) -> tuple[Decimal | None, Decimal | None]:
    raw_payload = quote.raw_payload or {}
    latest_bar = raw_payload.get("latest_stream_bar")
    if not isinstance(latest_bar, dict):
        return None, None
    low_price = _optional_decimal(latest_bar.get("low"))
    high_price = _optional_decimal(latest_bar.get("high"))
    return low_price, high_price


def _virtual_condition_price_for_order(
    quote: VirtualMarketQuoteRecord,
    *,
    action: str,
    order_type: str,
) -> tuple[Decimal | None, str | None]:
    quote_price = _quote_price_for_action(quote, action=action)
    low_price, high_price = _stream_bar_range_from_quote(quote)
    normalized_action = action.strip().upper()
    normalized_order_type = order_type.strip().upper()

    if normalized_order_type == "LMT":
        if normalized_action == "BUY":
            candidates = [
                (price, source)
                for price, source in (
                    (quote_price, "QUOTE"),
                    (low_price, "STREAM_BAR_LOW"),
                )
                if price is not None
            ]
            if not candidates:
                return None, None
            return min(candidates, key=lambda item: item[0])
        candidates = [
            (price, source)
            for price, source in (
                (quote_price, "QUOTE"),
                (high_price, "STREAM_BAR_HIGH"),
            )
            if price is not None
        ]
        if not candidates:
            return None, None
        return max(candidates, key=lambda item: item[0])

    if normalized_order_type.startswith("STP"):
        if normalized_action == "BUY":
            candidates = [
                (price, source)
                for price, source in (
                    (quote_price, "QUOTE"),
                    (high_price, "STREAM_BAR_HIGH"),
                )
                if price is not None
            ]
            if not candidates:
                return None, None
            return max(candidates, key=lambda item: item[0])
        candidates = [
            (price, source)
            for price, source in (
                (quote_price, "QUOTE"),
                (low_price, "STREAM_BAR_LOW"),
            )
            if price is not None
        ]
        if not candidates:
            return None, None
        return min(candidates, key=lambda item: item[0])

    return quote_price, "QUOTE" if quote_price is not None else None


def _virtual_execution_price_for_order(
    *,
    quote: VirtualMarketQuoteRecord | None,
    action: str,
    order_type: str,
    condition_price: Decimal | None,
    condition_source: str | None,
    limit_price: Decimal | None,
    stop_price: Decimal | None,
) -> Decimal | None:
    if condition_price is None:
        return None
    normalized_action = action.strip().upper()
    normalized_order_type = order_type.strip().upper()
    if normalized_order_type == "LMT" and limit_price is not None:
        if condition_source in {"STREAM_BAR_LOW", "STREAM_BAR_HIGH"} and (
            quote is None or _uses_training_limit_fill_price(quote)
        ):
            return limit_price
        quote_price = _quote_price_for_action(quote, action=action) if quote else None
        reference_price = quote_price or condition_price
        if normalized_action == "BUY":
            return min(reference_price, limit_price)
        return max(reference_price, limit_price)
    if normalized_order_type.startswith("STP") and stop_price is not None:
        if condition_source in {"STREAM_BAR_LOW", "STREAM_BAR_HIGH"} and (
            quote is None or _uses_training_limit_fill_price(quote)
        ):
            return stop_price
        quote_price = _quote_price_for_action(quote, action=action) if quote else None
        if quote_price is not None:
            return quote_price
    return condition_price


def _virtual_price_condition(
    *,
    action: str,
    order_type: str,
    market_price: Decimal | None,
    limit_price: Decimal | None,
    stop_price: Decimal | None,
) -> tuple[bool, str]:
    if market_price is None:
        return False, "NO_MARKET_PRICE"

    normalized_action = action.strip().upper()
    normalized_order_type = order_type.strip().upper()
    if normalized_order_type == "MKT":
        return True, "MARKET_ORDER"
    if normalized_order_type == "LMT":
        if limit_price is None:
            return False, "LIMIT_PRICE_MISSING"
        if normalized_action == "BUY":
            return market_price <= limit_price, "BUY_LIMIT_MET"
        return market_price >= limit_price, "SELL_LIMIT_MET"
    if normalized_order_type.startswith("STP"):
        if stop_price is None:
            return False, "STOP_PRICE_MISSING"
        if normalized_action == "BUY":
            return market_price >= stop_price, "BUY_STOP_MET"
        return market_price <= stop_price, "SELL_STOP_MET"
    return False, f"UNSUPPORTED_ORDER_TYPE:{normalized_order_type}"



__all__ = [name for name in globals() if not name.startswith("__")]
