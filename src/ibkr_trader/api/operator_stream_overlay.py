from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping

from ibkr_trader.domain.execution_payloads import parse_datetime


def _stream_payload(stream_snapshot: Mapping[str, Any]) -> Mapping[str, Any]:
    nested = stream_snapshot.get("stream")
    return nested if isinstance(nested, Mapping) else stream_snapshot


def _operator_stream_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    if not parsed.is_finite() or parsed <= 0 or abs(parsed) >= Decimal("1e12"):
        return None
    return parsed


def _operator_any_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:
        return None
    return parsed if parsed.is_finite() and abs(parsed) < Decimal("1e12") else None


def _operator_plain_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if text in {"", "-0"}:
        return "0"
    return text


def _operator_signed_decimal(value: Decimal | None, *, places: str = "0.01") -> str | None:
    if value is None:
        return None
    quantized = value.quantize(Decimal(places))
    prefix = "+" if quantized > 0 else ""
    return f"{prefix}{quantized}"


def _parse_operator_stream_time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return parse_datetime(str(value))
    except Exception:
        return None


def _operator_stream_symbol_keys(symbol: Any) -> set[str]:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        return set()
    keys = {normalized}
    if "-" in normalized:
        keys.add(normalized.replace("-", " "))
    if " " in normalized:
        keys.add(normalized.replace(" ", "-"))
    return keys


def _operator_stream_quote_price(quote: Mapping[str, Any]) -> Decimal | None:
    bid = _operator_stream_decimal(quote.get("bid_price"))
    ask = _operator_stream_decimal(quote.get("ask_price"))
    midpoint = None
    if bid is not None and ask is not None:
        midpoint = (bid + ask) / Decimal("2")
    for value in (
        quote.get("last_price"),
        midpoint,
        quote.get("midpoint_price"),
        quote.get("close_price"),
        bid,
        ask,
    ):
        parsed = value if isinstance(value, Decimal) else _operator_stream_decimal(value)
        if parsed is not None:
            return parsed
    return None


def _operator_stream_marks_by_symbol(
    stream_snapshot: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    stream = _stream_payload(stream_snapshot)
    quotes_by_symbol: dict[str, Mapping[str, Any]] = {}
    raw_quotes = stream.get("quotes")
    if isinstance(raw_quotes, list):
        for quote in raw_quotes:
            if not isinstance(quote, Mapping):
                continue
            for key in _operator_stream_symbol_keys(quote.get("symbol")):
                quotes_by_symbol[key] = quote

    bars_by_symbol = stream.get("bars_by_symbol")
    if not isinstance(bars_by_symbol, Mapping):
        bars_by_symbol = {}

    all_keys = set(quotes_by_symbol)
    for symbol in bars_by_symbol:
        all_keys.update(_operator_stream_symbol_keys(symbol))

    marks: dict[str, dict[str, Any]] = {}
    for key in all_keys:
        quote = quotes_by_symbol.get(key)
        bars = bars_by_symbol.get(key)
        if not isinstance(bars, list):
            for candidate in _operator_stream_symbol_keys(key):
                candidate_bars = bars_by_symbol.get(candidate)
                if isinstance(candidate_bars, list):
                    bars = candidate_bars
                    break
        bars = bars if isinstance(bars, list) else []
        latest_bar = bars[-1] if bars and isinstance(bars[-1], Mapping) else None
        previous_bar = bars[-2] if len(bars) >= 2 and isinstance(bars[-2], Mapping) else None

        price = _operator_stream_quote_price(quote) if quote is not None else None
        source = "quote"
        observed_at = (
            _parse_operator_stream_time(
                quote.get("last_trade_at") or quote.get("updated_at")
            )
            if quote is not None
            else None
        )
        if price is None and latest_bar is not None:
            price = _operator_stream_decimal(latest_bar.get("close"))
            observed_at = _parse_operator_stream_time(latest_bar.get("timestamp"))
            source = "bar"
        elif observed_at is None and latest_bar is not None:
            observed_at = _parse_operator_stream_time(latest_bar.get("timestamp"))
        if price is None:
            continue

        previous_price = (
            _operator_stream_decimal(previous_bar.get("close"))
            if previous_bar is not None
            else None
        )
        if previous_price is None and quote is not None:
            previous_price = _operator_stream_decimal(quote.get("close_price"))
        direction = None
        if previous_price is not None:
            if price > previous_price:
                direction = "UP"
            elif price < previous_price:
                direction = "DOWN"
            else:
                direction = "UNCHANGED"

        canonical_symbol = (
            str(quote.get("symbol")).strip().upper()
            if quote is not None and quote.get("symbol") not in (None, "")
            else key
        )
        mark = {
            "symbol": canonical_symbol,
            "price": price,
            "previous_price": previous_price,
            "observed_at": observed_at,
            "source": source,
            "direction": direction,
        }
        for candidate in _operator_stream_symbol_keys(key):
            marks[candidate] = mark
        for candidate in _operator_stream_symbol_keys(canonical_symbol):
            marks[candidate] = mark
    return marks


def _operator_row_symbol(row: Mapping[str, Any]) -> str:
    return str(row.get("symbol") or row.get("local_symbol") or "").strip().upper()


def _operator_stream_mark_for_row(
    marks_by_symbol: Mapping[str, dict[str, Any]],
    row: Mapping[str, Any],
) -> dict[str, Any] | None:
    for key in _operator_stream_symbol_keys(_operator_row_symbol(row)):
        mark = marks_by_symbol.get(key)
        if mark is not None:
            return mark
    return None


def _operator_enrich_day_performance(
    account: dict[str, Any],
    *,
    net_liquidation: Decimal,
    observed_at: datetime,
) -> None:
    day_performance = account.get("day_performance")
    if not isinstance(day_performance, dict):
        return
    start_value = _operator_stream_decimal(day_performance.get("start_net_liquidation"))
    points = day_performance.get("points")
    if not isinstance(points, list):
        points = []
        day_performance["points"] = points
    if start_value is None and points:
        start_value = _operator_stream_decimal(points[0].get("net_liquidation"))
    if start_value is None or start_value == 0:
        return

    latest_return = ((net_liquidation - start_value) / start_value) * Decimal("100")
    day_performance["latest_at"] = observed_at.isoformat()
    day_performance["latest_net_liquidation"] = _operator_plain_decimal(net_liquidation)
    day_performance["latest_return_pct"] = _operator_signed_decimal(latest_return)
    point = {
        "snapshot_at": observed_at.isoformat(),
        "net_liquidation": _operator_plain_decimal(net_liquidation),
        "return_pct": _operator_signed_decimal(latest_return) or "0.00",
    }
    latest_point_at = (
        _parse_operator_stream_time(points[-1].get("snapshot_at"))
        if points
        else None
    )
    if latest_point_at is None or observed_at > latest_point_at:
        points.append(point)
    elif points:
        points[-1] = point


def enrich_operator_snapshot_with_market_stream(
    snapshot_payload: dict[str, Any],
    stream_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    marks_by_symbol = _operator_stream_marks_by_symbol(stream_snapshot)
    if not marks_by_symbol:
        snapshot_payload["market_stream_overlay"] = {
            "applied": False,
            "reason": "no local stream marks available",
        }
        return snapshot_payload

    account_deltas: dict[str, Decimal] = {}
    account_latest_at: dict[str, datetime] = {}
    virtual_accounts = {
        account.get("account_key")
        for account in snapshot_payload.get("accounts", [])
        if isinstance(account, dict) and account.get("is_virtual")
    }
    account_position_counts: dict[str, int] = {}
    for position in snapshot_payload.get("positions", []):
        if not isinstance(position, dict):
            continue
        quantity = _operator_any_decimal(position.get("quantity"))
        if quantity is None or quantity == 0:
            continue
        account_key = position.get("account_key")
        if account_key:
            account_position_counts[account_key] = (
                account_position_counts.get(account_key, 0) + 1
            )
    account_marked_position_counts: dict[str, int] = {}
    account_stream_market_values: dict[str, Decimal] = {}
    marked_positions = 0
    for position in snapshot_payload.get("positions", []):
        if not isinstance(position, dict):
            continue
        quantity = _operator_any_decimal(position.get("quantity"))
        if quantity is None:
            continue
        mark = _operator_stream_mark_for_row(marks_by_symbol, position)
        if mark is None:
            continue
        price = mark["price"]
        old_market_value = _operator_any_decimal(position.get("market_value"))
        old_market_value_was_available = old_market_value is not None
        if old_market_value is None:
            old_market_price = _operator_stream_decimal(position.get("market_price"))
            old_market_value = quantity * old_market_price if old_market_price is not None else Decimal("0")
        market_value = quantity * price
        average_cost = _operator_stream_decimal(position.get("average_cost"))
        unrealized_pnl = (
            quantity * (price - average_cost)
            if average_cost is not None
            else None
        )
        position["market_price"] = _operator_plain_decimal(price)
        position["market_value"] = _operator_plain_decimal(market_value)
        position["unrealized_pnl"] = _operator_plain_decimal(unrealized_pnl)
        position["market_data_source"] = "market_stream"
        if mark.get("observed_at") is not None:
            position["market_price_at"] = mark["observed_at"].isoformat()
            account_latest_at[position["account_key"]] = max(
                account_latest_at.get(position["account_key"], mark["observed_at"]),
                mark["observed_at"],
            )
        account_key = position["account_key"]
        account_marked_position_counts[account_key] = (
            account_marked_position_counts.get(account_key, 0) + 1
        )
        account_stream_market_values[account_key] = (
            account_stream_market_values.get(account_key, Decimal("0"))
            + market_value
        )
        can_apply_account_delta = (
            account_key in virtual_accounts
            or (old_market_value_was_available and old_market_value != 0)
        )
        if can_apply_account_delta:
            account_deltas[account_key] = (
                account_deltas.get(account_key, Decimal("0"))
                + market_value
                - old_market_value
            )
        marked_positions += 1

    marked_orders = 0
    for order in snapshot_payload.get("open_orders", []):
        if not isinstance(order, dict):
            continue
        mark = _operator_stream_mark_for_row(marks_by_symbol, order)
        if mark is None:
            continue
        price = mark["price"]
        order["reference_market_price"] = _operator_plain_decimal(price)
        order["reference_market_price_at"] = (
            mark["observed_at"].isoformat() if mark.get("observed_at") is not None else None
        )
        order["last_market_price_direction"] = mark.get("direction")
        working_price = (
            _operator_stream_decimal(order.get("working_price"))
            or _operator_stream_decimal(order.get("limit_price"))
            or _operator_stream_decimal(order.get("stop_price"))
        )
        if working_price is not None:
            spread = working_price - price
            order["price_spread"] = _operator_signed_decimal(spread)
            order["price_spread_pct"] = (
                _operator_signed_decimal((spread / price) * Decimal("100"))
                if price != 0
                else None
            )
            order["spread_reference"] = order.get("working_price_reference") or (
                "LIMIT" if order.get("limit_price") else "STOP"
            )
        order["market_data_source"] = "market_stream"
        marked_orders += 1

    marked_accounts = 0
    for account in snapshot_payload.get("accounts", []):
        if not isinstance(account, dict):
            continue
        account_key = account.get("account_key")
        delta = account_deltas.get(account_key)
        current_net = _operator_stream_decimal(account.get("net_liquidation"))
        if current_net is None:
            continue
        valuation_method = "mark_delta"
        base_net = current_net
        stream_net = None
        if (
            account_key not in virtual_accounts
            and account_position_counts.get(account_key, 0) > 0
            and account_marked_position_counts.get(account_key)
            == account_position_counts.get(account_key)
        ):
            cash_value = _operator_any_decimal(account.get("total_cash_value"))
            if cash_value is not None:
                stream_net = cash_value + account_stream_market_values.get(
                    account_key,
                    Decimal("0"),
                )
                delta = stream_net - current_net
                valuation_method = "cash_plus_stream_positions"
        if stream_net is None:
            if delta is None:
                continue
            stream_net = current_net + delta
        account["net_liquidation"] = _operator_plain_decimal(stream_net)
        account["stream_valuation"] = {
            "source": "market_stream",
            "method": valuation_method,
            "base_net_liquidation": _operator_plain_decimal(base_net),
            "mark_delta": _operator_plain_decimal(delta),
            "stream_position_market_value": _operator_plain_decimal(
                account_stream_market_values.get(account_key),
            ),
            "marked_at": (
                account_latest_at[account_key].isoformat()
                if account_key in account_latest_at
                else None
            ),
        }
        if account_key in account_latest_at:
            _operator_enrich_day_performance(
                account,
                net_liquidation=stream_net,
                observed_at=account_latest_at[account_key],
            )
        marked_accounts += 1

    stream = _stream_payload(stream_snapshot)
    snapshot_payload["market_stream_overlay"] = {
        "applied": marked_positions > 0 or marked_orders > 0 or marked_accounts > 0,
        "marked_position_count": marked_positions,
        "marked_open_order_count": marked_orders,
        "marked_account_count": marked_accounts,
        "running": stream.get("running"),
        "desired_subscription_count": stream.get("desired_subscription_count"),
        "quote_count": stream.get("quote_count"),
    }
    return snapshot_payload
