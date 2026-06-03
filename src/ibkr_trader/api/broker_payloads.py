from __future__ import annotations

import ipaddress
from typing import Any, Mapping

from ibkr_trader.api.shortability_payloads import parse_shortability_snapshot_payload
from ibkr_trader.domain.contract_resolution import ContractResolveQuery
from ibkr_trader.domain.execution_payloads import parse_date
from ibkr_trader.domain.execution_payloads import parse_datetime
from ibkr_trader.ibkr.account_summary import DEFAULT_ACCOUNT_SUMMARY_TAGS
from ibkr_trader.ibkr.historical_bars import HistoricalBarsQuery
from ibkr_trader.ibkr.stockholm_intraday import DEFAULT_STOCKHOLM_INTRADAY_TYPES
from ibkr_trader.ibkr.stockholm_intraday import StockholmIntradayBackfillQuery
from ibkr_trader.ibkr.tick_stream import TickStreamQuery
from ibkr_trader.ibkr.tick_stream import _normalize_tick_type


def is_loopback_host(host: str | None) -> bool:
    if not host:
        return False
    if host == "localhost":
        return True

    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def enforce_loopback_binding(host: str, *, require_loopback_only: bool) -> None:
    if require_loopback_only and not is_loopback_host(host):
        raise ValueError(
            "API host must be loopback when API_REQUIRE_LOOPBACK_ONLY is enabled."
        )


def parse_positive_limit(
    value: int,
    *,
    field_name: str,
    maximum: int,
) -> int:
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")
    if value > maximum:
        raise ValueError(f"{field_name} must be at most {maximum}")
    return value


def parse_contract_resolve_payload(payload: Mapping[str, Any]) -> ContractResolveQuery:
    query = ContractResolveQuery(
        symbol=str(payload["symbol"]).upper(),
        security_type=str(payload.get("security_type", "STK")).upper(),
        exchange=str(payload["exchange"]).upper(),
        currency=str(payload["currency"]).upper(),
        primary_exchange=(
            str(payload["primary_exchange"]).upper()
            if payload.get("primary_exchange") is not None
            else None
        ),
        local_symbol=(
            str(payload["local_symbol"])
            if payload.get("local_symbol") is not None
            else None
        ),
        include_expired=bool(payload.get("include_expired", False)),
        isin=str(payload["isin"]) if payload.get("isin") is not None else None,
    )
    query.validate()
    return query


def parse_account_summary_payload(
    payload: Mapping[str, Any],
) -> tuple[tuple[str, ...], str, str | None]:
    raw_tags = payload.get("tags")
    if raw_tags is None:
        tags = DEFAULT_ACCOUNT_SUMMARY_TAGS
    else:
        if not isinstance(raw_tags, list) or not raw_tags:
            raise ValueError("tags must be a non-empty array of strings")
        tags = tuple(str(tag) for tag in raw_tags)
        if not all(tag for tag in tags):
            raise ValueError("tags must contain only non-empty strings")

    group = str(payload.get("group", "All"))
    account_id = (
        str(payload["account_id"])
        if payload.get("account_id") is not None
        else None
    )
    return tags, group, account_id


def parse_historical_bars_payload(payload: Mapping[str, Any]) -> HistoricalBarsQuery:
    end_at = (
        parse_datetime(payload["end_at"], "end_at")
        if payload.get("end_at") is not None
        else None
    )
    query = HistoricalBarsQuery(
        symbol=str(payload["symbol"]).upper(),
        security_type=str(payload.get("security_type", "STK")).upper(),
        exchange=str(payload["exchange"]).upper(),
        currency=str(payload["currency"]).upper(),
        primary_exchange=(
            str(payload["primary_exchange"]).upper()
            if payload.get("primary_exchange") is not None
            else None
        ),
        local_symbol=(
            str(payload["local_symbol"])
            if payload.get("local_symbol") is not None
            else None
        ),
        isin=str(payload["isin"]) if payload.get("isin") is not None else None,
        duration=str(payload["duration"]),
        bar_size=str(payload["bar_size"]),
        what_to_show=str(payload.get("what_to_show", "TRADES")).upper(),
        use_rth=bool(payload.get("use_rth", True)),
        end_at=end_at,
    )
    query.validate()
    return query


def parse_stockholm_intraday_backfill_payload(
    payload: Mapping[str, Any],
) -> StockholmIntradayBackfillQuery:
    if payload.get("as_of_date") is None:
        raise ValueError("as_of_date is required")

    as_of_date = parse_date(payload["as_of_date"], "as_of_date")

    raw_what_to_show = payload.get("what_to_show")
    if raw_what_to_show is None:
        what_to_show = DEFAULT_STOCKHOLM_INTRADAY_TYPES
    else:
        if not isinstance(raw_what_to_show, list) or not raw_what_to_show:
            raise ValueError("what_to_show must be a non-empty array of strings")
        what_to_show = tuple(str(item).strip().upper() for item in raw_what_to_show)
        if not all(what_to_show):
            raise ValueError("what_to_show must contain only non-empty strings")
        if len(set(what_to_show)) != len(what_to_show):
            raise ValueError("what_to_show must not contain duplicates")

    raw_symbols = payload.get("symbols")
    symbols: tuple[str, ...] | None = None
    if raw_symbols is not None:
        if not isinstance(raw_symbols, list) or not raw_symbols:
            raise ValueError("symbols must be a non-empty array of strings")
        parsed_symbols = tuple(str(item).strip().lower() for item in raw_symbols)
        if not all(parsed_symbols):
            raise ValueError("symbols must contain only non-empty strings")
        if len(set(parsed_symbols)) != len(parsed_symbols):
            raise ValueError("symbols must not contain duplicates")
        symbols = parsed_symbols

    raw_max_runtime_seconds = payload.get("max_runtime_seconds", 55.0)
    query = StockholmIntradayBackfillQuery(
        as_of_date=as_of_date,
        bar_size=str(payload.get("bar_size", "1 min")),
        what_to_show=what_to_show,
        use_rth=bool(payload.get("use_rth", True)),
        max_symbols=int(payload.get("max_symbols", 25)),
        start_after=(
            str(payload["start_after"]).strip().lower()
            if payload.get("start_after") is not None
            else None
        ),
        symbols=symbols,
        include_remapped=bool(payload.get("include_remapped", False)),
        sleep_seconds=float(payload.get("sleep_seconds", 0.05)),
        max_runtime_seconds=(
            None
            if raw_max_runtime_seconds is None
            else float(raw_max_runtime_seconds)
        ),
    )
    query.validate()
    return query

def parse_tick_stream_payload(payload: Mapping[str, Any]) -> TickStreamQuery:
    raw_tick_types = payload.get("tick_types", ["Last", "BidAsk"])
    if not isinstance(raw_tick_types, list) or not raw_tick_types:
        raise ValueError("tick_types must be a non-empty array of strings")

    query = TickStreamQuery(
        symbol=str(payload["symbol"]).upper(),
        security_type=str(payload.get("security_type", "STK")).upper(),
        exchange=str(payload["exchange"]).upper(),
        currency=str(payload["currency"]).upper(),
        primary_exchange=(
            str(payload["primary_exchange"]).upper()
            if payload.get("primary_exchange") is not None
            else None
        ),
        local_symbol=(
            str(payload["local_symbol"])
            if payload.get("local_symbol") is not None
            else None
        ),
        isin=str(payload["isin"]) if payload.get("isin") is not None else None,
        tick_types=tuple(_normalize_tick_type(item) for item in raw_tick_types),
        duration_seconds=float(payload.get("duration_seconds", 5.0)),
        max_events=int(payload.get("max_events", 500)),
        ignore_size=bool(payload.get("ignore_size", False)),
    )
    query.validate()
    return query
