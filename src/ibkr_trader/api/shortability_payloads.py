from __future__ import annotations

from typing import Any, Mapping

from ibkr_trader.domain.execution_payloads import parse_date
from ibkr_trader.ibkr.shortability import ShortabilityMarketDataType
from ibkr_trader.ibkr.shortability import ShortabilitySnapshotQuery
from ibkr_trader.ibkr.shortability import ShortabilitySource


def parse_shortability_snapshot_payload(
    payload: Mapping[str, Any],
) -> ShortabilitySnapshotQuery:
    """Validate the operator shortability snapshot request."""

    raw_symbols = payload.get("symbols")
    symbols: tuple[str, ...] | None = None
    if raw_symbols is not None:
        if not isinstance(raw_symbols, list) or not raw_symbols:
            raise ValueError("symbols must be a non-empty array of strings")
        symbols = tuple(str(symbol).strip().upper() for symbol in raw_symbols)
        if not all(symbols):
            raise ValueError("symbols must contain only non-empty strings")
        if len(set(symbols)) != len(symbols):
            raise ValueError("symbols must not contain duplicates")

    raw_market_data_type = str(payload.get("market_data_type", "LIVE")).strip().upper()
    normalized_market_data_type = raw_market_data_type.replace("-", "_").replace(" ", "_")
    try:
        market_data_type = ShortabilityMarketDataType(normalized_market_data_type)
    except ValueError as exc:
        raise ValueError(
            "market_data_type must be one of LIVE, FROZEN, DELAYED, DELAYED_FROZEN"
        ) from exc

    raw_source = str(
        payload.get("source", ShortabilitySource.OFFICIAL_IBKR_PAGE.value)
    ).strip()
    normalized_source = raw_source.upper().replace("-", "_").replace(" ", "_")
    source_aliases = {
        "OFFICIAL": ShortabilitySource.OFFICIAL_IBKR_PAGE,
        "OFFICIAL_PAGE": ShortabilitySource.OFFICIAL_IBKR_PAGE,
        "OFFICIAL_IBKR_PAGE": ShortabilitySource.OFFICIAL_IBKR_PAGE,
        "BROKER": ShortabilitySource.BROKER_TICKS,
        "BROKER_TICK": ShortabilitySource.BROKER_TICKS,
        "BROKER_TICKS": ShortabilitySource.BROKER_TICKS,
    }
    source = source_aliases.get(normalized_source)
    if source is None:
        raise ValueError("source must be OFFICIAL_IBKR_PAGE or BROKER_TICKS")

    query = ShortabilitySnapshotQuery(
        symbols=symbols,
        as_of_date=(
            parse_date(payload["as_of_date"], "as_of_date")
            if payload.get("as_of_date") is not None
            else None
        ),
        exchange=str(payload.get("exchange", "SMART")).upper(),
        primary_exchange=str(payload.get("primary_exchange", "SFB")).upper(),
        currency=str(payload.get("currency", "SEK")).upper(),
        security_type=str(payload.get("security_type", "STK")).upper(),
        source=source,
        only_shortable=bool(payload.get("only_shortable", True)),
        market_data_type=market_data_type,
        per_symbol_timeout_seconds=float(payload.get("per_symbol_timeout_seconds", 2.0)),
        max_concurrent=int(payload.get("max_concurrent", 25)),
        max_symbols=(
            int(payload["max_symbols"])
            if payload.get("max_symbols") is not None
            else None
        ),
    )
    query.validate()
    return query
