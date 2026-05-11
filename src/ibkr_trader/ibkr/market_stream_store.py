from __future__ import annotations

from datetime import datetime
from datetime import timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.models import MarketStreamBarRecord

DEFAULT_STREAM_BAR_SOURCE = "ibkr_live_market_stream_1m"


def persist_market_stream_snapshot_bars(
    session_factory: sessionmaker[Session],
    *,
    stream_snapshot: Mapping[str, Any],
    source: str = DEFAULT_STREAM_BAR_SOURCE,
) -> dict[str, Any]:
    stream = _stream_payload(stream_snapshot)
    bars_by_symbol = stream.get("bars_by_symbol")
    if not isinstance(bars_by_symbol, Mapping):
        bars_by_symbol = {}
    return persist_market_stream_bars(
        session_factory,
        bars_by_symbol=bars_by_symbol,
        instruments_by_symbol=_instruments_from_stream(stream),
        source=source,
    )


def persist_market_stream_bars(
    session_factory: sessionmaker[Session],
    *,
    bars_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
    instruments_by_symbol: Mapping[str, Mapping[str, Any]] | None = None,
    source: str = DEFAULT_STREAM_BAR_SOURCE,
) -> dict[str, Any]:
    instruments = {
        _normalize_symbol(symbol): dict(instrument)
        for symbol, instrument in (instruments_by_symbol or {}).items()
        if _normalize_symbol(symbol)
    }
    inserted = 0
    updated = 0
    skipped = 0
    symbols_seen: set[str] = set()

    with session_scope(session_factory) as session:
        for raw_symbol, raw_bars in bars_by_symbol.items():
            symbol = _normalize_symbol(raw_symbol)
            if not symbol or not isinstance(raw_bars, Sequence) or isinstance(raw_bars, (str, bytes)):
                skipped += 1
                continue
            instrument = instruments.get(symbol, {})
            for raw_bar in raw_bars:
                if not isinstance(raw_bar, Mapping):
                    skipped += 1
                    continue
                parsed = _parse_bar_payload(
                    symbol=symbol,
                    bar=raw_bar,
                    instrument=instrument,
                    source=source,
                )
                if parsed is None:
                    skipped += 1
                    continue
                symbols_seen.add(symbol)
                existing = session.execute(
                    select(MarketStreamBarRecord).where(
                        MarketStreamBarRecord.symbol == parsed["symbol"],
                        MarketStreamBarRecord.exchange == parsed["exchange"],
                        MarketStreamBarRecord.currency == parsed["currency"],
                        MarketStreamBarRecord.security_type == parsed["security_type"],
                        MarketStreamBarRecord.started_at == parsed["started_at"],
                        MarketStreamBarRecord.source == parsed["source"],
                    )
                ).scalar_one_or_none()
                if existing is None:
                    session.add(MarketStreamBarRecord(**parsed))
                    inserted += 1
                    continue
                existing.primary_exchange = parsed["primary_exchange"]
                existing.local_symbol = parsed["local_symbol"]
                existing.open_price = parsed["open_price"]
                existing.high_price = parsed["high_price"]
                existing.low_price = parsed["low_price"]
                existing.close_price = parsed["close_price"]
                existing.volume = parsed["volume"]
                existing.bar_count = parsed["bar_count"]
                existing.raw_payload = parsed["raw_payload"]
                updated += 1

    return {
        "inserted_count": inserted,
        "updated_count": updated,
        "skipped_count": skipped,
        "symbol_count": len(symbols_seen),
        "symbols": sorted(symbols_seen),
        "source": source,
    }


def list_market_stream_bars(
    session_factory: sessionmaker[Session],
    *,
    symbols: Sequence[str],
    started_at: datetime,
    ended_at: datetime,
    limit_per_symbol: int = 390,
) -> dict[str, list[dict[str, Any]]]:
    normalized_symbols = sorted({_normalize_symbol(symbol) for symbol in symbols if _normalize_symbol(symbol)})
    if not normalized_symbols:
        return {}
    started_at = _storage_datetime(started_at)
    ended_at = _storage_datetime(ended_at)
    with session_scope(session_factory) as session:
        rows = session.execute(
            select(MarketStreamBarRecord)
            .where(
                MarketStreamBarRecord.symbol.in_(normalized_symbols),
                MarketStreamBarRecord.started_at >= started_at,
                MarketStreamBarRecord.started_at <= ended_at,
            )
            .order_by(MarketStreamBarRecord.symbol.asc(), MarketStreamBarRecord.started_at.asc())
        ).scalars().all()

    grouped: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in normalized_symbols}
    for row in rows:
        grouped.setdefault(row.symbol, []).append(_serialize_bar_record(row))
    return {
        symbol: bars[-limit_per_symbol:]
        for symbol, bars in grouped.items()
        if bars
    }


def merge_bar_lists(
    *bar_lists: Sequence[Mapping[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    by_timestamp: dict[str, dict[str, Any]] = {}
    for bars in bar_lists:
        for bar in bars:
            if not isinstance(bar, Mapping):
                continue
            timestamp = str(bar.get("timestamp") or "").strip()
            if not timestamp:
                continue
            by_timestamp[timestamp] = dict(bar)
    return [
        by_timestamp[timestamp]
        for timestamp in sorted(by_timestamp)
    ][-limit:]


def _parse_bar_payload(
    *,
    symbol: str,
    bar: Mapping[str, Any],
    instrument: Mapping[str, Any],
    source: str,
) -> dict[str, Any] | None:
    started_at = _parse_datetime(bar.get("timestamp") or bar.get("started_at"))
    open_price = _parse_decimal_text(bar.get("open"))
    high_price = _parse_decimal_text(bar.get("high"))
    low_price = _parse_decimal_text(bar.get("low"))
    close_price = _parse_decimal_text(bar.get("close"))
    if (
        started_at is None
        or open_price is None
        or high_price is None
        or low_price is None
        or close_price is None
    ):
        return None
    started_at = _storage_datetime(started_at)
    return {
        "symbol": symbol,
        "exchange": _normalize_text(instrument.get("exchange")) or "SMART",
        "currency": _normalize_text(bar.get("currency"))
        or _normalize_text(instrument.get("currency"))
        or "SEK",
        "security_type": _normalize_text(instrument.get("security_type")) or "STK",
        "primary_exchange": _normalize_text(instrument.get("primary_exchange")),
        "local_symbol": _normalize_text(instrument.get("local_symbol")),
        "started_at": started_at,
        "open_price": open_price,
        "high_price": high_price,
        "low_price": low_price,
        "close_price": close_price,
        "volume": _optional_text(bar.get("volume")),
        "bar_count": _optional_text(bar.get("bar_count")),
        "source": source,
        "raw_payload": dict(bar),
    }


def _serialize_bar_record(row: MarketStreamBarRecord) -> dict[str, Any]:
    started_at = row.started_at
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)
    return {
        "timestamp": started_at.isoformat(),
        "open": row.open_price,
        "high": row.high_price,
        "low": row.low_price,
        "close": row.close_price,
        "volume": row.volume,
        "bar_count": row.bar_count,
        "currency": row.currency,
        "source": row.source,
    }


def _stream_payload(stream_snapshot: Mapping[str, Any]) -> Mapping[str, Any]:
    nested = stream_snapshot.get("stream")
    if isinstance(nested, Mapping):
        return nested
    return stream_snapshot


def _instruments_from_stream(stream: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    instruments: dict[str, dict[str, Any]] = {}
    for quote in stream.get("quotes") or []:
        if not isinstance(quote, Mapping):
            continue
        symbol = _normalize_symbol(quote.get("symbol"))
        if not symbol:
            continue
        instruments[symbol] = {
            "exchange": quote.get("exchange"),
            "currency": quote.get("currency"),
            "security_type": quote.get("security_type"),
            "primary_exchange": quote.get("primary_exchange"),
            "local_symbol": quote.get("local_symbol"),
        }
    for subscription in stream.get("subscriptions") or []:
        if not isinstance(subscription, Mapping):
            continue
        contract = subscription.get("contract")
        if not isinstance(contract, Mapping):
            continue
        symbol = _normalize_symbol(contract.get("symbol"))
        if not symbol:
            continue
        instruments.setdefault(
            symbol,
            {
                "exchange": contract.get("exchange"),
                "currency": contract.get("currency"),
                "security_type": contract.get("security_type"),
                "primary_exchange": contract.get("primary_exchange"),
                "local_symbol": contract.get("local_symbol"),
            },
        )
    return instruments


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        pass
    parts = raw.split()
    if len(parts) >= 2:
        for fmt in ("%Y%m%d %H:%M:%S", "%Y%m%d %H:%M"):
            try:
                return datetime.strptime(f"{parts[0]} {parts[1]}", fmt)
            except ValueError:
                continue
    return None


def _storage_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc)


def _parse_decimal_text(value: Any) -> str | None:
    if value is None:
        return None
    try:
        decimal_value = Decimal(str(value))
    except Exception:
        return None
    if not decimal_value.is_finite():
        return None
    return str(decimal_value)


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_text(value: Any) -> str | None:
    raw = str(value or "").strip().upper()
    return raw or None


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    raw = str(value).strip()
    return raw or None
