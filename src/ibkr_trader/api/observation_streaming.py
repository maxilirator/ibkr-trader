from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy.exc import SQLAlchemyError

from ibkr_trader.ibkr.market_stream_store import list_market_stream_bars
from ibkr_trader.ibkr.market_stream_store import merge_bar_lists
from ibkr_trader.ibkr.market_stream_store import persist_market_stream_snapshot_bars
from ibkr_trader.rl.observations import observation_config_from_contract


def _session_open_for_as_of(as_of: datetime, timezone_name: str) -> datetime:
    timezone = ZoneInfo(timezone_name)
    local_as_of = as_of.astimezone(timezone) if as_of.tzinfo else as_of.replace(tzinfo=timezone)
    return datetime.combine(local_as_of.date(), time(9, 0), tzinfo=timezone)


def _completed_rl_bar_as_of(
    *,
    as_of: datetime,
    observation_contract: Mapping[str, Any] | None,
    config_overrides: Mapping[str, Any] | None,
    fallback_timezone: str,
) -> datetime:
    """Snap broker backfill requests to the latest completed model bar."""

    config = observation_config_from_contract(
        observation_contract,
        overrides=config_overrides,
    )
    timezone = ZoneInfo(config.session_timezone or fallback_timezone)
    local_as_of = as_of.astimezone(timezone) if as_of.tzinfo else as_of.replace(tzinfo=timezone)
    session_open = datetime.combine(local_as_of.date(), config.session_open, tzinfo=timezone)
    session_close = datetime.combine(local_as_of.date(), config.session_close, tzinfo=timezone)
    if local_as_of <= session_open:
        return session_open
    capped = min(local_as_of, session_close)
    elapsed_minutes = int((capped - session_open).total_seconds() // 60)
    completed_bar_count = elapsed_minutes // config.target_bar_minutes
    if completed_bar_count <= 0:
        return session_open
    completed = session_open + timedelta(
        minutes=completed_bar_count * config.target_bar_minutes,
    )
    return min(completed, session_close)


def _stream_symbols_from_snapshot(stream_snapshot: Mapping[str, Any]) -> list[str]:
    raw_symbols = stream_snapshot.get("desired_symbols")
    if isinstance(raw_symbols, list):
        return sorted({str(symbol).strip().upper() for symbol in raw_symbols if str(symbol).strip()})
    bars_by_symbol = stream_snapshot.get("bars_by_symbol")
    if isinstance(bars_by_symbol, Mapping):
        return sorted(str(symbol).strip().upper() for symbol in bars_by_symbol if str(symbol).strip())
    return []


def _merge_persisted_stream_bars(
    session_factory: Any,
    *,
    stream_snapshot: dict[str, Any],
    symbols: list[str],
    bar_limit: int,
    as_of: datetime,
    timezone_name: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        persist_result = persist_market_stream_snapshot_bars(
            session_factory,
            stream_snapshot=stream_snapshot,
        )
    except SQLAlchemyError as exc:
        persist_result = {
            "available": False,
            "error": str(exc),
            "inserted_count": 0,
            "updated_count": 0,
            "skipped_count": 0,
            "symbol_count": 0,
            "symbols": [],
            "source": "ibkr_live_market_stream_1m",
        }
    requested_symbols = symbols or _stream_symbols_from_snapshot(stream_snapshot)
    if not requested_symbols:
        stream_snapshot["persistent_bar_store"] = persist_result
        return stream_snapshot, {}

    try:
        stored_bars = list_market_stream_bars(
            session_factory,
            symbols=requested_symbols,
            started_at=_session_open_for_as_of(as_of, timezone_name),
            ended_at=as_of,
            limit_per_symbol=bar_limit,
        )
    except SQLAlchemyError as exc:
        stored_bars = {}
        persist_result = {
            **persist_result,
            "available": False,
            "read_error": str(exc),
        }
    bars_by_symbol = dict(stream_snapshot.get("bars_by_symbol") or {})
    merged_symbol_count = 0
    for symbol in requested_symbols:
        merged = merge_bar_lists(
            stored_bars.get(symbol, []),
            bars_by_symbol.get(symbol, []),
            limit=bar_limit,
        )
        if merged:
            bars_by_symbol[symbol] = merged
            merged_symbol_count += 1
    stream_snapshot["bars_by_symbol"] = bars_by_symbol
    stream_snapshot["persistent_bar_store"] = {
        **persist_result,
        "merged_symbol_count": merged_symbol_count,
    }
    return stream_snapshot, stored_bars


def _paused_market_stream_observation(
    *,
    symbol: str,
    as_of: datetime,
    session_date: date,
    reason: str,
    backfill_request: Mapping[str, Any] | None,
    min_coverage_ratio: float,
) -> dict[str, Any]:
    backfill_status = (
        str(backfill_request.get("status"))
        if isinstance(backfill_request, Mapping)
        else None
    )
    data_quality = {
        "bar_sequence_policy": "observed_provider_bars_only",
        "coverage_policy": "complete_5m_bars_since_session_open",
        "min_coverage_ratio": min_coverage_ratio,
        "expected_complete_bar_count": None,
        "observed_complete_bar_count": 0,
        "missing_complete_bar_count": None,
        "coverage_ratio": None,
        "passes": False,
        "reason": reason,
        "backfill_request": dict(backfill_request or {}),
    }
    return {
        "symbol": symbol,
        "session_date": session_date.isoformat(),
        "bar_count": 0,
        "latest_bar_started_at": None,
        "latest_bar_ended_at": None,
        "latest_bar_complete": False,
        "data_quality": data_quality,
        "model_decision": {
            "ready": False,
            "reason": reason,
            "decision_policy": "completed_5m_bar_only",
            "decision_cadence": "5m",
            "usable_bar_count": 0,
            "latest_usable_bar_started_at": None,
            "latest_usable_bar_ended_at": None,
            "decision_id": None,
            "ignore_trailing_incomplete_bar": False,
            "next_decision_at": as_of.isoformat(),
            "backfill_status": backfill_status,
            "backfill_request": dict(backfill_request or {}),
            "data_quality": data_quality,
        },
        "phase1_bars": [],
        "features": {
            "static_features_ready": False,
            "static_feature_names": [],
            "static_features": [],
            "static_features_normalized": None,
            "static_features_source": "paused_no_stream_bars",
            "history_feature_names": [],
            "history_features": [],
            "history_features_named": {},
            "base_dynamic_feature_names": [],
            "base_dynamic": [],
            "extra_dynamic_feature_names": [],
            "extra_dynamic": [],
            "path_feature_names": [],
            "path_feature_stack": [],
        },
        "pricing_context": {},
        "source_session_dates": [],
    }
