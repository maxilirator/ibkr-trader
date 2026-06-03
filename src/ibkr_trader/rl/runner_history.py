from __future__ import annotations

import json
from datetime import date
from datetime import datetime, timezone
from typing import Any, Mapping

from ibkr_trader.rl.observations import HISTORY_FEATURE_NAMES
from ibkr_trader.rl.observations import build_history_override_from_source_bars
from ibkr_trader.rl.runner_http import post_json
from ibkr_trader.rl.runner_types import LoadedModel
from ibkr_trader.rl.runner_types import STOCKHOLM_TZ


def history_override_payload(
    *,
    api_base: str,
    loaded: LoadedModel,
    candidate: Mapping[str, Any],
    trade_date: str,
    history_cache: dict[str, Any],
    duration: str,
    bar_size: str,
    timeout: int,
    allow_metadata_fallback: bool = False,
    metadata_history_only: bool = False,
) -> dict[str, Any]:
    request_payload = build_historical_bars_payload(
        candidate,
        trade_date=trade_date,
        duration=duration,
        bar_size=bar_size,
    )
    if metadata_history_only:
        return candidate_metadata_history_override_payload(
            candidate,
            trade_date=trade_date,
            allow_neutral_fallback=allow_metadata_fallback,
        )

    cache_key = _history_cache_key(
        loaded.config.model_key,
        request_payload,
        trade_date=trade_date,
    )
    cached = history_cache.get(cache_key)
    if isinstance(cached, Mapping) and cached.get("history_override"):
        return dict(cached["history_override"])

    failure_key = f"{cache_key}:failure"
    recent_failure = history_cache.get(failure_key)
    if isinstance(recent_failure, Mapping) and _is_recent_failure(recent_failure):
        if allow_metadata_fallback:
            return metadata_history_override_payload(candidate, trade_date=trade_date)
        raise RuntimeError(
            "recent history backfill failure still cooling down: "
            f"{recent_failure.get('error')}"
        )

    try:
        response = post_json(
            f"{api_base}/v1/market-data/historical-bars?timeout={timeout}",
            request_payload,
            timeout=max(timeout + 5, 30),
        )
        bars = response.get("bars", [])
        override = build_history_override_from_source_bars(
            symbol=str(request_payload["symbol"]),
            source_bars=bars,
            target_date=trade_date,
            observation_contract={
                "bar_family": "phase1_intraday_ohlc_v1",
                "bar_interval": "5m",
                "update_cadence": "1m",
                "decision_cadence": "5m",
                "session_timezone": "Europe/Stockholm",
                "session_open_local": "09:00",
                "session_close_local": "17:30",
                "include_market_context": True,
                "include_vol_normalized_intraday_state": True,
            },
        )
    except Exception as exc:
        history_cache[failure_key] = {
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
        }
        if allow_metadata_fallback:
            return candidate_metadata_history_override_payload(
                candidate,
                trade_date=trade_date,
                allow_neutral_fallback=True,
            )
        raise

    history_cache[cache_key] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "request": request_payload,
        "bar_count": len(bars),
        "history_override": override,
    }
    history_cache.pop(failure_key, None)
    return override


def candidate_metadata_history_override_payload(
    candidate: Mapping[str, Any],
    *,
    trade_date: str,
    allow_neutral_fallback: bool = False,
) -> dict[str, Any]:
    metadata = candidate_trace_metadata(candidate)
    for key in (
        "history_override",
        "rl_history_override",
        "history_features_override",
    ):
        raw_override = metadata.get(key)
        if isinstance(raw_override, Mapping):
            return _metadata_history_payload_from_mapping(
                raw_override,
                trade_date=trade_date,
                source=f"candidate_metadata.{key}",
            )

    prev_close = (
        metadata.get("yesterday_close")
        or metadata.get("previous_close")
        or metadata.get("prev_close")
    )
    for key in ("history_features", "rl_history_features", "source_history_features"):
        raw_history = metadata.get(key)
        if raw_history is not None:
            return _metadata_history_payload_from_parts(
                prev_close=prev_close,
                raw_history=raw_history,
                trade_date=trade_date,
                source=f"candidate_metadata.{key}",
            )

    if allow_neutral_fallback:
        return metadata_history_override_payload(candidate, trade_date=trade_date)
    raise RuntimeError(
        "candidate metadata has no complete history override; refusing to call "
        "IBKR historical bars because metadata_history_only is enabled"
    )


def _metadata_history_payload_from_mapping(
    override: Mapping[str, Any],
    *,
    trade_date: str,
    source: str,
) -> dict[str, Any]:
    prev_close = (
        override.get("prev_close")
        or override.get("previous_close")
        or override.get("yesterday_close")
        or (
            override.get("previous_session", {}).get("close")
            if isinstance(override.get("previous_session"), Mapping)
            else None
        )
    )
    raw_history = override.get("history_features", override)
    return _metadata_history_payload_from_parts(
        prev_close=prev_close,
        raw_history=raw_history,
        trade_date=trade_date,
        source=source,
    )


def _metadata_history_payload_from_parts(
    *,
    prev_close: Any,
    raw_history: Any,
    trade_date: str,
    source: str,
) -> dict[str, Any]:
    if prev_close is None:
        raise RuntimeError(f"{source} is missing prev_close")
    history_features = _metadata_history_features(raw_history, source=source)
    return {
        "prev_close": str(prev_close),
        "history_features": history_features,
        "source": source,
        "source_bar_interval": None,
        "target_bar_interval": "5m",
        "target_date": trade_date,
    }


def _metadata_history_features(raw_history: Any, *, source: str) -> dict[str, float]:
    if isinstance(raw_history, Mapping):
        history = {
            name: float(raw_history[name])
            for name in HISTORY_FEATURE_NAMES
            if raw_history.get(name) is not None
        }
    elif isinstance(raw_history, list):
        if len(raw_history) != len(HISTORY_FEATURE_NAMES):
            raise RuntimeError(
                f"{source} history_features vector must have "
                f"{len(HISTORY_FEATURE_NAMES)} values"
            )
        history = {
            name: float(raw_history[idx])
            for idx, name in enumerate(HISTORY_FEATURE_NAMES)
        }
    else:
        raise RuntimeError(f"{source} history_features must be an object or vector")
    missing = [name for name in HISTORY_FEATURE_NAMES if name not in history]
    if missing:
        raise RuntimeError(f"{source} history_features missing features: {missing}")
    return history


def metadata_history_override_payload(
    candidate: Mapping[str, Any],
    *,
    trade_date: str,
) -> dict[str, Any]:
    metadata = candidate_trace_metadata(candidate)
    prev_close = (
        metadata.get("yesterday_close")
        or metadata.get("previous_close")
        or metadata.get("prev_close")
    )
    if prev_close is None:
        raise RuntimeError(
            "IBKR historical backfill failed and candidate metadata has no yesterday_close"
        )
    return {
        "prev_close": str(prev_close),
        "history_features": {name: 0.0 for name in HISTORY_FEATURE_NAMES},
        "source": "candidate_metadata_yesterday_close_fallback",
        "source_bar_interval": None,
        "target_bar_interval": "5m",
        "target_date": trade_date,
        "warning": (
            "IBKR historical bars were unavailable; neutral history features were "
            "used with candidate trace.metadata.yesterday_close."
        ),
    }


def candidate_trace_metadata(candidate: Mapping[str, Any]) -> Mapping[str, Any]:
    trace = candidate.get("trace")
    if isinstance(trace, Mapping):
        metadata = trace.get("metadata")
        if isinstance(metadata, Mapping):
            return metadata
    nested = candidate.get("candidate")
    if isinstance(nested, Mapping):
        payload = nested.get("payload")
        if isinstance(payload, Mapping):
            instruction = payload.get("instruction")
            if isinstance(instruction, Mapping):
                trace = instruction.get("trace")
                if isinstance(trace, Mapping):
                    metadata = trace.get("metadata")
                    if isinstance(metadata, Mapping):
                        return metadata
    payload = candidate.get("payload")
    if isinstance(payload, Mapping):
        instruction = payload.get("instruction")
        if isinstance(instruction, Mapping):
            trace = instruction.get("trace")
            if isinstance(trace, Mapping):
                metadata = trace.get("metadata")
                if isinstance(metadata, Mapping):
                    return metadata
    return {}


def build_historical_bars_payload(
    candidate: Mapping[str, Any],
    *,
    trade_date: str,
    duration: str,
    bar_size: str,
) -> dict[str, Any]:
    instrument = candidate_instrument(candidate)
    symbol = str(candidate.get("symbol") or instrument.get("symbol") or "").upper()
    if not symbol:
        raise ValueError("candidate symbol is required for historical backfill")
    exchange, primary_exchange = ibkr_historical_exchange(
        exchange=instrument.get("exchange") or candidate.get("exchange"),
        primary_exchange=instrument.get("primary_exchange"),
    )
    target_date = date.fromisoformat(trade_date)
    end_at = datetime.combine(
        target_date,
        datetime.strptime("09:00", "%H:%M").time(),
        tzinfo=STOCKHOLM_TZ,
    )
    return {
        "symbol": symbol,
        "security_type": str(instrument.get("security_type") or "STK").upper(),
        "exchange": exchange,
        "primary_exchange": primary_exchange,
        "currency": str(instrument.get("currency") or candidate.get("currency") or "SEK").upper(),
        "isin": instrument.get("isin"),
        "duration": duration,
        "bar_size": bar_size,
        "what_to_show": "TRADES",
        "use_rth": True,
        "end_at": end_at.isoformat(),
    }


def candidate_instrument(candidate: Mapping[str, Any]) -> Mapping[str, Any]:
    nested = candidate.get("candidate")
    if isinstance(nested, Mapping):
        payload = nested.get("payload")
        if isinstance(payload, Mapping):
            instruction = payload.get("instruction")
            if isinstance(instruction, Mapping):
                instrument = instruction.get("instrument")
                if isinstance(instrument, Mapping):
                    return instrument
        instrument = nested.get("instrument")
        if isinstance(instrument, Mapping):
            return instrument
    payload = candidate.get("payload")
    if isinstance(payload, Mapping):
        instruction = payload.get("instruction")
        if isinstance(instruction, Mapping):
            instrument = instruction.get("instrument")
            if isinstance(instrument, Mapping):
                return instrument
    return {}


def ibkr_historical_exchange(
    *,
    exchange: Any,
    primary_exchange: Any,
) -> tuple[str, str | None]:
    raw_exchange = str(exchange or "").strip().upper()
    raw_primary = str(primary_exchange or "").strip().upper()
    if raw_exchange in {"", "XSTO", "STO", "STOCKHOLM"}:
        return "SMART", raw_primary or "SFB"
    return raw_exchange, raw_primary or None


def _history_cache_key(
    model_key: str,
    request_payload: Mapping[str, Any],
    *,
    trade_date: str,
) -> str:
    parts = {
        key: request_payload.get(key)
        for key in (
            "symbol",
            "security_type",
            "exchange",
            "primary_exchange",
            "currency",
            "isin",
            "duration",
            "bar_size",
            "what_to_show",
            "use_rth",
        )
    }
    parts["model_key"] = model_key
    parts["trade_date"] = trade_date
    return json.dumps(parts, sort_keys=True)


def _is_recent_failure(payload: Mapping[str, Any], *, cooldown_seconds: int = 600) -> bool:
    try:
        failed_at = datetime.fromisoformat(str(payload.get("failed_at")).replace("Z", "+00:00"))
    except ValueError:
        return False
    if failed_at.tzinfo is None:
        failed_at = failed_at.replace(tzinfo=timezone.utc)
    age_seconds = (datetime.now(timezone.utc) - failed_at.astimezone(timezone.utc)).total_seconds()
    return age_seconds < cooldown_seconds


