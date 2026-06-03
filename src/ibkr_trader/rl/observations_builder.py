from __future__ import annotations

from ibkr_trader.rl.observations_common import *
from ibkr_trader.rl.observations_features import *

def _market_spread_dynamic(
    base_dynamic: Sequence[Sequence[float]],
    market_base_dynamic: Sequence[Sequence[float]],
) -> list[list[float]]:
    rows: list[list[float]] = []
    for idx, base_row in enumerate(base_dynamic):
        market_row = market_base_dynamic[idx]
        rows.append(
            [
                float(base_row[2]) - float(market_row[2]),
                float(base_row[4]) - float(market_row[4]),
                float(base_row[5]) - float(market_row[5]),
                float(base_row[6]) - float(market_row[6]),
                float(base_row[9]) - float(market_row[9]),
            ]
        )
    return rows


def _serialize_decimal(value: Decimal | None) -> str | None:
    return str(value) if value is not None else None


def _serialize_phase1_bar(bar: Phase1Bar) -> dict[str, Any]:
    return {
        "started_at": bar.started_at.isoformat(),
        "ended_at": bar.ended_at.isoformat(),
        "complete": bar.complete,
        "open": _serialize_decimal(bar.open),
        "high": _serialize_decimal(bar.high),
        "low": _serialize_decimal(bar.low),
        "close": _serialize_decimal(bar.close),
        "volume": _serialize_decimal(bar.volume),
        "bar_count": _serialize_decimal(bar.bar_count),
        "source_bar_count": bar.source_bar_count,
    }


def _serialize_source_bar(bar: SourceBar) -> dict[str, Any]:
    return {
        "timestamp": bar.timestamp.isoformat(),
        "open": _serialize_decimal(bar.open),
        "high": _serialize_decimal(bar.high),
        "low": _serialize_decimal(bar.low),
        "close": _serialize_decimal(bar.close),
        "volume": _serialize_decimal(bar.volume),
        "bar_count": _serialize_decimal(bar.bar_count),
        "currency": bar.currency,
    }


def _history_vector(history_features: Mapping[str, float]) -> list[float]:
    return [float(history_features[name]) for name in HISTORY_FEATURE_NAMES]


def _decision_metadata(
    *,
    deployment_key: str,
    symbol: str,
    bars: Sequence[Phase1Bar],
    config: ObservationConfig,
) -> dict[str, Any]:
    complete_bars = [bar for bar in bars if bar.complete]
    latest_bar = bars[-1]
    if not complete_bars:
        return {
            "ready": False,
            "reason": "waiting_for_first_completed_5m_bar",
            "decision_policy": "completed_5m_bar_only",
            "decision_cadence": f"{config.decision_cadence_minutes}m",
            "usable_bar_count": 0,
            "latest_usable_bar_started_at": None,
            "latest_usable_bar_ended_at": None,
            "decision_id": None,
            "ignore_trailing_incomplete_bar": not latest_bar.complete,
            "next_decision_at": latest_bar.ended_at.isoformat(),
        }
    latest_usable = complete_bars[-1]
    trailing_incomplete = not latest_bar.complete
    next_decision_at = (
        latest_bar.ended_at
        if trailing_incomplete
        else latest_usable.ended_at + timedelta(minutes=config.decision_cadence_minutes)
    )
    decision_id = (
        f"{deployment_key}:{symbol}:{latest_usable.ended_at.isoformat()}"
    )
    return {
        "ready": True,
        "reason": "latest_completed_5m_bar_available",
        "decision_policy": "completed_5m_bar_only",
        "decision_cadence": f"{config.decision_cadence_minutes}m",
        "usable_bar_count": len(complete_bars),
        "latest_usable_bar_started_at": latest_usable.started_at.isoformat(),
        "latest_usable_bar_ended_at": latest_usable.ended_at.isoformat(),
        "decision_id": decision_id,
        "ignore_trailing_incomplete_bar": trailing_incomplete,
        "next_decision_at": next_decision_at.isoformat(),
    }


def _observed_bar_quality_metadata(
    bars: Sequence[Phase1Bar],
    *,
    config: ObservationConfig,
) -> dict[str, Any]:
    complete_bars = [bar for bar in bars if bar.complete]
    if not complete_bars:
        return {
            "bar_sequence_policy": "observed_provider_bars_only",
            "coverage_policy": "complete_5m_bars_since_session_open",
            "min_coverage_ratio": config.min_observed_bar_coverage_ratio,
            "expected_complete_bar_count": 0,
            "observed_complete_bar_count": 0,
            "missing_complete_bar_count": 0,
            "coverage_ratio": None,
            "passes": False,
            "reason": "waiting_for_first_completed_5m_bar",
        }
    latest_complete = complete_bars[-1]
    session_open, _session_close = _session_bounds(latest_complete.started_at.date(), config)
    elapsed_minutes = (
        latest_complete.ended_at - session_open
    ).total_seconds() / 60.0
    expected_complete_count = max(
        1,
        int(math.ceil(elapsed_minutes / config.target_bar_minutes)),
    )
    observed_complete_count = len(complete_bars)
    coverage_ratio = min(
        1.0,
        float(observed_complete_count / expected_complete_count),
    )
    missing_complete_count = max(
        expected_complete_count - observed_complete_count,
        0,
    )
    passes = coverage_ratio >= config.min_observed_bar_coverage_ratio
    return {
        "bar_sequence_policy": "observed_provider_bars_only",
        "coverage_policy": "complete_5m_bars_since_session_open",
        "min_coverage_ratio": config.min_observed_bar_coverage_ratio,
        "expected_complete_bar_count": expected_complete_count,
        "observed_complete_bar_count": observed_complete_count,
        "missing_complete_bar_count": missing_complete_count,
        "coverage_ratio": coverage_ratio,
        "passes": passes,
        "reason": (
            "coverage_ok"
            if passes
            else "observed_bar_coverage_below_threshold"
        ),
    }


def _feature_payload_for_symbol(
    *,
    symbol: str,
    target_bars: Sequence[Phase1Bar],
    prev_close: float,
    history_features: Mapping[str, float],
    config: ObservationConfig,
) -> dict[str, Any]:
    base_dynamic, own_path_stack, base_columns = _dynamic_features_for_bars(
        target_bars,
        prev_close=prev_close,
        expected_session_bars=config.expected_session_bars,
    )
    extra_dynamic_parts: list[list[list[float]]] = []
    extra_names: list[str] = []
    if config.include_vol_normalized_intraday_state:
        extra_dynamic_parts.append(
            _vol_norm_dynamic(
                base_columns,
                history_features=history_features,
                vol_floor=config.vol_normalization_floor,
            )
        )
        extra_names.extend(VOL_NORM_DYNAMIC_FEATURE_NAMES)
    extra_dynamic = _concat_row_parts(extra_dynamic_parts, row_count=len(target_bars))
    return {
        "symbol": symbol,
        "prev_close": prev_close,
        "session_open": _to_float(target_bars[0].open),
        "history_features_named": {
            name: float(history_features[name]) for name in HISTORY_FEATURE_NAMES
        },
        "history_features": _history_vector(history_features),
        "base_dynamic": base_dynamic,
        "extra_dynamic": extra_dynamic,
        "extra_dynamic_feature_names": extra_names,
        "own_path_feature_stack": own_path_stack,
    }


def _concat_row_parts(
    parts: Sequence[Sequence[Sequence[float]]],
    *,
    row_count: int,
) -> list[list[float]]:
    if not parts:
        return [[] for _ in range(row_count)]
    rows: list[list[float]] = []
    for row_idx in range(row_count):
        row: list[float] = []
        for part in parts:
            row.extend(float(value) for value in part[row_idx])
        rows.append(row)
    return rows


def _normalize_static_feature_payload(raw_value: Any) -> dict[str, Any]:
    if isinstance(raw_value, Mapping):
        raw_values = (
            raw_value.get("values")
            if raw_value.get("values") is not None
            else raw_value.get("static_features_norm")
            if raw_value.get("static_features_norm") is not None
            else raw_value.get("static_features")
        )
        raw_names = raw_value.get("feature_names")
        normalized = bool(raw_value.get("normalized", True))
        source = str(raw_value.get("source", "upstream_candidate_payload")).strip()
    else:
        raw_values = raw_value
        raw_names = None
        normalized = True
        source = "upstream_candidate_payload"

    if not isinstance(raw_values, Sequence) or isinstance(raw_values, (str, bytes)):
        raise ValueError("static feature payload must contain an array of values")
    values = [float(value) for value in raw_values]
    if not values:
        raise ValueError("static feature payload values must not be empty")
    if not all(math.isfinite(value) for value in values):
        raise ValueError("static feature payload values must be finite")

    if raw_names is None:
        names = [f"static_{idx}" for idx in range(len(values))]
    else:
        if not isinstance(raw_names, Sequence) or isinstance(raw_names, (str, bytes)):
            raise ValueError("static feature_names must be an array of strings")
        names = [str(name).strip() for name in raw_names]
        if len(names) != len(values):
            raise ValueError("static feature_names length must match values length")
        if not all(names):
            raise ValueError("static feature_names must contain only non-empty names")

    return {
        "feature_names": names,
        "values": values,
        "normalized": normalized,
        "source": source or "upstream_candidate_payload",
    }


def _append_extra_features(
    payload: dict[str, Any],
    *,
    feature_names: Sequence[str],
    feature_rows: Sequence[Sequence[float]],
) -> None:
    if not feature_names:
        return
    if len(payload["extra_dynamic"]) != len(feature_rows):
        raise ValueError("extra feature row count mismatch")
    payload["extra_dynamic_feature_names"].extend(feature_names)
    for idx, row in enumerate(feature_rows):
        payload["extra_dynamic"][idx].extend(float(value) for value in row)


def build_phase1_observation_payload(
    *,
    deployment_key: str,
    model_key: str,
    model_side: str,
    observation_contract: Mapping[str, Any] | None,
    action_space: Sequence[str],
    as_of: datetime,
    source_bars_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
    symbols: Sequence[str] | None = None,
    history_overrides: Mapping[str, Mapping[str, Any]] | None = None,
    static_features_by_symbol: Mapping[str, Any] | None = None,
    config_overrides: Mapping[str, Any] | None = None,
    include_source_bars: bool = False,
) -> dict[str, Any]:
    config = observation_config_from_contract(
        observation_contract,
        overrides=config_overrides,
    )
    if as_of.tzinfo is None:
        raise ValueError("as_of must include timezone information")
    if config.target_bar_minutes != 5:
        raise ValueError("phase1 RL observations currently require 5 minute bars")
    if config.update_cadence_minutes != 1:
        raise ValueError("phase1 RL observations currently refresh every 1 minute")
    if config.decision_cadence_minutes != config.target_bar_minutes:
        raise ValueError("phase1 RL decisions must use the 5 minute model bar cadence")
    parsed_source = parse_source_bars_by_symbol(
        source_bars_by_symbol,
        timezone_name=config.session_timezone,
    )
    normalized_symbols = (
        [str(symbol).strip().upper() for symbol in symbols]
        if symbols is not None
        else sorted(parsed_source)
    )
    normalized_symbols = [symbol for symbol in normalized_symbols if symbol]
    if not normalized_symbols:
        raise ValueError("symbols must not be empty")
    missing_source = [symbol for symbol in normalized_symbols if symbol not in parsed_source]
    if missing_source:
        raise ValueError(f"source_bars missing symbols: {missing_source}")

    zone = config.zoneinfo
    as_of_local = as_of.astimezone(zone)
    target_date = as_of_local.date()
    raw_history_overrides = {
        str(symbol).strip().upper(): dict(value)
        for symbol, value in dict(history_overrides or {}).items()
    }
    normalized_static_features = {
        str(symbol).strip().upper(): _normalize_static_feature_payload(value)
        for symbol, value in dict(static_features_by_symbol or {}).items()
    }
    symbol_payloads: dict[str, dict[str, Any]] = {}
    phase1_bars_by_symbol: dict[str, list[Phase1Bar]] = {}
    sessions_by_symbol: dict[str, dict[date, list[Phase1Bar]]] = {}
    for symbol in normalized_symbols:
        sessions = aggregate_to_phase1_bars(
            parsed_source[symbol],
            as_of=as_of_local,
            config=config,
        )
        if target_date not in sessions or not sessions[target_date]:
            raise ValueError(
                f"No target-session bars for {symbol} on {target_date.isoformat()}"
            )
        prev_close, history_features = _history_for_symbol(
            sessions,
            target_date=target_date,
            override=raw_history_overrides.get(symbol),
        )
        target_bars = sessions[target_date]
        phase1_bars_by_symbol[symbol] = target_bars
        sessions_by_symbol[symbol] = sessions
        symbol_payloads[symbol] = _feature_payload_for_symbol(
            symbol=symbol,
            target_bars=target_bars,
            prev_close=prev_close,
            history_features=history_features,
            config=config,
        )

    market_context_payload: dict[str, Any] | None = None
    if config.include_market_context:
        market_base_dynamic, market_path_stack, counts_by_bar = _build_market_context(
            symbol_payloads,
            expected_session_bars=config.expected_session_bars,
        )
        market_context_payload = {
            "base_dynamic_feature_names": list(MARKET_BASE_DYNAMIC_FEATURE_NAMES),
            "path_feature_names": list(MARKET_PATH_FEATURE_NAMES),
            "counts_by_bar": counts_by_bar,
            "base_dynamic": market_base_dynamic,
            "path_feature_stack": market_path_stack,
        }
        for symbol, payload in symbol_payloads.items():
            n_bars = len(payload["base_dynamic"])
            _append_extra_features(
                payload,
                feature_names=MARKET_BASE_DYNAMIC_FEATURE_NAMES,
                feature_rows=market_base_dynamic[:n_bars],
            )
            _append_extra_features(
                payload,
                feature_names=MARKET_SPREAD_DYNAMIC_FEATURE_NAMES,
                feature_rows=_market_spread_dynamic(
                    payload["base_dynamic"],
                    market_base_dynamic[:n_bars],
                ),
            )
            payload["path_feature_stack"] = [
                list(payload["own_path_feature_stack"][idx])
                + list(market_path_stack[idx])
                for idx in range(n_bars)
            ]
            payload["path_feature_names"] = list(OWN_PATH_FEATURE_NAMES + MARKET_PATH_FEATURE_NAMES)
    for payload in symbol_payloads.values():
        payload.setdefault("path_feature_stack", payload["own_path_feature_stack"])
        payload.setdefault("path_feature_names", list(OWN_PATH_FEATURE_NAMES))

    observations = {}
    for symbol, payload in symbol_payloads.items():
        latest_bar = phase1_bars_by_symbol[symbol][-1]
        static_payload = normalized_static_features.get(symbol)
        data_quality = _observed_bar_quality_metadata(
            phase1_bars_by_symbol[symbol],
            config=config,
        )
        model_decision = _decision_metadata(
            deployment_key=deployment_key,
            symbol=symbol,
            bars=phase1_bars_by_symbol[symbol],
            config=config,
        )
        if model_decision.get("ready") and not data_quality["passes"]:
            model_decision = {
                **model_decision,
                "ready": False,
                "reason": "paused_observed_bar_coverage_below_threshold",
                "decision_id": None,
                "data_quality": data_quality,
            }
        observations[symbol] = {
            "symbol": symbol,
            "session_date": target_date.isoformat(),
            "bar_count": len(phase1_bars_by_symbol[symbol]),
            "latest_bar_started_at": latest_bar.started_at.isoformat(),
            "latest_bar_ended_at": latest_bar.ended_at.isoformat(),
            "latest_bar_complete": latest_bar.complete,
            "data_quality": data_quality,
            "model_decision": model_decision,
            "phase1_bars": [
                _serialize_phase1_bar(bar) for bar in phase1_bars_by_symbol[symbol]
            ],
            "features": {
                "static_features_ready": static_payload is not None,
                "static_feature_names": (
                    static_payload["feature_names"] if static_payload is not None else []
                ),
                "static_features": (
                    static_payload["values"] if static_payload is not None else []
                ),
                "static_features_normalized": (
                    bool(static_payload["normalized"])
                    if static_payload is not None
                    else None
                ),
                "static_features_source": (
                    static_payload["source"] if static_payload is not None else "missing"
                ),
                "history_feature_names": list(HISTORY_FEATURE_NAMES),
                "history_features": payload["history_features"],
                "history_features_named": payload["history_features_named"],
                "base_dynamic_feature_names": list(BASE_DYNAMIC_FEATURE_NAMES),
                "base_dynamic": payload["base_dynamic"],
                "extra_dynamic_feature_names": payload["extra_dynamic_feature_names"],
                "extra_dynamic": payload["extra_dynamic"],
                "path_feature_names": payload["path_feature_names"],
                "path_feature_stack": payload["path_feature_stack"],
            },
            "pricing_context": {
                "prev_close": str(payload["prev_close"]),
                "session_open": str(payload["session_open"]),
            },
            "source_session_dates": [
                session_date.isoformat()
                for session_date in sorted(sessions_by_symbol[symbol])
            ],
        }
        if include_source_bars:
            observations[symbol]["source_bars"] = [
                _serialize_source_bar(bar) for bar in parsed_source[symbol]
            ]

    return {
        "deployment_key": deployment_key,
        "model_key": model_key,
        "model_side": model_side.upper(),
        "action_space": [str(action).lower() for action in action_space],
        "as_of": as_of_local.isoformat(),
        "symbols": normalized_symbols,
        "input_contract": {
            "bar_family": "phase1_intraday_ohlc_v1",
            "bar_interval": "5m",
            "refresh_cadence": "1m",
            "update_cadence": "1m",
            "decision_cadence": "5m",
            "decision_policy": "completed_5m_bar_only",
            "bar_sequence_policy": "observed_provider_bars_only",
            "min_observed_bar_coverage_ratio": (
                config.min_observed_bar_coverage_ratio
            ),
            "source_adapter": "ibkr_1m_trades_to_phase1_5m_ohlc_v1",
            "source_bar_interval": "1m",
            "session_timezone": config.session_timezone,
            "session_open_local": config.session_open.strftime("%H:%M"),
            "session_close_local": config.session_close.strftime("%H:%M"),
            "expected_session_bars": config.expected_session_bars,
            "growing_day_prefix": True,
            "current_bar_policy": "include_incomplete_5m_bar_for_monitoring_only",
            "include_market_context": config.include_market_context,
            "include_vol_normalized_intraday_state": (
                config.include_vol_normalized_intraday_state
            ),
            "vol_normalization_floor": config.vol_normalization_floor,
            "requires_static_features": True,
            "static_feature_policy": (
                "upstream must provide the promoted model's normalized static "
                "candidate feature vector"
            ),
        },
        "feature_schema": {
            "history_feature_names": list(HISTORY_FEATURE_NAMES),
            "base_dynamic_feature_names": list(BASE_DYNAMIC_FEATURE_NAMES),
            "vol_normalized_dynamic_feature_names": list(
                VOL_NORM_DYNAMIC_FEATURE_NAMES
            ),
            "market_base_dynamic_feature_names": list(
                MARKET_BASE_DYNAMIC_FEATURE_NAMES
            ),
            "market_spread_dynamic_feature_names": list(
                MARKET_SPREAD_DYNAMIC_FEATURE_NAMES
            ),
            "own_path_feature_names": list(OWN_PATH_FEATURE_NAMES),
            "market_path_feature_names": list(MARKET_PATH_FEATURE_NAMES),
            "runtime_dynamic_feature_names": list(RUNTIME_DYNAMIC_FEATURE_NAMES),
            "model_input_component_order": [
                "static_features",
                "base_dynamic[current_bar]",
                "extra_dynamic[current_bar]",
                "runtime_dynamic_from_runner_state",
                "history_features",
                "path_feature_stack_padded_to_expected_session_bars",
            ],
            "path_pad_length": config.expected_session_bars,
        },
        "market_context": market_context_payload,
        "observations": observations,
    }


__all__ = [
    "BASE_DYNAMIC_FEATURE_NAMES",
    "HISTORY_FEATURE_NAMES",
    "MARKET_BASE_DYNAMIC_FEATURE_NAMES",
    "MARKET_PATH_FEATURE_NAMES",
    "MARKET_SPREAD_DYNAMIC_FEATURE_NAMES",
    "OWN_PATH_FEATURE_NAMES",
    "RUNTIME_DYNAMIC_FEATURE_NAMES",
    "VOL_NORM_DYNAMIC_FEATURE_NAMES",
    "aggregate_to_phase1_bars",
    "build_history_override_from_source_bars",
    "build_phase1_observation_payload",
    "observation_config_from_contract",
    "parse_source_bars_by_symbol",
]

__all__ = [name for name in globals() if not name.startswith("__")]
