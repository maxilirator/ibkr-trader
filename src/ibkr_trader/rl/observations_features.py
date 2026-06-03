from __future__ import annotations

from ibkr_trader.rl.observations_common import *

def _history_from_prior_sessions(
    sessions: Mapping[date, Sequence[Phase1Bar]],
    *,
    target_date: date,
) -> tuple[float, dict[str, float]]:
    prior_dates = sorted(session_date for session_date in sessions if session_date < target_date)
    if not prior_dates:
        raise ValueError(
            "Need prior intraday sessions or a history override to build RL history features."
        )
    realized_vols = [
        _compute_intraday_realized_vol(sessions[session_date])
        for session_date in prior_dates
    ]
    prev_session = list(sessions[prior_dates[-1]])
    prev_open = _to_float(prev_session[0].open)
    prev_high = max(_to_float(bar.high) for bar in prev_session)
    prev_low = min(_to_float(bar.low) for bar in prev_session)
    prev_close = _to_float(prev_session[-1].close)
    trailing_vols = realized_vols[-20:]
    history = {
        "prev_open_rel_close": _safe_rel(
            prev_open,
            prev_close,
            field_name="prev_open_rel_close",
        ),
        "prev_high_rel_close": _safe_rel(
            prev_high,
            prev_close,
            field_name="prev_high_rel_close",
        ),
        "prev_low_rel_close": _safe_rel(
            prev_low,
            prev_close,
            field_name="prev_low_rel_close",
        ),
        "prev_close_rel_open": _safe_rel(
            prev_close,
            prev_open,
            field_name="prev_close_rel_open",
        ),
        "prev_high_rel_low": _safe_rel(
            prev_high,
            prev_low,
            field_name="prev_high_rel_low",
        ),
        "trailing_intraday_realized_vol": float(sum(trailing_vols) / len(trailing_vols)),
        "trailing_session_count_norm": float(min(len(trailing_vols), 20) / 20.0),
    }
    return prev_close, history


def _history_from_override(
    override: Mapping[str, Any],
) -> tuple[float, dict[str, float]]:
    prev_close_value = (
        override.get("prev_close")
        or override.get("previous_close")
        or (
            override.get("previous_session", {}).get("close")
            if isinstance(override.get("previous_session"), Mapping)
            else None
        )
    )
    prev_close = _to_float(_parse_decimal(prev_close_value, field_name="prev_close"))
    raw_history = override.get("history_features", override)
    if isinstance(raw_history, Sequence) and not isinstance(raw_history, (str, bytes)):
        if len(raw_history) != len(HISTORY_FEATURE_NAMES):
            raise ValueError(
                f"history_features vector must have {len(HISTORY_FEATURE_NAMES)} values"
            )
        history = {
            name: float(raw_history[idx])
            for idx, name in enumerate(HISTORY_FEATURE_NAMES)
        }
    elif isinstance(raw_history, Mapping):
        history = {
            name: float(raw_history[name])
            for name in HISTORY_FEATURE_NAMES
            if raw_history.get(name) is not None
        }
    else:
        raise ValueError("history_features must be an object or vector")
    missing = [name for name in HISTORY_FEATURE_NAMES if name not in history]
    if missing:
        raise ValueError(f"history override missing features: {missing}")
    return prev_close, history


def _history_for_symbol(
    sessions: Mapping[date, Sequence[Phase1Bar]],
    *,
    target_date: date,
    override: Mapping[str, Any] | None,
) -> tuple[float, dict[str, float]]:
    if override:
        return _history_from_override(override)
    return _history_from_prior_sessions(sessions, target_date=target_date)


def build_history_override_from_source_bars(
    *,
    symbol: str,
    source_bars: Sequence[Mapping[str, Any]],
    target_date: date | str,
    observation_contract: Mapping[str, Any] | None = None,
    config_overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the history override the live runner needs from prior 1-minute bars."""

    normalized_symbol = str(symbol).strip().upper()
    if not normalized_symbol:
        raise ValueError("symbol is required")
    resolved_target_date = (
        target_date
        if isinstance(target_date, date)
        else date.fromisoformat(str(target_date))
    )
    config = observation_config_from_contract(
        observation_contract,
        overrides=config_overrides,
    )
    parsed_source = parse_source_bars_by_symbol(
        {normalized_symbol: source_bars},
        timezone_name=config.session_timezone,
    )
    _, session_close = _session_bounds(resolved_target_date, config)
    sessions = aggregate_to_phase1_bars(
        parsed_source[normalized_symbol],
        as_of=session_close,
        config=config,
    )
    prev_close, history_features = _history_from_prior_sessions(
        sessions,
        target_date=resolved_target_date,
    )
    return {
        "prev_close": str(prev_close),
        "history_features": {
            name: float(history_features[name]) for name in HISTORY_FEATURE_NAMES
        },
        "source": "historical_source_bars_prior_sessions",
        "source_bar_interval": "1m",
        "target_bar_interval": f"{config.target_bar_minutes}m",
        "target_date": resolved_target_date.isoformat(),
    }


def _prefix_std(values: Sequence[float], idx: int) -> float:
    if idx <= 0:
        return 0.0
    prefix = values[:idx]
    mean = sum(prefix) / len(prefix)
    variance = max(sum((value - mean) ** 2 for value in prefix) / len(prefix), 0.0)
    return math.sqrt(variance)


def _dynamic_features_for_bars(
    bars: Sequence[Phase1Bar],
    *,
    prev_close: float,
    expected_session_bars: int,
) -> tuple[list[list[float]], list[list[float]], dict[str, list[float]]]:
    if not bars:
        raise ValueError("target session has no model-facing bars")
    session_open = _to_float(bars[0].open)
    denominator = max(float(expected_session_bars - 1), 1.0)
    opens = [_to_float(bar.open) for bar in bars]
    highs = [_to_float(bar.high) for bar in bars]
    lows = [_to_float(bar.low) for bar in bars]
    closes = [_to_float(bar.close) for bar in bars]
    base_dynamic: list[list[float]] = []
    path_stack: list[list[float]] = []
    named_columns: dict[str, list[float]] = {name: [] for name in BASE_DYNAMIC_FEATURE_NAMES}

    for idx, bar in enumerate(bars):
        if idx == 0:
            prev_seen_high = session_open
            prev_seen_low = session_open
            prev_seen_close = session_open
            close_seen_max = session_open
            close_seen_min = session_open
        else:
            prev_seen_high = max(highs[:idx])
            prev_seen_low = min(lows[:idx])
            prev_seen_close = closes[idx - 1]
            close_seen_max = max(closes[:idx])
            close_seen_min = min(closes[:idx])
        close_seen_std = _prefix_std(closes, idx)
        row = [
            float(idx) / denominator,
            max((float(expected_session_bars - 1) - float(idx)) / denominator, 0.0),
            _safe_rel(opens[idx], prev_close, field_name="open_rel_prev_close"),
            _safe_rel(opens[idx], session_open, field_name="open_rel_session_open"),
            _safe_rel(prev_seen_high, prev_close, field_name="prev_seen_high"),
            _safe_rel(prev_seen_low, prev_close, field_name="prev_seen_low"),
            _safe_rel(prev_seen_close, prev_close, field_name="prev_seen_close"),
            _safe_rel(close_seen_max, prev_close, field_name="close_seen_max"),
            _safe_rel(close_seen_min, prev_close, field_name="close_seen_min"),
            close_seen_std / prev_close,
        ]
        base_dynamic.append(row)
        path_stack.append(
            [
                row[2],
                _safe_rel(_to_float(bar.high), prev_close, field_name="high_rel_prev_close"),
                _safe_rel(_to_float(bar.low), prev_close, field_name="low_rel_prev_close"),
                _safe_rel(_to_float(bar.close), prev_close, field_name="close_rel_prev_close"),
            ]
        )
        for column_idx, name in enumerate(BASE_DYNAMIC_FEATURE_NAMES):
            named_columns[name].append(row[column_idx])
    return base_dynamic, path_stack, named_columns


def _vol_norm_dynamic(
    base_columns: Mapping[str, Sequence[float]],
    *,
    history_features: Mapping[str, float],
    vol_floor: float,
) -> list[list[float]]:
    trailing_vol = max(
        float(history_features["trailing_intraday_realized_vol"]),
        vol_floor,
    )
    names_to_scale = (
        "open_rel_prev_close",
        "open_rel_session_open",
        "prev_seen_high_rel_prev_close",
        "prev_seen_low_rel_prev_close",
        "prev_seen_close_rel_prev_close",
        "close_seen_max_rel_prev_close",
        "close_seen_min_rel_prev_close",
        "close_seen_std_rel_prev_close",
    )
    row_count = len(base_columns["open_rel_prev_close"])
    rows: list[list[float]] = []
    for idx in range(row_count):
        prev_seen_range = (
            base_columns["prev_seen_high_rel_prev_close"][idx]
            - base_columns["prev_seen_low_rel_prev_close"][idx]
        )
        close_seen_range = (
            base_columns["close_seen_max_rel_prev_close"][idx]
            - base_columns["close_seen_min_rel_prev_close"][idx]
        )
        rows.append(
            [float(base_columns[name][idx]) / trailing_vol for name in names_to_scale]
            + [prev_seen_range / trailing_vol, close_seen_range / trailing_vol]
        )
    return rows


def _build_market_context(
    symbol_payloads: Mapping[str, Mapping[str, Any]],
    *,
    expected_session_bars: int,
) -> tuple[list[list[float]], list[list[float]], list[int]]:
    max_bars = max(
        len(payload["own_path_feature_stack"])
        for payload in symbol_payloads.values()
    )
    market_path_stack: list[list[float]] = []
    for idx in range(max_bars):
        sums = [0.0, 0.0, 0.0, 0.0]
        count = 0
        for payload in symbol_payloads.values():
            path_stack = payload["own_path_feature_stack"]
            if idx >= len(path_stack):
                continue
            for feature_idx, value in enumerate(path_stack[idx]):
                sums[feature_idx] += float(value)
            count += 1
        if count <= 0:
            break
        market_path_stack.append([value / float(count) for value in sums])

    pseudo_bars = [
        Phase1Bar(
            started_at=datetime.min,
            ended_at=datetime.min,
            complete=True,
            open=Decimal(str(1.0 + row[0])),
            high=Decimal(str(1.0 + row[1])),
            low=Decimal(str(1.0 + row[2])),
            close=Decimal(str(1.0 + row[3])),
            volume=None,
            bar_count=None,
            source_bar_count=1,
        )
        for row in market_path_stack
    ]
    market_base_dynamic, market_path_stack, _ = _dynamic_features_for_bars(
        pseudo_bars,
        prev_close=1.0,
        expected_session_bars=expected_session_bars,
    )
    counts_by_bar = []
    for idx in range(len(market_path_stack)):
        counts_by_bar.append(
            sum(
                1
                for payload in symbol_payloads.values()
                if idx < len(payload["own_path_feature_stack"])
            )
        )
    return market_base_dynamic, market_path_stack, counts_by_bar



__all__ = [name for name in globals() if not name.startswith("__")]
