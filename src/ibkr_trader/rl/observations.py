from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from decimal import Decimal
from decimal import InvalidOperation
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo


HISTORY_FEATURE_NAMES: tuple[str, ...] = (
    "prev_open_rel_close",
    "prev_high_rel_close",
    "prev_low_rel_close",
    "prev_close_rel_open",
    "prev_high_rel_low",
    "trailing_intraday_realized_vol",
    "trailing_session_count_norm",
)
BASE_DYNAMIC_FEATURE_NAMES: tuple[str, ...] = (
    "bar_norm",
    "bars_remaining_norm",
    "open_rel_prev_close",
    "open_rel_session_open",
    "prev_seen_high_rel_prev_close",
    "prev_seen_low_rel_prev_close",
    "prev_seen_close_rel_prev_close",
    "close_seen_max_rel_prev_close",
    "close_seen_min_rel_prev_close",
    "close_seen_std_rel_prev_close",
)
VOL_NORM_DYNAMIC_FEATURE_NAMES: tuple[str, ...] = (
    "vol_norm_open_rel_prev_close",
    "vol_norm_open_rel_session_open",
    "vol_norm_prev_seen_high_rel_prev_close",
    "vol_norm_prev_seen_low_rel_prev_close",
    "vol_norm_prev_seen_close_rel_prev_close",
    "vol_norm_close_seen_max_rel_prev_close",
    "vol_norm_close_seen_min_rel_prev_close",
    "vol_norm_close_seen_std_rel_prev_close",
    "vol_norm_prev_seen_range_rel_prev_close",
    "vol_norm_close_seen_range_rel_prev_close",
)
MARKET_BASE_DYNAMIC_FEATURE_NAMES: tuple[str, ...] = tuple(
    f"market_{name}" for name in BASE_DYNAMIC_FEATURE_NAMES
)
MARKET_SPREAD_DYNAMIC_FEATURE_NAMES: tuple[str, ...] = (
    "spread_open_rel_prev_close",
    "spread_prev_seen_high_rel_prev_close",
    "spread_prev_seen_low_rel_prev_close",
    "spread_prev_seen_close_rel_prev_close",
    "spread_close_seen_std_rel_prev_close",
)
OWN_PATH_FEATURE_NAMES: tuple[str, ...] = (
    "open_rel_prev_close",
    "high_rel_prev_close",
    "low_rel_prev_close",
    "close_rel_prev_close",
)
MARKET_PATH_FEATURE_NAMES: tuple[str, ...] = tuple(
    f"market_{name}" for name in OWN_PATH_FEATURE_NAMES
)
RUNTIME_DYNAMIC_FEATURE_NAMES: tuple[str, ...] = (
    "is_flat_no_pending_entry",
    "is_flat_with_pending_entry",
    "is_in_position",
    "pending_entry_anchor_prev_close",
    "pending_entry_anchor_session_open",
    "pending_entry_rel_norm",
    "has_pending_exit_tp",
    "pending_exit_tp_norm",
    "entry_price_rel_prev_close",
    "entry_price_rel_session_open",
    "unrealized_at_open",
    "bars_since_entry_fill_norm",
    "bars_since_entry_order_norm",
    "bars_since_exit_order_norm",
    "is_last_bar",
)

DEFAULT_SESSION_TIMEZONE = "Europe/Stockholm"
DEFAULT_SESSION_OPEN = time(9, 0)
DEFAULT_SESSION_CLOSE = time(17, 30)
DEFAULT_TARGET_BAR_MINUTES = 5
DEFAULT_UPDATE_CADENCE_MINUTES = 1
DEFAULT_DECISION_CADENCE_MINUTES = 5
DEFAULT_VOL_NORMALIZATION_FLOOR = 1.0e-6


@dataclass(frozen=True, slots=True)
class SourceBar:
    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None = None
    bar_count: Decimal | None = None
    currency: str | None = None


@dataclass(frozen=True, slots=True)
class Phase1Bar:
    started_at: datetime
    ended_at: datetime
    complete: bool
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None
    bar_count: Decimal | None
    source_bar_count: int


@dataclass(frozen=True, slots=True)
class ObservationConfig:
    session_timezone: str = DEFAULT_SESSION_TIMEZONE
    session_open: time = DEFAULT_SESSION_OPEN
    session_close: time = DEFAULT_SESSION_CLOSE
    target_bar_minutes: int = DEFAULT_TARGET_BAR_MINUTES
    update_cadence_minutes: int = DEFAULT_UPDATE_CADENCE_MINUTES
    decision_cadence_minutes: int = DEFAULT_DECISION_CADENCE_MINUTES
    include_incomplete_bar: bool = True
    include_market_context: bool = True
    include_vol_normalized_intraday_state: bool = True
    vol_normalization_floor: float = DEFAULT_VOL_NORMALIZATION_FLOOR

    @property
    def zoneinfo(self) -> ZoneInfo:
        return ZoneInfo(self.session_timezone)

    @property
    def expected_session_bars(self) -> int:
        open_minutes = self.session_open.hour * 60 + self.session_open.minute
        close_minutes = self.session_close.hour * 60 + self.session_close.minute
        session_minutes = close_minutes - open_minutes
        return int(math.ceil(session_minutes / self.target_bar_minutes))


def _parse_hhmm(value: Any, *, field_name: str) -> time:
    if isinstance(value, time):
        return value
    raw = str(value).strip()
    try:
        parsed = datetime.strptime(raw, "%H:%M").time()
    except ValueError as exc:
        raise ValueError(f"{field_name} must use HH:MM") from exc
    return parsed


def observation_config_from_contract(
    observation_contract: Mapping[str, Any] | None,
    *,
    overrides: Mapping[str, Any] | None = None,
) -> ObservationConfig:
    contract = dict(observation_contract or {})
    raw_overrides = dict(overrides or {})
    merged = {**contract, **raw_overrides}
    bar_interval = str(merged.get("bar_interval", "5m")).strip().lower()
    target_bar_minutes = _parse_minutes_interval(
        merged.get("target_bar_interval", bar_interval),
        field_name="target_bar_interval",
    )
    update_cadence_minutes = _parse_minutes_interval(
        merged.get("update_cadence", "1m"),
        field_name="update_cadence",
    )
    decision_cadence_minutes = _parse_minutes_interval(
        merged.get("decision_cadence", f"{target_bar_minutes}m"),
        field_name="decision_cadence",
    )
    vol_floor = float(
        merged.get("vol_normalization_floor", DEFAULT_VOL_NORMALIZATION_FLOOR)
    )
    if target_bar_minutes <= 0:
        raise ValueError("target_bar_interval must be positive")
    if update_cadence_minutes <= 0:
        raise ValueError("update_cadence must be positive")
    if decision_cadence_minutes <= 0:
        raise ValueError("decision_cadence must be positive")
    if vol_floor <= 0.0:
        raise ValueError("vol_normalization_floor must be positive")
    return ObservationConfig(
        session_timezone=str(
            merged.get("session_timezone", DEFAULT_SESSION_TIMEZONE)
        ),
        session_open=_parse_hhmm(
            merged.get("session_open_local", DEFAULT_SESSION_OPEN.strftime("%H:%M")),
            field_name="session_open_local",
        ),
        session_close=_parse_hhmm(
            merged.get("session_close_local", DEFAULT_SESSION_CLOSE.strftime("%H:%M")),
            field_name="session_close_local",
        ),
        target_bar_minutes=target_bar_minutes,
        update_cadence_minutes=update_cadence_minutes,
        decision_cadence_minutes=decision_cadence_minutes,
        include_incomplete_bar=bool(merged.get("include_incomplete_bar", True)),
        include_market_context=bool(merged.get("include_market_context", True)),
        include_vol_normalized_intraday_state=bool(
            merged.get("include_vol_normalized_intraday_state", True)
        ),
        vol_normalization_floor=vol_floor,
    )


def _parse_minutes_interval(value: Any, *, field_name: str) -> int:
    if isinstance(value, int):
        return value
    raw = str(value).strip().lower()
    if raw.endswith("mins"):
        raw = raw[:-4].strip()
    elif raw.endswith("min"):
        raw = raw[:-3].strip()
    elif raw.endswith("m"):
        raw = raw[:-1].strip()
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a minute interval") from exc


def _parse_decimal(value: Any, *, field_name: str) -> Decimal:
    if value is None:
        raise ValueError(f"{field_name} is required")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be decimal-compatible") from exc
    if not parsed.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return parsed


def _parse_optional_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _parse_bar_timestamp(value: Any, *, timezone_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value).strip()
        if not raw:
            raise ValueError("bar timestamp is required")
        normalized = raw.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            parts = raw.split()
            if len(parts) < 2:
                raise ValueError(f"Unsupported bar timestamp: {raw!r}") from None
            parsed = _parse_ibkr_datetime_parts(parts[0], parts[1], raw=raw)
    zone = ZoneInfo(timezone_name)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=zone)
    return parsed.astimezone(zone)


def _parse_ibkr_datetime_parts(date_part: str, time_part: str, *, raw: str) -> datetime:
    for fmt in ("%Y%m%d %H:%M:%S", "%Y%m%d %H:%M"):
        try:
            return datetime.strptime(f"{date_part} {time_part}", fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported IBKR bar timestamp: {raw!r}")


def _source_bar_from_payload(
    symbol: str,
    payload: Mapping[str, Any],
    *,
    timezone_name: str,
) -> SourceBar:
    timestamp_value = (
        payload.get("timestamp")
        or payload.get("date")
        or payload.get("time")
        or payload.get("started_at")
    )
    parsed = SourceBar(
        symbol=symbol.upper(),
        timestamp=_parse_bar_timestamp(timestamp_value, timezone_name=timezone_name),
        open=_parse_decimal(payload.get("open"), field_name=f"{symbol}.open"),
        high=_parse_decimal(payload.get("high"), field_name=f"{symbol}.high"),
        low=_parse_decimal(payload.get("low"), field_name=f"{symbol}.low"),
        close=_parse_decimal(payload.get("close"), field_name=f"{symbol}.close"),
        volume=_parse_optional_decimal(payload.get("volume")),
        bar_count=_parse_optional_decimal(payload.get("bar_count")),
        currency=str(payload["currency"]).upper()
        if payload.get("currency") is not None
        else None,
    )
    if min(parsed.open, parsed.high, parsed.low, parsed.close) <= 0:
        raise ValueError(f"{symbol} prices must be greater than zero")
    if parsed.low > parsed.high:
        raise ValueError(f"{symbol} low must be <= high")
    return parsed


def parse_source_bars_by_symbol(
    raw_source_bars: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    timezone_name: str,
) -> dict[str, list[SourceBar]]:
    out: dict[str, list[SourceBar]] = {}
    for raw_symbol, raw_bars in raw_source_bars.items():
        symbol = str(raw_symbol).strip().upper()
        if not symbol:
            raise ValueError("source_bars keys must be non-empty symbols")
        if not isinstance(raw_bars, Sequence) or isinstance(raw_bars, (str, bytes)):
            raise ValueError(f"source_bars.{symbol} must be an array")
        parsed_bars = [
            _source_bar_from_payload(symbol, dict(raw_bar), timezone_name=timezone_name)
            for raw_bar in raw_bars
        ]
        if not parsed_bars:
            raise ValueError(f"source_bars.{symbol} must not be empty")
        out[symbol] = sorted(parsed_bars, key=lambda bar: bar.timestamp)
    return out


def _session_bounds(session_date: date, config: ObservationConfig) -> tuple[datetime, datetime]:
    zone = config.zoneinfo
    session_open = datetime.combine(session_date, config.session_open, tzinfo=zone)
    session_close = datetime.combine(session_date, config.session_close, tzinfo=zone)
    return session_open, session_close


def _bucket_start_for_timestamp(
    timestamp: datetime,
    *,
    config: ObservationConfig,
) -> datetime | None:
    session_open, session_close = _session_bounds(timestamp.date(), config)
    if timestamp < session_open or timestamp >= session_close:
        return None
    minutes_since_open = int((timestamp - session_open).total_seconds() // 60)
    bucket_index = minutes_since_open // config.target_bar_minutes
    return session_open + timedelta(minutes=bucket_index * config.target_bar_minutes)


def aggregate_to_phase1_bars(
    source_bars: Sequence[SourceBar],
    *,
    as_of: datetime,
    config: ObservationConfig,
) -> dict[date, list[Phase1Bar]]:
    zone = config.zoneinfo
    as_of_local = as_of.astimezone(zone)
    buckets: dict[tuple[date, datetime], list[SourceBar]] = {}
    for bar in sorted(source_bars, key=lambda item: item.timestamp):
        local_timestamp = bar.timestamp.astimezone(zone)
        if local_timestamp > as_of_local:
            continue
        bucket_start = _bucket_start_for_timestamp(local_timestamp, config=config)
        if bucket_start is None:
            continue
        buckets.setdefault((bucket_start.date(), bucket_start), []).append(bar)

    sessions: dict[date, list[Phase1Bar]] = {}
    for (session_date, started_at), bucket_bars in sorted(
        buckets.items(),
        key=lambda item: (item[0][0], item[0][1]),
    ):
        session_open, session_close = _session_bounds(session_date, config)
        _ = session_open
        ended_at = min(
            started_at + timedelta(minutes=config.target_bar_minutes),
            session_close,
        )
        is_current_bucket = (
            session_date == as_of_local.date() and started_at <= as_of_local < ended_at
        )
        complete = ended_at <= as_of_local and not is_current_bucket
        if is_current_bucket and not config.include_incomplete_bar:
            continue
        ordered = sorted(bucket_bars, key=lambda item: item.timestamp)
        volume = _sum_optional_decimal(bar.volume for bar in ordered)
        bar_count = _sum_optional_decimal(bar.bar_count for bar in ordered)
        sessions.setdefault(session_date, []).append(
            Phase1Bar(
                started_at=started_at,
                ended_at=ended_at,
                complete=complete,
                open=ordered[0].open,
                high=max(bar.high for bar in ordered),
                low=min(bar.low for bar in ordered),
                close=ordered[-1].close,
                volume=volume,
                bar_count=bar_count,
                source_bar_count=len(ordered),
            )
        )
    return sessions


def _sum_optional_decimal(values: Sequence[Decimal | None]) -> Decimal | None:
    total = Decimal("0")
    seen = False
    for value in values:
        if value is None:
            continue
        seen = True
        total += value
    return total if seen else None


def _to_float(value: Decimal | float | int) -> float:
    return float(value)


def _safe_rel(numerator: float, denominator: float, *, field_name: str) -> float:
    if not math.isfinite(denominator) or denominator <= 0.0:
        raise ValueError(f"{field_name} denominator must be finite and > 0")
    value = numerator / denominator - 1.0
    if not math.isfinite(value):
        raise ValueError(f"{field_name} must be finite")
    return value


def _compute_intraday_realized_vol(bars: Sequence[Phase1Bar]) -> float:
    if not bars:
        raise ValueError("cannot compute realized vol for an empty session")
    path = [_to_float(bars[0].open)] + [_to_float(bar.close) for bar in bars]
    if any(not math.isfinite(value) or value <= 0.0 for value in path):
        raise ValueError("realized-vol prices must be finite and > 0")
    squared_sum = 0.0
    for previous, current in zip(path, path[1:]):
        squared_sum += math.log(current / previous) ** 2
    return math.sqrt(squared_sum)


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
        observations[symbol] = {
            "symbol": symbol,
            "session_date": target_date.isoformat(),
            "bar_count": len(phase1_bars_by_symbol[symbol]),
            "latest_bar_started_at": latest_bar.started_at.isoformat(),
            "latest_bar_ended_at": latest_bar.ended_at.isoformat(),
            "latest_bar_complete": latest_bar.complete,
            "model_decision": _decision_metadata(
                deployment_key=deployment_key,
                symbol=symbol,
                bars=phase1_bars_by_symbol[symbol],
                config=config,
            ),
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
