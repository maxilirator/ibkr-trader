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
    min_observed_bar_coverage_ratio: float = 0.8

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
    min_coverage_ratio = float(
        merged.get("min_observed_bar_coverage_ratio", 0.8)
    )
    if not 0.0 <= min_coverage_ratio <= 1.0:
        raise ValueError("min_observed_bar_coverage_ratio must be between 0 and 1")
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
        min_observed_bar_coverage_ratio=min_coverage_ratio,
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



__all__ = [name for name in globals() if not name.startswith("__")]
