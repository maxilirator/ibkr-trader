from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from threading import RLock
from typing import Callable


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _serialize_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


@dataclass(frozen=True, slots=True)
class BrokerCircuitSnapshot:
    open: bool
    reason: str | None
    source: str | None
    last_error: str | None
    tripped_at: datetime | None
    open_until: datetime | None
    cooldown_seconds_remaining: int | None
    trip_count: int


class BrokerCircuitOpen(ConnectionError):
    """Raised when a broker operation is blocked by the shared circuit breaker."""


class BrokerHealthCircuit:
    """Shared broker-health gate for failures that should stop all live IBKR use."""

    def __init__(
        self,
        *,
        default_open_seconds: float = 900.0,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._default_open_seconds = max(0.0, default_open_seconds)
        self._clock = clock
        self._lock = RLock()
        self._reason: str | None = None
        self._source: str | None = None
        self._last_error: str | None = None
        self._tripped_at: datetime | None = None
        self._open_until: datetime | None = None
        self._trip_count = 0

    def trip(
        self,
        *,
        reason: str,
        source: str,
        error: str | None = None,
        duration_seconds: float | None = None,
    ) -> None:
        now = self._clock()
        duration = (
            self._default_open_seconds
            if duration_seconds is None
            else max(0.0, duration_seconds)
        )
        open_until = now + timedelta(seconds=duration)
        with self._lock:
            self._reason = reason
            self._source = source
            self._last_error = error
            self._tripped_at = now
            if self._open_until is None or self._open_until < open_until:
                self._open_until = open_until
            self._trip_count += 1

    def clear(self, *, source: str | None = None) -> None:
        with self._lock:
            _ = source
            self._reason = None
            self._source = None
            self._last_error = None
            self._tripped_at = None
            self._open_until = None

    def is_open(self) -> bool:
        with self._lock:
            return self._is_open_locked(self._clock())

    def raise_if_open(self, *, operation_name: str, source: str) -> None:
        with self._lock:
            now = self._clock()
            if not self._is_open_locked(now):
                return
            remaining = self._cooldown_seconds_remaining_locked(now)
            retry = _serialize_datetime(self._open_until)
            raise BrokerCircuitOpen(
                "Global IBKR broker circuit breaker is open"
                f" for {source}.{operation_name}; retry at {retry}"
                f" ({remaining}s remaining). Reason: {self._reason}."
                f" Last error: {self._last_error}."
            )

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            now = self._clock()
            snapshot = BrokerCircuitSnapshot(
                open=self._is_open_locked(now),
                reason=self._reason,
                source=self._source,
                last_error=self._last_error,
                tripped_at=self._tripped_at,
                open_until=self._open_until,
                cooldown_seconds_remaining=self._cooldown_seconds_remaining_locked(now),
                trip_count=self._trip_count,
            )
        payload = asdict(snapshot)
        payload["tripped_at"] = _serialize_datetime(snapshot.tripped_at)
        payload["open_until"] = _serialize_datetime(snapshot.open_until)
        return payload

    def _is_open_locked(self, now: datetime) -> bool:
        return self._open_until is not None and self._open_until > now

    def _cooldown_seconds_remaining_locked(self, now: datetime) -> int | None:
        if self._open_until is None:
            return None
        return max(0, int((self._open_until - now).total_seconds()))
