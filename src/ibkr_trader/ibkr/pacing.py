from __future__ import annotations

from collections import deque
from dataclasses import asdict
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from threading import RLock
import time
from typing import Callable


class BrokerPacingLimitExceeded(RuntimeError):
    """Raised when an IBKR API call would exceed a configured pacing limit."""


@dataclass(frozen=True, slots=True)
class BrokerPacingConfig:
    max_requests_per_second: float = 45.0
    request_acquire_timeout_seconds: float = 2.0
    max_market_data_lines: int = 80
    max_historical_requests_per_10_minutes: int = 50
    historical_window_seconds: float = 600.0


@dataclass(frozen=True, slots=True)
class BrokerPacingSnapshot:
    generated_at: datetime
    config: BrokerPacingConfig
    api_requests_last_second: int
    historical_requests_in_window: int
    total_api_request_permits: int
    total_historical_request_permits: int
    total_market_data_line_rejections: int
    last_api_operation: str | None
    last_historical_operation: str | None
    last_rejection: str | None


class BrokerApiPacingGovernor:
    """Process-local pacing governor for broker API requests and data lines."""

    def __init__(
        self,
        config: BrokerPacingConfig | None = None,
        *,
        monotonic: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self.config = config or BrokerPacingConfig()
        self._monotonic = monotonic or time.monotonic
        self._sleep = sleeper or time.sleep
        self._lock = RLock()
        self._api_request_times: deque[float] = deque()
        self._historical_request_times: deque[float] = deque()
        self._total_api_request_permits = 0
        self._total_historical_request_permits = 0
        self._total_market_data_line_rejections = 0
        self._last_api_operation: str | None = None
        self._last_historical_operation: str | None = None
        self._last_rejection: str | None = None

    def acquire_api_request(
        self,
        operation_name: str,
        *,
        permits: int = 1,
        timeout_seconds: float | None = None,
    ) -> None:
        max_per_second = int(self.config.max_requests_per_second)
        if max_per_second <= 0:
            return
        permits = max(1, int(permits))
        if permits > max_per_second:
            raise BrokerPacingLimitExceeded(
                f"IBKR API operation {operation_name} requests {permits} permits, "
                f"but max_requests_per_second is {max_per_second}."
            )
        timeout_seconds = (
            self.config.request_acquire_timeout_seconds
            if timeout_seconds is None
            else max(0.0, timeout_seconds)
        )
        deadline = self._monotonic() + timeout_seconds

        while True:
            wait_seconds = 0.0
            with self._lock:
                now = self._monotonic()
                self._prune_api_requests_locked(now)
                if len(self._api_request_times) + permits <= max_per_second:
                    self._api_request_times.extend([now] * permits)
                    self._total_api_request_permits += permits
                    self._last_api_operation = operation_name
                    return
                wait_seconds = max(0.0, 1.0 - (now - self._api_request_times[0]))

            now = self._monotonic()
            if now + wait_seconds > deadline:
                message = (
                    f"IBKR API pacing limit exceeded for {operation_name}: "
                    f"{max_per_second} request permits/second."
                )
                with self._lock:
                    self._last_rejection = message
                raise BrokerPacingLimitExceeded(message)
            self._sleep(min(wait_seconds, max(0.0, deadline - now)))

    def acquire_historical_request(
        self,
        operation_name: str,
        *,
        api_permits: int = 2,
        timeout_seconds: float | None = None,
    ) -> None:
        max_in_window = int(self.config.max_historical_requests_per_10_minutes)
        if max_in_window > 0:
            window_seconds = max(1.0, self.config.historical_window_seconds)
            with self._lock:
                now = self._monotonic()
                self._prune_historical_requests_locked(now, window_seconds=window_seconds)
                if len(self._historical_request_times) >= max_in_window:
                    message = (
                        f"IBKR historical pacing limit exceeded for {operation_name}: "
                        f"{max_in_window} requests/{int(window_seconds)}s."
                    )
                    self._last_rejection = message
                    raise BrokerPacingLimitExceeded(message)
                self._historical_request_times.append(now)
                self._total_historical_request_permits += 1
                self._last_historical_operation = operation_name
        self.acquire_api_request(
            operation_name,
            permits=api_permits,
            timeout_seconds=timeout_seconds,
        )

    def check_market_data_line_limit(
        self,
        *,
        requested_line_count: int,
        operation_name: str,
    ) -> None:
        max_lines = int(self.config.max_market_data_lines)
        if max_lines <= 0:
            return
        if requested_line_count <= max_lines:
            return
        message = (
            f"IBKR market data line limit exceeded for {operation_name}: "
            f"requested {requested_line_count}, limit {max_lines}."
        )
        with self._lock:
            self._total_market_data_line_rejections += 1
            self._last_rejection = message
        raise BrokerPacingLimitExceeded(message)

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            now = self._monotonic()
            self._prune_api_requests_locked(now)
            self._prune_historical_requests_locked(
                now,
                window_seconds=max(1.0, self.config.historical_window_seconds),
            )
            snapshot = BrokerPacingSnapshot(
                generated_at=datetime.now(UTC),
                config=self.config,
                api_requests_last_second=len(self._api_request_times),
                historical_requests_in_window=len(self._historical_request_times),
                total_api_request_permits=self._total_api_request_permits,
                total_historical_request_permits=self._total_historical_request_permits,
                total_market_data_line_rejections=self._total_market_data_line_rejections,
                last_api_operation=self._last_api_operation,
                last_historical_operation=self._last_historical_operation,
                last_rejection=self._last_rejection,
            )
        payload = asdict(snapshot)
        payload["generated_at"] = snapshot.generated_at.isoformat()
        return payload

    def _prune_api_requests_locked(self, now: float) -> None:
        cutoff = now - 1.0
        while self._api_request_times and self._api_request_times[0] <= cutoff:
            self._api_request_times.popleft()

    def _prune_historical_requests_locked(
        self,
        now: float,
        *,
        window_seconds: float,
    ) -> None:
        cutoff = now - window_seconds
        while self._historical_request_times and self._historical_request_times[0] <= cutoff:
            self._historical_request_times.popleft()

