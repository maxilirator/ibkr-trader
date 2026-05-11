from __future__ import annotations

from contextlib import contextmanager
import logging
import os
import time
from dataclasses import asdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from collections import deque
from threading import RLock
from typing import Any, Callable, Iterator

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.ibkr.broker_circuit import BrokerCircuitOpen
from ibkr_trader.ibkr.broker_circuit import BrokerHealthCircuit
from ibkr_trader.ibkr.gateway_diagnostics import format_gateway_diagnostic_hint
from ibkr_trader.ibkr.gateway_diagnostics import read_ibgateway_diagnostics
from ibkr_trader.ibkr.pacing import BrokerApiPacingGovernor
from ibkr_trader.ibkr.sync_wrapper import load_sync_wrapper_class

LOGGER = logging.getLogger(__name__)
API_STARTUP_FAILURE_MARKERS = (
    "api startup",
    "gateway did not complete",
    "nextvalidid",
    "socket connected but api startup",
)
STUCK_GATEWAY_CIRCUIT_STATUSES = {
    "deadlock_reported",
    "existing_session_detected",
    "stuck_shutdown",
    "stuck_shutdown_after_existing_session",
}
HISTORICAL_BROKER_OPERATIONS = {
    "historical_bars",
    "rl_observation_stream_backfill",
    "stockholm_intraday_backfill",
}
BROKER_OPERATION_API_PERMITS = {
    "heartbeat_probe": 2,
    "probe": 2,
    "historical_bars": 2,
    "rl_observation_stream_backfill": 2,
    "stockholm_intraday_backfill": 2,
    "broker_runtime_snapshot": 5,
    "runtime_snapshot": 5,
    "runtime_reconciliation_snapshot": 3,
    "persisted_entry_submit": 2,
    "runtime_exit_submit": 2,
    "broker_cancel": 1,
}


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _is_connected(app: Any) -> bool:
    checker = getattr(app, "isConnected", None)
    if not callable(checker):
        return True

    try:
        return bool(checker())
    except Exception:
        return False


def _serialize_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


@dataclass(slots=True, frozen=True)
class ManagedSessionMetrics:
    connect_attempt_count: int
    connect_success_count: int
    disconnect_count: int
    checkout_count: int
    failed_checkout_count: int
    connect_attempts_last_60_seconds: int
    checkouts_last_60_seconds: int
    last_connect_attempt_at: datetime | None
    last_connect_success_at: datetime | None
    last_disconnect_at: datetime | None
    last_checkout_at: datetime | None


@dataclass(slots=True, frozen=True)
class ManagedSessionStatus:
    role: str
    host: str
    port: int
    client_id: int
    connected: bool
    last_error: str | None
    consecutive_failures: int
    cooldown_until: datetime | None
    cooldown_seconds_remaining: int | None
    circuit_breaker_reason: str | None
    circuit_breaker_until: datetime | None
    metrics: ManagedSessionMetrics


@dataclass(slots=True, frozen=True)
class BrokerOperationRecord:
    role: str
    operation_name: str
    started_at: datetime
    completed_at: datetime
    duration_ms: int
    lock_wait_ms: int
    success: bool
    error: str | None


class BrokerActivityTracker:
    def __init__(self, *, recent_limit: int = 200) -> None:
        self._lock = RLock()
        self._operations: deque[BrokerOperationRecord] = deque(maxlen=recent_limit)

    def record(
        self,
        *,
        role: str,
        operation_name: str,
        started_at: datetime,
        completed_at: datetime,
        success: bool,
        error: str | None,
        lock_wait_ms: int = 0,
    ) -> None:
        duration_ms = max(
            0,
            int((completed_at - started_at).total_seconds() * 1000),
        )
        with self._lock:
            self._operations.append(
                BrokerOperationRecord(
                    role=role,
                    operation_name=operation_name,
                    started_at=started_at,
                    completed_at=completed_at,
                    duration_ms=duration_ms,
                    lock_wait_ms=max(0, lock_wait_ms),
                    success=success,
                    error=error,
                )
            )

    def snapshot(self, *, recent_limit: int = 20) -> dict[str, Any]:
        now = _utc_now()
        cutoff = now.timestamp() - 60
        with self._lock:
            operations = list(self._operations)

        total_operations = len(operations)
        successful_operations = sum(1 for item in operations if item.success)
        failed_operations = total_operations - successful_operations
        operations_last_60_seconds = sum(
            1 for item in operations if item.started_at.timestamp() >= cutoff
        )

        per_operation: dict[str, dict[str, int]] = {}
        for item in operations:
            bucket = per_operation.setdefault(
                item.operation_name,
                {"total": 0, "success": 0, "failure": 0},
            )
            bucket["total"] += 1
            if item.success:
                bucket["success"] += 1
            else:
                bucket["failure"] += 1

        recent = [
            {
                "role": item.role,
                "operation_name": item.operation_name,
                "started_at": _serialize_datetime(item.started_at),
                "completed_at": _serialize_datetime(item.completed_at),
                "duration_ms": item.duration_ms,
                "lock_wait_ms": item.lock_wait_ms,
                "success": item.success,
                "error": item.error,
            }
            for item in operations[-recent_limit:]
        ]

        return {
            "generated_at": _serialize_datetime(now),
            "total_operations": total_operations,
            "successful_operations": successful_operations,
            "failed_operations": failed_operations,
            "operations_last_60_seconds": operations_last_60_seconds,
            "per_operation": per_operation,
            "recent_operations": recent,
        }

def serialize_managed_session_status(status: ManagedSessionStatus) -> dict[str, Any]:
    payload = asdict(status)
    payload["cooldown_until"] = _serialize_datetime(status.cooldown_until)
    payload["circuit_breaker_until"] = _serialize_datetime(status.circuit_breaker_until)
    payload["metrics"]["last_connect_attempt_at"] = _serialize_datetime(
        status.metrics.last_connect_attempt_at
    )
    payload["metrics"]["last_connect_success_at"] = _serialize_datetime(
        status.metrics.last_connect_success_at
    )
    payload["metrics"]["last_disconnect_at"] = _serialize_datetime(
        status.metrics.last_disconnect_at
    )
    payload["metrics"]["last_checkout_at"] = _serialize_datetime(
        status.metrics.last_checkout_at
    )
    return payload


class ManagedSyncSession:
    def __init__(
        self,
        role: str,
        config: IbkrConnectionConfig,
        *,
        wrapper_cls: type[Any] | None = None,
        default_timeout: int = 30,
        activity_tracker: BrokerActivityTracker | None = None,
        initial_connect_backoff_seconds: float = 5.0,
        max_connect_backoff_seconds: float = 300.0,
        api_startup_failure_slow_probe_seconds: float | None = None,
        gateway_diagnostics_reader: Callable[[], dict[str, Any]] | None = None,
        pacing_governor: BrokerApiPacingGovernor | None = None,
        broker_circuit: BrokerHealthCircuit | None = None,
    ) -> None:
        self.role = role
        self.config = config
        self._wrapper_cls = wrapper_cls
        self._default_timeout = default_timeout
        self._activity_tracker = activity_tracker
        self._initial_connect_backoff_seconds = max(0.0, initial_connect_backoff_seconds)
        self._max_connect_backoff_seconds = max(
            self._initial_connect_backoff_seconds,
            max_connect_backoff_seconds,
        )
        if api_startup_failure_slow_probe_seconds is None:
            raw_slow_probe = os.getenv(
                "IBKR_API_STARTUP_FAILURE_SLOW_PROBE_SECONDS",
                "900",
            )
            try:
                api_startup_failure_slow_probe_seconds = float(raw_slow_probe)
            except ValueError:
                api_startup_failure_slow_probe_seconds = 900.0
        self._api_startup_failure_slow_probe_seconds = max(
            0.0,
            api_startup_failure_slow_probe_seconds,
        )
        self._gateway_diagnostics_reader = gateway_diagnostics_reader or (
            lambda: read_ibgateway_diagnostics()
        )
        self._pacing_governor = pacing_governor
        self._broker_circuit = broker_circuit
        self._lock = RLock()
        self._app: Any | None = None
        self._last_error: str | None = None
        self._consecutive_failures = 0
        self._cooldown_until: datetime | None = None
        self._circuit_breaker_reason: str | None = None
        self._circuit_breaker_until: datetime | None = None
        self._connect_attempt_count = 0
        self._connect_success_count = 0
        self._disconnect_count = 0
        self._checkout_count = 0
        self._failed_checkout_count = 0
        self._connect_attempt_times: deque[datetime] = deque()
        self._checkout_times: deque[datetime] = deque()
        self._last_connect_attempt_at: datetime | None = None
        self._last_connect_success_at: datetime | None = None
        self._last_disconnect_at: datetime | None = None
        self._last_checkout_at: datetime | None = None

    def _operation_api_permits(self, operation_name: str) -> int:
        return BROKER_OPERATION_API_PERMITS.get(operation_name, 1)

    def _pace_connection_attempt_locked(self, operation_name: str) -> None:
        if self._pacing_governor is None:
            return
        self._pacing_governor.acquire_api_request(
            f"{self.role}.connect.{operation_name}",
            permits=1,
        )

    def _pace_operation_locked(self, operation_name: str) -> None:
        if self._pacing_governor is None:
            return
        permits = self._operation_api_permits(operation_name)
        paced_operation = f"{self.role}.{operation_name}"
        if operation_name in HISTORICAL_BROKER_OPERATIONS:
            self._pacing_governor.acquire_historical_request(
                paced_operation,
                api_permits=permits,
            )
            return
        self._pacing_governor.acquire_api_request(
            paced_operation,
            permits=permits,
        )

    def _prune_times_locked(self, times: deque[datetime], *, now: datetime) -> None:
        cutoff = now.timestamp() - 60
        while times and times[0].timestamp() < cutoff:
            times.popleft()

    def _record_connect_attempt_locked(self) -> None:
        now = _utc_now()
        self._connect_attempt_count += 1
        self._last_connect_attempt_at = now
        self._connect_attempt_times.append(now)
        self._prune_times_locked(self._connect_attempt_times, now=now)

    def _record_connect_success_locked(self) -> None:
        self._connect_success_count += 1
        self._last_connect_success_at = _utc_now()
        self._consecutive_failures = 0
        self._cooldown_until = None
        self._circuit_breaker_reason = None
        self._circuit_breaker_until = None

    def _is_api_startup_failure(self, error: str) -> bool:
        lowered = error.lower()
        return any(marker in lowered for marker in API_STARTUP_FAILURE_MARKERS)

    def _gateway_failure_diagnostics(self, error: str) -> tuple[str, str | None]:
        if not self._is_api_startup_failure(error):
            return error, None
        try:
            diagnostics = self._gateway_diagnostics_reader()
        except Exception as exc:  # pragma: no cover - defensive diagnostics path.
            LOGGER.debug("Gateway diagnostics failed during circuit classification: %s", exc)
            return error, None

        hint = format_gateway_diagnostic_hint(diagnostics)
        enhanced_error = error
        if hint is not None and hint not in enhanced_error:
            enhanced_error = f"{enhanced_error} {hint}"
        status = str(diagnostics.get("status") or "")
        if status in STUCK_GATEWAY_CIRCUIT_STATUSES:
            summary = diagnostics.get("summary") or status
            return enhanced_error, f"{status}: {summary}"
        return enhanced_error, "api_startup_no_next_valid_id"

    def _record_gateway_failure_locked(self, error: str) -> str:
        recorded_error, circuit_reason = self._gateway_failure_diagnostics(error)
        self._last_error = recorded_error
        self._consecutive_failures += 1
        self._circuit_breaker_reason = circuit_reason
        if self._initial_connect_backoff_seconds <= 0:
            delay = 0.0
        else:
            delay = min(
                self._max_connect_backoff_seconds,
                self._initial_connect_backoff_seconds
                * (2 ** max(0, self._consecutive_failures - 1)),
            )
        if circuit_reason is not None:
            delay = max(delay, self._api_startup_failure_slow_probe_seconds)
        if delay <= 0:
            self._cooldown_until = None
            self._circuit_breaker_until = None
            return recorded_error
        self._cooldown_until = _utc_now() + timedelta(seconds=delay)
        self._circuit_breaker_until = self._cooldown_until if circuit_reason else None
        if circuit_reason is not None and self._broker_circuit is not None:
            self._broker_circuit.trip(
                reason=circuit_reason,
                source=self.role,
                error=recorded_error,
                duration_seconds=delay,
            )
        return recorded_error

    def _cooldown_seconds_remaining_locked(self, *, now: datetime) -> int | None:
        if self._cooldown_until is None:
            return None
        remaining = int((self._cooldown_until - now).total_seconds())
        return max(0, remaining)

    def _raise_if_cooling_down_locked(
        self,
        *,
        operation_name: str,
        ignore_cooldown: bool = False,
    ) -> None:
        if ignore_cooldown:
            return
        if self._broker_circuit is not None:
            self._broker_circuit.raise_if_open(
                operation_name=operation_name,
                source=self.role,
            )
        if self._cooldown_until is None:
            return
        now = _utc_now()
        if self._cooldown_until <= now:
            return
        retry_at = self._cooldown_until.isoformat()
        circuit = (
            f" Circuit breaker: {self._circuit_breaker_reason}."
            if self._circuit_breaker_reason
            else ""
        )
        raise ConnectionError(
            f"Managed IBKR session '{self.role}' is cooling down after "
            f"{self._consecutive_failures} failed broker attempt(s); next retry at "
            f"{retry_at}. Last error: {self._last_error}.{circuit}"
        )

    def _record_disconnect_locked(self) -> None:
        now = _utc_now()
        self._disconnect_count += 1
        self._last_disconnect_at = now

    def _record_checkout_locked(self) -> None:
        now = _utc_now()
        self._checkout_count += 1
        self._last_checkout_at = now
        self._checkout_times.append(now)
        self._prune_times_locked(self._checkout_times, now=now)

    def _build_app(self) -> Any:
        wrapper_cls = self._wrapper_cls or load_sync_wrapper_class()
        return wrapper_cls(timeout=self._default_timeout)

    def _disconnect_locked(self) -> None:
        if self._app is None:
            return

        app = self._app
        self._app = None
        try:
            app.disconnect_and_stop()
        finally:
            self._record_disconnect_locked()

    def _ensure_connected_locked(
        self,
        *,
        operation_name: str,
        ignore_cooldown: bool = False,
    ) -> None:
        self._raise_if_cooling_down_locked(
            operation_name=operation_name,
            ignore_cooldown=ignore_cooldown,
        )
        if self._app is not None and _is_connected(self._app):
            self._last_error = None
            self._circuit_breaker_reason = None
            self._circuit_breaker_until = None
            return

        self._pace_connection_attempt_locked("connect")
        self._record_connect_attempt_locked()
        self._disconnect_locked()
        app = self._build_app()
        if not app.connect_and_start(
            host=self.config.host,
            port=self.config.port,
            client_id=self.config.client_id,
        ):
            failure_reason = getattr(app, "last_connect_failure_reason", None)
            error = str(failure_reason).strip() if failure_reason else (
                f"Failed to connect to IBKR at {self.config.host}:{self.config.port} "
                f"with client_id={self.config.client_id}."
            )
            try:
                app.disconnect_and_stop()
            except Exception:
                pass
            recorded_error = self._record_gateway_failure_locked(error)
            raise ConnectionError(recorded_error)

        self._app = app
        self._record_connect_success_locked()
        if self._broker_circuit is not None:
            self._broker_circuit.clear(source=self.role)
        self._last_error = None

    def warmup(self) -> ManagedSessionStatus:
        with self._lock:
            try:
                self._ensure_connected_locked(operation_name="warmup")
            except Exception as exc:
                if "cooling down" not in str(exc):
                    self._last_error = str(exc)
            return self.status()

    def disconnect(self) -> None:
        with self._lock:
            self._disconnect_locked()

    def status(self, *, blocking: bool = True) -> ManagedSessionStatus:
        acquired = self._lock.acquire(blocking=blocking)
        if not acquired:
            return ManagedSessionStatus(
                role=self.role,
                host=self.config.host,
                port=self.config.port,
                client_id=self.config.client_id,
                connected=False,
                last_error="Session status is unavailable because the session lock is busy.",
                consecutive_failures=self._consecutive_failures,
                cooldown_until=self._cooldown_until,
                cooldown_seconds_remaining=self._cooldown_seconds_remaining_locked(
                    now=_utc_now()
                ),
                circuit_breaker_reason=self._circuit_breaker_reason,
                circuit_breaker_until=self._circuit_breaker_until,
                metrics=ManagedSessionMetrics(
                    connect_attempt_count=self._connect_attempt_count,
                    connect_success_count=self._connect_success_count,
                    disconnect_count=self._disconnect_count,
                    checkout_count=self._checkout_count,
                    failed_checkout_count=self._failed_checkout_count,
                    connect_attempts_last_60_seconds=len(self._connect_attempt_times),
                    checkouts_last_60_seconds=len(self._checkout_times),
                    last_connect_attempt_at=self._last_connect_attempt_at,
                    last_connect_success_at=self._last_connect_success_at,
                    last_disconnect_at=self._last_disconnect_at,
                    last_checkout_at=self._last_checkout_at,
                ),
            )

        try:
            now = _utc_now()
            self._prune_times_locked(self._connect_attempt_times, now=now)
            self._prune_times_locked(self._checkout_times, now=now)
            return ManagedSessionStatus(
                role=self.role,
                host=self.config.host,
                port=self.config.port,
                client_id=self.config.client_id,
                connected=self._app is not None and _is_connected(self._app),
                last_error=self._last_error,
                consecutive_failures=self._consecutive_failures,
                cooldown_until=self._cooldown_until,
                cooldown_seconds_remaining=self._cooldown_seconds_remaining_locked(
                    now=now
                ),
                circuit_breaker_reason=self._circuit_breaker_reason,
                circuit_breaker_until=self._circuit_breaker_until,
                metrics=ManagedSessionMetrics(
                    connect_attempt_count=self._connect_attempt_count,
                    connect_success_count=self._connect_success_count,
                    disconnect_count=self._disconnect_count,
                    checkout_count=self._checkout_count,
                    failed_checkout_count=self._failed_checkout_count,
                    connect_attempts_last_60_seconds=len(self._connect_attempt_times),
                    checkouts_last_60_seconds=len(self._checkout_times),
                    last_connect_attempt_at=self._last_connect_attempt_at,
                    last_connect_success_at=self._last_connect_success_at,
                    last_disconnect_at=self._last_disconnect_at,
                    last_checkout_at=self._last_checkout_at,
                ),
            )
        finally:
            self._lock.release()

    @contextmanager
    def checkout(
        self,
        *,
        operation_name: str = "unspecified",
        ignore_cooldown: bool = False,
    ) -> Iterator[Any]:
        started_at = _utc_now()
        lock_wait_started_at = time.monotonic()
        self._lock.acquire()
        lock_wait_ms = max(0, int((time.monotonic() - lock_wait_started_at) * 1000))
        if lock_wait_ms >= 1000:
            LOGGER.warning(
                "IBKR broker operation waited for session lock: role=%s "
                "operation=%s client_id=%s lock_wait_ms=%s",
                self.role,
                operation_name,
                self.config.client_id,
                lock_wait_ms,
            )

        try:
            try:
                self._ensure_connected_locked(
                    operation_name=operation_name,
                    ignore_cooldown=ignore_cooldown,
                )
                self._pace_operation_locked(operation_name)
                self._record_checkout_locked()
                app = self._app
                if app is None:
                    message = (
                        f"Managed IBKR session '{self.role}' is not connected for "
                        f"client_id={self.config.client_id}."
                    )
                    raise ConnectionError(message)
            except Exception as exc:
                self._failed_checkout_count += 1
                previous_last_error = self._last_error
                is_cooldown_error = (
                    isinstance(exc, ConnectionError)
                    and "cooling down" in str(exc)
                )
                is_circuit_open_error = isinstance(exc, BrokerCircuitOpen)
                if not is_cooldown_error:
                    self._last_error = str(exc)
                should_open_cooldown = (
                    isinstance(exc, (ConnectionError, TimeoutError))
                    and not is_cooldown_error
                    and not is_circuit_open_error
                    and previous_last_error != str(exc)
                )
                if should_open_cooldown:
                    self._record_gateway_failure_locked(str(exc))
                    self._disconnect_locked()
                completed_at = _utc_now()
                if self._activity_tracker is not None:
                    self._activity_tracker.record(
                        role=self.role,
                        operation_name=operation_name,
                        started_at=started_at,
                        completed_at=completed_at,
                        success=False,
                        error=str(exc),
                        lock_wait_ms=lock_wait_ms,
                    )
                duration_ms = max(
                    0,
                    int((completed_at - started_at).total_seconds() * 1000),
                )
                log_method = LOGGER.info if is_cooldown_error else LOGGER.warning
                log_method(
                    (
                        "IBKR broker operation skipped during cooldown: role=%s "
                        "operation=%s client_id=%s duration_ms=%s lock_wait_ms=%s "
                        "error=%s"
                    )
                    if is_cooldown_error
                    else (
                        "IBKR broker operation failed before checkout: role=%s "
                        "operation=%s client_id=%s duration_ms=%s lock_wait_ms=%s "
                        "error=%s"
                    ),
                    self.role,
                    operation_name,
                    self.config.client_id,
                    duration_ms,
                    lock_wait_ms,
                    exc,
                )
                raise

            try:
                yield app
            except Exception as exc:
                self._failed_checkout_count += 1
                self._last_error = str(exc)
                if isinstance(exc, (ConnectionError, TimeoutError)):
                    self._record_gateway_failure_locked(str(exc))
                    self._disconnect_locked()
                completed_at = _utc_now()
                if self._activity_tracker is not None:
                    self._activity_tracker.record(
                        role=self.role,
                        operation_name=operation_name,
                        started_at=started_at,
                        completed_at=completed_at,
                        success=False,
                        error=str(exc),
                        lock_wait_ms=lock_wait_ms,
                    )
                duration_ms = max(
                    0,
                    int((completed_at - started_at).total_seconds() * 1000),
                )
                LOGGER.warning(
                    "IBKR broker operation failed: role=%s operation=%s "
                    "client_id=%s duration_ms=%s lock_wait_ms=%s error=%s",
                    self.role,
                    operation_name,
                    self.config.client_id,
                    duration_ms,
                    lock_wait_ms,
                    exc,
                )
                raise
            else:
                completed_at = _utc_now()
                if self._activity_tracker is not None:
                    self._activity_tracker.record(
                        role=self.role,
                        operation_name=operation_name,
                        started_at=started_at,
                        completed_at=completed_at,
                        success=True,
                        error=None,
                        lock_wait_ms=lock_wait_ms,
                    )
            finally:
                if not _is_connected(app):
                    self._disconnect_locked()
        finally:
            self._lock.release()

    def execute(
        self,
        operation_name: str,
        operation: Callable[[Any], Any],
        *,
        ignore_cooldown: bool = False,
    ) -> Any:
        with self.checkout(
            operation_name=operation_name,
            ignore_cooldown=ignore_cooldown,
        ) as app:
            return operation(app)

    def drain_broker_callback_events(
        self,
        *,
        connect_if_needed: bool = True,
    ) -> list[dict[str, Any]]:
        def _drain(app: Any) -> list[dict[str, Any]]:
            drainer = getattr(app, "drain_broker_callback_events", None)
            if drainer is None:
                return []
            if not callable(drainer):
                raise TypeError(
                    f"Managed IBKR session '{self.role}' exposes a non-callable "
                    "drain_broker_callback_events attribute."
                )
            events = drainer()
            if not isinstance(events, list):
                raise TypeError(
                    f"Managed IBKR session '{self.role}' returned a non-list broker callback payload."
                )
            return events

        if connect_if_needed:
            return self.execute("drain_broker_callbacks", _drain)

        with self._lock:
            app = self._app
            if app is None or not _is_connected(app):
                return []
            try:
                return _drain(app)
            except Exception as exc:
                self._last_error = str(exc)
                if isinstance(exc, (ConnectionError, TimeoutError)):
                    self._record_gateway_failure_locked(str(exc))
                    self._disconnect_locked()
                raise
            finally:
                if app is self._app and not _is_connected(app):
                    self._disconnect_locked()


class CanonicalSyncSessions:
    def __init__(
        self,
        connection_config: IbkrConnectionConfig,
        *,
        wrapper_cls: type[Any] | None = None,
        default_timeout: int = 30,
        initial_connect_backoff_seconds: float = 5.0,
        max_connect_backoff_seconds: float = 300.0,
        api_startup_failure_slow_probe_seconds: float | None = None,
        pacing_governor: BrokerApiPacingGovernor | None = None,
        broker_circuit: BrokerHealthCircuit | None = None,
    ) -> None:
        self.activity_tracker = BrokerActivityTracker()
        self.primary = ManagedSyncSession(
            "primary",
            connection_config.primary_session(),
            wrapper_cls=wrapper_cls,
            default_timeout=default_timeout,
            activity_tracker=self.activity_tracker,
            initial_connect_backoff_seconds=initial_connect_backoff_seconds,
            max_connect_backoff_seconds=max_connect_backoff_seconds,
            api_startup_failure_slow_probe_seconds=api_startup_failure_slow_probe_seconds,
            pacing_governor=pacing_governor,
            broker_circuit=broker_circuit,
        )
        self.diagnostic = ManagedSyncSession(
            "diagnostic",
            connection_config.diagnostic_session(),
            wrapper_cls=wrapper_cls,
            default_timeout=default_timeout,
            activity_tracker=self.activity_tracker,
            initial_connect_backoff_seconds=initial_connect_backoff_seconds,
            max_connect_backoff_seconds=max_connect_backoff_seconds,
            api_startup_failure_slow_probe_seconds=api_startup_failure_slow_probe_seconds,
            pacing_governor=pacing_governor,
            broker_circuit=broker_circuit,
        )
        self.historical = ManagedSyncSession(
            "historical",
            connection_config.historical_session(),
            wrapper_cls=wrapper_cls,
            default_timeout=default_timeout,
            activity_tracker=self.activity_tracker,
            initial_connect_backoff_seconds=initial_connect_backoff_seconds,
            max_connect_backoff_seconds=max_connect_backoff_seconds,
            api_startup_failure_slow_probe_seconds=api_startup_failure_slow_probe_seconds,
            pacing_governor=pacing_governor,
            broker_circuit=broker_circuit,
        )

    def warmup(self) -> dict[str, dict[str, Any]]:
        return {
            "primary": serialize_managed_session_status(self.primary.warmup()),
            "diagnostic": serialize_managed_session_status(self.diagnostic.warmup()),
            "historical": serialize_managed_session_status(self.historical.status()),
        }

    def shutdown(self) -> None:
        self.primary.disconnect()
        self.diagnostic.disconnect()
        self.historical.disconnect()

    def status_snapshot(self, *, blocking: bool = True) -> dict[str, dict[str, Any]]:
        return {
            "primary": serialize_managed_session_status(
                self.primary.status(blocking=blocking)
            ),
            "diagnostic": serialize_managed_session_status(
                self.diagnostic.status(blocking=blocking)
            ),
            "historical": serialize_managed_session_status(
                self.historical.status(blocking=blocking)
            ),
        }

    def telemetry_snapshot(self, *, recent_limit: int = 20) -> dict[str, Any]:
        return {
            "sessions": self.status_snapshot(),
            "operations": self.activity_tracker.snapshot(recent_limit=recent_limit),
        }
