from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
from dataclasses import dataclass
from datetime import UTC, datetime
from collections import deque
from threading import RLock
from typing import Any, Callable, Iterator

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.ibkr.sync_wrapper import load_sync_wrapper_class


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
    metrics: ManagedSessionMetrics


@dataclass(slots=True, frozen=True)
class BrokerOperationRecord:
    role: str
    operation_name: str
    started_at: datetime
    completed_at: datetime
    duration_ms: int
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
    ) -> None:
        self.role = role
        self.config = config
        self._wrapper_cls = wrapper_cls
        self._default_timeout = default_timeout
        self._activity_tracker = activity_tracker
        self._lock = RLock()
        self._app: Any | None = None
        self._last_error: str | None = None
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

    def _ensure_connected_locked(self) -> None:
        if self._app is not None and _is_connected(self._app):
            self._last_error = None
            return

        self._record_connect_attempt_locked()
        self._disconnect_locked()
        app = self._build_app()
        if not app.connect_and_start(
            host=self.config.host,
            port=self.config.port,
            client_id=self.config.client_id,
        ):
            self._last_error = (
                f"Failed to connect to IBKR at {self.config.host}:{self.config.port} "
                f"with client_id={self.config.client_id}."
            )
            raise ConnectionError(self._last_error)

        self._app = app
        self._record_connect_success_locked()
        self._last_error = None

    def warmup(self) -> ManagedSessionStatus:
        with self._lock:
            try:
                self._ensure_connected_locked()
            except Exception as exc:
                self._last_error = str(exc)
            return self.status()

    def disconnect(self) -> None:
        with self._lock:
            self._disconnect_locked()

    def status(self) -> ManagedSessionStatus:
        with self._lock:
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

    @contextmanager
    def checkout(self, *, operation_name: str = "unspecified") -> Iterator[Any]:
        started_at = _utc_now()
        try:
            with self._lock:
                self._ensure_connected_locked()
                self._record_checkout_locked()
                app = self._app
                if app is None:
                    message = (
                        f"Managed IBKR session '{self.role}' is not connected for "
                        f"client_id={self.config.client_id}."
                    )
                    raise ConnectionError(message)
        except Exception as exc:
            with self._lock:
                self._failed_checkout_count += 1
                self._last_error = str(exc)
            if self._activity_tracker is not None:
                self._activity_tracker.record(
                    role=self.role,
                    operation_name=operation_name,
                    started_at=started_at,
                    completed_at=_utc_now(),
                    success=False,
                    error=str(exc),
                )
            raise

        try:
            yield app
        except Exception as exc:
            with self._lock:
                self._failed_checkout_count += 1
                self._last_error = str(exc)
            if self._activity_tracker is not None:
                self._activity_tracker.record(
                    role=self.role,
                    operation_name=operation_name,
                    started_at=started_at,
                    completed_at=_utc_now(),
                    success=False,
                    error=str(exc),
                )
            raise
        else:
            if self._activity_tracker is not None:
                self._activity_tracker.record(
                    role=self.role,
                    operation_name=operation_name,
                    started_at=started_at,
                    completed_at=_utc_now(),
                    success=True,
                    error=None,
                )
        finally:
            with self._lock:
                if not _is_connected(app):
                    self._disconnect_locked()

    def execute(self, operation_name: str, operation: Callable[[Any], Any]) -> Any:
        with self.checkout(operation_name=operation_name) as app:
            return operation(app)

    def drain_broker_callback_events(self) -> list[dict[str, Any]]:
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

        return self.execute("drain_broker_callbacks", _drain)


class CanonicalSyncSessions:
    def __init__(
        self,
        connection_config: IbkrConnectionConfig,
        *,
        wrapper_cls: type[Any] | None = None,
        default_timeout: int = 30,
    ) -> None:
        self.activity_tracker = BrokerActivityTracker()
        self.primary = ManagedSyncSession(
            "primary",
            connection_config.primary_session(),
            wrapper_cls=wrapper_cls,
            default_timeout=default_timeout,
            activity_tracker=self.activity_tracker,
        )
        self.diagnostic = ManagedSyncSession(
            "diagnostic",
            connection_config.diagnostic_session(),
            wrapper_cls=wrapper_cls,
            default_timeout=default_timeout,
            activity_tracker=self.activity_tracker,
        )

    def warmup(self) -> dict[str, dict[str, Any]]:
        return {
            "primary": serialize_managed_session_status(self.primary.warmup()),
            "diagnostic": serialize_managed_session_status(self.diagnostic.warmup()),
        }

    def shutdown(self) -> None:
        self.primary.disconnect()
        self.diagnostic.disconnect()

    def status_snapshot(self) -> dict[str, dict[str, Any]]:
        return {
            "primary": serialize_managed_session_status(self.primary.status()),
            "diagnostic": serialize_managed_session_status(self.diagnostic.status()),
        }

    def telemetry_snapshot(self, *, recent_limit: int = 20) -> dict[str, Any]:
        return {
            "sessions": self.status_snapshot(),
            "operations": self.activity_tracker.snapshot(recent_limit=recent_limit),
        }
