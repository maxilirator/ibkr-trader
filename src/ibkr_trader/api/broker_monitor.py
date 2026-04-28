from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from threading import Event
from threading import Lock
from threading import RLock
from threading import Thread
from typing import Any
from typing import Callable

from ibkr_trader.ibkr.probe import GatewayProbeResult
from ibkr_trader.ibkr.runtime_snapshot import BrokerRuntimeSnapshot


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _age_seconds(value: datetime | None, now: datetime) -> int | None:
    if value is None:
        return None
    return max(0, int((now - value).total_seconds()))


def _next_due_at(value: datetime | None, interval_seconds: float) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value.timestamp() + interval_seconds, tz=UTC)


def _serialize_for_json(payload: Any) -> Any:
    if isinstance(payload, datetime):
        return payload.isoformat()
    if isinstance(payload, list):
        return [_serialize_for_json(item) for item in payload]
    if isinstance(payload, tuple):
        return [_serialize_for_json(item) for item in payload]
    if isinstance(payload, dict):
        return {key: _serialize_for_json(value) for key, value in payload.items()}
    return payload


@dataclass(slots=True)
class BrokerHeartbeatStatus:
    ok: bool | None = None
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    last_attempt_age_seconds: int | None = None
    last_success_age_seconds: int | None = None
    next_check_due_at: datetime | None = None
    is_stale: bool = True
    broker_current_time: datetime | None = None
    error: str | None = None


@dataclass(slots=True)
class BrokerSnapshotRefreshStatus:
    ok: bool | None = None
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    last_attempt_age_seconds: int | None = None
    last_success_age_seconds: int | None = None
    next_check_due_at: datetime | None = None
    is_stale: bool = True
    captured_at: datetime | None = None
    account_count: int = 0
    portfolio_count: int = 0
    position_count: int = 0
    open_order_count: int = 0
    execution_count: int = 0
    error: str | None = None


@dataclass(slots=True)
class BrokerMonitorStatus:
    started_at: datetime | None
    status_checked_at: datetime
    running: bool
    refresh_in_flight: bool
    heartbeat: BrokerHeartbeatStatus
    snapshot_refresh: BrokerSnapshotRefreshStatus


def serialize_broker_monitor_status(status: BrokerMonitorStatus) -> dict[str, Any]:
    return _serialize_for_json(asdict(status))


class BrokerMonitorService:
    """Background broker heartbeat and snapshot persistence loop for the API server."""

    def __init__(
        self,
        *,
        heartbeat_probe: Callable[[], GatewayProbeResult],
        snapshot_fetcher: Callable[[], BrokerRuntimeSnapshot],
        snapshot_persister: Callable[[BrokerRuntimeSnapshot, datetime], None],
        heartbeat_interval_seconds: float,
        snapshot_refresh_interval_seconds: float,
    ) -> None:
        self._heartbeat_probe = heartbeat_probe
        self._snapshot_fetcher = snapshot_fetcher
        self._snapshot_persister = snapshot_persister
        self._heartbeat_interval_seconds = max(1.0, heartbeat_interval_seconds)
        self._snapshot_refresh_interval_seconds = max(1.0, snapshot_refresh_interval_seconds)
        self._lock = RLock()
        self._cycle_lock = Lock()
        self._stop_event = Event()
        self._thread: Thread | None = None
        self._refresh_thread: Thread | None = None
        self._status = BrokerMonitorStatus(
            started_at=None,
            status_checked_at=_utc_now(),
            running=False,
            refresh_in_flight=False,
            heartbeat=BrokerHeartbeatStatus(),
            snapshot_refresh=BrokerSnapshotRefreshStatus(),
        )

    def status(self) -> BrokerMonitorStatus:
        now = _utc_now()
        with self._lock:
            return BrokerMonitorStatus(
                started_at=self._status.started_at,
                status_checked_at=now,
                running=self._thread is not None and self._thread.is_alive(),
                refresh_in_flight=(
                    self._cycle_lock.locked()
                    or self._refresh_thread is not None
                    and self._refresh_thread.is_alive()
                ),
                heartbeat=self._heartbeat_with_freshness_locked(now),
                snapshot_refresh=self._snapshot_with_freshness_locked(now),
            )

    def request_cycle_if_due(self, *, min_interval_seconds: float | None = None) -> bool:
        """Start a non-blocking refresh when the cached broker status is old enough."""

        min_interval = (
            max(1.0, min_interval_seconds)
            if min_interval_seconds is not None
            else self._heartbeat_interval_seconds
        )
        now = _utc_now()
        with self._lock:
            if self._refresh_thread is not None and self._refresh_thread.is_alive():
                return False
            latest_attempts = [
                value
                for value in (
                    self._status.heartbeat.last_attempt_at,
                    self._status.snapshot_refresh.last_attempt_at,
                )
                if value is not None
            ]
            if latest_attempts:
                latest_attempt = max(latest_attempts)
                if (now - latest_attempt).total_seconds() < min_interval:
                    return False

            thread = Thread(
                target=self.run_cycle,
                name="ibkr-broker-monitor-refresh",
                daemon=True,
            )
            self._refresh_thread = thread
            thread.start()
            return True

    def _heartbeat_with_freshness_locked(self, now: datetime) -> BrokerHeartbeatStatus:
        heartbeat = BrokerHeartbeatStatus(**asdict(self._status.heartbeat))
        heartbeat.last_attempt_age_seconds = _age_seconds(heartbeat.last_attempt_at, now)
        heartbeat.last_success_age_seconds = _age_seconds(heartbeat.last_success_at, now)
        heartbeat.next_check_due_at = _next_due_at(
            heartbeat.last_attempt_at,
            self._heartbeat_interval_seconds,
        )
        evidence_age = _age_seconds(heartbeat.last_attempt_at or heartbeat.last_success_at, now)
        stale_after = max(
            self._heartbeat_interval_seconds * 2.0,
            self._heartbeat_interval_seconds + 10.0,
        )
        heartbeat.is_stale = evidence_age is None or evidence_age > stale_after
        return heartbeat

    def _snapshot_with_freshness_locked(self, now: datetime) -> BrokerSnapshotRefreshStatus:
        snapshot = BrokerSnapshotRefreshStatus(**asdict(self._status.snapshot_refresh))
        snapshot.last_attempt_age_seconds = _age_seconds(snapshot.last_attempt_at, now)
        snapshot.last_success_age_seconds = _age_seconds(snapshot.last_success_at, now)
        snapshot.next_check_due_at = _next_due_at(
            snapshot.last_attempt_at,
            self._snapshot_refresh_interval_seconds,
        )
        evidence_age = _age_seconds(snapshot.last_attempt_at or snapshot.last_success_at, now)
        stale_after = max(
            self._snapshot_refresh_interval_seconds * 2.0,
            self._snapshot_refresh_interval_seconds + 30.0,
        )
        snapshot.is_stale = evidence_age is None or evidence_age > stale_after
        return snapshot

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event.clear()
            self._status.started_at = _utc_now()
        self.run_cycle()
        thread = Thread(
            target=self._run_loop,
            name="ibkr-broker-monitor",
            daemon=True,
        )
        thread.start()
        with self._lock:
            self._thread = thread

    def stop(self) -> None:
        self._stop_event.set()
        with self._lock:
            thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=5)
        with self._lock:
            self._thread = None

    def run_cycle(self) -> None:
        if not self._cycle_lock.acquire(blocking=False):
            return
        try:
            if self._run_heartbeat():
                self._run_snapshot_refresh()
            else:
                self._skip_snapshot_refresh(
                    "Skipped broker snapshot refresh because the heartbeat probe failed."
                )
        finally:
            self._cycle_lock.release()

    def _run_loop(self) -> None:
        started_at = _utc_now().timestamp()
        next_heartbeat_at = started_at + self._heartbeat_interval_seconds
        next_snapshot_at = started_at + self._snapshot_refresh_interval_seconds
        while not self._stop_event.is_set():
            now = _utc_now().timestamp()
            wait_seconds: float | None = None

            if now >= next_heartbeat_at:
                heartbeat_ok = self._run_heartbeat()
                next_heartbeat_at = now + self._heartbeat_interval_seconds
            else:
                heartbeat_ok = self.status().heartbeat.ok
                wait_seconds = next_heartbeat_at - now

            now = _utc_now().timestamp()
            if now >= next_snapshot_at:
                if heartbeat_ok:
                    self._run_snapshot_refresh()
                else:
                    self._skip_snapshot_refresh(
                        "Skipped broker snapshot refresh because the heartbeat probe failed."
                    )
                next_snapshot_at = now + self._snapshot_refresh_interval_seconds
            else:
                snapshot_wait = next_snapshot_at - now
                wait_seconds = (
                    snapshot_wait
                    if wait_seconds is None
                    else min(wait_seconds, snapshot_wait)
                )

            if wait_seconds is None:
                wait_seconds = min(
                    self._heartbeat_interval_seconds,
                    self._snapshot_refresh_interval_seconds,
                )
            self._stop_event.wait(max(0.5, wait_seconds))

    def _run_heartbeat(self) -> bool:
        attempted_at = _utc_now()
        with self._lock:
            self._status.heartbeat.last_attempt_at = attempted_at

        try:
            result = self._heartbeat_probe()
        except Exception as exc:  # pragma: no cover - runtime safety
            with self._lock:
                self._status.heartbeat.ok = False
                self._status.heartbeat.last_failure_at = attempted_at
                self._status.heartbeat.error = str(exc)
            return False

        with self._lock:
            self._status.heartbeat.ok = True
            self._status.heartbeat.last_success_at = attempted_at
            self._status.heartbeat.broker_current_time = result.broker_current_time
            self._status.heartbeat.error = None
        return True

    def _skip_snapshot_refresh(self, reason: str) -> None:
        skipped_at = _utc_now()
        with self._lock:
            self._status.snapshot_refresh.ok = False
            self._status.snapshot_refresh.last_attempt_at = skipped_at
            self._status.snapshot_refresh.last_failure_at = skipped_at
            self._status.snapshot_refresh.error = reason

    def _run_snapshot_refresh(self) -> None:
        attempted_at = _utc_now()
        with self._lock:
            self._status.snapshot_refresh.last_attempt_at = attempted_at

        try:
            snapshot = self._snapshot_fetcher()
            self._snapshot_persister(snapshot, attempted_at)
        except Exception as exc:  # pragma: no cover - runtime safety
            with self._lock:
                self._status.snapshot_refresh.ok = False
                self._status.snapshot_refresh.last_failure_at = attempted_at
                self._status.snapshot_refresh.error = str(exc)
            return

        with self._lock:
            self._status.snapshot_refresh.ok = True
            self._status.snapshot_refresh.last_success_at = attempted_at
            self._status.snapshot_refresh.captured_at = attempted_at
            self._status.snapshot_refresh.account_count = len(snapshot.account_values)
            self._status.snapshot_refresh.portfolio_count = len(snapshot.portfolio)
            self._status.snapshot_refresh.position_count = len(snapshot.positions)
            self._status.snapshot_refresh.open_order_count = len(snapshot.open_orders)
            self._status.snapshot_refresh.execution_count = len(snapshot.executions)
            self._status.snapshot_refresh.error = None
