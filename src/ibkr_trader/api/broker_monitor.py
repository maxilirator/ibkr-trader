from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from threading import Event
from threading import RLock
from threading import Thread
from typing import Any
from typing import Callable

from ibkr_trader.ibkr.probe import GatewayProbeResult
from ibkr_trader.ibkr.runtime_snapshot import BrokerRuntimeSnapshot


def _utc_now() -> datetime:
    return datetime.now(UTC)


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
    broker_current_time: datetime | None = None
    error: str | None = None


@dataclass(slots=True)
class BrokerSnapshotRefreshStatus:
    ok: bool | None = None
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
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
        self._stop_event = Event()
        self._thread: Thread | None = None
        self._status = BrokerMonitorStatus(
            started_at=None,
            heartbeat=BrokerHeartbeatStatus(),
            snapshot_refresh=BrokerSnapshotRefreshStatus(),
        )

    def status(self) -> BrokerMonitorStatus:
        with self._lock:
            return BrokerMonitorStatus(
                started_at=self._status.started_at,
                heartbeat=BrokerHeartbeatStatus(**asdict(self._status.heartbeat)),
                snapshot_refresh=BrokerSnapshotRefreshStatus(
                    **asdict(self._status.snapshot_refresh)
                ),
            )

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
        self._run_heartbeat()
        self._run_snapshot_refresh()

    def _run_loop(self) -> None:
        next_heartbeat_at = 0.0
        next_snapshot_at = 0.0
        while not self._stop_event.is_set():
            now = _utc_now().timestamp()
            wait_seconds: float | None = None

            if now >= next_heartbeat_at:
                self._run_heartbeat()
                next_heartbeat_at = now + self._heartbeat_interval_seconds
            else:
                wait_seconds = next_heartbeat_at - now

            now = _utc_now().timestamp()
            if now >= next_snapshot_at:
                self._run_snapshot_refresh()
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

    def _run_heartbeat(self) -> None:
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
            return

        with self._lock:
            self._status.heartbeat.ok = True
            self._status.heartbeat.last_success_at = attempted_at
            self._status.heartbeat.broker_current_time = result.broker_current_time
            self._status.heartbeat.error = None

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
