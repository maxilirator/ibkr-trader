from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import RuntimeServiceEventRecord
from ibkr_trader.db.models import RuntimeServiceRecord

if TYPE_CHECKING:
    from ibkr_trader.orchestration.runtime_worker import RuntimeCycleResult

EXECUTION_RUNTIME_KEY = "EXECUTION_RUNTIME"
ACTIVE_RUNTIME_SERVICE_STATUSES = {"STARTING", "RUNNING", "DEGRADED", "STOPPING"}


class RuntimeServiceLeaseError(RuntimeError):
    """Raised when another process still owns the durable runtime-service lease."""


@dataclass(slots=True, frozen=True)
class RuntimeServiceStatusSnapshot:
    runtime_key: str
    service_type: str
    status: str
    owner_token: str | None
    owner_label: str | None
    hostname: str | None
    pid: int | None
    runtime_timezone: str | None
    broker_kind: str | None
    broker_client_id: int | None
    started_at: datetime | None
    heartbeat_at: datetime | None
    last_cycle_started_at: datetime | None
    last_cycle_completed_at: datetime | None
    last_successful_cycle_at: datetime | None
    lease_expires_at: datetime | None
    stop_requested: bool
    last_error: str | None
    metadata_json: dict[str, Any]


def serialize_runtime_service_status(
    snapshot: RuntimeServiceStatusSnapshot | None,
) -> dict[str, Any] | None:
    if snapshot is None:
        return None
    payload = asdict(snapshot)
    for key, value in list(payload.items()):
        if isinstance(value, datetime):
            payload[key] = value.isoformat()
    return payload


def _lease_expiry(lease_seconds: float, *, now: datetime) -> datetime:
    return now + timedelta(seconds=max(5.0, lease_seconds))


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _serialize_runtime_cycle_result(
    result: RuntimeCycleResult | None,
) -> dict[str, Any] | None:
    if result is None:
        return None
    return {
        "cycle_started_at": result.cycle_started_at.isoformat(),
        "cycle_completed_at": result.cycle_completed_at.isoformat(),
        "issue_count": len(result.issues),
        "submitted_entry_count": len(result.submitted_entries),
        "cancelled_entry_count": len(result.cancelled_entries),
        "filled_entry_count": len(result.filled_entries),
        "submitted_exit_count": len(result.submitted_exits),
        "completed_instruction_count": len(result.completed_instructions),
    }


def _build_snapshot(record: RuntimeServiceRecord) -> RuntimeServiceStatusSnapshot:
    return RuntimeServiceStatusSnapshot(
        runtime_key=record.runtime_key,
        service_type=record.service_type,
        status=record.status,
        owner_token=record.owner_token,
        owner_label=record.owner_label,
        hostname=record.hostname,
        pid=record.pid,
        runtime_timezone=record.runtime_timezone,
        broker_kind=record.broker_kind,
        broker_client_id=record.broker_client_id,
        started_at=record.started_at,
        heartbeat_at=record.heartbeat_at,
        last_cycle_started_at=record.last_cycle_started_at,
        last_cycle_completed_at=record.last_cycle_completed_at,
        last_successful_cycle_at=record.last_successful_cycle_at,
        lease_expires_at=record.lease_expires_at,
        stop_requested=record.stop_requested,
        last_error=record.last_error,
        metadata_json=dict(record.metadata_json or {}),
    )


def _get_runtime_record_for_update(
    session: Session,
    *,
    runtime_key: str,
) -> RuntimeServiceRecord | None:
    return session.execute(
        select(RuntimeServiceRecord)
        .where(RuntimeServiceRecord.runtime_key == runtime_key)
        .with_for_update()
    ).scalar_one_or_none()


def _append_runtime_event(
    session: Session,
    *,
    runtime_record: RuntimeServiceRecord,
    event_type: str,
    source: str,
    event_at: datetime,
    status_before: str | None,
    status_after: str | None,
    payload: dict[str, Any],
    note: str | None,
) -> None:
    session.add(
        RuntimeServiceEventRecord(
            runtime_service_id=runtime_record.id,
            event_type=event_type,
            source=source,
            event_at=event_at,
            status_before=status_before,
            status_after=status_after,
            payload=payload,
            note=note,
        )
    )


def acquire_runtime_service_lease(
    session_factory: sessionmaker[Session],
    *,
    runtime_key: str,
    service_type: str,
    owner_token: str,
    owner_label: str,
    hostname: str,
    pid: int,
    runtime_timezone: str,
    broker_kind: str,
    broker_client_id: int,
    lease_seconds: float,
    metadata_json: dict[str, Any] | None = None,
) -> RuntimeServiceStatusSnapshot:
    now = utc_now()
    metadata = dict(metadata_json or {})
    with session_scope(session_factory) as session:
        record = _get_runtime_record_for_update(session, runtime_key=runtime_key)
        if record is None:
            record = RuntimeServiceRecord(
                runtime_key=runtime_key,
                service_type=service_type,
                status="STOPPED",
                metadata_json={},
            )
            session.add(record)
            session.flush()

        if (
            record.owner_token
            and record.owner_token != owner_token
            and record.lease_expires_at is not None
            and _ensure_utc(record.lease_expires_at) > now
            and record.status in ACTIVE_RUNTIME_SERVICE_STATUSES
        ):
            raise RuntimeServiceLeaseError(
                f"Runtime service '{runtime_key}' is already owned by {record.owner_label}."
            )

        status_before = record.status
        record.service_type = service_type
        record.status = "STARTING"
        record.owner_token = owner_token
        record.owner_label = owner_label
        record.hostname = hostname
        record.pid = pid
        record.runtime_timezone = runtime_timezone
        record.broker_kind = broker_kind
        record.broker_client_id = broker_client_id
        record.started_at = now
        record.heartbeat_at = now
        record.last_cycle_started_at = None
        record.last_cycle_completed_at = None
        record.last_successful_cycle_at = None
        record.lease_expires_at = _lease_expiry(lease_seconds, now=now)
        record.stop_requested = False
        record.last_error = None
        record.metadata_json = metadata

        _append_runtime_event(
            session,
            runtime_record=record,
            event_type="runtime_started",
            source="runtime_service",
            event_at=now,
            status_before=status_before,
            status_after=record.status,
            payload={
                "owner_label": owner_label,
                "hostname": hostname,
                "pid": pid,
                "metadata": metadata,
            },
            note="Runtime service lease acquired.",
        )
        return _build_snapshot(record)


def renew_runtime_service_lease(
    session_factory: sessionmaker[Session],
    *,
    runtime_key: str,
    owner_token: str,
    lease_seconds: float,
    metadata_json: dict[str, Any] | None = None,
) -> RuntimeServiceStatusSnapshot:
    now = utc_now()
    with session_scope(session_factory) as session:
        record = _get_runtime_record_for_update(session, runtime_key=runtime_key)
        if record is None or record.owner_token != owner_token:
            raise RuntimeServiceLeaseError(
                f"Runtime service '{runtime_key}' is not owned by this process."
            )
        record.heartbeat_at = now
        record.lease_expires_at = _lease_expiry(lease_seconds, now=now)
        if metadata_json is not None:
            record.metadata_json = dict(metadata_json)
        return _build_snapshot(record)


def record_runtime_cycle_started(
    session_factory: sessionmaker[Session],
    *,
    runtime_key: str,
    owner_token: str,
    lease_seconds: float,
) -> RuntimeServiceStatusSnapshot:
    now = utc_now()
    with session_scope(session_factory) as session:
        record = _get_runtime_record_for_update(session, runtime_key=runtime_key)
        if record is None or record.owner_token != owner_token:
            raise RuntimeServiceLeaseError(
                f"Runtime service '{runtime_key}' is not owned by this process."
            )
        record.heartbeat_at = now
        record.last_cycle_started_at = now
        record.lease_expires_at = _lease_expiry(lease_seconds, now=now)
        return _build_snapshot(record)


def record_runtime_cycle_completed(
    session_factory: sessionmaker[Session],
    *,
    runtime_key: str,
    owner_token: str,
    lease_seconds: float,
    result: RuntimeCycleResult,
) -> RuntimeServiceStatusSnapshot:
    now = utc_now()
    status_after = "RUNNING" if not result.issues else "DEGRADED"
    last_error = None
    if result.issues:
        last_error = "; ".join(issue.message for issue in result.issues[:3])
    with session_scope(session_factory) as session:
        record = _get_runtime_record_for_update(session, runtime_key=runtime_key)
        if record is None or record.owner_token != owner_token:
            raise RuntimeServiceLeaseError(
                f"Runtime service '{runtime_key}' is not owned by this process."
            )
        status_before = record.status
        record.status = status_after
        record.heartbeat_at = now
        record.last_cycle_completed_at = result.cycle_completed_at
        if not result.issues:
            record.last_successful_cycle_at = result.cycle_completed_at
        record.lease_expires_at = _lease_expiry(lease_seconds, now=now)
        record.last_error = last_error

        _append_runtime_event(
            session,
            runtime_record=record,
            event_type="runtime_cycle_completed",
            source="runtime_service",
            event_at=now,
            status_before=status_before,
            status_after=status_after,
            payload=_serialize_runtime_cycle_result(result) or {},
            note=(
                "Runtime cycle completed cleanly."
                if not result.issues
                else f"Runtime cycle completed with {len(result.issues)} issue(s)."
            ),
        )
        return _build_snapshot(record)


def mark_runtime_service_startup_blocked(
    session_factory: sessionmaker[Session],
    *,
    runtime_key: str,
    owner_token: str,
    result: RuntimeCycleResult,
) -> RuntimeServiceStatusSnapshot:
    now = utc_now()
    last_error = "; ".join(issue.message for issue in result.issues[:3]) or None
    with session_scope(session_factory) as session:
        record = _get_runtime_record_for_update(session, runtime_key=runtime_key)
        if record is None or record.owner_token != owner_token:
            raise RuntimeServiceLeaseError(
                f"Runtime service '{runtime_key}' is not owned by this process."
            )
        status_before = record.status
        record.status = "STARTUP_BLOCKED"
        record.heartbeat_at = now
        record.last_cycle_completed_at = result.cycle_completed_at
        record.lease_expires_at = None
        record.owner_token = None
        record.owner_label = None
        record.pid = None
        record.last_error = last_error

        _append_runtime_event(
            session,
            runtime_record=record,
            event_type="startup_reconciliation_blocked",
            source="runtime_service",
            event_at=now,
            status_before=status_before,
            status_after=record.status,
            payload=_serialize_runtime_cycle_result(result) or {},
            note="Startup reconciliation blocked the runtime loop.",
        )
        return _build_snapshot(record)


def mark_runtime_service_stopped(
    session_factory: sessionmaker[Session],
    *,
    runtime_key: str,
    owner_token: str,
    note: str,
    event_type: str = "runtime_stopped",
) -> RuntimeServiceStatusSnapshot:
    now = utc_now()
    with session_scope(session_factory) as session:
        record = _get_runtime_record_for_update(session, runtime_key=runtime_key)
        if record is None or record.owner_token != owner_token:
            raise RuntimeServiceLeaseError(
                f"Runtime service '{runtime_key}' is not owned by this process."
            )
        status_before = record.status
        record.status = "STOPPED"
        record.heartbeat_at = now
        record.lease_expires_at = None
        record.owner_token = None
        record.owner_label = None
        record.pid = None

        _append_runtime_event(
            session,
            runtime_record=record,
            event_type=event_type,
            source="runtime_service",
            event_at=now,
            status_before=status_before,
            status_after=record.status,
            payload={},
            note=note,
        )
        return _build_snapshot(record)


def mark_runtime_service_failed(
    session_factory: sessionmaker[Session],
    *,
    runtime_key: str,
    owner_token: str,
    error: str,
) -> RuntimeServiceStatusSnapshot:
    now = utc_now()
    with session_scope(session_factory) as session:
        record = _get_runtime_record_for_update(session, runtime_key=runtime_key)
        if record is None or record.owner_token != owner_token:
            raise RuntimeServiceLeaseError(
                f"Runtime service '{runtime_key}' is not owned by this process."
            )
        status_before = record.status
        record.status = "FAILED"
        record.heartbeat_at = now
        record.lease_expires_at = None
        record.owner_token = None
        record.owner_label = None
        record.pid = None
        record.last_error = error

        _append_runtime_event(
            session,
            runtime_record=record,
            event_type="runtime_failed",
            source="runtime_service",
            event_at=now,
            status_before=status_before,
            status_after=record.status,
            payload={"error": error},
            note="Runtime service exited with an unhandled error.",
        )
        return _build_snapshot(record)


def read_runtime_service_status(
    session_factory: sessionmaker[Session],
    *,
    runtime_key: str = EXECUTION_RUNTIME_KEY,
) -> RuntimeServiceStatusSnapshot | None:
    with session_scope(session_factory) as session:
        record = session.execute(
            select(RuntimeServiceRecord).where(RuntimeServiceRecord.runtime_key == runtime_key)
        ).scalar_one_or_none()
        if record is None:
            return None
        return _build_snapshot(record)
