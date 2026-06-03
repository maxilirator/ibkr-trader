from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import ReconciliationIssueRecord
from ibkr_trader.db.models import ReconciliationRunRecord
from ibkr_trader.ibkr.gateway_diagnostics import format_gateway_diagnostic_hint
from ibkr_trader.ibkr.gateway_diagnostics import read_ibgateway_diagnostics
from ibkr_trader.ibkr.runtime_snapshot import BrokerRuntimeSnapshot
from ibkr_trader.ledger.persistence import BROKER_KIND_IBKR
from ibkr_trader.orchestration.runtime_types import RuntimeCycleAction
from ibkr_trader.orchestration.runtime_types import RuntimeCycleIssue
from ibkr_trader.orchestration.runtime_types import RuntimeCycleResult
from ibkr_trader.orchestration.runtime_types import serialize_for_json


BROKER_OUTAGE_RECONCILIATION_SUPPRESSION_WINDOW = timedelta(minutes=10)
BROKER_OUTAGE_AUDIT_STAGES = {
    "broker_callbacks_post_cycle",
    "broker_callbacks_pre_cycle",
    "broker_snapshot",
    "entry_submit",
    "reconcile_instruction",
}
BROKER_OUTAGE_MESSAGE_MARKERS = (
    "api startup",
    "broker snapshot was unavailable",
    "connection refused",
    "connection reset",
    "cooling down",
    "failed to connect to ibkr",
    "heartbeat",
    "nextvalidid",
    "no response received",
    "not connected",
    "socket",
    "timed out",
    "timeout",
)


def broker_snapshot_unavailable_message(error: Exception) -> str:
    """Attach Gateway UI diagnostic context to broker snapshot failures."""

    message = str(error)
    normalized = message.lower()
    if not any(marker in normalized for marker in BROKER_OUTAGE_MESSAGE_MARKERS):
        return message
    hint = format_gateway_diagnostic_hint(read_ibgateway_diagnostics())
    if hint is None or hint in message:
        return message
    return f"{message} {hint}"


def finalize_runtime_cycle_result(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    *,
    run_kind: str,
    runtime_timezone: str,
    cycle_started_at: datetime,
    submit_due_entries: bool,
    due_instruction_count: int,
    active_instruction_count: int,
    snapshot: BrokerRuntimeSnapshot | None,
    submitted_entries: list[RuntimeCycleAction],
    cancelled_entries: list[RuntimeCycleAction],
    filled_entries: list[RuntimeCycleAction],
    submitted_exits: list[RuntimeCycleAction],
    completed_instructions: list[RuntimeCycleAction],
    issues: list[RuntimeCycleIssue],
) -> RuntimeCycleResult:
    """Build the cycle result and persist its reconciliation audit row."""

    cycle_completed_at = _complete_cycle_timestamp(cycle_started_at)
    result = _build_runtime_cycle_result(
        cycle_started_at=cycle_started_at,
        cycle_completed_at=cycle_completed_at,
        runtime_timezone=runtime_timezone,
        submitted_entries=submitted_entries,
        cancelled_entries=cancelled_entries,
        filled_entries=filled_entries,
        submitted_exits=submitted_exits,
        completed_instructions=completed_instructions,
        issues=issues,
    )
    try:
        _persist_runtime_cycle_audit(
            session_factory,
            run_kind=run_kind,
            broker_config=broker_config,
            runtime_timezone=runtime_timezone,
            cycle_started_at=cycle_started_at,
            cycle_completed_at=cycle_completed_at,
            submit_due_entries=submit_due_entries,
            due_instruction_count=due_instruction_count,
            active_instruction_count=active_instruction_count,
            snapshot=snapshot,
            submitted_entries=submitted_entries,
            cancelled_entries=cancelled_entries,
            filled_entries=filled_entries,
            submitted_exits=submitted_exits,
            completed_instructions=completed_instructions,
            issues=issues,
        )
    except Exception as exc:  # pragma: no cover - broad by design for runtime safety
        issues.append(
            RuntimeCycleIssue(
                instruction_id=None,
                stage="runtime_cycle_audit",
                message=str(exc),
            )
        )
        cycle_completed_at = _complete_cycle_timestamp(cycle_started_at)
        result = _build_runtime_cycle_result(
            cycle_started_at=cycle_started_at,
            cycle_completed_at=cycle_completed_at,
            runtime_timezone=runtime_timezone,
            submitted_entries=submitted_entries,
            cancelled_entries=cancelled_entries,
            filled_entries=filled_entries,
            submitted_exits=submitted_exits,
            completed_instructions=completed_instructions,
            issues=issues,
        )
    return result


def _complete_cycle_timestamp(cycle_started_at: datetime) -> datetime:
    completed_at = utc_now()
    if completed_at < cycle_started_at:
        return cycle_started_at
    return completed_at


def _build_runtime_cycle_result(
    *,
    cycle_started_at: datetime,
    cycle_completed_at: datetime,
    runtime_timezone: str,
    submitted_entries: list[RuntimeCycleAction],
    cancelled_entries: list[RuntimeCycleAction],
    filled_entries: list[RuntimeCycleAction],
    submitted_exits: list[RuntimeCycleAction],
    completed_instructions: list[RuntimeCycleAction],
    issues: list[RuntimeCycleIssue],
) -> RuntimeCycleResult:
    return RuntimeCycleResult(
        cycle_started_at=cycle_started_at,
        cycle_completed_at=cycle_completed_at,
        runtime_timezone=runtime_timezone,
        submitted_entries=tuple(submitted_entries),
        cancelled_entries=tuple(cancelled_entries),
        filled_entries=tuple(filled_entries),
        submitted_exits=tuple(submitted_exits),
        completed_instructions=tuple(completed_instructions),
        issues=tuple(issues),
    )


def _persist_runtime_cycle_audit(
    session_factory: sessionmaker[Session],
    *,
    run_kind: str,
    broker_config: IbkrConnectionConfig,
    runtime_timezone: str,
    cycle_started_at: datetime,
    cycle_completed_at: datetime,
    submit_due_entries: bool,
    due_instruction_count: int,
    active_instruction_count: int,
    snapshot: BrokerRuntimeSnapshot | None,
    submitted_entries: list[RuntimeCycleAction],
    cancelled_entries: list[RuntimeCycleAction],
    filled_entries: list[RuntimeCycleAction],
    submitted_exits: list[RuntimeCycleAction],
    completed_instructions: list[RuntimeCycleAction],
    issues: list[RuntimeCycleIssue],
) -> None:
    snapshot_counts = (
        {
            "account_count": len(snapshot.account_values),
            "open_order_count": len(snapshot.open_orders),
            "execution_count": len(snapshot.executions),
            "position_count": len(snapshot.positions),
            "portfolio_count": len(snapshot.portfolio),
        }
        if snapshot is not None
        else None
    )
    action_payload = {
        "submitted_entries": [serialize_for_json(asdict(action)) for action in submitted_entries],
        "cancelled_entries": [serialize_for_json(asdict(action)) for action in cancelled_entries],
        "filled_entries": [serialize_for_json(asdict(action)) for action in filled_entries],
        "submitted_exits": [serialize_for_json(asdict(action)) for action in submitted_exits],
        "completed_instructions": [
            serialize_for_json(asdict(action)) for action in completed_instructions
        ],
    }
    action_count = sum(len(entries) for entries in action_payload.values())

    with session_scope(session_factory) as session:
        if _should_suppress_broker_outage_audit(
            action_count=action_count,
            issues=issues,
        ) and _suppress_repeated_broker_outage_audit(
            session,
            run_kind=run_kind,
            broker_config=broker_config,
            cycle_started_at=cycle_started_at,
            cycle_completed_at=cycle_completed_at,
            submit_due_entries=submit_due_entries,
            due_instruction_count=due_instruction_count,
            active_instruction_count=active_instruction_count,
            issues=issues,
        ):
            return

        run_record = ReconciliationRunRecord(
            run_kind=run_kind,
            broker_kind=BROKER_KIND_IBKR,
            account_key=broker_config.account_id,
            runtime_timezone=runtime_timezone,
            started_at=cycle_started_at,
            completed_at=cycle_completed_at,
            status="WARNINGS" if issues else "CLEAN",
            issue_count=len(issues),
            action_count=action_count,
            metadata_json=serialize_for_json(
                {
                    "submit_due_entries": submit_due_entries,
                    "due_instruction_count": due_instruction_count,
                    "active_instruction_count": active_instruction_count,
                    "snapshot_counts": snapshot_counts,
                    "actions": action_payload,
                }
            ),
        )
        session.add(run_record)
        session.flush()

        for issue in issues:
            session.add(
                ReconciliationIssueRecord(
                    reconciliation_run_id=run_record.id,
                    instruction_id=issue.instruction_id,
                    stage=issue.stage,
                    severity="ERROR",
                    message=issue.message,
                    observed_at=cycle_completed_at,
                    payload={},
                )
            )


def _normalise_broker_outage_message(message: str) -> str:
    lowered = message.lower()
    if "cooling down" in lowered:
        return "broker api connection cooling down"
    if "failed to connect to ibkr" in lowered:
        return "failed to connect to ibkr api"
    if "no response received" in lowered or "timed out" in lowered or "timeout" in lowered:
        return "broker api handshake timed out"
    if "nextvalidid" in lowered or "api startup" in lowered:
        return "broker api startup did not complete"
    if "broker snapshot was unavailable" in lowered:
        return "broker snapshot unavailable"
    if "connection refused" in lowered or "connection reset" in lowered or "socket" in lowered:
        return "broker api socket unavailable"
    if "not connected" in lowered:
        return "broker api not connected"
    if "heartbeat" in lowered:
        return "broker heartbeat failed"
    return "broker api unavailable"


def _is_broker_outage_issue(issue: RuntimeCycleIssue) -> bool:
    if issue.stage not in BROKER_OUTAGE_AUDIT_STAGES:
        return False
    message = issue.message.lower()
    return any(marker in message for marker in BROKER_OUTAGE_MESSAGE_MARKERS)


def _broker_outage_issue_signature(
    issues: list[RuntimeCycleIssue] | tuple[RuntimeCycleIssue, ...],
) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        sorted(
            (
                issue.instruction_id or "",
                issue.stage,
                _normalise_broker_outage_message(issue.message),
            )
            for issue in issues
        )
    )


def _should_suppress_broker_outage_audit(
    *,
    action_count: int,
    issues: list[RuntimeCycleIssue],
) -> bool:
    return bool(issues) and action_count == 0 and all(
        _is_broker_outage_issue(issue) for issue in issues
    )


def _suppress_repeated_broker_outage_audit(
    session: Session,
    *,
    run_kind: str,
    broker_config: IbkrConnectionConfig,
    cycle_started_at: datetime,
    cycle_completed_at: datetime,
    submit_due_entries: bool,
    due_instruction_count: int,
    active_instruction_count: int,
    issues: list[RuntimeCycleIssue],
) -> bool:
    signature = _broker_outage_issue_signature(issues)
    suppress_after = (
        cycle_completed_at - BROKER_OUTAGE_RECONCILIATION_SUPPRESSION_WINDOW
    )
    candidate_runs = list(
        session.execute(
            select(ReconciliationRunRecord)
            .where(
                ReconciliationRunRecord.run_kind == run_kind,
                ReconciliationRunRecord.broker_kind == BROKER_KIND_IBKR,
                ReconciliationRunRecord.account_key == broker_config.account_id,
                ReconciliationRunRecord.status == "WARNINGS",
                ReconciliationRunRecord.action_count == 0,
                ReconciliationRunRecord.completed_at >= suppress_after,
                ReconciliationRunRecord.issues.any(
                    ReconciliationIssueRecord.archived_at.is_(None)
                ),
            )
            .order_by(
                ReconciliationRunRecord.completed_at.desc(),
                ReconciliationRunRecord.id.desc(),
            )
            .limit(12)
        ).scalars()
    )
    for candidate_run in candidate_runs:
        candidate_issues = list(
            session.execute(
                select(ReconciliationIssueRecord)
                .where(
                    ReconciliationIssueRecord.reconciliation_run_id
                    == candidate_run.id,
                    ReconciliationIssueRecord.archived_at.is_(None),
                )
                .order_by(ReconciliationIssueRecord.id)
            ).scalars()
        )
        candidate_signature = tuple(
            sorted(
                (
                    issue.instruction_id or "",
                    issue.stage,
                    _normalise_broker_outage_message(issue.message),
                )
                for issue in candidate_issues
            )
        )
        if candidate_signature != signature:
            continue

        metadata = dict(candidate_run.metadata_json or {})
        metadata["suppressed_reconciliation_repeats"] = (
            int(metadata.get("suppressed_reconciliation_repeats") or 0) + 1
        )
        metadata["suppressed_reconciliation_window_seconds"] = int(
            BROKER_OUTAGE_RECONCILIATION_SUPPRESSION_WINDOW.total_seconds()
        )
        metadata["last_suppressed_cycle_started_at"] = cycle_started_at.isoformat()
        metadata["last_suppressed_at"] = cycle_completed_at.isoformat()
        metadata["last_suppressed_submit_due_entries"] = submit_due_entries
        metadata["last_suppressed_due_instruction_count"] = due_instruction_count
        metadata["last_suppressed_active_instruction_count"] = active_instruction_count
        candidate_run.completed_at = cycle_completed_at
        candidate_run.metadata_json = serialize_for_json(metadata)
        return True
    return False
