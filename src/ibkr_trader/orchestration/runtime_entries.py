from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.models import InstructionEventRecord, InstructionRecord
from ibkr_trader.orchestration.entry_submission import submit_persisted_instruction_entry
from ibkr_trader.orchestration.runtime_broker_errors import broker_exception_payload as _broker_exception_payload
from ibkr_trader.orchestration.runtime_broker_errors import is_retryable_broker_error as _is_retryable_broker_error
from ibkr_trader.orchestration.runtime_broker_errors import run_with_broker_retries as _run_with_broker_retries
from ibkr_trader.orchestration.runtime_planning import is_pending_entry_expired as _is_pending_entry_expired
from ibkr_trader.orchestration.runtime_types import RuntimeCycleAction, RuntimeCycleIssue
from ibkr_trader.orchestration.runtime_types import serialize_for_json as _serialize_for_json
from ibkr_trader.orchestration.state_machine import ExecutionState


def _append_issue(
    issues: list[RuntimeCycleIssue],
    *,
    instruction_id: str | None,
    stage: str,
    message: str,
) -> None:
    issues.append(
        RuntimeCycleIssue(
            instruction_id=instruction_id,
            stage=stage,
            message=message,
        )
    )


def _record_runtime_note(
    session_factory: sessionmaker[Session],
    *,
    instruction_id: str,
    event_type: str,
    note: str,
    payload: dict[str, Any],
) -> None:
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == instruction_id)
            .with_for_update()
        ).scalar_one_or_none()
        if record is None:
            return
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type=event_type,
                source="runtime_cycle",
                state_before=record.state,
                state_after=record.state,
                payload=_serialize_for_json(payload),
                note=note,
            )
        )


def _mark_pending_entry_cancelled(
    session_factory: sessionmaker[Session],
    instruction_id: str,
    *,
    note: str,
    event_type: str = "entry_expired_before_submit",
    action: str = "entry_cancelled_before_submit",
) -> RuntimeCycleAction:
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == instruction_id)
            .with_for_update()
        ).scalar_one()
        if record.state != ExecutionState.ENTRY_PENDING.value:
            return RuntimeCycleAction(
                instruction_id=instruction_id,
                action="entry_cancel_skip",
                state=record.state,
                detail={},
            )

        previous_state = record.state
        record.state = ExecutionState.ENTRY_CANCELLED.value
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type=event_type,
                source="runtime_cycle",
                state_before=previous_state,
                state_after=record.state,
                payload={},
                note=note,
            )
        )
        return RuntimeCycleAction(
            instruction_id=instruction_id,
            action=action,
            state=record.state,
            detail={"note": note},
        )


def _mark_pending_entry_failed(
    session_factory: sessionmaker[Session],
    instruction_id: str,
    *,
    note: str,
    payload: dict[str, Any],
    event_type: str = "entry_submit_failed",
) -> RuntimeCycleAction:
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == instruction_id)
            .with_for_update()
        ).scalar_one()
        if record.state != ExecutionState.ENTRY_PENDING.value:
            return RuntimeCycleAction(
                instruction_id=instruction_id,
                action="entry_fail_skip",
                state=record.state,
                detail={},
            )

        previous_state = record.state
        record.state = ExecutionState.FAILED.value
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type=event_type,
                source="runtime_cycle",
                state_before=previous_state,
                state_after=record.state,
                payload=_serialize_for_json(payload),
                note=note,
            )
        )
        return RuntimeCycleAction(
            instruction_id=instruction_id,
            action="entry_failed",
            state=record.state,
            detail=_serialize_for_json(payload),
        )





def _submit_due_pending_entries(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    *,
    due_instruction_ids: list[str],
    cycle_started_at: datetime,
    session_calendar_path: Path,
    timeout: int,
    kill_switch_enabled: bool,
    entry_submitter: Callable[..., Any] | None,
    broker_retry_delays: tuple[float, ...],
    sleep_fn: Callable[[float], None],
    submitted_entries: list[RuntimeCycleAction],
    cancelled_entries: list[RuntimeCycleAction],
    issues: list[RuntimeCycleIssue],
) -> None:
    if not due_instruction_ids:
        return

    if kill_switch_enabled:
        _append_issue(
            issues,
            instruction_id=None,
            stage="kill_switch",
            message=(
                "Global kill switch is enabled; skipped submission of "
                f"{len(due_instruction_ids)} due entries."
            ),
        )
        return

    for instruction_id in due_instruction_ids:
        with session_scope(session_factory) as session:
            current_state = session.execute(
                select(InstructionRecord.state).where(
                    InstructionRecord.instruction_id == instruction_id
                )
            ).scalar_one_or_none()
        if current_state is None:
            _append_issue(
                issues,
                instruction_id=instruction_id,
                stage="entry_submit",
                message=f"Instruction '{instruction_id}' was not found.",
            )
            continue
        if current_state != ExecutionState.ENTRY_PENDING.value:
            _record_runtime_note(
                session_factory,
                instruction_id=instruction_id,
                event_type="runtime_entry_submit_skipped",
                note="Runtime skipped a stale due-entry candidate because it is no longer pending.",
                payload={
                    "current_state": current_state,
                    "expected_state": ExecutionState.ENTRY_PENDING.value,
                },
            )
            continue

        if _is_pending_entry_expired(
                session_factory,
                instruction_id=instruction_id,
                cycle_at=cycle_started_at,
                session_calendar_path=session_calendar_path,
        ):
            cancelled_entries.append(
                _mark_pending_entry_cancelled(
                    session_factory,
                    instruction_id,
                    note=(
                        "Entry window expired before the runtime could submit the order "
                        "to IBKR."
                    ),
                )
            )
            continue

        try:
            submission = _run_with_broker_retries(
                lambda: submit_persisted_instruction_entry(
                    session_factory,
                    broker_config,
                    instruction_id,
                    timeout=timeout,
                    submitter=entry_submitter,
                ),
                retry_delays=broker_retry_delays,
                sleep_fn=sleep_fn,
            )
            submitted_entries.append(
                RuntimeCycleAction(
                    instruction_id=instruction_id,
                    action="entry_submitted",
                    state=submission.state,
                    detail={
                        "broker_order_id": submission.broker_order_id,
                        "broker_order_status": submission.broker_order_status,
                    },
                )
            )
        except Exception as exc:  # pragma: no cover - broad by design for runtime safety
            _append_issue(
                issues,
                instruction_id=instruction_id,
                stage="entry_submit",
                message=str(exc),
            )
            if _is_retryable_broker_error(exc):
                _record_runtime_note(
                    session_factory,
                    instruction_id=instruction_id,
                    event_type="runtime_entry_submit_failed",
                    note="Runtime cycle could not submit the due entry order.",
                    payload=_broker_exception_payload(exc),
                )
                continue

            _mark_pending_entry_failed(
                session_factory,
                instruction_id,
                note=(
                    "Runtime cycle marked the due entry as failed after a terminal "
                    "broker submission error."
                ),
                payload=_broker_exception_payload(exc),
            )
