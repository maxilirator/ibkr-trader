from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from zoneinfo import ZoneInfo
from zoneinfo import ZoneInfoNotFoundError

from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.domain.execution_contract import ExecutionInstructionBatch
from ibkr_trader.orchestration.session_calendar import find_matching_session_boundary
from ibkr_trader.orchestration.session_calendar import find_next_session_open
from ibkr_trader.orchestration.session_calendar import find_session_for_date


_STOCKHOLM_EXCHANGE_CODES = {"XSTO", "SFB"}


class NextSessionExitStatus(StrEnum):
    NOT_REQUESTED = "not_requested"
    RESOLVED = "resolved"
    CALENDAR_REQUIRED = "calendar_required"


@dataclass(slots=True)
class NextSessionExitPreview:
    requested: bool
    status: NextSessionExitStatus
    reference_after_local: datetime | None = None
    reference_after_date: date | None = None
    next_session_open_local: datetime | None = None
    next_session_open_utc: datetime | None = None
    session_kind: str | None = None
    calendar_source: str | None = None
    note: str | None = None


@dataclass(slots=True)
class InstructionRuntimeSchedule:
    instruction_id: str
    runtime_timezone: str
    submit_at_utc: datetime
    submit_at_runtime: datetime
    expire_at_utc: datetime
    expire_at_runtime: datetime
    entry_window_seconds: int
    next_session_exit: NextSessionExitPreview


@dataclass(slots=True)
class BatchRuntimeSchedule:
    schema_version: str
    batch_id: str
    runtime_timezone: str
    generated_at_utc: datetime
    instructions: tuple[InstructionRuntimeSchedule, ...]


def resolve_runtime_timezone(timezone_name: str) -> ZoneInfo:
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Unknown runtime timezone: {timezone_name}") from exc


def _build_next_session_exit_preview(
    instruction: ExecutionInstruction,
    *,
    expire_at_runtime: datetime,
    session_calendar_path: Path | None,
) -> NextSessionExitPreview:
    if instruction.exit is None:
        return NextSessionExitPreview(
            requested=False,
            status=NextSessionExitStatus.NOT_REQUESTED,
        )
    if not instruction.exit.force_exit_next_session_open:
        return NextSessionExitPreview(
            requested=False,
            status=NextSessionExitStatus.NOT_REQUESTED,
        )

    uses_stockholm_calendar = uses_stockholm_session_calendar(instruction)
    if uses_stockholm_calendar and session_calendar_path is not None:
        try:
            resolution = find_next_session_open(
                expire_at_runtime,
                session_calendar_path=session_calendar_path,
            )
        except (FileNotFoundError, ValueError) as exc:
            return NextSessionExitPreview(
                requested=True,
                status=NextSessionExitStatus.CALENDAR_REQUIRED,
                reference_after_local=expire_at_runtime,
                reference_after_date=expire_at_runtime.date(),
                note=str(exc),
            )

        if resolution is not None:
            return NextSessionExitPreview(
                requested=True,
                status=NextSessionExitStatus.RESOLVED,
                reference_after_local=expire_at_runtime,
                reference_after_date=expire_at_runtime.date(),
                next_session_open_local=resolution.open_at,
                next_session_open_utc=resolution.open_at.astimezone(timezone.utc),
                session_kind=resolution.session_kind,
                calendar_source=resolution.source_path,
                note="Resolved from the local q-data session calendar.",
            )

    return NextSessionExitPreview(
        requested=True,
        status=NextSessionExitStatus.CALENDAR_REQUIRED,
        reference_after_local=expire_at_runtime,
        reference_after_date=expire_at_runtime.date(),
        note=(
            "Next-session exit resolution needs an exchange calendar; this preview "
            "only anchors the request after the entry expiry window."
        ),
    )


def uses_stockholm_session_calendar(instruction: ExecutionInstruction) -> bool:
    return (
        instruction.instrument.exchange in _STOCKHOLM_EXCHANGE_CODES
        or instruction.instrument.primary_exchange in _STOCKHOLM_EXCHANGE_CODES
    )


def resolve_scheduled_submission_due_at(
    instruction: ExecutionInstruction,
    *,
    scheduled_at: datetime,
    session_calendar_path: Path | None,
    submission_lead_time: timedelta,
) -> datetime:
    scheduled_at_utc = scheduled_at.astimezone(timezone.utc)
    if submission_lead_time <= timedelta(0):
        return scheduled_at_utc
    if session_calendar_path is None or not uses_stockholm_session_calendar(instruction):
        return scheduled_at_utc

    try:
        resolution = find_matching_session_boundary(
            scheduled_at,
            session_calendar_path=session_calendar_path,
        )
    except (FileNotFoundError, ValueError):
        return scheduled_at_utc
    if resolution is None:
        return scheduled_at_utc

    due_at = resolution.boundary_at.astimezone(timezone.utc) - submission_lead_time
    return due_at if due_at < scheduled_at_utc else scheduled_at_utc


def resolve_effective_entry_expire_at(
    instruction: ExecutionInstruction,
    *,
    session_calendar_path: Path | None,
) -> datetime:
    if instruction.entry is None:
        raise ValueError("entry must be an object")

    return resolve_effective_entry_expire_at_for_schedule(
        instruction,
        submit_at=instruction.entry.submit_at,
        expire_at=instruction.entry.expire_at,
        session_calendar_path=session_calendar_path,
    )


def resolve_effective_entry_expire_at_for_schedule(
    instruction: ExecutionInstruction,
    *,
    submit_at: datetime,
    expire_at: datetime,
    session_calendar_path: Path | None,
) -> datetime:
    if submit_at.tzinfo is None:
        submit_at = submit_at.replace(tzinfo=timezone.utc)
    if expire_at.tzinfo is None:
        expire_at = expire_at.replace(tzinfo=timezone.utc)
    if session_calendar_path is None or not uses_stockholm_session_calendar(instruction):
        return expire_at

    session_date = submit_at.astimezone(ZoneInfo("Europe/Stockholm")).date()
    try:
        session = find_session_for_date(
            session_date,
            session_calendar_path=session_calendar_path,
        )
    except (FileNotFoundError, ValueError):
        return expire_at
    if session is None:
        return expire_at

    session_close = session.close_at
    return session_close if session_close < expire_at else expire_at


def build_instruction_runtime_schedule(
    instruction: ExecutionInstruction,
    *,
    runtime_timezone: str,
    session_calendar_path: Path | None = None,
) -> InstructionRuntimeSchedule:
    instruction.validate()
    runtime_zone = resolve_runtime_timezone(runtime_timezone)

    if instruction.is_model_routed:
        if instruction.execution is None:
            raise ValueError("execution is required for model-routed instructions")
        submit_at = instruction.execution.window.start_at
        expire_at = instruction.execution.window.end_at
    else:
        if instruction.entry is None:
            raise ValueError("entry must be an object")
        submit_at = instruction.entry.submit_at
        expire_at = instruction.entry.expire_at

    submit_at_utc = submit_at.astimezone(timezone.utc)
    expire_at_utc = expire_at.astimezone(timezone.utc)
    submit_at_runtime = submit_at.astimezone(runtime_zone)
    expire_at_runtime = expire_at.astimezone(runtime_zone)

    return InstructionRuntimeSchedule(
        instruction_id=instruction.instruction_id,
        runtime_timezone=runtime_timezone,
        submit_at_utc=submit_at_utc,
        submit_at_runtime=submit_at_runtime,
        expire_at_utc=expire_at_utc,
        expire_at_runtime=expire_at_runtime,
        entry_window_seconds=int((expire_at_utc - submit_at_utc).total_seconds()),
        next_session_exit=_build_next_session_exit_preview(
            instruction,
            expire_at_runtime=expire_at_runtime,
            session_calendar_path=session_calendar_path,
        ),
    )


def build_batch_runtime_schedule(
    batch: ExecutionInstructionBatch,
    *,
    runtime_timezone: str,
    session_calendar_path: Path | None = None,
) -> BatchRuntimeSchedule:
    batch.validate()
    return BatchRuntimeSchedule(
        schema_version=batch.schema_version,
        batch_id=batch.source.batch_id,
        runtime_timezone=runtime_timezone,
        generated_at_utc=batch.source.generated_at.astimezone(timezone.utc),
        instructions=tuple(
            build_instruction_runtime_schedule(
                instruction,
                runtime_timezone=runtime_timezone,
                session_calendar_path=session_calendar_path,
            )
            for instruction in batch.instructions
        ),
    )
