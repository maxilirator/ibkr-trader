from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from decimal import InvalidOperation
from typing import Any, Mapping

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.orchestration.state_machine import ExecutionState


DEFAULT_RL_CANDIDATE_LIFECYCLE_REASON = (
    "Model-routed RL candidate retired after its per-day lifecycle completed. "
    "Generated orders, fills, and positions remain owned by their normal "
    "instruction rows."
)


@dataclass(frozen=True, slots=True)
class RlCandidateLifecycleRetirementResult:
    retired_at: datetime
    retired_by: str
    retire_reason: str
    matched_candidate_count: int
    retired_candidate_count: int
    candidate_ids: tuple[str, ...]


def retire_completed_rl_candidates(
    session_factory: sessionmaker[Session],
    *,
    requested_by: str = "rl_candidate_lifecycle",
    reason: str = DEFAULT_RL_CANDIDATE_LIFECYCLE_REASON,
    limit: int = 1000,
) -> RlCandidateLifecycleRetirementResult:
    requested_by = requested_by.strip()
    if not requested_by:
        raise ValueError("requested_by must be a non-empty string")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if limit > 5000:
        raise ValueError("limit must be at most 5000")

    retired_at = utc_now()
    with session_scope(session_factory) as session:
        records = tuple(
            session.execute(
                select(InstructionRecord)
                .where(
                    InstructionRecord.archived_at.is_(None),
                    InstructionRecord.state
                    == ExecutionState.MODEL_ROUTED_PENDING.value,
                    InstructionRecord.order_type == "MODEL_ROUTED",
                )
                .order_by(InstructionRecord.id.desc())
                .limit(limit)
            ).scalars()
        )

        matched_ids: list[str] = []
        retired_ids: list[str] = []
        for candidate in records:
            lifecycle = _candidate_lifecycle(candidate)
            if lifecycle is None or not _retire_when_flat(lifecycle):
                continue
            matched_ids.append(candidate.instruction_id)
            detail = _candidate_lifecycle_retirement_detail(
                session,
                candidate,
                lifecycle=lifecycle,
            )
            if detail is None:
                continue

            candidate.archived_at = retired_at
            candidate.archived_by = requested_by
            candidate.archive_reason = reason
            candidate.updated_at = retired_at
            session.add(
                InstructionEventRecord(
                    instruction_id=candidate.id,
                    event_type="rl_candidate_lifecycle_retired",
                    source="rl_candidate_lifecycle",
                    event_at=retired_at,
                    state_before=candidate.state,
                    state_after=candidate.state,
                    payload={
                        "requested_by": requested_by,
                        "reason": reason,
                        "lifecycle": dict(lifecycle),
                        "retirement_detail": detail,
                    },
                    note=reason,
                )
            )
            retired_ids.append(candidate.instruction_id)

    return RlCandidateLifecycleRetirementResult(
        retired_at=retired_at,
        retired_by=requested_by,
        retire_reason=reason,
        matched_candidate_count=len(matched_ids),
        retired_candidate_count=len(retired_ids),
        candidate_ids=tuple(retired_ids),
    )


def _candidate_lifecycle(record: InstructionRecord) -> Mapping[str, Any] | None:
    instruction = _stored_instruction(record)
    if instruction is None:
        return None
    lifecycle = instruction.get("lifecycle")
    if not isinstance(lifecycle, Mapping):
        return None
    scope = str(lifecycle.get("scope") or "").strip()
    if scope != "account_book_side_symbol_trade_date":
        return None
    return lifecycle


def _retire_when_flat(lifecycle: Mapping[str, Any]) -> bool:
    return _policy_bool(lifecycle.get("retire_from_active_universe_when_flat"))


def _candidate_lifecycle_retirement_detail(
    session: Session,
    candidate: InstructionRecord,
    *,
    lifecycle: Mapping[str, Any],
) -> dict[str, Any] | None:
    generated = _generated_records_for_candidate(session, candidate)
    if not generated:
        return None

    completed_round_trips = [
        record for record in generated if _is_completed_round_trip(record)
    ]
    if completed_round_trips and not _policy_bool(
        lifecycle.get("allow_reentry_after_exit")
    ):
        return {
            "retirement_trigger": "completed_entry_and_exit",
            "generated_instruction_ids": [
                record.instruction_id for record in completed_round_trips
            ],
        }

    max_entry_orders = _positive_int_or_none(lifecycle.get("max_entry_orders"))
    if (
        max_entry_orders is not None
        and not _policy_bool(lifecycle.get("allow_reentry_after_cancel"))
        and len(generated) >= max_entry_orders
        and all(_is_terminal_without_entry_fill(record) for record in generated)
    ):
        return {
            "retirement_trigger": "entry_order_terminal_without_fill",
            "generated_instruction_ids": [
                record.instruction_id for record in generated
            ],
        }

    return None


def _generated_records_for_candidate(
    session: Session,
    candidate: InstructionRecord,
) -> tuple[InstructionRecord, ...]:
    rows = tuple(
        session.execute(
            select(InstructionRecord)
            .where(
                InstructionRecord.account_key == candidate.account_key,
                InstructionRecord.book_key == candidate.book_key,
                InstructionRecord.symbol == candidate.symbol,
                InstructionRecord.source_system == "rl-runner",
            )
            .order_by(InstructionRecord.id.asc())
        ).scalars()
    )
    return tuple(
        row
        for row in rows
        if _source_instruction_id(row) == candidate.instruction_id
    )


def _source_instruction_id(record: InstructionRecord) -> str | None:
    instruction = _stored_instruction(record)
    if instruction is None:
        return None
    trace = instruction.get("trace")
    if not isinstance(trace, Mapping):
        return None
    metadata = trace.get("metadata")
    if not isinstance(metadata, Mapping):
        return None
    value = metadata.get("rl_source_instruction_id")
    return str(value) if value is not None else None


def _stored_instruction(record: InstructionRecord) -> Mapping[str, Any] | None:
    payload = record.payload if isinstance(record.payload, Mapping) else {}
    instruction = payload.get("instruction")
    return instruction if isinstance(instruction, Mapping) else None


def _is_completed_round_trip(record: InstructionRecord) -> bool:
    return (
        record.state == ExecutionState.COMPLETED.value
        and _decimal_quantity(record.entry_filled_quantity) > 0
        and _decimal_quantity(record.exit_filled_quantity) > 0
    )


def _is_terminal_without_entry_fill(record: InstructionRecord) -> bool:
    return (
        record.state
        in {
            ExecutionState.ENTRY_CANCELLED.value,
            ExecutionState.FAILED.value,
        }
        and _decimal_quantity(record.entry_filled_quantity) <= 0
    )


def _decimal_quantity(value: str | None) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _positive_int_or_none(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _policy_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes"}
    return False
