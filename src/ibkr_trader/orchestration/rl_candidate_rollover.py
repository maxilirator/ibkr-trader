from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.orchestration.state_machine import ExecutionState


DEFAULT_RL_CANDIDATE_ROLLOVER_REASON = (
    "Expired model-routed source candidate archived during RL day rollover. "
    "Generated orders, positions, and next-open exits remain owned by their "
    "normal instruction rows."
)


@dataclass(frozen=True, slots=True)
class RlCandidateRolloverResult:
    archived_at: datetime
    cutoff: datetime
    archived_by: str
    archive_reason: str
    matched_candidate_count: int
    archived_candidate_count: int
    candidate_ids: tuple[str, ...]


def archive_expired_rl_candidates(
    session_factory: sessionmaker[Session],
    *,
    cutoff: datetime | None = None,
    requested_by: str = "rl_candidate_rollover",
    reason: str = DEFAULT_RL_CANDIDATE_ROLLOVER_REASON,
    limit: int = 1000,
) -> RlCandidateRolloverResult:
    """Archive stale model-routed source candidates after their trading window.

    Model-routed rows are an input queue for the RL runner, not broker orders.
    Once their `expire_at` has passed they should no longer appear as active
    candidates, even if the generated order or position remains active
    overnight. Those generated rows have their own instruction ids and states.
    """

    requested_by = requested_by.strip()
    if not requested_by:
        raise ValueError("requested_by must be a non-empty string")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if limit > 5000:
        raise ValueError("limit must be at most 5000")

    cutoff = cutoff or utc_now()
    archived_at = utc_now()
    with session_scope(session_factory) as session:
        records = tuple(
            session.execute(
                select(InstructionRecord)
                .where(
                    InstructionRecord.archived_at.is_(None),
                    InstructionRecord.state
                    == ExecutionState.MODEL_ROUTED_PENDING.value,
                    InstructionRecord.expire_at <= cutoff,
                )
                .order_by(InstructionRecord.expire_at.asc(), InstructionRecord.id.asc())
                .limit(limit)
            ).scalars()
        )

        archived_ids: list[str] = []
        for record in records:
            record.archived_at = archived_at
            record.archived_by = requested_by
            record.archive_reason = reason
            record.updated_at = archived_at
            session.add(
                InstructionEventRecord(
                    instruction_id=record.id,
                    event_type="rl_candidate_archived",
                    source="rl_candidate_rollover",
                    event_at=archived_at,
                    state_before=record.state,
                    state_after=record.state,
                    payload={
                        "requested_by": requested_by,
                        "reason": reason,
                        "cutoff": cutoff.isoformat(),
                    },
                    note=reason,
                )
            )
            archived_ids.append(record.instruction_id)

    return RlCandidateRolloverResult(
        archived_at=archived_at,
        cutoff=cutoff,
        archived_by=requested_by,
        archive_reason=reason,
        matched_candidate_count=len(records),
        archived_candidate_count=len(archived_ids),
        candidate_ids=tuple(archived_ids),
    )


def serialize_rl_candidate_rollover_result(
    result: RlCandidateRolloverResult,
) -> dict[str, Any]:
    return {
        "archived_at": result.archived_at.isoformat(),
        "cutoff": result.cutoff.isoformat(),
        "archived_by": result.archived_by,
        "archive_reason": result.archive_reason,
        "matched_candidate_count": result.matched_candidate_count,
        "archived_candidate_count": result.archived_candidate_count,
        "candidate_ids": list(result.candidate_ids),
    }
