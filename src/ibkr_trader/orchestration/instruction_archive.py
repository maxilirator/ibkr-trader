from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord


class InstructionArchiveSelectorError(ValueError):
    """Raised when an archive request is too broad or unsafe."""


@dataclass(frozen=True, slots=True)
class InstructionArchiveResult:
    archived_at: datetime
    archived_by: str
    archive_reason: str | None
    matched_instruction_count: int
    archived_instruction_count: int
    skipped_instruction_count: int
    instruction_ids: tuple[str, ...]


_ARCHIVE_SAFE_STATES = {
    "ENTRY_CANCELLED",
    "COMPLETED",
    "FAILED",
    "MODEL_ROUTED_PENDING",
}


def archive_instruction_set(
    session_factory: sessionmaker[Session],
    *,
    requested_by: str,
    reason: str | None = None,
    instruction_ids: tuple[str, ...] | None = None,
    states: tuple[str, ...] | None = None,
    batch_id: str | None = None,
    account_key: str | None = None,
    book_key: str | None = None,
    source_system: str | None = None,
    model_routed: bool | None = None,
    expire_before: datetime | None = None,
    include_active: bool = False,
    limit: int = 500,
) -> InstructionArchiveResult:
    requested_by = requested_by.strip()
    if not requested_by:
        raise InstructionArchiveSelectorError("requested_by must be a non-empty string")
    if limit <= 0:
        raise InstructionArchiveSelectorError("limit must be positive")
    if limit > 1000:
        raise InstructionArchiveSelectorError("limit must be at most 1000")

    has_selector = any(
        selector is not None
        for selector in (
            instruction_ids,
            states,
            batch_id,
            account_key,
            book_key,
            source_system,
            model_routed,
            expire_before,
        )
    )
    if not has_selector:
        raise InstructionArchiveSelectorError(
            "Provide at least one archive selector."
        )

    normalized_states = tuple(state.strip().upper() for state in states or ())
    if states is not None and not normalized_states:
        raise InstructionArchiveSelectorError("states must contain at least one state")

    archived_at = utc_now()
    with session_scope(session_factory) as session:
        statement = select(InstructionRecord).where(
            InstructionRecord.archived_at.is_(None)
        )
        if instruction_ids is not None:
            statement = statement.where(InstructionRecord.instruction_id.in_(instruction_ids))
        if normalized_states:
            statement = statement.where(InstructionRecord.state.in_(normalized_states))
        if batch_id is not None:
            statement = statement.where(InstructionRecord.batch_id == batch_id)
        if account_key is not None:
            statement = statement.where(InstructionRecord.account_key == account_key)
        if book_key is not None:
            statement = statement.where(InstructionRecord.book_key == book_key)
        if source_system is not None:
            statement = statement.where(InstructionRecord.source_system == source_system)
        if expire_before is not None:
            statement = statement.where(InstructionRecord.expire_at < expire_before)
        if model_routed is True:
            statement = statement.where(
                or_(
                    InstructionRecord.state == "MODEL_ROUTED_PENDING",
                    InstructionRecord.order_type == "MODEL_ROUTED",
                )
            )
        elif model_routed is False:
            statement = statement.where(
                InstructionRecord.state != "MODEL_ROUTED_PENDING",
                InstructionRecord.order_type != "MODEL_ROUTED",
            )

        records = tuple(
            session.execute(
                statement.order_by(InstructionRecord.id.desc()).limit(limit)
            ).scalars()
        )
        archived_ids: list[str] = []
        skipped_count = 0
        for record in records:
            if not include_active and record.state not in _ARCHIVE_SAFE_STATES:
                skipped_count += 1
                continue
            record.archived_at = archived_at
            record.archived_by = requested_by
            record.archive_reason = reason
            record.updated_at = archived_at
            session.add(
                InstructionEventRecord(
                    instruction_id=record.id,
                    event_type="instruction_archived",
                    source="operator_archive",
                    event_at=archived_at,
                    state_before=record.state,
                    state_after=record.state,
                    payload={
                        "requested_by": requested_by,
                        "reason": reason,
                        "include_active": include_active,
                    },
                    note=reason,
                )
            )
            archived_ids.append(record.instruction_id)

    return InstructionArchiveResult(
        archived_at=archived_at,
        archived_by=requested_by,
        archive_reason=reason,
        matched_instruction_count=len(records),
        archived_instruction_count=len(archived_ids),
        skipped_instruction_count=skipped_count,
        instruction_ids=tuple(archived_ids),
    )


def serialize_instruction_archive_result(
    result: InstructionArchiveResult,
) -> dict[str, Any]:
    return {
        "archived_at": result.archived_at.isoformat(),
        "archived_by": result.archived_by,
        "archive_reason": result.archive_reason,
        "matched_instruction_count": result.matched_instruction_count,
        "archived_instruction_count": result.archived_instruction_count,
        "skipped_instruction_count": result.skipped_instruction_count,
        "instruction_ids": list(result.instruction_ids),
    }
