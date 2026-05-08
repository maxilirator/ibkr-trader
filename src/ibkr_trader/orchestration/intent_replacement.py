from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from typing import Callable

from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.domain.execution_contract import ExecutionInstructionBatch
from ibkr_trader.domain.execution_payloads import parse_execution_instruction_payload
from ibkr_trader.orchestration.entry_submission import (
    cancel_persisted_instruction_entry,
)
from ibkr_trader.orchestration.entry_submission import (
    serialize_persisted_broker_cancellation,
)
from ibkr_trader.orchestration.state_machine import ExecutionState


_ENTRY_ACTIVE_STATES = {
    ExecutionState.ENTRY_PENDING.value,
    ExecutionState.ENTRY_SUBMITTED.value,
}
_POSITION_ACTIVE_STATES = {
    ExecutionState.POSITION_OPEN.value,
    ExecutionState.EXIT_PENDING.value,
}
_INTENT_ACTIVE_STATES = _ENTRY_ACTIVE_STATES | _POSITION_ACTIVE_STATES


class IntentReplacementConflictError(ValueError):
    """Raised when current broker/instruction state blocks a replacement."""

    def __init__(
        self,
        message: str,
        *,
        result: "IntentCleanupResult | None" = None,
    ) -> None:
        super().__init__(message)
        self.result = result


class IntentCleanupSelectorError(ValueError):
    """Raised when an operator cleanup request is too broad or malformed."""


@dataclass(frozen=True, slots=True)
class IntentGroupKey:
    account_key: str
    book_key: str
    book_side: str
    symbol: str
    exchange: str
    currency: str

    def as_string(self) -> str:
        return (
            f"{self.account_key}|{self.book_key}|{self.book_side}|"
            f"{self.symbol}|{self.exchange}|{self.currency}"
        )


@dataclass(frozen=True, slots=True)
class IntentCleanupAction:
    instruction_id: str
    group_key: IntentGroupKey
    state_before: str
    state_after: str
    action: str
    reason: str
    broker_order_id: int | None = None
    broker_order_status: str | None = None
    applied: bool = False
    error: str | None = None


@dataclass(frozen=True, slots=True)
class IntentCleanupBlocker:
    instruction_id: str
    group_key: IntentGroupKey
    state: str
    reason: str


@dataclass(frozen=True, slots=True)
class IntentCleanupResult:
    requested_at: datetime
    requested_by: str
    reason: str | None
    apply: bool
    status: str
    group_count: int
    matched_instruction_count: int
    action_count: int
    applied_action_count: int
    cancelled_pending_count: int
    cancelled_submitted_count: int
    blocked_count: int
    failed_count: int
    actions: tuple[IntentCleanupAction, ...]
    blockers: tuple[IntentCleanupBlocker, ...]


def serialize_intent_cleanup_result(result: IntentCleanupResult) -> dict[str, Any]:
    return _serialize_for_json(asdict(result))


def cleanup_intent_groups(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    *,
    requested_by: str,
    reason: str | None = None,
    apply: bool = False,
    account_key: str | None = None,
    book_key: str | None = None,
    book_side: str | None = None,
    symbol: str | None = None,
    exchange: str | None = None,
    currency: str | None = None,
    instruction_ids: tuple[str, ...] | None = None,
    keep_instruction_id: str | None = None,
    cancel_all_entries: bool = False,
    timeout: int = 10,
    canceler: Callable[..., dict[str, Any]] | None = None,
) -> IntentCleanupResult:
    normalized_requested_by = _normalize_requested_by(requested_by)
    normalized_reason = _normalize_reason(reason)
    if timeout <= 0:
        raise IntentCleanupSelectorError("timeout must be positive")

    has_selector = any(
        value is not None
        for value in (
            account_key,
            book_key,
            book_side,
            symbol,
            exchange,
            currency,
            instruction_ids,
        )
    )
    if not has_selector:
        raise IntentCleanupSelectorError(
            "Provide at least one cleanup selector."
        )

    requested_at = utc_now()
    records = _load_active_records(
        session_factory,
        account_key=account_key,
        book_key=book_key,
        book_side=book_side,
        symbol=symbol,
        exchange=exchange,
        currency=currency,
        instruction_ids=instruction_ids,
    )
    plan_actions, blockers, group_count = _plan_cleanup_actions(
        records,
        incoming_group_keys=None,
        incoming_instruction_ids=frozenset(),
        keep_instruction_id=keep_instruction_id,
        cancel_all_entries=cancel_all_entries,
    )
    return _apply_cleanup_plan(
        session_factory,
        broker_config,
        requested_at=requested_at,
        requested_by=normalized_requested_by,
        reason=normalized_reason,
        apply=apply,
        matched_instruction_count=len(records),
        group_count=group_count,
        planned_actions=plan_actions,
        blockers=blockers,
        timeout=timeout,
        canceler=canceler,
    )


def supersede_batch_intent_entries(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    batch: ExecutionInstructionBatch,
    *,
    requested_by: str = "api",
    reason: str | None = None,
    timeout: int = 10,
    canceler: Callable[..., dict[str, Any]] | None = None,
) -> IntentCleanupResult:
    """Cancel older active entries that compete with a submitted batch.

    Open positions are never mutated here. If a position is already open for the
    same group as an incoming entry, the replacement is blocked after any stale
    entries in that group have been cancelled.
    """

    deterministic_instructions = tuple(
        instruction for instruction in batch.instructions if not instruction.is_model_routed
    )
    requested_at = utc_now()
    normalized_requested_by = _normalize_requested_by(requested_by)
    normalized_reason = _normalize_reason(reason)
    if not deterministic_instructions:
        return IntentCleanupResult(
            requested_at=requested_at,
            requested_by=normalized_requested_by,
            reason=normalized_reason,
            apply=True,
            status="NOOP",
            group_count=0,
            matched_instruction_count=0,
            action_count=0,
            applied_action_count=0,
            cancelled_pending_count=0,
            cancelled_submitted_count=0,
            blocked_count=0,
            failed_count=0,
            actions=(),
            blockers=(),
        )
    if timeout <= 0:
        raise IntentCleanupSelectorError("timeout must be positive")

    incoming_groups_by_key: dict[IntentGroupKey, list[str]] = {}
    for instruction in deterministic_instructions:
        incoming_groups_by_key.setdefault(
            intent_group_key_for_instruction(instruction),
            [],
        ).append(instruction.instruction_id)
    duplicate_incoming_groups = {
        group_key: instruction_ids
        for group_key, instruction_ids in incoming_groups_by_key.items()
        if len(instruction_ids) > 1
    }
    if duplicate_incoming_groups:
        duplicate_payload = {
            group_key.as_string(): sorted(instruction_ids)
            for group_key, instruction_ids in duplicate_incoming_groups.items()
        }
        raise IntentReplacementConflictError(
            "Incoming batch contains multiple deterministic entries for the "
            f"same intent group: {duplicate_payload}"
        )

    incoming_group_keys = set(incoming_groups_by_key)
    incoming_instruction_ids = frozenset(
        instruction.instruction_id for instruction in deterministic_instructions
    )
    records = _load_active_records_for_group_keys(
        session_factory,
        incoming_group_keys,
    )
    records = tuple(
        record
        for record in records
        if record.instruction_id not in incoming_instruction_ids
    )
    plan_actions, blockers, group_count = _plan_cleanup_actions(
        records,
        incoming_group_keys=incoming_group_keys,
        incoming_instruction_ids=incoming_instruction_ids,
        keep_instruction_id=None,
    )
    result = _apply_cleanup_plan(
        session_factory,
        broker_config,
        requested_at=requested_at,
        requested_by=normalized_requested_by,
        reason=normalized_reason
        or "Incoming instruction batch superseded older active entries.",
        apply=True,
        matched_instruction_count=len(records),
        group_count=group_count,
        planned_actions=plan_actions,
        blockers=blockers,
        timeout=timeout,
        canceler=canceler,
    )
    if result.failed_count:
        raise IntentReplacementConflictError(
            "Could not clean up older active entries before submitting the new intent.",
            result=result,
        )
    if result.blocked_count:
        raise IntentReplacementConflictError(
            "A current open position already owns this intent group; refusing to "
            "submit a fresh entry that could add or cross risk.",
            result=result,
        )
    return result


def intent_group_key_for_instruction(
    instruction: ExecutionInstruction,
) -> IntentGroupKey:
    book_side = (
        instruction.account.book_side.value
        if instruction.account.book_side is not None
        else instruction.intent.position_side.value
    )
    return IntentGroupKey(
        account_key=_normalize_upper(instruction.account.account_key),
        book_key=_normalize_lower(instruction.account.book_key),
        book_side=_normalize_upper(book_side),
        symbol=_normalize_upper(instruction.instrument.symbol),
        exchange=_normalize_upper(instruction.instrument.exchange),
        currency=_normalize_upper(instruction.instrument.currency),
    )


def intent_group_key_for_record(record: InstructionRecord) -> IntentGroupKey:
    instruction = _parse_record_instruction(record)
    if instruction is not None:
        return intent_group_key_for_instruction(instruction)
    return IntentGroupKey(
        account_key=_normalize_upper(record.account_key),
        book_key=_normalize_lower(record.book_key),
        book_side="LONG" if _normalize_upper(record.side) == "BUY" else "SHORT",
        symbol=_normalize_upper(record.symbol),
        exchange=_normalize_upper(record.exchange),
        currency=_normalize_upper(record.currency),
    )


def _apply_cleanup_plan(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    *,
    requested_at: datetime,
    requested_by: str,
    reason: str | None,
    apply: bool,
    matched_instruction_count: int,
    group_count: int,
    planned_actions: tuple[IntentCleanupAction, ...],
    blockers: tuple[IntentCleanupBlocker, ...],
    timeout: int,
    canceler: Callable[..., dict[str, Any]] | None,
) -> IntentCleanupResult:
    if not apply:
        return _build_result(
            requested_at=requested_at,
            requested_by=requested_by,
            reason=reason,
            apply=False,
            matched_instruction_count=matched_instruction_count,
            group_count=group_count,
            actions=planned_actions,
            blockers=blockers,
        )

    applied_actions: list[IntentCleanupAction] = []
    for action in planned_actions:
        try:
            if action.action == "cancel_pending_entry":
                applied_actions.append(
                    _cancel_pending_entry_for_intent_cleanup(
                        session_factory,
                        action,
                        requested_at=requested_at,
                        requested_by=requested_by,
                        reason=reason,
                    )
                )
            elif action.action == "cancel_submitted_entry":
                applied_actions.append(
                    _cancel_submitted_entry_for_intent_cleanup(
                        session_factory,
                        broker_config,
                        action,
                        requested_at=requested_at,
                        requested_by=requested_by,
                        reason=reason,
                        timeout=timeout,
                        canceler=canceler,
                    )
                )
            else:
                applied_actions.append(
                    IntentCleanupAction(
                        instruction_id=action.instruction_id,
                        group_key=action.group_key,
                        state_before=action.state_before,
                        state_after=action.state_before,
                        action=action.action,
                        reason=action.reason,
                        broker_order_id=action.broker_order_id,
                        broker_order_status=action.broker_order_status,
                        applied=False,
                        error=f"Unsupported cleanup action '{action.action}'.",
                    )
                )
        except Exception as exc:
            applied_actions.append(
                IntentCleanupAction(
                    instruction_id=action.instruction_id,
                    group_key=action.group_key,
                    state_before=action.state_before,
                    state_after=action.state_before,
                    action=action.action,
                    reason=action.reason,
                    broker_order_id=action.broker_order_id,
                    broker_order_status=action.broker_order_status,
                    applied=False,
                    error=str(exc),
                )
            )

    return _build_result(
        requested_at=requested_at,
        requested_by=requested_by,
        reason=reason,
        apply=True,
        matched_instruction_count=matched_instruction_count,
        group_count=group_count,
        actions=tuple(applied_actions),
        blockers=blockers,
    )


def _build_result(
    *,
    requested_at: datetime,
    requested_by: str,
    reason: str | None,
    apply: bool,
    matched_instruction_count: int,
    group_count: int,
    actions: tuple[IntentCleanupAction, ...],
    blockers: tuple[IntentCleanupBlocker, ...],
) -> IntentCleanupResult:
    failed_count = sum(1 for action in actions if action.error is not None)
    applied_action_count = sum(1 for action in actions if action.applied)
    blocked_count = len(blockers)
    cancelled_pending_count = sum(
        1
        for action in actions
        if action.applied and action.action == "cancel_pending_entry"
    )
    cancelled_submitted_count = sum(
        1
        for action in actions
        if action.applied and action.action == "cancel_submitted_entry"
    )
    if failed_count:
        status = "FAILED" if failed_count == len(actions) else "PARTIAL_FAILURE"
    elif blocked_count:
        status = "BLOCKED"
    elif actions:
        status = "APPLIED" if apply else "PLANNED"
    else:
        status = "NOOP"
    return IntentCleanupResult(
        requested_at=requested_at,
        requested_by=requested_by,
        reason=reason,
        apply=apply,
        status=status,
        group_count=group_count,
        matched_instruction_count=matched_instruction_count,
        action_count=len(actions),
        applied_action_count=applied_action_count,
        cancelled_pending_count=cancelled_pending_count,
        cancelled_submitted_count=cancelled_submitted_count,
        blocked_count=blocked_count,
        failed_count=failed_count,
        actions=actions,
        blockers=blockers,
    )


def _plan_cleanup_actions(
    records: tuple[InstructionRecord, ...],
    *,
    incoming_group_keys: set[IntentGroupKey] | None,
    incoming_instruction_ids: frozenset[str],
    keep_instruction_id: str | None,
    cancel_all_entries: bool = False,
) -> tuple[tuple[IntentCleanupAction, ...], tuple[IntentCleanupBlocker, ...], int]:
    groups: dict[IntentGroupKey, list[InstructionRecord]] = {}
    for record in records:
        if record.instruction_id in incoming_instruction_ids:
            continue
        group_key = intent_group_key_for_record(record)
        if incoming_group_keys is not None and group_key not in incoming_group_keys:
            continue
        groups.setdefault(group_key, []).append(record)

    actions: list[IntentCleanupAction] = []
    blockers: list[IntentCleanupBlocker] = []
    for group_key, group_records in groups.items():
        ordered_records = sorted(group_records, key=lambda record: (record.id, record.updated_at))
        active_positions = [
            record for record in ordered_records if record.state in _POSITION_ACTIVE_STATES
        ]
        entries = [
            record for record in ordered_records if record.state in _ENTRY_ACTIVE_STATES
        ]
        if active_positions:
            for record in active_positions:
                blockers.append(
                    IntentCleanupBlocker(
                        instruction_id=record.instruction_id,
                        group_key=group_key,
                        state=record.state,
                        reason=(
                            "Open position owns this intent group; automatic cleanup "
                            "will not cancel or replace positions."
                        ),
                    )
                )
        if cancel_all_entries:
            stale_entries = entries
        elif active_positions:
            stale_entries = entries
        elif incoming_group_keys is not None:
            stale_entries = entries
        else:
            stale_entries = _stale_entries_without_position(
                entries,
                keep_instruction_id=keep_instruction_id,
            )

        for record in stale_entries:
            actions.append(_build_entry_cleanup_action(record, group_key))

    return tuple(actions), tuple(blockers), len(groups)


def _stale_entries_without_position(
    entries: list[InstructionRecord],
    *,
    keep_instruction_id: str | None,
) -> list[InstructionRecord]:
    if not entries:
        return []
    if keep_instruction_id is not None:
        return [record for record in entries if record.instruction_id != keep_instruction_id]
    if len(entries) <= 1:
        return []
    current_entry = max(entries, key=lambda record: (record.submit_at, record.id))
    return [record for record in entries if record.id != current_entry.id]


def _build_entry_cleanup_action(
    record: InstructionRecord,
    group_key: IntentGroupKey,
) -> IntentCleanupAction:
    if record.state == ExecutionState.ENTRY_PENDING.value:
        return IntentCleanupAction(
            instruction_id=record.instruction_id,
            group_key=group_key,
            state_before=record.state,
            state_after=ExecutionState.ENTRY_CANCELLED.value,
            action="cancel_pending_entry",
            reason="Older active entry no longer matches the current intent group.",
        )
    return IntentCleanupAction(
        instruction_id=record.instruction_id,
        group_key=group_key,
        state_before=record.state,
        state_after=ExecutionState.ENTRY_CANCELLED.value,
        action="cancel_submitted_entry",
        reason="Submitted entry must be cancelled before replacing this intent group.",
        broker_order_id=record.broker_order_id,
        broker_order_status=record.broker_order_status,
    )


def _cancel_pending_entry_for_intent_cleanup(
    session_factory: sessionmaker[Session],
    action: IntentCleanupAction,
    *,
    requested_at: datetime,
    requested_by: str,
    reason: str | None,
) -> IntentCleanupAction:
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == action.instruction_id)
            .with_for_update()
        ).scalar_one_or_none()
        if record is None:
            raise LookupError(
                f"Persisted instruction '{action.instruction_id}' was not found."
            )
        if record.state != ExecutionState.ENTRY_PENDING.value:
            raise ValueError(
                f"Instruction '{record.instruction_id}' is in state '{record.state}', "
                "expected ENTRY_PENDING."
            )
        previous_state = record.state
        record.state = ExecutionState.ENTRY_CANCELLED.value
        record.updated_at = requested_at
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type="intent_cleanup_entry_cancelled",
                source="intent_cleanup",
                event_at=requested_at,
                state_before=previous_state,
                state_after=record.state,
                payload={
                    "requested_by": requested_by,
                    "reason": reason,
                    "intent_group_key": action.group_key.as_string(),
                    "cleanup_action": action.action,
                },
                note=(
                    "Intent cleanup cancelled an older pending entry before "
                    "allowing the current intent group to proceed."
                ),
            )
        )
        return IntentCleanupAction(
            instruction_id=record.instruction_id,
            group_key=action.group_key,
            state_before=previous_state,
            state_after=record.state,
            action=action.action,
            reason=action.reason,
            broker_order_id=record.broker_order_id,
            broker_order_status=record.broker_order_status,
            applied=True,
        )


def _cancel_submitted_entry_for_intent_cleanup(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    action: IntentCleanupAction,
    *,
    requested_at: datetime,
    requested_by: str,
    reason: str | None,
    timeout: int,
    canceler: Callable[..., dict[str, Any]] | None,
) -> IntentCleanupAction:
    cancellation = cancel_persisted_instruction_entry(
        session_factory,
        broker_config,
        action.instruction_id,
        timeout=timeout,
        canceler=canceler,
    )
    cancellation_payload = serialize_persisted_broker_cancellation(cancellation)
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == action.instruction_id)
            .with_for_update()
        ).scalar_one_or_none()
        if record is not None:
            session.add(
                InstructionEventRecord(
                    instruction_id=record.id,
                    event_type="intent_cleanup_entry_cancelled",
                    source="intent_cleanup",
                    event_at=requested_at,
                    state_before=action.state_before,
                    state_after=record.state,
                    payload={
                        "requested_by": requested_by,
                        "reason": reason,
                        "intent_group_key": action.group_key.as_string(),
                        "cleanup_action": action.action,
                        "broker_cancellation": cancellation_payload,
                    },
                    note=(
                        "Intent cleanup cancelled an older submitted entry before "
                        "allowing the current intent group to proceed."
                    ),
                )
            )
    return IntentCleanupAction(
        instruction_id=cancellation.instruction_id,
        group_key=action.group_key,
        state_before=action.state_before,
        state_after=cancellation.state,
        action=action.action,
        reason=action.reason,
        broker_order_id=cancellation.broker_order_id,
        broker_order_status=cancellation.broker_order_status,
        applied=True,
    )


def _load_active_records(
    session_factory: sessionmaker[Session],
    *,
    account_key: str | None,
    book_key: str | None,
    book_side: str | None,
    symbol: str | None,
    exchange: str | None,
    currency: str | None,
    instruction_ids: tuple[str, ...] | None,
) -> tuple[InstructionRecord, ...]:
    with session_scope(session_factory) as session:
        statement = _active_record_statement()
        if account_key is not None:
            statement = statement.where(
                func.upper(InstructionRecord.account_key) == _normalize_upper(account_key)
            )
        if book_key is not None:
            statement = statement.where(
                func.lower(InstructionRecord.book_key) == _normalize_lower(book_key)
            )
        if symbol is not None:
            statement = statement.where(
                func.upper(InstructionRecord.symbol) == _normalize_upper(symbol)
            )
        if exchange is not None:
            statement = statement.where(
                func.upper(InstructionRecord.exchange) == _normalize_upper(exchange)
            )
        if currency is not None:
            statement = statement.where(
                func.upper(InstructionRecord.currency) == _normalize_upper(currency)
            )
        if instruction_ids is not None:
            statement = statement.where(InstructionRecord.instruction_id.in_(instruction_ids))
        records = tuple(session.execute(statement).scalars().all())

    if book_side is None:
        return records
    normalized_book_side = _normalize_upper(book_side)
    return tuple(
        record
        for record in records
        if intent_group_key_for_record(record).book_side == normalized_book_side
    )


def _load_active_records_for_group_keys(
    session_factory: sessionmaker[Session],
    group_keys: set[IntentGroupKey],
) -> tuple[InstructionRecord, ...]:
    if not group_keys:
        return ()
    with session_scope(session_factory) as session:
        statement = _active_record_statement()
        account_keys = sorted({group.account_key for group in group_keys})
        symbols = sorted({group.symbol for group in group_keys})
        statement = statement.where(
            func.upper(InstructionRecord.account_key).in_(account_keys),
            func.upper(InstructionRecord.symbol).in_(symbols),
        )
        records = tuple(session.execute(statement).scalars().all())
    return tuple(
        record
        for record in records
        if intent_group_key_for_record(record) in group_keys
    )


def _active_record_statement() -> Any:
    return (
        select(InstructionRecord)
        .where(
            InstructionRecord.archived_at.is_(None),
            InstructionRecord.state.in_(tuple(sorted(_INTENT_ACTIVE_STATES))),
            InstructionRecord.order_type != "MODEL_ROUTED",
        )
        .order_by(InstructionRecord.id.asc())
    )


def _parse_record_instruction(record: InstructionRecord) -> ExecutionInstruction | None:
    payload = record.payload or {}
    instruction_payload = payload.get("instruction")
    if not isinstance(instruction_payload, dict):
        return None
    try:
        return parse_execution_instruction_payload(instruction_payload)
    except Exception:
        return None


def _normalize_requested_by(value: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise IntentCleanupSelectorError("requested_by must be a non-empty string")
    return normalized


def _normalize_reason(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_upper(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_lower(value: Any) -> str:
    return str(value or "").strip().lower()


def _serialize_for_json(payload: Any) -> Any:
    if isinstance(payload, Enum):
        return payload.value
    if isinstance(payload, Decimal):
        return str(payload)
    if isinstance(payload, datetime):
        return payload.isoformat()
    if isinstance(payload, date):
        return payload.isoformat()
    if isinstance(payload, list):
        return [_serialize_for_json(item) for item in payload]
    if isinstance(payload, tuple):
        return [_serialize_for_json(item) for item in payload]
    if isinstance(payload, dict):
        return {key: _serialize_for_json(value) for key, value in payload.items()}
    return payload
