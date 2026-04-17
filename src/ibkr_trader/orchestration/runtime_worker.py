from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timezone
from decimal import Decimal
from decimal import InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any
from typing import Callable

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.config import AppConfig
from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.domain.execution_contract import OrderType
from ibkr_trader.domain.execution_payloads import parse_execution_instruction_payload
from ibkr_trader.ibkr.order_execution import cancel_broker_order
from ibkr_trader.ibkr.order_execution import submit_order_from_instruction
from ibkr_trader.ibkr.order_execution import submit_exit_order_from_instruction
from ibkr_trader.ibkr.runtime_snapshot import BrokerExecution
from ibkr_trader.ibkr.runtime_snapshot import BrokerRuntimeSnapshot
from ibkr_trader.ibkr.runtime_snapshot import fetch_broker_runtime_snapshot
from ibkr_trader.ibkr.session_manager import CanonicalSyncSessions
from ibkr_trader.orchestration.entry_submission import (
    cancel_persisted_instruction_entry,
    submit_persisted_instruction_entry,
)
from ibkr_trader.orchestration.scheduling import (
    NextSessionExitStatus,
    build_instruction_runtime_schedule,
)
from ibkr_trader.orchestration.state_machine import ExecutionState

DEFAULT_BROKER_RETRY_DELAYS: tuple[float, ...] = (1.0, 2.0)


@dataclass(slots=True)
class RuntimeCycleIssue:
    instruction_id: str | None
    stage: str
    message: str


@dataclass(slots=True)
class RuntimeCycleAction:
    instruction_id: str
    action: str
    state: str
    detail: dict[str, Any]


@dataclass(slots=True)
class RuntimeCycleResult:
    cycle_started_at: datetime
    cycle_completed_at: datetime
    runtime_timezone: str
    submitted_entries: tuple[RuntimeCycleAction, ...]
    cancelled_entries: tuple[RuntimeCycleAction, ...]
    filled_entries: tuple[RuntimeCycleAction, ...]
    submitted_exits: tuple[RuntimeCycleAction, ...]
    completed_instructions: tuple[RuntimeCycleAction, ...]
    issues: tuple[RuntimeCycleIssue, ...]


@dataclass(slots=True)
class ExecutionAggregate:
    quantity: Decimal = Decimal("0")
    average_price: Decimal | None = None
    executed_at: datetime | None = None
    execution_count: int = 0

    @property
    def has_fill(self) -> bool:
        return self.quantity > 0


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


def serialize_runtime_cycle_result(result: RuntimeCycleResult) -> dict[str, Any]:
    return _serialize_for_json(asdict(result))


def _complete_cycle_timestamp(cycle_started_at: datetime) -> datetime:
    completed_at = utc_now()
    if completed_at < cycle_started_at:
        return cycle_started_at
    return completed_at


def _parse_decimal(value: str | None) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid decimal payload value: {value}") from exc


def _ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


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


def _instruction_payload(record: InstructionRecord) -> ExecutionInstruction:
    raw_instruction_payload = record.payload.get("instruction")
    if not isinstance(raw_instruction_payload, dict):
        raise ValueError(
            f"Instruction '{record.instruction_id}' does not contain a valid persisted payload."
        )
    return parse_execution_instruction_payload(raw_instruction_payload)


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


def _fetch_instruction_ids(
    session_factory: sessionmaker[Session],
    *,
    states: tuple[str, ...],
    submit_before: datetime | None = None,
    instruction_ids: tuple[str, ...] | None = None,
) -> list[str]:
    with session_scope(session_factory) as session:
        query = select(InstructionRecord.instruction_id).where(
            InstructionRecord.state.in_(states)
        )
        if instruction_ids:
            query = query.where(InstructionRecord.instruction_id.in_(instruction_ids))
        if submit_before is not None:
            query = query.where(InstructionRecord.submit_at <= submit_before)
        return list(
            session.execute(
                query.order_by(InstructionRecord.submit_at, InstructionRecord.id)
            ).scalars()
        )


def _is_retryable_broker_error(exc: Exception) -> bool:
    if isinstance(exc, ConnectionError):
        return True
    message = str(exc).lower()
    return "[326]" in message or "client id is already in use" in message


def _run_with_broker_retries(
    operation: Callable[[], Any],
    *,
    retry_delays: tuple[float, ...],
    sleep_fn: Callable[[float], None],
) -> Any:
    attempts = len(retry_delays) + 1
    for attempt_index in range(attempts):
        try:
            return operation()
        except Exception as exc:
            is_last_attempt = attempt_index >= attempts - 1
            if is_last_attempt or not _is_retryable_broker_error(exc):
                raise
            sleep_fn(retry_delays[attempt_index])


def _aggregate_executions(
    executions: tuple[BrokerExecution, ...],
    *,
    order_id: int | None = None,
    order_ref_exact: str | None = None,
    order_ref_prefix: str | None = None,
) -> ExecutionAggregate:
    seen_exec_ids: set[str] = set()
    matched: list[BrokerExecution] = []
    for execution in executions:
        if order_id is not None and execution.order_id == order_id:
            pass
        elif order_ref_exact is not None and execution.order_ref == order_ref_exact:
            pass
        elif (
            order_ref_prefix is not None
            and execution.order_ref is not None
            and execution.order_ref.startswith(order_ref_prefix)
        ):
            pass
        else:
            continue

        dedupe_key = execution.exec_id or (
            f"{execution.order_id}:{execution.executed_at}:{execution.shares}:{execution.price}"
        )
        if dedupe_key in seen_exec_ids:
            continue
        seen_exec_ids.add(dedupe_key)
        matched.append(execution)

    if not matched:
        return ExecutionAggregate()

    total_quantity = Decimal("0")
    weighted_notional = Decimal("0")
    last_execution_at: datetime | None = None
    for execution in matched:
        shares = _parse_decimal(str(execution.shares) if execution.shares is not None else None)
        if shares <= 0:
            continue
        total_quantity += shares
        price = _parse_decimal(str(execution.price) if execution.price is not None else None)
        if price > 0:
            weighted_notional += price * shares
        if execution.executed_at is not None and (
            last_execution_at is None or execution.executed_at > last_execution_at
        ):
            last_execution_at = execution.executed_at

    average_price = None
    if total_quantity > 0 and weighted_notional > 0:
        average_price = weighted_notional / total_quantity

    return ExecutionAggregate(
        quantity=total_quantity,
        average_price=average_price,
        executed_at=_ensure_utc(last_execution_at),
        execution_count=len(matched),
    )


def _quantize_like(value: Decimal, reference: Decimal) -> Decimal:
    exponent = reference.as_tuple().exponent
    if exponent >= 0:
        return value.quantize(Decimal("1"))
    return value.quantize(Decimal("1").scaleb(exponent))


def _compute_take_profit_price(
    instruction: ExecutionInstruction,
    entry_average_price: Decimal,
) -> Decimal:
    take_profit_pct = instruction.exit.take_profit_pct
    if take_profit_pct is None:
        raise ValueError("take_profit_pct is required to compute a take-profit exit.")

    if instruction.intent.side == "BUY":
        raw_price = entry_average_price * (Decimal("1") + take_profit_pct)
    elif instruction.intent.side == "SELL":
        raw_price = entry_average_price * (Decimal("1") - take_profit_pct)
    else:
        raise ValueError(f"Unsupported instruction side: {instruction.intent.side}")

    if raw_price <= 0:
        raise ValueError("Computed take-profit limit price is not positive.")

    reference_price = instruction.entry.limit_price or entry_average_price
    return _quantize_like(raw_price, reference_price)


def _compute_stop_price(
    instruction: ExecutionInstruction,
    entry_average_price: Decimal,
    *,
    stop_loss_pct: Decimal,
) -> Decimal:
    if instruction.intent.side == "BUY":
        raw_price = entry_average_price * (Decimal("1") - stop_loss_pct)
    elif instruction.intent.side == "SELL":
        raw_price = entry_average_price * (Decimal("1") + stop_loss_pct)
    else:
        raise ValueError(f"Unsupported instruction side: {instruction.intent.side}")

    if raw_price <= 0:
        raise ValueError("Computed stop price is not positive.")

    reference_price = instruction.entry.limit_price or entry_average_price
    return _quantize_like(raw_price, reference_price)


def _open_order_ids_with_ref_prefix(
    snapshot: BrokerRuntimeSnapshot,
    *,
    order_ref_prefix: str,
) -> tuple[int, ...]:
    return tuple(
        order_id
        for order_id, open_order in snapshot.open_orders.items()
        if open_order.order_ref is not None
        and open_order.order_ref.startswith(order_ref_prefix)
    )


def _remaining_position_quantity(
    record: InstructionRecord,
    exit_fill: ExecutionAggregate,
) -> Decimal:
    entry_filled = _parse_decimal(record.entry_filled_quantity)
    if entry_filled <= 0:
        return Decimal("0")
    remaining = entry_filled - exit_fill.quantity
    return remaining if remaining > 0 else Decimal("0")


def _record_entry_fill_and_optional_exit(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    instruction_id: str,
    *,
    entry_fill: ExecutionAggregate,
    timeout: int,
    exit_submitter: Callable[..., dict[str, Any]] | None,
) -> tuple[RuntimeCycleAction, tuple[RuntimeCycleAction, ...]]:
    submitted_exits: list[RuntimeCycleAction] = []
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == instruction_id)
            .with_for_update()
        ).scalar_one()
        if record.state != ExecutionState.ENTRY_SUBMITTED.value:
            return (
                RuntimeCycleAction(
                    instruction_id=instruction_id,
                    action="entry_fill_already_reconciled",
                    state=record.state,
                    detail={},
                ),
                (),
            )

        instruction = _instruction_payload(record)
        previous_state = record.state
        record.entry_filled_quantity = str(entry_fill.quantity)
        record.entry_avg_fill_price = (
            str(entry_fill.average_price) if entry_fill.average_price is not None else None
        )
        record.entry_filled_at = entry_fill.executed_at
        record.state = ExecutionState.POSITION_OPEN.value

        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type="entry_order_filled",
                source="runtime_cycle",
                state_before=previous_state,
                state_after=record.state,
                payload=_serialize_for_json(
                    {
                        "fill": {
                            "quantity": entry_fill.quantity,
                            "average_price": entry_fill.average_price,
                            "executed_at": entry_fill.executed_at,
                            "execution_count": entry_fill.execution_count,
                        }
                    }
                ),
                note="Entry fill reconciled from IBKR executions.",
            )
        )

        entry_action = RuntimeCycleAction(
            instruction_id=instruction_id,
            action="entry_filled",
            state=record.state,
            detail={
                "entry_filled_quantity": str(entry_fill.quantity),
                "entry_avg_fill_price": (
                    str(entry_fill.average_price)
                    if entry_fill.average_price is not None
                    else None
                ),
                "entry_filled_at": entry_fill.executed_at,
            },
        )

        protective_exits: list[dict[str, Any]] = []
        if instruction.exit.take_profit_pct is not None:
            if entry_fill.average_price is None:
                raise ValueError(
                    f"Instruction '{instruction_id}' has fills but no average fill price."
                )
            protective_exits.append(
                {
                    "event_type": "take_profit_exit_submitted",
                    "action": "take_profit_exit_submitted",
                    "order_ref": f"{instruction_id}:exit:take_profit",
                    "order_type": OrderType.LIMIT,
                    "limit_price": _compute_take_profit_price(
                        instruction,
                        entry_fill.average_price,
                    ),
                    "stop_price": None,
                    "note": "Submitted take-profit exit order after entry fill.",
                }
            )

        if instruction.exit.stop_loss_pct is not None:
            if entry_fill.average_price is None:
                raise ValueError(
                    f"Instruction '{instruction_id}' has fills but no average fill price."
                )
            protective_exits.append(
                {
                    "event_type": "stop_loss_exit_submitted",
                    "action": "stop_loss_exit_submitted",
                    "order_ref": f"{instruction_id}:exit:stop_loss",
                    "order_type": "STOP",
                    "limit_price": None,
                    "stop_price": _compute_stop_price(
                        instruction,
                        entry_fill.average_price,
                        stop_loss_pct=instruction.exit.stop_loss_pct,
                    ),
                    "note": "Submitted stop-loss exit order after entry fill.",
                }
            )

        if instruction.exit.catastrophic_stop_loss_pct is not None:
            if entry_fill.average_price is None:
                raise ValueError(
                    f"Instruction '{instruction_id}' has fills but no average fill price."
                )
            protective_exits.append(
                {
                    "event_type": "catastrophic_stop_exit_submitted",
                    "action": "catastrophic_stop_exit_submitted",
                    "order_ref": f"{instruction_id}:exit:catastrophic_stop",
                    "order_type": "STOP",
                    "limit_price": None,
                    "stop_price": _compute_stop_price(
                        instruction,
                        entry_fill.average_price,
                        stop_loss_pct=instruction.exit.catastrophic_stop_loss_pct,
                    ),
                    "note": "Submitted catastrophic stop-loss exit order after entry fill.",
                }
            )

        if not protective_exits:
            return entry_action, ()

        runtime_exit_submitter = exit_submitter or submit_exit_order_from_instruction
        oca_group = (
            f"{instruction_id}:exit:oca"
            if len(protective_exits) > 1
            else None
        )

        for index, exit_spec in enumerate(protective_exits):
            broker_submission = runtime_exit_submitter(
                broker_config,
                instruction,
                quantity=entry_fill.quantity,
                order_type=exit_spec["order_type"],
                limit_price=exit_spec["limit_price"],
                stop_price=exit_spec["stop_price"],
                order_ref=exit_spec["order_ref"],
                oca_group=oca_group,
                oca_type=1 if oca_group is not None else None,
                timeout=timeout,
            )
            broker_status = broker_submission["broker_order_status"]
            if index == 0:
                record.exit_order_id = int(broker_status["orderId"])
                record.exit_perm_id = int(broker_status["permId"])
                record.exit_client_id = int(broker_status["clientId"])
                record.exit_order_status = str(broker_status["status"])
                record.exit_submitted_quantity = str(entry_fill.quantity)

            state_before_exit = record.state
            record.state = ExecutionState.EXIT_PENDING.value
            session.add(
                InstructionEventRecord(
                    instruction_id=record.id,
                    event_type=exit_spec["event_type"],
                    source="runtime_cycle",
                    state_before=state_before_exit,
                    state_after=record.state,
                    payload={"broker_submission": broker_submission},
                    note=exit_spec["note"],
                )
            )
            submitted_exits.append(
                RuntimeCycleAction(
                    instruction_id=instruction_id,
                    action=exit_spec["action"],
                    state=record.state,
                    detail={
                        "broker_order_id": int(broker_status["orderId"]),
                        "broker_order_status": str(broker_status["status"]),
                        "exit_submitted_quantity": str(entry_fill.quantity),
                        "limit_price": (
                            str(exit_spec["limit_price"])
                            if exit_spec["limit_price"] is not None
                            else None
                        ),
                        "stop_price": (
                            str(exit_spec["stop_price"])
                            if exit_spec["stop_price"] is not None
                            else None
                        ),
                        "oca_group": oca_group,
                    },
                )
            )

        return entry_action, tuple(submitted_exits)


def _mark_unfilled_entry_cancelled(
    session_factory: sessionmaker[Session],
    instruction_id: str,
    *,
    note: str,
) -> RuntimeCycleAction:
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == instruction_id)
            .with_for_update()
        ).scalar_one()
        if record.state != ExecutionState.ENTRY_SUBMITTED.value:
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
                event_type="entry_order_expired_without_fill",
                source="runtime_cycle",
                state_before=previous_state,
                state_after=record.state,
                payload={},
                note=note,
            )
        )
        return RuntimeCycleAction(
            instruction_id=instruction_id,
            action="entry_cancelled_without_fill",
            state=record.state,
            detail={"note": note},
        )


def _submit_forced_exit(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    instruction_id: str,
    *,
    quantity: Decimal,
    timeout: int,
    exit_submitter: Callable[..., dict[str, Any]] | None,
) -> RuntimeCycleAction:
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == instruction_id)
            .with_for_update()
        ).scalar_one()
        instruction = _instruction_payload(record)
        runtime_exit_submitter = exit_submitter or submit_exit_order_from_instruction
        broker_submission = runtime_exit_submitter(
            broker_config,
            instruction,
            quantity=quantity,
            order_type=OrderType.MARKET,
            order_ref=f"{instruction_id}:exit:forced",
            timeout=timeout,
        )
        broker_status = broker_submission["broker_order_status"]
        previous_state = record.state
        record.exit_order_id = int(broker_status["orderId"])
        record.exit_perm_id = int(broker_status["permId"])
        record.exit_client_id = int(broker_status["clientId"])
        record.exit_order_status = str(broker_status["status"])
        record.exit_submitted_quantity = str(quantity)
        record.state = ExecutionState.EXIT_PENDING.value
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type="forced_exit_submitted",
                source="runtime_cycle",
                state_before=previous_state,
                state_after=record.state,
                payload={"broker_submission": broker_submission},
                note="Submitted forced market exit at the next session open.",
            )
        )
        return RuntimeCycleAction(
            instruction_id=instruction_id,
            action="forced_exit_submitted",
            state=record.state,
            detail={
                "broker_order_id": record.exit_order_id,
                "broker_order_status": record.exit_order_status,
                "exit_submitted_quantity": record.exit_submitted_quantity,
            },
        )


def _record_exit_fill_and_complete(
    session_factory: sessionmaker[Session],
    instruction_id: str,
    *,
    exit_fill: ExecutionAggregate,
) -> RuntimeCycleAction:
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == instruction_id)
            .with_for_update()
        ).scalar_one()
        previous_state = record.state
        record.exit_filled_quantity = str(exit_fill.quantity)
        record.exit_avg_fill_price = (
            str(exit_fill.average_price) if exit_fill.average_price is not None else None
        )
        record.exit_filled_at = exit_fill.executed_at
        record.state = ExecutionState.COMPLETED.value
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type="exit_order_filled",
                source="runtime_cycle",
                state_before=previous_state,
                state_after=record.state,
                payload=_serialize_for_json(
                    {
                        "fill": {
                            "quantity": exit_fill.quantity,
                            "average_price": exit_fill.average_price,
                            "executed_at": exit_fill.executed_at,
                            "execution_count": exit_fill.execution_count,
                        }
                    }
                ),
                note="Exit fill reconciled from IBKR executions; instruction completed.",
            )
        )
        return RuntimeCycleAction(
            instruction_id=instruction_id,
            action="instruction_completed",
            state=record.state,
            detail={
                "exit_filled_quantity": str(exit_fill.quantity),
                "exit_avg_fill_price": (
                    str(exit_fill.average_price)
                    if exit_fill.average_price is not None
                    else None
                ),
                "exit_filled_at": exit_fill.executed_at,
            },
        )


def _is_next_session_exit_due(
    instruction: ExecutionInstruction,
    *,
    runtime_timezone: str,
    session_calendar_path: Path,
    cycle_at: datetime,
) -> bool:
    schedule = build_instruction_runtime_schedule(
        instruction,
        runtime_timezone=runtime_timezone,
        session_calendar_path=session_calendar_path,
    )
    preview = schedule.next_session_exit
    if (
        not preview.requested
        or preview.status is not NextSessionExitStatus.RESOLVED
        or preview.next_session_open_utc is None
    ):
        return False
    return preview.next_session_open_utc <= cycle_at.astimezone(timezone.utc)


def run_runtime_cycle(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    *,
    runtime_timezone: str,
    session_calendar_path: Path,
    now: datetime | None = None,
    timeout: int = 10,
    instruction_ids: tuple[str, ...] | None = None,
    entry_submitter: Callable[..., Any] | None = None,
    entry_canceler: Callable[..., Any] | None = None,
    exit_submitter: Callable[..., dict[str, Any]] | None = None,
    broker_snapshot_fetcher: Callable[..., BrokerRuntimeSnapshot] | None = None,
    broker_order_canceler: Callable[..., dict[str, Any]] | None = None,
    broker_retry_delays: tuple[float, ...] = DEFAULT_BROKER_RETRY_DELAYS,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> RuntimeCycleResult:
    cycle_started_at = now.astimezone(timezone.utc) if now is not None else utc_now()
    runtime_snapshot_fetch = broker_snapshot_fetcher or fetch_broker_runtime_snapshot
    runtime_order_canceler = broker_order_canceler or cancel_broker_order
    submitted_entries: list[RuntimeCycleAction] = []
    cancelled_entries: list[RuntimeCycleAction] = []
    filled_entries: list[RuntimeCycleAction] = []
    submitted_exits: list[RuntimeCycleAction] = []
    completed_instructions: list[RuntimeCycleAction] = []
    issues: list[RuntimeCycleIssue] = []

    due_instruction_ids = _fetch_instruction_ids(
        session_factory,
        states=(ExecutionState.ENTRY_PENDING.value,),
        submit_before=cycle_started_at,
        instruction_ids=instruction_ids,
    )
    for instruction_id in due_instruction_ids:
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
            _record_runtime_note(
                session_factory,
                instruction_id=instruction_id,
                event_type="runtime_entry_submit_failed",
                note="Runtime cycle could not submit the due entry order.",
                payload={"error": str(exc)},
            )

    active_instruction_ids = _fetch_instruction_ids(
        session_factory,
        states=(
            ExecutionState.ENTRY_SUBMITTED.value,
            ExecutionState.POSITION_OPEN.value,
            ExecutionState.EXIT_PENDING.value,
        ),
        instruction_ids=instruction_ids,
    )
    if not active_instruction_ids:
        cycle_completed_at = _complete_cycle_timestamp(cycle_started_at)
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

    try:
        snapshot = _run_with_broker_retries(
            lambda: runtime_snapshot_fetch(
                broker_config,
                timeout=timeout,
            ),
            retry_delays=broker_retry_delays,
            sleep_fn=sleep_fn,
        )
    except Exception as exc:  # pragma: no cover - broad by design for runtime safety
        _append_issue(
            issues,
            instruction_id=None,
            stage="broker_snapshot",
            message=str(exc),
        )
        cycle_completed_at = _complete_cycle_timestamp(cycle_started_at)
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

    with session_scope(session_factory) as session:
        records = session.execute(
            select(InstructionRecord).where(
                InstructionRecord.instruction_id.in_(active_instruction_ids)
            )
        ).scalars().all()

    records_by_instruction_id = {
        record.instruction_id: record for record in records
    }

    for instruction_id in active_instruction_ids:
        record = records_by_instruction_id.get(instruction_id)
        if record is None:
            continue

        try:
            instruction = _instruction_payload(record)
            entry_fill = _aggregate_executions(
                snapshot.executions,
                order_id=record.broker_order_id,
                order_ref_exact=instruction.instruction_id,
            )
            exit_fill = _aggregate_executions(
                snapshot.executions,
                order_ref_prefix=f"{instruction.instruction_id}:exit:",
            )

            entry_open = (
                record.broker_order_id is not None
                and record.broker_order_id in snapshot.open_orders
            )
            exit_open_order_ids = _open_order_ids_with_ref_prefix(
                snapshot,
                order_ref_prefix=f"{instruction.instruction_id}:exit:",
            )
            exit_open = bool(exit_open_order_ids)
            expire_at = _ensure_utc(record.expire_at) or record.expire_at

            if record.state == ExecutionState.ENTRY_SUBMITTED.value:
                if entry_fill.has_fill:
                    if entry_open and cycle_started_at < expire_at:
                        continue
                    if entry_open and instruction.entry.cancel_unfilled_at_expiry:
                        _run_with_broker_retries(
                            lambda: runtime_order_canceler(
                                broker_config,
                                record.broker_order_id,
                                timeout=timeout,
                            ),
                            retry_delays=broker_retry_delays,
                            sleep_fn=sleep_fn,
                        )
                    entry_action, exit_actions = _run_with_broker_retries(
                        lambda: _record_entry_fill_and_optional_exit(
                            session_factory,
                            broker_config,
                            instruction_id,
                            entry_fill=entry_fill,
                            timeout=timeout,
                            exit_submitter=exit_submitter,
                        ),
                        retry_delays=broker_retry_delays,
                        sleep_fn=sleep_fn,
                    )
                    filled_entries.append(entry_action)
                    submitted_exits.extend(exit_actions)
                    continue

                if (
                    instruction.entry.cancel_unfilled_at_expiry
                    and cycle_started_at >= expire_at
                ):
                    if entry_open:
                        cancellation = _run_with_broker_retries(
                            lambda: cancel_persisted_instruction_entry(
                                session_factory,
                                broker_config,
                                instruction_id,
                                timeout=timeout,
                                canceler=entry_canceler,
                            ),
                            retry_delays=broker_retry_delays,
                            sleep_fn=sleep_fn,
                        )
                        cancelled_entries.append(
                            RuntimeCycleAction(
                                instruction_id=instruction_id,
                                action="entry_cancelled_at_expiry",
                                state=cancellation.state,
                                detail={
                                    "broker_order_id": cancellation.broker_order_id,
                                    "broker_order_status": cancellation.broker_order_status,
                                },
                            )
                        )
                    else:
                        cancelled_entries.append(
                            _mark_unfilled_entry_cancelled(
                                session_factory,
                                instruction_id,
                                note=(
                                    "Entry window expired without fills and no open broker "
                                    "entry order remained."
                                ),
                            )
                        )
                continue

            remaining_quantity = _remaining_position_quantity(record, exit_fill)
            if record.state in {
                ExecutionState.POSITION_OPEN.value,
                ExecutionState.EXIT_PENDING.value,
            }:
                if exit_fill.has_fill and exit_fill.quantity > 0:
                    with session_scope(session_factory) as session:
                        writable_record = session.execute(
                            select(InstructionRecord)
                            .where(InstructionRecord.instruction_id == instruction_id)
                            .with_for_update()
                        ).scalar_one()
                        writable_record.exit_filled_quantity = str(exit_fill.quantity)
                        writable_record.exit_avg_fill_price = (
                            str(exit_fill.average_price)
                            if exit_fill.average_price is not None
                            else None
                        )
                        writable_record.exit_filled_at = exit_fill.executed_at

                if remaining_quantity <= 0 and not exit_open:
                    completed_instructions.append(
                        _record_exit_fill_and_complete(
                            session_factory,
                            instruction_id,
                            exit_fill=exit_fill,
                        )
                    )
                    continue

                if not _is_next_session_exit_due(
                    instruction,
                    runtime_timezone=runtime_timezone,
                    session_calendar_path=session_calendar_path,
                    cycle_at=cycle_started_at,
                ):
                    continue

                if remaining_quantity <= 0:
                    continue

                for open_exit_order_id in exit_open_order_ids:
                    _run_with_broker_retries(
                        lambda order_id=open_exit_order_id: runtime_order_canceler(
                            broker_config,
                            order_id,
                            timeout=timeout,
                        ),
                        retry_delays=broker_retry_delays,
                        sleep_fn=sleep_fn,
                    )
                submitted_exits.append(
                    _run_with_broker_retries(
                        lambda: _submit_forced_exit(
                            session_factory,
                            broker_config,
                            instruction_id,
                            quantity=remaining_quantity,
                            timeout=timeout,
                            exit_submitter=exit_submitter,
                        ),
                        retry_delays=broker_retry_delays,
                        sleep_fn=sleep_fn,
                    )
                )

        except Exception as exc:  # pragma: no cover - broad by design for runtime safety
            _append_issue(
                issues,
                instruction_id=instruction_id,
                stage="reconcile_instruction",
                message=str(exc),
            )
            _record_runtime_note(
                session_factory,
                instruction_id=instruction_id,
                event_type="runtime_reconcile_failed",
                note="Runtime cycle could not reconcile the instruction cleanly.",
                payload={"error": str(exc)},
            )

    cycle_completed_at = _complete_cycle_timestamp(cycle_started_at)
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the IBKR Trader MVP runtime loop."
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=5.0,
        help="Seconds to sleep between runtime cycles.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run exactly one runtime cycle and exit.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Broker request timeout in seconds.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    app_config = AppConfig.from_env()
    session_factory = create_session_factory(build_engine(app_config.database_url))
    broker_sessions = CanonicalSyncSessions(app_config.ibkr)

    def submit_entry_with_primary(
        broker_config: IbkrConnectionConfig,
        instruction: ExecutionInstruction,
        *,
        timeout: int = 10,
    ) -> dict[str, Any]:
        return broker_sessions.primary.execute(
            "runtime_entry_submit",
            lambda broker_app: submit_order_from_instruction(
                broker_config,
                instruction,
                timeout=timeout,
                app=broker_app,
            ),
        )

    def submit_exit_with_primary(
        broker_config: IbkrConnectionConfig,
        instruction: ExecutionInstruction,
        *,
        quantity: Decimal,
        order_type: OrderType | str,
        order_ref: str,
        timeout: int = 10,
        limit_price: Decimal | None = None,
        stop_price: Decimal | None = None,
        oca_group: str | None = None,
        oca_type: int | None = None,
    ) -> dict[str, Any]:
        return broker_sessions.primary.execute(
            "runtime_exit_submit",
            lambda broker_app: submit_exit_order_from_instruction(
                broker_config,
                instruction,
                quantity=quantity,
                order_type=order_type,
                order_ref=order_ref,
                timeout=timeout,
                limit_price=limit_price,
                stop_price=stop_price,
                oca_group=oca_group,
                oca_type=oca_type,
                app=broker_app,
            ),
        )

    def fetch_snapshot_with_primary(
        broker_config: IbkrConnectionConfig,
        *,
        timeout: int = 10,
    ) -> BrokerRuntimeSnapshot:
        return broker_sessions.primary.execute(
            "runtime_snapshot",
            lambda broker_app: fetch_broker_runtime_snapshot(
                broker_config,
                timeout=timeout,
                app=broker_app,
            ),
        )

    def cancel_order_with_primary(
        broker_config: IbkrConnectionConfig,
        order_id: int,
        *,
        timeout: int = 10,
    ) -> dict[str, Any]:
        return broker_sessions.primary.execute(
            "runtime_cancel",
            lambda broker_app: cancel_broker_order(
                broker_config,
                order_id,
                timeout=timeout,
                app=broker_app,
            ),
        )

    broker_sessions.warmup()
    try:
        while True:
            result = run_runtime_cycle(
                session_factory,
                app_config.ibkr.primary_session(),
                runtime_timezone=app_config.timezone,
                session_calendar_path=app_config.session_calendar_path,
                timeout=args.timeout,
                entry_submitter=submit_entry_with_primary,
                exit_submitter=submit_exit_with_primary,
                broker_snapshot_fetcher=fetch_snapshot_with_primary,
                broker_order_canceler=cancel_order_with_primary,
            )
            print(json.dumps(serialize_runtime_cycle_result(result), indent=2))
            if args.once:
                break
            time.sleep(args.interval_seconds)
    finally:
        broker_sessions.shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
