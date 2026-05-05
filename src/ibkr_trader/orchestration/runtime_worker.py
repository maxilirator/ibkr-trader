from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import sys
import time
import uuid
from dataclasses import asdict
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from decimal import InvalidOperation
from enum import Enum
from pathlib import Path
from threading import Event
from threading import Thread
from typing import Any
from typing import Callable

from sqlalchemy import and_
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.config import AppConfig
from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import ExecutionFillRecord
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import ReconciliationIssueRecord
from ibkr_trader.db.models import ReconciliationRunRecord
from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.domain.execution_contract import OrderType
from ibkr_trader.domain.execution_payloads import parse_execution_instruction_payload
from ibkr_trader.ibkr.historical_bars import read_latest_trade_price
from ibkr_trader.ibkr.order_execution import cancel_broker_order
from ibkr_trader.ibkr.order_execution import submit_order_from_instruction
from ibkr_trader.ibkr.order_execution import submit_exit_order_from_instruction
from ibkr_trader.ibkr.runtime_snapshot import BrokerExecution
from ibkr_trader.ibkr.runtime_snapshot import BrokerRuntimeSnapshot
from ibkr_trader.ibkr.runtime_snapshot import fetch_broker_runtime_snapshot
from ibkr_trader.ibkr.session_manager import CanonicalSyncSessions
from ibkr_trader.ledger.persistence import BROKER_KIND_IBKR
from ibkr_trader.ledger.persistence import persist_broker_callback_events
from ibkr_trader.ledger.persistence import persist_broker_order_cancellation_result
from ibkr_trader.ledger.persistence import persist_broker_order_submission
from ibkr_trader.ledger.persistence import persist_broker_runtime_snapshot
from ibkr_trader.orchestration.entry_submission import (
    cancel_persisted_instruction_entry,
    submit_persisted_instruction_entry,
)
from ibkr_trader.orchestration.operator_controls import read_kill_switch_state
from ibkr_trader.orchestration.scheduling import (
    NextSessionExitStatus,
    build_instruction_runtime_schedule,
    resolve_effective_entry_expire_at_for_schedule,
    resolve_scheduled_submission_due_at,
)
from ibkr_trader.orchestration.runtime_service_state import (
    EXECUTION_RUNTIME_KEY,
    RuntimeServiceLeaseError,
    acquire_runtime_service_lease,
    mark_runtime_service_failed,
    mark_runtime_service_startup_blocked,
    mark_runtime_service_stopped,
    read_runtime_service_status,
    record_runtime_cycle_completed,
    record_runtime_cycle_started,
    serialize_runtime_service_status,
)
from ibkr_trader.orchestration.state_machine import ExecutionState
from ibkr_trader.virtual.accounts import is_virtual_account_key
from ibkr_trader.virtual.execution import cancel_virtual_order
from ibkr_trader.virtual.execution import has_real_broker_work
from ibkr_trader.virtual.execution import read_virtual_market_price
from ibkr_trader.virtual.execution import submit_virtual_exit_order

DEFAULT_BROKER_RETRY_DELAYS: tuple[float, ...] = (1.0, 2.0)
DEFAULT_SUBMISSION_LEAD_TIME = timedelta(seconds=60)
PROTECTIVE_EXIT_RETRY_COOLDOWN = timedelta(minutes=15)
_CLOSED_BROKER_ORDER_STATUSES = {
    "API_CANCELLED",
    "CANCELLED",
    "ERROR",
    "FILLED",
    "INACTIVE",
    "NOT_FOUND_AT_BROKER",
    "REJECTED",
}


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


def _emit_runtime_cycle_result(result: RuntimeCycleResult) -> None:
    print(json.dumps(serialize_runtime_cycle_result(result), indent=2))


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


def _persist_drained_broker_callbacks(
    session_factory: sessionmaker[Session],
    *,
    broker_config: IbkrConnectionConfig,
    callback_events: list[dict[str, Any]],
) -> None:
    if not callback_events:
        return
    persist_broker_callback_events(
        session_factory,
        callback_events,
        broker_kind=BROKER_KIND_IBKR,
        default_account_key=broker_config.account_id,
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


def _protective_exit_oca_group(instruction_id: str) -> str:
    digest = hashlib.blake2s(
        instruction_id.encode("utf-8"),
        digest_size=8,
    ).hexdigest().upper()
    return f"OCA{digest}"


def _build_protective_exit_specs(
    *,
    instruction_id: str,
    instruction: ExecutionInstruction,
    entry_average_price: Decimal | None,
) -> list[dict[str, Any]]:
    if (
        instruction.exit.take_profit_pct is not None
        or instruction.exit.stop_loss_pct is not None
        or instruction.exit.catastrophic_stop_loss_pct is not None
    ) and entry_average_price is None:
        raise ValueError(
            f"Instruction '{instruction_id}' has fills but no average fill price."
        )

    protective_exits: list[dict[str, Any]] = []
    if instruction.exit.stop_loss_pct is not None:
        protective_exits.append(
            {
                "event_type": "stop_loss_exit_submitted",
                "action": "stop_loss_exit_submitted",
                "order_ref": f"{instruction_id}:exit:stop_loss",
                "order_type": "STOP",
                "limit_price": None,
                "stop_price": _compute_stop_price(
                    instruction,
                    entry_average_price,
                    stop_loss_pct=instruction.exit.stop_loss_pct,
                ),
                "note": "Submitted stop-loss exit order after entry fill.",
                "protective_role": "stop_loss",
            }
        )

    if instruction.exit.catastrophic_stop_loss_pct is not None:
        protective_exits.append(
            {
                "event_type": "catastrophic_stop_exit_submitted",
                "action": "catastrophic_stop_exit_submitted",
                "order_ref": f"{instruction_id}:exit:catastrophic_stop",
                "order_type": "STOP",
                "limit_price": None,
                "stop_price": _compute_stop_price(
                    instruction,
                    entry_average_price,
                    stop_loss_pct=instruction.exit.catastrophic_stop_loss_pct,
                ),
                "note": "Submitted catastrophic stop-loss exit order after entry fill.",
                "protective_role": "catastrophic_stop",
            }
        )

    if instruction.exit.take_profit_pct is not None:
        protective_exits.append(
            {
                "event_type": "take_profit_exit_submitted",
                "action": "take_profit_exit_submitted",
                "order_ref": f"{instruction_id}:exit:take_profit",
                "order_type": OrderType.LIMIT,
                "limit_price": _compute_take_profit_price(
                    instruction,
                    entry_average_price,
                ),
                "stop_price": None,
                "note": "Submitted take-profit exit order after entry fill.",
                "protective_role": "take_profit",
            }
        )

    return protective_exits


def _runtime_exit_submitter(
    session_factory: sessionmaker[Session],
    instruction: ExecutionInstruction,
    exit_submitter: Callable[..., dict[str, Any]] | None,
) -> Callable[..., dict[str, Any]]:
    if exit_submitter is not None:
        return exit_submitter
    if is_virtual_account_key(instruction.account.account_key):
        def _submit_virtual_exit(
            broker_config: IbkrConnectionConfig,
            runtime_instruction: ExecutionInstruction,
            **kwargs: Any,
        ) -> dict[str, Any]:
            return submit_virtual_exit_order(
                session_factory,
                broker_config,
                runtime_instruction,
                **kwargs,
            )

        return _submit_virtual_exit
    return submit_exit_order_from_instruction


def _record_protective_exit_submit_failed(
    session_factory: sessionmaker[Session],
    *,
    instruction_id: str,
    error: Exception,
    exit_spec: dict[str, Any],
    oca_group: str | None,
    oca_type: int | None,
    fallback_without_oca: bool = False,
) -> None:
    _record_runtime_note(
        session_factory,
        instruction_id=instruction_id,
        event_type="protective_exit_submit_failed",
        note=(
            "Entry fill was reconciled, but the protective exit order "
            "could not be submitted."
        ),
        payload={
            "error": str(error),
            "fallback_without_oca": fallback_without_oca,
            "exit_spec": {
                "order_ref": exit_spec["order_ref"],
                "order_type": exit_spec["order_type"],
                "limit_price": exit_spec["limit_price"],
                "stop_price": exit_spec["stop_price"],
                "oca_group": oca_group,
                "oca_type": oca_type,
            },
        },
    )


def _persist_protective_exit_submission(
    session_factory: sessionmaker[Session],
    *,
    instruction_id: str,
    entry_quantity: Decimal,
    exit_spec: dict[str, Any],
    broker_submission: dict[str, Any],
    broker_config: IbkrConnectionConfig,
    fallback_without_oca: bool = False,
) -> RuntimeCycleAction:
    broker_status = broker_submission["broker_order_status"]
    broker_kind = str(broker_submission.get("broker_kind") or BROKER_KIND_IBKR)
    fallback_account_key = (
        str(broker_submission["account"])
        if broker_submission.get("account") not in (None, "")
        else broker_config.account_id
    )
    event_type = str(exit_spec["event_type"])
    action = str(exit_spec["action"])
    note = str(exit_spec["note"])
    if fallback_without_oca:
        event_type = f"{event_type}_without_oca_fallback"
        action = f"{action}_without_oca_fallback"
        note = f"{note} OCA grouping was rejected, so only this stop was submitted."

    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == instruction_id)
            .with_for_update()
        ).scalar_one()
        if record.exit_order_id is None:
            record.exit_order_id = int(broker_status["orderId"])
            record.exit_perm_id = int(broker_status["permId"])
            record.exit_client_id = int(broker_status["clientId"])
            record.exit_order_status = str(broker_status["status"])
            record.exit_submitted_quantity = str(entry_quantity)

        event_at = utc_now()
        persist_broker_order_submission(
            session,
            broker_kind=broker_kind,
            instruction_record=record,
            broker_submission=broker_submission,
            observed_at=event_at,
            fallback_account_key=fallback_account_key,
            order_role="EXIT",
            event_type=event_type,
            note=note,
        )
        state_before_exit = record.state
        record.state = ExecutionState.EXIT_PENDING.value
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type=event_type,
                source="runtime_cycle",
                event_at=event_at,
                state_before=state_before_exit,
                state_after=record.state,
                payload={"broker_submission": broker_submission},
                note=note,
            )
        )
    submitted_order = broker_submission.get("order", {})
    submitted_oca_group = (
        submitted_order.get("oca_group") if isinstance(submitted_order, dict) else None
    )
    return RuntimeCycleAction(
        instruction_id=instruction_id,
        action=action,
        state=ExecutionState.EXIT_PENDING.value,
        detail={
            "broker_order_id": int(broker_status["orderId"]),
            "broker_order_status": str(broker_status["status"]),
            "exit_submitted_quantity": str(entry_quantity),
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
            "oca_group": submitted_oca_group,
            "fallback_without_oca": fallback_without_oca,
        },
    )


def _submit_protective_exits(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    *,
    instruction_id: str,
    instruction: ExecutionInstruction,
    entry_quantity: Decimal,
    protective_exits: list[dict[str, Any]],
    timeout: int,
    exit_submitter: Callable[..., dict[str, Any]] | None,
) -> tuple[RuntimeCycleAction, ...]:
    if not protective_exits:
        return ()

    runtime_submitter = _runtime_exit_submitter(
        session_factory,
        instruction,
        exit_submitter,
    )
    oca_group = (
        _protective_exit_oca_group(instruction_id)
        if len(protective_exits) > 1
        else None
    )
    oca_type = 1 if oca_group is not None else None
    submitted_exits: list[RuntimeCycleAction] = []

    for exit_spec in protective_exits:
        try:
            broker_submission = runtime_submitter(
                broker_config,
                instruction,
                quantity=entry_quantity,
                order_type=exit_spec["order_type"],
                limit_price=exit_spec["limit_price"],
                stop_price=exit_spec["stop_price"],
                order_ref=exit_spec["order_ref"],
                oca_group=oca_group,
                oca_type=oca_type,
                timeout=timeout,
            )
        except Exception as exc:  # pragma: no cover - broker safety path
            _record_protective_exit_submit_failed(
                session_factory,
                instruction_id=instruction_id,
                error=exc,
                exit_spec=exit_spec,
                oca_group=oca_group,
                oca_type=oca_type,
            )
            if exit_spec["stop_price"] is None or oca_group is None:
                continue
            try:
                fallback_submission = runtime_submitter(
                    broker_config,
                    instruction,
                    quantity=entry_quantity,
                    order_type=exit_spec["order_type"],
                    limit_price=exit_spec["limit_price"],
                    stop_price=exit_spec["stop_price"],
                    order_ref=exit_spec["order_ref"],
                    oca_group=None,
                    oca_type=None,
                    timeout=timeout,
                )
            except Exception as fallback_exc:  # pragma: no cover - broker safety path
                _record_protective_exit_submit_failed(
                    session_factory,
                    instruction_id=instruction_id,
                    error=fallback_exc,
                    exit_spec=exit_spec,
                    oca_group=None,
                    oca_type=None,
                    fallback_without_oca=True,
                )
                continue

            submitted_exits.append(
                _persist_protective_exit_submission(
                    session_factory,
                    instruction_id=instruction_id,
                    entry_quantity=entry_quantity,
                    exit_spec=exit_spec,
                    broker_submission=fallback_submission,
                    broker_config=broker_config,
                    fallback_without_oca=True,
                )
            )
            break

        submitted_exits.append(
            _persist_protective_exit_submission(
                session_factory,
                instruction_id=instruction_id,
                entry_quantity=entry_quantity,
                exit_spec=exit_spec,
                broker_submission=broker_submission,
                broker_config=broker_config,
            )
        )

    return tuple(submitted_exits)


def _protective_exit_retry_on_cooldown(
    session_factory: sessionmaker[Session],
    *,
    instruction_id: str,
    oca_group: str | None,
    cycle_at: datetime,
) -> bool:
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord.id).where(
                InstructionRecord.instruction_id == instruction_id
            )
        ).scalar_one_or_none()
        if record is None:
            return False
        events = list(
            session.execute(
                select(InstructionEventRecord)
                .where(
                    InstructionEventRecord.instruction_id == record,
                    InstructionEventRecord.event_type == "protective_exit_submit_failed",
                )
                .order_by(InstructionEventRecord.event_at.desc())
            ).scalars()
        )

    for event in events:
        payload = event.payload or {}
        exit_spec = payload.get("exit_spec")
        if not isinstance(exit_spec, dict):
            continue
        if exit_spec.get("oca_group") != oca_group:
            continue
        event_at = _ensure_utc(event.event_at)
        if event_at is None:
            return True
        return cycle_at.astimezone(timezone.utc) - event_at < PROTECTIVE_EXIT_RETRY_COOLDOWN
    return False


def _submit_missing_protective_exits(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    instruction_id: str,
    *,
    quantity: Decimal,
    cycle_at: datetime,
    timeout: int,
    exit_submitter: Callable[..., dict[str, Any]] | None,
) -> tuple[RuntimeCycleAction, ...]:
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == instruction_id)
            .with_for_update()
        ).scalar_one()
        if record.state != ExecutionState.POSITION_OPEN.value:
            return ()
        instruction = _instruction_payload(record)
        entry_average_price = _parse_decimal(record.entry_avg_fill_price)
        if entry_average_price <= 0:
            return ()
        protective_exits = _build_protective_exit_specs(
            instruction_id=instruction_id,
            instruction=instruction,
            entry_average_price=entry_average_price,
        )

    if not protective_exits:
        return ()

    oca_group = (
        _protective_exit_oca_group(instruction_id)
        if len(protective_exits) > 1
        else None
    )
    if _protective_exit_retry_on_cooldown(
        session_factory,
        instruction_id=instruction_id,
        oca_group=oca_group,
        cycle_at=cycle_at,
    ):
        return ()

    return _submit_protective_exits(
        session_factory,
        broker_config,
        instruction_id=instruction_id,
        instruction=instruction,
        entry_quantity=quantity,
        protective_exits=protective_exits,
        timeout=timeout,
        exit_submitter=exit_submitter,
    )


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
        "submitted_entries": [_serialize_for_json(asdict(action)) for action in submitted_entries],
        "cancelled_entries": [_serialize_for_json(asdict(action)) for action in cancelled_entries],
        "filled_entries": [_serialize_for_json(asdict(action)) for action in filled_entries],
        "submitted_exits": [_serialize_for_json(asdict(action)) for action in submitted_exits],
        "completed_instructions": [
            _serialize_for_json(asdict(action)) for action in completed_instructions
        ],
    }
    action_count = sum(len(entries) for entries in action_payload.values())

    with session_scope(session_factory) as session:
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
            metadata_json=_serialize_for_json(
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


def _finalize_runtime_cycle_result(
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
        _append_issue(
            issues,
            instruction_id=None,
            stage="runtime_cycle_audit",
            message=str(exc),
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


def _fetch_instruction_account_keys(
    session_factory: sessionmaker[Session],
    instruction_ids: list[str],
) -> dict[str, str]:
    if not instruction_ids:
        return {}
    with session_scope(session_factory) as session:
        rows = session.execute(
            select(InstructionRecord.instruction_id, InstructionRecord.account_key).where(
                InstructionRecord.instruction_id.in_(instruction_ids)
            )
        ).all()
    return {instruction_id: account_key for instruction_id, account_key in rows}


def _split_instruction_ids_by_virtual(
    session_factory: sessionmaker[Session],
    instruction_ids: list[str],
) -> tuple[list[str], list[str]]:
    if not instruction_ids:
        return [], []
    with session_scope(session_factory) as session:
        rows = session.execute(
            select(InstructionRecord.instruction_id, InstructionRecord.is_virtual).where(
                InstructionRecord.instruction_id.in_(instruction_ids)
            )
        ).all()
    virtual_ids: list[str] = []
    real_ids: list[str] = []
    virtual_by_instruction = {
        instruction_id: bool(is_virtual)
        for instruction_id, is_virtual in rows
    }
    for instruction_id in instruction_ids:
        if virtual_by_instruction.get(instruction_id):
            virtual_ids.append(instruction_id)
        else:
            real_ids.append(instruction_id)
    return virtual_ids, real_ids


def _has_virtual_runtime_work(
    session_factory: sessionmaker[Session],
) -> bool:
    with session_scope(session_factory) as session:
        row = session.execute(
            select(InstructionRecord.id)
            .where(
                InstructionRecord.is_virtual.is_(True),
                InstructionRecord.state.in_(
                    (
                        ExecutionState.ENTRY_PENDING.value,
                        ExecutionState.ENTRY_SUBMITTED.value,
                        ExecutionState.POSITION_OPEN.value,
                        ExecutionState.EXIT_PENDING.value,
                    )
                ),
            )
            .limit(1)
        ).first()
    return row is not None


def _is_entry_submission_due(
    record: InstructionRecord,
    *,
    cycle_at: datetime,
    session_calendar_path: Path,
    submission_lead_time: timedelta,
) -> bool:
    submit_at = _ensure_utc(record.submit_at)
    if submit_at is None:
        return False
    if submit_at <= cycle_at:
        return True
    if submit_at > cycle_at + submission_lead_time:
        return False

    try:
        instruction = _instruction_payload(record)
    except Exception:
        return False

    due_at = resolve_scheduled_submission_due_at(
        instruction,
        scheduled_at=instruction.entry.submit_at,
        session_calendar_path=session_calendar_path,
        submission_lead_time=submission_lead_time,
    )
    return due_at <= cycle_at


def _fetch_due_entry_instruction_ids(
    session_factory: sessionmaker[Session],
    *,
    cycle_at: datetime,
    session_calendar_path: Path,
    submission_lead_time: timedelta,
    instruction_ids: tuple[str, ...] | None = None,
) -> list[str]:
    candidate_cutoff = cycle_at + submission_lead_time
    with session_scope(session_factory) as session:
        query = select(InstructionRecord).where(
            InstructionRecord.state == ExecutionState.ENTRY_PENDING.value,
            InstructionRecord.submit_at <= candidate_cutoff,
        )
        if instruction_ids:
            query = query.where(InstructionRecord.instruction_id.in_(instruction_ids))
        records = list(
            session.execute(
                query.order_by(InstructionRecord.submit_at, InstructionRecord.id)
            ).scalars()
        )

    return [
        record.instruction_id
        for record in records
        if _is_entry_submission_due(
            record,
            cycle_at=cycle_at,
            session_calendar_path=session_calendar_path,
            submission_lead_time=submission_lead_time,
        )
    ]


def _is_pending_entry_expired(
    session_factory: sessionmaker[Session],
    *,
    instruction_id: str,
    cycle_at: datetime,
    session_calendar_path: Path,
) -> bool:
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord).where(
                InstructionRecord.instruction_id == instruction_id
            )
        ).scalar_one_or_none()
    if record is None:
        return False
    try:
        instruction = _instruction_payload(record)
        expire_at = resolve_effective_entry_expire_at_for_schedule(
            instruction,
            submit_at=_ensure_utc(record.submit_at) or record.submit_at,
            expire_at=_ensure_utc(record.expire_at) or record.expire_at,
            session_calendar_path=session_calendar_path,
        )
    except Exception:
        expire_at = record.expire_at
    normalized_expire_at = _ensure_utc(expire_at)
    if normalized_expire_at is None:
        return False
    return normalized_expire_at <= cycle_at


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


def _aggregate_broker_order_status_fill(
    session_factory: sessionmaker[Session],
    *,
    record: InstructionRecord,
    order_role: str,
    external_order_id: int | None = None,
) -> ExecutionAggregate:
    with session_scope(session_factory) as session:
        statement = select(BrokerOrderRecord).where(
            BrokerOrderRecord.instruction_id == record.id,
            BrokerOrderRecord.order_role == order_role,
        )
        if external_order_id is not None:
            statement = statement.where(
                BrokerOrderRecord.external_order_id == str(external_order_id)
            )
        broker_orders = session.execute(statement).scalars().all()

    seen_order_keys: set[str] = set()
    total_quantity = Decimal("0")
    weighted_notional = Decimal("0")
    last_execution_at: datetime | None = None
    matched_count = 0

    for broker_order in broker_orders:
        order_key = (
            broker_order.external_order_id
            or broker_order.external_perm_id
            or f"broker-order:{broker_order.id}"
        )
        if order_key in seen_order_keys:
            continue
        seen_order_keys.add(order_key)

        status_payload = broker_order.metadata_json.get("last_order_status_callback")
        if not isinstance(status_payload, dict):
            continue

        filled_quantity = _parse_decimal(
            str(status_payload.get("filled"))
            if status_payload.get("filled") not in (None, "")
            else None
        )
        if filled_quantity <= 0:
            continue

        average_fill_price = _parse_decimal(
            str(status_payload.get("avgFillPrice"))
            if status_payload.get("avgFillPrice") not in (None, "")
            else None
        )
        if average_fill_price <= 0:
            average_fill_price = _parse_decimal(
                str(status_payload.get("lastFillPrice"))
                if status_payload.get("lastFillPrice") not in (None, "")
                else None
            )

        total_quantity += filled_quantity
        if average_fill_price > 0:
            weighted_notional += average_fill_price * filled_quantity
        matched_count += 1
        if broker_order.last_status_at is not None and (
            last_execution_at is None or broker_order.last_status_at > last_execution_at
        ):
            last_execution_at = broker_order.last_status_at

    average_price = None
    if total_quantity > 0 and weighted_notional > 0:
        average_price = weighted_notional / total_quantity

    return ExecutionAggregate(
        quantity=total_quantity,
        average_price=average_price,
        executed_at=_ensure_utc(last_execution_at),
        execution_count=matched_count,
    )


def _aggregate_persisted_execution_fill(
    session_factory: sessionmaker[Session],
    *,
    record: InstructionRecord,
    order_role: str,
    external_order_id: int | None = None,
) -> ExecutionAggregate:
    with session_scope(session_factory) as session:
        statement = select(ExecutionFillRecord).where(
            ExecutionFillRecord.instruction_id == record.id,
        )
        if external_order_id is not None:
            statement = statement.where(
                ExecutionFillRecord.external_order_id == str(external_order_id)
            )
        elif order_role == "ENTRY":
            entry_predicates = [
                ExecutionFillRecord.order_ref == record.instruction_id,
            ]
            if record.broker_order_id is not None:
                entry_predicates.append(
                    ExecutionFillRecord.external_order_id == str(record.broker_order_id)
                )
            statement = statement.where(or_(*entry_predicates))
        else:
            exit_predicates = [
                ExecutionFillRecord.order_ref.like(f"{record.instruction_id}:exit:%"),
            ]
            if record.exit_order_id is not None:
                exit_predicates.append(
                    ExecutionFillRecord.external_order_id == str(record.exit_order_id)
                )
            statement = statement.where(or_(*exit_predicates))

        fills = session.execute(statement).scalars().all()

    seen_execution_ids: set[str] = set()
    total_quantity = Decimal("0")
    weighted_notional = Decimal("0")
    last_execution_at: datetime | None = None
    matched_count = 0

    for fill in fills:
        execution_id = fill.external_execution_id or f"execution-fill:{fill.id}"
        if execution_id in seen_execution_ids:
            continue
        seen_execution_ids.add(execution_id)

        quantity = _parse_decimal(fill.quantity)
        if quantity <= 0:
            continue

        price = _parse_decimal(fill.price)
        total_quantity += quantity
        if price > 0:
            weighted_notional += price * quantity
        matched_count += 1
        if fill.executed_at is not None and (
            last_execution_at is None or fill.executed_at > last_execution_at
        ):
            last_execution_at = fill.executed_at

    average_price = None
    if total_quantity > 0 and weighted_notional > 0:
        average_price = weighted_notional / total_quantity

    return ExecutionAggregate(
        quantity=total_quantity,
        average_price=average_price,
        executed_at=_ensure_utc(last_execution_at),
        execution_count=matched_count,
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


def _is_delayed_limit_exit_due(
    instruction: ExecutionInstruction,
    *,
    cycle_at: datetime,
    session_calendar_path: Path,
    submission_lead_time: timedelta,
) -> bool:
    delayed_limit = instruction.exit.delayed_limit
    if delayed_limit is None:
        return False
    due_at = resolve_scheduled_submission_due_at(
        instruction,
        scheduled_at=delayed_limit.submit_at,
        session_calendar_path=session_calendar_path,
        submission_lead_time=submission_lead_time,
    )
    return due_at <= cycle_at.astimezone(timezone.utc)


def _compute_delayed_limit_price(
    instruction: ExecutionInstruction,
    *,
    market_price: Decimal,
) -> Decimal:
    delayed_limit = instruction.exit.delayed_limit
    if delayed_limit is None:
        raise ValueError("exit.delayed_limit is required to compute the delayed limit price.")
    if market_price <= 0:
        raise ValueError("Delayed-exit market anchor price must be positive.")

    if instruction.intent.side == "BUY":
        raw_price = market_price * (Decimal("1") + delayed_limit.limit_offset_pct)
    elif instruction.intent.side == "SELL":
        raw_price = market_price * (Decimal("1") - delayed_limit.limit_offset_pct)
    else:
        raise ValueError(f"Unsupported instruction side: {instruction.intent.side}")

    if raw_price <= 0:
        raise ValueError("Computed delayed exit limit price is not positive.")

    reference_price = instruction.entry.limit_price or market_price
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


def _normalize_broker_order_status(status: str | None) -> str | None:
    if status is None:
        return None
    normalized = status.strip()
    if not normalized:
        return None
    return normalized.upper()


def _persisted_open_order_ids_by_instruction(
    session_factory: sessionmaker[Session],
    *,
    records: list[InstructionRecord],
    order_role: str,
) -> dict[str, tuple[int, ...]]:
    if not records:
        return {}

    instruction_ids_by_record_id = {
        record.id: record.instruction_id
        for record in records
    }

    persisted_order_ids: dict[str, list[int]] = {
        record.instruction_id: []
        for record in records
    }

    with session_scope(session_factory) as session:
        rows = session.execute(
            select(BrokerOrderRecord).where(
                BrokerOrderRecord.instruction_id.in_(tuple(instruction_ids_by_record_id.keys())),
                BrokerOrderRecord.order_role == order_role,
            ).order_by(
                BrokerOrderRecord.last_status_at.desc(),
                BrokerOrderRecord.id.desc(),
            )
        ).scalars().all()

    seen_lineages: dict[str, set[tuple[str, str]]] = {
        record.instruction_id: set()
        for record in records
    }

    for broker_order in rows:
        public_instruction_id = instruction_ids_by_record_id.get(broker_order.instruction_id)
        if public_instruction_id is None:
            continue
        if _normalize_broker_order_status(broker_order.status) in _CLOSED_BROKER_ORDER_STATUSES:
            continue
        if broker_order.external_order_id in (None, ""):
            continue
        if _broker_order_has_matching_execution_fill(
            session_factory,
            broker_order=broker_order,
        ):
            continue
        lineage_key = (
            (
                str(broker_order.external_perm_id).strip()
                if broker_order.external_perm_id not in (None, "")
                else ""
            ),
            (
                str(broker_order.order_ref).strip()
                if broker_order.order_ref not in (None, "")
                else str(broker_order.external_order_id)
            ),
        )
        if lineage_key in seen_lineages[public_instruction_id]:
            continue
        try:
            persisted_order_ids[public_instruction_id].append(
                int(str(broker_order.external_order_id))
            )
        except ValueError:
            continue
        seen_lineages[public_instruction_id].add(lineage_key)

    return {
        instruction_id: tuple(sorted(set(order_ids)))
        for instruction_id, order_ids in persisted_order_ids.items()
    }


def _has_persisted_open_forced_exit_order(
    session_factory: sessionmaker[Session],
    *,
    record: InstructionRecord,
) -> bool:
    with session_scope(session_factory) as session:
        forced_exit_order = session.execute(
            select(BrokerOrderRecord.id).where(
                BrokerOrderRecord.instruction_id == record.id,
                BrokerOrderRecord.order_role == "EXIT",
                BrokerOrderRecord.order_ref == f"{record.instruction_id}:exit:forced",
                or_(
                    BrokerOrderRecord.status.is_(None),
                    func.upper(BrokerOrderRecord.status).not_in(
                        _CLOSED_BROKER_ORDER_STATUSES
                    ),
                ),
            )
        ).first()
    return forced_exit_order is not None


def _broker_order_has_matching_execution_fill(
    session_factory: sessionmaker[Session],
    *,
    broker_order: BrokerOrderRecord,
) -> bool:
    lineage_predicates = []
    if broker_order.external_perm_id not in (None, ""):
        lineage_predicates.append(
            ExecutionFillRecord.external_perm_id == broker_order.external_perm_id
        )
    if broker_order.external_order_id not in (None, ""):
        lineage_predicates.append(
            ExecutionFillRecord.external_order_id == broker_order.external_order_id
        )
    if broker_order.order_ref not in (None, ""):
        lineage_predicates.append(ExecutionFillRecord.order_ref == broker_order.order_ref)
    if broker_order.instruction_id is not None and (
        (broker_order.order_role or "").strip().upper() == "EXIT"
    ):
        lineage_predicates.append(
            and_(
                ExecutionFillRecord.instruction_id == broker_order.instruction_id,
                ExecutionFillRecord.order_ref.is_not(None),
                ExecutionFillRecord.order_ref.like("%:exit:%"),
            )
        )

    if not lineage_predicates:
        return False

    with session_scope(session_factory) as session:
        row = session.execute(
            select(ExecutionFillRecord.id)
            .where(
                ExecutionFillRecord.broker_account_id == broker_order.broker_account_id,
                or_(*lineage_predicates),
            )
            .limit(1)
        ).first()
    return row is not None


def _is_virtual_broker_order_id(
    session_factory: sessionmaker[Session],
    *,
    order_id: int,
) -> bool:
    with session_scope(session_factory) as session:
        row = session.execute(
            select(BrokerOrderRecord.is_virtual).where(
                BrokerOrderRecord.external_order_id == str(order_id)
            )
        ).scalar_one_or_none()
    return bool(row)


def _remaining_position_quantity(
    record: InstructionRecord,
    exit_fill: ExecutionAggregate,
) -> Decimal:
    entry_filled = _parse_decimal(record.entry_filled_quantity)
    if entry_filled <= 0:
        return Decimal("0")
    remaining = entry_filled - exit_fill.quantity
    return remaining if remaining > 0 else Decimal("0")


def _cancel_broker_order_and_persist(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    *,
    order_id: int,
    timeout: int,
    canceler: Callable[..., dict[str, Any]],
    event_type: str,
    note: str,
) -> dict[str, Any]:
    broker_cancellation = canceler(
        broker_config,
        order_id,
        timeout=timeout,
    )
    broker_kind = str(broker_cancellation.get("broker_kind") or BROKER_KIND_IBKR)
    fallback_account_key = (
        str(broker_cancellation["account"])
        if broker_cancellation.get("account") not in (None, "")
        else broker_config.account_id
    )
    persist_broker_order_cancellation_result(
        session_factory,
        broker_kind=broker_kind,
        broker_cancellation=broker_cancellation,
        observed_at=utc_now(),
        fallback_account_key=fallback_account_key,
        event_type=event_type,
        note=note,
    )
    return broker_cancellation


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

        protective_exits = _build_protective_exit_specs(
            instruction_id=instruction_id,
            instruction=instruction,
            entry_average_price=entry_fill.average_price,
        )

        if not protective_exits:
            return entry_action, ()

    submitted_exits = list(
        _submit_protective_exits(
            session_factory,
            broker_config,
            instruction_id=instruction_id,
            instruction=instruction,
            entry_quantity=entry_fill.quantity,
            protective_exits=protective_exits,
            timeout=timeout,
            exit_submitter=exit_submitter,
        )
    )

    return entry_action, tuple(submitted_exits)


def _mark_unfilled_entry_cancelled(
    session_factory: sessionmaker[Session],
    instruction_id: str,
    *,
    note: str,
    event_type: str = "entry_order_expired_without_fill",
    action: str = "entry_cancelled_without_fill",
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


def _submit_delayed_limit_exit(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    instruction_id: str,
    *,
    quantity: Decimal,
    market_reference: dict[str, Any],
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
        market_price = _parse_decimal(str(market_reference.get("price")))
        if market_price <= 0:
            raise ValueError(
                f"Instruction '{instruction_id}' did not receive a usable delayed-exit market price."
            )
        limit_price = _compute_delayed_limit_price(
            instruction,
            market_price=market_price,
        )
        runtime_exit_submitter = exit_submitter
        if runtime_exit_submitter is None:
            if is_virtual_account_key(instruction.account.account_key):
                def _submit_virtual_exit(
                    broker_config: IbkrConnectionConfig,
                    runtime_instruction: ExecutionInstruction,
                    **kwargs: Any,
                ) -> dict[str, Any]:
                    return submit_virtual_exit_order(
                        session_factory,
                        broker_config,
                        runtime_instruction,
                        **kwargs,
                    )

                runtime_exit_submitter = _submit_virtual_exit
            else:
                runtime_exit_submitter = submit_exit_order_from_instruction
        broker_submission = runtime_exit_submitter(
            broker_config,
            instruction,
            quantity=quantity,
            order_type=OrderType.LIMIT,
            order_ref=f"{instruction_id}:exit:delayed_limit",
            timeout=timeout,
            limit_price=limit_price,
        )
        broker_status = broker_submission["broker_order_status"]
        broker_kind = str(broker_submission.get("broker_kind") or BROKER_KIND_IBKR)
        fallback_account_key = (
            str(broker_submission["account"])
            if broker_submission.get("account") not in (None, "")
            else broker_config.account_id
        )
        previous_state = record.state
        record.exit_order_id = int(broker_status["orderId"])
        record.exit_perm_id = int(broker_status["permId"])
        record.exit_client_id = int(broker_status["clientId"])
        record.exit_order_status = str(broker_status["status"])
        record.exit_submitted_quantity = str(quantity)
        record.state = ExecutionState.EXIT_PENDING.value
        event_at = utc_now()
        persist_broker_order_submission(
            session,
            broker_kind=broker_kind,
            instruction_record=record,
            broker_submission=broker_submission,
            observed_at=event_at,
            fallback_account_key=fallback_account_key,
            order_role="EXIT",
            event_type="delayed_limit_exit_submitted",
            note="Submitted delayed limit exit anchored to live market at trigger time.",
        )
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type="delayed_limit_exit_submitted",
                source="runtime_cycle",
                event_at=event_at,
                state_before=previous_state,
                state_after=record.state,
                payload=_serialize_for_json(
                    {
                        "broker_submission": broker_submission,
                        "market_reference": market_reference,
                        "computed_limit_price": limit_price,
                    }
                ),
                note="Submitted delayed limit exit anchored to live market at trigger time.",
            )
        )
        return RuntimeCycleAction(
            instruction_id=instruction_id,
            action="delayed_limit_exit_submitted",
            state=record.state,
            detail={
                "broker_order_id": record.exit_order_id,
                "broker_order_status": record.exit_order_status,
                "exit_submitted_quantity": record.exit_submitted_quantity,
                "limit_price": str(limit_price),
                "market_reference": _serialize_for_json(market_reference),
            },
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
        runtime_exit_submitter = exit_submitter
        if runtime_exit_submitter is None:
            if is_virtual_account_key(instruction.account.account_key):
                def _submit_virtual_exit(
                    broker_config: IbkrConnectionConfig,
                    runtime_instruction: ExecutionInstruction,
                    **kwargs: Any,
                ) -> dict[str, Any]:
                    return submit_virtual_exit_order(
                        session_factory,
                        broker_config,
                        runtime_instruction,
                        **kwargs,
                    )

                runtime_exit_submitter = _submit_virtual_exit
            else:
                runtime_exit_submitter = submit_exit_order_from_instruction
        broker_submission = runtime_exit_submitter(
            broker_config,
            instruction,
            quantity=quantity,
            order_type=OrderType.MARKET,
            order_ref=f"{instruction_id}:exit:forced",
            timeout=timeout,
        )
        broker_status = broker_submission["broker_order_status"]
        broker_kind = str(broker_submission.get("broker_kind") or BROKER_KIND_IBKR)
        fallback_account_key = (
            str(broker_submission["account"])
            if broker_submission.get("account") not in (None, "")
            else broker_config.account_id
        )
        previous_state = record.state
        record.exit_order_id = int(broker_status["orderId"])
        record.exit_perm_id = int(broker_status["permId"])
        record.exit_client_id = int(broker_status["clientId"])
        record.exit_order_status = str(broker_status["status"])
        record.exit_submitted_quantity = str(quantity)
        record.state = ExecutionState.EXIT_PENDING.value
        event_at = utc_now()
        persist_broker_order_submission(
            session,
            broker_kind=broker_kind,
            instruction_record=record,
            broker_submission=broker_submission,
            observed_at=event_at,
            fallback_account_key=fallback_account_key,
            order_role="EXIT",
            event_type="forced_exit_submitted",
            note="Submitted forced market exit at the next session open.",
        )
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type="forced_exit_submitted",
                source="runtime_cycle",
                event_at=event_at,
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
        record.exit_order_status = "Filled"
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
    submission_lead_time: timedelta,
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
    due_at = preview.next_session_open_utc - submission_lead_time
    return due_at <= cycle_at.astimezone(timezone.utc)


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
                    payload={"error": str(exc)},
                )
                continue

            _mark_pending_entry_failed(
                session_factory,
                instruction_id,
                note=(
                    "Runtime cycle marked the due entry as failed after a terminal "
                    "broker submission error."
                ),
                payload={"error": str(exc)},
            )


def run_runtime_cycle(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    *,
    run_kind: str = "runtime_cycle",
    runtime_timezone: str,
    session_calendar_path: Path,
    now: datetime | None = None,
    timeout: int = 10,
    instruction_ids: tuple[str, ...] | None = None,
    submit_due_entries: bool = True,
    entry_submitter: Callable[..., Any] | None = None,
    entry_canceler: Callable[..., Any] | None = None,
    exit_submitter: Callable[..., dict[str, Any]] | None = None,
    market_price_reader: Callable[..., dict[str, Any]] | None = None,
    broker_snapshot_fetcher: Callable[..., BrokerRuntimeSnapshot] | None = None,
    broker_callback_fetcher: Callable[[], list[dict[str, Any]]] | None = None,
    broker_order_canceler: Callable[..., dict[str, Any]] | None = None,
    virtual_market_sync: Callable[[datetime], Any] | None = None,
    broker_retry_delays: tuple[float, ...] = DEFAULT_BROKER_RETRY_DELAYS,
    submission_lead_time: timedelta = DEFAULT_SUBMISSION_LEAD_TIME,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> RuntimeCycleResult:
    cycle_started_at = now.astimezone(timezone.utc) if now is not None else utc_now()
    runtime_snapshot_fetch = broker_snapshot_fetcher or fetch_broker_runtime_snapshot
    if broker_order_canceler is None:
        def runtime_order_canceler(
            broker_config: IbkrConnectionConfig,
            order_id: int,
            *,
            timeout: int = 10,
        ) -> dict[str, Any]:
            if _is_virtual_broker_order_id(session_factory, order_id=order_id):
                return cancel_virtual_order(
                    session_factory,
                    broker_config,
                    order_id,
                    timeout=timeout,
                )
            return cancel_broker_order(
                broker_config,
                order_id,
                timeout=timeout,
            )
    else:
        runtime_order_canceler = broker_order_canceler
    due_instruction_count = 0
    active_instruction_count = 0
    snapshot: BrokerRuntimeSnapshot | None = None
    submitted_entries: list[RuntimeCycleAction] = []
    cancelled_entries: list[RuntimeCycleAction] = []
    filled_entries: list[RuntimeCycleAction] = []
    submitted_exits: list[RuntimeCycleAction] = []
    completed_instructions: list[RuntimeCycleAction] = []
    issues: list[RuntimeCycleIssue] = []
    broker_snapshot_unavailable = False

    if broker_callback_fetcher is not None:
        try:
            _persist_drained_broker_callbacks(
                session_factory,
                broker_config=broker_config,
                callback_events=broker_callback_fetcher(),
            )
        except Exception as exc:  # pragma: no cover - broad by design for runtime safety
            _append_issue(
                issues,
                instruction_id=None,
                stage="broker_callbacks_pre_cycle",
                message=str(exc),
            )
            return _finalize_runtime_cycle_result(
                session_factory,
                broker_config,
                run_kind=run_kind,
                runtime_timezone=runtime_timezone,
                cycle_started_at=cycle_started_at,
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

    if virtual_market_sync is not None:
        try:
            virtual_market_sync(cycle_started_at)
        except Exception as exc:  # pragma: no cover - broad by design for runtime safety
            _append_issue(
                issues,
                instruction_id=None,
                stage="virtual_market_sync",
                message=str(exc),
            )

    kill_switch_state = read_kill_switch_state(session_factory)
    kill_switch_enabled = kill_switch_state.enabled
    due_instruction_ids = _fetch_due_entry_instruction_ids(
        session_factory,
        cycle_at=cycle_started_at,
        session_calendar_path=session_calendar_path,
        submission_lead_time=submission_lead_time,
        instruction_ids=instruction_ids,
    )
    due_instruction_count = len(due_instruction_ids)

    active_instruction_ids = _fetch_instruction_ids(
        session_factory,
        states=(
            ExecutionState.ENTRY_SUBMITTED.value,
            ExecutionState.POSITION_OPEN.value,
            ExecutionState.EXIT_PENDING.value,
        ),
        instruction_ids=instruction_ids,
    )
    active_instruction_count = len(active_instruction_ids)
    if not active_instruction_ids:
        if submit_due_entries:
            _submit_due_pending_entries(
                session_factory,
                broker_config,
                due_instruction_ids=due_instruction_ids,
                cycle_started_at=cycle_started_at,
                session_calendar_path=session_calendar_path,
                timeout=timeout,
                kill_switch_enabled=kill_switch_enabled,
                entry_submitter=entry_submitter,
                broker_retry_delays=broker_retry_delays,
                sleep_fn=sleep_fn,
                submitted_entries=submitted_entries,
                cancelled_entries=cancelled_entries,
                issues=issues,
            )
        if broker_callback_fetcher is not None:
            try:
                _persist_drained_broker_callbacks(
                    session_factory,
                    broker_config=broker_config,
                    callback_events=broker_callback_fetcher(),
                )
            except Exception as exc:  # pragma: no cover - broad by design for runtime safety
                _append_issue(
                    issues,
                    instruction_id=None,
                    stage="broker_callbacks_post_cycle",
                    message=str(exc),
                )
        return _finalize_runtime_cycle_result(
            session_factory,
            broker_config,
            run_kind=run_kind,
            runtime_timezone=runtime_timezone,
            cycle_started_at=cycle_started_at,
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

    if has_real_broker_work(session_factory, instruction_ids=instruction_ids):
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
            broker_snapshot_unavailable = True
            _append_issue(
                issues,
                instruction_id=None,
                stage="broker_snapshot",
                message=str(exc),
            )
            if broker_callback_fetcher is not None:
                try:
                    _persist_drained_broker_callbacks(
                        session_factory,
                        broker_config=broker_config,
                        callback_events=broker_callback_fetcher(),
                    )
                except Exception as callback_exc:  # pragma: no cover - broad by design for runtime safety
                    _append_issue(
                        issues,
                        instruction_id=None,
                        stage="broker_callbacks_post_cycle",
                        message=str(callback_exc),
                    )
            snapshot = BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            )
    else:
        snapshot = BrokerRuntimeSnapshot(
            open_orders={},
            executions=(),
            portfolio=(),
            positions=(),
            account_values={},
        )

    try:
        persist_broker_runtime_snapshot(
            session_factory,
            snapshot,
            broker_kind=BROKER_KIND_IBKR,
            captured_at=cycle_started_at,
            default_account_key=broker_config.account_id,
        )
    except Exception as exc:  # pragma: no cover - broad by design for runtime safety
        _append_issue(
            issues,
            instruction_id=None,
            stage="ledger_persist",
            message=str(exc),
        )
        if broker_callback_fetcher is not None:
            try:
                _persist_drained_broker_callbacks(
                    session_factory,
                    broker_config=broker_config,
                    callback_events=broker_callback_fetcher(),
                )
            except Exception as callback_exc:  # pragma: no cover - broad by design for runtime safety
                _append_issue(
                    issues,
                    instruction_id=None,
                    stage="broker_callbacks_post_cycle",
                    message=str(callback_exc),
                )
        return _finalize_runtime_cycle_result(
            session_factory,
            broker_config,
            run_kind=run_kind,
            runtime_timezone=runtime_timezone,
            cycle_started_at=cycle_started_at,
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

    with session_scope(session_factory) as session:
        records = session.execute(
            select(InstructionRecord).where(
                InstructionRecord.instruction_id.in_(active_instruction_ids)
            )
        ).scalars().all()

    records_by_instruction_id = {
        record.instruction_id: record for record in records
    }
    persisted_open_exit_order_ids_by_instruction = _persisted_open_order_ids_by_instruction(
        session_factory,
        records=records,
        order_role="EXIT",
    )
    persisted_open_entry_order_ids_by_instruction = _persisted_open_order_ids_by_instruction(
        session_factory,
        records=records,
        order_role="ENTRY",
    )
    blocking_due_exit_instruction_ids: list[str] = []
    blocking_due_exit_account_keys: set[str] = set()

    for instruction_id in active_instruction_ids:
        record = records_by_instruction_id.get(instruction_id)
        if record is None:
            continue
        if broker_snapshot_unavailable and not record.is_virtual:
            continue

        try:
            instruction = _instruction_payload(record)
            entry_fill = _aggregate_executions(
                snapshot.executions,
                order_id=record.broker_order_id,
                order_ref_exact=instruction.instruction_id,
            )
            if not entry_fill.has_fill:
                entry_fill = _aggregate_persisted_execution_fill(
                    session_factory,
                    record=record,
                    order_role="ENTRY",
                    external_order_id=record.broker_order_id,
                )
            if not entry_fill.has_fill:
                entry_fill = _aggregate_broker_order_status_fill(
                    session_factory,
                    record=record,
                    order_role="ENTRY",
                    external_order_id=record.broker_order_id,
                )
            exit_fill = _aggregate_executions(
                snapshot.executions,
                order_ref_prefix=f"{instruction.instruction_id}:exit:",
            )
            if not exit_fill.has_fill:
                exit_fill = _aggregate_persisted_execution_fill(
                    session_factory,
                    record=record,
                    order_role="EXIT",
                )
            if not exit_fill.has_fill:
                exit_fill = _aggregate_broker_order_status_fill(
                    session_factory,
                    record=record,
                    order_role="EXIT",
                )

            persisted_entry_open_order_ids = persisted_open_entry_order_ids_by_instruction.get(
                instruction_id,
                (),
            )
            entry_open = (
                (
                    record.broker_order_id is not None
                    and record.broker_order_id in snapshot.open_orders
                )
                or bool(persisted_entry_open_order_ids)
            )
            exit_open_order_ids = _open_order_ids_with_ref_prefix(
                snapshot,
                order_ref_prefix=f"{instruction.instruction_id}:exit:",
            )
            persisted_exit_open_order_ids = persisted_open_exit_order_ids_by_instruction.get(
                instruction_id,
                (),
            )
            combined_exit_open_order_ids = tuple(
                sorted(set(exit_open_order_ids) | set(persisted_exit_open_order_ids))
            )
            exit_open = bool(combined_exit_open_order_ids)
            try:
                effective_expire_at = resolve_effective_entry_expire_at_for_schedule(
                    instruction,
                    submit_at=_ensure_utc(record.submit_at) or record.submit_at,
                    expire_at=_ensure_utc(record.expire_at) or record.expire_at,
                    session_calendar_path=session_calendar_path,
                )
            except Exception:
                effective_expire_at = record.expire_at
            expire_at = _ensure_utc(effective_expire_at) or effective_expire_at

            if record.state == ExecutionState.ENTRY_SUBMITTED.value:
                if kill_switch_enabled:
                    if entry_fill.has_fill:
                        if entry_open:
                            _run_with_broker_retries(
                                lambda: _cancel_broker_order_and_persist(
                                    session_factory,
                                    broker_config,
                                    order_id=record.broker_order_id,
                                    timeout=timeout,
                                    canceler=runtime_order_canceler,
                                    event_type="entry_order_cancelled_after_fill",
                                    note=(
                                        "Persisted broker cancellation after the entry "
                                        "fill was already observed."
                                    ),
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
                                action="entry_cancelled_by_kill_switch",
                                state=cancellation.state,
                                detail={
                                    "broker_order_id": cancellation.broker_order_id,
                                    "broker_order_status": cancellation.broker_order_status,
                                    "reason": kill_switch_state.reason,
                                },
                            )
                        )
                    else:
                        cancelled_entries.append(
                            _mark_unfilled_entry_cancelled(
                                session_factory,
                                instruction_id,
                                note=(
                                    "Global kill switch was enabled and no open broker "
                                    "entry order remained."
                                ),
                                event_type="entry_order_cancelled_by_kill_switch",
                                action="entry_cancelled_by_kill_switch",
                            )
                        )
                    continue

                if entry_fill.has_fill:
                    if entry_open and cycle_started_at < expire_at:
                        continue
                    if entry_open and instruction.entry.cancel_unfilled_at_expiry:
                        _run_with_broker_retries(
                            lambda: _cancel_broker_order_and_persist(
                                session_factory,
                                broker_config,
                                order_id=record.broker_order_id,
                                timeout=timeout,
                                canceler=runtime_order_canceler,
                                event_type="entry_order_cancelled_post_expiry_fill",
                                note=(
                                    "Persisted broker cancellation after an entry fill "
                                    "arrived beyond the entry expiry window."
                                ),
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

                if (
                    record.state == ExecutionState.POSITION_OPEN.value
                    and remaining_quantity > 0
                    and not exit_open
                ):
                    repaired_exits = _run_with_broker_retries(
                        lambda: _submit_missing_protective_exits(
                            session_factory,
                            broker_config,
                            instruction_id,
                            quantity=remaining_quantity,
                            cycle_at=cycle_started_at,
                            timeout=timeout,
                            exit_submitter=exit_submitter,
                        ),
                        retry_delays=broker_retry_delays,
                        sleep_fn=sleep_fn,
                    )
                    if repaired_exits:
                        submitted_exits.extend(repaired_exits)
                        continue

                if (
                    _is_delayed_limit_exit_due(
                        instruction,
                        cycle_at=cycle_started_at,
                        session_calendar_path=session_calendar_path,
                        submission_lead_time=submission_lead_time,
                    )
                    and remaining_quantity > 0
                    and not exit_open
                ):
                    runtime_market_price_reader = market_price_reader
                    if (
                        runtime_market_price_reader is None
                        and is_virtual_account_key(instruction.account.account_key)
                    ):
                        def _read_virtual_market_price(
                            _broker_config: IbkrConnectionConfig,
                            runtime_instruction: ExecutionInstruction,
                            *,
                            at: datetime,
                            timeout: int = 10,
                        ) -> dict[str, Any]:
                            del at, timeout
                            return read_virtual_market_price(
                                session_factory,
                                runtime_instruction,
                            )

                        runtime_market_price_reader = _read_virtual_market_price
                    if runtime_market_price_reader is None:
                        raise ValueError(
                            "Delayed limit exits require a market_price_reader."
                        )
                    market_reference = _run_with_broker_retries(
                        lambda: runtime_market_price_reader(
                            broker_config,
                            instruction,
                            at=cycle_started_at,
                            timeout=timeout,
                        ),
                        retry_delays=broker_retry_delays,
                        sleep_fn=sleep_fn,
                    )
                    submitted_exits.append(
                        _run_with_broker_retries(
                            lambda: _submit_delayed_limit_exit(
                                session_factory,
                                broker_config,
                                instruction_id,
                                quantity=remaining_quantity,
                                market_reference=market_reference,
                                timeout=timeout,
                                exit_submitter=exit_submitter,
                            ),
                            retry_delays=broker_retry_delays,
                            sleep_fn=sleep_fn,
                        )
                    )
                    continue

                if not _is_next_session_exit_due(
                    instruction,
                    runtime_timezone=runtime_timezone,
                    session_calendar_path=session_calendar_path,
                    cycle_at=cycle_started_at,
                    submission_lead_time=submission_lead_time,
                ):
                    continue

                if remaining_quantity <= 0:
                    continue

                blocking_due_exit_instruction_ids.append(instruction_id)
                blocking_due_exit_account_keys.add(record.account_key)
                if _has_persisted_open_forced_exit_order(
                    session_factory,
                    record=record,
                ):
                    continue
                for open_exit_order_id in combined_exit_open_order_ids:
                    _run_with_broker_retries(
                        lambda order_id=open_exit_order_id: _cancel_broker_order_and_persist(
                            session_factory,
                            broker_config,
                            order_id=order_id,
                            timeout=timeout,
                            canceler=runtime_order_canceler,
                            event_type="exit_order_cancelled_before_forced_exit",
                            note=(
                                "Persisted broker cancellation before submitting the "
                                "next-session forced exit."
                            ),
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

    if submit_due_entries:
        # Active exit workflows take priority over fresh entries so we do not
        # size or submit new risk before urgent carry-over positions are handled.
        # The gate is account-scoped: a virtual account cleanup issue must not
        # prevent an unrelated live account from submitting its own due entry.
        due_instruction_ids_to_submit = due_instruction_ids
        if blocking_due_exit_account_keys and due_instruction_ids:
            due_account_keys = _fetch_instruction_account_keys(
                session_factory,
                due_instruction_ids,
            )
            due_instruction_ids_to_submit = [
                instruction_id
                for instruction_id in due_instruction_ids
                if due_account_keys.get(instruction_id) not in blocking_due_exit_account_keys
            ]
        if broker_snapshot_unavailable and due_instruction_ids_to_submit:
            due_instruction_ids_to_submit, skipped_real_due_instruction_ids = (
                _split_instruction_ids_by_virtual(
                    session_factory,
                    due_instruction_ids_to_submit,
                )
            )
            if skipped_real_due_instruction_ids:
                _append_issue(
                    issues,
                    instruction_id=None,
                    stage="entry_submit",
                    message=(
                        "Skipped real broker due entries because the broker snapshot "
                        "was unavailable; virtual due entries were still processed."
                    ),
                )
        if due_instruction_ids_to_submit:
            _submit_due_pending_entries(
                session_factory,
                broker_config,
                due_instruction_ids=due_instruction_ids_to_submit,
                cycle_started_at=cycle_started_at,
                session_calendar_path=session_calendar_path,
                timeout=timeout,
                kill_switch_enabled=kill_switch_enabled,
                entry_submitter=entry_submitter,
                broker_retry_delays=broker_retry_delays,
                sleep_fn=sleep_fn,
                submitted_entries=submitted_entries,
                cancelled_entries=cancelled_entries,
                issues=issues,
            )

    if broker_callback_fetcher is not None:
        try:
            _persist_drained_broker_callbacks(
                session_factory,
                broker_config=broker_config,
                callback_events=broker_callback_fetcher(),
            )
        except Exception as exc:  # pragma: no cover - broad by design for runtime safety
            _append_issue(
                issues,
                instruction_id=None,
                stage="broker_callbacks_post_cycle",
                message=str(exc),
            )
    return _finalize_runtime_cycle_result(
        session_factory,
        broker_config,
        run_kind=run_kind,
        runtime_timezone=runtime_timezone,
        cycle_started_at=cycle_started_at,
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


def run_startup_reconciliation(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    *,
    runtime_timezone: str,
    session_calendar_path: Path,
    now: datetime | None = None,
    timeout: int = 10,
    instruction_ids: tuple[str, ...] | None = None,
    exit_submitter: Callable[..., dict[str, Any]] | None = None,
    market_price_reader: Callable[..., dict[str, Any]] | None = None,
    broker_snapshot_fetcher: Callable[..., BrokerRuntimeSnapshot] | None = None,
    broker_callback_fetcher: Callable[[], list[dict[str, Any]]] | None = None,
    broker_order_canceler: Callable[..., dict[str, Any]] | None = None,
    virtual_market_sync: Callable[[datetime], Any] | None = None,
    broker_retry_delays: tuple[float, ...] = DEFAULT_BROKER_RETRY_DELAYS,
    submission_lead_time: timedelta = DEFAULT_SUBMISSION_LEAD_TIME,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> RuntimeCycleResult:
    """Reconcile live broker state on startup without submitting new entry orders."""

    return run_runtime_cycle(
        session_factory,
        broker_config,
        run_kind="startup_reconciliation",
        runtime_timezone=runtime_timezone,
        session_calendar_path=session_calendar_path,
        now=now,
        timeout=timeout,
        instruction_ids=instruction_ids,
        submit_due_entries=False,
        exit_submitter=exit_submitter,
        market_price_reader=market_price_reader,
        broker_snapshot_fetcher=broker_snapshot_fetcher,
        broker_callback_fetcher=broker_callback_fetcher,
        broker_order_canceler=broker_order_canceler,
        virtual_market_sync=virtual_market_sync,
        broker_retry_delays=broker_retry_delays,
        submission_lead_time=submission_lead_time,
        sleep_fn=sleep_fn,
    )


@dataclass(slots=True)
class RuntimeBrokerOperations:
    submit_entry: Callable[..., dict[str, Any]]
    submit_exit: Callable[..., dict[str, Any]]
    read_market_price: Callable[..., dict[str, Any]]
    fetch_snapshot: Callable[..., BrokerRuntimeSnapshot]
    fetch_reconciliation_snapshot: Callable[..., BrokerRuntimeSnapshot]
    drain_callbacks: Callable[[], list[dict[str, Any]]]
    cancel_order: Callable[..., dict[str, Any]]


def _build_runtime_broker_operations(
    broker_sessions: CanonicalSyncSessions,
    session_factory: sessionmaker[Session] | None = None,
) -> RuntimeBrokerOperations:
    def submit_entry_with_primary(
        broker_config: IbkrConnectionConfig,
        instruction: ExecutionInstruction,
        *,
        timeout: int = 10,
    ) -> dict[str, Any]:
        if session_factory is not None and is_virtual_account_key(
            instruction.account.account_key
        ):
            from ibkr_trader.virtual.execution import submit_virtual_entry_order

            return submit_virtual_entry_order(
                session_factory,
                broker_config,
                instruction,
                timeout=timeout,
            )
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
        if session_factory is not None and is_virtual_account_key(
            instruction.account.account_key
        ):
            return submit_virtual_exit_order(
                session_factory,
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
            )
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

    def read_market_price_with_primary(
        broker_config: IbkrConnectionConfig,
        instruction: ExecutionInstruction,
        *,
        at: datetime,
        timeout: int = 10,
    ) -> dict[str, Any]:
        if session_factory is not None and is_virtual_account_key(
            instruction.account.account_key
        ):
            return read_virtual_market_price(session_factory, instruction)
        return broker_sessions.primary.execute(
            "runtime_market_reference",
            lambda broker_app: read_latest_trade_price(
                broker_config,
                symbol=instruction.instrument.symbol,
                exchange=instruction.instrument.exchange,
                currency=instruction.instrument.currency,
                security_type=instruction.instrument.security_type.value,
                primary_exchange=instruction.instrument.primary_exchange,
                isin=instruction.instrument.isin,
                end_at=at,
                timeout=timeout,
                app=broker_app,
            ),
        )

    def fetch_snapshot_with_primary(
        broker_config: IbkrConnectionConfig,
        *,
        timeout: int = 10,
    ) -> BrokerRuntimeSnapshot:
        if session_factory is not None and not has_real_broker_work(session_factory):
            return BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            )
        return broker_sessions.primary.execute(
            "runtime_snapshot",
            lambda broker_app: fetch_broker_runtime_snapshot(
                broker_config,
                timeout=timeout,
                include_open_orders=False,
                include_executions=False,
                include_account_updates=False,
                include_positions=False,
                app=broker_app,
            ),
        )

    def fetch_reconciliation_snapshot_with_primary(
        broker_config: IbkrConnectionConfig,
        *,
        timeout: int = 10,
    ) -> BrokerRuntimeSnapshot:
        if session_factory is not None and not has_real_broker_work(session_factory):
            return BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            )
        return broker_sessions.primary.execute(
            "runtime_reconciliation_snapshot",
            lambda broker_app: fetch_broker_runtime_snapshot(
                broker_config,
                timeout=timeout,
                include_open_orders=True,
                include_executions=True,
                include_account_updates=False,
                include_positions=True,
                app=broker_app,
            ),
        )

    def drain_callbacks_with_primary() -> list[dict[str, Any]]:
        if session_factory is not None and not has_real_broker_work(session_factory):
            return []
        return broker_sessions.primary.drain_broker_callback_events()

    def cancel_order_with_primary(
        broker_config: IbkrConnectionConfig,
        order_id: int,
        *,
        timeout: int = 10,
    ) -> dict[str, Any]:
        if session_factory is not None and _is_virtual_broker_order_id(
            session_factory,
            order_id=order_id,
        ):
            return cancel_virtual_order(
                session_factory,
                broker_config,
                order_id,
                timeout=timeout,
            )
        return broker_sessions.primary.execute(
            "runtime_cancel",
            lambda broker_app: cancel_broker_order(
                broker_config,
                order_id,
                timeout=timeout,
                app=broker_app,
            ),
        )

    return RuntimeBrokerOperations(
        submit_entry=submit_entry_with_primary,
        submit_exit=submit_exit_with_primary,
        read_market_price=read_market_price_with_primary,
        fetch_snapshot=fetch_snapshot_with_primary,
        fetch_reconciliation_snapshot=fetch_reconciliation_snapshot_with_primary,
        drain_callbacks=drain_callbacks_with_primary,
        cancel_order=cancel_order_with_primary,
    )


def _runtime_owner_label() -> tuple[str, str, int]:
    hostname = socket.gethostname()
    pid = os.getpid()
    return f"{hostname}:{pid}", hostname, pid


def run_persistent_execution_runtime(
    session_factory: sessionmaker[Session],
    app_config: AppConfig,
    broker_sessions: CanonicalSyncSessions,
    *,
    interval_seconds: float,
    timeout: int,
    once: bool = False,
    skip_startup_reconciliation: bool = False,
    allow_startup_issues: bool = False,
    runtime_key: str = EXECUTION_RUNTIME_KEY,
    lease_seconds: float = 30.0,
    stop_event: Event | None = None,
    emit_results: bool = True,
    shutdown_sessions_on_exit: bool = True,
    virtual_market_sync: Callable[[datetime], Any] | None = None,
) -> int:
    runtime_stop_event = stop_event or Event()
    owner_token = uuid.uuid4().hex
    owner_label, hostname, pid = _runtime_owner_label()
    broker_config = app_config.ibkr.primary_session()
    broker_ops = _build_runtime_broker_operations(broker_sessions, session_factory)
    submission_lead_time = timedelta(
        seconds=app_config.execution_runtime_submission_lead_seconds
    )

    acquire_runtime_service_lease(
        session_factory,
        runtime_key=runtime_key,
        service_type="execution",
        owner_token=owner_token,
        owner_label=owner_label,
        hostname=hostname,
        pid=pid,
        runtime_timezone=app_config.timezone,
        broker_kind=BROKER_KIND_IBKR,
        broker_client_id=broker_config.client_id,
        lease_seconds=lease_seconds,
        metadata_json={
            "interval_seconds": interval_seconds,
            "timeout_seconds": timeout,
            "submission_lead_seconds": app_config.execution_runtime_submission_lead_seconds,
            "allow_startup_issues": allow_startup_issues,
        },
    )

    broker_sessions.warmup()
    runtime_released = False
    try:
        if not skip_startup_reconciliation:
            record_runtime_cycle_started(
                session_factory,
                runtime_key=runtime_key,
                owner_token=owner_token,
                lease_seconds=lease_seconds,
            )
            startup_result = run_startup_reconciliation(
                session_factory,
                broker_config,
                runtime_timezone=app_config.timezone,
                session_calendar_path=app_config.session_calendar_path,
                timeout=timeout,
                exit_submitter=broker_ops.submit_exit,
                market_price_reader=broker_ops.read_market_price,
                broker_snapshot_fetcher=broker_ops.fetch_reconciliation_snapshot,
                broker_callback_fetcher=broker_ops.drain_callbacks,
                broker_order_canceler=broker_ops.cancel_order,
                virtual_market_sync=virtual_market_sync,
                submission_lead_time=submission_lead_time,
            )
            record_runtime_cycle_completed(
                session_factory,
                runtime_key=runtime_key,
                owner_token=owner_token,
                lease_seconds=lease_seconds,
                result=startup_result,
            )
            if emit_results:
                _emit_runtime_cycle_result(startup_result)
            if (
                startup_result.issues
                and not allow_startup_issues
                and not _has_virtual_runtime_work(session_factory)
            ):
                mark_runtime_service_startup_blocked(
                    session_factory,
                    runtime_key=runtime_key,
                    owner_token=owner_token,
                    result=startup_result,
                )
                runtime_released = True
                print(
                    (
                        "Startup reconciliation reported issues; refusing to start the "
                        "runtime loop. Re-run with --allow-startup-issues to override."
                    ),
                    file=sys.stderr,
                )
                return 2
            if startup_result.issues and not allow_startup_issues:
                print(
                    (
                        "Startup reconciliation reported issues; continuing the "
                        "runtime loop for virtual work. Real broker mutations remain "
                        "blocked until broker state is readable."
                    ),
                    file=sys.stderr,
                )

        while True:
            if runtime_stop_event.is_set():
                break

            lease_snapshot = record_runtime_cycle_started(
                session_factory,
                runtime_key=runtime_key,
                owner_token=owner_token,
                lease_seconds=lease_seconds,
            )
            if lease_snapshot.stop_requested:
                break

            result = run_runtime_cycle(
                session_factory,
                broker_config,
                runtime_timezone=app_config.timezone,
                session_calendar_path=app_config.session_calendar_path,
                timeout=timeout,
                entry_submitter=broker_ops.submit_entry,
                exit_submitter=broker_ops.submit_exit,
                market_price_reader=broker_ops.read_market_price,
                broker_snapshot_fetcher=broker_ops.fetch_snapshot,
                broker_callback_fetcher=broker_ops.drain_callbacks,
                broker_order_canceler=broker_ops.cancel_order,
                virtual_market_sync=virtual_market_sync,
                submission_lead_time=submission_lead_time,
            )
            record_runtime_cycle_completed(
                session_factory,
                runtime_key=runtime_key,
                owner_token=owner_token,
                lease_seconds=lease_seconds,
                result=result,
            )
            if emit_results:
                _emit_runtime_cycle_result(result)
            if once:
                break
            if runtime_stop_event.wait(interval_seconds):
                break

        stop_note = (
            "Completed the requested one-shot execution-runtime cycle."
            if once
            else "Execution runtime stopped cleanly."
        )
        mark_runtime_service_stopped(
            session_factory,
            runtime_key=runtime_key,
            owner_token=owner_token,
            note=stop_note,
        )
        runtime_released = True
        return 0
    except Exception as exc:
        if not runtime_released:
            try:
                mark_runtime_service_failed(
                    session_factory,
                    runtime_key=runtime_key,
                    owner_token=owner_token,
                    error=str(exc),
                )
                runtime_released = True
            except RuntimeServiceLeaseError:
                pass
        raise
    finally:
        if shutdown_sessions_on_exit:
            broker_sessions.shutdown()


class BackgroundExecutionRuntimeService:
    """Run the execution runtime loop inside the long-lived API host process."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        app_config: AppConfig,
        broker_sessions: CanonicalSyncSessions,
        virtual_market_sync: Callable[[datetime], Any] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._app_config = app_config
        self._broker_sessions = broker_sessions
        self._virtual_market_sync = virtual_market_sync
        self._stop_event = Event()
        self._thread: Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        thread = Thread(
            target=self._run,
            name="execution-runtime-service",
            daemon=True,
        )
        thread.start()
        self._thread = thread

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=max(5.0, self._app_config.execution_runtime_interval_seconds + 5.0))
        self._thread = None

    def status(self) -> dict[str, Any] | None:
        return serialize_runtime_service_status(
            read_runtime_service_status(
                self._session_factory,
                runtime_key=EXECUTION_RUNTIME_KEY,
            )
        )

    def _run(self) -> None:
        restart_delay = max(
            0.0,
            self._app_config.execution_runtime_restart_backoff_initial_seconds,
        )
        max_restart_delay = max(
            restart_delay,
            self._app_config.execution_runtime_restart_backoff_max_seconds,
        )
        while not self._stop_event.is_set():
            try:
                exit_code = run_persistent_execution_runtime(
                    self._session_factory,
                    self._app_config,
                    self._broker_sessions,
                    interval_seconds=self._app_config.execution_runtime_interval_seconds,
                    timeout=self._app_config.execution_runtime_timeout_seconds,
                    allow_startup_issues=self._app_config.execution_runtime_allow_startup_issues,
                    lease_seconds=self._app_config.execution_runtime_lease_seconds,
                    stop_event=self._stop_event,
                    emit_results=False,
                    shutdown_sessions_on_exit=False,
                    virtual_market_sync=self._virtual_market_sync,
                )
            except RuntimeServiceLeaseError:
                if self._stop_event.wait(restart_delay):
                    return
                continue
            except Exception:
                if self._stop_event.wait(restart_delay):
                    return
                restart_delay = min(max_restart_delay, restart_delay * 2)
                continue

            if self._stop_event.is_set() or exit_code == 0:
                return
            if self._stop_event.wait(restart_delay):
                return
            restart_delay = min(max_restart_delay, restart_delay * 2)


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
    parser.add_argument(
        "--skip-startup-reconciliation",
        action="store_true",
        help=(
            "Skip the startup reconciliation pass. This is not recommended for the "
            "persistent runtime."
        ),
    )
    parser.add_argument(
        "--allow-startup-issues",
        action="store_true",
        help=(
            "Continue into the runtime loop even if startup reconciliation reports issues."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    app_config = AppConfig.from_env()
    session_factory = create_session_factory(build_engine(app_config.database_url))
    broker_sessions = CanonicalSyncSessions(app_config.ibkr)
    try:
        return run_persistent_execution_runtime(
            session_factory,
            app_config,
            broker_sessions,
            interval_seconds=args.interval_seconds,
            timeout=args.timeout,
            once=args.once,
            skip_startup_reconciliation=args.skip_startup_reconciliation,
            allow_startup_issues=args.allow_startup_issues,
            lease_seconds=app_config.execution_runtime_lease_seconds,
        )
    except RuntimeServiceLeaseError as exc:
        print(str(exc), file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
