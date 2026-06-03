from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import session_scope, utc_now
from ibkr_trader.db.models import BrokerOrderRecord, InstructionEventRecord, InstructionRecord
from ibkr_trader.domain.execution_contract import ExecutionInstruction, OrderType
from ibkr_trader.domain.execution_payloads import parse_execution_instruction_payload
from ibkr_trader.ibkr.order_execution import submit_exit_order_from_instruction
from ibkr_trader.ledger.persistence import BROKER_KIND_IBKR, persist_broker_order_submission
from ibkr_trader.orchestration.runtime_broker_matching import _normalize_broker_order_status
from ibkr_trader.orchestration.runtime_exit_pricing import _compute_delayed_limit_price
from ibkr_trader.orchestration.runtime_protective_exits import _build_protective_exit_specs, _submit_missing_protective_exits
from ibkr_trader.orchestration.runtime_types import EntryBrokerOrderSnapshot, ExecutionAggregate, RuntimeCycleAction
from ibkr_trader.orchestration.runtime_types import parse_decimal as _parse_decimal
from ibkr_trader.orchestration.runtime_types import serialize_for_json as _serialize_for_json
from ibkr_trader.orchestration.scheduling import NextSessionExitStatus, build_instruction_runtime_schedule
from ibkr_trader.orchestration.state_machine import ExecutionState
from ibkr_trader.virtual.accounts import is_virtual_account_key
from ibkr_trader.virtual.execution import submit_virtual_exit_order


_CLOSED_BROKER_ORDER_STATUSES = {
    "API_CANCELLED",
    "CANCELLED",
    "ERROR",
    "FILLED",
    "INACTIVE",
    "NOT_FOUND_AT_BROKER",
    "REJECTED",
}


def _instruction_payload(record: InstructionRecord) -> ExecutionInstruction:
    raw_instruction_payload = record.payload.get("instruction")
    if not isinstance(raw_instruction_payload, dict):
        raise ValueError(
            f"Instruction '{record.instruction_id}' does not contain a valid persisted payload."
        )
    return parse_execution_instruction_payload(raw_instruction_payload)


def _record_entry_fill_and_optional_exit(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    instruction_id: str,
    *,
    entry_fill: ExecutionAggregate,
    cycle_at: datetime,
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
        _submit_missing_protective_exits(
            session_factory,
            broker_config,
            instruction_id,
            quantity=entry_fill.quantity,
            cycle_at=cycle_at,
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
        latest_entry_order_status = session.execute(
            select(BrokerOrderRecord.status)
            .where(
                BrokerOrderRecord.instruction_id == record.id,
                BrokerOrderRecord.order_role == "ENTRY",
            )
            .order_by(BrokerOrderRecord.id.desc())
            .limit(1)
        ).scalar_one_or_none()
        if latest_entry_order_status not in (None, ""):
            record.broker_order_status = str(latest_entry_order_status)
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


def _latest_entry_broker_order_snapshot(
    session: Session,
    record: InstructionRecord,
) -> EntryBrokerOrderSnapshot | None:
    broker_order = session.execute(
        select(BrokerOrderRecord)
        .where(
            BrokerOrderRecord.instruction_id == record.id,
            BrokerOrderRecord.order_role == "ENTRY",
        )
        .order_by(BrokerOrderRecord.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    if broker_order is None:
        return None
    return EntryBrokerOrderSnapshot(
        broker_order_id=broker_order.id,
        external_order_id=broker_order.external_order_id,
        external_perm_id=broker_order.external_perm_id,
        status=broker_order.status,
        order_ref=broker_order.order_ref,
        raw_payload=dict(broker_order.raw_payload or {}),
        metadata_json=dict(broker_order.metadata_json or {}),
    )


def _is_resubmittable_entry_order_status(status: str | None) -> bool:
    normalized_status = _normalize_broker_order_status(status)
    if normalized_status is None:
        return True
    return (
        normalized_status in _CLOSED_BROKER_ORDER_STATUSES
        and normalized_status != "FILLED"
    )


def _mark_entry_requeued_for_resubmit(
    session_factory: sessionmaker[Session],
    instruction_id: str,
    *,
    latest_broker_order: EntryBrokerOrderSnapshot | None,
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
                action="entry_requeue_skip",
                state=record.state,
                detail={},
            )

        previous_state = record.state
        previous_broker_order_id = record.broker_order_id
        previous_broker_perm_id = record.broker_perm_id
        previous_broker_client_id = record.broker_client_id
        previous_broker_order_status = record.broker_order_status

        record.state = ExecutionState.ENTRY_PENDING.value
        record.broker_order_id = None
        record.broker_perm_id = None
        record.broker_client_id = None
        record.broker_order_status = None
        record.entry_submitted_quantity = None

        latest_payload = (
            _serialize_for_json(asdict(latest_broker_order))
            if latest_broker_order is not None
            else None
        )
        payload = {
            "previous_broker_order_id": previous_broker_order_id,
            "previous_broker_perm_id": previous_broker_perm_id,
            "previous_broker_client_id": previous_broker_client_id,
            "previous_broker_order_status": previous_broker_order_status,
            "latest_broker_order": latest_payload,
        }
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type="entry_order_requeued_for_resubmit",
                source="runtime_cycle",
                state_before=previous_state,
                state_after=record.state,
                payload=_serialize_for_json(payload),
                note=note,
            )
        )
        return RuntimeCycleAction(
            instruction_id=instruction_id,
            action="entry_requeued_for_resubmit",
            state=record.state,
            detail=_serialize_for_json(payload),
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
        serialized_broker_submission = _serialize_for_json(broker_submission)
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
                payload={"broker_submission": serialized_broker_submission},
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


def _has_due_real_forced_exit_candidate(
    records: list[InstructionRecord],
    *,
    runtime_timezone: str,
    session_calendar_path: Path,
    cycle_at: datetime,
    submission_lead_time: timedelta,
) -> bool:
    for record in records:
        if record.is_virtual:
            continue
        if record.state not in {
            ExecutionState.POSITION_OPEN.value,
            ExecutionState.EXIT_PENDING.value,
        }:
            continue
        if _parse_decimal(record.entry_filled_quantity) <= 0:
            continue
        try:
            instruction = _instruction_payload(record)
            if _is_next_session_exit_due(
                instruction,
                runtime_timezone=runtime_timezone,
                session_calendar_path=session_calendar_path,
                cycle_at=cycle_at,
                submission_lead_time=submission_lead_time,
            ):
                return True
        except Exception:
            continue
    return False
