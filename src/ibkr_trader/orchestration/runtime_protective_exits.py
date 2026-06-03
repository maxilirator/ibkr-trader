from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, sessionmaker

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import session_scope, utc_now
from ibkr_trader.db.models import BrokerOrderRecord, InstructionEventRecord, InstructionRecord
from ibkr_trader.domain.execution_contract import ExecutionInstruction, OrderType
from ibkr_trader.domain.execution_payloads import parse_execution_instruction_payload
from ibkr_trader.ibkr.order_execution import submit_exit_order_from_instruction
from ibkr_trader.ledger.persistence import BROKER_KIND_IBKR, persist_broker_order_submission
from ibkr_trader.orchestration.runtime_broker_errors import broker_exception_payload as _broker_exception_payload
from ibkr_trader.orchestration.runtime_exit_pricing import _compute_stop_price, _compute_take_profit_price
from ibkr_trader.orchestration.runtime_types import RuntimeCycleAction
from ibkr_trader.orchestration.runtime_types import ensure_utc as _ensure_utc
from ibkr_trader.orchestration.runtime_types import parse_decimal as _parse_decimal
from ibkr_trader.orchestration.runtime_types import serialize_for_json as _serialize_for_json
from ibkr_trader.orchestration.state_machine import ExecutionState
from ibkr_trader.virtual.accounts import is_virtual_account_key
from ibkr_trader.virtual.execution import submit_virtual_exit_order


PROTECTIVE_EXIT_RETRY_COOLDOWN = timedelta(minutes=15)
PROTECTIVE_EXIT_SUBMISSION_CLAIM_WINDOW = timedelta(minutes=15)
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


def _desired_protective_exit_order_refs(
    *,
    instruction_id: str,
    instruction: ExecutionInstruction,
) -> set[str]:
    refs: set[str] = set()
    if instruction.exit.stop_loss_pct is not None:
        refs.add(f"{instruction_id}:exit:stop_loss")
    if instruction.exit.catastrophic_stop_loss_pct is not None:
        refs.add(f"{instruction_id}:exit:catastrophic_stop")
    if instruction.exit.take_profit_pct is not None:
        refs.add(f"{instruction_id}:exit:take_profit")
    return refs


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
            **_broker_exception_payload(error),
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
    serialized_broker_submission = _serialize_for_json(broker_submission)
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
                payload={"broker_submission": serialized_broker_submission},
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
    force_oca_group: bool = False,
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
        if force_oca_group or len(protective_exits) > 1
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


def _open_exit_order_ref_rows_for_record(
    session: Session,
    *,
    instruction_record_id: int,
) -> dict[str, list[BrokerOrderRecord]]:
    rows = session.execute(
        select(BrokerOrderRecord).where(
            BrokerOrderRecord.instruction_id == instruction_record_id,
            BrokerOrderRecord.order_role == "EXIT",
            BrokerOrderRecord.order_ref.is_not(None),
            or_(
                BrokerOrderRecord.status.is_(None),
                func.upper(BrokerOrderRecord.status).not_in(
                    _CLOSED_BROKER_ORDER_STATUSES
                ),
            ),
        )
    ).scalars().all()

    rows_by_ref: dict[str, list[BrokerOrderRecord]] = {}
    for broker_order in rows:
        order_ref = str(broker_order.order_ref or "").strip()
        if order_ref:
            rows_by_ref.setdefault(order_ref, []).append(broker_order)
    return rows_by_ref


def _recent_protective_exit_claimed_refs(
    session: Session,
    *,
    instruction_record_id: int,
    cycle_at: datetime,
) -> set[str]:
    cutoff = cycle_at.astimezone(timezone.utc) - PROTECTIVE_EXIT_SUBMISSION_CLAIM_WINDOW
    events = session.execute(
        select(InstructionEventRecord)
        .where(
            InstructionEventRecord.instruction_id == instruction_record_id,
            InstructionEventRecord.event_type == "protective_exit_submission_claimed",
            InstructionEventRecord.event_at >= cutoff,
        )
        .order_by(InstructionEventRecord.event_at.desc())
    ).scalars()

    claimed_refs: set[str] = set()
    for event in events:
        payload = event.payload or {}
        if not isinstance(payload, dict):
            continue
        order_refs = payload.get("order_refs")
        if not isinstance(order_refs, list):
            continue
        claimed_refs.update(
            str(order_ref).strip()
            for order_ref in order_refs
            if str(order_ref).strip()
        )
    return claimed_refs


def _record_duplicate_protective_exit_block(
    session: Session,
    *,
    record: InstructionRecord,
    duplicate_refs: dict[str, list[BrokerOrderRecord]],
) -> None:
    existing_event = session.execute(
        select(InstructionEventRecord.id)
        .where(
            InstructionEventRecord.instruction_id == record.id,
            InstructionEventRecord.event_type == "protective_exit_duplicate_blocked",
        )
        .limit(1)
    ).first()
    if existing_event is not None:
        return

    session.add(
        InstructionEventRecord(
            instruction_id=record.id,
            event_type="protective_exit_duplicate_blocked",
            source="runtime_cycle",
            state_before=record.state,
            state_after=record.state,
            payload=_serialize_for_json(
                {
                    "active_duplicate_exit_order_refs": {
                        order_ref: [
                            {
                                "broker_order_id": broker_order.id,
                                "external_order_id": broker_order.external_order_id,
                                "external_perm_id": broker_order.external_perm_id,
                                "status": broker_order.status,
                            }
                            for broker_order in broker_orders
                        ]
                        for order_ref, broker_orders in duplicate_refs.items()
                    }
                }
            ),
            note=(
                "Multiple active broker exit orders share the same protective "
                "order_ref. Runtime is fail-closed and will not submit more "
                "protective exits until an operator cleans up the duplicates."
            ),
        )
    )


def _submit_missing_protective_exits(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    instruction_id: str,
    *,
    quantity: Decimal,
    cycle_at: datetime,
    timeout: int,
    exit_submitter: Callable[..., dict[str, Any]] | None,
    existing_exit_order_refs: set[str] | None = None,
) -> tuple[RuntimeCycleAction, ...]:
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == instruction_id)
            .with_for_update()
        ).scalar_one()
        if record.state not in {
            ExecutionState.POSITION_OPEN.value,
            ExecutionState.EXIT_PENDING.value,
        }:
            return ()
        instruction = _instruction_payload(record)
        entry_average_price = _parse_decimal(record.entry_avg_fill_price)
        if entry_average_price <= 0:
            return ()
        all_protective_exits = _build_protective_exit_specs(
            instruction_id=instruction_id,
            instruction=instruction,
            entry_average_price=entry_average_price,
        )
        if not all_protective_exits:
            return ()

        rows_by_ref = _open_exit_order_ref_rows_for_record(
            session,
            instruction_record_id=record.id,
        )
        duplicate_refs = {
            order_ref: broker_orders
            for order_ref, broker_orders in rows_by_ref.items()
            if len(broker_orders) > 1
        }
        if duplicate_refs:
            _record_duplicate_protective_exit_block(
                session,
                record=record,
                duplicate_refs=duplicate_refs,
            )
            return ()

        existing_refs = set(existing_exit_order_refs or set())
        existing_refs.update(rows_by_ref.keys())
        existing_refs.update(
            _recent_protective_exit_claimed_refs(
                session,
                instruction_record_id=record.id,
                cycle_at=cycle_at,
            )
        )
        protective_exits = [
            exit_spec
            for exit_spec in all_protective_exits
            if str(exit_spec["order_ref"]) not in existing_refs
        ]
        if not protective_exits:
            return ()

        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type="protective_exit_submission_claimed",
                source="runtime_cycle",
                event_at=cycle_at,
                state_before=record.state,
                state_after=record.state,
                payload=_serialize_for_json(
                    {
                        "order_refs": [
                            str(exit_spec["order_ref"])
                            for exit_spec in protective_exits
                        ],
                    }
                ),
                note=(
                    "Runtime claimed protective exit submission before broker "
                    "mutation to prevent duplicate exit orders."
                ),
            )
        )

    oca_group = (
        _protective_exit_oca_group(instruction_id)
        if len(all_protective_exits) > 1
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
        force_oca_group=len(all_protective_exits) > 1,
    )
