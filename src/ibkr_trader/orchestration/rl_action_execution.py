from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from decimal import Decimal
from decimal import InvalidOperation
from enum import Enum
from typing import Any
from typing import Callable

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.domain.execution_contract import OrderType
from ibkr_trader.domain.execution_payloads import parse_execution_instruction_payload
from ibkr_trader.ibkr.order_execution import cancel_broker_order
from ibkr_trader.ibkr.order_execution import submit_exit_order_from_instruction
from ibkr_trader.ledger.persistence import BROKER_KIND_IBKR
from ibkr_trader.ledger.persistence import persist_broker_order_cancellation
from ibkr_trader.ledger.persistence import persist_broker_order_submission
from ibkr_trader.orchestration.entry_submission import cancel_persisted_instruction_entry
from ibkr_trader.orchestration.entry_submission import serialize_persisted_broker_cancellation
from ibkr_trader.orchestration.state_machine import ExecutionState
from ibkr_trader.virtual.accounts import is_virtual_account_key
from ibkr_trader.virtual.execution import cancel_virtual_order
from ibkr_trader.virtual.execution import submit_virtual_exit_order


_EXIT_TP_RE = re.compile(r"^exit_tp_(\d+)bp$")
_CLOSED_BROKER_ORDER_STATUSES = {
    "API_CANCELLED",
    "CANCELLED",
    "ERROR",
    "FILLED",
    "INACTIVE",
    "NOT_FOUND_AT_BROKER",
    "REJECTED",
}
_OWNED_ACTIVE_STATES = {
    ExecutionState.ENTRY_PENDING.value,
    ExecutionState.ENTRY_SUBMITTED.value,
    ExecutionState.POSITION_OPEN.value,
    ExecutionState.EXIT_PENDING.value,
}


class RLActionOwnershipError(ValueError):
    """Raised when an RL action cannot be tied to one durable generated instruction."""


class RLActionStateError(ValueError):
    """Raised when an RL action targets an owned instruction in the wrong state."""


@dataclass(slots=True)
class RLOwnedActionExecution:
    deployment_key: str
    action_name: str
    source_instruction_id: str
    instruction_id: str
    state_before: str
    state_after: str
    broker_order_id: int | None = None
    exit_order_id: int | None = None
    order_status: str | None = None
    quantity: str | None = None
    limit_price: str | None = None
    cancellations: tuple[dict[str, Any], ...] = ()
    submission: dict[str, Any] | None = None
    note: str | None = None


def serialize_rl_owned_action_execution(
    execution: RLOwnedActionExecution,
) -> dict[str, Any]:
    return _serialize_for_json(
        {
            "deployment_key": execution.deployment_key,
            "action_name": execution.action_name,
            "source_instruction_id": execution.source_instruction_id,
            "instruction_id": execution.instruction_id,
            "state_before": execution.state_before,
            "state_after": execution.state_after,
            "broker_order_id": execution.broker_order_id,
            "exit_order_id": execution.exit_order_id,
            "order_status": execution.order_status,
            "quantity": execution.quantity,
            "limit_price": execution.limit_price,
            "cancellations": execution.cancellations,
            "submission": execution.submission,
            "note": execution.note,
        }
    )


def execute_owned_rl_action(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    source_instruction: ExecutionInstruction,
    *,
    deployment_key: str,
    action_name: str,
    timeout: int = 10,
    canceler: Callable[..., dict[str, Any]] | None = None,
    exit_submitter: Callable[..., dict[str, Any]] | None = None,
) -> RLOwnedActionExecution:
    if action_name == "cancel_entry":
        return _execute_cancel_entry(
            session_factory,
            broker_config,
            source_instruction,
            deployment_key=deployment_key,
            timeout=timeout,
            canceler=canceler,
        )
    if action_name == "clear_exit":
        return _execute_clear_exit(
            session_factory,
            broker_config,
            source_instruction,
            deployment_key=deployment_key,
            timeout=timeout,
            canceler=canceler,
        )
    if action_name == "exit_market":
        return _execute_exit(
            session_factory,
            broker_config,
            source_instruction,
            deployment_key=deployment_key,
            action_name=action_name,
            timeout=timeout,
            canceler=canceler,
            exit_submitter=exit_submitter,
            order_type=OrderType.MARKET,
            take_profit_basis_points=None,
        )
    exit_match = _EXIT_TP_RE.match(action_name)
    if exit_match is not None:
        return _execute_exit(
            session_factory,
            broker_config,
            source_instruction,
            deployment_key=deployment_key,
            action_name=action_name,
            timeout=timeout,
            canceler=canceler,
            exit_submitter=exit_submitter,
            order_type=OrderType.LIMIT,
            take_profit_basis_points=Decimal(exit_match.group(1)),
        )
    raise RLActionStateError(f"Action '{action_name}' is not a durable RL-owned action.")


def _execute_cancel_entry(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    source_instruction: ExecutionInstruction,
    *,
    deployment_key: str,
    timeout: int,
    canceler: Callable[..., dict[str, Any]] | None,
) -> RLOwnedActionExecution:
    with session_scope(session_factory) as session:
        record = _find_owned_instruction(
            session,
            source_instruction,
            deployment_key=deployment_key,
            expected_states={
                ExecutionState.ENTRY_PENDING.value,
                ExecutionState.ENTRY_SUBMITTED.value,
            },
        )
        instruction_id = record.instruction_id
        state_before = record.state

    if state_before == ExecutionState.ENTRY_SUBMITTED.value:
        cancellation = cancel_persisted_instruction_entry(
            session_factory,
            broker_config,
            instruction_id,
            timeout=timeout,
            canceler=canceler,
        )
        payload = serialize_persisted_broker_cancellation(cancellation)
        return RLOwnedActionExecution(
            deployment_key=deployment_key,
            action_name="cancel_entry",
            source_instruction_id=source_instruction.instruction_id,
            instruction_id=instruction_id,
            state_before=state_before,
            state_after=cancellation.state,
            broker_order_id=cancellation.broker_order_id,
            order_status=cancellation.broker_order_status,
            cancellations=(payload,),
            note="RL cancelled its submitted entry order.",
        )

    with session_scope(session_factory) as session:
        record = _find_owned_instruction(
            session,
            source_instruction,
            deployment_key=deployment_key,
            expected_states={ExecutionState.ENTRY_PENDING.value},
        )
        state_before = record.state
        record.state = ExecutionState.ENTRY_CANCELLED.value
        event_at = utc_now()
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type="rl_entry_cancelled_before_submit",
                source="rl_action",
                event_at=event_at,
                state_before=state_before,
                state_after=record.state,
                payload={
                    "deployment_key": deployment_key,
                    "source_instruction_id": source_instruction.instruction_id,
                    "action_name": "cancel_entry",
                },
                note="RL cancelled its pending entry before broker submission.",
            )
        )
        return RLOwnedActionExecution(
            deployment_key=deployment_key,
            action_name="cancel_entry",
            source_instruction_id=source_instruction.instruction_id,
            instruction_id=record.instruction_id,
            state_before=state_before,
            state_after=record.state,
            note="RL cancelled its pending entry before broker submission.",
        )


def _execute_clear_exit(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    source_instruction: ExecutionInstruction,
    *,
    deployment_key: str,
    timeout: int,
    canceler: Callable[..., dict[str, Any]] | None,
) -> RLOwnedActionExecution:
    with session_scope(session_factory) as session:
        record = _find_owned_instruction(
            session,
            source_instruction,
            deployment_key=deployment_key,
            expected_states={ExecutionState.EXIT_PENDING.value},
        )
        state_before = record.state
        cancellations = _cancel_active_exit_orders(
            session,
            session_factory,
            broker_config,
            record,
            timeout=timeout,
            canceler=canceler,
            event_type="rl_exit_order_cancelled",
            note="RL cancelled its pending exit order.",
        )
        record.exit_order_status = _last_cancellation_status(cancellations)
        record.state = ExecutionState.POSITION_OPEN.value
        event_at = utc_now()
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type="rl_exit_cleared",
                source="rl_action",
                event_at=event_at,
                state_before=state_before,
                state_after=record.state,
                payload={
                    "deployment_key": deployment_key,
                    "source_instruction_id": source_instruction.instruction_id,
                    "action_name": "clear_exit",
                    "cancellations": cancellations,
                },
                note="RL cancelled the pending exit and kept the position open.",
            )
        )
        return RLOwnedActionExecution(
            deployment_key=deployment_key,
            action_name="clear_exit",
            source_instruction_id=source_instruction.instruction_id,
            instruction_id=record.instruction_id,
            state_before=state_before,
            state_after=record.state,
            exit_order_id=record.exit_order_id,
            order_status=record.exit_order_status,
            cancellations=tuple(cancellations),
            note="RL cancelled the pending exit and kept the position open.",
        )


def _execute_exit(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    source_instruction: ExecutionInstruction,
    *,
    deployment_key: str,
    action_name: str,
    timeout: int,
    canceler: Callable[..., dict[str, Any]] | None,
    exit_submitter: Callable[..., dict[str, Any]] | None,
    order_type: OrderType,
    take_profit_basis_points: Decimal | None,
) -> RLOwnedActionExecution:
    with session_scope(session_factory) as session:
        record = _find_owned_instruction(
            session,
            source_instruction,
            deployment_key=deployment_key,
            expected_states={
                ExecutionState.POSITION_OPEN.value,
                ExecutionState.EXIT_PENDING.value,
            },
        )
        state_before = record.state
        cancellations: list[dict[str, Any]] = []
        if record.state == ExecutionState.EXIT_PENDING.value:
            cancellations = _cancel_active_exit_orders(
                session,
                session_factory,
                broker_config,
                record,
                timeout=timeout,
                canceler=canceler,
                event_type="rl_exit_order_replaced",
                note="RL cancelled its existing exit before replacement.",
            )

        quantity = _remaining_quantity(record)
        if quantity <= 0:
            raise RLActionStateError(
                f"Instruction '{record.instruction_id}' has no remaining position quantity."
            )
        instruction = _instruction_payload(record)
        limit_price = (
            _compute_take_profit_price(
                record,
                instruction,
                take_profit_basis_points=take_profit_basis_points,
            )
            if take_profit_basis_points is not None
            else None
        )
        runtime_exit_submitter = _resolve_exit_submitter(
            session_factory,
            instruction,
            exit_submitter=exit_submitter,
        )
        broker_submission = runtime_exit_submitter(
            broker_config,
            instruction,
            quantity=quantity,
            order_type=order_type,
            limit_price=limit_price,
            order_ref=_exit_order_ref(record.instruction_id, action_name),
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
        record.exit_perm_id = (
            int(broker_status["permId"])
            if broker_status.get("permId") not in (None, "")
            else None
        )
        record.exit_client_id = (
            int(broker_status["clientId"])
            if broker_status.get("clientId") not in (None, "")
            else None
        )
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
            event_type=_exit_event_type(action_name),
            note=_exit_note(action_name),
        )
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type=_exit_event_type(action_name),
                source="rl_action",
                event_at=event_at,
                state_before=previous_state,
                state_after=record.state,
                payload=_serialize_for_json(
                    {
                        "deployment_key": deployment_key,
                        "source_instruction_id": source_instruction.instruction_id,
                        "action_name": action_name,
                        "quantity": quantity,
                        "limit_price": limit_price,
                        "broker_submission": broker_submission,
                        "cancellations": cancellations,
                    }
                ),
                note=_exit_note(action_name),
            )
        )
        return RLOwnedActionExecution(
            deployment_key=deployment_key,
            action_name=action_name,
            source_instruction_id=source_instruction.instruction_id,
            instruction_id=record.instruction_id,
            state_before=state_before,
            state_after=record.state,
            broker_order_id=record.broker_order_id,
            exit_order_id=record.exit_order_id,
            order_status=record.exit_order_status,
            quantity=str(quantity),
            limit_price=str(limit_price) if limit_price is not None else None,
            cancellations=tuple(cancellations),
            submission=_serialize_for_json(broker_submission),
            note=_exit_note(action_name),
        )


def _find_owned_instruction(
    session: Session,
    source_instruction: ExecutionInstruction,
    *,
    deployment_key: str,
    expected_states: set[str],
) -> InstructionRecord:
    statement = (
        select(InstructionRecord)
        .where(
            InstructionRecord.source_system == "rl-runner",
            InstructionRecord.account_key == source_instruction.account.account_key,
            InstructionRecord.book_key == source_instruction.account.book_key,
            InstructionRecord.symbol == source_instruction.instrument.symbol,
            InstructionRecord.archived_at.is_(None),
            InstructionRecord.state.in_(tuple(_OWNED_ACTIVE_STATES)),
        )
        .order_by(
            InstructionRecord.updated_at.desc(),
            InstructionRecord.id.desc(),
        )
        .with_for_update()
    )
    candidates = [
        record
        for record in session.execute(statement).scalars().all()
        if _metadata_matches_source(
            record,
            deployment_key=deployment_key,
            source_instruction_id=source_instruction.instruction_id,
        )
    ]
    if len(candidates) > 1:
        active_ids = ", ".join(record.instruction_id for record in candidates)
        raise RLActionOwnershipError(
            "Multiple active RL-generated instructions matched one source "
            f"instruction; refusing to mutate broker state: {active_ids}"
        )
    if not candidates:
        raise RLActionOwnershipError(
            "No active RL-generated instruction matched deployment "
            f"'{deployment_key}', source instruction "
            f"'{source_instruction.instruction_id}', and states "
            f"{sorted(expected_states)}."
        )
    record = candidates[0]
    if record.state not in expected_states:
        raise RLActionStateError(
            f"Instruction '{record.instruction_id}' is in state '{record.state}', "
            f"expected one of {sorted(expected_states)}."
        )
    return record


def _metadata_matches_source(
    record: InstructionRecord,
    *,
    deployment_key: str,
    source_instruction_id: str,
) -> bool:
    metadata = _instruction_metadata(record)
    return (
        str(metadata.get("rl_deployment_key") or "") == deployment_key
        and str(metadata.get("rl_source_instruction_id") or "") == source_instruction_id
    )


def _instruction_metadata(record: InstructionRecord) -> dict[str, Any]:
    payload = record.payload or {}
    instruction = payload.get("instruction")
    if not isinstance(instruction, dict):
        return {}
    trace = instruction.get("trace")
    if not isinstance(trace, dict):
        return {}
    metadata = trace.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def _instruction_payload(record: InstructionRecord) -> ExecutionInstruction:
    payload = record.payload or {}
    instruction_payload = payload.get("instruction")
    if not isinstance(instruction_payload, dict):
        raise ValueError(
            f"Instruction '{record.instruction_id}' does not contain an instruction payload."
        )
    return parse_execution_instruction_payload(instruction_payload)


def _cancel_active_exit_orders(
    session: Session,
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    record: InstructionRecord,
    *,
    timeout: int,
    canceler: Callable[..., dict[str, Any]] | None,
    event_type: str,
    note: str,
) -> list[dict[str, Any]]:
    order_ids = _active_exit_order_ids(session, record)
    if not order_ids:
        raise RLActionStateError(
            f"Instruction '{record.instruction_id}' is EXIT_PENDING but has no "
            "active durable exit order to cancel."
        )

    runtime_canceler = _resolve_canceler(session_factory, canceler=canceler)
    cancellations: list[dict[str, Any]] = []
    for order_id in order_ids:
        broker_cancellation = runtime_canceler(
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
        persist_broker_order_cancellation(
            session,
            broker_kind=broker_kind,
            broker_cancellation=broker_cancellation,
            observed_at=utc_now(),
            instruction_record=record,
            fallback_account_key=fallback_account_key,
            event_type=event_type,
            note=note,
        )
        cancellations.append(_serialize_for_json(broker_cancellation))
    return cancellations


def _active_exit_order_ids(session: Session, record: InstructionRecord) -> list[int]:
    rows = (
        session.execute(
            select(BrokerOrderRecord.external_order_id, BrokerOrderRecord.status)
            .where(
                BrokerOrderRecord.instruction_id == record.id,
                BrokerOrderRecord.order_role == "EXIT",
                BrokerOrderRecord.external_order_id.is_not(None),
            )
            .order_by(BrokerOrderRecord.id.asc())
        )
        .all()
    )
    order_ids = [
        int(str(order_id))
        for order_id, status in rows
        if order_id not in (None, "")
        and str(status or "").strip().upper() not in _CLOSED_BROKER_ORDER_STATUSES
    ]
    if order_ids:
        return order_ids
    if (
        record.exit_order_id is not None
        and str(record.exit_order_status or "").strip().upper()
        not in _CLOSED_BROKER_ORDER_STATUSES
    ):
        return [record.exit_order_id]
    return []


def _remaining_quantity(record: InstructionRecord) -> Decimal:
    entry_filled = _parse_decimal(record.entry_filled_quantity)
    exit_filled = _parse_decimal(record.exit_filled_quantity)
    remaining = entry_filled - exit_filled
    return remaining if remaining > 0 else Decimal("0")


def _compute_take_profit_price(
    record: InstructionRecord,
    instruction: ExecutionInstruction,
    *,
    take_profit_basis_points: Decimal | None,
) -> Decimal:
    if take_profit_basis_points is None:
        raise ValueError("take_profit_basis_points is required")
    entry_average_price = _parse_decimal(record.entry_avg_fill_price)
    if entry_average_price <= 0:
        raise RLActionStateError(
            f"Instruction '{record.instruction_id}' has no entry average price."
        )
    take_profit_pct = take_profit_basis_points / Decimal("10000")
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


def _quantize_like(value: Decimal, reference: Decimal) -> Decimal:
    exponent = reference.as_tuple().exponent
    if exponent >= 0:
        return value.quantize(Decimal("1"))
    return value.quantize(Decimal("1").scaleb(exponent))


def _parse_decimal(value: str | None) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid decimal payload value: {value}") from exc


def _resolve_exit_submitter(
    session_factory: sessionmaker[Session],
    instruction: ExecutionInstruction,
    *,
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


def _resolve_canceler(
    session_factory: sessionmaker[Session],
    *,
    canceler: Callable[..., dict[str, Any]] | None,
) -> Callable[..., dict[str, Any]]:
    if canceler is not None:
        return canceler

    def _cancel_virtual_or_broker(
        broker_config: IbkrConnectionConfig,
        order_id: int,
        *,
        timeout: int = 10,
    ) -> dict[str, Any]:
        if _is_virtual_order(session_factory, order_id):
            return cancel_virtual_order(
                session_factory,
                broker_config,
                order_id,
                timeout=timeout,
            )
        return cancel_broker_order(broker_config, order_id, timeout=timeout)

    return _cancel_virtual_or_broker


def _is_virtual_order(
    session_factory: sessionmaker[Session],
    order_id: int,
) -> bool:
    with session_scope(session_factory) as session:
        row = session.execute(
            select(BrokerOrderRecord.is_virtual).where(
                BrokerOrderRecord.external_order_id == str(order_id)
            )
        ).scalar_one_or_none()
    return bool(row)


def _exit_order_ref(instruction_id: str, action_name: str) -> str:
    if action_name == "exit_market":
        return f"{instruction_id}:exit:rl_market"
    return f"{instruction_id}:exit:rl_take_profit"


def _exit_event_type(action_name: str) -> str:
    if action_name == "exit_market":
        return "rl_market_exit_submitted"
    return "rl_take_profit_exit_submitted"


def _exit_note(action_name: str) -> str:
    if action_name == "exit_market":
        return "RL submitted a market exit for its open position."
    return "RL submitted or replaced a take-profit exit for its open position."


def _last_cancellation_status(cancellations: list[dict[str, Any]]) -> str | None:
    if not cancellations:
        return None
    status = cancellations[-1].get("broker_order_status")
    if isinstance(status, dict) and status.get("status") not in (None, ""):
        return str(status["status"])
    return None


def _serialize_for_json(payload: Any) -> Any:
    if isinstance(payload, Enum):
        return payload.value
    if isinstance(payload, Decimal):
        return str(payload)
    if isinstance(payload, datetime):
        return payload.isoformat()
    if isinstance(payload, date):
        return payload.isoformat()
    if isinstance(payload, tuple):
        return [_serialize_for_json(item) for item in payload]
    if isinstance(payload, list):
        return [_serialize_for_json(item) for item in payload]
    if isinstance(payload, dict):
        return {key: _serialize_for_json(value) for key, value in payload.items()}
    return payload
