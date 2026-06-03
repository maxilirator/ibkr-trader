from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.models import InstructionEventRecord, InstructionRecord
from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.domain.execution_payloads import parse_execution_instruction_payload
from ibkr_trader.ledger.persistence import BROKER_KIND_IBKR, persist_broker_callback_events
from ibkr_trader.orchestration.runtime_types import RuntimeCycleIssue
from ibkr_trader.orchestration.runtime_types import serialize_for_json as _serialize_for_json


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
