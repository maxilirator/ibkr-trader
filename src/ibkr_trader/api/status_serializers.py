from __future__ import annotations

from dataclasses import asdict
from typing import Any

from ibkr_trader.api.serialization import serialize_for_json
from ibkr_trader.config import AppConfig
from ibkr_trader.domain.execution_payloads import parse_execution_instruction_payload
from ibkr_trader.orchestration.instruction_status import serialize_instruction_status
from ibkr_trader.orchestration.scheduling import build_instruction_runtime_schedule


def serialize_rl_candidate_status(payload: Any) -> dict[str, Any]:
    serialized_instruction = serialize_instruction_status(payload)
    stored_payload = serialized_instruction.get("payload", {})
    stored_instruction = (
        stored_payload.get("instruction", {})
        if isinstance(stored_payload, dict)
        else {}
    )
    execution = (
        stored_instruction.get("execution", {})
        if isinstance(stored_instruction, dict)
        else {}
    )
    model_id = None
    if isinstance(execution, dict):
        model_id = execution.get("model_id")
    if model_id is None and isinstance(stored_instruction, dict):
        model_id = stored_instruction.get("model")

    return serialize_for_json({
        "candidate_id": payload.instruction_id,
        "instruction_id": payload.instruction_id,
        "state": payload.state,
        "account_key": payload.account_key,
        "book_key": payload.book_key,
        "is_virtual": payload.is_virtual,
        "symbol": payload.symbol,
        "exchange": payload.exchange,
        "currency": payload.currency,
        "side": payload.side,
        "model_id": model_id,
        "model_family": (
            execution.get("model_family") if isinstance(execution, dict) else None
        ),
        "model_version": (
            execution.get("model_version") if isinstance(execution, dict) else None
        ),
        "model_artifact_id": (
            execution.get("model_artifact_id") if isinstance(execution, dict) else None
        ),
        "execution_window": (
            execution.get("window") if isinstance(execution, dict) else None
        ),
        "sizing": (
            stored_instruction.get("sizing", {})
            if isinstance(stored_instruction, dict)
            else {}
        ),
        "trace": (
            stored_instruction.get("trace", {})
            if isinstance(stored_instruction, dict)
            else {}
        ),
        "source": (
            stored_payload.get("source", {})
            if isinstance(stored_payload, dict)
            else {}
        ),
        "updated_at": payload.updated_at,
        "candidate": serialized_instruction,
    })


def serialize_runtime_schedule_preview(payload: Any) -> dict[str, Any]:
    serialized = asdict(payload)
    return serialize_for_json(serialized)


def serialize_operator_instruction_status(payload: Any, app_config: AppConfig) -> dict[str, Any]:
    serialized_instruction = serialize_instruction_status(payload)
    raw_instruction = (
        payload.payload.get("instruction")
        if isinstance(payload.payload, dict)
        else None
    )
    if not isinstance(raw_instruction, dict):
        serialized_instruction["runtime_schedule_error"] = (
            "persisted payload does not contain instruction object"
        )
        return serialized_instruction

    try:
        instruction = parse_execution_instruction_payload(raw_instruction)
        schedule = build_instruction_runtime_schedule(
            instruction,
            runtime_timezone=app_config.timezone,
            session_calendar_path=app_config.session_calendar_path,
        )
    except (KeyError, ValueError) as exc:
        serialized_instruction["runtime_schedule_error"] = str(exc)
    else:
        serialized_instruction["runtime_schedule"] = serialize_runtime_schedule_preview(
            schedule
        )
    return serialized_instruction


def serialize_submitted_batch(payload: Any) -> dict[str, Any]:
    serialized = asdict(payload)
    return serialize_for_json(serialized)


def broker_exception_detail(exc: Exception) -> str | dict[str, Any]:
    wire_audit = getattr(exc, "ibkr_wire_audit", None)
    if isinstance(wire_audit, list):
        return {
            "message": str(exc),
            "ibkr_wire_audit": serialize_for_json(wire_audit),
        }
    return str(exc)
