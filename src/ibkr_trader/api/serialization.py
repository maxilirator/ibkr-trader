from __future__ import annotations

from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from ibkr_trader.domain.execution_contract import ExecutionInstructionBatch


def serialize_for_json(payload: Any) -> Any:
    if isinstance(payload, Enum):
        return payload.value
    if isinstance(payload, Decimal):
        return str(payload)
    if isinstance(payload, datetime):
        return payload.isoformat()
    if isinstance(payload, date):
        return payload.isoformat()
    if isinstance(payload, list):
        return [serialize_for_json(item) for item in payload]
    if isinstance(payload, tuple):
        return [serialize_for_json(item) for item in payload]
    if isinstance(payload, dict):
        return {key: serialize_for_json(value) for key, value in payload.items()}
    return payload


def serialize_execution_batch(batch: ExecutionInstructionBatch) -> dict[str, Any]:
    return serialize_for_json(asdict(batch))
