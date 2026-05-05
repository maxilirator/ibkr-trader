from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from decimal import ROUND_HALF_UP
from enum import Enum
from typing import Any

from ibkr_trader.domain.execution_contract import AccountRef
from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.domain.execution_contract import ExecutionInstructionBatch
from ibkr_trader.domain.execution_contract import PositionSide


ENTRY_PENDING = "ENTRY_PENDING"
EXIT_PENDING = "EXIT_PENDING"
FLAT = "FLAT"
LONG_OPEN = "LONG_OPEN"
SHORT_OPEN = "SHORT_OPEN"

ACTION_STATUS_LOGGED = "logged"
ACTION_STATUS_TRANSLATED = "translated"
ACTION_STATUS_EXECUTED = "executed"
ACTION_STATUS_INVALID = "invalid_action"

_ENTRY_PREVCLOSE_RE = re.compile(r"^entry_prevclose_(-?\d+)bp$")
_EXIT_TP_RE = re.compile(r"^exit_tp_(\d+)bp$")
_PRICE_QUANTUM = Decimal("0.0001")


@dataclass(frozen=True, slots=True)
class RLActionTranslation:
    deployment_key: str
    action_name: str
    action_status: str
    state_before: str
    state_after: str
    instruction_payload: dict[str, Any] | None = None
    note: str | None = None


def translate_rl_action(
    source_batch: ExecutionInstructionBatch,
    source_instruction: ExecutionInstruction,
    *,
    deployment_key: str,
    action_name: str,
    state_before: str,
    observed_at: datetime,
    previous_close: Decimal | None = None,
    decision_id: str | None = None,
) -> RLActionTranslation:
    """Translate one RL action into the normal deterministic instruction contract.

    The translator is intentionally strict. Entry actions with clear long/short market
    meaning produce normal instructions. Actions that mutate an existing broker order
    or position return a translated action without an instruction payload; the API
    layer must execute those against the durable RL-owned instruction record.
    """

    source_batch.validate()
    source_instruction.validate()
    if not source_instruction.is_model_routed:
        raise ValueError("source_instruction must be model-routed")
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must include timezone information")

    side = source_instruction.intent.position_side
    normalized_state = state_before.upper()

    if action_name in {"skip", "wait"}:
        if normalized_state not in {FLAT, ENTRY_PENDING, SHORT_OPEN, LONG_OPEN, EXIT_PENDING}:
            return _invalid(
                deployment_key,
                action_name,
                state_before,
                note=f"{action_name} is not valid from state {state_before}",
            )
        return RLActionTranslation(
            deployment_key=deployment_key,
            action_name=action_name,
            action_status=ACTION_STATUS_LOGGED,
            state_before=state_before,
            state_after=state_before,
            note=f"{action_name} logged; no market instruction generated.",
        )

    if normalized_state == FLAT and action_name == "market_entry":
        payload = _build_entry_payload(
            source_batch,
            source_instruction,
            deployment_key=deployment_key,
            action_name=action_name,
            observed_at=observed_at,
            decision_id=decision_id,
            order_type="MARKET",
            limit_price=None,
            previous_close=previous_close,
        )
        return RLActionTranslation(
            deployment_key=deployment_key,
            action_name=action_name,
            action_status=ACTION_STATUS_TRANSLATED,
            state_before=state_before,
            state_after=ENTRY_PENDING,
            instruction_payload=payload,
            note="Market entry translated to normal instruction contract.",
        )

    entry_match = _ENTRY_PREVCLOSE_RE.match(action_name)
    if normalized_state == FLAT and entry_match is not None:
        if previous_close is None:
            return _invalid(
                deployment_key,
                action_name,
                state_before,
                note="previous_close is required for entry_prevclose actions",
            )
        basis_points = Decimal(entry_match.group(1))
        if side is PositionSide.LONG and basis_points >= 0:
            return _invalid(
                deployment_key,
                action_name,
                state_before,
                note="long prev-close entry must use a negative basis-point offset",
            )
        if side is PositionSide.SHORT and basis_points <= 0:
            return _invalid(
                deployment_key,
                action_name,
                state_before,
                note="short prev-close entry must use a positive basis-point offset",
            )
        limit_price = _round_price(previous_close * (Decimal("1") + basis_points / Decimal("10000")))
        payload = _build_entry_payload(
            source_batch,
            source_instruction,
            deployment_key=deployment_key,
            action_name=action_name,
            observed_at=observed_at,
            decision_id=decision_id,
            order_type="LIMIT",
            limit_price=limit_price,
            previous_close=previous_close,
        )
        return RLActionTranslation(
            deployment_key=deployment_key,
            action_name=action_name,
            action_status=ACTION_STATUS_TRANSLATED,
            state_before=state_before,
            state_after=ENTRY_PENDING,
            instruction_payload=payload,
            note="Prev-close entry translated to normal instruction contract.",
        )

    if normalized_state == FLAT and action_name.startswith("entry_prevclose_"):
        return _invalid(
            deployment_key,
            action_name,
            state_before,
            note="entry_prevclose action name must use entry_prevclose_<signed_bp>bp",
        )

    if normalized_state == ENTRY_PENDING and (
        action_name == "market_entry" or entry_match is not None
    ):
        return RLActionTranslation(
            deployment_key=deployment_key,
            action_name=action_name,
            action_status=ACTION_STATUS_LOGGED,
            state_before=state_before,
            state_after=ENTRY_PENDING,
            note=(
                "Entry action observed while an entry is already pending; "
                "maintaining the existing pending entry without submitting a duplicate."
            ),
        )

    if normalized_state == ENTRY_PENDING and action_name == "cancel_entry":
        return RLActionTranslation(
            deployment_key=deployment_key,
            action_name=action_name,
            action_status=ACTION_STATUS_TRANSLATED,
            state_before=state_before,
            state_after=FLAT,
            note="Entry cancellation will target the durable RL-owned pending entry.",
        )

    if (
        _is_open_state_for_side(normalized_state, side)
        or normalized_state == EXIT_PENDING
    ) and action_name == "exit_market":
        return RLActionTranslation(
            deployment_key=deployment_key,
            action_name=action_name,
            action_status=ACTION_STATUS_TRANSLATED,
            state_before=state_before,
            state_after=EXIT_PENDING,
            note="Market exit will target the durable RL-owned open position.",
        )

    exit_match = _EXIT_TP_RE.match(action_name)
    if (
        _is_open_state_for_side(normalized_state, side)
        or normalized_state == EXIT_PENDING
    ) and exit_match is not None:
        basis_points = Decimal(exit_match.group(1))
        if side is PositionSide.LONG and basis_points != Decimal("200"):
            return _invalid(
                deployment_key,
                action_name,
                state_before,
                note="long take-profit action must be exit_tp_200bp",
            )
        if side is PositionSide.SHORT and basis_points != Decimal("180"):
            return _invalid(
                deployment_key,
                action_name,
                state_before,
                note="short take-profit action must be exit_tp_180bp",
            )
        return RLActionTranslation(
            deployment_key=deployment_key,
            action_name=action_name,
            action_status=ACTION_STATUS_TRANSLATED,
            state_before=state_before,
            state_after=EXIT_PENDING,
            note="Take-profit exit will target the durable RL-owned open position.",
        )

    if normalized_state == EXIT_PENDING and action_name == "clear_exit":
        open_state = LONG_OPEN if side is PositionSide.LONG else SHORT_OPEN
        return RLActionTranslation(
            deployment_key=deployment_key,
            action_name=action_name,
            action_status=ACTION_STATUS_TRANSLATED,
            state_before=state_before,
            state_after=open_state,
            note="Exit cancellation will target the durable RL-owned pending exit.",
        )

    return _invalid(
        deployment_key,
        action_name,
        state_before,
        note=f"{action_name} is not allowed from state {state_before} for {side.value}",
    )


def _invalid(
    deployment_key: str,
    action_name: str,
    state_before: str,
    *,
    note: str,
) -> RLActionTranslation:
    return RLActionTranslation(
        deployment_key=deployment_key,
        action_name=action_name,
        action_status=ACTION_STATUS_INVALID,
        state_before=state_before,
        state_after=state_before,
        note=note,
    )


def _is_open_state_for_side(state: str, side: PositionSide) -> bool:
    return (side is PositionSide.LONG and state == LONG_OPEN) or (
        side is PositionSide.SHORT and state == SHORT_OPEN
    )


def _build_entry_payload(
    source_batch: ExecutionInstructionBatch,
    source_instruction: ExecutionInstruction,
    *,
    deployment_key: str,
    action_name: str,
    observed_at: datetime,
    decision_id: str | None,
    order_type: str,
    limit_price: Decimal | None,
    previous_close: Decimal | None,
) -> dict[str, Any]:
    if source_instruction.execution is None:
        raise ValueError("source_instruction.execution is required")

    submit_at = max(observed_at, source_instruction.execution.window.start_at)
    expire_at = source_instruction.execution.window.end_at
    if expire_at <= submit_at:
        raise ValueError("source execution window has already expired")

    deterministic_instruction_id = _stable_id(
        "rl",
        deployment_key,
        source_instruction.instruction_id,
        source_instruction.instrument.symbol,
        action_name,
        decision_id or observed_at.isoformat(),
    )
    batch_id = _stable_id(
        "rl-batch",
        deployment_key,
        deterministic_instruction_id,
    )

    account = _serialize_account(source_instruction.account)
    account["book_side"] = source_instruction.intent.position_side.value

    metadata = dict(source_instruction.trace.metadata)
    metadata.update(
        {
            "rl_action_name": action_name,
            "rl_deployment_key": deployment_key,
            "rl_source_instruction_id": source_instruction.instruction_id,
            "rl_source_batch_id": source_batch.source.batch_id,
            "rl_decision_id": decision_id or observed_at.isoformat(),
        }
    )
    if previous_close is not None:
        metadata["previous_close"] = str(previous_close)
    if limit_price is not None:
        metadata["entry_limit_price"] = str(limit_price)

    entry: dict[str, Any] = {
        "order_type": order_type,
        "submit_at": submit_at.isoformat(),
        "expire_at": expire_at.isoformat(),
        "time_in_force": "DAY",
        "max_submit_count": 1,
        "cancel_unfilled_at_expiry": True,
    }
    if limit_price is not None:
        entry["limit_price"] = str(limit_price)

    return {
        "schema_version": "2026-04-10",
        "source": {
            "system": "rl-runner",
            "batch_id": batch_id,
            "generated_at": observed_at.isoformat(),
            "release_id": source_batch.source.release_id,
            "strategy_id": deployment_key,
            "policy_id": source_instruction.execution.model_id,
        },
        "instructions": [
            {
                "instruction_id": deterministic_instruction_id,
                "account": account,
                "instrument": _serialize_ref(source_instruction.instrument),
                "intent": {
                    "side": _entry_side(source_instruction.intent.position_side),
                    "position_side": source_instruction.intent.position_side.value,
                },
                "sizing": _serialize_ref(source_instruction.sizing),
                "entry": entry,
                "exit": {
                    "force_exit_next_session_open": True,
                },
                "trace": {
                    "reason_code": "rl_action_translated",
                    "execution_policy": source_instruction.trace.execution_policy,
                    "trade_date": _serialize_value(source_instruction.trace.trade_date),
                    "data_cutoff_date": _serialize_value(
                        source_instruction.trace.data_cutoff_date
                    ),
                    "company_name": source_instruction.trace.company_name,
                    "metadata": metadata,
                },
            }
        ],
    }


def _entry_side(position_side: PositionSide) -> str:
    if position_side is PositionSide.LONG:
        return "BUY"
    if position_side is PositionSide.SHORT:
        return "SELL"
    raise ValueError(f"unsupported position_side: {position_side}")


def _serialize_account(account: AccountRef) -> dict[str, Any]:
    payload = _serialize_ref(account)
    return {key: value for key, value in payload.items() if value is not None}


def _serialize_ref(value: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field_name in value.__dataclass_fields__:
        field_value = getattr(value, field_name)
        serialized = _serialize_value(field_value)
        if serialized is not None:
            result[field_name] = serialized
    return result


def _serialize_value(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_serialize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize_value(item) for key, item in value.items()}
    return value


def _round_price(value: Decimal) -> Decimal:
    return value.quantize(_PRICE_QUANTUM, rounding=ROUND_HALF_UP)


def _stable_id(*parts: str) -> str:
    raw = "|".join(parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "-", "-".join(parts)).strip("-").lower()
    prefix = cleaned[:108].strip("-")
    return f"{prefix}-{digest}"
