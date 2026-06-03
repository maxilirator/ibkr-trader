from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.orchestration.scheduling import resolve_scheduled_submission_due_at


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
