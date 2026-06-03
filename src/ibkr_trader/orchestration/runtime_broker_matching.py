from __future__ import annotations

from decimal import Decimal
from typing import Any

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.ibkr.runtime_snapshot import BrokerPortfolioItem
from ibkr_trader.ibkr.runtime_snapshot import BrokerPosition
from ibkr_trader.ibkr.runtime_snapshot import BrokerRuntimeSnapshot
from ibkr_trader.orchestration.runtime_types import parse_decimal as _parse_decimal
from ibkr_trader.virtual.accounts import is_virtual_account_key


_CLOSED_BROKER_ORDER_STATUSES = {
    "API_CANCELLED",
    "CANCELLED",
    "ERROR",
    "FILLED",
    "INACTIVE",
    "NOT_FOUND_AT_BROKER",
    "REJECTED",
}


def _is_market_broker_order(order_type: str | None) -> bool:
    normalized = _normalize_broker_identity(order_type)
    return normalized in {"MKT", "MARKET"}


def _normalize_broker_order_status(status: str | None) -> str | None:
    if status is None:
        return None
    normalized = status.strip()
    if not normalized:
        return None
    return normalized.upper()


def _normalize_broker_identity(value: object | None) -> str | None:
    if value in (None, ""):
        return None
    normalized = " ".join(str(value).strip().upper().split())
    return normalized or None


def _is_open_broker_order_status(status: str | None) -> bool:
    return _normalize_broker_order_status(status) not in _CLOSED_BROKER_ORDER_STATUSES


def _exit_side_for_instruction(instruction: ExecutionInstruction) -> str:
    entry_side = str(instruction.intent.side).strip().upper()
    if entry_side == "BUY":
        return "SELL"
    if entry_side == "SELL":
        return "BUY"
    raise ValueError(f"Unsupported instruction side for exit cleanup: {entry_side!r}")


def _broker_account_candidates(
    broker_config: IbkrConnectionConfig,
    *,
    record: InstructionRecord,
    instruction: ExecutionInstruction,
) -> set[str]:
    return {
        normalized
        for normalized in (
            _normalize_broker_identity(broker_config.account_id),
            _normalize_broker_identity(record.account_key),
            _normalize_broker_identity(instruction.account.account_key),
        )
        if normalized is not None
    }


def _instrument_symbol_candidates(
    record: InstructionRecord,
    instruction: ExecutionInstruction,
) -> set[str]:
    values: list[object | None] = [
        record.symbol,
        instruction.instrument.symbol,
        *instruction.instrument.aliases,
    ]
    return {
        normalized
        for normalized in (_normalize_broker_identity(value) for value in values)
        if normalized is not None
    }


def _matches_optional_identity(
    observed: object | None,
    candidates: set[str],
    *,
    default_when_missing: bool,
) -> bool:
    normalized_observed = _normalize_broker_identity(observed)
    if normalized_observed is None:
        return default_when_missing
    if not candidates:
        return True
    return normalized_observed in candidates


def _matches_exit_cleanup_instrument(
    *,
    symbol: object | None,
    local_symbol: object | None,
    currency: object | None,
    security_type: object | None,
    record: InstructionRecord,
    instruction: ExecutionInstruction,
) -> bool:
    symbol_candidates = _instrument_symbol_candidates(record, instruction)
    observed_symbols = {
        normalized
        for normalized in (
            _normalize_broker_identity(symbol),
            _normalize_broker_identity(local_symbol),
        )
        if normalized is not None
    }
    if not observed_symbols or observed_symbols.isdisjoint(symbol_candidates):
        return False

    currency_candidates = {
        normalized
        for normalized in (
            _normalize_broker_identity(record.currency),
            _normalize_broker_identity(instruction.instrument.currency),
        )
        if normalized is not None
    }
    if not _matches_optional_identity(
        currency,
        currency_candidates,
        default_when_missing=True,
    ):
        return False

    security_type_candidates = {
        normalized
        for normalized in (
            _normalize_broker_identity(instruction.instrument.security_type),
        )
        if normalized is not None
    }
    return _matches_optional_identity(
        security_type,
        security_type_candidates,
        default_when_missing=True,
    )


def _sum_matching_broker_position_items(
    items: tuple[BrokerPosition, ...] | tuple[BrokerPortfolioItem, ...],
    broker_config: IbkrConnectionConfig,
    *,
    record: InstructionRecord,
    instruction: ExecutionInstruction,
) -> Decimal | None:
    account_candidates = _broker_account_candidates(
        broker_config,
        record=record,
        instruction=instruction,
    )
    quantity = Decimal("0")
    matched = False
    for item in items:
        if item.position is None:
            continue
        if not _matches_optional_identity(
            item.account,
            account_candidates,
            default_when_missing=False,
        ):
            continue
        if not _matches_exit_cleanup_instrument(
            symbol=item.symbol,
            local_symbol=item.local_symbol,
            currency=item.currency,
            security_type=item.security_type,
            record=record,
            instruction=instruction,
        ):
            continue
        quantity += item.position
        matched = True
    return quantity if matched else None


def _matching_broker_position_quantity(
    snapshot: BrokerRuntimeSnapshot,
    broker_config: IbkrConnectionConfig,
    *,
    record: InstructionRecord,
    instruction: ExecutionInstruction,
) -> Decimal | None:
    position_quantity = _sum_matching_broker_position_items(
        snapshot.positions,
        broker_config,
        record=record,
        instruction=instruction,
    )
    if position_quantity is not None:
        return position_quantity
    return _sum_matching_broker_position_items(
        snapshot.portfolio,
        broker_config,
        record=record,
        instruction=instruction,
    )


def _forced_exit_broker_position_block(
    snapshot: BrokerRuntimeSnapshot,
    broker_config: IbkrConnectionConfig,
    *,
    record: InstructionRecord,
    instruction: ExecutionInstruction,
    remaining_quantity: Decimal,
) -> dict[str, Any] | None:
    if record.is_virtual or is_virtual_account_key(instruction.account.account_key):
        return None

    observed_quantity = _matching_broker_position_quantity(
        snapshot,
        broker_config,
        record=record,
        instruction=instruction,
    )
    entry_side = str(instruction.intent.side).strip().upper()
    required_quantity = remaining_quantity.copy_abs()
    base_payload = {
        "instruction_id": record.instruction_id,
        "account_candidates": sorted(
            _broker_account_candidates(
                broker_config,
                record=record,
                instruction=instruction,
            )
        ),
        "symbol_candidates": sorted(_instrument_symbol_candidates(record, instruction)),
        "entry_side": entry_side,
        "required_quantity": str(required_quantity),
        "observed_quantity": (
            str(observed_quantity) if observed_quantity is not None else None
        ),
        "position_source_counts": {
            "positions": len(snapshot.positions),
            "portfolio": len(snapshot.portfolio),
        },
    }
    if observed_quantity is None:
        return {
            **base_payload,
            "reason": "missing_broker_position",
            "message": (
                "Skipped forced exit because no matching live broker position "
                "was present in the broker snapshot."
            ),
        }
    if entry_side == "BUY" and observed_quantity < required_quantity:
        return {
            **base_payload,
            "reason": "insufficient_long_broker_position",
            "message": (
                "Skipped forced exit because the live broker long position was "
                "smaller than the remaining instruction quantity."
            ),
        }
    if entry_side == "SELL" and observed_quantity > -required_quantity:
        return {
            **base_payload,
            "reason": "insufficient_short_broker_position",
            "message": (
                "Skipped forced exit because the live broker short position was "
                "smaller than the remaining instruction quantity."
            ),
        }
    if entry_side not in {"BUY", "SELL"}:
        return {
            **base_payload,
            "reason": "unsupported_entry_side",
            "message": "Skipped forced exit because the instruction side is unsupported.",
        }
    return None
