from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from ibkr_trader.domain.execution_contract import (
    AccountRef,
    EntrySpec,
    ExecutionInstruction,
    ExecutionInstructionBatch,
    ExitSpec,
    InstrumentRef,
    IntentSpec,
    OrderType,
    PositionSide,
    SecurityType,
    SizingMode,
    SizingSpec,
    SourceContext,
    TimeInForce,
    TraceSpec,
)


def parse_decimal(value: Any, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be a valid decimal-compatible value") from exc


def parse_datetime(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO-8601 string with timezone")

    normalized_value = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized_value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid ISO-8601 datetime") from exc

    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include timezone information")
    return parsed


def parse_date(value: Any, field_name: str) -> date:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO-8601 date string")

    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid ISO-8601 date") from exc


def parse_string_map(value: Any, field_name: str) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a string-to-string object")

    parsed: dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not isinstance(item, str):
            raise ValueError(f"{field_name} must be a string-to-string object")
        parsed[key] = item
    return parsed


def parse_execution_instruction_payload(payload: Mapping[str, Any]) -> ExecutionInstruction:
    account_payload = payload.get("account")
    if not isinstance(account_payload, Mapping):
        raise ValueError("account must be an object")

    instrument_payload = payload.get("instrument")
    if not isinstance(instrument_payload, Mapping):
        raise ValueError("instrument must be an object")

    intent_payload = payload.get("intent")
    if not isinstance(intent_payload, Mapping):
        raise ValueError("intent must be an object")

    sizing_payload = payload.get("sizing")
    if not isinstance(sizing_payload, Mapping):
        raise ValueError("sizing must be an object")

    entry_payload = payload.get("entry")
    if not isinstance(entry_payload, Mapping):
        raise ValueError("entry must be an object")

    exit_payload = payload.get("exit")
    if not isinstance(exit_payload, Mapping):
        raise ValueError("exit must be an object")

    trace_payload = payload.get("trace", {})
    if not isinstance(trace_payload, Mapping):
        raise ValueError("trace must be an object")

    instruction = ExecutionInstruction(
        instruction_id=str(payload["instruction_id"]),
        account=AccountRef(
            account_key=str(account_payload["account_key"]),
            book_key=str(account_payload["book_key"]),
            book_role=(
                str(account_payload["book_role"])
                if account_payload.get("book_role") is not None
                else None
            ),
            book_side=(
                PositionSide(str(account_payload["book_side"]).upper())
                if account_payload.get("book_side") is not None
                else None
            ),
        ),
        instrument=InstrumentRef(
            symbol=str(instrument_payload["symbol"]).upper(),
            exchange=str(instrument_payload["exchange"]).upper(),
            currency=str(instrument_payload["currency"]).upper(),
            security_type=SecurityType(
                str(instrument_payload.get("security_type", "STK")).upper()
            ),
            isin=(
                str(instrument_payload["isin"])
                if instrument_payload.get("isin") is not None
                else None
            ),
            primary_exchange=(
                str(instrument_payload["primary_exchange"]).upper()
                if instrument_payload.get("primary_exchange") is not None
                else None
            ),
            aliases=tuple(
                str(alias) for alias in instrument_payload.get("aliases", ())
            ),
        ),
        intent=IntentSpec(
            side=str(intent_payload["side"]).upper(),
            position_side=PositionSide(str(intent_payload["position_side"]).upper()),
        ),
        sizing=SizingSpec(
            mode=SizingMode(str(sizing_payload["mode"])),
            target_fraction_of_account=(
                parse_decimal(
                    sizing_payload["target_fraction_of_account"],
                    "sizing.target_fraction_of_account",
                )
                if sizing_payload.get("target_fraction_of_account") is not None
                else None
            ),
            target_notional=(
                parse_decimal(sizing_payload["target_notional"], "sizing.target_notional")
                if sizing_payload.get("target_notional") is not None
                else None
            ),
            target_quantity=(
                parse_decimal(sizing_payload["target_quantity"], "sizing.target_quantity")
                if sizing_payload.get("target_quantity") is not None
                else None
            ),
        ),
        entry=EntrySpec(
            order_type=OrderType(str(entry_payload["order_type"]).upper()),
            submit_at=parse_datetime(entry_payload["submit_at"], "entry.submit_at"),
            expire_at=parse_datetime(entry_payload["expire_at"], "entry.expire_at"),
            limit_price=(
                parse_decimal(entry_payload["limit_price"], "entry.limit_price")
                if entry_payload.get("limit_price") is not None
                else None
            ),
            time_in_force=TimeInForce(
                str(entry_payload.get("time_in_force", "DAY")).upper()
            ),
            max_submit_count=int(entry_payload.get("max_submit_count", 1)),
            cancel_unfilled_at_expiry=bool(
                entry_payload.get("cancel_unfilled_at_expiry", True)
            ),
        ),
        exit=ExitSpec(
            take_profit_pct=(
                parse_decimal(exit_payload["take_profit_pct"], "exit.take_profit_pct")
                if exit_payload.get("take_profit_pct") is not None
                else None
            ),
            stop_loss_pct=(
                parse_decimal(exit_payload["stop_loss_pct"], "exit.stop_loss_pct")
                if exit_payload.get("stop_loss_pct") is not None
                else None
            ),
            catastrophic_stop_loss_pct=(
                parse_decimal(
                    exit_payload["catastrophic_stop_loss_pct"],
                    "exit.catastrophic_stop_loss_pct",
                )
                if exit_payload.get("catastrophic_stop_loss_pct") is not None
                else None
            ),
            force_exit_next_session_open=bool(
                exit_payload.get("force_exit_next_session_open", False)
            ),
        ),
        trace=TraceSpec(
            reason_code=str(trace_payload["reason_code"]),
            execution_policy=(
                str(trace_payload["execution_policy"])
                if trace_payload.get("execution_policy") is not None
                else None
            ),
            trade_date=(
                parse_date(trace_payload["trade_date"], "trace.trade_date")
                if trace_payload.get("trade_date") is not None
                else None
            ),
            data_cutoff_date=(
                parse_date(trace_payload["data_cutoff_date"], "trace.data_cutoff_date")
                if trace_payload.get("data_cutoff_date") is not None
                else None
            ),
            company_name=(
                str(trace_payload["company_name"])
                if trace_payload.get("company_name") is not None
                else None
            ),
            metadata=parse_string_map(trace_payload.get("metadata"), "trace.metadata"),
        ),
    )
    instruction.validate()
    return instruction


def parse_execution_batch_payload(payload: Mapping[str, Any]) -> ExecutionInstructionBatch:
    source_payload = payload.get("source")
    if not isinstance(source_payload, Mapping):
        raise ValueError("source must be an object")

    instructions_payload = payload.get("instructions")
    if not isinstance(instructions_payload, list):
        raise ValueError("instructions must be an array")

    batch = ExecutionInstructionBatch(
        schema_version=str(payload["schema_version"]),
        source=SourceContext(
            system=str(source_payload["system"]),
            batch_id=str(source_payload["batch_id"]),
            generated_at=parse_datetime(source_payload["generated_at"], "source.generated_at"),
            release_id=(
                str(source_payload["release_id"])
                if source_payload.get("release_id") is not None
                else None
            ),
            strategy_id=(
                str(source_payload["strategy_id"])
                if source_payload.get("strategy_id") is not None
                else None
            ),
            policy_id=(
                str(source_payload["policy_id"])
                if source_payload.get("policy_id") is not None
                else None
            ),
        ),
        instructions=tuple(
            parse_execution_instruction_payload(item) for item in instructions_payload
        ),
    )
    batch.validate()
    return batch
