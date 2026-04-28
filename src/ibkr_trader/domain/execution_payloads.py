from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import math
from typing import Any, Mapping, Sequence

from ibkr_trader.domain.execution_contract import (
    AccountRef,
    DelayedExitReference,
    DelayedLimitExitSpec,
    EntrySpec,
    ExecutionMode,
    ExecutionInstruction,
    ExecutionInstructionBatch,
    ExecutionWindow,
    ExitSpec,
    FundingBasis,
    InstrumentRef,
    IntentSpec,
    ModelRoutedExecutionSpec,
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


def parse_metadata_map(value: Any, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")

    parsed: dict[str, Any] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise ValueError(f"{field_name} keys must be strings")
        parsed[key] = parse_metadata_value(item, f"{field_name}.{key}")
    return parsed


def parse_metadata_value(value: Any, field_name: str) -> Any:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)):
            raise ValueError(f"{field_name} must be finite")
        return value
    if isinstance(value, Mapping):
        return parse_metadata_map(value, field_name)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [
            parse_metadata_value(item, f"{field_name}[{idx}]")
            for idx, item in enumerate(value)
        ]
    raise ValueError(
        f"{field_name} must be a JSON-compatible value"
    )


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _parse_execution_window(payload: Mapping[str, Any]) -> ExecutionWindow:
    window_payload = payload.get("window")
    if not isinstance(window_payload, Mapping):
        raise ValueError("execution.window must be an object")

    start_value = (
        window_payload.get("start_at")
        or window_payload.get("submit_at")
        or window_payload.get("valid_from")
    )
    end_value = (
        window_payload.get("end_at")
        or window_payload.get("expire_at")
        or window_payload.get("valid_until")
    )
    if start_value is None:
        raise ValueError("execution.window.start_at is required")
    if end_value is None:
        raise ValueError("execution.window.end_at is required")

    return ExecutionWindow(
        start_at=parse_datetime(start_value, "execution.window.start_at"),
        end_at=parse_datetime(end_value, "execution.window.end_at"),
    )


def _parse_model_routed_execution(
    payload: Mapping[str, Any],
) -> ModelRoutedExecutionSpec | None:
    execution_payload = payload.get("execution")
    if execution_payload is None:
        return None
    if not isinstance(execution_payload, Mapping):
        raise ValueError("execution must be an object")

    mode = str(execution_payload.get("mode", ExecutionMode.DETERMINISTIC.value)).lower()
    if mode != ExecutionMode.MODEL_ROUTED.value:
        return None

    model_payload = execution_payload.get("model")
    if model_payload is not None and not isinstance(model_payload, Mapping):
        model_payload = {"model_id": model_payload}
    model_payload = model_payload or {}
    root_model_payload = payload.get("model")
    if root_model_payload is not None and not isinstance(root_model_payload, Mapping):
        root_model_payload = {"model_id": root_model_payload}
    root_model_payload = root_model_payload or {}

    model_id = (
        _string_or_none(execution_payload.get("model_id"))
        or _string_or_none(execution_payload.get("model_key"))
        or _string_or_none(model_payload.get("model_id"))
        or _string_or_none(model_payload.get("model_key"))
        or _string_or_none(model_payload.get("id"))
        or _string_or_none(payload.get("model_id"))
        or _string_or_none(payload.get("model_key"))
        or _string_or_none(root_model_payload.get("model_id"))
        or _string_or_none(root_model_payload.get("model_key"))
        or _string_or_none(root_model_payload.get("id"))
    )
    if model_id is None:
        raise ValueError("execution.model_id is required")

    return ModelRoutedExecutionSpec(
        mode=ExecutionMode.MODEL_ROUTED,
        model_id=model_id,
        model_family=(
            _string_or_none(execution_payload.get("model_family"))
            or _string_or_none(model_payload.get("model_family"))
            or _string_or_none(model_payload.get("family"))
            or _string_or_none(payload.get("model_family"))
            or _string_or_none(root_model_payload.get("model_family"))
            or _string_or_none(root_model_payload.get("family"))
        ),
        model_version=(
            _string_or_none(execution_payload.get("model_version"))
            or _string_or_none(model_payload.get("model_version"))
            or _string_or_none(model_payload.get("version"))
            or _string_or_none(payload.get("model_version"))
            or _string_or_none(root_model_payload.get("model_version"))
            or _string_or_none(root_model_payload.get("version"))
        ),
        model_artifact_id=(
            _string_or_none(execution_payload.get("model_artifact_id"))
            or _string_or_none(model_payload.get("model_artifact_id"))
            or _string_or_none(model_payload.get("artifact_id"))
            or _string_or_none(payload.get("model_artifact_id"))
            or _string_or_none(root_model_payload.get("model_artifact_id"))
            or _string_or_none(root_model_payload.get("artifact_id"))
        ),
        window=_parse_execution_window(execution_payload),
    )


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

    model_routed_execution = _parse_model_routed_execution(payload)

    entry_payload = payload.get("entry")
    if entry_payload is not None and not isinstance(entry_payload, Mapping):
        raise ValueError("entry must be an object")
    if entry_payload is None and model_routed_execution is None:
        raise ValueError("entry must be an object")

    exit_payload = payload.get("exit")
    if exit_payload is not None and not isinstance(exit_payload, Mapping):
        raise ValueError("exit must be an object")
    if exit_payload is None and model_routed_execution is None:
        raise ValueError("exit must be an object")

    trace_payload = payload.get("trace", {})
    if not isinstance(trace_payload, Mapping):
        raise ValueError("trace must be an object")

    delayed_limit_payload = (
        exit_payload.get("delayed_limit") if exit_payload is not None else None
    )
    if delayed_limit_payload is not None and not isinstance(delayed_limit_payload, Mapping):
        raise ValueError("exit.delayed_limit must be an object")

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
            funding_basis=(
                FundingBasis(str(sizing_payload["funding_basis"]).lower())
                if sizing_payload.get("funding_basis") is not None
                else None
            ),
            allow_leverage=bool(sizing_payload.get("allow_leverage", False)),
        ),
        entry=(
            EntrySpec(
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
            )
            if entry_payload is not None
            else None
        ),
        exit=(
            ExitSpec(
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
                delayed_limit=(
                    DelayedLimitExitSpec(
                        submit_at=parse_datetime(
                            delayed_limit_payload["submit_at"],
                            "exit.delayed_limit.submit_at",
                        ),
                        limit_offset_pct=parse_decimal(
                            delayed_limit_payload["limit_offset_pct"],
                            "exit.delayed_limit.limit_offset_pct",
                        ),
                        reference=DelayedExitReference(
                            str(
                                delayed_limit_payload.get(
                                    "reference",
                                    DelayedExitReference.MARKET_AT_TRIGGER.value,
                                )
                            ).upper()
                        ),
                    )
                    if delayed_limit_payload is not None
                    else None
                ),
                force_exit_next_session_open=bool(
                    exit_payload.get("force_exit_next_session_open", False)
                ),
            )
            if exit_payload is not None
            else None
        ),
        trace=TraceSpec(
            reason_code=(
                str(trace_payload["reason_code"])
                if trace_payload.get("reason_code") is not None
                else "model_routed_selection"
                if model_routed_execution is not None
                else str(trace_payload["reason_code"])
            ),
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
            metadata=parse_metadata_map(trace_payload.get("metadata"), "trace.metadata"),
        ),
        execution=model_routed_execution,
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
