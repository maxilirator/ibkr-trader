from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from ibkr_trader.api.payload_fields import _parse_json_object_field
from ibkr_trader.api.payload_fields import _parse_required_string
from ibkr_trader.domain.execution_payloads import parse_datetime
from ibkr_trader.domain.execution_payloads import parse_decimal
from ibkr_trader.virtual.accounts import normalize_virtual_account_key


def parse_virtual_account_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    account_key = normalize_virtual_account_key(str(payload.get("account_key", "")))
    base_currency = _parse_required_string(
        payload,
        "base_currency",
        normalize=lambda value: value.upper(),
    )
    return {
        "account_key": account_key,
        "base_currency": base_currency,
        "account_label": (
            str(payload["account_label"]).strip()
            if payload.get("account_label") is not None
            else None
        ),
        "cash_balance": (
            parse_decimal(payload["cash_balance"], "cash_balance")
            if payload.get("cash_balance") is not None
            else None
        ),
    }


def parse_virtual_market_quote_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    observed_at = parse_datetime(
        _parse_required_string(payload, "observed_at"),
        "observed_at",
    )
    parsed_prices = {
        "bid_price": (
            parse_decimal(payload["bid_price"], "bid_price")
            if payload.get("bid_price") is not None
            else None
        ),
        "ask_price": (
            parse_decimal(payload["ask_price"], "ask_price")
            if payload.get("ask_price") is not None
            else None
        ),
        "last_price": (
            parse_decimal(payload["last_price"], "last_price")
            if payload.get("last_price") is not None
            else None
        ),
        "midpoint_price": (
            parse_decimal(payload["midpoint_price"], "midpoint_price")
            if payload.get("midpoint_price") is not None
            else None
        ),
    }
    if all(value is None for value in parsed_prices.values()):
        raise ValueError(
            "At least one of bid_price, ask_price, last_price, or midpoint_price is required"
        )
    symbol = _parse_required_string(
        payload,
        "symbol",
        normalize=lambda value: value.upper(),
    )
    exchange = _parse_required_string(
        payload,
        "exchange",
        normalize=lambda value: value.upper(),
    )
    currency = _parse_required_string(
        payload,
        "currency",
        normalize=lambda value: value.upper(),
    )
    security_type = _parse_required_string(
        payload,
        "security_type",
        normalize=lambda value: value.upper(),
    )

    return {
        "account_key": normalize_virtual_account_key(
            str(payload.get("account_key", ""))
        ),
        "symbol": symbol,
        "exchange": exchange,
        "currency": currency,
        "security_type": security_type,
        "observed_at": observed_at,
        "primary_exchange": (
            str(payload["primary_exchange"]).strip().upper()
            if payload.get("primary_exchange") is not None
            else None
        ),
        "local_symbol": (
            str(payload["local_symbol"]).strip()
            if payload.get("local_symbol") is not None
            else None
        ),
        **parsed_prices,
        "source": str(payload["source"]).strip() if payload.get("source") is not None else None,
        "raw_payload": dict(payload),
        "metadata": _parse_json_object_field(payload, "metadata"),
    }


def parse_runtime_cycle_payload(
    payload: Mapping[str, Any],
) -> tuple[datetime | None, int, tuple[str, ...] | None]:
    now_at = (
        parse_datetime(payload["now_at"], "now_at")
        if payload.get("now_at") is not None
        else None
    )
    timeout = int(payload.get("timeout", 10))
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    raw_instruction_ids = payload.get("instruction_ids")
    instruction_ids: tuple[str, ...] | None = None
    if raw_instruction_ids is not None:
        if not isinstance(raw_instruction_ids, list) or not raw_instruction_ids:
            raise ValueError("instruction_ids must be a non-empty array of strings")
        parsed_instruction_ids = tuple(str(item).strip() for item in raw_instruction_ids)
        if not all(parsed_instruction_ids):
            raise ValueError("instruction_ids must contain only non-empty strings")
        if len(set(parsed_instruction_ids)) != len(parsed_instruction_ids):
            raise ValueError("instruction_ids must not contain duplicates")
        instruction_ids = parsed_instruction_ids
    return now_at, timeout, instruction_ids


def parse_kill_switch_payload(payload: Mapping[str, Any]) -> tuple[bool, str | None, str]:
    if "enabled" not in payload:
        raise ValueError("enabled is required")
    enabled = payload["enabled"]
    if not isinstance(enabled, bool):
        raise ValueError("enabled must be a boolean")

    reason = payload.get("reason")
    if reason is not None:
        reason = str(reason).strip()
        if not reason:
            reason = None

    updated_by = str(payload.get("updated_by", "api")).strip()
    if not updated_by:
        raise ValueError("updated_by must be a non-empty string")

    return enabled, reason, updated_by


def parse_operator_review_payload(
    payload: Mapping[str, Any],
) -> tuple[str, str, str | None]:
    if "action" not in payload:
        raise ValueError("action is required")

    action = str(payload["action"]).strip()
    if not action:
        raise ValueError("action must be a non-empty string")

    updated_by = str(payload.get("updated_by", "api")).strip()
    if not updated_by:
        raise ValueError("updated_by must be a non-empty string")

    note = payload.get("note")
    if note is not None:
        note = str(note).strip()
        if not note:
            note = None

    return action, updated_by, note


def parse_instruction_set_cancellation_payload(
    payload: Mapping[str, Any],
) -> tuple[str, str | None, str | None, str | None, str | None, tuple[str, ...] | None, int]:
    requested_by = str(payload.get("requested_by", "api")).strip()
    if not requested_by:
        raise ValueError("requested_by must be a non-empty string")

    reason = payload.get("reason")
    if reason is not None:
        reason = str(reason).strip()
        if not reason:
            reason = None

    batch_id = str(payload["batch_id"]).strip() if payload.get("batch_id") is not None else None
    account_key = (
        str(payload["account_key"]).strip() if payload.get("account_key") is not None else None
    )
    book_key = str(payload["book_key"]).strip() if payload.get("book_key") is not None else None

    raw_instruction_ids = payload.get("instruction_ids")
    instruction_ids: tuple[str, ...] | None = None
    if raw_instruction_ids is not None:
        if not isinstance(raw_instruction_ids, list) or not raw_instruction_ids:
            raise ValueError("instruction_ids must be a non-empty array of strings")
        normalized_instruction_ids = tuple(str(item).strip() for item in raw_instruction_ids)
        if not all(normalized_instruction_ids):
            raise ValueError("instruction_ids must contain only non-empty strings")
        if len(set(normalized_instruction_ids)) != len(normalized_instruction_ids):
            raise ValueError("instruction_ids must not contain duplicates")
        instruction_ids = normalized_instruction_ids

    timeout = int(payload.get("timeout", 10))
    if timeout <= 0:
        raise ValueError("timeout must be positive")

    return requested_by, reason, batch_id, account_key, book_key, instruction_ids, timeout


def parse_intent_cleanup_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    requested_by = str(payload.get("requested_by", "api")).strip()
    if not requested_by:
        raise ValueError("requested_by must be a non-empty string")

    reason = payload.get("reason")
    if reason is not None:
        reason = str(reason).strip() or None

    raw_instruction_ids = payload.get("instruction_ids")
    instruction_ids: tuple[str, ...] | None = None
    if raw_instruction_ids is not None:
        if not isinstance(raw_instruction_ids, list) or not raw_instruction_ids:
            raise ValueError("instruction_ids must be a non-empty array of strings")
        parsed_instruction_ids = tuple(str(item).strip() for item in raw_instruction_ids)
        if not all(parsed_instruction_ids):
            raise ValueError("instruction_ids must contain only non-empty strings")
        if len(set(parsed_instruction_ids)) != len(parsed_instruction_ids):
            raise ValueError("instruction_ids must not contain duplicates")
        instruction_ids = parsed_instruction_ids

    keep_instruction_id = (
        str(payload["keep_instruction_id"]).strip()
        if payload.get("keep_instruction_id") is not None
        else None
    )
    if keep_instruction_id == "":
        raise ValueError("keep_instruction_id must be a non-empty string")

    timeout = int(payload.get("timeout", 10))
    if timeout <= 0:
        raise ValueError("timeout must be positive")

    return {
        "requested_by": requested_by,
        "reason": reason,
        "apply": bool(payload.get("apply", False)),
        "account_key": (
            str(payload["account_key"]).strip()
            if payload.get("account_key") is not None
            else None
        ),
        "book_key": (
            str(payload["book_key"]).strip()
            if payload.get("book_key") is not None
            else None
        ),
        "book_side": (
            str(payload["book_side"]).strip()
            if payload.get("book_side") is not None
            else None
        ),
        "symbol": (
            str(payload["symbol"]).strip()
            if payload.get("symbol") is not None
            else None
        ),
        "exchange": (
            str(payload["exchange"]).strip()
            if payload.get("exchange") is not None
            else None
        ),
        "currency": (
            str(payload["currency"]).strip()
            if payload.get("currency") is not None
            else None
        ),
        "instruction_ids": instruction_ids,
        "keep_instruction_id": keep_instruction_id,
        "cancel_all_entries": bool(payload.get("cancel_all_entries", False)),
        "timeout": timeout,
    }


def parse_instruction_archive_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    requested_by = str(payload.get("requested_by", "api")).strip()
    if not requested_by:
        raise ValueError("requested_by must be a non-empty string")

    reason = payload.get("reason")
    if reason is not None:
        reason = str(reason).strip() or None

    raw_instruction_ids = payload.get("instruction_ids")
    instruction_ids: tuple[str, ...] | None = None
    if raw_instruction_ids is not None:
        if not isinstance(raw_instruction_ids, list) or not raw_instruction_ids:
            raise ValueError("instruction_ids must be a non-empty array of strings")
        parsed_instruction_ids = tuple(str(item).strip() for item in raw_instruction_ids)
        if not all(parsed_instruction_ids):
            raise ValueError("instruction_ids must contain only non-empty strings")
        if len(set(parsed_instruction_ids)) != len(parsed_instruction_ids):
            raise ValueError("instruction_ids must not contain duplicates")
        instruction_ids = parsed_instruction_ids

    raw_states = payload.get("states")
    states: tuple[str, ...] | None = None
    if raw_states is not None:
        if not isinstance(raw_states, list) or not raw_states:
            raise ValueError("states must be a non-empty array of strings")
        states = tuple(str(item).strip().upper() for item in raw_states)
        if not all(states):
            raise ValueError("states must contain only non-empty strings")

    expire_before = (
        parse_datetime(payload["expire_before"], "expire_before")
        if payload.get("expire_before") is not None
        else None
    )
    limit = int(payload.get("limit", 500))
    include_active = bool(payload.get("include_active", False))
    model_routed = payload.get("model_routed")
    if model_routed is not None and not isinstance(model_routed, bool):
        raise ValueError("model_routed must be a boolean when provided")

    return {
        "requested_by": requested_by,
        "reason": reason,
        "instruction_ids": instruction_ids,
        "states": states,
        "batch_id": (
            str(payload["batch_id"]).strip()
            if payload.get("batch_id") is not None
            else None
        ),
        "account_key": (
            str(payload["account_key"]).strip()
            if payload.get("account_key") is not None
            else None
        ),
        "book_key": (
            str(payload["book_key"]).strip()
            if payload.get("book_key") is not None
            else None
        ),
        "source_system": (
            str(payload["source_system"]).strip()
            if payload.get("source_system") is not None
            else None
        ),
        "model_routed": model_routed,
        "expire_before": expire_before,
        "include_active": include_active,
        "limit": limit,
    }
