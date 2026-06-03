from __future__ import annotations

from typing import Any, Mapping

from ibkr_trader.api.payload_fields import _parse_json_object_field
from ibkr_trader.api.payload_fields import _parse_optional_string_list_update
from ibkr_trader.api.payload_fields import _parse_required_string
from ibkr_trader.api.payload_fields import _parse_string_list
from ibkr_trader.db.base import utc_now
from ibkr_trader.domain.execution_payloads import parse_datetime
from ibkr_trader.domain.execution_payloads import parse_decimal


def parse_trader_model_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    model_key = _parse_required_string(
        payload,
        "model_key",
        normalize=lambda value: value.lower(),
    )
    display_name = _parse_required_string(payload, "display_name")
    strategy_family = _parse_required_string(payload, "strategy_family")
    side = _parse_required_string(
        payload,
        "side",
        normalize=lambda value: value.upper(),
    )
    action_space = _parse_string_list(
        payload,
        "action_space",
        required=True,
        normalize=lambda value: value.lower(),
    )
    return {
        "model_key": model_key,
        "display_name": display_name,
        "strategy_family": strategy_family,
        "side": side,
        "source_workflow_path": (
            str(payload["source_workflow_path"]).strip()
            if payload.get("source_workflow_path") is not None
            else None
        ),
        "promoted_checkpoint_path": (
            str(payload["promoted_checkpoint_path"]).strip()
            if payload.get("promoted_checkpoint_path") is not None
            else None
        ),
        "action_space": action_space,
        "observation_contract": _parse_json_object_field(
            payload,
            "observation_contract",
        ),
        "execution_mapping_version": (
            str(payload["execution_mapping_version"]).strip()
            if payload.get("execution_mapping_version") is not None
            else None
        ),
        "metadata": _parse_json_object_field(payload, "metadata"),
    }


def parse_trader_deployment_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    account_key = _parse_required_string(
        payload,
        "account_key",
        normalize=lambda value: value.upper(),
    )
    return {
        "deployment_key": _parse_required_string(
            payload,
            "deployment_key",
            normalize=lambda value: value.lower(),
        ),
        "model_key": _parse_required_string(
            payload,
            "model_key",
            normalize=lambda value: value.lower(),
        ),
        "account_key": account_key,
        "book_key": _parse_required_string(
            payload,
            "book_key",
            normalize=lambda value: value.lower(),
        ),
        "mode": _parse_required_string(
            payload,
            "mode",
            normalize=lambda value: value.lower(),
        ),
        "status": _parse_required_string(
            payload,
            "status",
            normalize=lambda value: value.lower(),
        ),
        "allowed_symbols": _parse_string_list(
            payload,
            "allowed_symbols",
            normalize=lambda value: value.upper(),
        ),
        "risk_limits": _parse_json_object_field(payload, "risk_limits"),
        "action_constraints": _parse_json_object_field(payload, "action_constraints"),
        "metadata": _parse_json_object_field(payload, "metadata"),
    }




def parse_trader_deployment_update_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    if "account_key" in payload:
        parsed["account_key"] = _parse_required_string(
            payload,
            "account_key",
            normalize=lambda value: value.upper(),
        )
    if "book_key" in payload:
        parsed["book_key"] = _parse_required_string(
            payload,
            "book_key",
            normalize=lambda value: value.lower(),
        )
    if "mode" in payload:
        parsed["mode"] = _parse_required_string(
            payload,
            "mode",
            normalize=lambda value: value.lower(),
        )
    if "status" in payload:
        parsed["status"] = _parse_required_string(
            payload,
            "status",
            normalize=lambda value: value.lower(),
        )
    if "allowed_symbols" in payload:
        parsed["allowed_symbols"] = _parse_optional_string_list_update(
            payload,
            "allowed_symbols",
            normalize=lambda value: value.upper(),
        )
    if "risk_limits" in payload:
        parsed["risk_limits"] = _parse_json_object_field(payload, "risk_limits")
    if "action_constraints" in payload:
        parsed["action_constraints"] = _parse_json_object_field(
            payload,
            "action_constraints",
        )
    if "metadata" in payload:
        parsed["metadata"] = _parse_json_object_field(payload, "metadata")
    if not parsed:
        raise ValueError("at least one deployment field is required")
    return parsed


def parse_trader_action_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    observed_at = parse_datetime(
        _parse_required_string(payload, "observed_at"),
        "observed_at",
    )
    return {
        "deployment_key": _parse_required_string(
            payload,
            "deployment_key",
            normalize=lambda value: value.lower(),
        ),
        "symbol": _parse_required_string(
            payload,
            "symbol",
            normalize=lambda value: value.upper(),
        ),
        "action_name": _parse_required_string(
            payload,
            "action_name",
            normalize=lambda value: value.lower(),
        ),
        "observed_at": observed_at,
        "state_before": (
            str(payload["state_before"]).strip().upper()
            if payload.get("state_before") is not None
            else None
        ),
        "state_after": (
            str(payload["state_after"]).strip().upper()
            if payload.get("state_after") is not None
            else None
        ),
        "action_status": _parse_required_string(
            payload,
            "action_status",
            normalize=lambda value: value.lower(),
        ),
        "instruction_id": (
            str(payload["instruction_id"]).strip()
            if payload.get("instruction_id") is not None
            else None
        ),
        "payload": _parse_json_object_field(payload, "payload"),
        "note": str(payload["note"]).strip() if payload.get("note") is not None else None,
    }


def parse_rl_action_translate_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    observed_at = (
        parse_datetime(payload["observed_at"], "observed_at")
        if payload.get("observed_at") is not None
        else utc_now()
    )
    previous_close = (
        parse_decimal(payload["previous_close"], "previous_close")
        if payload.get("previous_close") is not None
        else None
    )
    decision_id = (
        str(payload["decision_id"]).strip()
        if payload.get("decision_id") is not None
        else None
    )
    if decision_id == "":
        decision_id = None
    model_diagnostics = _parse_json_object_field(payload, "model_diagnostics")
    return {
        "deployment_key": _parse_required_string(
            payload,
            "deployment_key",
            normalize=lambda value: value.lower(),
        ),
        "source_instruction_id": _parse_required_string(
            payload,
            "source_instruction_id",
        ),
        "action_name": _parse_required_string(
            payload,
            "action_name",
            normalize=lambda value: value.lower(),
        ),
        "state_before": str(payload.get("state_before", "FLAT")).strip().upper(),
        "observed_at": observed_at,
        "previous_close": previous_close,
        "decision_id": decision_id,
        "submit": bool(payload.get("submit", False)),
        "log_action": bool(payload.get("log_action", False)),
        "model_diagnostics": model_diagnostics,
    }


def parse_trader_heartbeat_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    last_seen_at = parse_datetime(
        _parse_required_string(payload, "last_seen_at"),
        "last_seen_at",
    )
    last_bar_at = (
        parse_datetime(payload["last_bar_at"], "last_bar_at")
        if payload.get("last_bar_at") is not None
        else None
    )
    last_action_at = (
        parse_datetime(payload["last_action_at"], "last_action_at")
        if payload.get("last_action_at") is not None
        else None
    )
    return {
        "status": _parse_required_string(
            payload,
            "status",
            normalize=lambda value: value.lower(),
        ),
        "last_seen_at": last_seen_at,
        "last_bar_at": last_bar_at,
        "last_action_at": last_action_at,
        "runtime_error": (
            str(payload["runtime_error"]).strip()
            if payload.get("runtime_error") is not None
            else None
        ),
        "metrics": _parse_json_object_field(payload, "metrics"),
    }


def parse_rl_observation_build_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw_source_bars = payload.get("source_bars")
    if raw_source_bars is None:
        source_bars: dict[str, Any] = {}
    elif isinstance(raw_source_bars, Mapping):
        source_bars = dict(raw_source_bars)
    else:
        raise ValueError("source_bars must be an object keyed by symbol")

    raw_history_overrides = (
        payload.get("history_overrides")
        if payload.get("history_overrides") is not None
        else payload.get("history_features")
    )
    if raw_history_overrides is None:
        history_overrides: dict[str, Any] = {}
    elif isinstance(raw_history_overrides, Mapping):
        history_overrides = dict(raw_history_overrides)
    else:
        raise ValueError("history_overrides must be an object keyed by symbol")

    raw_static_features = (
        payload.get("static_features")
        if payload.get("static_features") is not None
        else payload.get("static_features_by_symbol")
    )
    if raw_static_features is None:
        static_features: dict[str, Any] = {}
    elif isinstance(raw_static_features, Mapping):
        static_features = dict(raw_static_features)
    else:
        raise ValueError("static_features must be an object keyed by symbol")

    raw_fetch = payload.get("fetch", {})
    if not isinstance(raw_fetch, Mapping):
        raise ValueError("fetch must be an object")

    return {
        "deployment_key": _parse_required_string(
            payload,
            "deployment_key",
            normalize=lambda value: value.lower(),
        ),
        "symbols": _parse_string_list(
            payload,
            "symbols",
            normalize=lambda value: value.upper(),
        ),
        "as_of": (
            parse_datetime(payload["as_of"], "as_of")
            if payload.get("as_of") is not None
            else utc_now()
        ),
        "source_bars": source_bars,
        "history_overrides": history_overrides,
        "static_features": static_features,
        "config_overrides": _parse_json_object_field(payload, "observation"),
        "include_source_bars": bool(payload.get("include_source_bars", False)),
        "fetch": dict(raw_fetch),
    }
