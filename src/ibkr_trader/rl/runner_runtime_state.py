from __future__ import annotations

import urllib.parse
from typing import Any, Mapping

from ibkr_trader.rl.inference_vector import RunnerSymbolState
from ibkr_trader.rl.inference_vector import has_pending_entry
from ibkr_trader.rl.runner_http import get_json
from ibkr_trader.rl.runner_types import RuntimeStateContext


def load_runtime_state_context(
    *,
    api_base: str,
    deployment_key: str,
    symbols: list[str],
    side: str,
) -> RuntimeStateContext:
    states = load_runtime_states_from_instructions(
        api_base=api_base,
        deployment_key=deployment_key,
        symbols=symbols,
        side=side,
    )
    blocked = getattr(load_runtime_states_from_instructions, "_last_blocked_symbols", {})
    source = getattr(load_runtime_states_from_instructions, "_last_source", "instructions")
    return RuntimeStateContext(
        states=states,
        blocked_symbols=dict(blocked if isinstance(blocked, Mapping) else {}),
        source=str(source),
    )


def load_runtime_states_from_instructions(
    *,
    api_base: str,
    deployment_key: str,
    symbols: list[str],
    side: str,
) -> dict[str, RunnerSymbolState]:
    """Recover per-symbol runner state from the API runtime-state contract."""

    symbol_set = {symbol.upper() for symbol in symbols}
    runtime_payload = get_json(
        f"{api_base}/v1/rl/runtime-state?"
        + urllib.parse.urlencode(
            {
                "deployment_key": deployment_key,
                "symbols": ",".join(sorted(symbol_set)),
            }
        )
    )
    if isinstance(runtime_payload.get("runtime_state"), Mapping):
        return _runtime_states_from_runtime_state_payload(runtime_payload)

    load_runtime_states_from_instructions._last_blocked_symbols = {}
    load_runtime_states_from_instructions._last_source = "instructions"
    payload = runtime_payload
    latest_by_symbol: dict[str, Mapping[str, Any]] = {}
    for instruction in payload.get("instructions", []):
        if not isinstance(instruction, Mapping):
            continue
        if str(instruction.get("source_system") or "") != "rl-runner":
            continue
        symbol = str(instruction.get("symbol") or "").upper()
        if symbol not in symbol_set:
            continue
        metadata = _instruction_metadata(instruction)
        if str(metadata.get("rl_deployment_key") or "") != deployment_key:
            continue
        previous = latest_by_symbol.get(symbol)
        if previous is None or str(instruction.get("activity_at") or "") > str(
            previous.get("activity_at") or ""
        ):
            latest_by_symbol[symbol] = instruction

    states: dict[str, RunnerSymbolState] = {}
    for symbol, instruction in latest_by_symbol.items():
        state_name = str(instruction.get("state") or "").upper()
        metadata = _instruction_metadata(instruction)
        if state_name in {"ENTRY_PENDING", "ENTRY_SUBMITTED"}:
            action_name = str(metadata.get("rl_action_name") or "")
            states[symbol] = RunnerSymbolState(
                in_position=False,
                pending_entry_anchor=_pending_entry_anchor(action_name),
                pending_entry_rel_bp=_entry_rel_bp(action_name),
                bars_since_entry_order=1,
            )
        elif state_name in {"POSITION_OPEN", "EXIT_PENDING"}:
            states[symbol] = RunnerSymbolState(
                in_position=True,
                entry_price=_float_or_none(instruction.get("entry_avg_fill_price")),
                pending_exit_tp_bp=(180 if side.upper() == "SHORT" else 200)
                if state_name == "EXIT_PENDING"
                else None,
                bars_since_exit_order=1 if state_name == "EXIT_PENDING" else 0,
            )
    return states


def _runtime_states_from_runtime_state_payload(
    payload: Mapping[str, Any],
) -> dict[str, RunnerSymbolState]:
    runtime_state = payload.get("runtime_state")
    if not isinstance(runtime_state, Mapping):
        raise ValueError("runtime_state payload must be an object")
    blocked_symbols: dict[str, Mapping[str, Any]] = {}
    states: dict[str, RunnerSymbolState] = {}
    for item in runtime_state.get("symbols", []):
        if not isinstance(item, Mapping):
            continue
        symbol = str(item.get("symbol") or "").upper()
        if not symbol:
            continue
        if str(item.get("status") or "").lower() != "ready":
            blocked_symbols[symbol] = item
            continue
        runner_state_payload = item.get("runner_state")
        if not isinstance(runner_state_payload, Mapping):
            blocked_symbols[symbol] = {
                **dict(item),
                "status": "blocked",
                "blockers": [
                    {
                        "reason": "missing_runner_state",
                        "message": "Runtime-state endpoint did not return a runner_state.",
                    }
                ],
            }
            continue
        states[symbol] = RunnerSymbolState(
            in_position=bool(runner_state_payload.get("in_position")),
            pending_entry_anchor=_str_or_none(
                runner_state_payload.get("pending_entry_anchor")
            ),
            pending_entry_rel_bp=_int_or_none(
                runner_state_payload.get("pending_entry_rel_bp")
            ),
            pending_exit_tp_bp=_int_or_none(
                runner_state_payload.get("pending_exit_tp_bp")
            ),
            entry_price=_float_or_none(runner_state_payload.get("entry_price")),
            entry_bar_idx=_int_or_none(runner_state_payload.get("entry_bar_idx")),
            bars_since_entry_order=int(
                runner_state_payload.get("bars_since_entry_order") or 0
            ),
            bars_since_exit_order=int(
                runner_state_payload.get("bars_since_exit_order") or 0
            ),
        )

    load_runtime_states_from_instructions._last_blocked_symbols = blocked_symbols
    load_runtime_states_from_instructions._last_source = "runtime-state"
    return states


def translation_state_before(state: RunnerSymbolState, side: str) -> str:
    if state.in_position:
        if state.pending_exit_tp_bp is not None:
            return "EXIT_PENDING"
        return "SHORT_OPEN" if side.upper() == "SHORT" else "LONG_OPEN"
    if has_pending_entry(state):
        return "ENTRY_PENDING"
    return "FLAT"


def _api_error_is_conflict(exc: ApiError) -> bool:
    return "HTTP 409:" in str(exc)


def _instruction_metadata(instruction: Mapping[str, Any]) -> Mapping[str, Any]:
    payload = instruction.get("payload", {})
    if not isinstance(payload, Mapping):
        return {}
    instruction_payload = payload.get("instruction", {})
    if not isinstance(instruction_payload, Mapping):
        return {}
    trace = instruction_payload.get("trace", {})
    if not isinstance(trace, Mapping):
        return {}
    metadata = trace.get("metadata", {})
    return metadata if isinstance(metadata, Mapping) else {}


def _entry_rel_bp(action_name: Any) -> int | None:
    raw = str(action_name or "")
    prefix = "entry_prevclose_"
    suffix = "bp"
    if not raw.startswith(prefix) or not raw.endswith(suffix):
        return None
    try:
        return int(raw.removeprefix(prefix).removesuffix(suffix))
    except ValueError:
        return None


def _pending_entry_anchor(action_name: Any) -> str:
    raw = str(action_name or "")
    if raw == "market_entry":
        return "market"
    if raw.startswith("entry_prevclose_"):
        return "prev_close"
    if raw.startswith("entry_sessionopen_"):
        return "session_open"
    return "unknown"


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


