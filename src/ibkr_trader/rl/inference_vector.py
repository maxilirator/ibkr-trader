from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

import numpy as np


RUNTIME_DYNAMIC_DIM = 15


@dataclass(frozen=True, slots=True)
class RunnerSymbolState:
    in_position: bool = False
    pending_entry_anchor: str | None = None
    pending_entry_rel_bp: int | None = None
    pending_exit_tp_bp: int | None = None
    entry_price: float | None = None
    entry_bar_idx: int | None = None
    bars_since_entry_order: int = 0
    bars_since_exit_order: int = 0


def has_pending_entry(state: RunnerSymbolState) -> bool:
    """Return whether the runner has a live entry order, including market entries."""

    return (
        not state.in_position
        and (
            state.pending_entry_anchor is not None
            or state.bars_since_entry_order > 0
        )
    )


def flat_runner_state() -> RunnerSymbolState:
    return RunnerSymbolState()


def build_runtime_dynamic_features(
    *,
    state: RunnerSymbolState,
    bar_idx: int,
    n_bars: int,
    previous_close: float,
    session_open: float,
    open_now: float,
    trade_sign: float,
) -> list[float]:
    if previous_close <= 0.0:
        raise ValueError("previous_close must be positive")
    if session_open <= 0.0:
        raise ValueError("session_open must be positive")
    bar_norm_denom = max(float(n_bars - 1), 1.0)
    bars_norm_denom = max(float(n_bars), 1.0)
    entry_price = 0.0 if state.entry_price is None else float(state.entry_price)
    entry_price_rel_prev_close = (
        0.0 if state.entry_price is None else float((entry_price / previous_close) - 1.0)
    )
    entry_price_rel_session_open = (
        0.0 if state.entry_price is None else float((entry_price / session_open) - 1.0)
    )
    unrealized_at_open = 0.0
    if state.in_position and state.entry_price is not None:
        unrealized_at_open = float(trade_sign * ((open_now / float(state.entry_price)) - 1.0))

    return [
        1.0 if not state.in_position and not has_pending_entry(state) else 0.0,
        1.0 if has_pending_entry(state) else 0.0,
        1.0 if state.in_position else 0.0,
        1.0 if state.pending_entry_anchor == "prev_close" else 0.0,
        1.0 if state.pending_entry_anchor == "session_open" else 0.0,
        0.0 if state.pending_entry_rel_bp is None else float(state.pending_entry_rel_bp) / 100.0,
        1.0 if state.pending_exit_tp_bp is not None else 0.0,
        0.0 if state.pending_exit_tp_bp is None else float(state.pending_exit_tp_bp) / 100.0,
        entry_price_rel_prev_close,
        entry_price_rel_session_open,
        unrealized_at_open,
        (
            0.0
            if state.entry_bar_idx is None
            else float(bar_idx - int(state.entry_bar_idx)) / bar_norm_denom
        ),
        float(state.bars_since_entry_order) / bars_norm_denom,
        float(state.bars_since_exit_order) / bars_norm_denom,
        1.0 if bar_idx == (n_bars - 1) else 0.0,
    ]


def assemble_dqn_observation_vector(
    observation: Mapping[str, Any],
    *,
    state: RunnerSymbolState | None = None,
    model_side: str | None = None,
    path_pad_length: int | None = None,
    expected_obs_dim: int | None = None,
) -> np.ndarray:
    """Assemble the bucket DQN vector from one API observation payload.

    The order intentionally mirrors q-training's DQN encoder:
    static, current base dynamic, current extra dynamic, runtime dynamic,
    history, then feature-major padded path stack.
    """

    symbol_state = state or flat_runner_state()
    features = _mapping(observation["features"], "features")
    decision = _mapping(observation["model_decision"], "model_decision")
    if not bool(features.get("static_features_ready")):
        raise ValueError("static features are missing")

    usable_bar_count = int(decision.get("usable_bar_count") or 0)
    if usable_bar_count <= 0:
        raise ValueError("model_decision.usable_bar_count must be positive")
    bar_idx = usable_bar_count - 1

    base_dynamic = _rows(features["base_dynamic"], "base_dynamic")
    extra_dynamic = _rows(features.get("extra_dynamic", []), "extra_dynamic")
    path_stack = _rows(features["path_feature_stack"], "path_feature_stack")
    if bar_idx >= len(base_dynamic):
        raise ValueError("usable bar index is outside base_dynamic")
    if extra_dynamic and bar_idx >= len(extra_dynamic):
        raise ValueError("usable bar index is outside extra_dynamic")
    if bar_idx >= len(path_stack):
        raise ValueError("usable bar index is outside path_feature_stack")

    phase1_bars = _sequence(observation["phase1_bars"], "phase1_bars")
    if usable_bar_count > len(phase1_bars):
        raise ValueError("usable_bar_count exceeds phase1_bars length")
    current_bar = _mapping(phase1_bars[bar_idx], "phase1_bars[current]")
    pricing_context = _mapping(observation["pricing_context"], "pricing_context")
    previous_close = float(pricing_context["prev_close"])
    session_open = float(pricing_context["session_open"])
    open_now = float(current_bar["open"])

    normalized_model_side = str(model_side or observation.get("model_side") or "").upper()
    trade_sign = -1.0 if normalized_model_side == "SHORT" else 1.0
    n_bars = int(observation.get("bar_count") or len(phase1_bars))

    runtime_dynamic = build_runtime_dynamic_features(
        state=symbol_state,
        bar_idx=bar_idx,
        n_bars=n_bars,
        previous_close=previous_close,
        session_open=session_open,
        open_now=open_now,
        trade_sign=trade_sign,
    )
    resolved_path_pad_length = (
        int(path_pad_length) if path_pad_length is not None else _path_pad_length(observation)
    )
    path_dim = len(path_stack[0]) if path_stack else 0
    padded_path = np.zeros(path_dim * resolved_path_pad_length, dtype=np.float32)
    prefix_end = bar_idx + 1
    for path_idx in range(path_dim):
        segment_start = path_idx * resolved_path_pad_length
        padded_path[segment_start : segment_start + prefix_end] = [
            float(row[path_idx]) for row in path_stack[:prefix_end]
        ]

    parts = [
        np.asarray(_float_sequence(features["static_features"], "static_features"), dtype=np.float32),
        np.asarray(_float_sequence(base_dynamic[bar_idx], "base_dynamic[current]"), dtype=np.float32),
        np.asarray(
            _float_sequence(extra_dynamic[bar_idx], "extra_dynamic[current]")
            if extra_dynamic
            else [],
            dtype=np.float32,
        ),
        np.asarray(runtime_dynamic, dtype=np.float32),
        np.asarray(_float_sequence(features["history_features"], "history_features"), dtype=np.float32),
        padded_path,
    ]
    vector = np.concatenate(parts).astype(np.float32, copy=False)
    if expected_obs_dim is not None and int(vector.shape[0]) != int(expected_obs_dim):
        raise ValueError(
            f"observation vector width mismatch: expected {expected_obs_dim}, "
            f"got {int(vector.shape[0])}"
        )
    return vector


def valid_action_mask(action_names: Sequence[str], state: RunnerSymbolState | None = None) -> np.ndarray:
    symbol_state = state or flat_runner_state()
    pending_entry = has_pending_entry(symbol_state)
    mask = np.zeros(len(action_names), dtype=bool)
    for idx, raw_name in enumerate(action_names):
        name = str(raw_name)
        if not symbol_state.in_position and not pending_entry:
            mask[idx] = (
                name in {"skip", "wait", "market_entry"}
                or name.startswith("entry_prevclose_")
                or name.startswith("entry_sessionopen_")
            )
        elif not symbol_state.in_position:
            mask[idx] = (
                name in {"skip", "wait", "market_entry", "cancel_entry"}
                or name.startswith("entry_prevclose_")
                or name.startswith("entry_sessionopen_")
            )
        elif symbol_state.pending_exit_tp_bp is None:
            mask[idx] = name in {"wait", "exit_market"} or name.startswith("exit_tp_")
        else:
            mask[idx] = (
                name in {"wait", "exit_market", "clear_exit"}
                or name.startswith("exit_tp_")
            )
    return mask


def _path_pad_length(observation: Mapping[str, Any]) -> int:
    feature_schema = observation.get("feature_schema")
    if isinstance(feature_schema, Mapping):
        raw = feature_schema.get("path_pad_length")
        if raw is not None:
            return int(raw)
    input_contract = observation.get("input_contract")
    if isinstance(input_contract, Mapping) and input_contract.get("expected_session_bars") is not None:
        return int(input_contract["expected_session_bars"])
    return 102


def _mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return value


def _sequence(value: Any, field_name: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"{field_name} must be an array")
    return value


def _rows(value: Any, field_name: str) -> list[list[float]]:
    sequence = _sequence(value, field_name)
    rows: list[list[float]] = []
    for row in sequence:
        rows.append(_float_sequence(row, field_name))
    return rows


def _float_sequence(value: Any, field_name: str) -> list[float]:
    sequence = _sequence(value, field_name)
    return [float(item) for item in sequence]
