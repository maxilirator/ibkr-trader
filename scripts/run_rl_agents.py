#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict
from dataclasses import dataclass
from datetime import date
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

try:
    import torch
    from torch import nn
except ModuleNotFoundError:  # pragma: no cover - exercised by runner import smoke tests.
    torch = None
    nn = None

from ibkr_trader.rl.inference_vector import RunnerSymbolState
from ibkr_trader.rl.inference_vector import assemble_dqn_observation_vector
from ibkr_trader.rl.inference_vector import valid_action_mask
from ibkr_trader.rl.model_artifacts import PromotedRLModelArtifact
from ibkr_trader.rl.model_artifacts import promoted_rl_models
from ibkr_trader.rl.model_artifacts import read_static_feature_names
from ibkr_trader.rl.observations import build_history_override_from_source_bars


STOCKHOLM_TZ = ZoneInfo("Europe/Stockholm")
MODEL_CONFIGS: dict[str, PromotedRLModelArtifact] = {
    artifact.model_key: artifact for artifact in promoted_rl_models()
}
DEFAULT_BENCHMARK_SYMBOLS = ("XACT-OMXS30",)


@dataclass(slots=True)
class LoadedModel:
    config: PromotedRLModelArtifact
    action_names: list[str]
    obs_dim: int
    model: Any
    static_feature_names: list[str]
    candidate_tape: pd.DataFrame


class ApiError(RuntimeError):
    pass


def parse_symbol_list(raw_value: str | None) -> list[str]:
    if raw_value is None:
        return []
    return sorted(
        {
            item.strip().upper()
            for item in raw_value.replace("\n", ",").split(",")
            if item.strip()
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Run promoted virtual RL agents against the trader API.")
    parser.add_argument("--api-base", default="http://quant.geisler.se:8000")
    parser.add_argument("--poll-seconds", type=float, default=60.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--execute-virtual", action="store_true")
    parser.add_argument("--include-smoke", action="store_true")
    parser.add_argument("--stop-stream-on-empty", action="store_true")
    parser.add_argument("--market-data-type", default="LIVE")
    parser.add_argument("--candidate-reason-code", default="rl_model_routed_selected_candidate")
    parser.add_argument("--state-file", default=".rl_runner_state.json")
    parser.add_argument("--history-cache-file", default=".rl_runner_history_cache.json")
    parser.add_argument("--history-duration", default="30 D")
    parser.add_argument("--history-bar-size", default="1 min")
    parser.add_argument("--history-timeout", type=int, default=20)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument(
        "--benchmark-symbols",
        default=",".join(DEFAULT_BENCHMARK_SYMBOLS),
        help="Comma-separated symbols to keep in the market stream for dashboard benchmarking.",
    )
    parser.add_argument(
        "--trade-date",
        default=None,
        help="Only process candidates whose trace.trade_date matches YYYY-MM-DD. Defaults to today's Stockholm date.",
    )
    args = parser.parse_args()

    state_path = Path(args.state_file)
    if str(state_path.parent) not in {"", "."}:
        state_path.parent.mkdir(parents=True, exist_ok=True)
    processed_decisions = _load_processed_decisions(state_path)
    history_cache_path = Path(args.history_cache_file)
    if str(history_cache_path.parent) not in {"", "."}:
        history_cache_path.parent.mkdir(parents=True, exist_ok=True)
    history_cache = _load_history_cache(history_cache_path)
    loaded_models = {key: load_model(config) for key, config in MODEL_CONFIGS.items()}
    print(
        "Loaded models: "
        + ", ".join(
            f"{model.config.model_key}(obs_dim={model.obs_dim}, actions={model.action_names})"
            for model in loaded_models.values()
        ),
        flush=True,
    )

    while True:
        try:
            run_once(
                api_base=args.api_base.rstrip("/"),
                limit=args.limit,
                loaded_models=loaded_models,
                processed_decisions=processed_decisions,
                execute_virtual=args.execute_virtual,
                include_smoke=args.include_smoke,
                stop_stream_on_empty=args.stop_stream_on_empty,
                market_data_type=args.market_data_type,
                candidate_reason_code=args.candidate_reason_code,
                trade_date=args.trade_date or datetime.now(STOCKHOLM_TZ).date().isoformat(),
                history_cache=history_cache,
                history_duration=args.history_duration,
                history_bar_size=args.history_bar_size,
                history_timeout=args.history_timeout,
                benchmark_symbols=parse_symbol_list(args.benchmark_symbols),
            )
            _save_processed_decisions(state_path, processed_decisions)
            _save_history_cache(history_cache_path, history_cache)
        except Exception as exc:
            _save_history_cache(history_cache_path, history_cache)
            print(f"runner_error: {exc}", file=sys.stderr, flush=True)
        if args.once:
            return 0
        time.sleep(max(args.poll_seconds, 5.0))


def run_once(
    *,
    api_base: str,
    limit: int,
    loaded_models: Mapping[str, LoadedModel],
    processed_decisions: set[str],
    execute_virtual: bool,
    include_smoke: bool,
    stop_stream_on_empty: bool,
    market_data_type: str,
    candidate_reason_code: str,
    trade_date: str,
    history_cache: dict[str, Any],
    history_duration: str,
    history_bar_size: str,
    history_timeout: int,
    benchmark_symbols: list[str],
) -> None:
    candidates = get_json(f"{api_base}/v1/rl/candidates?limit={limit}")["candidates"]
    candidates = [
        candidate
        for candidate in candidates
        if candidate.get("model_id") in loaded_models
        and candidate.get("is_virtual") is True
        and candidate.get("trace", {}).get("trade_date") == trade_date
        and (
            not candidate_reason_code
            or candidate.get("trace", {}).get("reason_code") == candidate_reason_code
        )
        and (include_smoke or candidate.get("source", {}).get("system") != "codex-smoke")
    ]
    if not candidates:
        if benchmark_symbols:
            try:
                subscribe_symbols(
                    api_base,
                    benchmark_symbols,
                    market_data_type=market_data_type,
                )
            except ApiError as exc:
                print(f"Benchmark stream unavailable: {exc}", flush=True)
        for loaded in loaded_models.values():
            heartbeat(
                api_base,
                loaded.config.deployment_key,
                "running",
                runtime_error=None,
                metrics={"candidate_count": 0, "runner_mode": "idle"},
            )
        print(f"No virtual RL candidates found for trade_date={trade_date}.", flush=True)
        return

    symbols = sorted({str(candidate["symbol"]).upper() for candidate in candidates})
    stream_symbols = sorted(set(symbols) | set(benchmark_symbols))
    try:
        subscribe_symbols(api_base, stream_symbols, market_data_type=market_data_type)
        stream = get_json(
            f"{api_base}/v1/market-data/stream/snapshot?"
            + urllib.parse.urlencode({"symbols": ",".join(stream_symbols), "bar_limit": "390"})
        )["stream"]
    except ApiError as exc:
        heartbeat_stream_failure(
            api_base=api_base,
            loaded_models=loaded_models,
            candidates=candidates,
            error=str(exc),
            market_data_type=market_data_type,
            stop_stream_on_empty=stop_stream_on_empty,
        )
        print(f"Market stream unavailable; not running model: {exc}", flush=True)
        return
    symbols_with_bars = {
        symbol
        for symbol, bars in stream.get("bars_by_symbol", {}).items()
        if symbol in symbols
        if isinstance(bars, list) and bars
    }
    if not symbols_with_bars:
        if stop_stream_on_empty:
            post_json(f"{api_base}/v1/market-data/stream/stop", {})
        candidate_models = {str(candidate.get("model_id")) for candidate in candidates}
        for loaded in loaded_models.values():
            if loaded.config.model_key not in candidate_models:
                heartbeat(
                    api_base,
                    loaded.config.deployment_key,
                    "running",
                    runtime_error=None,
                    metrics={"candidate_count": 0, "runner_mode": "idle"},
                )
                continue
            heartbeat(
                api_base,
                loaded.config.deployment_key,
                "degraded",
                runtime_error="market stream has no 1-minute bars for active RL candidates",
                metrics={
                    "candidate_count": sum(
                        1
                        for candidate in candidates
                        if candidate.get("model_id") == loaded.config.model_key
                    ),
                    "symbols": sorted(
                        {
                            str(candidate["symbol"]).upper()
                            for candidate in candidates
                            if candidate.get("model_id") == loaded.config.model_key
                        }
                    ),
                    "stream_running": stream.get("running"),
                    "stream_connect_attempt_count": stream.get("connect_attempt_count"),
                    "desired_subscription_count": stream.get("desired_subscription_count"),
                    "stopped_stream": stop_stream_on_empty,
                },
            )
        print("No stream bars yet; not running model.", flush=True)
        return

    for model_key, loaded in loaded_models.items():
        model_candidates = [
            candidate
            for candidate in candidates
            if candidate.get("model_id") == model_key and str(candidate["symbol"]).upper() in symbols_with_bars
        ]
        if not model_candidates:
            heartbeat(
                api_base,
                loaded.config.deployment_key,
                "running",
                runtime_error=None,
                metrics={
                    "candidate_count": 0,
                    "runner_mode": "no_candidates_with_stream_bars",
                },
            )
            continue
        run_model_candidates(
            api_base=api_base,
            loaded=loaded,
            candidates=model_candidates,
            processed_decisions=processed_decisions,
            execute_virtual=execute_virtual,
            history_cache=history_cache,
            history_duration=history_duration,
            history_bar_size=history_bar_size,
            history_timeout=history_timeout,
        )


def run_model_candidates(
    *,
    api_base: str,
    loaded: LoadedModel,
    candidates: list[Mapping[str, Any]],
    processed_decisions: set[str],
    execute_virtual: bool,
    history_cache: dict[str, Any],
    history_duration: str,
    history_bar_size: str,
    history_timeout: int,
) -> None:
    symbols = sorted({str(candidate["symbol"]).upper() for candidate in candidates})
    runtime_states = load_runtime_states_from_instructions(
        api_base=api_base,
        deployment_key=loaded.config.deployment_key,
        symbols=symbols,
        side=loaded.config.side,
    )
    active_candidates: list[Mapping[str, Any]] = []
    static_features: dict[str, Any] = {}
    history_overrides: dict[str, Any] = {}
    skipped_candidates: list[dict[str, Any]] = []
    for candidate in candidates:
        symbol = str(candidate["symbol"]).upper()
        trace = candidate.get("trace", {})
        cutoff = trace.get("data_cutoff_date") if isinstance(trace, Mapping) else None
        trade_date = trace.get("trade_date") if isinstance(trace, Mapping) else None
        if not cutoff:
            skipped_candidates.append(
                {
                    "symbol": symbol,
                    "status": "skipped",
                    "reason": "candidate missing trace.data_cutoff_date",
                }
            )
            continue
        if not trade_date:
            skipped_candidates.append(
                {
                    "symbol": symbol,
                    "status": "skipped",
                    "reason": "candidate missing trace.trade_date",
                }
            )
            continue
        try:
            static_features[symbol] = static_feature_payload(
                loaded,
                candidate=candidate,
                symbol=symbol,
                trade_date=str(cutoff),
            )
            history_overrides[symbol] = history_override_payload(
                api_base=api_base,
                loaded=loaded,
                candidate=candidate,
                trade_date=str(trade_date),
                history_cache=history_cache,
                duration=history_duration,
                bar_size=history_bar_size,
                timeout=history_timeout,
            )
        except Exception as exc:
            skipped_candidates.append(
                {
                    "symbol": symbol,
                    "status": "skipped",
                    "reason": str(exc),
                }
            )
            continue
        active_candidates.append(candidate)

    if not active_candidates:
        heartbeat(
            api_base,
            loaded.config.deployment_key,
            "degraded",
            runtime_error="no candidates had complete static features and history overrides",
            metrics={
                "candidate_count": len(candidates),
                "symbols": symbols,
                "skipped_candidates": skipped_candidates,
            },
        )
        print(
            json.dumps(
                {
                    "deployment_key": loaded.config.deployment_key,
                    "actions": skipped_candidates,
                },
                indent=2,
            ),
            flush=True,
        )
        return

    active_symbols = sorted({str(candidate["symbol"]).upper() for candidate in active_candidates})

    observation_response = post_json(
        f"{api_base}/v1/rl/observations/build",
        {
            "deployment_key": loaded.config.deployment_key,
            "symbols": active_symbols,
            "history_overrides": history_overrides,
            "static_features": static_features,
            "fetch": {"mode": "market_stream", "bar_limit": 390},
        },
    )
    rl_observation = observation_response["rl_observation"]
    observations = rl_observation["observations"]
    last_bar_at = None
    last_action_at = None
    actions = list(skipped_candidates)

    for candidate in active_candidates:
        symbol = str(candidate["symbol"]).upper()
        symbol_observation = observations[symbol]
        decision = symbol_observation["model_decision"]
        last_bar_at = (
            decision.get("latest_usable_bar_ended_at")
            or symbol_observation.get("latest_bar_ended_at")
            or last_bar_at
        )
        if not decision.get("ready"):
            actions.append({"symbol": symbol, "status": "not_ready", "decision": decision})
            continue
        decision_id = str(decision["decision_id"])
        dedupe_key = f"{loaded.config.deployment_key}:{candidate['instruction_id']}:{decision_id}"
        if dedupe_key in processed_decisions:
            actions.append({"symbol": symbol, "status": "already_processed", "decision_id": decision_id})
            continue
        runner_state = runtime_states.get(symbol, RunnerSymbolState())
        vector = assemble_dqn_observation_vector(
            symbol_observation,
            state=runner_state,
            model_side=loaded.config.side,
            path_pad_length=int(rl_observation["feature_schema"]["path_pad_length"]),
            expected_obs_dim=loaded.obs_dim,
        )
        action_name, q_values = choose_action(
            loaded.model,
            loaded.action_names,
            vector,
            runner_state,
        )
        previous_close = symbol_observation["pricing_context"]["prev_close"]
        state_before = translation_state_before(runner_state, loaded.config.side)
        observed_at = decision_observed_at(symbol_observation)
        translation = post_json(
            f"{api_base}/v1/rl/actions/translate",
            {
                "deployment_key": loaded.config.deployment_key,
                "source_instruction_id": candidate["instruction_id"],
                "action_name": action_name,
                "state_before": state_before,
                "observed_at": observed_at,
                "previous_close": previous_close,
                "decision_id": decision_id,
                "submit": bool(execute_virtual and _is_executable_action(action_name)),
                "log_action": True,
            },
        )
        processed_decisions.add(dedupe_key)
        last_action_at = datetime.now(timezone.utc).isoformat()
        actions.append(
            {
                "symbol": symbol,
                "decision_id": decision_id,
                "action_name": action_name,
                "state_before": state_before,
                "runner_state": asdict(runner_state),
                "q_values": q_values,
                "submitted": translation.get("submitted"),
                "action_status": translation.get("translation", {}).get("action_status"),
            }
        )

    heartbeat(
        api_base,
        loaded.config.deployment_key,
        "running",
        last_bar_at=last_bar_at,
        last_action_at=last_action_at,
        metrics={
            "candidate_count": len(candidates),
            "active_candidate_count": len(active_candidates),
            "symbols": active_symbols,
            "actions": actions,
            "execute_virtual": execute_virtual,
            "history_cache_count": len(history_cache),
        },
    )
    print(json.dumps({"deployment_key": loaded.config.deployment_key, "actions": actions}, indent=2), flush=True)


def choose_action(
    model: Any,
    action_names: list[str],
    vector: np.ndarray,
    state: RunnerSymbolState,
) -> tuple[str, list[float]]:
    _require_torch()
    with torch.no_grad():
        tensor = torch.as_tensor(vector, dtype=torch.float32).unsqueeze(0)
        q_values = model(tensor).squeeze(0).cpu().numpy().astype(float)
    mask = valid_action_mask(action_names, state)
    masked = np.where(mask, q_values, -np.inf)
    action_idx = int(np.argmax(masked))
    return action_names[action_idx], [float(value) for value in q_values]


def decision_observed_at(symbol_observation: Mapping[str, Any]) -> str:
    decision = symbol_observation.get("model_decision")
    if not isinstance(decision, Mapping):
        decision = {}
    return str(
        decision.get("latest_usable_bar_ended_at")
        or symbol_observation.get("latest_bar_ended_at")
        or datetime.now(timezone.utc).isoformat()
    )


def load_runtime_states_from_instructions(
    *,
    api_base: str,
    deployment_key: str,
    symbols: list[str],
    side: str,
) -> dict[str, RunnerSymbolState]:
    """Recover per-symbol runner state from translated RL instructions."""

    symbol_set = {symbol.upper() for symbol in symbols}
    payload = get_json(f"{api_base}/v1/instructions?limit=500")
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
            states[symbol] = RunnerSymbolState(
                in_position=False,
                pending_entry_anchor=(
                    "prev_close"
                    if str(metadata.get("rl_action_name") or "").startswith("entry_prevclose_")
                    else None
                ),
                pending_entry_rel_bp=_entry_rel_bp(metadata.get("rl_action_name")),
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


def translation_state_before(state: RunnerSymbolState, side: str) -> str:
    if state.in_position:
        if state.pending_exit_tp_bp is not None:
            return "EXIT_PENDING"
        return "SHORT_OPEN" if side.upper() == "SHORT" else "LONG_OPEN"
    if state.pending_entry_anchor is not None:
        return "ENTRY_PENDING"
    return "FLAT"


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


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _require_torch() -> tuple[Any, Any]:
    if torch is None or nn is None:
        raise RuntimeError(
            "The promoted RL runner needs PyTorch. Install the trader RL extras "
            "for pandas/pyarrow plus a CPU or GPU PyTorch wheel appropriate for "
            "this host, or run the script from the q-training environment."
        )
    return torch, nn


def _q_network_class(nn_module: Any) -> type[Any]:
    class QNetwork(nn_module.Module):
        def __init__(self, obs_dim: int, action_dim: int, hidden_dim: int) -> None:
            super().__init__()
            self.net = nn_module.Sequential(
                nn_module.Linear(obs_dim, hidden_dim),
                nn_module.ReLU(),
                nn_module.Linear(hidden_dim, hidden_dim),
                nn_module.ReLU(),
                nn_module.Linear(hidden_dim, action_dim),
            )

        def forward(self, x: Any) -> Any:
            return self.net(x)

    return QNetwork


def load_model(config: PromotedRLModelArtifact) -> LoadedModel:
    torch_module, nn_module = _require_torch()
    summary = json.loads(config.summary_path.read_text())
    action_names = [str(item) for item in summary["action_names"]]
    if action_names != list(config.action_space):
        raise ValueError(
            f"{config.model_key} action space mismatch between q-training summary and trader registry"
        )
    state_dict = torch_module.load(config.promoted_checkpoint_path, map_location="cpu")
    if isinstance(state_dict, dict) and "model_state_dict" in state_dict:
        state_dict = state_dict["model_state_dict"]
    if isinstance(state_dict, dict) and "state_dict" in state_dict:
        state_dict = state_dict["state_dict"]
    obs_dim = int(state_dict["net.0.weight"].shape[1])
    hidden_dim = int(state_dict["net.0.weight"].shape[0])
    action_dim = int(state_dict["net.4.weight"].shape[0])
    if action_dim != len(action_names):
        raise ValueError(
            f"{config.model_key} checkpoint action dimension {action_dim} "
            f"does not match action names {len(action_names)}"
        )
    q_network_cls = _q_network_class(nn_module)
    model = q_network_cls(obs_dim=obs_dim, action_dim=action_dim, hidden_dim=hidden_dim)
    model.load_state_dict(state_dict, strict=True)
    model.eval()
    static_feature_names = list(read_static_feature_names(config.static_feature_cols_path))
    expected_static_count = summary.get("static_feature_count")
    if expected_static_count is not None and int(expected_static_count) != len(static_feature_names):
        raise ValueError(
            f"{config.model_key} static feature count mismatch: "
            f"summary={expected_static_count} csv={len(static_feature_names)}"
        )
    if obs_dim <= len(static_feature_names):
        raise ValueError(
            f"{config.model_key} observation width {obs_dim} is not large enough "
            f"for {len(static_feature_names)} static features"
        )
    candidate_tape = pd.read_parquet(config.candidate_tape_path)
    candidate_tape = candidate_tape.copy()
    candidate_tape["instrument_norm"] = (
        candidate_tape["instrument"].astype(str).str.upper().str.replace("-", " ", regex=False)
    )
    candidate_tape["date_norm"] = pd.to_datetime(candidate_tape["datetime"]).dt.strftime("%Y-%m-%d")
    return LoadedModel(
        config=config,
        action_names=action_names,
        obs_dim=obs_dim,
        model=model,
        static_feature_names=static_feature_names,
        candidate_tape=candidate_tape,
    )


def static_feature_payload(
    loaded: LoadedModel,
    *,
    candidate: Mapping[str, Any] | None = None,
    symbol: str,
    trade_date: str,
) -> dict[str, Any]:
    candidate_payload = candidate_static_feature_payload(
        loaded,
        candidate=candidate,
        symbol=symbol,
    )
    if candidate_payload is not None:
        return candidate_payload

    normalized_symbol = symbol.upper().replace("-", " ")
    rows = loaded.candidate_tape[
        (loaded.candidate_tape["instrument_norm"] == normalized_symbol)
        & (loaded.candidate_tape["date_norm"] == trade_date)
    ]
    if rows.empty:
        raise ValueError(f"no static feature row for {loaded.config.model_key} {symbol} {trade_date}")
    row = rows.iloc[0]
    values = [float(row[name]) for name in loaded.static_feature_names]
    if any(np.isnan(values)):
        raise ValueError(f"static features contain NaN for {loaded.config.model_key} {symbol} {trade_date}")
    return {
        "feature_names": loaded.static_feature_names,
        "values": values,
        "normalized": True,
        "source": str(loaded.config.candidate_tape_path),
    }


def candidate_static_feature_payload(
    loaded: LoadedModel,
    *,
    candidate: Mapping[str, Any] | None,
    symbol: str,
) -> dict[str, Any] | None:
    raw_payload = extract_candidate_static_features(candidate)
    if raw_payload is None:
        return None
    if isinstance(raw_payload, str):
        try:
            raw_payload = json.loads(raw_payload)
        except json.JSONDecodeError as exc:
            raise ValueError(f"candidate static_features JSON is invalid for {symbol}") from exc
    if not isinstance(raw_payload, Mapping):
        raise ValueError(f"candidate static_features must be an object for {symbol}")

    raw_model_key = raw_payload.get("model_key")
    if raw_model_key is not None and str(raw_model_key) != loaded.config.model_key:
        raise ValueError(
            f"candidate static_features model_key mismatch for {symbol}: "
            f"{raw_model_key!r} != {loaded.config.model_key!r}"
        )

    raw_names = raw_payload.get("feature_names")
    if not isinstance(raw_names, list) or not all(
        isinstance(name, str) and name.strip() for name in raw_names
    ):
        raise ValueError(f"candidate static_features.feature_names must be strings for {symbol}")
    names = [name.strip() for name in raw_names]
    if names != loaded.static_feature_names:
        raise ValueError(
            f"candidate static feature_names mismatch for {loaded.config.model_key} {symbol}: "
            f"got {len(names)} names, expected {len(loaded.static_feature_names)}"
        )

    raw_values = (
        raw_payload.get("values")
        if raw_payload.get("values") is not None
        else raw_payload.get("static_features_norm")
        if raw_payload.get("static_features_norm") is not None
        else raw_payload.get("static_features")
    )
    if not isinstance(raw_values, list):
        raise ValueError(f"candidate static_features.values must be an array for {symbol}")
    if len(raw_values) != len(names):
        raise ValueError(
            f"candidate static feature value count mismatch for {loaded.config.model_key} {symbol}: "
            f"got {len(raw_values)}, expected {len(names)}"
        )
    values = [float(value) for value in raw_values]
    if any(np.isnan(values)) or not all(np.isfinite(values)):
        raise ValueError(f"candidate static features contain non-finite values for {symbol}")

    normalized = bool(raw_payload.get("normalized", True))
    if not normalized:
        raise ValueError(
            f"candidate static features must already be normalized for {loaded.config.model_key} {symbol}"
        )

    return {
        "feature_names": names,
        "values": values,
        "normalized": True,
        "source": str(raw_payload.get("source") or "upstream_candidate_payload"),
    }


def extract_candidate_static_features(
    candidate: Mapping[str, Any] | None,
) -> Any | None:
    if not isinstance(candidate, Mapping):
        return None
    trace = candidate.get("trace")
    metadata = trace.get("metadata") if isinstance(trace, Mapping) else None
    if not isinstance(metadata, Mapping):
        return None
    for key in ("static_features", "rl_static_features", "model_static_features"):
        if key in metadata:
            return metadata[key]
    return None


def history_override_payload(
    *,
    api_base: str,
    loaded: LoadedModel,
    candidate: Mapping[str, Any],
    trade_date: str,
    history_cache: dict[str, Any],
    duration: str,
    bar_size: str,
    timeout: int,
) -> dict[str, Any]:
    request_payload = build_historical_bars_payload(
        candidate,
        trade_date=trade_date,
        duration=duration,
        bar_size=bar_size,
    )
    cache_key = _history_cache_key(
        loaded.config.model_key,
        request_payload,
        trade_date=trade_date,
    )
    cached = history_cache.get(cache_key)
    if isinstance(cached, Mapping) and cached.get("history_override"):
        return dict(cached["history_override"])

    failure_key = f"{cache_key}:failure"
    recent_failure = history_cache.get(failure_key)
    if isinstance(recent_failure, Mapping) and _is_recent_failure(recent_failure):
        raise RuntimeError(
            "recent history backfill failure still cooling down: "
            f"{recent_failure.get('error')}"
        )

    try:
        response = post_json(
            f"{api_base}/v1/market-data/historical-bars?timeout={timeout}",
            request_payload,
            timeout=max(timeout + 5, 30),
        )
        bars = response.get("bars", [])
        override = build_history_override_from_source_bars(
            symbol=str(request_payload["symbol"]),
            source_bars=bars,
            target_date=trade_date,
            observation_contract={
                "bar_family": "phase1_intraday_ohlc_v1",
                "bar_interval": "5m",
                "update_cadence": "1m",
                "decision_cadence": "5m",
                "session_timezone": "Europe/Stockholm",
                "session_open_local": "09:00",
                "session_close_local": "17:30",
                "include_market_context": True,
                "include_vol_normalized_intraday_state": True,
            },
        )
    except Exception as exc:
        history_cache[failure_key] = {
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "error": str(exc),
        }
        raise

    history_cache[cache_key] = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "request": request_payload,
        "bar_count": len(bars),
        "history_override": override,
    }
    history_cache.pop(failure_key, None)
    return override


def build_historical_bars_payload(
    candidate: Mapping[str, Any],
    *,
    trade_date: str,
    duration: str,
    bar_size: str,
) -> dict[str, Any]:
    instrument = candidate_instrument(candidate)
    symbol = str(candidate.get("symbol") or instrument.get("symbol") or "").upper()
    if not symbol:
        raise ValueError("candidate symbol is required for historical backfill")
    target_date = date.fromisoformat(trade_date)
    end_at = datetime.combine(
        target_date,
        datetime.strptime("09:00", "%H:%M").time(),
        tzinfo=STOCKHOLM_TZ,
    )
    return {
        "symbol": symbol,
        "security_type": str(instrument.get("security_type") or "STK").upper(),
        "exchange": str(instrument.get("exchange") or candidate.get("exchange") or "SMART").upper(),
        "primary_exchange": str(instrument.get("primary_exchange") or "SFB").upper(),
        "currency": str(instrument.get("currency") or candidate.get("currency") or "SEK").upper(),
        "isin": instrument.get("isin"),
        "duration": duration,
        "bar_size": bar_size,
        "what_to_show": "TRADES",
        "use_rth": True,
        "end_at": end_at.isoformat(),
    }


def candidate_instrument(candidate: Mapping[str, Any]) -> Mapping[str, Any]:
    nested = candidate.get("candidate")
    if isinstance(nested, Mapping):
        instrument = nested.get("instrument")
        if isinstance(instrument, Mapping):
            return instrument
    return {}


def _history_cache_key(
    model_key: str,
    request_payload: Mapping[str, Any],
    *,
    trade_date: str,
) -> str:
    parts = {
        key: request_payload.get(key)
        for key in (
            "symbol",
            "security_type",
            "exchange",
            "primary_exchange",
            "currency",
            "isin",
            "duration",
            "bar_size",
            "what_to_show",
            "use_rth",
        )
    }
    parts["model_key"] = model_key
    parts["trade_date"] = trade_date
    return json.dumps(parts, sort_keys=True)


def _is_recent_failure(payload: Mapping[str, Any], *, cooldown_seconds: int = 600) -> bool:
    try:
        failed_at = datetime.fromisoformat(str(payload.get("failed_at")).replace("Z", "+00:00"))
    except ValueError:
        return False
    if failed_at.tzinfo is None:
        failed_at = failed_at.replace(tzinfo=timezone.utc)
    age_seconds = (datetime.now(timezone.utc) - failed_at.astimezone(timezone.utc)).total_seconds()
    return age_seconds < cooldown_seconds


def subscribe_symbols(api_base: str, symbols: list[str], *, market_data_type: str) -> None:
    post_json(
        f"{api_base}/v1/market-data/stream/subscribe",
        {
            "symbols": symbols,
            "exchange": "SMART",
            "primary_exchange": "SFB",
            "currency": "SEK",
            "market_data_type": market_data_type,
            "replace": True,
        },
    )


def heartbeat_stream_failure(
    *,
    api_base: str,
    loaded_models: Mapping[str, LoadedModel],
    candidates: list[Mapping[str, Any]],
    error: str,
    market_data_type: str,
    stop_stream_on_empty: bool,
) -> None:
    """Publish a truthful runner heartbeat when stream setup fails before bars exist."""

    if stop_stream_on_empty:
        try:
            post_json(f"{api_base}/v1/market-data/stream/stop", {})
        except ApiError:
            pass
    candidate_models = {str(candidate.get("model_id")) for candidate in candidates}
    for loaded in loaded_models.values():
        if loaded.config.model_key not in candidate_models:
            heartbeat(
                api_base,
                loaded.config.deployment_key,
                "running",
                runtime_error=None,
                metrics={"candidate_count": 0, "runner_mode": "idle"},
            )
            continue
        model_candidates = [
            candidate
            for candidate in candidates
            if candidate.get("model_id") == loaded.config.model_key
        ]
        heartbeat(
            api_base,
            loaded.config.deployment_key,
            "degraded",
            runtime_error="market stream unavailable for active RL candidates",
            metrics={
                "candidate_count": len(model_candidates),
                "symbols": sorted({str(candidate["symbol"]).upper() for candidate in model_candidates}),
                "market_data_type": market_data_type,
                "stream_error": error,
                "stopped_stream": stop_stream_on_empty,
            },
        )


def heartbeat(
    api_base: str,
    deployment_key: str,
    status: str,
    *,
    runtime_error: str | None = None,
    last_bar_at: str | None = None,
    last_action_at: str | None = None,
    metrics: Mapping[str, Any] | None = None,
) -> None:
    post_json(
        f"{api_base}/v1/rl/deployments/{deployment_key}/heartbeat",
        {
            "status": status,
            "last_seen_at": datetime.now(timezone.utc).isoformat(),
            "last_bar_at": last_bar_at,
            "last_action_at": last_action_at,
            "runtime_error": runtime_error,
            "metrics": dict(metrics or {}),
        },
    )


def get_json(url: str, *, timeout: int = 30) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    return _open_json(request, timeout=timeout)


def post_json(
    url: str,
    payload: Mapping[str, Any],
    *,
    timeout: int = 30,
) -> dict[str, Any]:
    encoded = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=encoded,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    return _open_json(request, timeout=timeout)


def _open_json(request: urllib.request.Request, *, timeout: int) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise ApiError(f"{request.full_url} -> HTTP {exc.code}: {detail}") from exc


def _is_executable_action(action_name: str) -> bool:
    return (
        action_name == "market_entry"
        or action_name.startswith("entry_prevclose_")
        or action_name in {"cancel_entry", "exit_market", "clear_exit"}
        or action_name.startswith("exit_tp_")
    )


def _load_processed_decisions(path: Path) -> set[str]:
    if not path.exists():
        return set()
    payload = json.loads(path.read_text())
    if not isinstance(payload, list):
        return set()
    return {str(item) for item in payload}


def _save_processed_decisions(path: Path, values: set[str]) -> None:
    path.write_text(json.dumps(sorted(values), indent=2) + "\n")


def _load_history_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text())
    return payload if isinstance(payload, dict) else {}


def _save_history_cache(path: Path, values: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(dict(values), indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    raise SystemExit(main())
