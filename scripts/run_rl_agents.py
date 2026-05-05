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
DEFAULT_BENCHMARK_SYMBOLS = ("OMXS30",)
BENCHMARK_STREAM_CONTRACTS = {
    "OMXS30": {
        "symbol": "OMXS30",
        "security_type": "IND",
        "exchange": "OMS",
        "currency": "SEK",
        "primary_exchange": "",
    }
}
DEFAULT_CANDIDATE_REASON_CODES = (
    "rl_model_routed_selected_candidate",
    "rl_model_routed_candidate",
    "rl_model_routed_candidate_tape_selected",
)


@dataclass(slots=True)
class LoadedModel:
    config: PromotedRLModelArtifact
    action_names: list[str]
    obs_dim: int
    model: Any
    static_feature_names: list[str]


@dataclass(frozen=True, slots=True)
class LoadedDeployment:
    deployment_key: str
    model_key: str
    account_key: str
    book_key: str
    mode: str
    loaded: LoadedModel


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


def parse_reason_code_filter(raw_value: str | None) -> set[str]:
    if raw_value is None:
        return set(DEFAULT_CANDIDATE_REASON_CODES)
    return {
        item.strip()
        for item in raw_value.replace("\n", ",").split(",")
        if item.strip()
    }


def load_running_deployments(
    api_base: str,
    loaded_models: Mapping[str, LoadedModel],
    *,
    account_mode: str,
) -> dict[str, LoadedDeployment]:
    """Bind deployed model artifacts to currently running deployment rows.

    The runner owns deployments, not just model keys. That keeps virtual and
    future paper/live deployments of the same model from sharing state or
    accidentally consuming each other's candidates.
    """

    payload = get_json(f"{api_base}/v1/read/rl-dashboard")
    dashboard = payload.get("rl_dashboard", {})
    deployments = dashboard.get("deployments", [])
    if not isinstance(deployments, list):
        raise ValueError("rl_dashboard.deployments must be an array")

    active: dict[str, LoadedDeployment] = {}
    for row in deployments:
        if not isinstance(row, Mapping):
            continue
        model_key = str(row.get("model_key") or "").strip()
        loaded = loaded_models.get(model_key)
        if loaded is None:
            continue
        mode = str(row.get("mode") or "").strip().lower()
        if not _mode_selected(mode, account_mode):
            continue
        if str(row.get("status") or "").strip().lower() != "running":
            continue
        deployment_key = str(row.get("deployment_key") or "").strip()
        account_key = str(row.get("account_key") or "").strip().upper()
        book_key = str(row.get("book_key") or "").strip().lower()
        if not deployment_key or not account_key or not book_key:
            continue
        active[deployment_key] = LoadedDeployment(
            deployment_key=deployment_key,
            model_key=model_key,
            account_key=account_key,
            book_key=book_key,
            mode=mode,
            loaded=loaded,
        )
    return active


def legacy_loaded_deployments(
    loaded_models: Mapping[str, LoadedModel],
    *,
    account_mode: str,
) -> dict[str, LoadedDeployment]:
    """Compatibility path for unit tests and older APIs without dashboard rows."""

    deployments: dict[str, LoadedDeployment] = {}
    for loaded in loaded_models.values():
        mode = "virtual" if account_mode == "all" else account_mode
        deployment_key = str(loaded.config.deployment_key)
        deployments[deployment_key] = LoadedDeployment(
            deployment_key=deployment_key,
            model_key=str(loaded.config.model_key),
            account_key="",
            book_key="",
            mode=mode,
            loaded=loaded,
        )
    return deployments


def group_candidates_by_deployment(
    candidates: list[Mapping[str, Any]],
    deployments: Mapping[str, LoadedDeployment],
    *,
    account_mode: str,
) -> dict[str, list[Mapping[str, Any]]]:
    grouped = {deployment_key: [] for deployment_key in deployments}
    for candidate in candidates:
        for deployment in deployments.values():
            if candidate_matches_deployment(
                candidate,
                deployment,
                account_mode=account_mode,
            ):
                grouped[deployment.deployment_key].append(candidate)
                break
    return grouped


def candidate_matches_deployment(
    candidate: Mapping[str, Any],
    deployment: LoadedDeployment,
    *,
    account_mode: str,
) -> bool:
    if str(candidate.get("model_id") or "") != deployment.model_key:
        return False
    if deployment.account_key and str(candidate.get("account_key") or "").upper() != deployment.account_key:
        return False
    if deployment.book_key and str(candidate.get("book_key") or "").lower() != deployment.book_key:
        return False
    if deployment.mode == "virtual":
        return candidate.get("is_virtual") is True
    if deployment.mode in {"paper", "live"}:
        return candidate.get("is_virtual") is not True
    return _candidate_mode_selected(candidate, account_mode)


def _mode_selected(mode: str, account_mode: str) -> bool:
    normalized = account_mode.strip().lower()
    return normalized == "all" or mode == normalized


def _candidate_mode_selected(candidate: Mapping[str, Any], account_mode: str) -> bool:
    normalized = account_mode.strip().lower()
    if normalized == "all":
        return True
    if normalized == "virtual":
        return candidate.get("is_virtual") is True
    if normalized in {"paper", "live"}:
        return candidate.get("is_virtual") is not True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Run promoted virtual RL agents against the trader API.")
    parser.add_argument("--api-base", default="http://quant.geisler.se:8000")
    parser.add_argument("--poll-seconds", type=float, default=60.0)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--execute-virtual", action="store_true")
    parser.add_argument(
        "--execute-broker",
        action="store_true",
        help=(
            "Allow translated actions to submit for paper/live deployments. "
            "This is intentionally separate from --execute-virtual."
        ),
    )
    parser.add_argument("--include-smoke", action="store_true")
    parser.add_argument("--stop-stream-on-empty", action="store_true")
    parser.add_argument("--market-data-type", default="LIVE")
    parser.add_argument(
        "--account-mode",
        choices=("virtual", "paper", "live", "all"),
        default="virtual",
        help="Which running RL deployments the runner should own.",
    )
    parser.add_argument(
        "--candidate-reason-code",
        default=",".join(DEFAULT_CANDIDATE_REASON_CODES),
        help=(
            "Comma-separated trace.reason_code allow-list for model-routed candidates. "
            "Use an empty string to accept every reason code."
        ),
    )
    parser.add_argument("--state-file", default=".rl_runner_state.json")
    parser.add_argument("--history-cache-file", default=".rl_runner_history_cache.json")
    parser.add_argument(
        "--history-duration",
        default="5 D",
        help=(
            "IBKR historical warmup window for live RL observations. Keep this "
            "small because large 1-minute Stockholm requests are slow and can be rejected."
        ),
    )
    parser.add_argument("--history-bar-size", default="1 min")
    parser.add_argument("--history-timeout", type=int, default=45)
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
    model_configs = {artifact.model_key: artifact for artifact in promoted_rl_models()}
    loaded_models = {key: load_model(config) for key, config in model_configs.items()}
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
            loaded_deployments = load_running_deployments(
                args.api_base.rstrip("/"),
                loaded_models,
                account_mode=args.account_mode,
            )
            run_once(
                api_base=args.api_base.rstrip("/"),
                limit=args.limit,
                loaded_models=loaded_models,
                loaded_deployments=loaded_deployments,
                processed_decisions=processed_decisions,
                execute_virtual=args.execute_virtual,
                execute_broker=args.execute_broker,
                include_smoke=args.include_smoke,
                stop_stream_on_empty=args.stop_stream_on_empty,
                market_data_type=args.market_data_type,
                account_mode=args.account_mode,
                candidate_reason_codes=parse_reason_code_filter(args.candidate_reason_code),
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
    loaded_deployments: Mapping[str, LoadedDeployment] | None = None,
    processed_decisions: set[str],
    execute_virtual: bool,
    execute_broker: bool = False,
    include_smoke: bool,
    stop_stream_on_empty: bool,
    market_data_type: str,
    account_mode: str = "virtual",
    candidate_reason_codes: set[str],
    trade_date: str,
    history_cache: dict[str, Any],
    history_duration: str,
    history_bar_size: str,
    history_timeout: int,
    benchmark_symbols: list[str],
) -> None:
    active_deployments = dict(
        loaded_deployments
        if loaded_deployments is not None
        else legacy_loaded_deployments(
            loaded_models,
            account_mode=account_mode,
        )
    )
    if not active_deployments:
        print(
            f"No running RL deployments found for account_mode={account_mode}.",
            flush=True,
        )
        return

    raw_candidates = get_json(f"{api_base}/v1/rl/candidates?limit={limit}")["candidates"]
    candidate_pool = [
        candidate
        for candidate in raw_candidates
        if candidate.get("model_id") in loaded_models
        and candidate.get("trace", {}).get("trade_date") == trade_date
        and (
            not candidate_reason_codes
            or candidate.get("trace", {}).get("reason_code") in candidate_reason_codes
        )
        and (include_smoke or candidate.get("source", {}).get("system") != "codex-smoke")
    ]
    candidates_by_deployment = group_candidates_by_deployment(
        candidate_pool,
        active_deployments,
        account_mode=account_mode,
    )
    candidates = [
        candidate
        for deployment_candidates in candidates_by_deployment.values()
        for candidate in deployment_candidates
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
        for deployment in active_deployments.values():
            heartbeat(
                api_base,
                deployment.deployment_key,
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
            loaded_deployments=active_deployments,
            candidates_by_deployment=candidates_by_deployment,
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
    if not symbols_with_bars and stop_stream_on_empty:
        post_json(f"{api_base}/v1/market-data/stream/stop", {})

    for deployment in active_deployments.values():
        deployment_candidates = candidates_by_deployment.get(deployment.deployment_key, [])
        if not deployment_candidates:
            heartbeat(
                api_base,
                deployment.deployment_key,
                "running",
                runtime_error=None,
                metrics={
                    "candidate_count": len(deployment_candidates),
                    "runner_mode": "no_candidates_with_stream_bars",
                },
            )
            continue
        execute_actions = (
            deployment.mode == "virtual"
            and execute_virtual
            or deployment.mode in {"paper", "live"}
            and execute_broker
        )
        run_model_candidates(
            api_base=api_base,
            loaded=deployment.loaded,
            deployment_key=deployment.deployment_key,
            deployment_mode=deployment.mode,
            candidates=deployment_candidates,
            processed_decisions=processed_decisions,
            execute_actions=execute_actions,
            history_cache=history_cache,
            history_duration=history_duration,
            history_bar_size=history_bar_size,
            history_timeout=history_timeout,
            stream_bar_ready_symbols=symbols_with_bars,
        )


def run_model_candidates(
    *,
    api_base: str,
    loaded: LoadedModel,
    deployment_key: str,
    deployment_mode: str,
    candidates: list[Mapping[str, Any]],
    processed_decisions: set[str],
    execute_actions: bool,
    history_cache: dict[str, Any],
    history_duration: str,
    history_bar_size: str,
    history_timeout: int,
    stream_bar_ready_symbols: set[str] | None = None,
) -> None:
    symbols = sorted({str(candidate["symbol"]).upper() for candidate in candidates})
    runtime_states = load_runtime_states_from_instructions(
        api_base=api_base,
        deployment_key=deployment_key,
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
            deployment_key,
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
                    "deployment_key": deployment_key,
                    "actions": skipped_candidates,
                },
                indent=2,
            ),
            flush=True,
        )
        return

    active_symbols = sorted({str(candidate["symbol"]).upper() for candidate in active_candidates})

    observation_response = post_json(
        f"{api_base}/v1/rl/observations/build?timeout={history_timeout}",
        {
            "deployment_key": loaded.config.deployment_key,
            "symbols": active_symbols,
            "history_overrides": history_overrides,
            "static_features": static_features,
            "fetch": {
                "mode": "market_stream",
                "bar_limit": 390,
                "backfill_missing": True,
                "backfill_duration": "1 D",
                "backfill_bar_size": "1 min",
                "instruments": {
                    str(candidate["symbol"]).upper(): dict(candidate_instrument(candidate))
                    for candidate in active_candidates
                },
            },
        },
        timeout=max(history_timeout + 15, 60),
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
        dedupe_key = f"{deployment_key}:{candidate['instruction_id']}:{decision_id}"
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
        diagnostics = action_diagnostics(
            loaded.action_names,
            q_values,
            runner_state,
            chosen_action=action_name,
        )
        previous_close = symbol_observation["pricing_context"]["prev_close"]
        state_before = translation_state_before(runner_state, loaded.config.side)
        observed_at = decision_observed_at(symbol_observation)
        translation = post_json(
            f"{api_base}/v1/rl/actions/translate",
            {
                "deployment_key": deployment_key,
                "source_instruction_id": candidate["instruction_id"],
                "action_name": action_name,
                "state_before": state_before,
                "observed_at": observed_at,
                "previous_close": previous_close,
                "decision_id": decision_id,
                "submit": bool(execute_actions and _is_executable_action(action_name)),
                "log_action": True,
                "model_diagnostics": diagnostics,
            },
        )
        virtual_quote_result = None
        if deployment_mode == "virtual":
            try:
                virtual_quote_result = publish_virtual_decision_bar(
                    api_base,
                    candidate=candidate,
                    symbol_observation=symbol_observation,
                    deployment_key=deployment_key,
                    action_name=action_name,
                    decision_id=decision_id,
                )
            except ApiError as exc:
                virtual_quote_result = {"accepted": False, "error": str(exc)}
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
                "action_margin": diagnostics.get("action_margin"),
                "submitted": translation.get("submitted"),
                "action_status": translation.get("translation", {}).get("action_status"),
                "virtual_decision_bar": virtual_quote_result,
            }
        )

    heartbeat(
        api_base,
        deployment_key,
        "running",
        last_bar_at=last_bar_at,
        last_action_at=last_action_at,
        metrics={
            "candidate_count": len(candidates),
            "active_candidate_count": len(active_candidates),
            "stream_bar_ready_candidate_count": len(
                [
                    candidate
                    for candidate in active_candidates
                    if str(candidate["symbol"]).upper() in (stream_bar_ready_symbols or set())
                ]
            ),
            "backfilled_symbol_count": len(observation_response.get("fetched_symbols", [])),
            "symbols": active_symbols,
            "actions": actions,
            "deployment_mode": deployment_mode,
            "execute_actions": execute_actions,
            "history_cache_count": len(history_cache),
        },
    )
    print(json.dumps({"deployment_key": deployment_key, "actions": actions}, indent=2), flush=True)


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


def action_diagnostics(
    action_names: list[str],
    q_values: list[float],
    state: RunnerSymbolState,
    *,
    chosen_action: str,
) -> dict[str, Any]:
    mask = valid_action_mask(action_names, state)
    valid_actions = [
        {
            "action_name": action_names[idx],
            "q_value": float(q_values[idx]),
        }
        for idx, allowed in enumerate(mask)
        if bool(allowed)
    ]
    ranked = sorted(valid_actions, key=lambda item: item["q_value"], reverse=True)
    best = ranked[0]["q_value"] if ranked else None
    second = ranked[1]["q_value"] if len(ranked) > 1 else None
    return {
        "action_names": list(action_names),
        "q_values": [float(value) for value in q_values],
        "valid_action_mask": [bool(value) for value in mask],
        "valid_actions_ranked": ranked,
        "chosen_action": chosen_action,
        "action_margin": (
            float(best - second)
            if best is not None and second is not None
            else None
        ),
    }


def publish_virtual_decision_bar(
    api_base: str,
    *,
    candidate: Mapping[str, Any],
    symbol_observation: Mapping[str, Any],
    deployment_key: str,
    action_name: str,
    decision_id: str,
) -> dict[str, Any] | None:
    bar = latest_decision_phase1_bar(symbol_observation)
    if bar is None:
        return None
    instrument = candidate_instrument(candidate)
    symbol = str(candidate.get("symbol") or instrument.get("symbol") or "").upper()
    if not symbol:
        return None
    observed_at = (
        str(bar.get("ended_at") or bar.get("timestamp") or bar.get("started_at") or "")
        or decision_observed_at(symbol_observation)
    )
    close_price = bar.get("close")
    if close_price is None:
        return None
    payload = {
        "account_key": candidate.get("account_key"),
        "observed_at": observed_at,
        "symbol": symbol,
        "security_type": str(instrument.get("security_type") or "STK").upper(),
        "exchange": str(instrument.get("exchange") or candidate.get("exchange") or "SMART").upper(),
        "currency": str(instrument.get("currency") or candidate.get("currency") or "SEK").upper(),
        "primary_exchange": instrument.get("primary_exchange"),
        "local_symbol": instrument.get("local_symbol"),
        "bid_price": close_price,
        "ask_price": close_price,
        "last_price": close_price,
        "source": "rl_decision_bar",
        "latest_stream_bar": {
            "timestamp": bar.get("started_at") or bar.get("timestamp"),
            "open": bar.get("open"),
            "high": bar.get("high"),
            "low": bar.get("low"),
            "close": bar.get("close"),
            "ended_at": bar.get("ended_at"),
            "complete": bar.get("complete"),
            "source": "rl_phase1_decision_bar",
        },
        "metadata": {
            "deployment_key": deployment_key,
            "source_instruction_id": candidate.get("instruction_id"),
            "decision_id": decision_id,
            "action_name": action_name,
            "purpose": "virtual_same_bar_fill_parity",
        },
    }
    return post_json(f"{api_base}/v1/virtual/market-watch", payload)


def latest_decision_phase1_bar(symbol_observation: Mapping[str, Any]) -> Mapping[str, Any] | None:
    phase1_bars = symbol_observation.get("phase1_bars")
    if not isinstance(phase1_bars, list) or not phase1_bars:
        return None
    decision = symbol_observation.get("model_decision")
    decision_ended_at = (
        decision.get("latest_usable_bar_ended_at")
        if isinstance(decision, Mapping)
        else None
    )
    if decision_ended_at:
        for bar in reversed(phase1_bars):
            if isinstance(bar, Mapping) and str(bar.get("ended_at")) == str(decision_ended_at):
                return bar
    for bar in reversed(phase1_bars):
        if isinstance(bar, Mapping) and bool(bar.get("complete", True)):
            return bar
    last = phase1_bars[-1]
    return last if isinstance(last, Mapping) else None


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
            "plus a CPU or GPU PyTorch wheel appropriate for this host."
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
            f"{config.model_key} action space mismatch between deployed bundle summary and trader registry"
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
    return LoadedModel(
        config=config,
        action_names=action_names,
        obs_dim=obs_dim,
        model=model,
        static_feature_names=static_feature_names,
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

    raise ValueError(
        f"missing required instruction static_features for {loaded.config.model_key} "
        f"{symbol} {trade_date}; production RL candidates must carry "
        "trace.metadata.static_features"
    )


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
    exchange, primary_exchange = ibkr_historical_exchange(
        exchange=instrument.get("exchange") or candidate.get("exchange"),
        primary_exchange=instrument.get("primary_exchange"),
    )
    target_date = date.fromisoformat(trade_date)
    end_at = datetime.combine(
        target_date,
        datetime.strptime("09:00", "%H:%M").time(),
        tzinfo=STOCKHOLM_TZ,
    )
    return {
        "symbol": symbol,
        "security_type": str(instrument.get("security_type") or "STK").upper(),
        "exchange": exchange,
        "primary_exchange": primary_exchange,
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
        payload = nested.get("payload")
        if isinstance(payload, Mapping):
            instruction = payload.get("instruction")
            if isinstance(instruction, Mapping):
                instrument = instruction.get("instrument")
                if isinstance(instrument, Mapping):
                    return instrument
        instrument = nested.get("instrument")
        if isinstance(instrument, Mapping):
            return instrument
    payload = candidate.get("payload")
    if isinstance(payload, Mapping):
        instruction = payload.get("instruction")
        if isinstance(instruction, Mapping):
            instrument = instruction.get("instrument")
            if isinstance(instrument, Mapping):
                return instrument
    return {}


def ibkr_historical_exchange(
    *,
    exchange: Any,
    primary_exchange: Any,
) -> tuple[str, str | None]:
    raw_exchange = str(exchange or "").strip().upper()
    raw_primary = str(primary_exchange or "").strip().upper()
    if raw_exchange in {"", "XSTO", "STO", "STOCKHOLM"}:
        return "SMART", raw_primary or "SFB"
    return raw_exchange, raw_primary or None


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
    contracts: list[dict[str, Any]] = []
    for symbol in symbols:
        normalized_symbol = str(symbol).strip().upper()
        benchmark_contract = BENCHMARK_STREAM_CONTRACTS.get(normalized_symbol)
        if benchmark_contract is not None:
            contracts.append(dict(benchmark_contract))
            continue
        contracts.append(
            {
                "symbol": normalized_symbol,
                "security_type": "STK",
                "exchange": "SMART",
                "primary_exchange": "SFB",
                "currency": "SEK",
            }
        )
    post_json(
        f"{api_base}/v1/market-data/stream/subscribe",
        {
            "contracts": contracts,
            "market_data_type": market_data_type,
            "replace": False,
        },
    )


def heartbeat_stream_failure(
    *,
    api_base: str,
    loaded_deployments: Mapping[str, LoadedDeployment],
    candidates_by_deployment: Mapping[str, list[Mapping[str, Any]]],
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
    for deployment in loaded_deployments.values():
        deployment_candidates = candidates_by_deployment.get(deployment.deployment_key, [])
        if not deployment_candidates:
            heartbeat(
                api_base,
                deployment.deployment_key,
                "running",
                runtime_error=None,
                metrics={"candidate_count": 0, "runner_mode": "idle"},
            )
            continue
        heartbeat(
            api_base,
            deployment.deployment_key,
            "degraded",
            runtime_error="market stream unavailable for active RL candidates",
            metrics={
                "candidate_count": len(deployment_candidates),
                "symbols": sorted(
                    {
                        str(candidate["symbol"]).upper()
                        for candidate in deployment_candidates
                    }
                ),
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
