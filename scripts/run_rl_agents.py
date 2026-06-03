#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Mapping

import numpy as np

from ibkr_trader.rl import runner_decisions as _decisions
from ibkr_trader.rl import runner_deployments as _deployments
from ibkr_trader.rl import runner_history as _history
from ibkr_trader.rl import runner_loop as _loop
from ibkr_trader.rl import runner_runtime_state as _runtime_state
from ibkr_trader.rl import runner_stream as _stream
from ibkr_trader.rl.inference_vector import RunnerSymbolState
from ibkr_trader.rl.inference_vector import assemble_dqn_observation_vector
from ibkr_trader.rl.model_artifacts import promoted_rl_models
from ibkr_trader.rl.observations import HISTORY_FEATURE_NAMES
from ibkr_trader.rl.runner_decisions import _elapsed_seconds
from ibkr_trader.rl.runner_decisions import _finalize_timing_metrics
from ibkr_trader.rl.runner_decisions import action_diagnostics
from ibkr_trader.rl.runner_decisions import action_distribution_metrics
from ibkr_trader.rl.runner_decisions import build_stream_symbol_plan
from ibkr_trader.rl.runner_decisions import classify_decision_bar_freshness
from ibkr_trader.rl.runner_decisions import decision_observed_at as _decision_observed_at_impl
from ibkr_trader.rl.runner_decisions import expected_decision_bar_ended_at
from ibkr_trader.rl.runner_decisions import latest_decision_phase1_bar
from ibkr_trader.rl.runner_decisions import publish_virtual_decision_bar as _publish_virtual_decision_bar_impl
from ibkr_trader.rl.runner_decisions import trade_date_from_candidates
from ibkr_trader.rl.runner_decisions import choose_action
from ibkr_trader.rl.runner_deployments import candidate_matches_deployment
from ibkr_trader.rl.runner_deployments import group_candidates_by_deployment
from ibkr_trader.rl.runner_deployments import legacy_loaded_deployments
from ibkr_trader.rl.runner_deployments import load_running_deployments as _load_running_deployments_impl
from ibkr_trader.rl.runner_deployments import parse_reason_code_filter
from ibkr_trader.rl.runner_deployments import parse_symbol_list
from ibkr_trader.rl.runner_history import build_historical_bars_payload
from ibkr_trader.rl.runner_history import candidate_instrument
from ibkr_trader.rl.runner_history import candidate_metadata_history_override_payload
from ibkr_trader.rl.runner_history import candidate_trace_metadata
from ibkr_trader.rl.runner_history import history_override_payload as _history_override_payload_impl
from ibkr_trader.rl.runner_history import ibkr_historical_exchange
from ibkr_trader.rl.runner_history import metadata_history_override_payload
from ibkr_trader.rl.runner_http import ApiError
from ibkr_trader.rl.runner_http import _is_executable_action
from ibkr_trader.rl.runner_http import _load_history_cache
from ibkr_trader.rl.runner_http import _load_processed_decisions
from ibkr_trader.rl.runner_http import _open_json
from ibkr_trader.rl.runner_http import _save_history_cache
from ibkr_trader.rl.runner_http import _save_processed_decisions
from ibkr_trader.rl.runner_http import get_json
from ibkr_trader.rl.runner_http import post_json
from ibkr_trader.rl.runner_loop import run_model_candidates as _run_model_candidates_impl
from ibkr_trader.rl.runner_loop import run_once as _run_once_impl
from ibkr_trader.rl.runner_model import _q_network_class
from ibkr_trader.rl.runner_model import _require_torch
from ibkr_trader.rl.runner_model import candidate_static_feature_payload
from ibkr_trader.rl.runner_model import extract_candidate_static_features
from ibkr_trader.rl.runner_model import load_model
from ibkr_trader.rl.runner_model import load_static_feature_normalization
from ibkr_trader.rl.runner_model import nn
from ibkr_trader.rl.runner_model import static_feature_payload
from ibkr_trader.rl.runner_model import torch
from ibkr_trader.rl.runner_runtime_state import load_runtime_state_context as _load_runtime_state_context_impl
from ibkr_trader.rl.runner_runtime_state import load_runtime_states_from_instructions as _load_runtime_states_from_instructions_impl
from ibkr_trader.rl.runner_runtime_state import _api_error_is_conflict
from ibkr_trader.rl.runner_runtime_state import translation_state_before
from ibkr_trader.rl.runner_stream import heartbeat as _heartbeat_impl
from ibkr_trader.rl.runner_stream import heartbeat_stream_failure as _heartbeat_stream_failure_impl
from ibkr_trader.rl.runner_stream import publish_desired_stream_symbols as _publish_desired_stream_symbols_impl
from ibkr_trader.rl.runner_stream import stream_desired_state_needs_publish
from ibkr_trader.rl.runner_stream import stream_subscription_needs_repair
from ibkr_trader.rl.runner_stream import stream_subscription_pending_symbols
from ibkr_trader.rl.runner_stream import subscribe_symbols as _subscribe_symbols_impl
from ibkr_trader.rl.runner_types import DEFAULT_BENCHMARK_SYMBOLS
from ibkr_trader.rl.runner_types import DEFAULT_CANDIDATE_REASON_CODES
from ibkr_trader.rl.runner_types import DEFAULT_MAX_STREAM_SYMBOLS
from ibkr_trader.rl.runner_types import DEFAULT_STREAM_WARNING_SYMBOLS
from ibkr_trader.rl.runner_types import LoadedDeployment
from ibkr_trader.rl.runner_types import LoadedModel
from ibkr_trader.rl.runner_types import RuntimeStateContext
from ibkr_trader.rl.runner_types import STOCKHOLM_TZ


@contextmanager
def _patched_runner_modules() -> Iterator[None]:
    patches: list[tuple[Any, str, Any]] = [
        (_deployments, "get_json", get_json),
        (_runtime_state, "get_json", get_json),
        (_runtime_state, "load_runtime_states_from_instructions", load_runtime_states_from_instructions),
        (_history, "post_json", post_json),
        (_decisions, "post_json", post_json),
        (_decisions, "candidate_instrument", candidate_instrument),
        (_stream, "post_json", post_json),
        (_stream, "heartbeat", heartbeat),
        (_loop, "get_json", get_json),
        (_loop, "post_json", post_json),
        (_loop, "legacy_loaded_deployments", legacy_loaded_deployments),
        (_loop, "group_candidates_by_deployment", group_candidates_by_deployment),
        (_loop, "build_stream_symbol_plan", build_stream_symbol_plan),
        (_loop, "publish_desired_stream_symbols", publish_desired_stream_symbols),
        (_loop, "stream_desired_state_needs_publish", stream_desired_state_needs_publish),
        (_loop, "stream_subscription_pending_symbols", stream_subscription_pending_symbols),
        (_loop, "heartbeat_stream_failure", heartbeat_stream_failure),
        (_loop, "heartbeat", heartbeat),
        (_loop, "run_model_candidates", run_model_candidates),
        (_loop, "load_runtime_state_context", load_runtime_state_context),
        (_loop, "static_feature_payload", static_feature_payload),
        (_loop, "history_override_payload", history_override_payload),
        (_loop, "expected_decision_bar_ended_at", expected_decision_bar_ended_at),
        (_loop, "trade_date_from_candidates", trade_date_from_candidates),
        (_loop, "classify_decision_bar_freshness", classify_decision_bar_freshness),
            (_loop, "choose_action", choose_action),
            (_loop, "assemble_dqn_observation_vector", assemble_dqn_observation_vector),
            (_loop, "action_diagnostics", action_diagnostics),
        (_loop, "translation_state_before", translation_state_before),
        (_loop, "publish_virtual_decision_bar", publish_virtual_decision_bar),
        (_loop, "decision_observed_at", decision_observed_at),
        (_loop, "candidate_instrument", candidate_instrument),
            (_loop, "action_distribution_metrics", action_distribution_metrics),
            (_loop, "_is_executable_action", _is_executable_action),
            (_loop, "_api_error_is_conflict", _api_error_is_conflict),
        ]
    originals = [(module, name, getattr(module, name)) for module, name, _ in patches]
    try:
        for module, name, value in patches:
            setattr(module, name, value)
        yield
    finally:
        for module, name, original in originals:
            setattr(module, name, original)


def load_running_deployments(
    api_base: str,
    loaded_models: Mapping[str, LoadedModel],
    *,
    account_mode: str,
) -> dict[str, LoadedDeployment]:
    with _patched_runner_modules():
        return _load_running_deployments_impl(
            api_base,
            loaded_models,
            account_mode=account_mode,
        )


def load_runtime_state_context(
    *,
    api_base: str,
    deployment_key: str,
    symbols: list[str],
    side: str,
) -> RuntimeStateContext:
    with _patched_runner_modules():
        return _load_runtime_state_context_impl(
            api_base=api_base,
            deployment_key=deployment_key,
            symbols=symbols,
            side=side,
        )


def load_runtime_states_from_instructions(
    *,
    api_base: str,
    deployment_key: str,
    symbols: list[str],
    side: str,
) -> dict[str, RunnerSymbolState]:
    with _patched_runner_modules():
        return _load_runtime_states_from_instructions_impl(
            api_base=api_base,
            deployment_key=deployment_key,
            symbols=symbols,
            side=side,
        )


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
    allow_metadata_fallback: bool = False,
    metadata_history_only: bool = False,
) -> dict[str, Any]:
    with _patched_runner_modules():
        return _history_override_payload_impl(
            api_base=api_base,
            loaded=loaded,
            candidate=candidate,
            trade_date=trade_date,
            history_cache=history_cache,
            duration=duration,
            bar_size=bar_size,
            timeout=timeout,
            allow_metadata_fallback=allow_metadata_fallback,
            metadata_history_only=metadata_history_only,
        )


def publish_desired_stream_symbols(
    api_base: str,
    symbols: list[str],
    *,
    market_data_type: str,
    subscription_state: dict[str, Any] | None = None,
) -> bool:
    with _patched_runner_modules():
        return _publish_desired_stream_symbols_impl(
            api_base,
            symbols,
            market_data_type=market_data_type,
            subscription_state=subscription_state,
        )


def subscribe_symbols(
    api_base: str,
    symbols: list[str],
    *,
    market_data_type: str,
    subscription_state: dict[str, Any] | None = None,
) -> bool:
    with _patched_runner_modules():
        return _subscribe_symbols_impl(
            api_base,
            symbols,
            market_data_type=market_data_type,
            subscription_state=subscription_state,
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
    with _patched_runner_modules():
        _heartbeat_impl(
            api_base,
            deployment_key,
            status,
            runtime_error=runtime_error,
            last_bar_at=last_bar_at,
            last_action_at=last_action_at,
            metrics=metrics,
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
    with _patched_runner_modules():
        _heartbeat_stream_failure_impl(
            api_base=api_base,
            loaded_deployments=loaded_deployments,
            candidates_by_deployment=candidates_by_deployment,
            error=error,
            market_data_type=market_data_type,
            stop_stream_on_empty=stop_stream_on_empty,
        )


def publish_virtual_decision_bar(
    api_base: str,
    *,
    candidate: Mapping[str, Any],
    symbol_observation: Mapping[str, Any],
    deployment_key: str,
    action_name: str,
    decision_id: str,
) -> dict[str, Any] | None:
    with _patched_runner_modules():
        return _publish_virtual_decision_bar_impl(
            api_base,
            candidate=candidate,
            symbol_observation=symbol_observation,
            deployment_key=deployment_key,
            action_name=action_name,
            decision_id=decision_id,
        )


def decision_observed_at(symbol_observation: Mapping[str, Any]) -> str:
    return _decision_observed_at_impl(symbol_observation)


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
    stream_subscription_state: dict[str, Any] | None = None,
    history_duration: str,
    history_bar_size: str,
    history_timeout: int,
    benchmark_symbols: list[str],
    max_stream_symbols: int = DEFAULT_MAX_STREAM_SYMBOLS,
    stream_warning_symbols: int = DEFAULT_STREAM_WARNING_SYMBOLS,
    allow_metadata_history_fallback: bool = False,
    metadata_history_only: bool = True,
) -> None:
    with _patched_runner_modules():
        _run_once_impl(
            api_base=api_base,
            limit=limit,
            loaded_models=loaded_models,
            loaded_deployments=loaded_deployments,
            processed_decisions=processed_decisions,
            execute_virtual=execute_virtual,
            execute_broker=execute_broker,
            include_smoke=include_smoke,
            stop_stream_on_empty=stop_stream_on_empty,
            market_data_type=market_data_type,
            account_mode=account_mode,
            candidate_reason_codes=candidate_reason_codes,
            trade_date=trade_date,
            history_cache=history_cache,
            stream_subscription_state=stream_subscription_state,
            history_duration=history_duration,
            history_bar_size=history_bar_size,
            history_timeout=history_timeout,
            benchmark_symbols=benchmark_symbols,
            max_stream_symbols=max_stream_symbols,
            stream_warning_symbols=stream_warning_symbols,
            allow_metadata_history_fallback=allow_metadata_history_fallback,
            metadata_history_only=metadata_history_only,
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
    allow_metadata_history_fallback: bool = False,
    metadata_history_only: bool = True,
    stream_bar_ready_symbols: set[str] | None = None,
    stream_plan: Mapping[str, Any] | None = None,
    trade_date: str | None = None,
) -> None:
    with _patched_runner_modules():
        _run_model_candidates_impl(
            api_base=api_base,
            loaded=loaded,
            deployment_key=deployment_key,
            deployment_mode=deployment_mode,
            candidates=candidates,
            processed_decisions=processed_decisions,
            execute_actions=execute_actions,
            history_cache=history_cache,
            history_duration=history_duration,
            history_bar_size=history_bar_size,
            history_timeout=history_timeout,
            allow_metadata_history_fallback=allow_metadata_history_fallback,
            metadata_history_only=metadata_history_only,
            stream_bar_ready_symbols=stream_bar_ready_symbols,
            stream_plan=stream_plan,
            trade_date=trade_date,
        )


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
    parser.add_argument(
        "--allow-metadata-history-fallback",
        action="store_true",
        help=(
            "Allow live RL observations to use candidate trace.metadata.yesterday_close "
            "with neutral history features when IBKR historical bars are unavailable. "
            "Intended for virtual/live-feed continuity only; prefer real historical bars."
        ),
    )
    parser.add_argument(
        "--metadata-history-only",
        action="store_true",
        default=True,
        help=(
            "Keep the live RL loop off IBKR historical bars. This is the default: "
            "use candidate trace.metadata.history_features when present, otherwise "
            "require --allow-metadata-history-fallback to use yesterday_close with "
            "neutral history features."
        ),
    )
    parser.add_argument(
        "--allow-live-historical-backfill",
        dest="metadata_history_only",
        action="store_false",
        help=(
            "Allow the live RL loop to call /v1/market-data/historical-bars. "
            "Use only for controlled backfills or diagnostics, not the normal "
            "market-minute loop."
        ),
    )
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument(
        "--benchmark-symbols",
        default=",".join(DEFAULT_BENCHMARK_SYMBOLS),
        help="Comma-separated symbols to keep in the market stream for dashboard benchmarking.",
    )
    parser.add_argument(
        "--max-stream-symbols",
        type=int,
        default=DEFAULT_MAX_STREAM_SYMBOLS,
        help=(
            "Maximum symbols the runner will ask the API stream to maintain. "
            "Candidates are prioritized over benchmark symbols."
        ),
    )
    parser.add_argument(
        "--stream-warning-symbols",
        type=int,
        default=DEFAULT_STREAM_WARNING_SYMBOLS,
        help="Heartbeat warning threshold for active stream symbols.",
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
    stream_subscription_state: dict[str, Any] = {}
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
                stream_subscription_state=stream_subscription_state,
                history_duration=args.history_duration,
                history_bar_size=args.history_bar_size,
                history_timeout=args.history_timeout,
                allow_metadata_history_fallback=args.allow_metadata_history_fallback,
                metadata_history_only=args.metadata_history_only,
                benchmark_symbols=parse_symbol_list(args.benchmark_symbols),
                max_stream_symbols=args.max_stream_symbols,
                stream_warning_symbols=args.stream_warning_symbols,
            )
            _save_processed_decisions(state_path, processed_decisions)
            _save_history_cache(history_cache_path, history_cache)
        except Exception as exc:
            _save_history_cache(history_cache_path, history_cache)
            print(f"runner_error: {exc}", file=sys.stderr, flush=True)
        if args.once:
            return 0
        time.sleep(max(args.poll_seconds, 5.0))




if __name__ == "__main__":
    raise SystemExit(main())
