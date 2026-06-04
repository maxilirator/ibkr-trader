from __future__ import annotations

import json
import time
import urllib.parse
from collections import Counter
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Mapping

from ibkr_trader.rl.inference_vector import RunnerSymbolState
from ibkr_trader.rl.inference_vector import assemble_dqn_observation_vector
from ibkr_trader.rl.runner_decisions import _elapsed_seconds
from ibkr_trader.rl.runner_decisions import _finalize_timing_metrics
from ibkr_trader.rl.runner_decisions import action_diagnostics
from ibkr_trader.rl.runner_decisions import action_distribution_metrics
from ibkr_trader.rl.runner_decisions import build_stream_symbol_plan
from ibkr_trader.rl.runner_decisions import classify_decision_bar_freshness
from ibkr_trader.rl.runner_decisions import decision_observed_at
from ibkr_trader.rl.runner_decisions import expected_decision_bar_ended_at
from ibkr_trader.rl.runner_decisions import publish_virtual_decision_bar
from ibkr_trader.rl.runner_decisions import trade_date_from_candidates
from ibkr_trader.rl.runner_decisions import choose_action
from ibkr_trader.rl.runner_deployments import group_candidates_by_deployment
from ibkr_trader.rl.runner_deployments import legacy_loaded_deployments
from ibkr_trader.rl.runner_history import candidate_instrument
from ibkr_trader.rl.runner_history import history_override_payload
from ibkr_trader.rl.runner_http import ApiError
from ibkr_trader.rl.runner_http import _is_executable_action
from ibkr_trader.rl.runner_http import get_json
from ibkr_trader.rl.runner_http import post_json
from ibkr_trader.rl.runner_model import static_feature_payload
from ibkr_trader.rl.runner_runtime_state import _api_error_is_conflict
from ibkr_trader.rl.runner_runtime_state import load_runtime_state_context
from ibkr_trader.rl.runner_runtime_state import translation_state_before
from ibkr_trader.rl.runner_stream import heartbeat
from ibkr_trader.rl.runner_stream import heartbeat_stream_failure
from ibkr_trader.rl.runner_stream import publish_desired_stream_symbols
from ibkr_trader.rl.runner_stream import stream_desired_state_needs_publish
from ibkr_trader.rl.runner_stream import stream_subscription_pending_symbols
from ibkr_trader.rl.runner_types import DEFAULT_MAX_STREAM_SYMBOLS
from ibkr_trader.rl.runner_types import DEFAULT_STREAM_WARNING_SYMBOLS
from ibkr_trader.rl.runner_types import LoadedDeployment
from ibkr_trader.rl.runner_types import LoadedModel


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
                publish_desired_stream_symbols(
                    api_base,
                    benchmark_symbols,
                    market_data_type=market_data_type,
                    subscription_state=stream_subscription_state,
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
    stream_plan = build_stream_symbol_plan(
        candidate_symbols=symbols,
        benchmark_symbols=benchmark_symbols,
        max_stream_symbols=max_stream_symbols,
        warning_symbols=stream_warning_symbols,
    )
    stream_symbols = stream_plan["stream_symbols"]
    try:
        publish_desired_stream_symbols(
            api_base,
            stream_symbols,
            market_data_type=market_data_type,
            subscription_state=stream_subscription_state,
        )
        stream = get_json(
            f"{api_base}/v1/market-data/stream/snapshot?"
            + urllib.parse.urlencode({"symbols": ",".join(stream_symbols), "bar_limit": "390"})
        )["stream"]
        if stream_desired_state_needs_publish(stream, stream_symbols):
            if stream_subscription_state is not None:
                stream_subscription_state.pop("signature", None)
            publish_desired_stream_symbols(
                api_base,
                stream_symbols,
                market_data_type=market_data_type,
                subscription_state=stream_subscription_state,
            )
            stream = get_json(
                f"{api_base}/v1/market-data/stream/snapshot?"
                + urllib.parse.urlencode({"symbols": ",".join(stream_symbols), "bar_limit": "390"})
            )["stream"]
        pending_symbols = stream_subscription_pending_symbols(stream, stream_symbols)
        if pending_symbols:
            print(
                "Market stream desired state is published; waiting for API "
                f"stream owner to subscribe: {', '.join(pending_symbols)}",
                flush=True,
            )
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
            allow_metadata_history_fallback=allow_metadata_history_fallback,
            metadata_history_only=metadata_history_only,
            stream_bar_ready_symbols=symbols_with_bars,
            stream_plan=stream_plan,
            trade_date=trade_date,
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
    run_started = time.perf_counter()
    timing_metrics: dict[str, Any] = {}
    symbols = sorted({str(candidate["symbol"]).upper() for candidate in candidates})
    stage_started = time.perf_counter()
    runtime_context = load_runtime_state_context(
        api_base=api_base,
        deployment_key=deployment_key,
        symbols=symbols,
        side=loaded.config.side,
    )
    runtime_states = runtime_context.states
    timing_metrics["load_runtime_states_seconds"] = _elapsed_seconds(stage_started)
    active_candidates: list[Mapping[str, Any]] = []
    static_features: dict[str, Any] = {}
    history_overrides: dict[str, Any] = {}
    skipped_candidates: list[dict[str, Any]] = []
    stream_ready_symbols = {
        str(symbol).upper() for symbol in (stream_bar_ready_symbols or set())
    }
    stage_started = time.perf_counter()
    for candidate in candidates:
        symbol = str(candidate["symbol"]).upper()
        if stream_bar_ready_symbols is not None and symbol not in stream_ready_symbols:
            skipped_candidates.append(
                {
                    "symbol": symbol,
                    "status": "waiting_for_stream_bars",
                    "reason": "market stream has no 1-minute bars for symbol yet",
                }
            )
            continue
        runtime_block = runtime_context.blocked_symbols.get(symbol)
        if runtime_block is not None:
            skipped_candidates.append(
                {
                    "symbol": symbol,
                    "status": "runtime_state_blocked",
                    "reason": "authoritative runtime state is ambiguous",
                    "runtime_state": dict(runtime_block),
                }
            )
            continue
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
                allow_metadata_fallback=allow_metadata_history_fallback,
                metadata_history_only=metadata_history_only,
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
    timing_metrics["prepare_features_history_seconds"] = _elapsed_seconds(stage_started)

    if not active_candidates:
        runtime_blocked_candidate_count = sum(
            1
            for candidate in skipped_candidates
            if candidate.get("status") == "runtime_state_blocked"
        )
        runtime_error = (
            "authoritative runtime state blocked all RL candidates"
            if candidates and runtime_blocked_candidate_count == len(candidates)
            else "market stream has no bars for active RL candidates"
            if candidates
            and skipped_candidates
            and all(
                candidate.get("status") == "waiting_for_stream_bars"
                for candidate in skipped_candidates
            )
            else "no candidates had complete static features and history overrides"
        )
        _finalize_timing_metrics(
            timing_metrics,
            started_at=run_started,
            active_candidate_count=0,
        )
        heartbeat(
            api_base,
            deployment_key,
            "degraded",
            runtime_error=runtime_error,
            metrics={
                "candidate_count": len(candidates),
                "active_candidate_count": 0,
                "symbols": symbols,
                "skipped_candidates": skipped_candidates,
                "waiting_for_stream_bar_candidate_count": sum(
                    1
                    for candidate in skipped_candidates
                    if candidate.get("status") == "waiting_for_stream_bars"
                ),
                "runtime_state_source": runtime_context.source,
                "runtime_state_blocked_symbol_count": len(runtime_context.blocked_symbols),
                "runtime_state_blocked_candidate_count": runtime_blocked_candidate_count,
                "timing": timing_metrics,
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

    target_decision_bar_ended_at = expected_decision_bar_ended_at(
        trade_date=trade_date_from_candidates(active_candidates, fallback=trade_date)
    )
    stage_started = time.perf_counter()
    try:
        observation_payload: dict[str, object] = {
            "deployment_key": deployment_key,
            "symbols": active_symbols,
            "history_overrides": history_overrides,
            "static_features": static_features,
            "fetch": {
                "mode": "market_stream",
                "bar_limit": 390,
                "backfill_missing": True,
                "backfill_duration": "2 D",
                "backfill_bar_size": "1 min",
                "instruments": {
                    str(candidate["symbol"]).upper(): dict(candidate_instrument(candidate))
                    for candidate in active_candidates
                },
            },
        }
        if target_decision_bar_ended_at is not None:
            observation_payload["as_of"] = target_decision_bar_ended_at
        observation_response = post_json(
            f"{api_base}/v1/rl/observations/build?timeout={history_timeout}",
            observation_payload,
            timeout=max(history_timeout + 15, 60),
        )
        timing_metrics["build_observations_seconds"] = _elapsed_seconds(stage_started)
    except ApiError as exc:
        timing_metrics["build_observations_seconds"] = _elapsed_seconds(stage_started)
        _finalize_timing_metrics(
            timing_metrics,
            started_at=run_started,
            active_candidate_count=len(active_candidates),
        )
        heartbeat(
            api_base,
            deployment_key,
            "degraded",
            runtime_error="failed to build RL observations from market stream",
            metrics={
                "candidate_count": len(candidates),
                "active_candidate_count": len(active_candidates),
                "symbols": active_symbols,
                "target_decision_bar_ended_at": target_decision_bar_ended_at,
                "observation_error": str(exc),
                "stream_plan": dict(stream_plan or {}),
                "timing": timing_metrics,
            },
        )
        print(
            json.dumps(
                {
                    "deployment_key": deployment_key,
                    "error": "failed to build RL observations from market stream",
                    "detail": str(exc),
                },
                indent=2,
            ),
            flush=True,
        )
        return
    rl_observation = observation_response["rl_observation"]
    observations = rl_observation["observations"]
    last_bar_at = None
    last_action_at = None
    actions = list(skipped_candidates)
    decision_coverage: dict[str, dict[str, Any]] = {}
    action_loop_started = time.perf_counter()
    model_inference_seconds = 0.0
    translate_seconds = 0.0
    virtual_publish_seconds = 0.0

    for candidate in active_candidates:
        symbol = str(candidate["symbol"]).upper()
        symbol_observation = observations[symbol]
        decision = symbol_observation["model_decision"]
        freshness = classify_decision_bar_freshness(
            decision,
            target_decision_bar_ended_at=target_decision_bar_ended_at,
        )
        decision_coverage[symbol] = freshness
        last_bar_at = (
            decision.get("latest_usable_bar_ended_at")
            or symbol_observation.get("latest_bar_ended_at")
            or last_bar_at
        )
        if not decision.get("ready"):
            actions.append(
                {
                    "symbol": symbol,
                    "status": freshness["status"],
                    "decision": decision,
                    "target_decision_bar_ended_at": target_decision_bar_ended_at,
                }
            )
            continue
        if freshness["status"] not in {"fresh_bar", "no_target_bar"}:
            actions.append(
                {
                    "symbol": symbol,
                    "status": freshness["status"],
                    "decision_id": decision.get("decision_id"),
                    "latest_usable_bar_ended_at": freshness.get(
                        "latest_usable_bar_ended_at"
                    ),
                    "target_decision_bar_ended_at": target_decision_bar_ended_at,
                }
            )
            continue
        decision_id = str(decision["decision_id"])
        dedupe_key = f"{deployment_key}:{candidate['instruction_id']}:{decision_id}"
        if dedupe_key in processed_decisions:
            actions.append(
                {
                    "symbol": symbol,
                    "status": "already_processed",
                    "decision_id": decision_id,
                    "latest_usable_bar_ended_at": freshness.get(
                        "latest_usable_bar_ended_at"
                    ),
                    "target_decision_bar_ended_at": target_decision_bar_ended_at,
                }
            )
            continue
        runner_state = runtime_states.get(symbol, RunnerSymbolState())
        inference_started = time.perf_counter()
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
        model_inference_seconds += time.perf_counter() - inference_started
        previous_close = symbol_observation["pricing_context"]["prev_close"]
        state_before = translation_state_before(runner_state, loaded.config.side)
        observed_at = decision_observed_at(symbol_observation)
        translate_started = time.perf_counter()
        try:
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
        except ApiError as exc:
            translate_seconds += time.perf_counter() - translate_started
            if _api_error_is_conflict(exc):
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
                    "submitted": False,
                    "action_status": "translate_error",
                    "status": "translate_error",
                    "retryable": not _api_error_is_conflict(exc),
                    "error": str(exc),
                }
            )
            continue
        translate_seconds += time.perf_counter() - translate_started
        virtual_quote_result = None
        if deployment_mode == "virtual":
            try:
                virtual_publish_started = time.perf_counter()
                virtual_quote_result = publish_virtual_decision_bar(
                    api_base,
                    candidate=candidate,
                    symbol_observation=symbol_observation,
                    deployment_key=deployment_key,
                    action_name=action_name,
                    decision_id=decision_id,
                )
                virtual_publish_seconds += time.perf_counter() - virtual_publish_started
            except ApiError as exc:
                virtual_publish_seconds += time.perf_counter() - virtual_publish_started
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
    timing_metrics["action_loop_seconds"] = _elapsed_seconds(action_loop_started)
    timing_metrics["model_inference_seconds"] = round(model_inference_seconds, 6)
    timing_metrics["translate_actions_seconds"] = round(translate_seconds, 6)
    timing_metrics["publish_virtual_bars_seconds"] = round(virtual_publish_seconds, 6)
    _finalize_timing_metrics(
        timing_metrics,
        started_at=run_started,
        active_candidate_count=len(active_candidates),
    )

    status_counts = dict(Counter(str(action.get("status") or "unknown") for action in actions))
    coverage_counts = dict(
        Counter(item["status"] for item in decision_coverage.values())
    )
    action_distribution = action_distribution_metrics(
        actions,
        model_side=loaded.config.side,
    )
    heartbeat_status = "running"
    runtime_error = None
    if active_candidates and coverage_counts.get("fresh_bar", 0) == 0 and coverage_counts.get("stale_bar", 0):
        heartbeat_status = "degraded"
        runtime_error = "market stream bars are stale for all active RL candidates"
    elif action_distribution.get("warning"):
        heartbeat_status = "degraded"
        runtime_error = str(action_distribution.get("warning_detail") or action_distribution["warning"])
    elif status_counts.get("translate_error", 0):
        heartbeat_status = "degraded"
        runtime_error = (
            f"{status_counts['translate_error']} RL action translation request(s) failed"
        )
    elif timing_metrics.get("cadence_over_budget"):
        heartbeat_status = "degraded"
        runtime_error = "RL runner processing exceeded the 5-minute decision cadence"
    heartbeat(
        api_base,
        deployment_key,
        heartbeat_status,
        runtime_error=runtime_error,
        last_bar_at=last_bar_at,
        last_action_at=last_action_at,
        metrics={
            "candidate_count": len(candidates),
            "active_candidate_count": len(active_candidates),
            "stream_any_bar_candidate_count": len(
                {
                    str(candidate["symbol"]).upper()
                    for candidate in active_candidates
                    if str(candidate["symbol"]).upper() in stream_ready_symbols
                }
            ),
            "stream_bar_ready_candidate_count": coverage_counts.get("fresh_bar", 0),
            "fresh_decision_bar_candidate_count": coverage_counts.get("fresh_bar", 0),
            "stale_decision_bar_candidate_count": coverage_counts.get("stale_bar", 0),
            "not_ready_candidate_count": coverage_counts.get("not_ready", 0)
            + coverage_counts.get("waiting_for_target_bar", 0)
            + coverage_counts.get("paused_backfill_pending", 0)
            + coverage_counts.get("paused_data_quality", 0),
            "paused_backfill_candidate_count": coverage_counts.get(
                "paused_backfill_pending",
                0,
            ),
            "paused_data_quality_candidate_count": coverage_counts.get(
                "paused_data_quality",
                0,
            ),
            "already_processed_candidate_count": status_counts.get("already_processed", 0),
            "evaluated_candidate_count": len(
                [
                    action
                    for action in actions
                    if action.get("action_name") is not None
                ]
            ),
            "backfilled_symbol_count": len(observation_response.get("fetched_symbols", [])),
            "symbols": active_symbols,
            "actions": actions,
            "action_status_counts": status_counts,
            "action_distribution": action_distribution,
            "decision_bar_status_counts": coverage_counts,
            "target_decision_bar_ended_at": target_decision_bar_ended_at,
            "stream_plan": dict(stream_plan or {}),
            "deployment_mode": deployment_mode,
            "execute_actions": execute_actions,
            "history_cache_count": len(history_cache),
            "metadata_history_fallback_allowed": allow_metadata_history_fallback,
            "metadata_history_only": metadata_history_only,
            "runtime_state_source": runtime_context.source,
            "runtime_state_blocked_symbol_count": len(runtime_context.blocked_symbols),
            "timing": timing_metrics,
        },
    )
    print(json.dumps({"deployment_key": deployment_key, "actions": actions}, indent=2), flush=True)
