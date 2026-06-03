from __future__ import annotations

import json
import time
from collections import Counter
from datetime import date
from datetime import datetime, timezone
from datetime import timedelta
from typing import Any, Mapping

import numpy as np

try:
    import torch
except ModuleNotFoundError:  # pragma: no cover - exercised by runner import smoke tests.
    torch = None

from ibkr_trader.rl.inference_vector import RunnerSymbolState
from ibkr_trader.rl.inference_vector import valid_action_mask
from ibkr_trader.rl.runner_history import candidate_instrument
from ibkr_trader.rl.runner_http import post_json
from ibkr_trader.rl.runner_model import _require_torch
from ibkr_trader.rl.runner_types import STOCKHOLM_TZ


def _elapsed_seconds(started_at: float) -> float:
    return round(time.perf_counter() - started_at, 6)


def _finalize_timing_metrics(
    timing_metrics: dict[str, Any],
    *,
    started_at: float,
    active_candidate_count: int,
) -> None:
    total_seconds = _elapsed_seconds(started_at)
    cadence_budget_seconds = 300.0
    timing_metrics["total_seconds"] = total_seconds
    timing_metrics["cadence_budget_seconds"] = cadence_budget_seconds
    timing_metrics["cadence_budget_used_pct"] = round(
        total_seconds / cadence_budget_seconds * 100.0,
        3,
    )
    timing_metrics["cadence_over_budget"] = total_seconds > cadence_budget_seconds
    if active_candidate_count > 0:
        timing_metrics["seconds_per_active_candidate"] = round(
            total_seconds / active_candidate_count,
            6,
        )


def action_distribution_metrics(
    actions: list[Mapping[str, Any]],
    *,
    model_side: str,
) -> dict[str, Any]:
    """Summarize model choices so input/model drift is visible in heartbeat.

    The bucket booster policies should usually enter most flat names early in
    the session. If a runner evaluates many flat candidates and emits only
    skip/wait, that is important operator evidence even when every API call is
    technically succeeding.
    """

    evaluated_actions = [
        action for action in actions if action.get("action_name") is not None
    ]
    action_name_counts = Counter(
        str(action.get("action_name")) for action in evaluated_actions
    )
    entry_count = sum(
        count
        for action_name, count in action_name_counts.items()
        if action_name == "market_entry" or action_name.startswith("entry_prevclose_")
    )
    exit_count = sum(
        count
        for action_name, count in action_name_counts.items()
        if action_name == "exit_market" or action_name.startswith("exit_tp_")
    )
    cancel_count = sum(
        count
        for action_name, count in action_name_counts.items()
        if action_name in {"cancel_entry", "clear_exit"}
    )
    idle_count = action_name_counts.get("skip", 0) + action_name_counts.get("wait", 0)
    flat_actions = [
        action
        for action in evaluated_actions
        if str(action.get("state_before") or "").upper() == "FLAT"
    ]
    flat_action_counts = Counter(str(action.get("action_name")) for action in flat_actions)
    flat_entry_count = sum(
        count
        for action_name, count in flat_action_counts.items()
        if action_name == "market_entry" or action_name.startswith("entry_prevclose_")
    )
    flat_idle_count = flat_action_counts.get("skip", 0) + flat_action_counts.get("wait", 0)
    evaluated_count = len(evaluated_actions)
    flat_evaluated_count = len(flat_actions)
    warning = None
    warning_detail = None
    if flat_evaluated_count >= 5 and flat_entry_count == 0 and flat_idle_count == flat_evaluated_count:
        side = str(model_side).upper()
        if side == "SHORT" and flat_action_counts.get("skip", 0) == flat_evaluated_count:
            warning = "short_flat_candidates_all_skip"
        elif side == "LONG" and flat_action_counts.get("wait", 0) == flat_evaluated_count:
            warning = "long_flat_candidates_all_wait"
        else:
            warning = "flat_candidates_all_idle"
        warning_detail = (
            "The runner evaluated flat candidates but produced no entry actions. "
            "For bucket booster policies this is a strong signal of feature, "
            "bar, state, or model-bundle drift."
        )

    metrics: dict[str, Any] = {
        "model_side": str(model_side).upper(),
        "evaluated_action_count": evaluated_count,
        "entry_action_count": entry_count,
        "exit_action_count": exit_count,
        "cancel_action_count": cancel_count,
        "idle_action_count": idle_count,
        "flat_evaluated_action_count": flat_evaluated_count,
        "flat_entry_action_count": flat_entry_count,
        "flat_idle_action_count": flat_idle_count,
        "action_name_counts": dict(action_name_counts),
        "flat_action_name_counts": dict(flat_action_counts),
        "warning": warning,
    }
    if evaluated_count:
        metrics["entry_action_rate"] = entry_count / evaluated_count
        metrics["idle_action_rate"] = idle_count / evaluated_count
    if flat_evaluated_count:
        metrics["flat_entry_action_rate"] = flat_entry_count / flat_evaluated_count
        metrics["flat_idle_action_rate"] = flat_idle_count / flat_evaluated_count
    if warning_detail:
        metrics["warning_detail"] = warning_detail
    return metrics


def build_stream_symbol_plan(
    *,
    candidate_symbols: list[str],
    benchmark_symbols: list[str],
    max_stream_symbols: int,
    warning_symbols: int,
) -> dict[str, Any]:
    if max_stream_symbols <= 0:
        raise ValueError("max_stream_symbols must be positive")
    normalized_candidates = sorted({str(symbol).strip().upper() for symbol in candidate_symbols if str(symbol).strip()})
    normalized_benchmarks = sorted({str(symbol).strip().upper() for symbol in benchmark_symbols if str(symbol).strip()})
    desired_symbols = list(dict.fromkeys(normalized_candidates + normalized_benchmarks))
    stream_symbols = desired_symbols[:max_stream_symbols]
    dropped_symbols = desired_symbols[max_stream_symbols:]
    dropped_candidate_symbols = [
        symbol for symbol in dropped_symbols if symbol in normalized_candidates
    ]
    return {
        "candidate_symbol_count": len(normalized_candidates),
        "benchmark_symbol_count": len(normalized_benchmarks),
        "desired_symbol_count": len(desired_symbols),
        "stream_symbol_count": len(stream_symbols),
        "max_stream_symbols": max_stream_symbols,
        "warning_symbols": warning_symbols,
        "over_warning_threshold": len(stream_symbols) >= warning_symbols,
        "overflow_symbol_count": len(dropped_symbols),
        "overflow_symbols": dropped_symbols,
        "overflow_candidate_symbol_count": len(dropped_candidate_symbols),
        "stream_symbols": stream_symbols,
    }


def trade_date_from_candidates(
    candidates: list[Mapping[str, Any]],
    *,
    fallback: str | None = None,
) -> str | None:
    for candidate in candidates:
        trace = candidate.get("trace")
        if isinstance(trace, Mapping) and trace.get("trade_date"):
            return str(trace["trade_date"])
    return fallback


def expected_decision_bar_ended_at(
    *,
    trade_date: str | None,
    now: datetime | None = None,
) -> str | None:
    if not trade_date:
        return None
    try:
        session_date = date.fromisoformat(str(trade_date))
    except ValueError:
        return None
    local_now = (now or datetime.now(STOCKHOLM_TZ)).astimezone(STOCKHOLM_TZ)
    session_open = datetime.combine(
        session_date,
        datetime.strptime("09:00", "%H:%M").time(),
        tzinfo=STOCKHOLM_TZ,
    )
    session_close = datetime.combine(
        session_date,
        datetime.strptime("17:30", "%H:%M").time(),
        tzinfo=STOCKHOLM_TZ,
    )
    first_decision = session_open + timedelta(minutes=5)
    if local_now.date() < session_date:
        return None
    if local_now.date() > session_date:
        return session_close.isoformat()
    if local_now < first_decision:
        return None
    capped = min(local_now, session_close)
    floored_minute = (capped.minute // 5) * 5
    ended_at = capped.replace(minute=floored_minute, second=0, microsecond=0)
    if ended_at < first_decision:
        return None
    return ended_at.isoformat()


def classify_decision_bar_freshness(
    decision: Mapping[str, Any],
    *,
    target_decision_bar_ended_at: str | None,
) -> dict[str, Any]:
    latest_raw = decision.get("latest_usable_bar_ended_at")
    if not decision.get("ready"):
        reason = str(decision.get("reason") or "")
        backfill_status = str(decision.get("backfill_status") or "")
        if reason in {
            "paused_market_stream_bars_missing_backfill_pending",
            "paused_observed_bar_coverage_below_threshold",
        }:
            status = (
                "paused_backfill_pending"
                if backfill_status
                in {"PENDING", "RUNNING", "FAILED_RETRYABLE"}
                else "paused_data_quality"
            )
            return {
                "status": status,
                "reason": reason,
                "backfill_status": backfill_status or None,
                "latest_usable_bar_ended_at": latest_raw,
                "target_decision_bar_ended_at": target_decision_bar_ended_at,
            }
        return {
            "status": "not_ready",
            "reason": reason or None,
            "latest_usable_bar_ended_at": latest_raw,
            "target_decision_bar_ended_at": target_decision_bar_ended_at,
        }
    if target_decision_bar_ended_at is None:
        return {
            "status": "no_target_bar",
            "latest_usable_bar_ended_at": latest_raw,
            "target_decision_bar_ended_at": target_decision_bar_ended_at,
        }
    latest = _parse_iso_datetime(latest_raw)
    target = _parse_iso_datetime(target_decision_bar_ended_at)
    if latest is None or target is None:
        return {
            "status": "unknown_bar_freshness",
            "latest_usable_bar_ended_at": latest_raw,
            "target_decision_bar_ended_at": target_decision_bar_ended_at,
        }
    if latest == target:
        status = "fresh_bar"
    elif latest < target:
        status = "stale_bar"
    else:
        status = "future_bar"
    return {
        "status": status,
        "latest_usable_bar_ended_at": latest_raw,
        "target_decision_bar_ended_at": target_decision_bar_ended_at,
    }


def _parse_iso_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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


