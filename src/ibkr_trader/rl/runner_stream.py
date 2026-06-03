from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Mapping

from ibkr_trader.rl.runner_http import ApiError
from ibkr_trader.rl.runner_http import post_json
from ibkr_trader.rl.runner_types import BENCHMARK_STREAM_CONTRACTS
from ibkr_trader.rl.runner_types import LoadedDeployment


def publish_desired_stream_symbols(
    api_base: str,
    symbols: list[str],
    *,
    market_data_type: str,
    subscription_state: dict[str, Any] | None = None,
) -> bool:
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
    signature = _stream_subscription_signature(
        contracts,
        market_data_type=market_data_type,
        replace=True,
    )
    if subscription_state is not None:
        if subscription_state.get("signature") == signature:
            return False
    post_json(
        f"{api_base}/v1/market-data/stream/desired",
        {
            "contracts": contracts,
            "market_data_type": market_data_type,
            "replace": True,
        },
    )
    if subscription_state is not None:
        subscription_state["signature"] = signature
    return True


def subscribe_symbols(
    api_base: str,
    symbols: list[str],
    *,
    market_data_type: str,
    subscription_state: dict[str, Any] | None = None,
) -> bool:
    return publish_desired_stream_symbols(
        api_base,
        symbols,
        market_data_type=market_data_type,
        subscription_state=subscription_state,
    )


def _stream_subscription_signature(
    contracts: list[Mapping[str, Any]],
    *,
    market_data_type: str,
    replace: bool,
) -> str:
    normalized_contracts = sorted(
        (
            {
                str(key): value
                for key, value in contract.items()
                if value is not None
            }
            for contract in contracts
        ),
        key=lambda contract: (
            str(contract.get("symbol") or "").upper(),
            str(contract.get("security_type") or "").upper(),
            str(contract.get("exchange") or "").upper(),
            str(contract.get("primary_exchange") or "").upper(),
            str(contract.get("currency") or "").upper(),
        ),
    )
    return json.dumps(
        {
            "contracts": normalized_contracts,
            "market_data_type": market_data_type,
            "replace": replace,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def stream_desired_state_needs_publish(
    stream: Mapping[str, Any],
    stream_symbols: list[str],
) -> bool:
    expected_symbols = {str(symbol).strip().upper() for symbol in stream_symbols if symbol}
    if not expected_symbols:
        return False
    desired_symbols = {
        str(symbol).strip().upper()
        for symbol in stream.get("desired_symbols", [])
        if symbol
    }
    return not expected_symbols.issubset(desired_symbols)


def stream_subscription_pending_symbols(
    stream: Mapping[str, Any],
    stream_symbols: list[str],
) -> list[str]:
    expected_symbols = {str(symbol).strip().upper() for symbol in stream_symbols if symbol}
    subscribed_symbols = {
        str(subscription.get("contract", {}).get("symbol") or "").strip().upper()
        for subscription in stream.get("subscriptions", [])
        if isinstance(subscription, Mapping)
    }
    return sorted(expected_symbols - subscribed_symbols)


def stream_subscription_needs_repair(
    stream: Mapping[str, Any],
    stream_symbols: list[str],
) -> bool:
    return stream_desired_state_needs_publish(stream, stream_symbols)


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

