#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from typing import Any, Mapping

import pandas as pd

from ibkr_trader.rl.model_artifacts import DEFAULT_SHARED_VIRTUAL_ACCOUNT
from ibkr_trader.rl.model_artifacts import deployment_registry_payload
from ibkr_trader.rl.model_artifacts import model_registry_payload
from ibkr_trader.rl.model_artifacts import promoted_rl_models
from ibkr_trader.rl.model_artifacts import validate_promoted_artifact


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Register promoted q-training RL models and shared deployments."
    )
    parser.add_argument("--api-base", default="http://quant.geisler.se:8000")
    parser.add_argument("--account-key", default=DEFAULT_SHARED_VIRTUAL_ACCOUNT)
    parser.add_argument("--cash-balance", default="200000")
    parser.add_argument("--mode", choices=("virtual", "paper", "live"), default="virtual")
    parser.add_argument("--status", default="running")
    parser.add_argument(
        "--allowed-symbols",
        default="",
        help=(
            "Comma-separated deployment allow-list for both models. Empty leaves "
            "the deployment uncapped so the daily model-routed candidate payload "
            "defines the active universe."
        ),
    )
    parser.add_argument(
        "--derive-allowed-symbols-from-candidates",
        action="store_true",
        help=(
            "Use the selected candidate tape for deployment allowed_symbols. "
            "Normally leave this off for dynamic daily boosters."
        ),
    )
    parser.add_argument("--candidate-date", default="latest")
    parser.add_argument(
        "--long-symbol-limit",
        type=int,
        default=None,
        help="Optional cap when deriving long allowed_symbols.",
    )
    parser.add_argument(
        "--short-symbol-limit",
        type=int,
        default=None,
        help="Optional cap when deriving short allowed_symbols.",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    api_base = args.api_base.rstrip("/")
    allowed_symbols = tuple(
        symbol.strip().upper()
        for symbol in args.allowed_symbols.split(",")
        if symbol.strip()
    )
    results: list[dict[str, Any]] = []
    artifacts = promoted_rl_models()
    for artifact in artifacts:
        validation = validate_promoted_artifact(artifact)
        if allowed_symbols:
            artifact_allowed_symbols = allowed_symbols
        elif args.derive_allowed_symbols_from_candidates:
            artifact_allowed_symbols = selected_candidate_symbols(
                artifact.candidate_tape_path,
                candidate_date=args.candidate_date,
                limit=(
                    args.long_symbol_limit
                    if artifact.side_upper == "LONG"
                    else args.short_symbol_limit
                ),
            )
        else:
            artifact_allowed_symbols = ()
        model_payload = model_registry_payload(artifact)
        deployment_payload = deployment_registry_payload(
            artifact,
            account_key=args.account_key,
            mode=args.mode,
            status=args.status,
            allowed_symbols=artifact_allowed_symbols,
        )
        results.append(
            {
                "model_key": artifact.model_key,
                "deployment_key": artifact.deployment_key,
                "artifact_validation": {
                    "action_count": validation["action_count"],
                    "static_feature_count": validation["static_feature_count"],
                },
                "model_payload": model_payload,
                "deployment_payload": deployment_payload,
            }
        )

    if args.dry_run:
        print(json.dumps({"accepted": True, "dry_run": True, "results": results}, indent=2))
        return 0

    applied: list[dict[str, Any]] = []
    if args.mode == "virtual":
        applied.append(
            {
                "resource": "virtual_account",
                "response": post_json(
                    f"{api_base}/v1/virtual/accounts",
                    {
                        "account_key": args.account_key,
                        "base_currency": "SEK",
                        "account_label": "Shared RL virtual account",
                        "cash_balance": str(args.cash_balance),
                    },
                ),
            }
        )

    for result in results:
        model_response = post_json(
            f"{api_base}/v1/rl/models/upsert",
            result["model_payload"],
        )
        deployment_response = create_or_patch_deployment(
            api_base,
            result["deployment_payload"],
        )
        applied.append(
            {
                "resource": "rl_model",
                "model_key": result["model_key"],
                "response": model_response,
            }
        )
        applied.append(
            {
                "resource": "rl_deployment",
                "deployment_key": result["deployment_key"],
                "response": deployment_response,
            }
        )

    print(json.dumps({"accepted": True, "dry_run": False, "applied": applied}, indent=2))
    return 0


def create_or_patch_deployment(
    api_base: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        return post_json(f"{api_base}/v1/rl/deployments", payload)
    except ApiError as exc:
        if exc.status_code != 409:
            raise
    deployment_key = str(payload["deployment_key"])
    patch_payload = {
        key: value
        for key, value in payload.items()
        if key
        in {
            "account_key",
            "book_key",
            "mode",
            "status",
            "allowed_symbols",
            "risk_limits",
            "action_constraints",
            "metadata",
        }
    }
    return patch_json(
        f"{api_base}/v1/rl/deployments/{deployment_key}",
        patch_payload,
    )


def selected_candidate_symbols(
    path: Any,
    *,
    candidate_date: str,
    limit: int | None,
) -> tuple[str, ...]:
    if limit is not None and limit <= 0:
        raise ValueError("symbol limit must be positive")
    frame = pd.read_parquet(path)
    frame = frame.copy()
    frame["_candidate_date"] = pd.to_datetime(frame["datetime"]).dt.strftime("%Y-%m-%d")
    resolved_date = (
        str(frame["_candidate_date"].max())
        if candidate_date == "latest"
        else str(candidate_date)
    )
    day = frame[frame["_candidate_date"] == resolved_date].copy()
    if day.empty:
        raise ValueError(f"no candidate rows for {resolved_date} in {path}")
    if "selected" not in day.columns:
        raise ValueError(f"{path} does not contain a selected column")
    selected = day[day["selected"].astype(bool)].copy()
    if selected.empty:
        raise ValueError(f"no selected rows for {resolved_date} in {path}")
    score_column = _score_column(selected)
    if score_column is not None:
        selected = selected.sort_values(score_column, ascending=False)
    if limit is not None:
        selected = selected.head(limit)
    return tuple(
        str(symbol).strip().upper()
        for symbol in selected["instrument"]
        if str(symbol).strip()
    )


def _score_column(frame: pd.DataFrame) -> str | None:
    for candidate in (
        "meta_score",
        "panel_general__prob_mean",
        "panel_all__prob_mean",
    ):
        if candidate in frame.columns:
            return candidate
    prob_cols = [column for column in frame.columns if column.endswith("__prob")]
    return prob_cols[0] if prob_cols else None


class ApiError(RuntimeError):
    def __init__(self, message: str, *, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


def post_json(url: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    return _json_request(url, payload, method="POST")


def patch_json(url: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    return _json_request(url, payload, method="PATCH")


def _json_request(
    url: str,
    payload: Mapping[str, Any],
    *,
    method: str,
) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise ApiError(
            f"{method} {url} failed with HTTP {exc.code}: {body}",
            status_code=exc.code,
        ) from exc


if __name__ == "__main__":
    raise SystemExit(main())
