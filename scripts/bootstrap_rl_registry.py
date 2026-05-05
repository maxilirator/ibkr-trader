#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from typing import Any, Mapping

from ibkr_trader.rl.model_artifacts import DEFAULT_SHARED_VIRTUAL_ACCOUNT
from ibkr_trader.rl.model_artifacts import deployment_registry_payload
from ibkr_trader.rl.model_artifacts import model_registry_payload
from ibkr_trader.rl.model_artifacts import promoted_rl_models
from ibkr_trader.rl.model_artifacts import validate_promoted_artifact


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Register deployed trader-local RL model bundles and shared deployments."
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
        artifact_allowed_symbols = allowed_symbols
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
