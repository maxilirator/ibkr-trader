from __future__ import annotations

from typing import Any, Mapping

from ibkr_trader.rl.runner_http import get_json
from ibkr_trader.rl.runner_types import DEFAULT_CANDIDATE_REASON_CODES
from ibkr_trader.rl.runner_types import LoadedDeployment
from ibkr_trader.rl.runner_types import LoadedModel


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


