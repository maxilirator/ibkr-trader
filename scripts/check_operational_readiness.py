#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime
from datetime import time
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from ibkr_trader.rl.model_artifacts import promoted_rl_models
from ibkr_trader.rl.model_artifacts import validate_promoted_artifact


STOCKHOLM_TZ = ZoneInfo("Europe/Stockholm")
EXPECTED_RL_ACTIONS = {
    "LONG": {
        "entry": "entry_prevclose_-50bp",
        "take_profit": "exit_tp_200bp",
        "side": "BUY",
    },
    "SHORT": {
        "entry": "entry_prevclose_88bp",
        "take_profit": "exit_tp_180bp",
        "side": "SELL",
    },
}
ACTIVE_INSTRUCTION_STATES = {
    "ENTRY_PENDING",
    "ENTRY_SUBMITTED",
    "EXIT_PENDING",
    "POSITION_OPEN",
}
CANDIDATE_WINDOW_CLOSE = time(17, 30)


@dataclass(slots=True)
class ReadinessCheck:
    name: str
    status: str
    message: str
    details: dict[str, Any]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check whether the trader/RL path is ready for an unattended run."
    )
    parser.add_argument(
        "--api-base",
        default="http://quant.geisler.se:8000",
        help="Trader API base URL.",
    )
    parser.add_argument(
        "--trade-date",
        default=datetime.now(STOCKHOLM_TZ).date().isoformat(),
        help="Expected candidate trade date, YYYY-MM-DD.",
    )
    parser.add_argument("--model-root", default=None)
    parser.add_argument("--skip-local-model-bundles", action="store_true")
    parser.add_argument("--expected-model-count", type=int, default=2)
    parser.add_argument("--expected-deployment-count", type=int, default=2)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    report = build_readiness_report(
        api_base=args.api_base.rstrip("/"),
        trade_date=args.trade_date,
        model_root=Path(args.model_root).expanduser() if args.model_root else None,
        check_local_model_bundles=not args.skip_local_model_bundles,
        expected_model_count=args.expected_model_count,
        expected_deployment_count=args.expected_deployment_count,
    )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print_text_report(report)
    return 2 if report["blocker_count"] else 0


def build_readiness_report(
    *,
    api_base: str,
    trade_date: str,
    model_root: Path | None = None,
    check_local_model_bundles: bool = True,
    expected_model_count: int = 2,
    expected_deployment_count: int = 2,
) -> dict[str, Any]:
    checks: list[ReadinessCheck] = []
    generated_at = datetime.now(STOCKHOLM_TZ)
    allow_empty_candidates = _candidate_window_closed(
        trade_date,
        reference_at=generated_at,
    )
    checks.extend(check_python_dependencies())
    if check_local_model_bundles:
        checks.extend(check_model_bundles(model_root))

    healthz: dict[str, Any] | None = None
    dashboard: dict[str, Any] | None = None
    instructions: dict[str, Any] | None = None
    try:
        healthz = get_json(f"{api_base}/healthz")
    except Exception as exc:
        checks.append(
            ReadinessCheck(
                "api.healthz",
                "blocker",
                "Trader API health endpoint is not reachable.",
                {"error": str(exc)},
            )
        )
    if healthz is not None:
        checks.extend(check_healthz(healthz))

    try:
        dashboard_payload = get_json(f"{api_base}/v1/read/rl-dashboard")
        dashboard = dashboard_payload.get("rl_dashboard")
        if not isinstance(dashboard, dict):
            raise ValueError("response did not contain rl_dashboard object")
    except Exception as exc:
        checks.append(
            ReadinessCheck(
                "api.rl_dashboard",
                "blocker",
                "RL dashboard read model is not reachable.",
                {"error": str(exc)},
            )
        )
    if dashboard is not None:
        checks.extend(
            check_rl_dashboard(
                dashboard,
                trade_date=trade_date,
                allow_empty_candidates=allow_empty_candidates,
                expected_model_count=expected_model_count,
                expected_deployment_count=expected_deployment_count,
            )
        )

    try:
        instructions = get_json(
            f"{api_base}/v1/instructions?"
            + urllib.parse.urlencode({"limit": "500"})
        )
    except Exception as exc:
        checks.append(
            ReadinessCheck(
                "api.instructions",
                "blocker",
                "Instruction list endpoint is not reachable.",
                {"error": str(exc)},
            )
        )
    if instructions is not None:
        checks.extend(check_instruction_states(instructions, trade_date=trade_date))

    payload_checks = [asdict(check) for check in checks]
    blocker_count = sum(1 for check in checks if check.status == "blocker")
    warning_count = sum(1 for check in checks if check.status == "warning")
    return {
        "accepted": True,
        "generated_at": generated_at.isoformat(),
        "api_base": api_base,
        "trade_date": trade_date,
        "status": "blocked" if blocker_count else "ready_with_warnings" if warning_count else "ready",
        "blocker_count": blocker_count,
        "warning_count": warning_count,
        "checks": payload_checks,
    }


def check_python_dependencies() -> list[ReadinessCheck]:
    modules = [
        "ibapi.client",
        "ibapi.contract",
        "ibapi.order",
        "ibapi.order_cancel",
        "ibapi.sync_wrapper",
        "google.protobuf",
    ]
    missing: list[str] = []
    for module_name in modules:
        try:
            importlib.import_module(module_name)
        except ModuleNotFoundError:
            missing.append(module_name)
    if missing:
        return [
            ReadinessCheck(
                "python.ibapi",
                "blocker",
                "The official full IBKR Python API package is incomplete or missing.",
                {"missing_modules": missing},
            )
        ]
    return [
        ReadinessCheck(
            "python.ibapi",
            "ok",
            "Full IBKR Python API modules are importable.",
            {"checked_modules": modules},
        )
    ]


def check_model_bundles(root: Path | None) -> list[ReadinessCheck]:
    try:
        artifacts = promoted_rl_models(root)
        validations = {
            artifact.model_key: validate_promoted_artifact(artifact)
            for artifact in artifacts
        }
    except Exception as exc:
        return [
            ReadinessCheck(
                "rl.model_bundles",
                "blocker",
                "Trader-local RL model bundles are not loadable.",
                {"error": str(exc), "model_root": str(root) if root else None},
            )
        ]

    checks = [
        ReadinessCheck(
            "rl.model_bundles",
            "ok",
            "Trader-local RL model bundles are present and internally consistent.",
            {
                "models": sorted(validations),
                "static_feature_counts": {
                    key: value["static_feature_count"]
                    for key, value in validations.items()
                },
            },
        )
    ]
    for artifact in artifacts:
        checks.extend(
            check_action_mapping(
                model_key=artifact.model_key,
                side=artifact.side_upper,
                action_space=list(artifact.action_space),
                entry_action=artifact.entry_action_name,
                take_profit_action=artifact.take_profit_action_name,
            )
        )
    return checks


def check_healthz(payload: Mapping[str, Any]) -> list[ReadinessCheck]:
    checks: list[ReadinessCheck] = []
    checks.append(
        ReadinessCheck(
            "api.healthz",
            "ok" if payload.get("status") == "ok" else "blocker",
            "Trader API health endpoint responded.",
            {"status": payload.get("status")},
        )
    )
    broker_sessions = payload.get("broker_sessions")
    if isinstance(broker_sessions, Mapping):
        for role in ("primary", "diagnostic"):
            session = broker_sessions.get(role)
            connected = isinstance(session, Mapping) and session.get("connected") is True
            checks.append(
                ReadinessCheck(
                    f"broker.{role}",
                    "ok" if connected else "blocker",
                    f"IBKR {role} session is connected."
                    if connected
                    else f"IBKR {role} session is not connected.",
                    dict(session or {}) if isinstance(session, Mapping) else {},
                )
            )
    runtime = payload.get("execution_runtime")
    if isinstance(runtime, Mapping):
        running = runtime.get("effective_status") == "RUNNING" and not runtime.get("last_error")
        checks.append(
            ReadinessCheck(
                "runtime.execution",
                "ok" if running else "blocker",
                "Execution runtime is running without a last error."
                if running
                else "Execution runtime is not healthy.",
                dict(runtime),
            )
        )
    broker_monitor = payload.get("broker_monitor")
    if isinstance(broker_monitor, Mapping):
        checks.extend(_check_monitor_part(broker_monitor, "heartbeat"))
        checks.extend(_check_monitor_part(broker_monitor, "snapshot_refresh"))
    return checks


def _check_monitor_part(
    broker_monitor: Mapping[str, Any],
    key: str,
) -> list[ReadinessCheck]:
    part = broker_monitor.get(key)
    if not isinstance(part, Mapping):
        status = "blocker"
        message = f"Broker monitor {key} status is missing."
        details: dict[str, Any] = {}
    elif part.get("ok") is True and part.get("is_stale") is not True:
        status = "ok"
        message = f"Broker monitor {key} is fresh."
        details = dict(part)
    elif part.get("is_stale") is True or not part.get("last_success_at"):
        status = "blocker"
        message = f"Broker monitor {key} is stale or has no successful sample."
        details = dict(part)
    else:
        status = "warning"
        message = (
            f"Broker monitor {key} had a recent failure, but the last successful "
            "sample is still fresh."
        )
        details = dict(part)
    return [
        ReadinessCheck(
            f"broker_monitor.{key}",
            status,
            message,
            details,
        )
    ]


def check_rl_dashboard(
    dashboard: Mapping[str, Any],
    *,
    trade_date: str,
    allow_empty_candidates: bool = False,
    expected_model_count: int,
    expected_deployment_count: int,
) -> list[ReadinessCheck]:
    checks: list[ReadinessCheck] = []
    models = dashboard.get("models") if isinstance(dashboard.get("models"), list) else []
    deployments = (
        dashboard.get("deployments") if isinstance(dashboard.get("deployments"), list) else []
    )
    candidates = (
        dashboard.get("candidates") if isinstance(dashboard.get("candidates"), list) else []
    )
    checks.append(
        _count_check(
            "rl.models",
            len(models),
            expected_model_count,
            "registered RL models",
        )
    )
    checks.append(
        _count_check(
            "rl.deployments",
            len(deployments),
            expected_deployment_count,
            "RL deployments",
        )
    )
    for model in models:
        if not isinstance(model, Mapping):
            continue
        checks.extend(
            check_action_mapping(
                model_key=str(model.get("model_key") or ""),
                side=str(model.get("side") or "").upper(),
                action_space=[str(item) for item in model.get("action_space") or []],
                entry_action=str(
                    (model.get("metadata") or {}).get("entry_action_name")
                    or (model.get("action_constraints") or {}).get("entry_action_name")
                    or ""
                ),
                take_profit_action=str(
                    (model.get("metadata") or {}).get("take_profit_action_name")
                    or (model.get("action_constraints") or {}).get("take_profit_action_name")
                    or ""
                ),
                allow_missing_named_actions=True,
            )
        )
    for deployment in deployments:
        if not isinstance(deployment, Mapping):
            continue
        checks.extend(check_deployment_health(deployment))
    checks.extend(
        check_candidate_payloads(
            candidates,
            trade_date=trade_date,
            allow_empty=allow_empty_candidates,
        )
    )
    return checks


def _count_check(
    name: str,
    actual: int,
    expected: int,
    label: str,
) -> ReadinessCheck:
    return ReadinessCheck(
        name,
        "ok" if actual == expected else "blocker",
        f"Found expected {expected} {label}."
        if actual == expected
        else f"Expected {expected} {label}, found {actual}.",
        {"actual": actual, "expected": expected},
    )


def check_action_mapping(
    *,
    model_key: str,
    side: str,
    action_space: list[str],
    entry_action: str = "",
    take_profit_action: str = "",
    allow_missing_named_actions: bool = False,
) -> list[ReadinessCheck]:
    expected = EXPECTED_RL_ACTIONS.get(side)
    if expected is None:
        return [
            ReadinessCheck(
                f"rl.actions.{model_key or 'unknown'}",
                "blocker",
                "RL model side is not LONG or SHORT.",
                {"side": side},
            )
        ]
    missing = [
        action
        for action in ("skip", "wait", "market_entry", "cancel_entry", "exit_market", "clear_exit")
        if action not in action_space
    ]
    for action in (expected["entry"], expected["take_profit"]):
        if action not in action_space:
            missing.append(action)
    mismatches: dict[str, str] = {}
    if entry_action and entry_action != expected["entry"]:
        mismatches["entry_action"] = entry_action
    if take_profit_action and take_profit_action != expected["take_profit"]:
        mismatches["take_profit_action"] = take_profit_action
    if not allow_missing_named_actions:
        if not entry_action:
            mismatches["entry_action"] = ""
        if not take_profit_action:
            mismatches["take_profit_action"] = ""
    status = "ok" if not missing and not mismatches else "blocker"
    return [
        ReadinessCheck(
            f"rl.actions.{model_key or side.lower()}",
            status,
            f"{side} RL action mapping is explicit and side-correct."
            if status == "ok"
            else f"{side} RL action mapping is incomplete or wrong.",
            {
                "side": side,
                "missing_actions": missing,
                "mismatches": mismatches,
                "expected": expected,
            },
        )
    ]


def check_deployment_health(deployment: Mapping[str, Any]) -> list[ReadinessCheck]:
    key = str(deployment.get("deployment_key") or "unknown")
    heartbeat = deployment.get("heartbeat")
    if not isinstance(heartbeat, Mapping):
        return [
            ReadinessCheck(
                f"rl.deployment.{key}",
                "blocker",
                "Deployment has no heartbeat.",
                {"deployment": key},
            )
        ]
    status = str(heartbeat.get("status") or "").lower()
    stale = bool(heartbeat.get("is_stale"))
    runtime_error = heartbeat.get("runtime_error")
    ok = status == "running" and not stale and not runtime_error
    return [
        ReadinessCheck(
            f"rl.deployment.{key}",
            "ok" if ok else "blocker",
            "Deployment heartbeat is running and fresh."
            if ok
            else "Deployment heartbeat is degraded, stale, or reporting an error.",
            {
                "deployment_key": key,
                "status": status,
                "runtime_error": runtime_error,
                "last_seen_at": heartbeat.get("last_seen_at"),
                "last_bar_at": heartbeat.get("last_bar_at"),
                "last_action_at": heartbeat.get("last_action_at"),
            },
        )
    ]


def check_candidate_payloads(
    candidates: list[Any],
    *,
    trade_date: str,
    allow_empty: bool = False,
) -> list[ReadinessCheck]:
    todays = [
        candidate
        for candidate in candidates
        if isinstance(candidate, Mapping)
        and (candidate.get("trace") or {}).get("trade_date") == trade_date
    ]
    if not todays:
        if allow_empty:
            return [
                ReadinessCheck(
                    "rl.candidates",
                    "ok",
                    "No active model-routed candidates remain after the trade window.",
                    {"trade_date": trade_date, "candidate_count": len(candidates)},
                )
            ]
        return [
            ReadinessCheck(
                "rl.candidates",
                "warning",
                "No model-routed candidates found for the requested trade date.",
                {"trade_date": trade_date, "candidate_count": len(candidates)},
            )
        ]
    missing_static: list[str] = []
    bad_static: list[str] = []
    missing_capital_plan: list[str] = []
    for candidate in todays:
        candidate_id = str(candidate.get("instruction_id") or candidate.get("candidate_id"))
        trace = candidate.get("trace") if isinstance(candidate.get("trace"), Mapping) else {}
        metadata = trace.get("metadata") if isinstance(trace.get("metadata"), Mapping) else {}
        static_features = metadata.get("static_features")
        if not isinstance(static_features, Mapping):
            missing_static.append(candidate_id)
        elif not _static_features_valid(static_features, candidate):
            bad_static.append(candidate_id)
        if not isinstance(metadata.get("capital_plan"), Mapping):
            missing_capital_plan.append(candidate_id)
    status = "ok" if not missing_static and not bad_static else "blocker"
    if status == "ok" and missing_capital_plan:
        status = "warning"
    return [
        ReadinessCheck(
            "rl.candidates",
            status,
            "Candidates carry static features and capital plans."
            if status == "ok"
            else "Some candidates are missing required model inputs or sizing metadata.",
            {
                "trade_date": trade_date,
                "candidate_count": len(todays),
                "missing_static_features": missing_static,
                "bad_static_features": bad_static,
                "missing_capital_plan": missing_capital_plan,
                "by_model": _count_by(todays, "model_id"),
            },
        )
    ]


def _candidate_window_closed(trade_date: str, *, reference_at: datetime) -> bool:
    try:
        parsed_trade_date = datetime.strptime(trade_date, "%Y-%m-%d").date()
    except ValueError:
        return False
    local_reference = reference_at.astimezone(STOCKHOLM_TZ)
    if local_reference.date() > parsed_trade_date:
        return True
    if local_reference.date() < parsed_trade_date:
        return False
    return local_reference.time() >= CANDIDATE_WINDOW_CLOSE


def _static_features_valid(
    static_features: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> bool:
    names = static_features.get("feature_names")
    values = static_features.get("values")
    return (
        static_features.get("normalized") is True
        and str(static_features.get("model_key") or "") == str(candidate.get("model_id") or "")
        and isinstance(names, list)
        and isinstance(values, list)
        and len(names) == len(values)
        and len(names) > 0
    )


def _count_by(rows: list[Mapping[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def check_instruction_states(
    payload: Mapping[str, Any],
    *,
    trade_date: str,
) -> list[ReadinessCheck]:
    instructions = payload.get("instructions")
    rows = instructions if isinstance(instructions, list) else []
    counts = _count_by(
        [row for row in rows if isinstance(row, Mapping)],
        "state",
    )
    expired_active = [
        str(row.get("instruction_id"))
        for row in rows
        if isinstance(row, Mapping)
        and str(row.get("state") or "") in ACTIVE_INSTRUCTION_STATES
        and _instruction_trade_date(row) < trade_date
    ]
    return [
        ReadinessCheck(
            "instructions.states",
            "ok" if not expired_active else "blocker",
            "No stale active instructions from earlier trade dates."
            if not expired_active
            else "There are stale active instructions from earlier trade dates.",
            {"counts": counts, "stale_active_instruction_ids": expired_active},
        )
    ]


def _instruction_trade_date(row: Mapping[str, Any]) -> str:
    payload = row.get("payload") if isinstance(row.get("payload"), Mapping) else {}
    instruction = payload.get("instruction") if isinstance(payload.get("instruction"), Mapping) else {}
    trace = instruction.get("trace") if isinstance(instruction.get("trace"), Mapping) else {}
    return str(trace.get("trade_date") or "")[:10]


def get_json(url: str, *, timeout: int = 30) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{url} -> HTTP {exc.code}: {detail}") from exc


def print_text_report(report: Mapping[str, Any]) -> None:
    print(
        f"RL readiness: {report['status']} "
        f"({report['blocker_count']} blockers, {report['warning_count']} warnings)"
    )
    for check in report["checks"]:
        marker = {
            "ok": "OK",
            "warning": "WARN",
            "blocker": "BLOCKER",
        }.get(check["status"], check["status"].upper())
        print(f"- {marker} {check['name']}: {check['message']}")


if __name__ == "__main__":
    raise SystemExit(main())
