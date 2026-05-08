from __future__ import annotations

import os
import re
import subprocess
import time
from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from typing import Any


_CACHE_TTL_SECONDS = 5.0
_CACHE: tuple[float, dict[str, Any]] | None = None
_DEDICATED_GATEWAY_EXISTING_SESSION_ACTION = "primary"


@dataclass(slots=True)
class IbGatewayDiagnostics:
    status: str
    severity: str
    summary: str
    unit: str
    latest_event_at: str | None = None
    latest_event: str | None = None
    latest_dialog: str | None = None
    latest_frame: str | None = None
    existing_session_detected_at: str | None = None
    existing_session_decision: str | None = None
    existing_session_action: str | None = None
    shutdown_progress_at: str | None = None
    command_server_shutdown_at: str | None = None
    restart_in_progress_at: str | None = None
    second_factor_at: str | None = None
    login_completed_at: str | None = None
    deadlock_at: str | None = None
    configured_existing_session_action: str | None = None
    recommended_existing_session_action: str | None = None
    configuration_warnings: tuple[str, ...] = ()
    shutdown_progress_age_seconds: int | None = None
    stuck_shutdown_threshold_seconds: int | None = None
    api_client_ids_seen: tuple[int, ...] = ()
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _diagnostics_enabled() -> bool:
    return os.getenv("IBKR_GATEWAY_DIAGNOSTICS_ENABLED", "true").lower() not in {
        "0",
        "false",
        "no",
    }


def _line_timestamp(line: str) -> str | None:
    token = line.split(maxsplit=1)[0] if line else ""
    if re.match(r"^\d{4}-\d{2}-\d{2}T", token):
        return token
    return None


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _age_seconds(event_at: str | None, *, now: datetime) -> int | None:
    parsed = _parse_iso_timestamp(event_at)
    if parsed is None:
        return None
    return max(0, int((now - parsed).total_seconds()))


def _stuck_shutdown_threshold_seconds() -> int:
    raw_value = os.getenv("IBKR_GATEWAY_STUCK_SHUTDOWN_SECONDS", "90")
    try:
        return max(1, int(raw_value))
    except ValueError:
        return 90


def _configuration_warnings(state: dict[str, Any]) -> tuple[str, ...]:
    configured_action = str(
        state.get("configured_existing_session_action") or ""
    ).strip().lower()
    if configured_action and configured_action != _DEDICATED_GATEWAY_EXISTING_SESSION_ACTION:
        return (
            "Dedicated live Gateway should use "
            f"ExistingSessionDetectedAction={_DEDICATED_GATEWAY_EXISTING_SESSION_ACTION}; "
            f"current value is {configured_action}.",
        )
    return ()


def _event_after(event_at: str | None, baseline_at: str | None) -> bool:
    return event_at is not None and (baseline_at is None or event_at > baseline_at)


def _summarize_status(state: dict[str, Any]) -> tuple[str, str, str]:
    if state.get("error"):
        return "unavailable", "warn", "Gateway diagnostics unavailable."
    if state.get("deadlock_at"):
        return "deadlock_reported", "bad", "IB Gateway reported a Java deadlock."
    if state.get("shutdown_progress_at"):
        age_seconds = state.get("shutdown_progress_age_seconds")
        threshold_seconds = state.get("stuck_shutdown_threshold_seconds")
        if (
            isinstance(age_seconds, int)
            and isinstance(threshold_seconds, int)
            and age_seconds < threshold_seconds
        ):
            return "shutdown_in_progress", "warn", "IB Gateway shutdown is in progress."
        if state.get("existing_session_detected_at"):
            return (
                "stuck_shutdown_after_existing_session",
                "bad",
                "IB Gateway is stuck shutting down after an existing-session conflict.",
            )
        return "stuck_shutdown", "bad", "IB Gateway is stuck in shutdown progress."
    if state.get("existing_session_detected_at"):
        return (
            "existing_session_detected",
            "bad",
            "IB Gateway detected another active broker session.",
        )
    if _event_after(
        state.get("restart_in_progress_at"),
        state.get("login_completed_at"),
    ):
        return "restart_in_progress", "warn", "IB Gateway restart is in progress."
    if _event_after(state.get("second_factor_at"), state.get("login_completed_at")):
        return "second_factor", "warn", "IB Gateway is waiting on second factor auth."
    if state.get("login_completed_at"):
        if state.get("restart_in_progress_at") or state.get("second_factor_at"):
            if state.get("configuration_warnings"):
                return (
                    "login_completed_after_restart_2fa",
                    "warn",
                    "IB Gateway login completed after restart/2FA, but Gateway configuration needs review.",
                )
            return (
                "login_completed_after_restart_2fa",
                "ok",
                "IB Gateway login completed after restart/2FA.",
            )
        if state.get("configuration_warnings"):
            return (
                "login_completed_with_config_warning",
                "warn",
                "IB Gateway login completed, but Gateway configuration needs review.",
            )
        return "login_completed", "ok", "IB Gateway login completed."
    return "unknown", "warn", "No recent IB Gateway UI state was recognized."


def _parse_journal(lines: list[str], *, unit: str) -> dict[str, Any]:
    state: dict[str, Any] = {
        "unit": unit,
        "latest_event_at": None,
        "latest_event": None,
        "latest_dialog": None,
        "latest_frame": None,
        "existing_session_detected_at": None,
        "existing_session_decision": None,
        "existing_session_action": None,
        "shutdown_progress_at": None,
        "command_server_shutdown_at": None,
        "restart_in_progress_at": None,
        "second_factor_at": None,
        "login_completed_at": None,
        "deadlock_at": None,
        "configured_existing_session_action": None,
        "recommended_existing_session_action": _DEDICATED_GATEWAY_EXISTING_SESSION_ACTION,
        "configuration_warnings": (),
        "shutdown_progress_age_seconds": None,
        "stuck_shutdown_threshold_seconds": _stuck_shutdown_threshold_seconds(),
        "api_client_ids_seen": set(),
        "error": None,
    }

    event_re = re.compile(
        r"IBC: detected (?P<kind>dialog|frame) entitled: "
        r"(?P<title>[^;]+); event=(?P<event>.+)$"
    )
    client_re = re.compile(r"addLogConsole Client (?P<client_id>\d+)")
    configured_action_re = re.compile(r"ExistingSessionDetectedAction=(?P<action>\S+)")

    for line in lines:
        timestamp = _line_timestamp(line)
        event_match = event_re.search(line)
        if event_match is not None:
            kind = event_match.group("kind")
            title = event_match.group("title").strip()
            event = event_match.group("event").strip()
            state["latest_event_at"] = timestamp or state["latest_event_at"]
            state["latest_event"] = f"{kind}:{title}:{event}"
            if kind == "dialog":
                state["latest_dialog"] = title
            else:
                state["latest_frame"] = title
            if title == "Existing session detected":
                state["existing_session_detected_at"] = (
                    timestamp or state["existing_session_detected_at"]
                )
            elif title == "Shutdown progress" and event == "Opened":
                state["shutdown_progress_at"] = timestamp or state["shutdown_progress_at"]
            elif title == "Restart in progress" and event == "Opened":
                state["restart_in_progress_at"] = timestamp or state["restart_in_progress_at"]
            elif title == "Second Factor Authentication" and event == "Opened":
                state["second_factor_at"] = timestamp or state["second_factor_at"]
            continue

        if "ExistingSessionDetectedAction=" in line:
            match = configured_action_re.search(line)
            if match is not None:
                state["configured_existing_session_action"] = match.group("action")
        if "Other session may be primary" in line:
            state["existing_session_decision"] = line.split("IBC:", 1)[-1].strip()
        if "IBC: Click button:" in line and state.get("existing_session_detected_at"):
            state["existing_session_action"] = line.split("IBC:", 1)[-1].strip()
        if "IBC: CommandServer is shutdown" in line:
            state["command_server_shutdown_at"] = timestamp or state[
                "command_server_shutdown_at"
            ]
        if "IBC: Login has completed" in line:
            state["login_completed_at"] = timestamp or state["login_completed_at"]
            state["latest_event_at"] = timestamp or state["latest_event_at"]
            state["latest_event"] = "login_completed"
            state["latest_dialog"] = None
            state["existing_session_detected_at"] = None
            state["existing_session_decision"] = None
            state["existing_session_action"] = None
            state["shutdown_progress_at"] = None
            state["command_server_shutdown_at"] = None
            state["deadlock_at"] = None
        if "JTS-DeadlockMonitor" in line or "DeadlockMonitor" in line:
            state["deadlock_at"] = timestamp or state["deadlock_at"]
        client_match = client_re.search(line)
        if client_match is not None:
            state["api_client_ids_seen"].add(int(client_match.group("client_id")))

    state["shutdown_progress_age_seconds"] = _age_seconds(
        state.get("shutdown_progress_at"),
        now=datetime.now(timezone.utc),
    )
    state["configuration_warnings"] = _configuration_warnings(state)
    status, severity, summary = _summarize_status(state)
    state["status"] = status
    state["severity"] = severity
    state["summary"] = summary
    state["api_client_ids_seen"] = tuple(sorted(state["api_client_ids_seen"]))
    return state


def read_ibgateway_diagnostics(
    *,
    unit: str | None = None,
    line_limit: int = 300,
    since: str = "6 hours ago",
    timeout_seconds: float = 0.8,
    use_cache: bool = True,
) -> dict[str, Any]:
    if not _diagnostics_enabled():
        return IbGatewayDiagnostics(
            status="disabled",
            severity="warn",
            summary="Gateway diagnostics are disabled.",
            unit=unit or os.getenv("IBKR_GATEWAY_SYSTEMD_UNIT", "ibgateway-ibc.service"),
            recommended_existing_session_action=_DEDICATED_GATEWAY_EXISTING_SESSION_ACTION,
        ).to_dict()

    resolved_unit = unit or os.getenv(
        "IBKR_GATEWAY_SYSTEMD_UNIT",
        "ibgateway-ibc.service",
    )
    now = time.monotonic()
    global _CACHE
    if use_cache and _CACHE is not None and now - _CACHE[0] <= _CACHE_TTL_SECONDS:
        return dict(_CACHE[1])

    command = [
        "journalctl",
        "-u",
        resolved_unit,
        "--since",
        since,
        "-n",
        str(line_limit),
        "-o",
        "short-iso",
        "--no-pager",
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        diagnostics = IbGatewayDiagnostics(
            status="unavailable",
            severity="warn",
            summary="Gateway diagnostics unavailable.",
            unit=resolved_unit,
            recommended_existing_session_action=_DEDICATED_GATEWAY_EXISTING_SESSION_ACTION,
            error=str(exc),
        ).to_dict()
    else:
        if completed.returncode != 0:
            diagnostics = IbGatewayDiagnostics(
                status="unavailable",
                severity="warn",
                summary="Gateway diagnostics unavailable.",
                unit=resolved_unit,
                recommended_existing_session_action=_DEDICATED_GATEWAY_EXISTING_SESSION_ACTION,
                error=(completed.stderr or completed.stdout or "journalctl failed").strip(),
            ).to_dict()
        else:
            diagnostics = _parse_journal(completed.stdout.splitlines(), unit=resolved_unit)

    _CACHE = (now, dict(diagnostics))
    return diagnostics


def format_gateway_diagnostic_hint(diagnostics: dict[str, Any]) -> str | None:
    status = str(diagnostics.get("status") or "")
    if status in {"", "disabled", "login_completed", "unavailable", "unknown"}:
        return None
    summary = str(diagnostics.get("summary") or "Gateway diagnostic available.")
    details: list[str] = []
    latest_dialog = diagnostics.get("latest_dialog")
    latest_event_at = diagnostics.get("latest_event_at")
    if latest_dialog:
        details.append(f"latest dialog {latest_dialog}")
    if latest_event_at:
        details.append(f"latest event at {latest_event_at}")
    if diagnostics.get("existing_session_detected_at"):
        details.append(
            f"existing session detected at {diagnostics['existing_session_detected_at']}"
        )
    if diagnostics.get("existing_session_action"):
        details.append(str(diagnostics["existing_session_action"]))
    if diagnostics.get("configured_existing_session_action"):
        details.append(
            "configured ExistingSessionDetectedAction="
            f"{diagnostics['configured_existing_session_action']}"
        )
    suffix = f" ({'; '.join(details)})" if details else ""
    return f"IB Gateway diagnostic: {summary}{suffix}."
