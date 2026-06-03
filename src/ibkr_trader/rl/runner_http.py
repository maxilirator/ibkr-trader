from __future__ import annotations

import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Mapping


class ApiError(RuntimeError):
    pass


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
