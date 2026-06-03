from __future__ import annotations

from typing import Any
from typing import Callable

from ibkr_trader.orchestration.runtime_types import serialize_for_json


def broker_exception_payload(exc: Exception) -> dict[str, Any]:
    """Serialize broker exceptions while preserving outbound wire-audit evidence."""

    payload: dict[str, Any] = {"error": str(exc)}
    wire_audit = getattr(exc, "ibkr_wire_audit", None)
    if isinstance(wire_audit, list):
        payload["ibkr_wire_audit"] = serialize_for_json(wire_audit)
    return payload


def is_retryable_broker_error(exc: Exception) -> bool:
    """Classify transient IBKR API errors that are safe to retry inside a cycle."""

    if isinstance(exc, ConnectionError):
        return True
    message = str(exc).lower()
    if "[326]" in message or "client id is already in use" in message:
        return True
    if "[103]" in message and "duplicate order id" in message:
        return True
    return _is_blank_broker_cancel_error_message(message)


def run_with_broker_retries(
    operation: Callable[[], Any],
    *,
    retry_delays: tuple[float, ...],
    sleep_fn: Callable[[float], None],
) -> Any:
    """Run one broker operation with the runtime's retry policy."""

    attempts = len(retry_delays) + 1
    for attempt_index in range(attempts):
        try:
            return operation()
        except Exception as exc:
            is_last_attempt = attempt_index >= attempts - 1
            if is_last_attempt or not is_retryable_broker_error(exc):
                raise
            sleep_fn(retry_delays[attempt_index])
    raise RuntimeError("broker retry loop exited without returning or raising")


def _is_blank_broker_cancel_error_message(message: str) -> bool:
    marker = "[202] order canceled - reason:"
    if marker not in message:
        return False
    return not message.split(marker, 1)[1].strip()
