from __future__ import annotations

import json
import logging
import os
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

_DEFAULT_NTFY_URL = "https://ntfy.sh"
_DEFAULT_TIMEOUT_SECONDS = 10


def send_operator_alert(
    message: str,
    *,
    title: str = "Instruction needs review",
) -> bool:
    normalized_message = message.strip()
    if not normalized_message:
        return False

    webhook_url = os.getenv("OPERATOR_ALERT_WEBHOOK_URL", "").strip()
    ntfy_topic = os.getenv("OPERATOR_ALERT_NTFY_TOPIC", "").strip()
    ntfy_url = os.getenv("OPERATOR_ALERT_NTFY_URL", _DEFAULT_NTFY_URL).strip()
    pushover_token = os.getenv("OPERATOR_ALERT_PUSHOVER_APP_TOKEN", "").strip()
    pushover_user = os.getenv("OPERATOR_ALERT_PUSHOVER_USER_KEY", "").strip()

    if not any((webhook_url, ntfy_topic, pushover_token and pushover_user)):
        return False

    if webhook_url and _send_webhook_alert(webhook_url, normalized_message):
        return True
    if ntfy_topic and _send_ntfy_alert(
        ntfy_url,
        ntfy_topic,
        normalized_message,
        title=title,
    ):
        return True
    if pushover_token and pushover_user and _send_pushover_alert(
        pushover_token,
        pushover_user,
        normalized_message,
        title=title,
    ):
        return True
    return False


def _send_webhook_alert(url: str, message: str) -> bool:
    body = json.dumps({"text": message}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    return _perform_request(request, context="webhook")


def _send_ntfy_alert(
    base_url: str,
    topic: str,
    message: str,
    *,
    title: str,
) -> bool:
    url = f"{base_url.rstrip('/')}/{urllib.parse.quote(topic)}"
    request = urllib.request.Request(
        url,
        data=message.encode("utf-8"),
        headers={
            "Title": title,
            "Priority": "urgent",
            "Tags": "warning",
        },
        method="POST",
    )
    return _perform_request(request, context="ntfy")


def _send_pushover_alert(
    token: str,
    user_key: str,
    message: str,
    *,
    title: str,
) -> bool:
    body = urllib.parse.urlencode(
        {
            "token": token,
            "user": user_key,
            "title": title,
            "message": message,
            "priority": "1",
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        "https://api.pushover.net/1/messages.json",
        data=body,
        method="POST",
    )
    return _perform_request(request, context="pushover")


def _perform_request(request: urllib.request.Request, *, context: str) -> bool:
    try:
        with urllib.request.urlopen(  # noqa: S310
            request,
            timeout=_DEFAULT_TIMEOUT_SECONDS,
        ) as response:
            response.read()
        return True
    except Exception:
        logger.exception("Failed to send operator alert via %s.", context)
        return False
