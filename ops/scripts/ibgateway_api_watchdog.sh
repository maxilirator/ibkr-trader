#!/usr/bin/env bash
set -u

TRADER_API_BASE_URL="${TRADER_API_BASE_URL:-http://127.0.0.1:8000}"
GATEWAY_SERVICE="${GATEWAY_SERVICE:-ibgateway-ibc.service}"
GATEWAY_RESTART_COMMAND="${GATEWAY_RESTART_COMMAND:-}"
FAILURE_THRESHOLD="${FAILURE_THRESHOLD:-6}"
CURL_TIMEOUT_SECONDS="${CURL_TIMEOUT_SECONDS:-20}"
STATE_FILE="${STATE_FILE:-/run/ibgateway-api-watchdog.failures}"
LAST_ERROR_FILE="${LAST_ERROR_FILE:-/run/ibgateway-api-watchdog.last-error}"
LAST_JOURNAL_FILE="${LAST_JOURNAL_FILE:-/run/ibgateway-api-watchdog.last-journal}"
LAST_RESTART_FILE="${LAST_RESTART_FILE:-/run/ibgateway-api-watchdog.last-restart}"
LAST_ALERT_FILE="${LAST_ALERT_FILE:-/run/ibgateway-api-watchdog.last-alert}"
JOURNAL_SINCE="${JOURNAL_SINCE:-20 minutes ago}"
RESTART_COOLDOWN_SECONDS="${RESTART_COOLDOWN_SECONDS:-900}"
RESTART_ALLOWED_WINDOWS="${RESTART_ALLOWED_WINDOWS:-}"
RESTART_WINDOW_TZ="${RESTART_WINDOW_TZ:-Europe/Stockholm}"
RESTART_ALLOWED_DAYS="${RESTART_ALLOWED_DAYS:-Mon,Tue,Wed,Thu,Fri,Sat,Sun}"
STARTUP_GRACE_SECONDS="${STARTUP_GRACE_SECONDS:-300}"
OPERATOR_ALERT_WEBHOOK_URL="${OPERATOR_ALERT_WEBHOOK_URL:-}"
OPERATOR_ALERT_NTFY_TOPIC="${OPERATOR_ALERT_NTFY_TOPIC:-}"
OPERATOR_ALERT_NTFY_URL="${OPERATOR_ALERT_NTFY_URL:-https://ntfy.sh}"
OPERATOR_ALERT_PUSHOVER_APP_TOKEN="${OPERATOR_ALERT_PUSHOVER_APP_TOKEN:-}"
OPERATOR_ALERT_PUSHOVER_USER_KEY="${OPERATOR_ALERT_PUSHOVER_USER_KEY:-}"
OPERATOR_ALERT_COOLDOWN_SECONDS="${OPERATOR_ALERT_COOLDOWN_SECONDS:-1800}"

log() {
  printf '%s %s\n' "$(date --iso-8601=seconds)" "$*"
}

read_failures() {
  if [[ -f "$STATE_FILE" ]]; then
    local value
    value="$(cat "$STATE_FILE" 2>/dev/null || true)"
    if [[ "$value" =~ ^[0-9]+$ ]]; then
      printf '%s\n' "$value"
      return
    fi
  fi
  printf '0\n'
}

write_failures() {
  local value="$1"
  printf '%s\n' "$value" > "$STATE_FILE"
}

reset_failures() {
  rm -f "$STATE_FILE" "$LAST_ERROR_FILE"
}

read_epoch_file() {
  local path="$1"
  if [[ -f "$path" ]]; then
    local value
    value="$(cat "$path" 2>/dev/null || true)"
    if [[ "$value" =~ ^[0-9]+$ ]]; then
      printf '%s\n' "$value"
      return
    fi
  fi
  printf '0\n'
}

seconds_since_epoch_file() {
  local path="$1"
  local value
  value="$(read_epoch_file "$path")"
  if [[ "$value" == "0" ]]; then
    printf '999999999\n'
    return
  fi
  printf '%s\n' "$(( $(date +%s) - value ))"
}

send_operator_alert() {
  local message="$1"
  if [[ -z "$OPERATOR_ALERT_WEBHOOK_URL" \
    && -z "$OPERATOR_ALERT_NTFY_TOPIC" \
    && ( -z "$OPERATOR_ALERT_PUSHOVER_APP_TOKEN" || -z "$OPERATOR_ALERT_PUSHOVER_USER_KEY" ) ]]; then
    return 0
  fi
  if (( $(seconds_since_epoch_file "$LAST_ALERT_FILE") < OPERATOR_ALERT_COOLDOWN_SECONDS )); then
    return 0
  fi
  if [[ -n "$OPERATOR_ALERT_WEBHOOK_URL" ]] && python3 - "$OPERATOR_ALERT_WEBHOOK_URL" "$message" <<'PY'
from __future__ import annotations

import json
import sys
import urllib.request

url, message = sys.argv[1:3]
body = json.dumps({"text": message}).encode("utf-8")
request = urllib.request.Request(
    url,
    data=body,
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=10) as response:
    response.read()
PY
  then
    date +%s > "$LAST_ALERT_FILE"
    log "Operator alert sent."
  elif [[ -n "$OPERATOR_ALERT_NTFY_TOPIC" ]] && python3 - "$OPERATOR_ALERT_NTFY_URL" "$OPERATOR_ALERT_NTFY_TOPIC" "$message" <<'PY'
from __future__ import annotations

import sys
import urllib.parse
import urllib.request

base_url, topic, message = sys.argv[1:4]
url = f"{base_url.rstrip('/')}/{urllib.parse.quote(topic)}"
request = urllib.request.Request(
    url,
    data=message.encode("utf-8"),
    headers={
        "Title": "IB Gateway needs attention",
        "Priority": "urgent",
        "Tags": "warning",
    },
    method="POST",
)
with urllib.request.urlopen(request, timeout=10) as response:
    response.read()
PY
  then
    date +%s > "$LAST_ALERT_FILE"
    log "Operator ntfy alert sent."
  elif [[ -n "$OPERATOR_ALERT_PUSHOVER_APP_TOKEN" && -n "$OPERATOR_ALERT_PUSHOVER_USER_KEY" ]] \
    && python3 - "$OPERATOR_ALERT_PUSHOVER_APP_TOKEN" "$OPERATOR_ALERT_PUSHOVER_USER_KEY" "$message" <<'PY'
from __future__ import annotations

import sys
import urllib.parse
import urllib.request

token, user_key, message = sys.argv[1:4]
body = urllib.parse.urlencode(
    {
        "token": token,
        "user": user_key,
        "title": "IB Gateway needs attention",
        "message": message,
        "priority": "1",
    }
).encode("utf-8")
request = urllib.request.Request(
    "https://api.pushover.net/1/messages.json",
    data=body,
    method="POST",
)
with urllib.request.urlopen(request, timeout=10) as response:
    response.read()
PY
  then
    date +%s > "$LAST_ALERT_FILE"
    log "Operator Pushover alert sent."
  else
    log "Operator alert failed."
  fi
}

restart_gateway() {
  if [[ -n "$GATEWAY_RESTART_COMMAND" ]]; then
    bash -lc "$GATEWAY_RESTART_COMMAND"
    return
  fi
  systemctl restart "$GATEWAY_SERVICE"
}

restart_window_allows_now() {
  if [[ -z "$RESTART_ALLOWED_WINDOWS" ]]; then
    return 0
  fi
  python3 - "$RESTART_ALLOWED_WINDOWS" "$RESTART_WINDOW_TZ" "$RESTART_ALLOWED_DAYS" <<'PY'
from __future__ import annotations

import sys
from datetime import datetime
from datetime import time
from zoneinfo import ZoneInfo

windows_raw, timezone_name, days_raw = sys.argv[1:4]
now = datetime.now(ZoneInfo(timezone_name))
allowed_days = {
    item.strip().lower()[:3]
    for item in days_raw.replace(",", " ").split()
    if item.strip()
}
if allowed_days and now.strftime("%a").lower()[:3] not in allowed_days:
    raise SystemExit(1)

for item in windows_raw.replace(",", " ").split():
    if "-" not in item:
        continue
    start_raw, end_raw = item.split("-", 1)
    start = time.fromisoformat(start_raw)
    end = time.fromisoformat(end_raw)
    current = now.time().replace(microsecond=0)
    if start <= end:
        if start <= current <= end:
            raise SystemExit(0)
    elif current >= start or current <= end:
        raise SystemExit(0)
raise SystemExit(1)
PY
}

api_health_url="${TRADER_API_BASE_URL%/}/healthz?refresh_broker_status=false"
probe_url="${TRADER_API_BASE_URL%/}/v1/ibkr/probe"

if ! curl -fsS --max-time "$CURL_TIMEOUT_SECONDS" "$api_health_url" >/dev/null 2>"$LAST_ERROR_FILE"; then
  log "Trader API health endpoint is unavailable; not restarting ${GATEWAY_SERVICE}."
  exit 0
fi

if curl -fsS --max-time "$CURL_TIMEOUT_SECONDS" -X POST "$probe_url" >/dev/null 2>"$LAST_ERROR_FILE"; then
  previous_failures="$(read_failures)"
  if [[ "$previous_failures" != "0" ]]; then
    log "IBKR API probe recovered after ${previous_failures} failure(s)."
  fi
  reset_failures
  exit 0
fi

if (( $(seconds_since_epoch_file "$LAST_RESTART_FILE") < STARTUP_GRACE_SECONDS )); then
  last_error="$(tr '\n' ' ' < "$LAST_ERROR_FILE" 2>/dev/null | cut -c1-500)"
  log "IBKR API probe is still failing during Gateway startup grace (${STARTUP_GRACE_SECONDS}s): ${last_error}"
  exit 0
fi

failures="$(( $(read_failures) + 1 ))"
write_failures "$failures"
last_error="$(tr '\n' ' ' < "$LAST_ERROR_FILE" 2>/dev/null | cut -c1-500)"
log "IBKR API probe failed (${failures}/${FAILURE_THRESHOLD}): ${last_error}"

if (( failures < FAILURE_THRESHOLD )); then
  exit 0
fi

journalctl -u "$GATEWAY_SERVICE" --since "$JOURNAL_SINCE" --no-pager 2>/dev/null \
  | grep -Ei 'JTS-DeadlockMonitor|Deadlock|EServerSocket|nextValidId|API|socket|Login has completed|addLogConsole|ERROR' \
  | tail -300 > "$LAST_JOURNAL_FILE" || true

if ! restart_window_allows_now; then
  log "Restart deferred; now is outside RESTART_ALLOWED_WINDOWS=${RESTART_ALLOWED_WINDOWS} ${RESTART_WINDOW_TZ} (${RESTART_ALLOWED_DAYS})."
  write_failures "$FAILURE_THRESHOLD"
  send_operator_alert "IB Gateway API probe is failing on $(hostname). Restart is outside the allowed window; manual login or 2FA may be required."
  exit 0
fi

if (( $(seconds_since_epoch_file "$LAST_RESTART_FILE") < RESTART_COOLDOWN_SECONDS )); then
  log "Restart due but deferred; ${GATEWAY_SERVICE} was restarted less than ${RESTART_COOLDOWN_SECONDS}s ago and the API probe is still failing."
  write_failures "$FAILURE_THRESHOLD"
  send_operator_alert "IB Gateway API is still failing on $(hostname) after a recent restart. Manual login or 2FA may be required."
  exit 0
fi

log "Restarting ${GATEWAY_SERVICE} after ${failures} consecutive failed IBKR API probes."
if restart_gateway; then
  date +%s > "$LAST_RESTART_FILE"
  reset_failures
  log "Restart command completed for ${GATEWAY_SERVICE}."
  send_operator_alert "IB Gateway was restarted on $(hostname) after ${failures} failed API probes. If IBKR prompts for 2FA, approve it on the mobile app."
  exit 0
fi

log "Restart command failed for ${GATEWAY_SERVICE}; keeping failure counter at ${failures}."
send_operator_alert "IB Gateway API probe is failing on $(hostname), and restarting ${GATEWAY_SERVICE} failed. Manual intervention is required."
exit 1
