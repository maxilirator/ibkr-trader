#!/usr/bin/env bash
set -u

TRADER_API_BASE_URL="${TRADER_API_BASE_URL:-http://127.0.0.1:8000}"
GATEWAY_SERVICE="${GATEWAY_SERVICE:-ibgateway-ibc.service}"
FAILURE_THRESHOLD="${FAILURE_THRESHOLD:-3}"
CURL_TIMEOUT_SECONDS="${CURL_TIMEOUT_SECONDS:-20}"
STATE_FILE="${STATE_FILE:-/run/ibgateway-api-watchdog.failures}"
LAST_ERROR_FILE="${LAST_ERROR_FILE:-/run/ibgateway-api-watchdog.last-error}"
LAST_JOURNAL_FILE="${LAST_JOURNAL_FILE:-/run/ibgateway-api-watchdog.last-journal}"
JOURNAL_SINCE="${JOURNAL_SINCE:-20 minutes ago}"

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

log "Restarting ${GATEWAY_SERVICE} after ${failures} consecutive failed IBKR API probes."
if systemctl restart "$GATEWAY_SERVICE"; then
  reset_failures
  log "Restart command completed for ${GATEWAY_SERVICE}."
  exit 0
fi

log "Restart command failed for ${GATEWAY_SERVICE}; keeping failure counter at ${failures}."
exit 1
