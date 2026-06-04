#!/usr/bin/env bash
set -u

WATCH_PATH="${WATCH_PATH:-/}"
CLEANUP_USE_PERCENT="${CLEANUP_USE_PERCENT:-80}"
CRITICAL_USE_PERCENT="${CRITICAL_USE_PERCENT:-90}"
CLEANUP_MIN_FREE_MIB="${CLEANUP_MIN_FREE_MIB:-6144}"
CRITICAL_MIN_FREE_MIB="${CRITICAL_MIN_FREE_MIB:-2048}"
CLEANUP_ENABLED="${CLEANUP_ENABLED:-yes}"
DRY_RUN="${DRY_RUN:-no}"
STATE_FILE="${STATE_FILE:-/run/ibkr-disk-watchdog.last-run}"
LAST_ALERT_FILE="${LAST_ALERT_FILE:-/run/ibkr-disk-watchdog.last-alert}"
OPERATOR_ALERT_WEBHOOK_URL="${OPERATOR_ALERT_WEBHOOK_URL:-}"
OPERATOR_ALERT_NTFY_TOPIC="${OPERATOR_ALERT_NTFY_TOPIC:-}"
OPERATOR_ALERT_NTFY_URL="${OPERATOR_ALERT_NTFY_URL:-https://ntfy.sh}"
OPERATOR_ALERT_PUSHOVER_APP_TOKEN="${OPERATOR_ALERT_PUSHOVER_APP_TOKEN:-}"
OPERATOR_ALERT_PUSHOVER_USER_KEY="${OPERATOR_ALERT_PUSHOVER_USER_KEY:-}"
OPERATOR_ALERT_COOLDOWN_SECONDS="${OPERATOR_ALERT_COOLDOWN_SECONDS:-1800}"
JOURNAL_VACUUM_ENABLED="${JOURNAL_VACUUM_ENABLED:-yes}"
JOURNAL_VACUUM_SIZE="${JOURNAL_VACUUM_SIZE:-1G}"
JOURNAL_VACUUM_TIME="${JOURNAL_VACUUM_TIME:-14d}"
LOG_PRUNE_ENABLED="${LOG_PRUNE_ENABLED:-yes}"
LOG_PRUNE_ROOTS="${LOG_PRUNE_ROOTS:-/var/log}"
LOG_PRUNE_DAYS="${LOG_PRUNE_DAYS:-14}"
LOG_PRUNE_FIND_MAX_DEPTH="${LOG_PRUNE_FIND_MAX_DEPTH:-3}"

log() {
  printf '%s %s\n' "$(date --iso-8601=seconds)" "$*"
}

is_int() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

normalize_int() {
  local value="$1"
  local fallback="$2"
  if is_int "$value"; then
    printf '%s\n' "$value"
  else
    printf '%s\n' "$fallback"
  fi
}

disk_stats() {
  df -Pm "$WATCH_PATH" | awk 'NR == 2 {gsub(/%/, "", $5); print $4, $5}'
}

run_or_log() {
  if [[ "${DRY_RUN,,}" == "yes" ]]; then
    printf '%s dry-run:' "$(date --iso-8601=seconds)"
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi
  "$@"
}

read_epoch_file() {
  local path="$1"
  if [[ -f "$path" ]]; then
    local value
    value="$(cat "$path" 2>/dev/null || true)"
    if is_int "$value"; then
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
        "Title": "Quant disk pressure",
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
        "title": "Quant disk pressure",
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

vacuum_journal() {
  if [[ "${JOURNAL_VACUUM_ENABLED,,}" != "yes" ]]; then
    return 0
  fi
  if ! command -v journalctl >/dev/null 2>&1; then
    log "journalctl not available; skipping journal vacuum."
    return 0
  fi
  if [[ -n "$JOURNAL_VACUUM_TIME" ]]; then
    run_or_log journalctl --vacuum-time="$JOURNAL_VACUUM_TIME" || true
  fi
  if [[ -n "$JOURNAL_VACUUM_SIZE" ]]; then
    run_or_log journalctl --vacuum-size="$JOURNAL_VACUUM_SIZE" || true
  fi
}

prune_rotated_logs_in_root() {
  local root="$1"
  local days="$2"
  local max_depth="$3"
  if [[ ! -d "$root" ]]; then
    return 0
  fi

  if [[ "${DRY_RUN,,}" == "yes" ]]; then
    find "$root" -xdev -maxdepth "$max_depth" -type f \
      \( -name '*.gz' -o -name '*.xz' -o -name '*.zst' -o -name '*.old' -o -regex '.*/[^/]+\.[0-9]+$' \) \
      -mtime +"$days" -print || true
    return 0
  fi

  find "$root" -xdev -maxdepth "$max_depth" -type f \
    \( -name '*.gz' -o -name '*.xz' -o -name '*.zst' -o -name '*.old' -o -regex '.*/[^/]+\.[0-9]+$' \) \
    -mtime +"$days" -print -delete || true
}

prune_rotated_logs() {
  if [[ "${LOG_PRUNE_ENABLED,,}" != "yes" ]]; then
    return 0
  fi

  local days
  local max_depth
  days="$(normalize_int "$LOG_PRUNE_DAYS" 14)"
  max_depth="$(normalize_int "$LOG_PRUNE_FIND_MAX_DEPTH" 3)"

  local roots_raw="$LOG_PRUNE_ROOTS"
  local root
  IFS=':' read -r -a roots <<< "$roots_raw"
  for root in "${roots[@]}"; do
    if [[ -n "$root" ]]; then
      prune_rotated_logs_in_root "$root" "$days" "$max_depth"
    fi
  done
}

write_state() {
  local before_free="$1"
  local before_use="$2"
  local after_free="$3"
  local after_use="$4"
  local action="$5"
  {
    printf 'checked_at=%s\n' "$(date --iso-8601=seconds)"
    printf 'watch_path=%s\n' "$WATCH_PATH"
    printf 'action=%s\n' "$action"
    printf 'before_free_mib=%s\n' "$before_free"
    printf 'before_use_percent=%s\n' "$before_use"
    printf 'after_free_mib=%s\n' "$after_free"
    printf 'after_use_percent=%s\n' "$after_use"
    printf 'cleanup_use_percent=%s\n' "$CLEANUP_USE_PERCENT"
    printf 'cleanup_min_free_mib=%s\n' "$CLEANUP_MIN_FREE_MIB"
    printf 'critical_use_percent=%s\n' "$CRITICAL_USE_PERCENT"
    printf 'critical_min_free_mib=%s\n' "$CRITICAL_MIN_FREE_MIB"
  } > "$STATE_FILE"
}

main() {
  local cleanup_use critical_use cleanup_min_free critical_min_free
  cleanup_use="$(normalize_int "$CLEANUP_USE_PERCENT" 80)"
  critical_use="$(normalize_int "$CRITICAL_USE_PERCENT" 90)"
  cleanup_min_free="$(normalize_int "$CLEANUP_MIN_FREE_MIB" 6144)"
  critical_min_free="$(normalize_int "$CRITICAL_MIN_FREE_MIB" 2048)"

  local before_free before_use
  read -r before_free before_use < <(disk_stats)
  if ! is_int "$before_free" || ! is_int "$before_use"; then
    log "Could not read disk stats for $WATCH_PATH."
    return 1
  fi

  local action="none"
  if (( before_use >= cleanup_use || before_free <= cleanup_min_free )); then
    action="cleanup"
    log "Disk pressure on $WATCH_PATH: use=${before_use}% free=${before_free}MiB; cleanup threshold use>=${cleanup_use}% or free<=${cleanup_min_free}MiB."
    if [[ "${CLEANUP_ENABLED,,}" == "yes" ]]; then
      vacuum_journal
      prune_rotated_logs
    else
      log "Cleanup disabled; recording pressure only."
    fi
  else
    log "Disk OK on $WATCH_PATH: use=${before_use}% free=${before_free}MiB."
  fi

  local after_free after_use
  read -r after_free after_use < <(disk_stats)
  write_state "$before_free" "$before_use" "$after_free" "$after_use" "$action"

  if (( after_use >= critical_use || after_free <= critical_min_free )); then
    local message
    message="Disk pressure remains critical on $(hostname): ${WATCH_PATH} use=${after_use}% free=${after_free}MiB after action=${action}."
    log "$message"
    send_operator_alert "$message"
    return 2
  fi

  return 0
}

main "$@"
