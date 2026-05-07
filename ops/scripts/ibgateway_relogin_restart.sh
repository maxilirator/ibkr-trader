#!/usr/bin/env bash
set -euo pipefail

GATEWAY_SERVICE="${GATEWAY_SERVICE:-ibgateway-ibc.service}"
IBC_CONFIG_PATH="${IBC_CONFIG_PATH:-/home/ibgateway/ibc/config.ini}"
IBC_COMMAND_HOST="${IBC_COMMAND_HOST:-127.0.0.1}"
IBC_COMMAND_PORT="${IBC_COMMAND_PORT:-}"
RELOGIN_AFTER_TWOFA_TIMEOUT="${RELOGIN_AFTER_TWOFA_TIMEOUT:-yes}"
LEGACY_EXIT_AFTER_TWOFA_TIMEOUT="${LEGACY_EXIT_AFTER_TWOFA_TIMEOUT:-no}"
FALLBACK_SYSTEMCTL_RESTART="${FALLBACK_SYSTEMCTL_RESTART:-no}"

log() {
  printf '%s %s\n' "$(date --iso-8601=seconds)" "$*"
}

ensure_ini_setting() {
  local key="$1"
  local value="$2"
  local path="$3"
  if grep -qE "^${key}=" "$path"; then
    sed -i "s|^${key}=.*|${key}=${value}|" "$path"
  else
    printf '\n%s=%s\n' "$key" "$value" >> "$path"
  fi
}

read_ini_setting() {
  local key="$1"
  local path="$2"
  awk -F= -v key="$key" '$1 == key { print $2; exit }' "$path" \
    | tr -d '\r' \
    | xargs
}

send_ibc_command() {
  local host="$1"
  local port="$2"
  local command="$3"
  python3 - "$host" "$port" "$command" <<'PY'
from __future__ import annotations

import socket
import sys

host, raw_port, command = sys.argv[1:4]
port = int(raw_port)
payload = f"{command}\nEXIT\n".encode("utf-8")

with socket.create_connection((host, port), timeout=10) as sock:
    sock.settimeout(10)
    sock.sendall(payload)
    try:
        response = sock.recv(4096)
    except socket.timeout:
        response = b""

if response:
    sys.stdout.write(response.decode("utf-8", errors="replace"))
PY
}

if [[ ! -f "$IBC_CONFIG_PATH" ]]; then
  log "IBC config not found: $IBC_CONFIG_PATH"
  exit 1
fi

ensure_ini_setting \
  "ReloginAfterSecondFactorAuthenticationTimeout" \
  "$RELOGIN_AFTER_TWOFA_TIMEOUT" \
  "$IBC_CONFIG_PATH"
ensure_ini_setting \
  "ExitAfterSecondFactorAuthenticationTimeout" \
  "$LEGACY_EXIT_AFTER_TWOFA_TIMEOUT" \
  "$IBC_CONFIG_PATH"

if [[ -z "$IBC_COMMAND_PORT" ]]; then
  IBC_COMMAND_PORT="$(read_ini_setting "CommandServerPort" "$IBC_CONFIG_PATH")"
fi

if [[ -z "$IBC_COMMAND_PORT" || "$IBC_COMMAND_PORT" == "0" ]]; then
  log "IBC CommandServerPort is not configured in $IBC_CONFIG_PATH."
else
  log "Requesting IBC CommandServer RESTART on ${IBC_COMMAND_HOST}:${IBC_COMMAND_PORT}."
  if send_ibc_command "$IBC_COMMAND_HOST" "$IBC_COMMAND_PORT" "RESTART"; then
    log "IBC CommandServer accepted RESTART for $GATEWAY_SERVICE."
    exit 0
  fi
  log "IBC CommandServer RESTART failed for ${IBC_COMMAND_HOST}:${IBC_COMMAND_PORT}."
fi

if [[ "${FALLBACK_SYSTEMCTL_RESTART,,}" == "yes" ]]; then
  log "Falling back to systemctl restart for $GATEWAY_SERVICE; this may require fresh 2FA."
  exec systemctl restart "$GATEWAY_SERVICE"
fi

log "Not falling back to systemctl restart. Manual operator attention may be required."
exit 1
