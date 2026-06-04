#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/../.." && pwd)"

WATCHDOG_SCRIPT="$REPO_ROOT/ops/scripts/ibkr_disk_watchdog.sh"
SERVICE_UNIT="$REPO_ROOT/ops/systemd/ibkr-disk-watchdog.service"
TIMER_UNIT="$REPO_ROOT/ops/systemd/ibkr-disk-watchdog.timer"

for path in "$WATCHDOG_SCRIPT" "$SERVICE_UNIT" "$TIMER_UNIT"; do
  if [[ ! -f "$path" ]]; then
    echo "Missing required file: $path" >&2
    exit 1
  fi
done

echo "Installing quant disk watchdog with sudo."
echo "This may ask for your password once."
sudo -v

sudo install -m 0755 "$WATCHDOG_SCRIPT" /usr/local/sbin/ibkr_disk_watchdog.sh
sudo install -m 0644 "$SERVICE_UNIT" /etc/systemd/system/ibkr-disk-watchdog.service
sudo install -m 0644 "$TIMER_UNIT" /etc/systemd/system/ibkr-disk-watchdog.timer

sudo systemctl daemon-reload
sudo systemctl enable --now ibkr-disk-watchdog.timer

echo
echo "Timer:"
sudo systemctl list-timers ibkr-disk-watchdog.timer --no-pager

echo
echo "Running one immediate watchdog check:"
sudo systemctl start ibkr-disk-watchdog.service

echo
echo "Service status:"
sudo systemctl status ibkr-disk-watchdog.service --no-pager --full || true

echo
echo "Latest watchdog state:"
sudo test -f /run/ibkr-disk-watchdog.last-run \
  && sudo cat /run/ibkr-disk-watchdog.last-run \
  || echo "No state file written yet."
