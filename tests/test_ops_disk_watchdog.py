from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
WATCHDOG = REPO_ROOT / "ops" / "scripts" / "ibkr_disk_watchdog.sh"


def _touch_old(path: Path, *, days: int) -> None:
    path.write_text("old log\n", encoding="utf-8")
    old = time.time() - days * 24 * 60 * 60
    os.utime(path, (old, old))


def _run_watchdog(tmp_path: Path, log_root: Path, *, dry_run: bool = False) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(
        {
            "WATCH_PATH": str(tmp_path),
            "CLEANUP_USE_PERCENT": "0",
            "CLEANUP_MIN_FREE_MIB": "0",
            "CRITICAL_USE_PERCENT": "100",
            "CRITICAL_MIN_FREE_MIB": "0",
            "JOURNAL_VACUUM_ENABLED": "no",
            "LOG_PRUNE_ROOTS": str(log_root),
            "LOG_PRUNE_DAYS": "1",
            "STATE_FILE": str(tmp_path / "watchdog.state"),
            "LAST_ALERT_FILE": str(tmp_path / "watchdog.alert"),
            "DRY_RUN": "yes" if dry_run else "no",
        }
    )
    return subprocess.run(
        ["bash", str(WATCHDOG)],
        env=env,
        check=True,
        text=True,
        capture_output=True,
    )


def test_disk_watchdog_prunes_only_old_rotated_logs(tmp_path: Path) -> None:
    log_root = tmp_path / "logs"
    log_root.mkdir()
    old_rotated = log_root / "syslog.1"
    old_archive = log_root / "app.log.gz"
    active = log_root / "syslog"
    recent_rotated = log_root / "auth.log.1"

    _touch_old(old_rotated, days=3)
    _touch_old(old_archive, days=3)
    _touch_old(active, days=3)
    recent_rotated.write_text("recent\n", encoding="utf-8")

    result = _run_watchdog(tmp_path, log_root)

    assert result.returncode == 0
    assert not old_rotated.exists()
    assert not old_archive.exists()
    assert active.exists()
    assert recent_rotated.exists()
    assert "action=cleanup" in (tmp_path / "watchdog.state").read_text(encoding="utf-8")


def test_disk_watchdog_dry_run_keeps_rotated_logs(tmp_path: Path) -> None:
    log_root = tmp_path / "logs"
    log_root.mkdir()
    old_rotated = log_root / "syslog.1"
    _touch_old(old_rotated, days=3)

    result = _run_watchdog(tmp_path, log_root, dry_run=True)

    assert result.returncode == 0
    assert old_rotated.exists()
    assert str(old_rotated) in result.stdout
