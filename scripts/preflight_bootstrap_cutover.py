#!/usr/bin/env python3
"""Go/no-go check before switching the trader to APP_ENV=production.

Production stops reading the checkout ``.env`` entirely and takes configuration
only from the protected bootstrap file. Anything the ``.env`` was supplying that
the bootstrap file does not silently reverts to a code default. That is the
dangerous part of this cutover, and it is invisible until something misbehaves.

The worst version is not an outage. ``DATABASE_URL`` is only checked for being
non-empty, and the global kill switch reads *disabled* when its
``operator_control`` row is absent. So a mistyped database name gives a service
that starts cleanly, creates an empty schema, reports healthy, and has no kill
switch - on a live account. This script exists to catch that before it happens.

Read-only: opens no broker connection, writes nothing, and only ever issues a
single SELECT against the target database.

Usage::

    python scripts/preflight_bootstrap_cutover.py \
        --bootstrap /etc/ibkr-trader/bootstrap.env \
        --dotenv /home/mattias/ibkr-trader/.env

Exit codes: ``0`` go, ``1`` no-go (blocking findings), ``2`` bad invocation.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from ibkr_trader.bootstrap import (  # noqa: E402
    PAPER_TRADING_PORTS,
    REQUIRED_PRODUCTION_ACCOUNT_KEYS,
    REQUIRED_PRODUCTION_KEYS,
    BootstrapConfigurationError,
    _assert_not_world_accessible,
    _parse_env_text,
    is_production_environment,
)

#: Keys that legitimately differ between a development .env and production, so
#: their absence from the bootstrap file is expected rather than a finding.
EXPECTED_TO_DIFFER = frozenset(
    {
        "APP_ENV",
        "SESSION_CALENDAR_PATH",
        "XSTO_INSTRUMENTS_PATH",
        "XSTO_IDENTITY_PATH",
    }
)


class Report:
    """Collects findings and decides go/no-go."""

    def __init__(self) -> None:
        self.blocking: list[str] = []
        self.warnings: list[str] = []
        self.notes: list[str] = []

    def block(self, message: str) -> None:
        self.blocking.append(message)

    def warn(self, message: str) -> None:
        self.warnings.append(message)

    def note(self, message: str) -> None:
        self.notes.append(message)

    def emit(self) -> int:
        for note in self.notes:
            print(f"  ok      {note}")
        for warning in self.warnings:
            print(f"  WARN    {warning}")
        for item in self.blocking:
            print(f"  BLOCK   {item}")
        print()
        if self.blocking:
            print(f"NO-GO: {len(self.blocking)} blocking finding(s).")
            return 1
        if self.warnings:
            print(f"GO, with {len(self.warnings)} warning(s) to confirm deliberately.")
            return 0
        print("GO.")
        return 0


def _read(path: Path, report: Report, label: str) -> dict[str, str] | None:
    try:
        if not path.exists():
            report.block(f"{label} does not exist: {path}")
            return None
    except OSError as exc:
        report.block(f"{label} could not be accessed ({exc}): {path}")
        return None
    try:
        return _parse_env_text(path.read_text(encoding="utf-8"))
    except OSError as exc:
        report.block(f"{label} could not be read ({exc}): {path}")
        return None


def check_permissions(path: Path, report: Report) -> None:
    try:
        _assert_not_world_accessible(path)
        report.note(f"bootstrap file permissions acceptable: {path}")
    except BootstrapConfigurationError as exc:
        report.block(str(exc))


def check_required_keys(values: dict[str, str], report: Report) -> None:
    missing = [k for k in REQUIRED_PRODUCTION_KEYS if not (values.get(k) or "").strip()]
    if missing:
        report.block(f"bootstrap file missing required keys: {', '.join(missing)}")
    else:
        report.note("all required production keys present")

    if not any((values.get(k) or "").strip() for k in REQUIRED_PRODUCTION_ACCOUNT_KEYS):
        report.block(
            "bootstrap file configures no IBKR account; set one of "
            + " or ".join(REQUIRED_PRODUCTION_ACCOUNT_KEYS)
        )


def check_port(values: dict[str, str], report: Report) -> None:
    raw = (values.get("IBKR_PORT") or "").strip()
    if not raw:
        return
    try:
        port = int(raw)
    except ValueError:
        report.block(f"IBKR_PORT is not an integer: {raw!r}")
        return
    if port in PAPER_TRADING_PORTS:
        report.block(
            f"IBKR_PORT={port} is a PAPER trading port "
            "(IB Gateway paper 4002, TWS paper 7497)"
        )
    else:
        report.note(f"IBKR_PORT={port} is not a known paper port")


def check_app_env(dotenv: dict[str, str], report: Report) -> None:
    declared = (dotenv.get("APP_ENV") or "").strip()
    if not declared:
        report.note("checkout .env does not set APP_ENV")
        return
    if is_production_environment(declared):
        report.block(
            f"checkout .env sets APP_ENV={declared!r}. This is refused at startup: "
            "APP_ENV must come from the systemd unit. Remove it from .env."
        )
    else:
        report.warn(
            f"checkout .env sets APP_ENV={declared!r}; it will be ignored in "
            "production but is best removed to avoid confusion"
        )


def check_dropped_keys(
    dotenv: dict[str, str], bootstrap: dict[str, str], report: Report
) -> None:
    """The core of this script: what silently reverts to a code default."""
    dropped = sorted(
        key
        for key in dotenv
        if key not in bootstrap and key not in EXPECTED_TO_DIFFER
    )
    if not dropped:
        report.note("no .env keys would be lost at cutover")
        return
    report.warn(
        f"{len(dropped)} key(s) set in .env are absent from the bootstrap file and "
        "will revert to code defaults at cutover:"
    )
    for key in dropped:
        report.warn(f"    - {key}")

    for key in ("API_HOST", "API_REQUIRE_LOOPBACK_ONLY"):
        if key in dropped:
            report.warn(
                f"    {key} in particular changes how the API is exposed; "
                "confirm the new default is what you want"
            )


def check_kill_switch(bootstrap: dict[str, str], report: Report) -> None:
    """Confirm the target database is the one holding the kill switch.

    An absent ``operator_control`` row reads as "kill switch disabled". If the
    database URL is wrong, the service comes up healthy with no kill switch and
    no ledger history, which is far more dangerous than failing to start.
    """
    database_url = (bootstrap.get("DATABASE_URL") or "").strip()
    if not database_url:
        return

    try:
        from sqlalchemy import select
        from sqlalchemy.orm import sessionmaker

        from ibkr_trader.db.base import build_engine, session_scope
        from ibkr_trader.db.models import OperatorControlRecord
        from ibkr_trader.orchestration.operator_controls import (
            KILL_SWITCH_CONTROL_KEY,
        )

        engine = build_engine(database_url)
        factory = sessionmaker(bind=engine, expire_on_commit=False)
        with session_scope(factory) as session:
            record = session.execute(
                select(OperatorControlRecord).where(
                    OperatorControlRecord.control_key == KILL_SWITCH_CONTROL_KEY
                )
            ).scalar_one_or_none()
            enabled = None if record is None else bool(record.enabled)
            changed_at = None if record is None else record.last_changed_at
        engine.dispose()
    except Exception as exc:  # noqa: BLE001 - reported, never assumed benign
        report.block(
            "could not read the global kill switch from the configured "
            f"DATABASE_URL ({type(exc).__name__}: {exc}). Do not cut over until "
            "this database is confirmed reachable and correct."
        )
        return

    if enabled is None:
        report.block(
            "the configured DATABASE_URL has NO global kill switch row. An absent "
            "row reads as 'kill switch disabled', so this is very likely the "
            "wrong database - cutting over would start a live service with no "
            "kill switch and no ledger history."
        )
    elif not enabled:
        report.block(
            "the global kill switch is DISABLED in the configured database. The "
            "approved goal requires it to remain enabled throughout."
        )
    else:
        report.note(
            f"global kill switch is enabled in the target database "
            f"(last changed {changed_at})"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", type=Path, required=True)
    parser.add_argument("--dotenv", type=Path, required=True)
    parser.add_argument(
        "--skip-kill-switch",
        action="store_true",
        help="skip the database check (it requires connectivity to DATABASE_URL)",
    )
    args = parser.parse_args(argv)

    report = Report()
    print(f"Bootstrap: {args.bootstrap}")
    print(f"Checkout .env: {args.dotenv}")
    print(f"Running as uid {os.geteuid()}\n")

    bootstrap = _read(args.bootstrap, report, "bootstrap file")
    dotenv = _read(args.dotenv, report, "checkout .env")

    if bootstrap is not None:
        check_permissions(args.bootstrap, report)
        check_required_keys(bootstrap, report)
        check_port(bootstrap, report)
        if not args.skip_kill_switch:
            check_kill_switch(bootstrap, report)

    if dotenv is not None:
        check_app_env(dotenv, report)
        if bootstrap is not None:
            check_dropped_keys(dotenv, bootstrap, report)

    return report.emit()


if __name__ == "__main__":
    raise SystemExit(main())
