"""Explicitly disable the kill switch for tests that exercise trading paths.

An absent kill-switch record now reads as *enabled*, so a fresh test database
blocks new entries. That is the intended production behaviour: absence is not
evidence that trading was approved.

Tests that submit entries therefore have to say so. Making the precondition
explicit is an improvement on relying on a permissive default - it is now
visible in each setUp which tests need the switch off, and a test that
accidentally depends on it can no longer pass by accident.

Uses the production write path rather than inserting a row directly, so the
record and its audit event are built exactly as they would be live.
"""

from __future__ import annotations

from sqlalchemy.orm import Session, sessionmaker

from ibkr_trader.orchestration.operator_controls import set_kill_switch_state


def disable_kill_switch_for_test(session_factory: sessionmaker[Session]) -> None:
    """Record an explicit operator decision to leave the switch off."""
    set_kill_switch_state(
        session_factory,
        enabled=False,
        reason="Disabled for a test that exercises entry submission.",
        updated_by="test",
        source="test",
    )
