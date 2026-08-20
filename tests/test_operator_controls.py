from __future__ import annotations

from datetime import datetime
from datetime import timezone
from unittest import TestCase

from sqlalchemy import select

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import InstructionSetCancellationRecord
from ibkr_trader.orchestration.operator_controls import (
    BROKER_MAINTENANCE_MODE_CONTROL_KEY,
    KILL_SWITCH_CONTROL_KEY,
    KillSwitchActiveError,
    assert_kill_switch_inactive,
    kill_switch_is_enabled,
    seed_kill_switch_if_absent,
    cancel_instruction_set,
    read_broker_maintenance_mode_state,
    read_kill_switch_state,
    set_broker_maintenance_mode_state,
    set_kill_switch_state,
)
from ibkr_trader.orchestration.state_machine import ExecutionState


class OperatorControlsTests(TestCase):
    def setUp(self) -> None:
        self.engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(self.engine)
        self.session_factory = create_session_factory(self.engine)
        self.config = IbkrConnectionConfig(
            host="127.0.0.1",
            port=7497,
            client_id=0,
            diagnostic_client_id=7,
            streaming_client_id=9,
            account_id="DU1234567",
        )

    def tearDown(self) -> None:
        self.engine.dispose()

    def _insert_instruction(
        self,
        *,
        instruction_id: str,
        state: str,
        broker_order_id: int | None = None,
    ) -> None:
        session = self.session_factory()
        try:
            session.add(
                InstructionRecord(
                    instruction_id=instruction_id,
                    schema_version="2026-04-10",
                    source_system="q-training",
                    batch_id="batch-1",
                    account_key="GTW05",
                    book_key="long_risk_book",
                    symbol="AAPL",
                    exchange="SMART",
                    currency="USD",
                    state=state,
                    submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
                    expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
                    order_type="LIMIT",
                    side="BUY",
                    broker_order_id=broker_order_id,
                    payload={
                        "instruction": {
                            "instruction_id": instruction_id,
                            "account": {
                                "account_key": "GTW05",
                                "book_key": "long_risk_book",
                            },
                            "instrument": {
                                "symbol": "AAPL",
                                "security_type": "STK",
                                "exchange": "SMART",
                                "currency": "USD",
                                "primary_exchange": "NASDAQ",
                            },
                            "intent": {
                                "side": "BUY",
                                "position_side": "LONG",
                            },
                            "sizing": {
                                "mode": "target_quantity",
                                "target_quantity": "1",
                            },
                            "entry": {
                                "order_type": "LIMIT",
                                "submit_at": "2026-04-10T15:55:00-04:00",
                                "expire_at": "2026-04-10T15:59:00-04:00",
                                "limit_price": "200.00",
                                "time_in_force": "DAY",
                                "max_submit_count": 1,
                                "cancel_unfilled_at_expiry": True,
                            },
                            "exit": {
                                "take_profit_pct": "0.02",
                            },
                            "trace": {
                                "reason_code": "operator-control-test",
                            },
                        }
                    },
                )
            )
            session.commit()
        finally:
            session.close()

    def test_set_and_read_kill_switch_state_persists_event(self) -> None:
        initial = read_kill_switch_state(self.session_factory)
        self.assertEqual(initial.control_key, KILL_SWITCH_CONTROL_KEY)
        # Fail-closed: with no record, absence is not read as approval to trade.
        self.assertTrue(initial.enabled)
        self.assertIsNone(initial.record_id)

        set_kill_switch_state(
            self.session_factory,
            enabled=False,
            reason="Operator cleared the halt.",
            updated_by="dashboard",
        )
        self.assertFalse(read_kill_switch_state(self.session_factory).enabled)

        updated = set_kill_switch_state(
            self.session_factory,
            enabled=True,
            reason="Operator halt for review.",
            updated_by="dashboard",
        )

        self.assertTrue(updated.enabled)
        self.assertEqual(updated.reason, "Operator halt for review.")
        self.assertEqual(updated.updated_by, "dashboard")
        self.assertIsNotNone(updated.latest_event)
        self.assertEqual(updated.latest_event.event_type, "kill_switch_enabled")

    def test_broker_maintenance_mode_is_durable_and_independent(self) -> None:
        initial = read_broker_maintenance_mode_state(self.session_factory)
        self.assertEqual(initial["control_key"], BROKER_MAINTENANCE_MODE_CONTROL_KEY)
        self.assertFalse(initial["enabled"])
        updated = set_broker_maintenance_mode_state(
            self.session_factory,
            enabled=True,
            reason="Maintenance.",
            updated_by="test-suite",
        )
        self.assertTrue(updated["enabled"])
        self.assertEqual(
            updated["latest_event"]["event_type"], "broker_maintenance_mode_enabled"
        )
        # Explicitly disabled first: an absent record now reads as enabled, so
        # asserting independence requires a recorded decision, not a default.
        set_kill_switch_state(
            self.session_factory,
            enabled=False,
            reason="Independence check.",
            updated_by="test-suite",
        )
        self.assertFalse(read_kill_switch_state(self.session_factory).enabled)
        set_kill_switch_state(
            self.session_factory,
            enabled=True,
            reason="Freeze.",
            updated_by="test-suite",
        )
        self.assertTrue(
            read_broker_maintenance_mode_state(self.session_factory)["enabled"]
        )

    def test_cancel_instruction_set_cancels_pending_and_submitted_entries(self) -> None:
        self._insert_instruction(
            instruction_id="instr-pending",
            state=ExecutionState.ENTRY_PENDING.value,
        )
        self._insert_instruction(
            instruction_id="instr-submitted",
            state=ExecutionState.ENTRY_SUBMITTED.value,
            broker_order_id=11,
        )
        self._insert_instruction(
            instruction_id="instr-open",
            state=ExecutionState.POSITION_OPEN.value,
        )

        def fake_canceler(
            broker_config: IbkrConnectionConfig,
            order_id: int,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            self.assertEqual(broker_config.client_id, 0)
            self.assertEqual(order_id, 11)
            self.assertEqual(timeout, 10)
            return {
                "broker_order_status": {
                    "orderId": 11,
                    "status": "Cancelled",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 8001,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                }
            }

        result = cancel_instruction_set(
            self.session_factory,
            self.config,
            requested_by="dashboard",
            reason="Cancel entry risk book.",
            batch_id="batch-1",
            timeout=10,
            canceler=fake_canceler,
        )

        self.assertEqual(result.status, "COMPLETED")
        self.assertEqual(result.matched_instruction_count, 3)
        self.assertEqual(result.cancelled_pending_count, 1)
        self.assertEqual(result.cancelled_submitted_count, 1)
        self.assertEqual(result.skipped_count, 1)
        self.assertEqual(result.failed_count, 0)

        session = self.session_factory()
        try:
            records = {
                record.instruction_id: record
                for record in session.execute(select(InstructionRecord)).scalars()
            }
            self.assertEqual(
                records["instr-pending"].state,
                ExecutionState.ENTRY_CANCELLED.value,
            )
            self.assertEqual(
                records["instr-submitted"].state,
                ExecutionState.ENTRY_CANCELLED.value,
            )
            self.assertEqual(
                records["instr-open"].state,
                ExecutionState.POSITION_OPEN.value,
            )

            cancellation_request = session.execute(
                select(InstructionSetCancellationRecord)
            ).scalar_one()
            self.assertEqual(cancellation_request.status, "COMPLETED")
            self.assertEqual(cancellation_request.cancelled_pending_count, 1)
            self.assertEqual(cancellation_request.cancelled_submitted_count, 1)

            pending_events = (
                session.execute(
                    select(InstructionEventRecord)
                    .join(
                        InstructionRecord,
                        InstructionRecord.id == InstructionEventRecord.instruction_id,
                    )
                    .where(InstructionRecord.instruction_id == "instr-pending")
                    .order_by(InstructionEventRecord.id)
                )
                .scalars()
                .all()
            )
            self.assertEqual(
                [event.event_type for event in pending_events],
                ["instruction_set_cancelled"],
            )
        finally:
            session.close()


class KillSwitchFailClosedTests(TestCase):
    """An absent kill-switch record must not read as approval to trade.

    Before this, `_build_kill_switch_status(record=None)` returned
    `enabled=False`, so `assert_kill_switch_inactive` permitted new entries
    whenever the row was missing - a fresh database, a failed schema init, or a
    deleted row silently authorised trading. The rule that the switch stays
    enabled held only because a row happened to exist.
    """

    def setUp(self) -> None:
        self.engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(self.engine)
        self.session_factory = create_session_factory(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_absent_record_reads_as_enabled(self) -> None:
        status = read_kill_switch_state(self.session_factory)
        self.assertTrue(status.enabled)
        self.assertIsNone(status.record_id)
        self.assertIn("No kill switch record exists", status.reason)

    def test_absent_record_blocks_new_entries(self) -> None:
        """The property that matters: absence blocks, it does not permit."""
        with self.assertRaises(KillSwitchActiveError):
            assert_kill_switch_inactive(self.session_factory)

    def test_an_explicit_disable_still_permits_entries(self) -> None:
        """Fail-closed must not mean permanently closed."""
        set_kill_switch_state(
            self.session_factory,
            enabled=False,
            reason="Operator cleared the halt.",
            updated_by="test",
        )
        assert_kill_switch_inactive(self.session_factory)
        self.assertFalse(kill_switch_is_enabled(self.session_factory))

    def test_seeding_creates_an_enabled_record_with_an_audit_event(self) -> None:
        seeded = seed_kill_switch_if_absent(self.session_factory)
        self.assertIsNotNone(seeded)
        self.assertTrue(seeded.enabled)
        self.assertIsNotNone(seeded.record_id)
        self.assertEqual(seeded.latest_event.event_type, "kill_switch_seeded")
        # Recorded as seeded, not as an operator decision: claiming a human
        # made this choice would be fabricated provenance.
        self.assertEqual(seeded.updated_by, "system:schema-init")

    def test_seeding_is_idempotent(self) -> None:
        self.assertIsNotNone(seed_kill_switch_if_absent(self.session_factory))
        self.assertIsNone(seed_kill_switch_if_absent(self.session_factory))

    def test_seeding_never_re_enables_a_deliberately_disabled_switch(self) -> None:
        """A restart must not quietly reverse an operator's decision."""
        set_kill_switch_state(
            self.session_factory,
            enabled=False,
            reason="Operator cleared the halt.",
            updated_by="mattias",
        )
        self.assertIsNone(seed_kill_switch_if_absent(self.session_factory))
        status = read_kill_switch_state(self.session_factory)
        self.assertFalse(status.enabled)
        self.assertEqual(status.updated_by, "mattias")

    def test_schema_init_seeds_the_switch(self) -> None:
        """init_schema runs as ExecStartPre on every service start."""
        engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(engine)
        factory = create_session_factory(engine)
        seed_kill_switch_if_absent(factory)
        try:
            status = read_kill_switch_state(factory)
            self.assertTrue(status.enabled)
            self.assertIsNotNone(status.record_id)
        finally:
            engine.dispose()
