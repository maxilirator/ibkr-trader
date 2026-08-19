from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

from sqlalchemy import select

from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.orchestration.runtime_planning import (
    promote_due_reentry_waiting_for_flat,
)
from ibkr_trader.orchestration.state_machine import ExecutionState
from tests._runtime_worker_shared import RuntimeWorkerTestCase
from tests._runtime_worker_shared import _aapl_payload


class RuntimeReentryPlanningTests(RuntimeWorkerTestCase):
    def test_deferred_reentry_waits_until_previous_position_is_completed(self) -> None:
        payload = _aapl_payload()
        self._insert_instruction(
            instruction_id="old-position",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            entry_filled_quantity="1",
            payload=payload,
        )
        self._insert_instruction(
            instruction_id="new-reentry",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.REENTRY_WAITING_FOR_FLAT.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
        )

        first_promotion = promote_due_reentry_waiting_for_flat(
            self.session_factory,
            cycle_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            submission_lead_time=timedelta(seconds=60),
        )
        self.assertEqual(first_promotion, [])
        self.assertEqual(
            self._read_record("new-reentry").state,
            ExecutionState.REENTRY_WAITING_FOR_FLAT.value,
        )

        session = self.session_factory()
        try:
            old_record = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "old-position"
                )
            ).scalar_one()
            old_record.state = ExecutionState.COMPLETED.value
            session.commit()
        finally:
            session.close()

        second_promotion = promote_due_reentry_waiting_for_flat(
            self.session_factory,
            cycle_at=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            submission_lead_time=timedelta(seconds=60),
        )

        self.assertEqual(second_promotion, ["new-reentry"])
        self.assertEqual(
            self._read_record("new-reentry").state,
            ExecutionState.ENTRY_PENDING.value,
        )
        session = self.session_factory()
        try:
            event = session.execute(
                select(InstructionEventRecord)
                .join(
                    InstructionRecord,
                    InstructionRecord.id == InstructionEventRecord.instruction_id,
                )
                .where(InstructionRecord.instruction_id == "new-reentry")
            ).scalar_one()
            self.assertEqual(event.event_type, "reentry_waiting_for_flat_promoted")
        finally:
            session.close()

    def test_deferred_reentry_expires_if_previous_position_never_goes_flat(self) -> None:
        payload = _aapl_payload()
        self._insert_instruction(
            instruction_id="old-position",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.POSITION_OPEN.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            entry_filled_quantity="1",
            payload=payload,
        )
        self._insert_instruction(
            instruction_id="new-reentry",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.REENTRY_WAITING_FOR_FLAT.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
        )

        promoted = promote_due_reentry_waiting_for_flat(
            self.session_factory,
            cycle_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            submission_lead_time=timedelta(seconds=60),
        )

        self.assertEqual(promoted, [])
        self.assertEqual(
            self._read_record("new-reentry").state,
            ExecutionState.ENTRY_CANCELLED.value,
        )
