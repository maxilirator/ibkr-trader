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
    KILL_SWITCH_CONTROL_KEY,
    cancel_instruction_set,
    read_kill_switch_state,
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
        self.assertFalse(initial.enabled)

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

            pending_events = session.execute(
                select(InstructionEventRecord)
                .join(
                    InstructionRecord,
                    InstructionRecord.id == InstructionEventRecord.instruction_id,
                )
                .where(InstructionRecord.instruction_id == "instr-pending")
                .order_by(InstructionEventRecord.id)
            ).scalars().all()
            self.assertEqual(
                [event.event_type for event in pending_events],
                ["instruction_set_cancelled"],
            )
        finally:
            session.close()
