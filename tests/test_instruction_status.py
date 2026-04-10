from __future__ import annotations

from datetime import datetime
from datetime import timezone
from unittest import TestCase

from sqlalchemy import select

from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.orchestration.instruction_status import (
    InstructionStatusNotFoundError,
    read_instruction_status,
    serialize_instruction_status,
)


def _persisted_instruction_payload() -> dict[str, object]:
    return {
        "schema_version": "2026-04-10",
        "source": {
            "system": "q-training",
            "batch_id": "batch-1",
            "generated_at": "2026-04-10T02:15:44Z",
        },
        "instruction": {
            "instruction_id": "status-aapl-1",
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
                "reason_code": "instruction-status-test",
            },
        },
    }


class InstructionStatusTests(TestCase):
    def setUp(self) -> None:
        self.engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(self.engine)
        self.session_factory = create_session_factory(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    def _insert_instruction(self) -> None:
        session = self.session_factory()
        try:
            record = InstructionRecord(
                instruction_id="status-aapl-1",
                schema_version="2026-04-10",
                source_system="q-training",
                batch_id="batch-1",
                account_key="GTW05",
                book_key="long_risk_book",
                symbol="AAPL",
                exchange="SMART",
                currency="USD",
                state="EXIT_PENDING",
                submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
                order_type="LIMIT",
                side="BUY",
                broker_order_id=11,
                broker_perm_id=8001,
                broker_client_id=0,
                broker_order_status="Filled",
                entry_submitted_quantity="1",
                entry_filled_quantity="1",
                entry_avg_fill_price="200.00",
                entry_filled_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                exit_order_id=21,
                exit_perm_id=9001,
                exit_client_id=0,
                exit_order_status="Submitted",
                exit_submitted_quantity="1",
                payload=_persisted_instruction_payload(),
            )
            session.add(record)
            session.flush()
            session.add_all(
                [
                    InstructionEventRecord(
                        instruction_id=record.id,
                        event_type="instruction_submitted",
                        source="api",
                        state_before=None,
                        state_after="ENTRY_PENDING",
                        payload={"runtime_schedule": {"runtime_timezone": "Europe/Stockholm"}},
                        note="Instruction validated and persisted for scheduled execution.",
                    ),
                    InstructionEventRecord(
                        instruction_id=record.id,
                        event_type="entry_order_submitted",
                        source="broker_submit",
                        state_before="ENTRY_PENDING",
                        state_after="ENTRY_SUBMITTED",
                        payload={"broker_submission": {"broker_order_status": {"orderId": 11}}},
                        note="Persisted instruction entry order submitted to IBKR.",
                    ),
                ]
            )
            session.commit()
        finally:
            session.close()

    def test_read_instruction_status_returns_record_and_events(self) -> None:
        self._insert_instruction()

        result = read_instruction_status(self.session_factory, "status-aapl-1")

        self.assertEqual(result.instruction_id, "status-aapl-1")
        self.assertEqual(result.state, "EXIT_PENDING")
        self.assertEqual(result.broker_order_id, 11)
        self.assertEqual(result.exit_order_id, 21)
        self.assertEqual(len(result.events), 2)
        self.assertEqual(result.events[0].event_type, "instruction_submitted")
        serialized = serialize_instruction_status(result)
        self.assertEqual(serialized["entry_avg_fill_price"], "200.00")
        self.assertEqual(serialized["events"][1]["event_type"], "entry_order_submitted")

    def test_read_instruction_status_can_omit_events(self) -> None:
        self._insert_instruction()

        result = read_instruction_status(
            self.session_factory,
            "status-aapl-1",
            include_events=False,
        )

        self.assertEqual(result.events, ())

    def test_read_instruction_status_requires_existing_instruction(self) -> None:
        with self.assertRaisesRegex(InstructionStatusNotFoundError, "was not found"):
            read_instruction_status(self.session_factory, "missing-instruction")
