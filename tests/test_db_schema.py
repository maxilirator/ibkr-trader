from __future__ import annotations

import unittest
from datetime import datetime
from datetime import timezone

from sqlalchemy import inspect

from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.base import normalize_database_url
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import InstrumentRecord


class DatabaseSchemaTests(unittest.TestCase):
    def test_postgres_url_is_normalized_to_psycopg3(self) -> None:
        self.assertEqual(
            normalize_database_url("postgresql://user:pass@db.example.com:5432/app"),
            "postgresql+psycopg://user:pass@db.example.com:5432/app",
        )

    def setUp(self) -> None:
        self.engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(self.engine)
        self.session_factory = create_session_factory(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_create_schema_builds_expected_tables(self) -> None:
        inspector = inspect(self.engine)
        self.assertEqual(
            set(inspector.get_table_names()),
            {"instruction", "instruction_event", "instrument"},
        )

    def test_instruction_event_relationship_round_trips(self) -> None:
        session = self.session_factory()
        try:
            instrument = InstrumentRecord(
                symbol="AAPL",
                exchange="SMART",
                currency="USD",
                security_type="STK",
                primary_exchange="NASDAQ",
                ibkr_con_id=265598,
            )
            session.add(instrument)

            instruction = InstructionRecord(
                instruction_id="instr-001",
                schema_version="v1",
                source_system="inference",
                batch_id="batch-001",
                account_key="DUP123456",
                book_key="long_risk_book",
                symbol="AAPL",
                exchange="SMART",
                currency="USD",
                state="RECEIVED",
                submit_at=datetime(2026, 4, 10, 13, 25, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                order_type="LIMIT",
                side="BUY",
                payload={"instruction_id": "instr-001"},
            )
            instruction.events.append(
                InstructionEventRecord(
                    event_type="instruction_received",
                    source="api",
                    state_after="RECEIVED",
                    payload={"ok": True},
                )
            )
            session.add(instruction)
            session.commit()
            session.refresh(instruction)

            self.assertEqual(instruction.id, 1)
            self.assertEqual(len(instruction.events), 1)
            self.assertEqual(instruction.events[0].event_type, "instruction_received")
        finally:
            session.close()


if __name__ == "__main__":
    unittest.main()
