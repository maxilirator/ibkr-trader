from __future__ import annotations

from datetime import datetime
from datetime import timezone
from unittest import TestCase

from sqlalchemy import select

from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.models import BrokerAccountRecord
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.orchestration.instruction_status import (
    InstructionStatusNotFoundError,
    list_instruction_statuses,
    read_instruction_status,
    serialize_instruction_status,
)


def _persisted_instruction_payload(
    instruction_id: str = "status-aapl-1",
) -> dict[str, object]:
    return {
        "schema_version": "2026-04-10",
        "source": {
            "system": "q-training",
            "batch_id": "batch-1",
            "generated_at": "2026-04-10T02:15:44Z",
        },
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

    def _insert_instruction(
        self,
        *,
        instruction_id: str = "status-aapl-1",
        state: str = "EXIT_PENDING",
        updated_at: datetime | None = None,
    ) -> None:
        session = self.session_factory()
        try:
            record = InstructionRecord(
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
                payload=_persisted_instruction_payload(instruction_id),
                updated_at=updated_at or datetime.now(timezone.utc),
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

    def _insert_broker_orders_for_instruction(
        self,
        *,
        instruction_id: str,
    ) -> None:
        session = self.session_factory()
        try:
            record = session.execute(
                select(InstructionRecord).where(InstructionRecord.instruction_id == instruction_id)
            ).scalar_one()
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key=record.account_key,
                base_currency=record.currency,
            )
            session.add(broker_account)
            session.flush()
            session.add_all(
                [
                    BrokerOrderRecord(
                        instruction_id=record.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key=record.account_key,
                        order_role="ENTRY",
                        external_order_id="44",
                        external_perm_id="844",
                        external_client_id="0",
                        order_ref=record.instruction_id,
                        symbol=record.symbol,
                        exchange=record.exchange,
                        currency=record.currency,
                        security_type="STK",
                        side="BUY",
                        order_type="LMT",
                        status="Filled",
                        submitted_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 10, 20, 1, tzinfo=timezone.utc),
                    ),
                    BrokerOrderRecord(
                        instruction_id=record.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key=record.account_key,
                        order_role="EXIT",
                        external_order_id="55",
                        external_perm_id="955",
                        external_client_id="0",
                        order_ref=f"{record.instruction_id}:exit:take_profit",
                        symbol=record.symbol,
                        exchange=record.exchange,
                        currency=record.currency,
                        security_type="STK",
                        side="SELL",
                        order_type="LMT",
                        status="Submitted",
                        submitted_at=datetime(2026, 4, 10, 20, 2, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 10, 20, 3, tzinfo=timezone.utc),
                    ),
                    BrokerOrderRecord(
                        instruction_id=record.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key=record.account_key,
                        order_role="EXIT",
                        external_order_id="56",
                        external_perm_id="956",
                        external_client_id="0",
                        order_ref=f"{record.instruction_id}:exit:catastrophic_stop",
                        symbol=record.symbol,
                        exchange=record.exchange,
                        currency=record.currency,
                        security_type="STK",
                        side="SELL",
                        order_type="STP",
                        status="PreSubmitted",
                        submitted_at=datetime(2026, 4, 10, 20, 2, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 10, 20, 4, tzinfo=timezone.utc),
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

    def test_list_instruction_statuses_returns_recent_first(self) -> None:
        self._insert_instruction(
            instruction_id="status-aapl-1",
            updated_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
        )
        self._insert_instruction(
            instruction_id="status-aapl-2",
            updated_at=datetime(2026, 4, 10, 21, 0, tzinfo=timezone.utc),
        )

        results = list_instruction_statuses(self.session_factory, limit=10)

        self.assertEqual([item.instruction_id for item in results], ["status-aapl-2", "status-aapl-1"])
        self.assertTrue(all(item.events == () for item in results))

    def test_list_instruction_statuses_can_filter_by_state(self) -> None:
        self._insert_instruction(
            instruction_id="status-aapl-1",
            state="ENTRY_PENDING",
        )
        self._insert_instruction(
            instruction_id="status-aapl-2",
            state="EXIT_PENDING",
        )

        results = list_instruction_statuses(
            self.session_factory,
            limit=10,
            state="ENTRY_PENDING",
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].instruction_id, "status-aapl-1")

    def test_instruction_status_prefers_current_broker_order_rows(self) -> None:
        self._insert_instruction(
            instruction_id="status-aapl-1",
            updated_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
        )
        self._insert_broker_orders_for_instruction(instruction_id="status-aapl-1")

        result = read_instruction_status(self.session_factory, "status-aapl-1", include_events=False)

        self.assertEqual(result.broker_order_id, 44)
        self.assertEqual(result.broker_order_status, "Filled")
        self.assertEqual(result.exit_order_id, 56)
        self.assertEqual(result.exit_order_status, "PreSubmitted")
        self.assertEqual(result.entry_order_display, "44 / Filled")
        self.assertEqual(result.exit_order_display, "55, 56 / Submitted, PreSubmitted")
        self.assertEqual(result.updated_at.isoformat(), "2026-04-10T20:04:00")
