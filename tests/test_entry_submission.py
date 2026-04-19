from __future__ import annotations

from datetime import datetime
from datetime import timezone
from unittest import TestCase

from sqlalchemy import select

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.models import BrokerAccountRecord
from ibkr_trader.db.models import BrokerOrderEventRecord
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.orchestration.entry_submission import (
    PersistedInstructionNotFoundError,
    PersistedInstructionStateError,
    cancel_persisted_instruction_entry,
    submit_persisted_instruction_entry,
)
from ibkr_trader.orchestration.operator_controls import KillSwitchActiveError
from ibkr_trader.orchestration.operator_controls import set_kill_switch_state
from ibkr_trader.orchestration.state_machine import ExecutionState


def _persisted_instruction_payload() -> dict[str, object]:
    return {
        "schema_version": "2026-04-10",
        "source": {
            "system": "q-training",
            "batch_id": "batch-1",
            "generated_at": "2026-04-10T02:15:44Z",
        },
        "instruction": {
            "instruction_id": "persisted-aapl-1",
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
                "reason_code": "persisted-entry-submit-test",
            },
        },
    }


class PersistedEntrySubmissionTests(TestCase):
    def setUp(self) -> None:
        self.engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(self.engine)
        self.session_factory = create_session_factory(self.engine)
        self.config = IbkrConnectionConfig(
            host="127.0.0.1",
            port=7497,
            client_id=0,
            diagnostic_client_id=7,
            account_id="DU1234567",
        )

    def tearDown(self) -> None:
        self.engine.dispose()

    def _insert_pending_instruction(self, *, state: str = ExecutionState.ENTRY_PENDING.value) -> None:
        session = self.session_factory()
        try:
            session.add(
                InstructionRecord(
                    instruction_id="persisted-aapl-1",
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
                    payload=_persisted_instruction_payload(),
                )
            )
            session.commit()
        finally:
            session.close()

    def test_submit_persisted_instruction_entry_updates_record_and_event(self) -> None:
        self._insert_pending_instruction()

        def fake_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            self.assertEqual(broker_config.client_id, 0)
            self.assertEqual(instruction.instruction_id, "persisted-aapl-1")
            self.assertEqual(timeout, 10)
            return {
                "instruction_id": "persisted-aapl-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 265598, "symbol": "AAPL"},
                "order": {
                    "order_ref": "persisted-aapl-1",
                    "action": "BUY",
                    "order_type": "LMT",
                    "time_in_force": "DAY",
                    "limit_price": "200.00",
                    "total_quantity": "1",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 11,
                    "status": "PreSubmitted",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 8001,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                },
                "tws_submission": {
                    "source": "openOrder",
                    "order_id": 11,
                    "order_ref": "persisted-aapl-1",
                    "order_state": {
                        "status": "Inactive",
                        "warning_text": "Order held in TWS pending manual transmit.",
                    },
                },
            }

        result = submit_persisted_instruction_entry(
            self.session_factory,
            self.config,
            "persisted-aapl-1",
            submitter=fake_submitter,
        )

        self.assertEqual(result.state, ExecutionState.ENTRY_SUBMITTED.value)
        self.assertEqual(result.broker_order_id, 11)
        self.assertEqual(result.broker_perm_id, 8001)
        self.assertEqual(result.broker_client_id, 0)
        self.assertEqual(result.broker_order_status, "PreSubmitted")
        self.assertEqual(result.submission_event.event_type, "entry_order_submitted")
        self.assertEqual(
            result.broker_submission["tws_submission"]["order_state"]["warning_text"],
            "Order held in TWS pending manual transmit.",
        )

        session = self.session_factory()
        try:
            record = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "persisted-aapl-1"
                )
            ).scalar_one()
            self.assertEqual(record.state, ExecutionState.ENTRY_SUBMITTED.value)
            self.assertEqual(record.broker_order_id, 11)
            self.assertEqual(record.broker_perm_id, 8001)
            self.assertEqual(record.broker_client_id, 0)
            self.assertEqual(record.broker_order_status, "PreSubmitted")

            broker_accounts = session.execute(select(BrokerAccountRecord)).scalars().all()
            broker_orders = session.execute(select(BrokerOrderRecord)).scalars().all()
            broker_order_events = session.execute(
                select(BrokerOrderEventRecord).order_by(BrokerOrderEventRecord.id)
            ).scalars().all()
            self.assertEqual(len(broker_accounts), 1)
            self.assertEqual(broker_accounts[0].account_key, "DU1234567")
            self.assertEqual(len(broker_orders), 1)
            self.assertEqual(broker_orders[0].account_key, "DU1234567")
            self.assertEqual(broker_orders[0].external_order_id, "11")
            self.assertEqual(broker_orders[0].status, "PreSubmitted")
            self.assertEqual(broker_orders[0].order_role, "ENTRY")
            self.assertEqual(
                [item.event_type for item in broker_order_events],
                ["open_order_observed", "entry_order_submitted"],
            )

            events = session.execute(
                select(InstructionEventRecord).where(
                    InstructionEventRecord.instruction_id == record.id
                )
            ).scalars().all()
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0].event_type, "entry_order_submitted")
            self.assertEqual(events[0].state_before, ExecutionState.ENTRY_PENDING.value)
            self.assertEqual(events[0].state_after, ExecutionState.ENTRY_SUBMITTED.value)
            self.assertEqual(
                events[0].payload["broker_submission"]["tws_submission"]["order_state"]["warning_text"],
                "Order held in TWS pending manual transmit.",
            )
        finally:
            session.close()

    def test_submit_persisted_instruction_entry_requires_existing_instruction(self) -> None:
        with self.assertRaisesRegex(PersistedInstructionNotFoundError, "was not found"):
            submit_persisted_instruction_entry(
                self.session_factory,
                self.config,
                "missing-instruction",
                submitter=lambda *args, **kwargs: {},
            )

    def test_submit_persisted_instruction_entry_requires_entry_pending_state(self) -> None:
        self._insert_pending_instruction(state=ExecutionState.COMPLETED.value)

        with self.assertRaisesRegex(PersistedInstructionStateError, "ENTRY_PENDING"):
            submit_persisted_instruction_entry(
                self.session_factory,
                self.config,
                "persisted-aapl-1",
                submitter=lambda *args, **kwargs: {},
            )

    def test_submit_persisted_instruction_entry_rejects_when_kill_switch_is_enabled(self) -> None:
        self._insert_pending_instruction()
        set_kill_switch_state(
            self.session_factory,
            enabled=True,
            reason="Freeze new entries.",
            updated_by="test",
        )

        with self.assertRaisesRegex(KillSwitchActiveError, "kill switch"):
            submit_persisted_instruction_entry(
                self.session_factory,
                self.config,
                "persisted-aapl-1",
                submitter=lambda *args, **kwargs: {},
            )

    def test_cancel_persisted_instruction_entry_updates_record_and_event(self) -> None:
        self._insert_pending_instruction(state=ExecutionState.ENTRY_SUBMITTED.value)
        session = self.session_factory()
        try:
            record = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "persisted-aapl-1"
                )
            ).scalar_one()
            record.broker_order_id = 11
            record.broker_perm_id = 8001
            record.broker_client_id = 0
            record.broker_order_status = "PreSubmitted"
            session.commit()
        finally:
            session.close()

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

        result = cancel_persisted_instruction_entry(
            self.session_factory,
            self.config,
            "persisted-aapl-1",
            canceler=fake_canceler,
        )

        self.assertEqual(result.state, ExecutionState.ENTRY_CANCELLED.value)
        self.assertEqual(result.broker_order_status, "Cancelled")
        self.assertEqual(result.cancellation_event.event_type, "entry_order_cancelled")

        session = self.session_factory()
        try:
            record = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "persisted-aapl-1"
                )
            ).scalar_one()
            self.assertEqual(record.state, ExecutionState.ENTRY_CANCELLED.value)
            self.assertEqual(record.broker_order_status, "Cancelled")

            broker_order = session.execute(select(BrokerOrderRecord)).scalar_one()
            broker_order_events = session.execute(
                select(BrokerOrderEventRecord).order_by(BrokerOrderEventRecord.id)
            ).scalars().all()
            self.assertEqual(broker_order.status, "Cancelled")
            self.assertEqual(
                [item.event_type for item in broker_order_events],
                ["entry_order_cancelled"],
            )

            events = session.execute(
                select(InstructionEventRecord).where(
                    InstructionEventRecord.instruction_id == record.id
                ).order_by(InstructionEventRecord.id)
            ).scalars().all()
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0].event_type, "entry_order_cancelled")
            self.assertEqual(events[0].state_before, ExecutionState.ENTRY_SUBMITTED.value)
            self.assertEqual(events[0].state_after, ExecutionState.ENTRY_CANCELLED.value)
        finally:
            session.close()
