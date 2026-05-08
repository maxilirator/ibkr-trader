from __future__ import annotations

from datetime import datetime
from datetime import timezone
from pathlib import Path
from unittest import TestCase

from sqlalchemy import select

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.domain.execution_payloads import parse_execution_batch_payload
from ibkr_trader.orchestration.intent_replacement import (
    IntentReplacementConflictError,
    cleanup_intent_groups,
    supersede_batch_intent_entries,
)
from ibkr_trader.orchestration.state_machine import ExecutionState
from ibkr_trader.orchestration.submission import submit_execution_batch


class IntentReplacementTests(TestCase):
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

    def _instruction_payload(
        self,
        instruction_id: str,
        *,
        limit_price: str = "200.00",
    ) -> dict[str, object]:
        return {
            "instruction_id": instruction_id,
            "account": {
                "account_key": "GTW05",
                "book_key": "long_risk_book",
                "book_side": "LONG",
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
                "limit_price": limit_price,
                "time_in_force": "DAY",
                "max_submit_count": 1,
                "cancel_unfilled_at_expiry": True,
            },
            "exit": {
                "take_profit_pct": "0.02",
                "catastrophic_stop_loss_pct": "0.15",
            },
            "trace": {
                "reason_code": "intent-replacement-test",
            },
        }

    def _batch_payload(
        self,
        instruction_id: str,
        *,
        limit_price: str = "199.00",
    ) -> dict[str, object]:
        return {
            "schema_version": "2026-04-10",
            "source": {
                "system": "q-training",
                "batch_id": f"{instruction_id}-batch",
                "generated_at": "2026-04-10T18:00:00Z",
            },
            "instructions": [
                self._instruction_payload(
                    instruction_id,
                    limit_price=limit_price,
                )
            ],
        }

    def _two_instruction_batch_payload(self) -> dict[str, object]:
        payload = self._batch_payload("new-entry-1")
        payload["instructions"] = [
            self._instruction_payload("new-entry-1", limit_price="199.00"),
            self._instruction_payload("new-entry-2", limit_price="198.00"),
        ]
        return payload

    def _insert_instruction(
        self,
        *,
        instruction_id: str,
        state: str,
        broker_order_id: int | None = None,
        entry_filled_quantity: str | None = None,
    ) -> None:
        payload = self._batch_payload(instruction_id)
        instruction = payload["instructions"][0]
        session = self.session_factory()
        try:
            session.add(
                InstructionRecord(
                    instruction_id=instruction_id,
                    schema_version=str(payload["schema_version"]),
                    source_system="q-training",
                    batch_id=str(payload["source"]["batch_id"]),  # type: ignore[index]
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
                    entry_filled_quantity=entry_filled_quantity,
                    payload={
                        "schema_version": payload["schema_version"],
                        "source": payload["source"],
                        "instruction": instruction,
                    },
                )
            )
            session.commit()
        finally:
            session.close()

    def _states_by_instruction_id(self) -> dict[str, str]:
        session = self.session_factory()
        try:
            return {
                record.instruction_id: record.state
                for record in session.execute(select(InstructionRecord)).scalars()
            }
        finally:
            session.close()

    def test_cleanup_dry_run_keeps_newest_entry_without_mutating(self) -> None:
        self._insert_instruction(
            instruction_id="old-pending-1",
            state=ExecutionState.ENTRY_PENDING.value,
        )
        self._insert_instruction(
            instruction_id="old-pending-2",
            state=ExecutionState.ENTRY_PENDING.value,
        )

        result = cleanup_intent_groups(
            self.session_factory,
            self.config,
            requested_by="test",
            apply=False,
            account_key="GTW05",
            book_key="long_risk_book",
            symbol="AAPL",
        )

        self.assertEqual(result.status, "PLANNED")
        self.assertEqual(result.action_count, 1)
        self.assertEqual(result.actions[0].instruction_id, "old-pending-1")
        self.assertEqual(
            self._states_by_instruction_id()["old-pending-1"],
            ExecutionState.ENTRY_PENDING.value,
        )

    def test_cleanup_apply_cancels_stale_pending_entry(self) -> None:
        self._insert_instruction(
            instruction_id="old-pending-1",
            state=ExecutionState.ENTRY_PENDING.value,
        )
        self._insert_instruction(
            instruction_id="old-pending-2",
            state=ExecutionState.ENTRY_PENDING.value,
        )

        result = cleanup_intent_groups(
            self.session_factory,
            self.config,
            requested_by="test",
            reason="Keep newest intent.",
            apply=True,
            account_key="GTW05",
            book_key="long_risk_book",
            symbol="AAPL",
        )

        self.assertEqual(result.status, "APPLIED")
        self.assertEqual(result.cancelled_pending_count, 1)
        self.assertEqual(
            self._states_by_instruction_id()["old-pending-1"],
            ExecutionState.ENTRY_CANCELLED.value,
        )
        session = self.session_factory()
        try:
            event = session.execute(
                select(InstructionEventRecord)
                .join(
                    InstructionRecord,
                    InstructionRecord.id == InstructionEventRecord.instruction_id,
                )
                .where(InstructionRecord.instruction_id == "old-pending-1")
            ).scalar_one()
            self.assertEqual(event.event_type, "intent_cleanup_entry_cancelled")
        finally:
            session.close()

    def test_cleanup_cancel_all_entries_cancels_single_pending_entry(self) -> None:
        self._insert_instruction(
            instruction_id="old-pending",
            state=ExecutionState.ENTRY_PENDING.value,
        )

        result = cleanup_intent_groups(
            self.session_factory,
            self.config,
            requested_by="test",
            reason="Operator cleared this intent group.",
            apply=True,
            account_key="GTW05",
            book_key="long_risk_book",
            symbol="AAPL",
            cancel_all_entries=True,
        )

        self.assertEqual(result.status, "APPLIED")
        self.assertEqual(result.cancelled_pending_count, 1)
        self.assertEqual(
            self._states_by_instruction_id()["old-pending"],
            ExecutionState.ENTRY_CANCELLED.value,
        )

    def test_cleanup_cancel_all_entries_preserves_position_owner(self) -> None:
        self._insert_instruction(
            instruction_id="open-position",
            state=ExecutionState.POSITION_OPEN.value,
            entry_filled_quantity="1",
        )
        self._insert_instruction(
            instruction_id="stale-pending",
            state=ExecutionState.ENTRY_PENDING.value,
        )

        result = cleanup_intent_groups(
            self.session_factory,
            self.config,
            requested_by="test",
            reason="Operator cleared entry rows but left the holding owner intact.",
            apply=True,
            account_key="GTW05",
            book_key="long_risk_book",
            symbol="AAPL",
            cancel_all_entries=True,
        )

        states = self._states_by_instruction_id()
        self.assertEqual(result.status, "BLOCKED")
        self.assertEqual(result.blocked_count, 1)
        self.assertEqual(result.cancelled_pending_count, 1)
        self.assertEqual(states["open-position"], ExecutionState.POSITION_OPEN.value)
        self.assertEqual(states["stale-pending"], ExecutionState.ENTRY_CANCELLED.value)

    def test_superseding_batch_cancels_all_older_active_entries(self) -> None:
        self._insert_instruction(
            instruction_id="old-pending",
            state=ExecutionState.ENTRY_PENDING.value,
        )
        self._insert_instruction(
            instruction_id="old-submitted",
            state=ExecutionState.ENTRY_SUBMITTED.value,
            broker_order_id=11,
        )
        batch = parse_execution_batch_payload(self._batch_payload("new-entry"))

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

        result = supersede_batch_intent_entries(
            self.session_factory,
            self.config,
            batch,
            requested_by="test",
            canceler=fake_canceler,
        )
        submit_execution_batch(
            self.session_factory,
            batch,
            runtime_timezone="America/New_York",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
        )

        states = self._states_by_instruction_id()
        self.assertEqual(result.status, "APPLIED")
        self.assertEqual(result.cancelled_pending_count, 1)
        self.assertEqual(result.cancelled_submitted_count, 1)
        self.assertEqual(states["old-pending"], ExecutionState.ENTRY_CANCELLED.value)
        self.assertEqual(states["old-submitted"], ExecutionState.ENTRY_CANCELLED.value)
        self.assertEqual(states["new-entry"], ExecutionState.ENTRY_PENDING.value)

    def test_superseding_batch_blocks_when_position_already_owns_group(self) -> None:
        self._insert_instruction(
            instruction_id="open-position",
            state=ExecutionState.POSITION_OPEN.value,
            entry_filled_quantity="1",
        )
        self._insert_instruction(
            instruction_id="stale-pending",
            state=ExecutionState.ENTRY_PENDING.value,
        )
        batch = parse_execution_batch_payload(self._batch_payload("new-entry"))

        with self.assertRaises(IntentReplacementConflictError) as raised:
            supersede_batch_intent_entries(
                self.session_factory,
                self.config,
                batch,
                requested_by="test",
            )

        result = raised.exception.result
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.status, "BLOCKED")
        self.assertEqual(result.blocked_count, 1)
        self.assertEqual(result.cancelled_pending_count, 1)
        states = self._states_by_instruction_id()
        self.assertEqual(states["open-position"], ExecutionState.POSITION_OPEN.value)
        self.assertEqual(states["stale-pending"], ExecutionState.ENTRY_CANCELLED.value)

    def test_superseding_batch_rejects_duplicate_incoming_intent_group(self) -> None:
        batch = parse_execution_batch_payload(self._two_instruction_batch_payload())

        with self.assertRaises(IntentReplacementConflictError):
            supersede_batch_intent_entries(
                self.session_factory,
                self.config,
                batch,
                requested_by="test",
            )
