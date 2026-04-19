from __future__ import annotations

import unittest
from datetime import datetime
from datetime import timezone

from sqlalchemy import inspect
from sqlalchemy import select

from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.base import normalize_database_url
from ibkr_trader.db.models import AccountSnapshotRecord
from ibkr_trader.db.models import BrokerAccountRecord
from ibkr_trader.db.models import BrokerOrderEventRecord
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import ExecutionFillRecord
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import InstructionSetCancellationRecord
from ibkr_trader.db.models import InstrumentRecord
from ibkr_trader.db.models import OperatorControlEventRecord
from ibkr_trader.db.models import OperatorControlRecord
from ibkr_trader.db.models import PositionSnapshotRecord
from ibkr_trader.db.models import ReconciliationIssueRecord
from ibkr_trader.db.models import ReconciliationRunRecord
from ibkr_trader.db.models import RuntimeServiceEventRecord
from ibkr_trader.db.models import RuntimeServiceRecord


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
            {
                "account_snapshot",
                "broker_account",
                "broker_order",
                "broker_order_event",
                "execution_fill",
                "instruction",
                "instruction_event",
                "instruction_set_cancellation",
                "instrument",
                "operator_control",
                "operator_control_event",
                "position_snapshot",
                "reconciliation_issue",
                "reconciliation_run",
                "runtime_service",
                "runtime_service_event",
            },
        )
        instruction_columns = {
            column["name"] for column in inspector.get_columns("instruction")
        }
        self.assertTrue(
            {
                "broker_order_id",
                "broker_perm_id",
                "broker_client_id",
                "broker_order_status",
            }.issubset(instruction_columns)
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

    def test_broker_ledger_tables_round_trip(self) -> None:
        session = self.session_factory()
        try:
            instruction = InstructionRecord(
                instruction_id="instr-002",
                schema_version="v1",
                source_system="agent-alpha",
                batch_id="batch-002",
                account_key="U25245596",
                book_key="long_risk_book",
                symbol="MSFT",
                exchange="SMART",
                currency="USD",
                state="ENTRY_SUBMITTED",
                submit_at=datetime(2026, 4, 10, 13, 25, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                order_type="LIMIT",
                side="BUY",
                payload={"instruction_id": "instr-002"},
            )
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="U25245596",
                account_label="Primary live account",
                base_currency="SEK",
            )
            broker_order = BrokerOrderRecord(
                instruction=instruction,
                broker_account=broker_account,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="ENTRY",
                external_order_id="101",
                external_perm_id="9001",
                external_client_id="0",
                order_ref="msft-entry-001",
                symbol="MSFT",
                exchange="SMART",
                currency="USD",
                security_type="STK",
                primary_exchange="NASDAQ",
                local_symbol="MSFT",
                side="BUY",
                order_type="LMT",
                time_in_force="DAY",
                status="Submitted",
                total_quantity="1",
                limit_price="402.50",
                submitted_at=datetime(2026, 4, 10, 13, 26, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 10, 13, 26, tzinfo=timezone.utc),
            )
            broker_order.events.append(
                BrokerOrderEventRecord(
                    event_type="submitted",
                    status_before="PENDING_SUBMIT",
                    status_after="Submitted",
                    payload={"external_order_id": "101"},
                )
            )
            broker_order.fills.append(
                ExecutionFillRecord(
                    instruction=instruction,
                    broker_account=broker_account,
                    broker_kind="IBKR",
                    account_key="U25245596",
                    external_execution_id="exec-001",
                    external_order_id="101",
                    external_perm_id="9001",
                    order_ref="msft-entry-001",
                    symbol="MSFT",
                    exchange="NASDAQ",
                    currency="USD",
                    security_type="STK",
                    side="BOT",
                    quantity="1",
                    price="401.75",
                    executed_at=datetime(2026, 4, 10, 13, 27, tzinfo=timezone.utc),
                )
            )
            account_snapshot = AccountSnapshotRecord(
                broker_account=broker_account,
                snapshot_at=datetime(2026, 4, 10, 13, 28, tzinfo=timezone.utc),
                source="runtime_snapshot",
                net_liquidation="100000.00",
                total_cash_value="50000.00",
                buying_power="200000.00",
                available_funds="120000.00",
                excess_liquidity="119000.00",
                cushion="0.91",
                currency="USD",
            )
            position_snapshot = PositionSnapshotRecord(
                broker_account=broker_account,
                snapshot_at=datetime(2026, 4, 10, 13, 28, tzinfo=timezone.utc),
                source="runtime_snapshot",
                symbol="MSFT",
                exchange="SMART",
                currency="USD",
                security_type="STK",
                primary_exchange="NASDAQ",
                local_symbol="MSFT",
                quantity="1",
                average_cost="401.75",
                market_price="402.10",
                market_value="402.10",
                unrealized_pnl="0.35",
                realized_pnl="0.00",
            )

            session.add_all(
                [
                    broker_account,
                    instruction,
                    broker_order,
                    account_snapshot,
                    position_snapshot,
                ]
            )
            session.commit()
            session.refresh(broker_account)
            session.refresh(broker_order)

            self.assertEqual(broker_account.id, 1)
            self.assertEqual(len(broker_account.orders), 1)
            self.assertEqual(len(broker_account.account_snapshots), 1)
            self.assertEqual(len(broker_account.position_snapshots), 1)
            self.assertEqual(broker_order.events[0].event_type, "submitted")
            self.assertEqual(broker_order.fills[0].external_execution_id, "exec-001")
        finally:
            session.close()

    def test_reconciliation_tables_round_trip(self) -> None:
        session = self.session_factory()
        try:
            reconciliation_run = ReconciliationRunRecord(
                run_kind="runtime_cycle",
                broker_kind="IBKR",
                account_key="U25245596",
                runtime_timezone="Europe/Stockholm",
                started_at=datetime(2026, 4, 19, 7, 25, tzinfo=timezone.utc),
                completed_at=datetime(2026, 4, 19, 7, 25, 5, tzinfo=timezone.utc),
                status="WARNINGS",
                issue_count=1,
                action_count=3,
                metadata_json={"active_instruction_count": 2},
            )
            reconciliation_run.issues.append(
                ReconciliationIssueRecord(
                    instruction_id="instr-002",
                    stage="reconcile_instruction",
                    severity="ERROR",
                    message="Synthetic test mismatch.",
                    payload={"order_id": 101},
                )
            )

            session.add(reconciliation_run)
            session.commit()
            session.refresh(reconciliation_run)

            self.assertEqual(reconciliation_run.id, 1)
            self.assertEqual(reconciliation_run.issues[0].stage, "reconcile_instruction")
            self.assertEqual(reconciliation_run.issues[0].instruction_id, "instr-002")
        finally:
            session.close()

    def test_operator_control_tables_round_trip(self) -> None:
        session = self.session_factory()
        try:
            control = OperatorControlRecord(
                control_key="GLOBAL_KILL_SWITCH",
                enabled=True,
                reason="Operator halt for review.",
                updated_by="dashboard",
                metadata_json={"scope": "global"},
            )
            session.add(control)
            session.flush()
            session.add(
                OperatorControlEventRecord(
                    operator_control_id=control.id,
                    event_type="kill_switch_enabled",
                    source="api",
                    enabled=True,
                    reason="Operator halt for review.",
                    updated_by="dashboard",
                    payload={"previous_enabled": False},
                    note="Kill switch enabled from test.",
                )
            )
            session.add(
                InstructionSetCancellationRecord(
                    requested_by="dashboard",
                    reason="Cancel long-risk-book entries.",
                    selectors={"book_key": "long_risk_book"},
                    status="COMPLETED",
                    matched_instruction_count=2,
                    cancelled_pending_count=1,
                    cancelled_submitted_count=1,
                    skipped_count=0,
                    failed_count=0,
                    result_payload={"results": []},
                )
            )
            session.commit()
            session.refresh(control)

            self.assertEqual(control.control_key, "GLOBAL_KILL_SWITCH")
            self.assertEqual(len(control.events), 1)
            self.assertEqual(control.events[0].event_type, "kill_switch_enabled")
            cancellation = session.execute(
                select(InstructionSetCancellationRecord)
            ).scalar_one()
            self.assertEqual(cancellation.status, "COMPLETED")
            self.assertEqual(cancellation.cancelled_submitted_count, 1)
        finally:
            session.close()

    def test_runtime_service_tables_round_trip(self) -> None:
        session = self.session_factory()
        try:
            runtime_service = RuntimeServiceRecord(
                runtime_key="EXECUTION_RUNTIME",
                service_type="execution",
                status="RUNNING",
                owner_token="token-123",
                owner_label="quant:1234",
                hostname="quant",
                pid=1234,
                runtime_timezone="Europe/Stockholm",
                broker_kind="IBKR",
                broker_client_id=0,
                stop_requested=False,
                metadata_json={"interval_seconds": 5},
            )
            session.add(runtime_service)
            session.flush()
            session.add(
                RuntimeServiceEventRecord(
                    runtime_service_id=runtime_service.id,
                    event_type="runtime_started",
                    source="runtime_service",
                    status_before="STOPPED",
                    status_after="RUNNING",
                    payload={"interval_seconds": 5},
                    note="Started from test.",
                )
            )
            session.commit()
            session.refresh(runtime_service)

            self.assertEqual(runtime_service.runtime_key, "EXECUTION_RUNTIME")
            self.assertEqual(len(runtime_service.events), 1)
            self.assertEqual(runtime_service.events[0].event_type, "runtime_started")
        finally:
            session.close()


if __name__ == "__main__":
    unittest.main()
