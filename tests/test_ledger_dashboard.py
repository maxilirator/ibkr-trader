from __future__ import annotations

import unittest
from datetime import datetime
from datetime import timezone

from sqlalchemy.orm import Session

from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.models import BrokerAccountRecord
from ibkr_trader.db.models import BrokerOrderEventRecord
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import ExecutionFillRecord
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import InstructionSetCancellationRecord
from ibkr_trader.db.models import ReconciliationIssueRecord
from ibkr_trader.db.models import ReconciliationRunRecord
from ibkr_trader.orchestration.operator_controls import set_kill_switch_state
from ibkr_trader.read_models.ledger_dashboard import build_ledger_dashboard_snapshot


class LedgerDashboardReadModelTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(self.engine)
        self.session_factory = create_session_factory(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    def _seed_ledger_data(self) -> None:
        session: Session = self.session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="U25245596",
                account_label="Live Sweden",
                base_currency="SEK",
            )
            session.add(broker_account)
            session.flush()

            instruction = InstructionRecord(
                instruction_id="instr-saab-1",
                schema_version="2026-04-10",
                source_system="q-training",
                batch_id="live_ops_20260419",
                account_key="U25245596",
                book_key="long_risk_book",
                symbol="SAAB",
                exchange="SMART",
                currency="SEK",
                state="ENTRY_SUBMITTED",
                submit_at=datetime(2026, 4, 19, 7, 20, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 19, 15, 30, tzinfo=timezone.utc),
                order_type="LIMIT",
                side="BUY",
                broker_order_id=11,
                broker_order_status="Submitted",
                payload={"instruction": "payload"},
            )
            session.add(instruction)
            session.flush()

            broker_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="ENTRY",
                external_order_id="11",
                external_perm_id="9001",
                external_client_id="0",
                order_ref="instr-saab-1",
                symbol="SAAB",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="SAAB-B",
                side="BUY",
                order_type="LMT",
                time_in_force="DAY",
                status="Submitted",
                total_quantity="2",
                limit_price="100.00",
                stop_price=None,
                submitted_at=datetime(2026, 4, 19, 7, 21, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 19, 7, 22, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={"warning_text": "Held in TWS for review."},
            )
            session.add(broker_order)
            session.flush()

            session.add_all(
                [
                    InstructionEventRecord(
                        instruction_id=instruction.id,
                        event_type="instruction_received",
                        source="api",
                        event_at=datetime(2026, 4, 19, 7, 18, tzinfo=timezone.utc),
                        state_before=None,
                        state_after="ENTRY_PENDING",
                        payload={},
                        note="Instruction was accepted.",
                    ),
                    InstructionEventRecord(
                        instruction_id=instruction.id,
                        event_type="entry_submitted",
                        source="runtime",
                        event_at=datetime(2026, 4, 19, 7, 21, tzinfo=timezone.utc),
                        state_before="ENTRY_PENDING",
                        state_after="ENTRY_SUBMITTED",
                        payload={"broker_order_id": 11},
                        note="Runtime submitted the entry order.",
                    ),
                    BrokerOrderEventRecord(
                        broker_order_id=broker_order.id,
                        event_type="order_error_callback",
                        event_at=datetime(2026, 4, 19, 7, 22, tzinfo=timezone.utc),
                        status_before="PreSubmitted",
                        status_after="Submitted",
                        payload={"errorCode": 201, "errorMsg": "Order held for review"},
                        note="Broker callback arrived.",
                    ),
                    ExecutionFillRecord(
                        broker_order_id=broker_order.id,
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        external_execution_id="exec-001",
                        external_order_id="11",
                        external_perm_id="9001",
                        order_ref="instr-saab-1",
                        symbol="SAAB",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        side="BOT",
                        quantity="1",
                        price="100.50",
                        commission="1.00",
                        commission_currency="SEK",
                        executed_at=datetime(2026, 4, 19, 7, 23, tzinfo=timezone.utc),
                        raw_payload={},
                    ),
                ]
            )

            reconciliation_run = ReconciliationRunRecord(
                run_kind="runtime_cycle",
                broker_kind="IBKR",
                account_key="U25245596",
                runtime_timezone="Europe/Stockholm",
                started_at=datetime(2026, 4, 19, 7, 25, tzinfo=timezone.utc),
                completed_at=datetime(2026, 4, 19, 7, 25, 3, tzinfo=timezone.utc),
                status="WARNINGS",
                issue_count=1,
                action_count=1,
                metadata_json={},
            )
            session.add(reconciliation_run)
            session.flush()

            session.add(
                ReconciliationIssueRecord(
                    reconciliation_run_id=reconciliation_run.id,
                    instruction_id="instr-saab-1",
                    stage="reconcile_instruction",
                    severity="ERROR",
                    message="Order state drift detected.",
                    observed_at=datetime(2026, 4, 19, 7, 25, 3, tzinfo=timezone.utc),
                    payload={"broker_order_id": 11},
                )
            )

            session.add(
                InstructionSetCancellationRecord(
                    requested_at=datetime(2026, 4, 19, 7, 26, tzinfo=timezone.utc),
                    requested_by="dashboard",
                    reason="Clean up stale instructions.",
                    selectors={"instruction_ids": ["instr-saab-1"]},
                    status="COMPLETED",
                    matched_instruction_count=1,
                    cancelled_pending_count=0,
                    cancelled_submitted_count=1,
                    skipped_count=0,
                    failed_count=0,
                    result_payload={
                        "results": [
                            {
                                "instruction_id": "instr-saab-1",
                                "action": "cancelled_submitted_entry",
                            }
                        ]
                    },
                )
            )

            session.commit()
        finally:
            session.close()

        set_kill_switch_state(
            self.session_factory,
            enabled=True,
            reason="Operator halt for review.",
            updated_by="dashboard",
        )

    def test_build_ledger_dashboard_snapshot_returns_append_only_views(self) -> None:
        self._seed_ledger_data()

        snapshot = build_ledger_dashboard_snapshot(
            self.session_factory,
            instruction_event_limit=10,
            order_event_limit=10,
            fill_limit=10,
            control_event_limit=10,
            cancellation_limit=10,
            reconciliation_issue_limit=10,
        )

        self.assertIsNone(snapshot.focus_instruction)
        self.assertEqual(snapshot.summary.instruction_count, 1)
        self.assertEqual(snapshot.summary.instruction_event_count, 2)
        self.assertEqual(snapshot.summary.broker_order_count, 1)
        self.assertEqual(snapshot.summary.broker_order_event_count, 1)
        self.assertEqual(snapshot.summary.execution_fill_count, 1)
        self.assertEqual(snapshot.summary.control_event_count, 1)
        self.assertEqual(snapshot.summary.instruction_set_cancellation_count, 1)
        self.assertEqual(snapshot.summary.reconciliation_issue_count, 1)
        self.assertEqual(snapshot.instruction_events[0].event_type, "entry_submitted")
        self.assertEqual(
            snapshot.broker_order_events[0].message,
            "[201] Order held for review",
        )
        self.assertEqual(snapshot.recent_fills[0].external_execution_id, "exec-001")
        self.assertEqual(snapshot.control_events[0].event_type, "kill_switch_enabled")
        self.assertEqual(
            snapshot.instruction_set_cancellations[0].requested_by,
            "dashboard",
        )
        self.assertEqual(snapshot.reconciliation_issues[0].severity, "ERROR")

    def test_build_ledger_dashboard_snapshot_can_focus_one_instruction(self) -> None:
        self._seed_ledger_data()

        snapshot = build_ledger_dashboard_snapshot(
            self.session_factory,
            focus_instruction_id="instr-saab-1",
            instruction_event_limit=10,
            order_event_limit=10,
            fill_limit=10,
            control_event_limit=10,
            cancellation_limit=10,
            reconciliation_issue_limit=10,
        )

        self.assertIsNotNone(snapshot.focus_instruction)
        self.assertEqual(snapshot.focus_instruction.instruction_id, "instr-saab-1")
        self.assertEqual(len(snapshot.instruction_events), 2)
        self.assertEqual(len(snapshot.broker_order_events), 1)
        self.assertEqual(len(snapshot.recent_fills), 1)
        self.assertEqual(len(snapshot.instruction_set_cancellations), 1)
        self.assertEqual(len(snapshot.reconciliation_issues), 1)


if __name__ == "__main__":
    unittest.main()
