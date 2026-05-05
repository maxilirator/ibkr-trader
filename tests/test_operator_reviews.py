from __future__ import annotations

from datetime import datetime
from datetime import timezone
from unittest import TestCase

from sqlalchemy.orm import Session

from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.models import BrokerAccountRecord
from ibkr_trader.db.models import BrokerOrderEventRecord
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import ReconciliationIssueRecord
from ibkr_trader.db.models import ReconciliationRunRecord
from ibkr_trader.orchestration.operator_reviews import (
    ACKNOWLEDGED_REVIEW_STATUS,
    ARCHIVED_REVIEW_STATUS,
    OPEN_REVIEW_STATUS,
    RESOLVED_REVIEW_STATUS,
    OperatorReviewTargetNotFoundError,
    archive_open_reconciliation_issues,
    extract_broker_attention_message,
    record_broker_attention_review_action,
    record_reconciliation_issue_review_action,
)


class OperatorReviewTests(TestCase):
    def setUp(self) -> None:
        self.engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(self.engine)
        self.session_factory = create_session_factory(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    def _seed_attention_and_issue(self) -> tuple[int, int]:
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

            broker_order = BrokerOrderRecord(
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="ENTRY",
                external_order_id="101",
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
                total_quantity="1",
                limit_price="100.00",
                submitted_at=datetime(2026, 4, 19, 7, 24, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 19, 7, 24, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add(broker_order)
            session.flush()

            broker_event = BrokerOrderEventRecord(
                broker_order_id=broker_order.id,
                event_type="order_error_callback",
                event_at=datetime(2026, 4, 19, 7, 24, 30, tzinfo=timezone.utc),
                status_before="PreSubmitted",
                status_after="Submitted",
                payload={"errorCode": 201, "errorMsg": "Order held for review"},
                note="Broker raised an order callback.",
            )
            session.add(broker_event)

            reconciliation_run = ReconciliationRunRecord(
                run_kind="runtime_cycle",
                broker_kind="IBKR",
                account_key="U25245596",
                runtime_timezone="Europe/Stockholm",
                started_at=datetime(2026, 4, 19, 7, 25, tzinfo=timezone.utc),
                completed_at=datetime(2026, 4, 19, 7, 25, 3, tzinfo=timezone.utc),
                status="WARNINGS",
                issue_count=1,
                action_count=2,
                metadata_json={},
            )
            session.add(reconciliation_run)
            session.flush()

            reconciliation_issue = ReconciliationIssueRecord(
                reconciliation_run_id=reconciliation_run.id,
                instruction_id="instr-saab-1",
                stage="reconcile_instruction",
                severity="ERROR",
                message="Order state drift detected.",
                observed_at=datetime(2026, 4, 19, 7, 25, 3, tzinfo=timezone.utc),
                payload={"order_id": 101},
            )
            session.add(reconciliation_issue)
            session.commit()

            return broker_event.id, reconciliation_issue.id
        finally:
            session.close()

    def test_broker_attention_review_actions_progress_status(self) -> None:
        event_id, _ = self._seed_attention_and_issue()

        acknowledged = record_broker_attention_review_action(
            self.session_factory,
            event_id=event_id,
            action_type="ACKNOWLEDGE",
            updated_by="dashboard",
        )
        resolved = record_broker_attention_review_action(
            self.session_factory,
            event_id=event_id,
            action_type="RESOLVE",
            updated_by="dashboard",
        )
        reopened = record_broker_attention_review_action(
            self.session_factory,
            event_id=event_id,
            action_type="REOPEN",
            updated_by="dashboard",
        )

        self.assertEqual(acknowledged.status, ACKNOWLEDGED_REVIEW_STATUS)
        self.assertEqual(resolved.status, RESOLVED_REVIEW_STATUS)
        self.assertEqual(reopened.status, OPEN_REVIEW_STATUS)

    def test_broker_attention_archive_marks_event_and_reopen_restores_it(self) -> None:
        event_id, _ = self._seed_attention_and_issue()

        archived = record_broker_attention_review_action(
            self.session_factory,
            event_id=event_id,
            action_type="ARCHIVE",
            updated_by="dashboard",
            note="Noise from an old run.",
        )

        self.assertEqual(archived.status, ARCHIVED_REVIEW_STATUS)
        session: Session = self.session_factory()
        try:
            event = session.get(BrokerOrderEventRecord, event_id)
            assert event is not None
            self.assertIsNotNone(event.archived_at)
            self.assertEqual(event.archived_by, "dashboard")
            self.assertEqual(event.archive_reason, "Noise from an old run.")
        finally:
            session.close()

        reopened = record_broker_attention_review_action(
            self.session_factory,
            event_id=event_id,
            action_type="REOPEN",
            updated_by="dashboard",
        )

        self.assertEqual(reopened.status, OPEN_REVIEW_STATUS)
        session = self.session_factory()
        try:
            event = session.get(BrokerOrderEventRecord, event_id)
            assert event is not None
            self.assertIsNone(event.archived_at)
            self.assertIsNone(event.archived_by)
            self.assertIsNone(event.archive_reason)
        finally:
            session.close()

    def test_reconciliation_issue_review_actions_progress_status(self) -> None:
        _, issue_id = self._seed_attention_and_issue()

        acknowledged = record_reconciliation_issue_review_action(
            self.session_factory,
            issue_id=issue_id,
            action_type="ACKNOWLEDGE",
            updated_by="dashboard",
        )
        resolved = record_reconciliation_issue_review_action(
            self.session_factory,
            issue_id=issue_id,
            action_type="RESOLVE",
            updated_by="dashboard",
        )

        self.assertEqual(acknowledged.status, ACKNOWLEDGED_REVIEW_STATUS)
        self.assertEqual(resolved.status, RESOLVED_REVIEW_STATUS)

    def test_reconciliation_issue_archive_marks_issue(self) -> None:
        _, issue_id = self._seed_attention_and_issue()

        archived = record_reconciliation_issue_review_action(
            self.session_factory,
            issue_id=issue_id,
            action_type="ARCHIVE",
            updated_by="dashboard",
        )

        self.assertEqual(archived.status, ARCHIVED_REVIEW_STATUS)
        session: Session = self.session_factory()
        try:
            issue = session.get(ReconciliationIssueRecord, issue_id)
            assert issue is not None
            self.assertIsNotNone(issue.archived_at)
            self.assertEqual(issue.archived_by, "dashboard")
        finally:
            session.close()

    def test_archive_open_reconciliation_issues_marks_all_unarchived_issues(self) -> None:
        _, issue_id = self._seed_attention_and_issue()

        result = archive_open_reconciliation_issues(
            self.session_factory,
            updated_by="dashboard",
            note="Clear dashboard noise.",
        )

        self.assertEqual(result.archived_issue_count, 1)
        self.assertEqual(result.issue_ids, (issue_id,))
        session: Session = self.session_factory()
        try:
            issue = session.get(ReconciliationIssueRecord, issue_id)
            assert issue is not None
            self.assertIsNotNone(issue.archived_at)
            self.assertEqual(issue.archived_by, "dashboard")
            self.assertEqual(issue.archive_reason, "Clear dashboard noise.")
        finally:
            session.close()

    def test_review_actions_reject_missing_targets(self) -> None:
        with self.assertRaises(OperatorReviewTargetNotFoundError):
            record_broker_attention_review_action(
                self.session_factory,
                event_id=999,
                action_type="ACKNOWLEDGE",
                updated_by="dashboard",
            )

        with self.assertRaises(OperatorReviewTargetNotFoundError):
            record_reconciliation_issue_review_action(
                self.session_factory,
                issue_id=999,
                action_type="ACKNOWLEDGE",
                updated_by="dashboard",
            )

    def test_extract_broker_attention_message_uses_error_string_and_normalizes_html(self) -> None:
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

            broker_order = BrokerOrderRecord(
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="ENTRY",
                external_order_id="101",
                external_perm_id="9001",
                external_client_id="0",
                order_ref="instr-volcar-1",
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="VOLCAR B",
                side="BUY",
                order_type="LMT",
                time_in_force="DAY",
                status="Inactive",
                total_quantity="830",
                limit_price="23.26",
                submitted_at=datetime(2026, 4, 21, 7, 25, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 21, 7, 25, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add(broker_order)
            session.flush()

            broker_event = BrokerOrderEventRecord(
                broker_order_id=broker_order.id,
                event_type="order_error_callback",
                event_at=datetime(2026, 4, 21, 7, 25, 1, tzinfo=timezone.utc),
                status_before="Submitted",
                status_after="Inactive",
                payload={
                    "errorCode": 201,
                    "errorString": (
                        "Order rejected - reason:We are unable to accept your order.<br>"
                        " Available Funds are insufficient."
                    ),
                },
                note="Persisted broker order error callback directly from the live session.",
            )
            session.add(broker_event)
            session.commit()

            self.assertEqual(
                extract_broker_attention_message(broker_event, broker_order),
                "[201] Order rejected - reason:We are unable to accept your order. Available Funds are insufficient.",
            )
        finally:
            session.close()
