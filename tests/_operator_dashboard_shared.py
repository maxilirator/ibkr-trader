from __future__ import annotations

import unittest
from datetime import datetime
from datetime import timezone
from unittest import TestCase
from unittest.mock import patch

from sqlalchemy.orm import Session

from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.models import AccountSnapshotRecord
from ibkr_trader.db.models import BrokerAccountRecord
from ibkr_trader.db.models import BrokerOrderEventRecord
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import ExecutionFillRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import PositionSnapshotRecord
from ibkr_trader.db.models import ReconciliationIssueRecord
from ibkr_trader.db.models import ReconciliationRunRecord
from ibkr_trader.orchestration.operator_controls import set_kill_switch_state
from ibkr_trader.orchestration.operator_reviews import (
    record_broker_attention_review_action,
    record_reconciliation_issue_review_action,
)
from ibkr_trader.read_models.operator_dashboard import build_operator_dashboard_snapshot

class OperatorDashboardReadModelTestCase(TestCase):
    def setUp(self) -> None:
        self.engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(self.engine)
        self.session_factory = create_session_factory(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    def _seed_operator_data(self) -> None:
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

            session.add_all(
                [
                    AccountSnapshotRecord(
                        broker_account_id=broker_account.id,
                        snapshot_at=datetime(2026, 4, 19, 7, 20, tzinfo=timezone.utc),
                        source="runtime_snapshot",
                        net_liquidation="100000.00",
                        total_cash_value="55000.00",
                        buying_power="200000.00",
                        available_funds="120000.00",
                        excess_liquidity="118000.00",
                        cushion="0.91",
                        currency="SEK",
                    ),
                    AccountSnapshotRecord(
                        broker_account_id=broker_account.id,
                        snapshot_at=datetime(2026, 4, 19, 7, 25, tzinfo=timezone.utc),
                        source="runtime_snapshot",
                        net_liquidation="101500.00",
                        total_cash_value="56000.00",
                        buying_power="201000.00",
                        available_funds="121000.00",
                        excess_liquidity="119000.00",
                        cushion="0.92",
                        currency="SEK",
                    ),
                    PositionSnapshotRecord(
                        broker_account_id=broker_account.id,
                        snapshot_at=datetime(2026, 4, 19, 7, 20, tzinfo=timezone.utc),
                        source="runtime_snapshot",
                        symbol="SAAB",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        primary_exchange="SFB",
                        local_symbol="SAAB-B",
                        quantity="1",
                        average_cost="100.00",
                        market_price="101.00",
                        market_value="101.00",
                        unrealized_pnl="1.00",
                        realized_pnl="0.00",
                    ),
                    PositionSnapshotRecord(
                        broker_account_id=broker_account.id,
                        snapshot_at=datetime(2026, 4, 19, 7, 25, tzinfo=timezone.utc),
                        source="runtime_snapshot",
                        symbol="SAAB",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        primary_exchange="SFB",
                        local_symbol="SAAB-B",
                        quantity="2",
                        average_cost="100.50",
                        market_price="102.00",
                        market_value="204.00",
                        unrealized_pnl="3.00",
                        realized_pnl="0.00",
                    ),
                    PositionSnapshotRecord(
                        broker_account_id=broker_account.id,
                        snapshot_at=datetime(2026, 4, 19, 7, 25, tzinfo=timezone.utc),
                        source="runtime_snapshot",
                        symbol="MSFT",
                        exchange="SMART",
                        currency="USD",
                        security_type="STK",
                        primary_exchange="NASDAQ",
                        local_symbol="MSFT",
                        quantity="0",
                        average_cost="0.00",
                        market_price="390.00",
                        market_value="0.00",
                        unrealized_pnl="0.00",
                        realized_pnl="0.00",
                    ),
                ]
            )

            open_order = BrokerOrderRecord(
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
                submitted_at=datetime(2026, 4, 19, 7, 24, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 19, 7, 24, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={"warning_text": "Held in TWS for review."},
            )
            closed_order = BrokerOrderRecord(
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="EXIT",
                external_order_id="12",
                external_perm_id="9002",
                external_client_id="0",
                order_ref="instr-saab-1:exit:take_profit",
                symbol="SAAB",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="SAAB-B",
                side="SELL",
                order_type="LMT",
                time_in_force="DAY",
                status="Filled",
                total_quantity="1",
                limit_price="102.00",
                stop_price=None,
                submitted_at=datetime(2026, 4, 19, 7, 10, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 19, 7, 12, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            errored_order = BrokerOrderRecord(
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="ENTRY",
                external_order_id="13",
                external_perm_id="9003",
                external_client_id="0",
                order_ref="instr-saab-2",
                symbol="SAAB",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="SAAB-B",
                side="BUY",
                order_type="LMT",
                time_in_force="DAY",
                status="ERROR",
                total_quantity="1",
                limit_price="99.00",
                stop_price=None,
                submitted_at=datetime(2026, 4, 19, 7, 26, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 19, 7, 26, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={"reject_reason": "Already absent at broker."},
            )
            session.add_all([open_order, closed_order, errored_order])
            session.flush()

            session.add(
                BrokerOrderEventRecord(
                    broker_order_id=open_order.id,
                    event_type="order_error_callback",
                    event_at=datetime(2026, 4, 19, 7, 24, 30, tzinfo=timezone.utc),
                    status_before="PreSubmitted",
                    status_after="Submitted",
                    payload={"errorCode": 201, "errorMsg": "Order held for review"},
                    note="Broker raised an order callback.",
                )
            )
            session.add(
                ExecutionFillRecord(
                    broker_order_id=closed_order.id,
                    instruction_id=None,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="U25245596",
                    external_execution_id="exec-001",
                    external_order_id="12",
                    external_perm_id="9002",
                    order_ref="instr-saab-1:exit:take_profit",
                    symbol="SAAB",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    side="SLD",
                    quantity="1",
                    price="102.00",
                    commission="1.00",
                    commission_currency="SEK",
                    executed_at=datetime(2026, 4, 19, 7, 12, tzinfo=timezone.utc),
                    raw_payload={},
                )
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
                action_count=2,
                metadata_json={"snapshot_counts": {"open_order_count": 1}},
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
                    payload={"order_id": 11},
                )
            )
            session.add(
                ReconciliationRunRecord(
                    run_kind="runtime_cycle",
                    broker_kind="IBKR",
                    account_key="U25245596",
                    runtime_timezone="Europe/Stockholm",
                    started_at=datetime(2026, 4, 19, 7, 26, tzinfo=timezone.utc),
                    completed_at=datetime(2026, 4, 19, 7, 26, 3, tzinfo=timezone.utc),
                    status="CLEAN",
                    issue_count=0,
                    action_count=1,
                    metadata_json={"snapshot_counts": {"open_order_count": 1}},
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


__all__ = [name for name in globals() if not name.startswith("__")]
