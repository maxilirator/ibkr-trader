from __future__ import annotations

import unittest
from datetime import datetime
from datetime import timezone
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


class OperatorDashboardReadModelTests(unittest.TestCase):
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

    def test_build_operator_dashboard_snapshot_returns_latest_durable_views(self) -> None:
        self._seed_operator_data()
        record_broker_attention_review_action(
            self.session_factory,
            event_id=1,
            action_type="ACKNOWLEDGE",
            updated_by="dashboard",
        )
        record_reconciliation_issue_review_action(
            self.session_factory,
            issue_id=1,
            action_type="RESOLVE",
            updated_by="dashboard",
        )

        snapshot = build_operator_dashboard_snapshot(
            self.session_factory,
            order_limit=10,
            fill_limit=10,
            attention_limit=10,
            reconciliation_run_limit=10,
        )

        self.assertTrue(snapshot.kill_switch.enabled)
        self.assertEqual(snapshot.kill_switch.reason, "Operator halt for review.")
        self.assertEqual(len(snapshot.accounts), 1)
        self.assertEqual(snapshot.accounts[0].net_liquidation, "101500.00")

        self.assertEqual(len(snapshot.positions), 1)
        self.assertEqual(snapshot.positions[0].symbol, "SAAB")
        self.assertEqual(snapshot.positions[0].quantity, "2")

        self.assertEqual(len(snapshot.open_orders), 1)
        self.assertEqual(snapshot.open_orders[0].external_order_id, "11")
        self.assertEqual(snapshot.open_orders[0].warning_text, "Held in TWS for review.")
        self.assertEqual(snapshot.open_orders[0].order_purpose, "Entry")
        self.assertEqual(snapshot.open_orders[0].working_price, "100")
        self.assertEqual(snapshot.open_orders[0].working_price_reference, "LIMIT")
        self.assertEqual(snapshot.open_orders[0].reference_market_price, "102.00")
        self.assertEqual(snapshot.open_orders[0].last_market_price_direction, "UP")
        self.assertEqual(snapshot.open_orders[0].price_spread, "-2.00")
        self.assertEqual(snapshot.open_orders[0].price_spread_pct, "-1.96")
        self.assertEqual(snapshot.open_orders[0].spread_reference, "LIMIT")

        self.assertEqual(len(snapshot.recent_fills), 1)
        self.assertEqual(snapshot.recent_fills[0].external_execution_id, "exec-001")

        self.assertEqual(len(snapshot.recent_broker_attention), 1)
        self.assertEqual(
            snapshot.recent_broker_attention[0].message,
            "[201] Order held for review",
        )
        self.assertEqual(
            snapshot.recent_broker_attention[0].operator_review.status,
            "ACKNOWLEDGED",
        )

        self.assertEqual(len(snapshot.recent_reconciliation_runs), 1)
        self.assertEqual(snapshot.recent_reconciliation_runs[0].status, "WARNINGS")
        self.assertEqual(len(snapshot.recent_reconciliation_runs[0].issues), 1)
        self.assertEqual(
            snapshot.recent_reconciliation_runs[0].issues[0].operator_review.status,
            "RESOLVED",
        )

    def test_build_operator_dashboard_snapshot_includes_account_day_performance(self) -> None:
        self._seed_operator_data()

        with patch(
            "ibkr_trader.read_models.operator_dashboard.utc_now",
            return_value=datetime(2026, 4, 19, 8, 0, tzinfo=timezone.utc),
        ):
            snapshot = build_operator_dashboard_snapshot(self.session_factory)

        performance = snapshot.accounts[0].day_performance
        self.assertEqual(performance.start_net_liquidation, "100000")
        self.assertEqual(performance.latest_net_liquidation, "101500")
        self.assertEqual(performance.latest_return_pct, "+1.50")
        self.assertEqual(len(performance.points), 2)
        self.assertEqual(performance.points[0].return_pct, "0.00")
        self.assertEqual(performance.points[1].return_pct, "+1.50")

    def test_build_operator_dashboard_snapshot_hides_archived_attention_and_warnings(self) -> None:
        self._seed_operator_data()
        record_broker_attention_review_action(
            self.session_factory,
            event_id=1,
            action_type="ARCHIVE",
            updated_by="dashboard",
        )
        record_reconciliation_issue_review_action(
            self.session_factory,
            issue_id=1,
            action_type="ARCHIVE",
            updated_by="dashboard",
        )

        snapshot = build_operator_dashboard_snapshot(
            self.session_factory,
            order_limit=10,
            fill_limit=10,
            attention_limit=10,
            reconciliation_run_limit=10,
        )

        self.assertEqual(snapshot.recent_broker_attention, ())
        self.assertEqual(snapshot.recent_reconciliation_runs, ())

    def test_build_operator_dashboard_snapshot_reports_exit_orders_against_fill_basis(self) -> None:
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
                instruction_id="2026-04-21-U25245596-long_risk_book-VOLCAR B-long-01",
                schema_version="2026-04-10",
                source_system="test",
                batch_id="batch-1",
                account_key="U25245596",
                book_key="long_risk_book",
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                state="EXIT_PENDING",
                submit_at=datetime(2026, 4, 21, 8, 0, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 21, 15, 30, tzinfo=timezone.utc),
                order_type="LMT",
                side="BUY",
                payload={},
            )
            session.add(instruction)
            session.flush()

            session.add_all(
                [
                    PositionSnapshotRecord(
                        broker_account_id=broker_account.id,
                        snapshot_at=datetime(2026, 4, 21, 12, 30, tzinfo=timezone.utc),
                        source="runtime_snapshot",
                        symbol="VOLCAR.B",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        primary_exchange="SFB",
                        local_symbol="VOLCAR B",
                        quantity="827",
                        average_cost="23.3192503",
                        market_price="23.30",
                        market_value="19269.10",
                        unrealized_pnl="-15.91",
                        realized_pnl="0.00",
                    ),
                    PositionSnapshotRecord(
                        broker_account_id=broker_account.id,
                        snapshot_at=datetime(2026, 4, 21, 12, 31, tzinfo=timezone.utc),
                        source="runtime_snapshot",
                        symbol="VOLCAR.B",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        primary_exchange="SFB",
                        local_symbol="VOLCAR B",
                        quantity="827",
                        average_cost="23.3192503",
                        market_price="23.30674555",
                        market_value="19274.68",
                        unrealized_pnl="-10.34",
                        realized_pnl="0.00",
                    ),
                ]
            )

            entry_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="ENTRY",
                external_order_id="85",
                external_perm_id="1030141445",
                external_client_id="0",
                order_ref=instruction.instruction_id,
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="VOLCAR B",
                side="BUY",
                order_type="MKT",
                time_in_force="DAY",
                status="Filled",
                total_quantity="827",
                limit_price=None,
                stop_price=None,
                submitted_at=datetime(2026, 4, 21, 8, 2, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 21, 8, 2, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            take_profit_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="EXIT",
                external_order_id="87",
                external_perm_id="1030141447",
                external_client_id="0",
                order_ref=f"{instruction.instruction_id}:exit:take_profit",
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="VOLCAR B",
                side="SELL",
                order_type="LMT",
                time_in_force="DAY",
                status="Submitted",
                total_quantity="827",
                limit_price="23.73",
                stop_price="0.0",
                submitted_at=datetime(2026, 4, 21, 8, 2, 1, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 21, 12, 31, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            catastrophic_stop_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="EXIT",
                external_order_id="88",
                external_perm_id="1030141448",
                external_client_id="0",
                order_ref=f"{instruction.instruction_id}:exit:catastrophic_stop",
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="VOLCAR B",
                side="SELL",
                order_type="STP",
                time_in_force="DAY",
                status="PreSubmitted",
                total_quantity="827",
                limit_price="0.0",
                stop_price="19.775",
                submitted_at=datetime(2026, 4, 21, 8, 2, 2, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 21, 12, 31, 1, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add_all([entry_order, take_profit_order, catastrophic_stop_order])
            session.flush()

            session.add(
                ExecutionFillRecord(
                    broker_order_id=entry_order.id,
                    instruction_id=instruction.id,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="U25245596",
                    external_execution_id="exec-volcar-entry",
                    external_order_id="85",
                    external_perm_id="1030141445",
                    order_ref=instruction.instruction_id,
                    symbol="VOLCAR.B",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    side="BOT",
                    quantity="827",
                    price="23.26",
                    commission="49.00",
                    commission_currency="SEK",
                    executed_at=datetime(2026, 4, 21, 8, 2, 0, tzinfo=timezone.utc),
                    raw_payload={},
                )
            )
            session.commit()
        finally:
            session.close()

        snapshot = build_operator_dashboard_snapshot(
            self.session_factory,
            order_limit=10,
            fill_limit=10,
            attention_limit=10,
            reconciliation_run_limit=10,
        )

        open_orders_by_ref = {row.order_ref: row for row in snapshot.open_orders}
        take_profit = open_orders_by_ref[
            "2026-04-21-U25245596-long_risk_book-VOLCAR B-long-01:exit:take_profit"
        ]
        catastrophic_stop = open_orders_by_ref[
            "2026-04-21-U25245596-long_risk_book-VOLCAR B-long-01:exit:catastrophic_stop"
        ]

        self.assertEqual(take_profit.order_purpose, "Take Profit")
        self.assertEqual(take_profit.working_price, "23.73")
        self.assertEqual(take_profit.working_price_reference, "LIMIT")
        self.assertEqual(take_profit.fill_basis_price, "23.26")
        self.assertEqual(take_profit.fill_price_spread, "+0.47")
        self.assertEqual(take_profit.fill_price_spread_pct, "+2.02")
        self.assertEqual(take_profit.spread_reference, "LIMIT")
        self.assertEqual(take_profit.price_spread, "+0.42")
        self.assertEqual(take_profit.price_spread_pct, "+1.82")

        self.assertEqual(catastrophic_stop.order_purpose, "Catastrophic Stop")
        self.assertEqual(catastrophic_stop.working_price, "19.775")
        self.assertEqual(catastrophic_stop.working_price_reference, "STOP")
        self.assertEqual(catastrophic_stop.fill_basis_price, "23.26")
        self.assertEqual(catastrophic_stop.fill_price_spread, "-3.48")
        self.assertEqual(catastrophic_stop.fill_price_spread_pct, "-14.98")
        self.assertEqual(catastrophic_stop.spread_reference, "STOP")

    def test_build_operator_dashboard_snapshot_hides_auto_recovered_insufficient_funds_rejects(self) -> None:
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
                instruction_id="2026-04-21-U25245596-long_risk_book-VOLCAR B-long-01",
                schema_version="2026-04-10",
                source_system="test",
                batch_id="batch-1",
                account_key="U25245596",
                book_key="long_risk_book",
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                state="POSITION_OPEN",
                submit_at=datetime(2026, 4, 21, 7, 20, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 21, 15, 30, tzinfo=timezone.utc),
                order_type="LMT",
                side="BUY",
                payload={},
            )
            session.add(instruction)
            session.flush()

            rejected_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="ENTRY",
                external_order_id="84",
                external_perm_id="10084",
                external_client_id="0",
                order_ref=instruction.instruction_id,
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
            replacement_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="ENTRY",
                external_order_id="85",
                external_perm_id="10085",
                external_client_id="0",
                order_ref=instruction.instruction_id,
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="VOLCAR B",
                side="BUY",
                order_type="LMT",
                time_in_force="DAY",
                status="Filled",
                total_quantity="827",
                limit_price="23.26",
                submitted_at=datetime(2026, 4, 21, 7, 25, 2, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 21, 7, 26, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add_all([rejected_order, replacement_order])
            session.flush()

            session.add(
                BrokerOrderEventRecord(
                    broker_order_id=rejected_order.id,
                    event_type="order_error_callback",
                    event_at=datetime(2026, 4, 21, 7, 25, 1, tzinfo=timezone.utc),
                    status_before="Submitted",
                    status_after="Inactive",
                    payload={
                        "errorCode": 201,
                        "errorString": (
                            "Order rejected - reason:We are unable to accept your order. "
                            "Your Available Funds are insufficient to cover the change in the "
                            "account's margin requirements."
                        ),
                    },
                    note="Persisted broker order error callback directly from the live session.",
                )
            )
            session.commit()
        finally:
            session.close()

        snapshot = build_operator_dashboard_snapshot(
            self.session_factory,
            order_limit=10,
            fill_limit=10,
            attention_limit=10,
            reconciliation_run_limit=10,
        )

        self.assertEqual(tuple(snapshot.recent_broker_attention), ())

    def test_build_operator_dashboard_snapshot_hides_expected_oca_exit_sibling_cancel(self) -> None:
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
                instruction_id="2026-05-04-U25245596-live_top1_31_seedpicker-HACK-long-01",
                schema_version="2026-04-10",
                source_system="test",
                batch_id="batch-1",
                account_key="U25245596",
                book_key="live_top1_31_seedpicker",
                symbol="HACK",
                exchange="SMART",
                currency="SEK",
                state="COMPLETED",
                submit_at=datetime(2026, 5, 4, 7, 25, tzinfo=timezone.utc),
                expire_at=datetime(2026, 5, 4, 15, 30, tzinfo=timezone.utc),
                order_type="LMT",
                side="BUY",
                payload={},
            )
            session.add(instruction)
            session.flush()

            oca_group = "OCAB5AB0E78DC34DB63"
            stop_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="EXIT",
                external_order_id="4840",
                external_perm_id="1010318184",
                external_client_id="0",
                order_ref=f"{instruction.instruction_id}:exit:catastrophic_stop",
                symbol="HACK",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="HACK",
                side="SELL",
                order_type="STP",
                time_in_force="DAY",
                status="Cancelled",
                total_quantity="229",
                stop_price="66.10",
                submitted_at=datetime(2026, 5, 4, 7, 25, 13, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 5, 4, 12, 22, 50, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={"oca_group": oca_group},
            )
            take_profit_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="EXIT",
                external_order_id="4841",
                external_perm_id="1010318185",
                external_client_id="0",
                order_ref=f"{instruction.instruction_id}:exit:take_profit",
                symbol="HACK",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="HACK",
                side="SELL",
                order_type="LMT",
                time_in_force="DAY",
                status="FILLED",
                total_quantity="229",
                limit_price="79.30",
                submitted_at=datetime(2026, 5, 4, 7, 25, 14, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 5, 4, 12, 22, 50, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={"oca_group": oca_group},
            )
            session.add_all([stop_order, take_profit_order])
            session.flush()

            session.add(
                BrokerOrderEventRecord(
                    broker_order_id=stop_order.id,
                    event_type="order_error_callback",
                    event_at=datetime(2026, 5, 4, 12, 22, 50, tzinfo=timezone.utc),
                    status_before="Cancelled",
                    status_after="Cancelled",
                    payload={
                        "orderId": 4840,
                        "errorCode": 202,
                        "errorString": "Order Canceled - reason:",
                    },
                    note="Persisted broker order error callback directly from the live session.",
                )
            )
            session.commit()
        finally:
            session.close()

        snapshot = build_operator_dashboard_snapshot(
            self.session_factory,
            order_limit=10,
            fill_limit=10,
            attention_limit=10,
            reconciliation_run_limit=10,
        )

        self.assertEqual(tuple(snapshot.recent_broker_attention), ())

    def test_build_operator_dashboard_snapshot_keeps_unmatched_exit_cancel_attention(self) -> None:
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
                instruction_id="2026-05-04-U25245596-live_top1_31_seedpicker-HACK-long-01",
                schema_version="2026-04-10",
                source_system="test",
                batch_id="batch-1",
                account_key="U25245596",
                book_key="live_top1_31_seedpicker",
                symbol="HACK",
                exchange="SMART",
                currency="SEK",
                state="POSITION_OPEN",
                submit_at=datetime(2026, 5, 4, 7, 25, tzinfo=timezone.utc),
                expire_at=datetime(2026, 5, 4, 15, 30, tzinfo=timezone.utc),
                order_type="LMT",
                side="BUY",
                payload={},
            )
            session.add(instruction)
            session.flush()

            stop_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="EXIT",
                external_order_id="4840",
                external_perm_id="1010318184",
                external_client_id="0",
                order_ref=f"{instruction.instruction_id}:exit:catastrophic_stop",
                symbol="HACK",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="HACK",
                side="SELL",
                order_type="STP",
                time_in_force="DAY",
                status="Cancelled",
                total_quantity="229",
                stop_price="66.10",
                submitted_at=datetime(2026, 5, 4, 7, 25, 13, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 5, 4, 12, 22, 50, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={"oca_group": "OCAB5AB0E78DC34DB63"},
            )
            session.add(stop_order)
            session.flush()

            session.add(
                BrokerOrderEventRecord(
                    broker_order_id=stop_order.id,
                    event_type="order_error_callback",
                    event_at=datetime(2026, 5, 4, 12, 22, 50, tzinfo=timezone.utc),
                    status_before="Cancelled",
                    status_after="Cancelled",
                    payload={
                        "orderId": 4840,
                        "errorCode": 202,
                        "errorString": "Order Canceled - reason:",
                    },
                    note="Persisted broker order error callback directly from the live session.",
                )
            )
            session.commit()
        finally:
            session.close()

        snapshot = build_operator_dashboard_snapshot(
            self.session_factory,
            order_limit=10,
            fill_limit=10,
            attention_limit=10,
            reconciliation_run_limit=10,
        )

        self.assertEqual(len(snapshot.recent_broker_attention), 1)
        self.assertEqual(
            snapshot.recent_broker_attention[0].message,
            "[202] Order Canceled - reason:",
        )

    def test_build_operator_dashboard_snapshot_dedupes_replaced_open_order_lineage(self) -> None:
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
                instruction_id="2026-04-21-U25245596-long_risk_book-VOLCAR B-long-01",
                schema_version="2026-04-10",
                source_system="test",
                batch_id="batch-1",
                account_key="U25245596",
                book_key="long_risk_book",
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                state="EXIT_PENDING",
                submit_at=datetime(2026, 4, 21, 7, 20, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 21, 15, 30, tzinfo=timezone.utc),
                order_type="LMT",
                side="BUY",
                payload={},
            )
            session.add(instruction)
            session.flush()

            session.add_all(
                [
                    BrokerOrderRecord(
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        order_role="EXIT",
                        external_order_id="3952",
                        external_perm_id="449407988",
                        external_client_id="0",
                        order_ref=f"{instruction.instruction_id}:exit:forced",
                        symbol="VOLCAR.B",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        primary_exchange="SFB",
                        local_symbol="VOLCAR B",
                        side="SELL",
                        order_type="MKT",
                        time_in_force="DAY",
                        status="PreSubmitted",
                        total_quantity="827",
                        submitted_at=datetime(2026, 4, 23, 6, 30, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 23, 6, 30, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    ),
                    BrokerOrderRecord(
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        order_role="EXIT",
                        external_order_id="3953",
                        external_perm_id="449407988",
                        external_client_id="0",
                        order_ref=f"{instruction.instruction_id}:exit:forced",
                        symbol="VOLCAR.B",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        primary_exchange="SFB",
                        local_symbol="VOLCAR B",
                        side="SELL",
                        order_type="MKT",
                        time_in_force="DAY",
                        status="PreSubmitted",
                        total_quantity="827",
                        submitted_at=datetime(2026, 4, 23, 6, 31, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 23, 6, 31, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    ),
                ]
            )
            session.commit()
        finally:
            session.close()

        snapshot = build_operator_dashboard_snapshot(
            self.session_factory,
            order_limit=10,
            fill_limit=10,
            attention_limit=10,
            reconciliation_run_limit=10,
        )

        volcar_orders = [row for row in snapshot.open_orders if row.symbol == "VOLCAR.B"]
        self.assertEqual(len(volcar_orders), 1)
        self.assertEqual(volcar_orders[0].external_order_id, "3953")

    def test_build_operator_dashboard_snapshot_hides_effectively_closed_exit_orders(self) -> None:
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

            volcar_instruction = InstructionRecord(
                instruction_id="2026-04-21-U25245596-long_risk_book-VOLCAR B-long-01",
                schema_version="2026-04-10",
                source_system="test",
                batch_id="batch-volcar",
                account_key="U25245596",
                book_key="long_risk_book",
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                state="EXIT_PENDING",
                submit_at=datetime(2026, 4, 21, 7, 20, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 21, 15, 30, tzinfo=timezone.utc),
                order_type="LMT",
                side="BUY",
                payload={},
            )
            sive_instruction = InstructionRecord(
                instruction_id="2026-04-20-U25245596-manual_delayed_sive-buy-01",
                schema_version="2026-04-10",
                source_system="manual-test",
                batch_id="batch-sive",
                account_key="U25245596",
                book_key="manual_delayed_sive",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                state="EXIT_PENDING",
                submit_at=datetime(2026, 4, 20, 13, 55, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 20, 13, 58, tzinfo=timezone.utc),
                order_type="MKT",
                side="BUY",
                payload={},
            )
            session.add_all([volcar_instruction, sive_instruction])
            session.flush()

            session.add_all(
                [
                    BrokerOrderRecord(
                        instruction_id=volcar_instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        order_role="EXIT",
                        external_order_id="3953",
                        external_perm_id="449407988",
                        external_client_id="0",
                        order_ref=f"{volcar_instruction.instruction_id}:exit:forced",
                        symbol="VOLCAR.B",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        primary_exchange="SFB",
                        local_symbol="VOLCAR B",
                        side="SELL",
                        order_type="MKT",
                        time_in_force="DAY",
                        status="PendingCancel",
                        total_quantity="827",
                        submitted_at=datetime(2026, 4, 23, 6, 31, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 23, 7, 44, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    ),
                    BrokerOrderRecord(
                        instruction_id=sive_instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        order_role="EXIT",
                        external_order_id="38",
                        external_perm_id="156906838",
                        external_client_id="0",
                        order_ref=f"{sive_instruction.instruction_id}:exit:delayed_limit",
                        symbol="SIVE",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        primary_exchange="SFB",
                        local_symbol="SIVE",
                        side="SELL",
                        order_type="LMT",
                        time_in_force="DAY",
                        status="Submitted",
                        total_quantity="1",
                        limit_price="32.7",
                        submitted_at=datetime(2026, 4, 20, 13, 58, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 21, 15, 15, 47, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    ),
                    PositionSnapshotRecord(
                        broker_account_id=broker_account.id,
                        snapshot_at=datetime(2026, 4, 23, 19, 42, tzinfo=timezone.utc),
                        source="runtime_snapshot",
                        symbol="VOLCAR.B",
                        exchange="SFB",
                        currency="SEK",
                        security_type="STK",
                        primary_exchange=None,
                        local_symbol="VOLCAR B",
                        quantity="0",
                        average_cost="0.0",
                        market_price="22.55",
                        market_value="0.0",
                        unrealized_pnl="0",
                        realized_pnl="-635.55",
                    ),
                    ExecutionFillRecord(
                        broker_order_id=None,
                        instruction_id=sive_instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        external_execution_id="00014800.69e72208.01.01",
                        external_order_id="67",
                        external_perm_id="156906838",
                        order_ref=f"{sive_instruction.instruction_id}:exit:delayed_limit",
                        symbol="SIVE",
                        exchange="SFB",
                        currency="SEK",
                        security_type="STK",
                        side="SLD",
                        quantity="1",
                        price="32.7",
                        commission="49.0",
                        commission_currency="SEK",
                        executed_at=datetime(2026, 4, 21, 15, 15, 58, tzinfo=timezone.utc),
                        raw_payload={},
                    ),
                ]
            )
            session.commit()
        finally:
            session.close()

        snapshot = build_operator_dashboard_snapshot(
            self.session_factory,
            order_limit=10,
            fill_limit=10,
            attention_limit=10,
            reconciliation_run_limit=10,
        )

        symbols = {row.symbol for row in snapshot.open_orders}
        self.assertNotIn("VOLCAR.B", symbols)
        self.assertNotIn("SIVE", symbols)

    def test_open_orders_are_not_starved_by_recent_closed_rows(self) -> None:
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
            session.add(
                BrokerOrderRecord(
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="U25245596",
                    order_role="ENTRY",
                    external_order_id="open-1",
                    external_perm_id="open-perm-1",
                    external_client_id="0",
                    order_ref="old-open-order",
                    symbol="SAAB",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    side="BUY",
                    order_type="LMT",
                    time_in_force="DAY",
                    status="Submitted",
                    total_quantity="2",
                    limit_price="100.00",
                    submitted_at=datetime(2026, 4, 19, 7, 0, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 19, 7, 0, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
            )
            for index in range(20):
                session.add(
                    BrokerOrderRecord(
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        order_role="ENTRY",
                        external_order_id=f"closed-{index}",
                        external_perm_id=f"closed-perm-{index}",
                        external_client_id="0",
                        order_ref=f"closed-order-{index}",
                        symbol="ERIC B",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        side="BUY",
                        order_type="LMT",
                        time_in_force="DAY",
                        status="Filled",
                        total_quantity="1",
                        limit_price="80.00",
                        submitted_at=datetime(
                            2026, 4, 19, 7, index + 1, tzinfo=timezone.utc
                        ),
                        last_status_at=datetime(
                            2026, 4, 19, 7, index + 1, tzinfo=timezone.utc
                        ),
                        raw_payload={},
                        metadata_json={},
                    )
                )
            session.commit()
        finally:
            session.close()

        snapshot = build_operator_dashboard_snapshot(
            self.session_factory,
            order_limit=1,
            fill_limit=10,
            attention_limit=10,
            reconciliation_run_limit=10,
        )

        self.assertEqual(len(snapshot.open_orders), 1)
        self.assertEqual(snapshot.open_orders[0].order_ref, "old-open-order")

    def test_partially_filled_exit_order_remains_open(self) -> None:
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
                order_role="EXIT",
                external_order_id="exit-1",
                external_perm_id="exit-perm-1",
                external_client_id="0",
                order_ref="position-1:exit:forced",
                symbol="SAAB",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                side="SELL",
                order_type="LMT",
                time_in_force="DAY",
                status="Submitted",
                total_quantity="10",
                limit_price="105.00",
                submitted_at=datetime(2026, 4, 19, 7, 0, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 19, 7, 0, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add(broker_order)
            session.flush()
            session.add(
                ExecutionFillRecord(
                    broker_order_id=broker_order.id,
                    instruction_id=None,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="U25245596",
                    external_execution_id="partial-exit-fill",
                    external_order_id="exit-1",
                    external_perm_id="exit-perm-1",
                    order_ref="position-1:exit:forced",
                    symbol="SAAB",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    side="SLD",
                    quantity="4",
                    price="105.00",
                    commission="1.00",
                    commission_currency="SEK",
                    executed_at=datetime(2026, 4, 19, 7, 1, tzinfo=timezone.utc),
                    raw_payload={},
                )
            )
            session.commit()
        finally:
            session.close()

        snapshot = build_operator_dashboard_snapshot(
            self.session_factory,
            order_limit=10,
            fill_limit=10,
            attention_limit=10,
            reconciliation_run_limit=10,
        )

        self.assertEqual(len(snapshot.open_orders), 1)
        self.assertEqual(snapshot.open_orders[0].external_order_id, "exit-1")


if __name__ == "__main__":
    unittest.main()
