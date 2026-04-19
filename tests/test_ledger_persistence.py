from __future__ import annotations

from datetime import datetime
from datetime import timezone
from decimal import Decimal
from unittest import TestCase

from sqlalchemy import select

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
from ibkr_trader.ibkr.runtime_snapshot import BrokerExecution
from ibkr_trader.ibkr.runtime_snapshot import BrokerOpenOrder
from ibkr_trader.ibkr.runtime_snapshot import BrokerPortfolioItem
from ibkr_trader.ibkr.runtime_snapshot import BrokerPosition
from ibkr_trader.ibkr.runtime_snapshot import BrokerRuntimeSnapshot
from ibkr_trader.ledger.persistence import BROKER_KIND_IBKR
from ibkr_trader.ledger.persistence import persist_broker_callback_events
from ibkr_trader.ledger.persistence import persist_broker_runtime_snapshot
from ibkr_trader.orchestration.state_machine import ExecutionState


class BrokerLedgerPersistenceTests(TestCase):
    def setUp(self) -> None:
        self.engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(self.engine)
        self.session_factory = create_session_factory(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    def _insert_instruction(self) -> None:
        session = self.session_factory()
        try:
            session.add(
                InstructionRecord(
                    instruction_id="persisted-aapl-1",
                    schema_version="2026-04-10",
                    source_system="q-training",
                    batch_id="batch-1",
                    account_key="DU1234567",
                    book_key="long_risk_book",
                    symbol="AAPL",
                    exchange="SMART",
                    currency="USD",
                    state=ExecutionState.ENTRY_SUBMITTED.value,
                    submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
                    expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
                    order_type="LIMIT",
                    side="BUY",
                    payload={
                        "instruction": {
                            "instruction_id": "persisted-aapl-1",
                            "account": {"account_key": "DU1234567", "book_key": "long_risk_book"},
                            "instrument": {
                                "symbol": "AAPL",
                                "security_type": "STK",
                                "exchange": "SMART",
                                "currency": "USD",
                                "primary_exchange": "NASDAQ",
                                "local_symbol": "AAPL",
                            },
                            "entry": {
                                "limit_price": "200.00",
                                "time_in_force": "DAY",
                            },
                        }
                    },
                )
            )
            session.commit()
        finally:
            session.close()

    def _insert_broker_order(self) -> None:
        session = self.session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind=BROKER_KIND_IBKR,
                account_key="DU1234567",
                base_currency="USD",
            )
            session.add(broker_account)
            session.flush()
            session.add(
                BrokerOrderRecord(
                    instruction_id=None,
                    broker_account_id=broker_account.id,
                    broker_kind=BROKER_KIND_IBKR,
                    account_key="DU1234567",
                    order_role="ENTRY",
                    external_order_id="11",
                    external_perm_id="9001",
                    external_client_id="0",
                    order_ref="persisted-aapl-1",
                    symbol="AAPL",
                    exchange="SMART",
                    currency="USD",
                    security_type="STK",
                    primary_exchange="NASDAQ",
                    local_symbol="AAPL",
                    side="BUY",
                    order_type="LMT",
                    time_in_force="DAY",
                    status="PreSubmitted",
                    total_quantity="1",
                    limit_price="200.00",
                    stop_price=None,
                    submitted_at=datetime(2026, 4, 19, 8, 30, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 19, 8, 30, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
            )
            session.commit()
        finally:
            session.close()

    def test_persist_broker_runtime_snapshot_writes_real_ledger_rows(self) -> None:
        self._insert_instruction()

        captured_at = datetime(2026, 4, 19, 8, 30, tzinfo=timezone.utc)
        initial_snapshot = BrokerRuntimeSnapshot(
            open_orders={
                17: BrokerOpenOrder(
                    order_id=17,
                    perm_id=9001,
                    client_id=0,
                    status="PreSubmitted",
                    order_ref="persisted-aapl-1",
                    action="BUY",
                    total_quantity=Decimal("1"),
                    symbol="AAPL",
                    account="DU1234567",
                    security_type="STK",
                    exchange="SMART",
                    primary_exchange="NASDAQ",
                    currency="USD",
                    local_symbol="AAPL",
                    order_type="LMT",
                    limit_price=Decimal("200.00"),
                    aux_price=None,
                    outside_rth=False,
                    oca_group=None,
                    oca_type=None,
                    transmit=True,
                    warning_text=None,
                    reject_reason=None,
                    completed_status=None,
                    completed_time=None,
                )
            },
            executions=(
                BrokerExecution(
                    exec_id="00014800.69ddd749.01.01",
                    order_id=17,
                    perm_id=9001,
                    client_id=0,
                    order_ref="persisted-aapl-1",
                    side="BOT",
                    shares=Decimal("1"),
                    price=Decimal("200.00"),
                    exchange="NASDAQ",
                    executed_at=datetime(2026, 4, 19, 8, 31, tzinfo=timezone.utc),
                    symbol="AAPL",
                    account="DU1234567",
                    security_type="STK",
                    primary_exchange="NASDAQ",
                    currency="USD",
                    local_symbol="AAPL",
                ),
            ),
            portfolio=(
                BrokerPortfolioItem(
                    account="DU1234567",
                    symbol="AAPL",
                    local_symbol="AAPL",
                    security_type="STK",
                    exchange="SMART",
                    primary_exchange="NASDAQ",
                    currency="USD",
                    position=Decimal("1"),
                    market_price=Decimal("201.50"),
                    market_value=Decimal("201.50"),
                    average_cost=Decimal("200.00"),
                    unrealized_pnl=Decimal("1.50"),
                    realized_pnl=Decimal("0"),
                ),
            ),
            positions=(
                BrokerPosition(
                    account="DU1234567",
                    symbol="AAPL",
                    local_symbol="AAPL",
                    security_type="STK",
                    exchange="SMART",
                    primary_exchange="NASDAQ",
                    currency="USD",
                    position=Decimal("1"),
                    average_cost=Decimal("200.00"),
                ),
            ),
            account_values={
                "DU1234567": {
                    "NetLiquidation": {"value": "100000.00", "currency": "USD"},
                    "BuyingPower": {"value": "200000.00", "currency": "USD"},
                    "TotalCashValue": {"value": "99800.00", "currency": "USD"},
                }
            },
        )

        persist_broker_runtime_snapshot(
            self.session_factory,
            initial_snapshot,
            broker_kind=BROKER_KIND_IBKR,
            captured_at=captured_at,
            default_account_key="DU1234567",
        )

        updated_snapshot = BrokerRuntimeSnapshot(
            open_orders={
                17: BrokerOpenOrder(
                    order_id=17,
                    perm_id=9001,
                    client_id=0,
                    status="Submitted",
                    order_ref="persisted-aapl-1",
                    action="BUY",
                    total_quantity=Decimal("1"),
                    symbol="AAPL",
                    account="DU1234567",
                    security_type="STK",
                    exchange="SMART",
                    primary_exchange="NASDAQ",
                    currency="USD",
                    local_symbol="AAPL",
                    order_type="LMT",
                    limit_price=Decimal("200.00"),
                    aux_price=None,
                    outside_rth=False,
                    oca_group=None,
                    oca_type=None,
                    transmit=True,
                    warning_text=None,
                    reject_reason=None,
                    completed_status=None,
                    completed_time=None,
                )
            },
            executions=initial_snapshot.executions,
            portfolio=initial_snapshot.portfolio,
            positions=initial_snapshot.positions,
            account_values=initial_snapshot.account_values,
        )

        persist_broker_runtime_snapshot(
            self.session_factory,
            updated_snapshot,
            broker_kind=BROKER_KIND_IBKR,
            captured_at=datetime(2026, 4, 19, 8, 32, tzinfo=timezone.utc),
            default_account_key="DU1234567",
        )

        session = self.session_factory()
        try:
            broker_accounts = session.execute(select(BrokerAccountRecord)).scalars().all()
            account_snapshots = session.execute(select(AccountSnapshotRecord)).scalars().all()
            position_snapshots = session.execute(select(PositionSnapshotRecord)).scalars().all()
            broker_orders = session.execute(select(BrokerOrderRecord)).scalars().all()
            broker_order_events = session.execute(
                select(BrokerOrderEventRecord).order_by(BrokerOrderEventRecord.id)
            ).scalars().all()
            execution_fills = session.execute(select(ExecutionFillRecord)).scalars().all()

            self.assertEqual(len(broker_accounts), 1)
            self.assertEqual(broker_accounts[0].account_key, "DU1234567")
            self.assertEqual(len(account_snapshots), 2)
            self.assertEqual(account_snapshots[0].net_liquidation, "100000.00")
            self.assertEqual(len(position_snapshots), 2)
            self.assertEqual(position_snapshots[0].quantity, "1")
            self.assertEqual(len(broker_orders), 1)
            self.assertEqual(broker_orders[0].status, "Submitted")
            self.assertEqual(broker_orders[0].instruction_id, 1)
            self.assertEqual(len(broker_order_events), 2)
            self.assertEqual(
                [event.status_after for event in broker_order_events],
                ["PreSubmitted", "Submitted"],
            )
            self.assertEqual(len(execution_fills), 1)
            self.assertEqual(execution_fills[0].external_execution_id, "00014800.69ddd749.01.01")
            self.assertEqual(execution_fills[0].instruction_id, 1)
        finally:
            session.close()

    def test_persist_broker_runtime_snapshot_raises_for_missing_execution_account(self) -> None:
        snapshot = BrokerRuntimeSnapshot(
            open_orders={},
            executions=(
                BrokerExecution(
                    exec_id="missing-account-exec",
                    order_id=17,
                    perm_id=9001,
                    client_id=0,
                    order_ref="persisted-aapl-1",
                    side="BOT",
                    shares=Decimal("1"),
                    price=Decimal("200.00"),
                    exchange="NASDAQ",
                    executed_at=datetime(2026, 4, 19, 8, 31, tzinfo=timezone.utc),
                    symbol="AAPL",
                    account=None,
                    security_type="STK",
                    primary_exchange="NASDAQ",
                    currency="USD",
                    local_symbol="AAPL",
                ),
            ),
            portfolio=(),
            positions=(),
            account_values={},
        )

        with self.assertRaisesRegex(ValueError, "did not include a broker account"):
            persist_broker_runtime_snapshot(
                self.session_factory,
                snapshot,
                broker_kind=BROKER_KIND_IBKR,
                captured_at=datetime(2026, 4, 19, 8, 30, tzinfo=timezone.utc),
            )

    def test_persist_broker_callback_events_updates_order_status_and_rejects(self) -> None:
        self._insert_broker_order()

        persist_broker_callback_events(
            self.session_factory,
            [
                {
                    "event_type": "order_status",
                    "event_at": datetime(2026, 4, 19, 8, 31, tzinfo=timezone.utc),
                    "order_status": {
                        "orderId": 11,
                        "status": "Submitted",
                        "filled": "0",
                        "remaining": "1",
                        "avgFillPrice": "0.0",
                        "permId": 9001,
                        "parentId": 0,
                        "lastFillPrice": "0.0",
                        "clientId": 0,
                        "whyHeld": "",
                        "mktCapPrice": "0.0",
                    },
                },
                {
                    "event_type": "order_error",
                    "event_at": datetime(2026, 4, 19, 8, 32, tzinfo=timezone.utc),
                    "error": {
                        "orderId": 11,
                        "errorTime": 0,
                        "errorCode": 202,
                        "errorString": "Rejected by exchange",
                        "advancedOrderRejectJson": '{"reason":"test"}',
                    },
                },
            ],
            broker_kind=BROKER_KIND_IBKR,
            default_account_key="DU1234567",
        )

        session = self.session_factory()
        try:
            broker_order = session.execute(select(BrokerOrderRecord)).scalar_one()
            broker_order_events = session.execute(
                select(BrokerOrderEventRecord).order_by(BrokerOrderEventRecord.id)
            ).scalars().all()

            self.assertEqual(broker_order.status, "Submitted")
            self.assertEqual(
                [event.event_type for event in broker_order_events],
                ["order_status_callback", "order_error_callback"],
            )
            self.assertEqual(
                broker_order.metadata_json["last_order_error_callback"]["errorCode"],
                202,
            )
        finally:
            session.close()
