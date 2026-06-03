from __future__ import annotations

from datetime import datetime
from datetime import timezone
from decimal import Decimal
from unittest import TestCase
from unittest.mock import patch

from sqlalchemy import select

from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.models import AccountSnapshotRecord
from ibkr_trader.db.models import BrokerAccountRecord
from ibkr_trader.db.models import BrokerOrderEventRecord
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import ExecutionFillRecord
from ibkr_trader.db.models import InstructionEventRecord
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

class BrokerLedgerPersistenceTestCase(TestCase):
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


__all__ = [name for name in globals() if not name.startswith("__")]
