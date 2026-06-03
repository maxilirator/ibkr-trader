from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from sqlalchemy import select

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.models import AccountSnapshotRecord
from ibkr_trader.db.models import BrokerAccountRecord
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import ExecutionFillRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import PositionSnapshotRecord
from ibkr_trader.db.models import ReconciliationIssueRecord
from ibkr_trader.db.models import ReconciliationRunRecord
from ibkr_trader.domain.execution_contract import OrderType
from ibkr_trader.ibkr.runtime_snapshot import BrokerExecution
from ibkr_trader.ibkr.runtime_snapshot import BrokerOpenOrder
from ibkr_trader.ibkr.runtime_snapshot import BrokerPortfolioItem
from ibkr_trader.ibkr.runtime_snapshot import BrokerPosition
from ibkr_trader.ibkr.runtime_snapshot import BrokerRuntimeSnapshot
from ibkr_trader.orchestration.operator_controls import set_kill_switch_state
from ibkr_trader.orchestration.runtime_worker import _build_runtime_broker_operations
from ibkr_trader.orchestration.runtime_worker import _persisted_open_order_ids_by_instruction
from ibkr_trader.orchestration.runtime_worker import _submit_due_pending_entries
from ibkr_trader.orchestration.runtime_worker import run_runtime_cycle
from ibkr_trader.orchestration.runtime_worker import run_startup_reconciliation
from ibkr_trader.orchestration.state_machine import ExecutionState


def _aapl_payload() -> dict[str, object]:
    return {
        "schema_version": "2026-04-10",
        "source": {
            "system": "q-training",
            "batch_id": "batch-1",
            "generated_at": "2026-04-10T02:15:44Z",
        },
        "instruction": {
            "instruction_id": "runtime-aapl-1",
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
                "reason_code": "runtime-test",
            },
        },
    }


def _sive_payload() -> dict[str, object]:
    return {
        "schema_version": "2026-04-10",
        "source": {
            "system": "q-training",
            "batch_id": "batch-1",
            "generated_at": "2026-04-10T02:15:44Z",
        },
        "instruction": {
            "instruction_id": "runtime-sive-1",
            "account": {
                "account_key": "GTW05",
                "book_key": "long_risk_book",
            },
            "instrument": {
                "symbol": "SIVE",
                "security_type": "STK",
                "exchange": "SMART",
                "currency": "SEK",
                "primary_exchange": "SFB",
            },
            "intent": {
                "side": "BUY",
                "position_side": "LONG",
            },
            "sizing": {
                "mode": "target_quantity",
                "target_quantity": "100",
            },
            "entry": {
                "order_type": "LIMIT",
                "submit_at": "2026-04-10T09:25:00+02:00",
                "expire_at": "2026-04-10T17:30:00+02:00",
                "limit_price": "11.3131",
                "time_in_force": "DAY",
                "max_submit_count": 1,
                "cancel_unfilled_at_expiry": True,
            },
            "exit": {
                "force_exit_next_session_open": True,
            },
            "trace": {
                "reason_code": "runtime-test",
            },
        },
    }


def _sive_broker_position(
    quantity: str = "100",
    *,
    account: str = "DU1234567",
) -> BrokerPosition:
    return BrokerPosition(
        account=account,
        symbol="SIVE",
        local_symbol="SIVE",
        security_type="STK",
        exchange="SMART",
        primary_exchange="SFB",
        currency="SEK",
        position=Decimal(quantity),
        average_cost=Decimal("11.3131"),
    )


def _duplicate_take_profit_open_orders() -> dict[int, BrokerOpenOrder]:
    return {
        42: BrokerOpenOrder(
            order_id=42,
            perm_id=9042,
            client_id=0,
            status="Submitted",
            order_ref="runtime-aapl-1:exit:take_profit",
            action="SELL",
            total_quantity=Decimal("1"),
            symbol="AAPL",
            account="DU1234567",
            security_type="STK",
            exchange="SMART",
            primary_exchange="NASDAQ",
            currency="USD",
            local_symbol="AAPL",
            order_type="LMT",
            limit_price=Decimal("204.00"),
        ),
        43: BrokerOpenOrder(
            order_id=43,
            perm_id=9043,
            client_id=0,
            status="PreSubmitted",
            order_ref="runtime-aapl-1:exit:take_profit",
            action="SELL",
            total_quantity=Decimal("1"),
            symbol="AAPL",
            account="DU1234567",
            security_type="STK",
            exchange="SMART",
            primary_exchange="NASDAQ",
            currency="USD",
            local_symbol="AAPL",
            order_type="LMT",
            limit_price=Decimal("204.00"),
        ),
    }


def _delayed_limit_open_orders() -> dict[int, BrokerOpenOrder]:
    return {
        41: BrokerOpenOrder(
            order_id=41,
            perm_id=9141,
            client_id=0,
            status="Submitted",
            order_ref="runtime-sive-1:exit:delayed_limit",
            action="SELL",
            total_quantity=Decimal("1"),
            symbol="SIVE",
            account="DU1234567",
            security_type="STK",
            exchange="SMART",
            primary_exchange="SFB",
            currency="SEK",
            local_symbol="SIVE",
            order_type="LMT",
            limit_price=Decimal("21.00"),
        ),
    }

class RuntimeWorkerTestCase(TestCase):
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

    def _insert_instruction(
        self,
        *,
        instruction_id: str,
        symbol: str,
        exchange: str,
        currency: str,
        state: str,
        submit_at: datetime,
        expire_at: datetime,
        payload: dict[str, object],
        broker_order_id: int | None = None,
        exit_order_id: int | None = None,
        entry_filled_quantity: str | None = None,
        entry_avg_fill_price: str | None = None,
        account_key: str = "GTW05",
        book_key: str = "long_risk_book",
        is_virtual: bool = False,
        side: str = "BUY",
    ) -> None:
        session = self.session_factory()
        try:
            session.add(
                InstructionRecord(
                    instruction_id=instruction_id,
                    schema_version="2026-04-10",
                    source_system="q-training",
                    batch_id="batch-1",
                    account_key=account_key,
                    book_key=book_key,
                    is_virtual=is_virtual,
                    symbol=symbol,
                    exchange=exchange,
                    currency=currency,
                    state=state,
                    submit_at=submit_at,
                    expire_at=expire_at,
                    order_type="LIMIT",
                    side=side,
                    broker_order_id=broker_order_id,
                    exit_order_id=exit_order_id,
                    entry_filled_quantity=entry_filled_quantity,
                    entry_avg_fill_price=entry_avg_fill_price,
                    payload=payload,
                )
            )
            session.commit()
        finally:
            session.close()

    def _read_record(self, instruction_id: str) -> InstructionRecord:
        session = self.session_factory()
        try:
            return session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == instruction_id
                )
            ).scalar_one()
        finally:
            session.close()

    def _read_reconciliation_runs(self) -> list[ReconciliationRunRecord]:
        session = self.session_factory()
        try:
            return list(
                session.execute(
                    select(ReconciliationRunRecord).order_by(ReconciliationRunRecord.id)
                ).scalars()
            )
        finally:
            session.close()

    def _insert_broker_order(
        self,
        *,
        external_order_id: str,
        status: str = "PreSubmitted",
    ) -> None:
        session = self.session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="DU1234567",
                base_currency="USD",
            )
            session.add(broker_account)
            session.flush()
            session.add(
                BrokerOrderRecord(
                    instruction_id=None,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="DU1234567",
                    order_role="ENTRY",
                    external_order_id=external_order_id,
                    external_perm_id="8001",
                    external_client_id="0",
                    order_ref="runtime-aapl-1",
                    symbol="AAPL",
                    exchange="SMART",
                    currency="USD",
                    security_type="STK",
                    primary_exchange="NASDAQ",
                    local_symbol="AAPL",
                    side="BUY",
                    order_type="LMT",
                    time_in_force="DAY",
                    status=status,
                    total_quantity="1",
                    limit_price="200.00",
                    stop_price=None,
                    submitted_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
            )
            session.commit()
        finally:
            session.close()

    def _insert_exit_broker_order(
        self,
        *,
        instruction_id: str,
        external_order_id: str,
        order_ref: str,
        symbol: str,
        currency: str,
        status: str = "Submitted",
        side: str = "SELL",
        order_type: str = "LMT",
        limit_price: str | None = None,
        stop_price: str | None = None,
    ) -> None:
        session = self.session_factory()
        try:
            instruction_record = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == instruction_id
                )
            ).scalar_one()
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="DU1234567",
                base_currency=currency,
            )
            session.add(broker_account)
            session.flush()
            session.add(
                BrokerOrderRecord(
                    instruction_id=instruction_record.id,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="DU1234567",
                    order_role="EXIT",
                    external_order_id=external_order_id,
                    external_perm_id=str(9000 + int(external_order_id)),
                    external_client_id="0",
                    order_ref=order_ref,
                    symbol=symbol,
                    exchange="SMART",
                    currency=currency,
                    security_type="STK",
                    primary_exchange="NASDAQ" if currency == "USD" else "SFB",
                    local_symbol=symbol,
                    side=side,
                    order_type=order_type,
                    time_in_force="DAY",
                    status=status,
                    total_quantity="1",
                    limit_price=limit_price,
                    stop_price=stop_price,
                    submitted_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
            )
            session.commit()
        finally:
            session.close()


__all__ = [name for name in globals() if not name.startswith("__")]
