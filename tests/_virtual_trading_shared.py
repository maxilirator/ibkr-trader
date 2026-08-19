from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from datetime import timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from sqlalchemy import select

from ibkr_trader.config import IbkrConnectionConfig
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
from ibkr_trader.domain.execution_payloads import parse_execution_batch_payload
from ibkr_trader.orchestration.runtime_worker import run_runtime_cycle
from ibkr_trader.orchestration.state_machine import ExecutionState
from ibkr_trader.orchestration.submission import submit_execution_batch
from ibkr_trader.virtual.accounts import BROKER_KIND_VIRTUAL
from ibkr_trader.virtual.execution import _new_virtual_order_id
from ibkr_trader.virtual.execution import _new_virtual_perm_id
from ibkr_trader.virtual.execution import ensure_virtual_account_record
from ibkr_trader.virtual.execution import persist_virtual_execution_fill
from ibkr_trader.virtual.execution import record_virtual_market_quote
from ibkr_trader.virtual.execution import record_virtual_market_quotes_from_stream_snapshot
from ibkr_trader.virtual.execution import submit_virtual_exit_order


def _write_schedule_fixture(schedule_path: Path) -> None:
    schedule_path.write_text(
        "\n".join(
            [
                "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                "2026-04-27,Europe/Stockholm,09:00,17:30,regular,base,override",
                "2026-04-28,Europe/Stockholm,09:00,17:30,regular,base,override",
            ]
        ),
        encoding="utf-8",
    )


def _virtual_payload() -> dict[str, object]:
    return {
        "schema_version": "2026-04-10",
        "source": {
            "system": "q-training",
            "batch_id": "virtual-smoke-1",
            "generated_at": "2026-04-27T06:55:00Z",
        },
        "instructions": [
            {
                "instruction_id": "virtual-sive-roundtrip-1",
                "account": {
                    "account_key": "virtual0001",
                    "book_key": "rl_virtual_book",
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
                    "submit_at": "2026-04-27T07:00:00Z",
                    "expire_at": "2026-04-27T15:30:00Z",
                    "limit_price": "10.50",
                    "time_in_force": "DAY",
                },
                "exit": {
                    "take_profit_pct": "0.10",
                },
                "trace": {
                    "reason_code": "virtual-smoke",
                },
            }
        ],
    }

class VirtualTradingTestsBase(TestCase):
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


__all__ = [name for name in globals() if not name.startswith("__")]
