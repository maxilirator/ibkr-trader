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
from ibkr_trader.virtual.execution import record_virtual_market_quote
from ibkr_trader.virtual.execution import record_virtual_market_quotes_from_stream_snapshot


def _write_schedule_fixture(schedule_path: Path) -> None:
    schedule_path.with_suffix(".csv").write_text(
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


class VirtualTradingTests(TestCase):
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

    def test_virtual_broker_ids_fit_instruction_columns(self) -> None:
        for _ in range(100):
            order_id = _new_virtual_order_id()
            perm_id = _new_virtual_perm_id(order_id)

            self.assertLessEqual(order_id, 2_147_483_647)
            self.assertLessEqual(perm_id, 2_147_483_647)
            self.assertGreater(order_id, 0)
            self.assertGreater(perm_id, 0)

    def test_virtual_trading_round_trip_uses_market_watch_and_fixed_fee(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            batch = parse_execution_batch_payload(_virtual_payload())

            record_virtual_market_quote(
                self.session_factory,
                account_key="virtual0001",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                last_price=Decimal("10.00"),
                ask_price=Decimal("10.00"),
                bid_price=Decimal("9.95"),
                observed_at=datetime(2026, 4, 27, 7, 0, tzinfo=timezone.utc),
                source="test",
            )
            submit_execution_batch(
                self.session_factory,
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )

            first_cycle = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 1, tzinfo=timezone.utc),
            )
            self.assertEqual(len(first_cycle.submitted_entries), 1)

            second_cycle = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 2, tzinfo=timezone.utc),
            )
            self.assertEqual(len(second_cycle.filled_entries), 1)
            self.assertEqual(len(second_cycle.submitted_exits), 1)

            quote_result = record_virtual_market_quote(
                self.session_factory,
                account_key="VIRTUAL0001",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                last_price=Decimal("11.50"),
                bid_price=Decimal("11.50"),
                ask_price=Decimal("11.55"),
                observed_at=datetime(2026, 4, 27, 7, 3, tzinfo=timezone.utc),
                source="test",
            )
            self.assertEqual(quote_result["filled_order_count"], 1)

            third_cycle = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 4, tzinfo=timezone.utc),
            )
            self.assertEqual(len(third_cycle.completed_instructions), 1)

        session = self.session_factory()
        try:
            instruction = session.execute(select(InstructionRecord)).scalar_one()
            broker_account = session.execute(select(BrokerAccountRecord)).scalar_one()
            orders = session.execute(
                select(BrokerOrderRecord).order_by(BrokerOrderRecord.id)
            ).scalars().all()
            fills = session.execute(
                select(ExecutionFillRecord).order_by(ExecutionFillRecord.id)
            ).scalars().all()
            positions = session.execute(
                select(PositionSnapshotRecord).order_by(PositionSnapshotRecord.id)
            ).scalars().all()
            account_snapshots = session.execute(
                select(AccountSnapshotRecord).order_by(AccountSnapshotRecord.id)
            ).scalars().all()

            self.assertEqual(instruction.state, ExecutionState.COMPLETED.value)
            self.assertTrue(instruction.is_virtual)
            self.assertEqual(broker_account.broker_kind, BROKER_KIND_VIRTUAL)
            self.assertTrue(broker_account.is_virtual)
            self.assertEqual(len(orders), 2)
            self.assertTrue(all(order.is_virtual for order in orders))
            self.assertEqual([order.order_role for order in orders], ["ENTRY", "EXIT"])
            self.assertEqual([order.status for order in orders], ["FILLED", "FILLED"])
            self.assertEqual(len(fills), 2)
            self.assertTrue(all(fill.is_virtual for fill in fills))
            self.assertEqual([fill.quantity for fill in fills], ["100", "100"])
            self.assertEqual([fill.commission for fill in fills], ["49", "49"])
            self.assertEqual([fill.commission_currency for fill in fills], ["SEK", "SEK"])
            self.assertEqual(positions[-1].quantity, "0")
            self.assertTrue(positions[-1].is_virtual)
            self.assertTrue(account_snapshots[-1].is_virtual)
            self.assertEqual(account_snapshots[-1].total_cash_value, "52.00")
            self.assertEqual(account_snapshots[-1].net_liquidation, "52.00")
        finally:
            session.close()

    def test_virtual_entry_target_notional_converts_to_whole_share_quantity(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            payload = deepcopy(_virtual_payload())
            instruction_payload = payload["instructions"][0]
            instruction_payload["sizing"] = {
                "mode": "target_notional",
                "target_notional": "1000",
            }
            batch = parse_execution_batch_payload(payload)

            record_virtual_market_quote(
                self.session_factory,
                account_key="virtual0001",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                last_price=Decimal("10.40"),
                ask_price=Decimal("10.40"),
                bid_price=Decimal("10.35"),
                observed_at=datetime(2026, 4, 27, 7, 0, tzinfo=timezone.utc),
                source="test",
            )
            submit_execution_batch(
                self.session_factory,
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )

            run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 1, tzinfo=timezone.utc),
            )
            run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 2, tzinfo=timezone.utc),
            )

        session = self.session_factory()
        try:
            order = session.execute(
                select(BrokerOrderRecord).where(BrokerOrderRecord.order_role == "ENTRY")
            ).scalar_one()
            fill = session.execute(select(ExecutionFillRecord)).scalar_one()
            self.assertEqual(order.total_quantity, "95")
            self.assertEqual(fill.quantity, "95")
            self.assertEqual(
                order.metadata_json["broker_submission"]["virtual_execution"]["sizing"][
                    "target_notional"
                ],
                "1000",
            )
        finally:
            session.close()

    def test_virtual_short_entry_does_not_credit_short_sale_proceeds_to_account_cash(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            ensure_virtual_account_record(
                self.session_factory,
                account_key="virtual0001",
                cash_balance=Decimal("200000"),
                snapshot_at=datetime(2026, 4, 27, 6, 55, tzinfo=timezone.utc),
            )
            payload = deepcopy(_virtual_payload())
            instruction_payload = payload["instructions"][0]
            instruction_payload["instruction_id"] = "virtual-sive-short-1"
            instruction_payload["intent"] = {
                "side": "SELL",
                "position_side": "SHORT",
            }
            instruction_payload["account"]["book_side"] = "SHORT"
            instruction_payload["sizing"] = {
                "mode": "target_notional",
                "target_notional": "1000",
            }
            batch = parse_execution_batch_payload(payload)

            record_virtual_market_quote(
                self.session_factory,
                account_key="virtual0001",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                last_price=Decimal("10.60"),
                ask_price=Decimal("10.65"),
                bid_price=Decimal("10.60"),
                observed_at=datetime(2026, 4, 27, 7, 0, tzinfo=timezone.utc),
                source="test",
            )
            submit_execution_batch(
                self.session_factory,
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )
            run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 1, tzinfo=timezone.utc),
            )
            run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 2, tzinfo=timezone.utc),
            )

        session = self.session_factory()
        try:
            fill = session.execute(select(ExecutionFillRecord)).scalar_one()
            snapshots = session.execute(
                select(AccountSnapshotRecord).order_by(AccountSnapshotRecord.id)
            ).scalars().all()
            self.assertEqual(fill.quantity, "95")
            self.assertEqual(snapshots[-1].total_cash_value, "199951")
            self.assertEqual(snapshots[-1].buying_power, "199951.00")
        finally:
            session.close()

    def test_virtual_quote_marks_open_positions_and_account_nav(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            ensure_virtual_account_record(
                self.session_factory,
                account_key="virtual0001",
                cash_balance=Decimal("200000"),
                snapshot_at=datetime(2026, 4, 27, 6, 55, tzinfo=timezone.utc),
            )
            batch = parse_execution_batch_payload(_virtual_payload())

            record_virtual_market_quote(
                self.session_factory,
                account_key="virtual0001",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                last_price=Decimal("10.00"),
                ask_price=Decimal("10.00"),
                bid_price=Decimal("9.95"),
                observed_at=datetime(2026, 4, 27, 7, 0, tzinfo=timezone.utc),
                source="test",
            )
            submit_execution_batch(
                self.session_factory,
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )
            run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 1, tzinfo=timezone.utc),
            )
            run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 2, tzinfo=timezone.utc),
            )

            mark_result = record_virtual_market_quote(
                self.session_factory,
                account_key="virtual0001",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                last_price=Decimal("10.50"),
                bid_price=Decimal("10.50"),
                ask_price=Decimal("10.55"),
                observed_at=datetime(2026, 4, 27, 7, 3, tzinfo=timezone.utc),
                source="test",
            )
            self.assertEqual(mark_result["filled_order_count"], 0)

        session = self.session_factory()
        try:
            position = session.execute(
                select(PositionSnapshotRecord)
                .where(PositionSnapshotRecord.symbol == "SIVE")
                .order_by(PositionSnapshotRecord.id.desc())
                .limit(1)
            ).scalar_one()
            account_snapshot = session.execute(
                select(AccountSnapshotRecord).order_by(AccountSnapshotRecord.id.desc()).limit(1)
            ).scalar_one()
            self.assertEqual(position.source, "virtual_market_mark")
            self.assertEqual(position.market_price, "10.50")
            self.assertEqual(position.unrealized_pnl, "50.00")
            self.assertEqual(account_snapshot.total_cash_value, "199951")
            self.assertEqual(account_snapshot.net_liquidation, "200001.00")
        finally:
            session.close()

    def test_virtual_orders_fill_from_stream_snapshot_when_limit_crosses(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            batch = parse_execution_batch_payload(_virtual_payload())
            submit_execution_batch(
                self.session_factory,
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )

            first_cycle = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 1, tzinfo=timezone.utc),
            )
            self.assertEqual(len(first_cycle.submitted_entries), 1)
            self.assertEqual(len(first_cycle.filled_entries), 0)

            entry_stream_snapshot = {
                "running": True,
                "quotes": [
                    {
                        "symbol": "SIVE",
                        "exchange": "SMART",
                        "currency": "SEK",
                        "security_type": "STK",
                        "primary_exchange": "SFB",
                        "bid_price": None,
                        "ask_price": None,
                        "last_price": None,
                        "updated_at": "2026-04-27T07:02:00Z",
                    }
                ],
                "bars_by_symbol": {
                    "SIVE": [
                        {
                            "timestamp": "2026-04-27T07:02:00Z",
                            "open": "10.70",
                            "high": "10.80",
                            "low": "10.40",
                            "close": "10.70",
                        }
                    ]
                },
            }

            second_cycle = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 2, tzinfo=timezone.utc),
                virtual_market_sync=lambda at: record_virtual_market_quotes_from_stream_snapshot(
                    self.session_factory,
                    stream_snapshot=entry_stream_snapshot,
                    observed_at=at,
                ),
            )
            self.assertEqual(len(second_cycle.filled_entries), 1)
            self.assertEqual(len(second_cycle.submitted_exits), 1)

            exit_stream_snapshot = {
                "running": True,
                "quotes": [
                    {
                        "symbol": "SIVE",
                        "exchange": "SMART",
                        "currency": "SEK",
                        "security_type": "STK",
                        "primary_exchange": "SFB",
                        "bid_price": None,
                        "ask_price": None,
                        "last_price": None,
                        "updated_at": "2026-04-27T07:03:00Z",
                    }
                ],
                "bars_by_symbol": {
                    "SIVE": [
                        {
                            "timestamp": "2026-04-27T07:03:00Z",
                            "open": "11.40",
                            "high": "11.60",
                            "low": "11.35",
                            "close": "11.45",
                        }
                    ]
                },
            }
            third_cycle = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 3, tzinfo=timezone.utc),
                virtual_market_sync=lambda at: record_virtual_market_quotes_from_stream_snapshot(
                    self.session_factory,
                    stream_snapshot=exit_stream_snapshot,
                    observed_at=at,
                ),
            )
            self.assertEqual(len(third_cycle.completed_instructions), 1)

        session = self.session_factory()
        try:
            instruction = session.execute(select(InstructionRecord)).scalar_one()
            orders = session.execute(
                select(BrokerOrderRecord).order_by(BrokerOrderRecord.id)
            ).scalars().all()
            fills = session.execute(
                select(ExecutionFillRecord).order_by(ExecutionFillRecord.id)
            ).scalars().all()

            self.assertEqual(instruction.state, ExecutionState.COMPLETED.value)
            self.assertEqual([order.status for order in orders], ["FILLED", "FILLED"])
            self.assertEqual([fill.price for fill in fills], ["10.50", "11.55"])
            self.assertTrue(
                all(
                    "STREAM_BAR" in fill.raw_payload.get("condition_code", "")
                    for fill in fills
                )
            )
        finally:
            session.close()

    def test_virtual_limit_fill_uses_quote_price_when_market_is_better_than_limit(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            payload = _virtual_payload()
            payload["instructions"][0]["entry"]["limit_price"] = "55.6802"  # type: ignore[index]
            payload["instructions"][0]["sizing"] = {  # type: ignore[index]
                "mode": "target_quantity",
                "target_quantity": "38",
            }
            batch = parse_execution_batch_payload(payload)
            submit_execution_batch(
                self.session_factory,
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )

            first_cycle = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 1, tzinfo=timezone.utc),
            )
            self.assertEqual(len(first_cycle.submitted_entries), 1)
            self.assertEqual(len(first_cycle.filled_entries), 0)

            record_virtual_market_quote(
                self.session_factory,
                account_key="virtual0001",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                bid_price=Decimal("51.95"),
                ask_price=Decimal("52.05"),
                last_price=Decimal("51.95"),
                observed_at=datetime(2026, 4, 27, 7, 2, tzinfo=timezone.utc),
                source="ibkr_live_market_stream_virtual_bridge",
                raw_payload={
                    "latest_stream_bar": {
                        "timestamp": "2026-04-27T07:02:00Z",
                        "open": "52.10",
                        "high": "52.20",
                        "low": "51.95",
                        "close": "51.95",
                    }
                },
            )

        session = self.session_factory()
        try:
            fill = session.execute(select(ExecutionFillRecord)).scalar_one()
            self.assertEqual(fill.price, "52.05")
            self.assertEqual(
                fill.raw_payload["condition_code"],
                "BUY_LIMIT_MET:STREAM_BAR_LOW",
            )
        finally:
            session.close()
