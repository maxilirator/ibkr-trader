from __future__ import annotations

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
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.ibkr.runtime_snapshot import BrokerExecution
from ibkr_trader.ibkr.runtime_snapshot import BrokerOpenOrder
from ibkr_trader.ibkr.runtime_snapshot import BrokerRuntimeSnapshot
from ibkr_trader.orchestration.runtime_worker import run_runtime_cycle
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


class RuntimeWorkerTests(TestCase):
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
    ) -> None:
        session = self.session_factory()
        try:
            session.add(
                InstructionRecord(
                    instruction_id=instruction_id,
                    schema_version="2026-04-10",
                    source_system="q-training",
                    batch_id="batch-1",
                    account_key="GTW05",
                    book_key="long_risk_book",
                    symbol=symbol,
                    exchange=exchange,
                    currency=currency,
                    state=state,
                    submit_at=submit_at,
                    expire_at=expire_at,
                    order_type="LIMIT",
                    side="BUY",
                    broker_order_id=broker_order_id,
                    exit_order_id=exit_order_id,
                    entry_filled_quantity=entry_filled_quantity,
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

    def test_run_runtime_cycle_submits_due_entry(self) -> None:
        payload = _aapl_payload()
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
        )

        def fake_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            self.assertEqual(broker_config.client_id, 0)
            self.assertEqual(instruction.instruction_id, "runtime-aapl-1")
            self.assertEqual(timeout, 10)
            return {
                "instruction_id": "runtime-aapl-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 265598, "symbol": "AAPL"},
                "order": {
                    "order_ref": "runtime-aapl-1",
                    "action": "BUY",
                    "order_type": "LMT",
                    "time_in_force": "DAY",
                    "limit_price": "200.00",
                    "total_quantity": "1",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 11,
                    "status": "PreSubmitted",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 8001,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                },
            }

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            entry_submitter=fake_submitter,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
            ),
        )

        self.assertEqual(len(result.submitted_entries), 1)
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.ENTRY_SUBMITTED.value)
        self.assertEqual(record.broker_order_id, 11)
        self.assertEqual(record.entry_submitted_quantity, "1")

    def test_run_runtime_cycle_can_target_selected_instruction_ids(self) -> None:
        payload = _aapl_payload()
        other_payload = _aapl_payload()
        other_payload["instruction"]["instruction_id"] = "runtime-aapl-2"
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
        )
        self._insert_instruction(
            instruction_id="runtime-aapl-2",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=other_payload,
        )

        submitted_ids: list[str] = []

        def fake_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            submitted_ids.append(instruction.instruction_id)
            return {
                "instruction_id": instruction.instruction_id,
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 265598, "symbol": "AAPL"},
                "order": {
                    "order_ref": instruction.instruction_id,
                    "action": "BUY",
                    "order_type": "LMT",
                    "time_in_force": "DAY",
                    "limit_price": "200.00",
                    "total_quantity": "1",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 11 if instruction.instruction_id == "runtime-aapl-1" else 12,
                    "status": "PreSubmitted",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 8001,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                },
            }

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            instruction_ids=("runtime-aapl-2",),
            entry_submitter=fake_submitter,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
            ),
        )

        self.assertEqual([action.instruction_id for action in result.submitted_entries], ["runtime-aapl-2"])
        self.assertEqual(submitted_ids, ["runtime-aapl-2"])
        self.assertEqual(
            self._read_record("runtime-aapl-1").state,
            ExecutionState.ENTRY_PENDING.value,
        )
        self.assertEqual(
            self._read_record("runtime-aapl-2").state,
            ExecutionState.ENTRY_SUBMITTED.value,
        )

    def test_run_runtime_cycle_retries_transient_entry_submit_connection_error(self) -> None:
        payload = _aapl_payload()
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
        )

        attempts = {"count": 0}
        sleep_calls: list[float] = []

        def flaky_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise ConnectionError(
                    "Failed to connect to IBKR at 127.0.0.1:7497 with client_id=0."
                )
            return {
                "instruction_id": instruction.instruction_id,
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 265598, "symbol": "AAPL"},
                "order": {
                    "order_ref": instruction.instruction_id,
                    "action": "BUY",
                    "order_type": "LMT",
                    "time_in_force": "DAY",
                    "limit_price": "200.00",
                    "total_quantity": "1",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 11,
                    "status": "PreSubmitted",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 8001,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                },
            }

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            entry_submitter=flaky_submitter,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
            ),
            broker_retry_delays=(0.25,),
            sleep_fn=sleep_calls.append,
        )

        self.assertEqual(attempts["count"], 2)
        self.assertEqual(sleep_calls, [0.25])
        self.assertEqual(len(result.submitted_entries), 1)
        self.assertEqual(
            self._read_record("runtime-aapl-1").state,
            ExecutionState.ENTRY_SUBMITTED.value,
        )

    def test_run_runtime_cycle_reconciles_entry_fill_and_submits_take_profit(self) -> None:
        payload = _aapl_payload()
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_SUBMITTED.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
            broker_order_id=11,
        )

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: object,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: object = None,
            stop_price: object = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            self.assertEqual(order_ref, "runtime-aapl-1:exit:take_profit")
            self.assertEqual(str(quantity), "1")
            self.assertEqual(str(limit_price), "204.00")
            self.assertIsNone(stop_price)
            self.assertIsNone(oca_group)
            self.assertIsNone(oca_type)
            return {
                "instruction_id": "runtime-aapl-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 265598, "symbol": "AAPL"},
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": "LMT",
                    "time_in_force": "DAY",
                    "limit_price": "204.00",
                    "total_quantity": "1",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 21,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 9001,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                },
            }

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 20, 5, tzinfo=timezone.utc),
            exit_submitter=fake_exit_submitter,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(
                    BrokerExecution(
                        exec_id="E-1",
                        order_id=11,
                        perm_id=8001,
                        client_id=0,
                        order_ref="runtime-aapl-1",
                        side="BOT",
                        shares="1",
                        price="200.00",
                        exchange="NASDAQ",
                        executed_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                        symbol="AAPL",
                    ),
                ),
            ),
        )

        self.assertEqual(len(result.filled_entries), 1)
        self.assertEqual(len(result.submitted_exits), 1)
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.entry_filled_quantity, "1")
        self.assertEqual(record.entry_avg_fill_price, "200.00")
        self.assertEqual(record.exit_order_id, 21)
        self.assertEqual(record.exit_submitted_quantity, "1")

    def test_run_runtime_cycle_submits_take_profit_and_catastrophic_stop(self) -> None:
        payload = _aapl_payload()
        payload["instruction"]["exit"]["catastrophic_stop_loss_pct"] = "0.15"
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_SUBMITTED.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
            broker_order_id=11,
        )

        calls: list[dict[str, object]] = []

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: object,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: object = None,
            stop_price: object = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            calls.append(
                {
                    "order_ref": order_ref,
                    "order_type": order_type,
                    "limit_price": limit_price,
                    "stop_price": stop_price,
                    "oca_group": oca_group,
                    "oca_type": oca_type,
                }
            )
            order_id = 21 if order_ref.endswith("take_profit") else 22
            order_type_code = "LMT" if order_ref.endswith("take_profit") else "STP"
            return {
                "instruction_id": "runtime-aapl-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 265598, "symbol": "AAPL"},
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": order_type_code,
                    "time_in_force": "DAY",
                    "limit_price": (
                        str(limit_price) if limit_price is not None else None
                    ),
                    "stop_price": (
                        str(stop_price) if stop_price is not None else None
                    ),
                    "total_quantity": "1",
                    "outside_rth": False,
                    "oca_group": oca_group,
                    "oca_type": oca_type,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": order_id,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 9000 + order_id,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                },
            }

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 20, 5, tzinfo=timezone.utc),
            exit_submitter=fake_exit_submitter,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(
                    BrokerExecution(
                        exec_id="E-1",
                        order_id=11,
                        perm_id=8001,
                        client_id=0,
                        order_ref="runtime-aapl-1",
                        side="BOT",
                        shares="1",
                        price="200.00",
                        exchange="NASDAQ",
                        executed_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                        symbol="AAPL",
                    ),
                ),
            ),
        )

        self.assertEqual(len(result.filled_entries), 1)
        self.assertEqual(len(result.submitted_exits), 2)
        self.assertEqual(
            [call["order_ref"] for call in calls],
            [
                "runtime-aapl-1:exit:take_profit",
                "runtime-aapl-1:exit:catastrophic_stop",
            ],
        )
        self.assertEqual(calls[0]["limit_price"], Decimal("204.00"))
        self.assertEqual(calls[1]["stop_price"], Decimal("170.00"))
        self.assertEqual(calls[0]["oca_group"], "runtime-aapl-1:exit:oca")
        self.assertEqual(calls[1]["oca_group"], "runtime-aapl-1:exit:oca")
        self.assertEqual(calls[0]["oca_type"], 1)
        self.assertEqual(calls[1]["oca_type"], 1)
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.exit_order_id, 21)
        self.assertEqual(record.exit_submitted_quantity, "1")

    def test_run_runtime_cycle_submits_forced_exit_when_next_session_is_due(self) -> None:
        payload = _sive_payload()
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.POSITION_OPEN.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            entry_filled_quantity="100",
        )

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: object,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: object = None,
            stop_price: object = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            self.assertEqual(order_ref, "runtime-sive-1:exit:forced")
            self.assertEqual(str(quantity), "100")
            self.assertIsNone(limit_price)
            self.assertIsNone(stop_price)
            return {
                "instruction_id": "runtime-sive-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 489000, "symbol": "SIVE"},
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": "MKT",
                    "time_in_force": "DAY",
                    "limit_price": None,
                    "total_quantity": "100",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 31,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": "100",
                    "avgFillPrice": 0.0,
                    "permId": 9101,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                },
            }

        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            schedule_path.with_suffix(".csv").write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
                        "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
                    ]
                ),
                encoding="utf-8",
            )

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 13, 7, 1, tzinfo=timezone.utc),
                exit_submitter=fake_exit_submitter,
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={},
                    executions=(),
                ),
            )

        self.assertEqual(len(result.submitted_exits), 1)
        record = self._read_record("runtime-sive-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.exit_order_id, 31)
        self.assertEqual(record.exit_submitted_quantity, "100")

    def test_run_runtime_cycle_cancels_all_open_exit_orders_before_forced_exit(self) -> None:
        payload = _sive_payload()
        payload["instruction"]["exit"]["take_profit_pct"] = "0.02"
        payload["instruction"]["exit"]["catastrophic_stop_loss_pct"] = "0.15"
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            entry_filled_quantity="100",
            exit_order_id=21,
        )

        cancelled_ids: list[int] = []

        def fake_canceler(
            broker_config: IbkrConnectionConfig,
            order_id: int,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            cancelled_ids.append(order_id)
            return {"broker_order_status": {"orderId": order_id, "status": "Cancelled"}}

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: object,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: object = None,
            stop_price: object = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            self.assertEqual(order_ref, "runtime-sive-1:exit:forced")
            return {
                "instruction_id": "runtime-sive-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 489000, "symbol": "SIVE"},
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": "MKT",
                    "time_in_force": "DAY",
                    "limit_price": None,
                    "total_quantity": "100",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 31,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": "100",
                    "avgFillPrice": 0.0,
                    "permId": 9101,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                },
            }

        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            schedule_path.with_suffix(".csv").write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
                        "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
                    ]
                ),
                encoding="utf-8",
            )

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 13, 7, 1, tzinfo=timezone.utc),
                exit_submitter=fake_exit_submitter,
                broker_order_canceler=fake_canceler,
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={
                        21: BrokerOpenOrder(
                            order_id=21,
                            perm_id=9001,
                            client_id=0,
                            status="Submitted",
                            order_ref="runtime-sive-1:exit:take_profit",
                            action="SELL",
                            total_quantity="100",
                            symbol="SIVE",
                        ),
                        22: BrokerOpenOrder(
                            order_id=22,
                            perm_id=9002,
                            client_id=0,
                            status="Submitted",
                            order_ref="runtime-sive-1:exit:catastrophic_stop",
                            action="SELL",
                            total_quantity="100",
                            symbol="SIVE",
                        ),
                    },
                    executions=(),
                ),
            )

        self.assertEqual(cancelled_ids, [21, 22])
        self.assertEqual(len(result.submitted_exits), 1)

    def test_run_runtime_cycle_completes_instruction_after_exit_fill(self) -> None:
        payload = _aapl_payload()
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
            entry_filled_quantity="1",
            exit_order_id=21,
        )

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 20, 10, tzinfo=timezone.utc),
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(
                    BrokerExecution(
                        exec_id="E-2",
                        order_id=21,
                        perm_id=9001,
                        client_id=0,
                        order_ref="runtime-aapl-1:exit:take_profit",
                        side="SLD",
                        shares="1",
                        price="204.00",
                        exchange="NASDAQ",
                        executed_at=datetime(2026, 4, 10, 20, 9, tzinfo=timezone.utc),
                        symbol="AAPL",
                    ),
                ),
            ),
        )

        self.assertEqual(len(result.completed_instructions), 1)
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.COMPLETED.value)
        self.assertEqual(record.exit_filled_quantity, "1")
        self.assertEqual(record.exit_avg_fill_price, "204.00")

    def test_run_runtime_cycle_marks_expired_unfilled_entry_cancelled(self) -> None:
        payload = _aapl_payload()
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_SUBMITTED.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
            broker_order_id=11,
        )

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 20, 10, tzinfo=timezone.utc),
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
            ),
        )

        self.assertEqual(len(result.cancelled_entries), 1)
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.ENTRY_CANCELLED.value)
