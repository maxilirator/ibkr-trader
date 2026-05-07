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

    def test_runtime_broker_operations_keep_normal_cycle_snapshot_light(self) -> None:
        recorded_operations: list[str] = []

        class _FakePrimary:
            def execute(self, operation_name: str, fn: object) -> object:
                recorded_operations.append(operation_name)
                return fn(object())

        class _FakeSessions:
            primary = _FakePrimary()

        with patch(
            "ibkr_trader.orchestration.runtime_worker.fetch_broker_runtime_snapshot",
            return_value=BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        ) as snapshot_fetch:
            broker_ops = _build_runtime_broker_operations(_FakeSessions())
            broker_ops.fetch_snapshot(self.config, timeout=17)

        self.assertEqual(recorded_operations, ["runtime_snapshot"])
        self.assertEqual(snapshot_fetch.call_args.kwargs["timeout"], 17)
        self.assertFalse(snapshot_fetch.call_args.kwargs["include_open_orders"])
        self.assertFalse(snapshot_fetch.call_args.kwargs["include_executions"])
        self.assertFalse(snapshot_fetch.call_args.kwargs["include_account_updates"])
        self.assertFalse(snapshot_fetch.call_args.kwargs["include_positions"])

    def test_runtime_broker_operations_use_rich_snapshot_for_reconciliation(self) -> None:
        recorded_operations: list[str] = []

        class _FakePrimary:
            def execute(self, operation_name: str, fn: object) -> object:
                recorded_operations.append(operation_name)
                return fn(object())

        class _FakeSessions:
            primary = _FakePrimary()

        with patch(
            "ibkr_trader.orchestration.runtime_worker.fetch_broker_runtime_snapshot",
            return_value=BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        ) as snapshot_fetch:
            broker_ops = _build_runtime_broker_operations(_FakeSessions())
            broker_ops.fetch_reconciliation_snapshot(self.config, timeout=23)

        self.assertEqual(recorded_operations, ["runtime_reconciliation_snapshot"])
        self.assertEqual(snapshot_fetch.call_args.kwargs["timeout"], 23)
        self.assertTrue(snapshot_fetch.call_args.kwargs["include_open_orders"])
        self.assertTrue(snapshot_fetch.call_args.kwargs["include_executions"])
        self.assertFalse(snapshot_fetch.call_args.kwargs["include_account_updates"])
        self.assertTrue(snapshot_fetch.call_args.kwargs["include_positions"])

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
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.submitted_entries), 1)
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.ENTRY_SUBMITTED.value)
        self.assertEqual(record.broker_order_id, 11)
        self.assertEqual(record.entry_submitted_quantity, "1")

    def test_submit_due_pending_entries_skips_stale_already_submitted_entry(self) -> None:
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_SUBMITTED.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=_aapl_payload(),
            broker_order_id=11,
        )
        submitted_entries = []
        cancelled_entries = []
        issues = []

        _submit_due_pending_entries(
            self.session_factory,
            self.config,
            due_instruction_ids=["runtime-aapl-1"],
            cycle_started_at=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            timeout=10,
            kill_switch_enabled=False,
            entry_submitter=lambda *args, **kwargs: self.fail(
                "stale already-submitted entries must not be submitted again"
            ),
            broker_retry_delays=(),
            sleep_fn=lambda seconds: None,
            submitted_entries=submitted_entries,
            cancelled_entries=cancelled_entries,
            issues=issues,
        )

        self.assertEqual(submitted_entries, [])
        self.assertEqual(cancelled_entries, [])
        self.assertEqual(issues, [])
        session = self.session_factory()
        try:
            event = session.execute(
                select(InstructionEventRecord).where(
                    InstructionEventRecord.event_type
                    == "runtime_entry_submit_skipped"
                )
            ).scalar_one()
            self.assertEqual(event.state_before, ExecutionState.ENTRY_SUBMITTED.value)
        finally:
            session.close()

    def test_run_runtime_cycle_submits_due_virtual_entry_with_active_real_work(self) -> None:
        real_payload = _sive_payload()
        virtual_payload = _aapl_payload()
        virtual_payload["instruction"]["instruction_id"] = "runtime-virtual-aapl-1"
        virtual_payload["instruction"]["account"]["account_key"] = "virtualrl01"
        virtual_payload["instruction"]["account"]["book_key"] = "rl_virtual_long"
        self._insert_instruction(
            instruction_id="runtime-sive-real-open-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.POSITION_OPEN.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=real_payload,
            broker_order_id=1001,
            entry_filled_quantity="100",
            entry_avg_fill_price="10.00",
            account_key="GTW05",
        )
        self._insert_instruction(
            instruction_id="runtime-virtual-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=virtual_payload,
            account_key="virtualrl01",
            book_key="rl_virtual_long",
            is_virtual=True,
        )

        submitted: list[str] = []

        def fake_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            del broker_config, timeout
            submitted.append(instruction.instruction_id)
            return {
                "instruction_id": instruction.instruction_id,
                "account": "virtualrl01",
                "is_virtual": True,
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
                    "transmit": False,
                    "is_virtual": True,
                },
                "broker_order_status": {
                    "orderId": 90001,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 990001,
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
            broker_snapshot_fetcher=lambda *args, **kwargs: (_ for _ in ()).throw(
                ConnectionError("gateway down")
            ),
        )

        self.assertEqual(submitted, ["runtime-virtual-aapl-1"])
        self.assertEqual(len(result.submitted_entries), 1)
        self.assertTrue(any(issue.stage == "broker_snapshot" for issue in result.issues))
        self.assertEqual(
            self._read_record("runtime-virtual-aapl-1").state,
            ExecutionState.ENTRY_SUBMITTED.value,
        )
        self.assertEqual(
            self._read_record("runtime-sive-real-open-1").state,
            ExecutionState.POSITION_OPEN.value,
        )

    def test_run_runtime_cycle_cancels_expired_pending_entry_before_submit(self) -> None:
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

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 20, 5, tzinfo=timezone.utc),
            entry_submitter=lambda *args, **kwargs: self.fail(
                "expired pending entries must not be submitted"
            ),
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(result.submitted_entries, ())
        self.assertEqual(len(result.cancelled_entries), 1)
        self.assertEqual(
            self._read_record("runtime-aapl-1").state,
            ExecutionState.ENTRY_CANCELLED.value,
        )

    def test_run_runtime_cycle_uses_session_close_as_effective_pending_expiry(self) -> None:
        payload = _sive_payload()
        payload["instruction"]["entry"]["submit_at"] = "2026-04-30T09:25:00+02:00"
        payload["instruction"]["entry"]["expire_at"] = "2026-04-30T17:30:00+02:00"
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 30, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 30, 15, 30, tzinfo=timezone.utc),
            payload=payload,
        )

        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            schedule_path.with_suffix(".csv").write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-30,Europe/Stockholm,09:00,13:00,override,base,override",
                    ]
                ),
                encoding="utf-8",
            )

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 30, 11, 11, tzinfo=timezone.utc),
                entry_submitter=lambda *args, **kwargs: self.fail(
                    "entries past the exchange session close must be cancelled"
                ),
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={},
                    executions=(),
                    portfolio=(),
                    positions=(),
                    account_values={},
                ),
            )

        self.assertEqual(len(result.cancelled_entries), 1)
        record = self._read_record("runtime-sive-1")
        self.assertEqual(record.state, ExecutionState.ENTRY_CANCELLED.value)
        self.assertIsNone(record.broker_order_id)

    def test_run_runtime_cycle_marks_terminal_due_entry_submit_failure(self) -> None:
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

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            entry_submitter=lambda *args, **kwargs: (_ for _ in ()).throw(
                ValueError("insufficient funds")
            ),
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.issues), 1)
        self.assertEqual(result.issues[0].stage, "entry_submit")
        self.assertEqual(
            self._read_record("runtime-aapl-1").state,
            ExecutionState.FAILED.value,
        )

    def test_run_runtime_cycle_submits_stockholm_open_entry_one_minute_early(self) -> None:
        payload = _sive_payload()
        payload["instruction"]["entry"]["submit_at"] = "2026-04-10T09:00:00+02:00"
        payload["instruction"]["entry"]["expire_at"] = "2026-04-10T10:00:00+02:00"
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 7, 0, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 8, 0, tzinfo=timezone.utc),
            payload=payload,
        )

        submit_calls: list[str] = []

        def fake_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            submit_calls.append(instruction.instruction_id)
            return {
                "instruction_id": "runtime-sive-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 489000, "symbol": "SIVE"},
                "order": {
                    "order_ref": "runtime-sive-1",
                    "action": "BUY",
                    "order_type": "LMT",
                    "time_in_force": "DAY",
                    "limit_price": "11.3131",
                    "total_quantity": "100",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 11,
                    "status": "PreSubmitted",
                    "filled": "0",
                    "remaining": "100",
                    "avgFillPrice": 0.0,
                    "permId": 8001,
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
                    ]
                ),
                encoding="utf-8",
            )

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 10, 6, 59, tzinfo=timezone.utc),
                entry_submitter=fake_submitter,
                submission_lead_time=timedelta(minutes=1),
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={},
                    executions=(),
                    portfolio=(),
                    positions=(),
                    account_values={},
                ),
            )

        self.assertEqual(submit_calls, ["runtime-sive-1"])
        self.assertEqual(len(result.submitted_entries), 1)

    def test_run_runtime_cycle_submits_stockholm_close_entry_one_minute_early(self) -> None:
        payload = _sive_payload()
        payload["instruction"]["entry"]["submit_at"] = "2026-04-10T17:30:00+02:00"
        payload["instruction"]["entry"]["expire_at"] = "2026-04-10T17:31:00+02:00"
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 31, tzinfo=timezone.utc),
            payload=payload,
        )

        submit_calls: list[str] = []

        def fake_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            submit_calls.append(instruction.instruction_id)
            return {
                "instruction_id": "runtime-sive-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 489000, "symbol": "SIVE"},
                "order": {
                    "order_ref": "runtime-sive-1",
                    "action": "BUY",
                    "order_type": "LMT",
                    "time_in_force": "DAY",
                    "limit_price": "11.3131",
                    "total_quantity": "100",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 11,
                    "status": "PreSubmitted",
                    "filled": "0",
                    "remaining": "100",
                    "avgFillPrice": 0.0,
                    "permId": 8001,
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
                    ]
                ),
                encoding="utf-8",
            )

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 10, 15, 29, tzinfo=timezone.utc),
                entry_submitter=fake_submitter,
                submission_lead_time=timedelta(minutes=1),
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={},
                    executions=(),
                    portfolio=(),
                    positions=(),
                    account_values={},
                ),
            )

        self.assertEqual(submit_calls, ["runtime-sive-1"])
        self.assertEqual(len(result.submitted_entries), 1)

    def test_run_runtime_cycle_skips_due_entry_when_kill_switch_is_enabled(self) -> None:
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
        set_kill_switch_state(
            self.session_factory,
            enabled=True,
            reason="Freeze new entries.",
            updated_by="test",
        )

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            entry_submitter=lambda *args, **kwargs: self.fail(
                "entry submitter should not be called while kill switch is enabled"
            ),
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(result.submitted_entries, ())
        self.assertEqual(len(result.issues), 1)
        self.assertEqual(result.issues[0].stage, "kill_switch")
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.ENTRY_PENDING.value)

    def test_run_runtime_cycle_cancels_open_entry_when_kill_switch_is_enabled(self) -> None:
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
        set_kill_switch_state(
            self.session_factory,
            enabled=True,
            reason="Freeze new entries.",
            updated_by="test",
        )

        def fake_canceler(
            broker_config: IbkrConnectionConfig,
            order_id: int,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            self.assertEqual(order_id, 11)
            return {
                "broker_order_status": {
                    "orderId": 11,
                    "status": "Cancelled",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 8001,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                }
            }

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            entry_canceler=fake_canceler,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={
                    11: BrokerOpenOrder(
                        order_id=11,
                        perm_id=8001,
                        client_id=0,
                        status="Submitted",
                        order_ref="runtime-aapl-1",
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
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.cancelled_entries), 1)
        self.assertEqual(
            result.cancelled_entries[0].action,
            "entry_cancelled_by_kill_switch",
        )
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.ENTRY_CANCELLED.value)
        self.assertEqual(record.broker_order_status, "Cancelled")

    def test_run_runtime_cycle_persists_runtime_snapshot_to_ledger(self) -> None:
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
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={
                    11: BrokerOpenOrder(
                        order_id=11,
                        perm_id=8001,
                        client_id=0,
                        status="Submitted",
                        order_ref="runtime-aapl-1",
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
                executions=(),
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
                        market_price=Decimal("201.00"),
                        market_value=Decimal("201.00"),
                        average_cost=Decimal("200.00"),
                        unrealized_pnl=Decimal("1.00"),
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
                    }
                },
            ),
        )

        self.assertEqual(result.issues, ())
        session = self.session_factory()
        try:
            self.assertEqual(
                len(session.execute(select(AccountSnapshotRecord)).scalars().all()),
                1,
            )
            self.assertEqual(
                len(session.execute(select(PositionSnapshotRecord)).scalars().all()),
                1,
            )
            broker_order = session.execute(select(BrokerOrderRecord)).scalar_one()
            self.assertEqual(broker_order.external_order_id, "11")
            self.assertEqual(broker_order.status, "Submitted")
        finally:
            session.close()

    def test_run_runtime_cycle_persists_callback_events_before_reconciliation(self) -> None:
        self._insert_broker_order(external_order_id="11", status="PreSubmitted")

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            broker_callback_fetcher=lambda: [
                {
                    "event_type": "order_status",
                    "event_at": datetime(2026, 4, 10, 19, 55, 30, tzinfo=timezone.utc),
                    "order_status": {
                        "orderId": 11,
                        "status": "Submitted",
                        "filled": "0",
                        "remaining": "1",
                        "avgFillPrice": "0.0",
                        "permId": 8001,
                        "parentId": 0,
                        "lastFillPrice": "0.0",
                        "clientId": 0,
                        "whyHeld": "",
                        "mktCapPrice": "0.0",
                    },
                }
            ],
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(result.issues, ())
        session = self.session_factory()
        try:
            broker_order = session.execute(select(BrokerOrderRecord)).scalar_one()
            self.assertEqual(broker_order.status, "Submitted")
        finally:
            session.close()

    def test_run_runtime_cycle_persists_reconciliation_run_summary(self) -> None:
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
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.submitted_entries), 1)
        runs = self._read_reconciliation_runs()
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0].run_kind, "runtime_cycle")
        self.assertEqual(runs[0].status, "CLEAN")
        self.assertEqual(runs[0].issue_count, 0)
        self.assertEqual(runs[0].action_count, 1)
        self.assertEqual(runs[0].metadata_json["due_instruction_count"], 1)
        self.assertEqual(runs[0].metadata_json["active_instruction_count"], 0)

    def test_run_runtime_cycle_persists_reconciliation_issues_on_early_return(self) -> None:
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
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            broker_snapshot_fetcher=lambda *args, **kwargs: (_ for _ in ()).throw(
                RuntimeError("snapshot down")
            ),
        )

        self.assertEqual(len(result.issues), 1)
        self.assertEqual(result.issues[0].stage, "broker_snapshot")

        session = self.session_factory()
        try:
            run = session.execute(select(ReconciliationRunRecord)).scalar_one()
            issue = session.execute(select(ReconciliationIssueRecord)).scalar_one()
            self.assertEqual(run.status, "WARNINGS")
            self.assertEqual(run.issue_count, 1)
            self.assertEqual(issue.stage, "broker_snapshot")
            self.assertEqual(issue.message, "snapshot down")
        finally:
            session.close()

    def test_run_runtime_cycle_skips_routine_broker_polling_after_session_close(self) -> None:
        payload = _sive_payload()
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            broker_order_id=11,
            entry_filled_quantity="80",
            entry_avg_fill_price="146.90",
        )

        with TemporaryDirectory() as tmpdir:
            calendar_path = Path(tmpdir) / "sessions.csv"
            calendar_path.write_text(
                "\n".join(
                    (
                        "session_date,timezone,open_time,close_time,session_kind",
                        "2026-04-10,Europe/Stockholm,09:00:00,17:30:00,regular",
                    )
                ),
                encoding="utf-8",
            )

            def fail_if_called(*args: object, **kwargs: object) -> BrokerRuntimeSnapshot:
                raise AssertionError("broker snapshot should not be fetched after close")

            def callbacks_fail_if_called() -> list[dict[str, object]]:
                raise AssertionError("broker callbacks should not be drained after close")

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=calendar_path,
                now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
                broker_snapshot_fetcher=fail_if_called,
                broker_callback_fetcher=callbacks_fail_if_called,
                virtual_market_sync=lambda at: (_ for _ in ()).throw(
                    AssertionError("virtual market sync should not run after close")
                ),
                exit_submitter=lambda *args, **kwargs: (_ for _ in ()).throw(
                    AssertionError("active exits should not be reconciled after close")
                ),
            )

        self.assertEqual(result.issues, ())
        self.assertEqual(self._read_reconciliation_runs()[0].status, "CLEAN")

    def test_run_startup_reconciliation_skips_active_scan_after_session_close(self) -> None:
        payload = _sive_payload()
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            broker_order_id=11,
            entry_filled_quantity="80",
            entry_avg_fill_price="146.90",
        )

        with TemporaryDirectory() as tmpdir:
            calendar_path = Path(tmpdir) / "sessions.csv"
            calendar_path.write_text(
                "\n".join(
                    (
                        "session_date,timezone,open_time,close_time,session_kind",
                        "2026-04-10,Europe/Stockholm,09:00:00,17:30:00,regular",
                    )
                ),
                encoding="utf-8",
            )

            result = run_startup_reconciliation(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=calendar_path,
                now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
                broker_snapshot_fetcher=lambda *args, **kwargs: (_ for _ in ()).throw(
                    AssertionError("startup should not fetch broker snapshot after close")
                ),
                broker_callback_fetcher=lambda: (_ for _ in ()).throw(
                    AssertionError("startup should not drain callbacks after close")
                ),
                virtual_market_sync=lambda at: (_ for _ in ()).throw(
                    AssertionError("startup should not sync virtual market after close")
                ),
                exit_submitter=lambda *args, **kwargs: (_ for _ in ()).throw(
                    AssertionError("startup should not reconcile active exits after close")
                ),
            )

        self.assertEqual(result.issues, ())
        self.assertEqual(self._read_reconciliation_runs()[0].status, "CLEAN")

    def test_run_runtime_cycle_suppresses_repeated_broker_outage_audits(self) -> None:
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

        def failing_snapshot(attempt: int):
            def _raise(*args: object, **kwargs: object) -> BrokerRuntimeSnapshot:
                raise ConnectionError(
                    "Broker session 'primary' is cooling down after "
                    f"{attempt} failed broker attempt(s); next retry at "
                    f"2026-04-10T19:{56 + attempt:02d}:00Z. Last error: "
                    "Failed to connect to IBKR at 127.0.0.1:4002 with client_id=0"
                )

            return _raise

        first = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            broker_snapshot_fetcher=failing_snapshot(1),
        )
        second = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 19, 57, tzinfo=timezone.utc),
            broker_snapshot_fetcher=failing_snapshot(2),
        )

        self.assertEqual(len(first.issues), 1)
        self.assertEqual(len(second.issues), 1)

        session = self.session_factory()
        try:
            runs = list(session.execute(select(ReconciliationRunRecord)).scalars())
            issues = list(session.execute(select(ReconciliationIssueRecord)).scalars())
            self.assertEqual(len(runs), 1)
            self.assertEqual(len(issues), 1)
            self.assertEqual(runs[0].issue_count, 1)
            self.assertEqual(
                runs[0].metadata_json["suppressed_reconciliation_repeats"],
                1,
            )
            self.assertEqual(
                runs[0].metadata_json["last_suppressed_active_instruction_count"],
                1,
            )
        finally:
            session.close()

    def test_run_runtime_cycle_records_new_broker_outage_after_cooldown(self) -> None:
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

        def fail_snapshot(*args: object, **kwargs: object) -> BrokerRuntimeSnapshot:
            raise ConnectionError(
                "No response received for current_time request 0 within 5 seconds"
            )

        run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2030, 4, 10, 19, 56, tzinfo=timezone.utc),
            broker_snapshot_fetcher=fail_snapshot,
        )
        run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2030, 4, 10, 20, 7, tzinfo=timezone.utc),
            broker_snapshot_fetcher=fail_snapshot,
        )

        self.assertEqual(len(self._read_reconciliation_runs()), 2)

    def test_run_startup_reconciliation_skips_due_entry_submission(self) -> None:
        pending_payload = _aapl_payload()
        active_payload = _aapl_payload()
        active_payload["instruction"]["instruction_id"] = "runtime-aapl-2"
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=pending_payload,
        )
        self._insert_instruction(
            instruction_id="runtime-aapl-2",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_SUBMITTED.value,
            submit_at=datetime(2026, 4, 10, 19, 50, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=active_payload,
            broker_order_id=22,
        )

        result = run_startup_reconciliation(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(result.submitted_entries, ())
        self.assertEqual(
            self._read_record("runtime-aapl-1").state,
            ExecutionState.ENTRY_PENDING.value,
        )

        runs = self._read_reconciliation_runs()
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0].run_kind, "startup_reconciliation")
        self.assertIs(runs[0].metadata_json["submit_due_entries"], False)
        self.assertEqual(runs[0].metadata_json["due_instruction_count"], 1)
        self.assertEqual(runs[0].metadata_json["active_instruction_count"], 1)

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
                portfolio=(),
                positions=(),
                account_values={},
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
                portfolio=(),
                positions=(),
                account_values={},
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
                        account="DU1234567",
                        security_type="STK",
                        primary_exchange="NASDAQ",
                        currency="USD",
                        local_symbol="AAPL",
                    ),
                ),
                portfolio=(),
                positions=(),
                account_values={},
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
        session = self.session_factory()
        try:
            broker_orders = session.execute(
                select(BrokerOrderRecord).order_by(BrokerOrderRecord.id)
            ).scalars().all()
            self.assertEqual(len(broker_orders), 2)
            self.assertEqual(
                [(item.order_role, item.status) for item in broker_orders],
                [("ENTRY", "FILLED"), ("EXIT", "Submitted")],
            )
        finally:
            session.close()

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
                        account="DU1234567",
                        security_type="STK",
                        primary_exchange="NASDAQ",
                        currency="USD",
                        local_symbol="AAPL",
                    ),
                ),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.filled_entries), 1)
        self.assertEqual(len(result.submitted_exits), 2)
        self.assertEqual(
            [call["order_ref"] for call in calls],
            [
                "runtime-aapl-1:exit:catastrophic_stop",
                "runtime-aapl-1:exit:take_profit",
            ],
        )
        self.assertEqual(calls[0]["stop_price"], Decimal("170.00"))
        self.assertEqual(calls[1]["limit_price"], Decimal("204.00"))
        self.assertTrue(str(calls[0]["oca_group"]).startswith("OCA"))
        self.assertLessEqual(len(str(calls[0]["oca_group"])), 32)
        self.assertNotIn(":", str(calls[0]["oca_group"]))
        self.assertEqual(calls[0]["oca_group"], calls[1]["oca_group"])
        self.assertEqual(calls[0]["oca_type"], 1)
        self.assertEqual(calls[1]["oca_type"], 1)
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.exit_order_id, 22)
        self.assertEqual(record.exit_submitted_quantity, "1")

    def test_run_runtime_cycle_keeps_entry_fill_when_protective_exit_fails(
        self,
    ) -> None:
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

        exit_submit_calls: list[str] = []

        def rejecting_exit_submitter(
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
            del (
                broker_config,
                instruction,
                quantity,
                order_type,
                timeout,
                limit_price,
                stop_price,
                oca_group,
                oca_type,
            )
            exit_submit_calls.append(order_ref)
            raise RuntimeError("IBKR rejected the order submission: [401] OCA Group")

        snapshot = BrokerRuntimeSnapshot(
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
                    account="DU1234567",
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

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 20, 5, tzinfo=timezone.utc),
            exit_submitter=rejecting_exit_submitter,
            broker_snapshot_fetcher=lambda *args, **kwargs: snapshot,
        )

        self.assertEqual(len(result.issues), 0)
        self.assertEqual(len(result.filled_entries), 1)
        self.assertEqual(len(result.submitted_exits), 0)
        self.assertEqual(
            exit_submit_calls,
            [
                "runtime-aapl-1:exit:catastrophic_stop",
                "runtime-aapl-1:exit:catastrophic_stop",
                "runtime-aapl-1:exit:take_profit",
            ],
        )
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.POSITION_OPEN.value)
        self.assertEqual(record.entry_filled_quantity, "1")
        self.assertEqual(record.entry_avg_fill_price, "200.00")
        self.assertIsNone(record.exit_order_id)

        run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 20, 6, tzinfo=timezone.utc),
            exit_submitter=rejecting_exit_submitter,
            broker_snapshot_fetcher=lambda *args, **kwargs: snapshot,
        )
        self.assertEqual(len(exit_submit_calls), 3)

        session = self.session_factory()
        try:
            event_types = [
                event.event_type
                for event in session.execute(
                    select(InstructionEventRecord).order_by(
                        InstructionEventRecord.id
                    )
                ).scalars()
            ]
            self.assertEqual(
                event_types,
                [
                    "entry_order_filled",
                    "protective_exit_submission_claimed",
                    "protective_exit_submit_failed",
                    "protective_exit_submit_failed",
                    "protective_exit_submit_failed",
                ],
            )
        finally:
            session.close()

    def test_run_runtime_cycle_falls_back_to_single_stop_when_oca_is_rejected(
        self,
    ) -> None:
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

        def oca_rejecting_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: Decimal,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: Decimal | None = None,
            stop_price: Decimal | None = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            del broker_config, instruction, timeout
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
            if oca_group is not None:
                raise RuntimeError("IBKR rejected the order submission: [401] OCA Group")
            self.assertEqual(order_ref, "runtime-aapl-1:exit:catastrophic_stop")
            return {
                "instruction_id": "runtime-aapl-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 265598, "symbol": "AAPL"},
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": "STP",
                    "time_in_force": "DAY",
                    "limit_price": None,
                    "stop_price": str(stop_price),
                    "total_quantity": str(quantity),
                    "outside_rth": False,
                    "oca_group": None,
                    "oca_type": None,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 31,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": str(quantity),
                    "avgFillPrice": 0.0,
                    "permId": 9031,
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
            exit_submitter=oca_rejecting_exit_submitter,
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
                        account="DU1234567",
                        security_type="STK",
                        primary_exchange="NASDAQ",
                        currency="USD",
                        local_symbol="AAPL",
                    ),
                ),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.filled_entries), 1)
        self.assertEqual(len(result.submitted_exits), 1)
        self.assertEqual(
            [call["order_ref"] for call in calls],
            [
                "runtime-aapl-1:exit:catastrophic_stop",
                "runtime-aapl-1:exit:catastrophic_stop",
            ],
        )
        self.assertIsNotNone(calls[0]["oca_group"])
        self.assertIsNone(calls[1]["oca_group"])
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.exit_order_id, 31)
        self.assertTrue(result.submitted_exits[0].detail["fallback_without_oca"])

    def test_run_runtime_cycle_repairs_missing_protective_exits_for_open_position(
        self,
    ) -> None:
        payload = _aapl_payload()
        payload["instruction"]["exit"]["catastrophic_stop_loss_pct"] = "0.15"
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.POSITION_OPEN.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
            broker_order_id=11,
            entry_filled_quantity="1",
            entry_avg_fill_price="200.00",
        )

        calls: list[dict[str, object]] = []

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: Decimal,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: Decimal | None = None,
            stop_price: Decimal | None = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            del broker_config, instruction, timeout
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
            order_id = 41 if order_ref.endswith("catastrophic_stop") else 42
            order_type_code = "STP" if order_ref.endswith("catastrophic_stop") else "LMT"
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
                    "limit_price": str(limit_price) if limit_price is not None else None,
                    "stop_price": str(stop_price) if stop_price is not None else None,
                    "total_quantity": str(quantity),
                    "outside_rth": False,
                    "oca_group": oca_group,
                    "oca_type": oca_type,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": order_id,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": str(quantity),
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
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.submitted_exits), 2)
        self.assertEqual(
            [call["order_ref"] for call in calls],
            [
                "runtime-aapl-1:exit:catastrophic_stop",
                "runtime-aapl-1:exit:take_profit",
            ],
        )
        self.assertEqual(calls[0]["oca_group"], calls[1]["oca_group"])
        self.assertTrue(str(calls[0]["oca_group"]).startswith("OCA"))
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.exit_order_id, 41)

    def test_run_runtime_cycle_repairs_missing_take_profit_when_stop_is_open(
        self,
    ) -> None:
        payload = _aapl_payload()
        payload["instruction"]["exit"]["catastrophic_stop_loss_pct"] = "0.15"
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
            broker_order_id=11,
            exit_order_id=41,
            entry_filled_quantity="1",
            entry_avg_fill_price="200.00",
        )

        calls: list[dict[str, object]] = []

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: Decimal,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: Decimal | None = None,
            stop_price: Decimal | None = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            del broker_config, instruction, timeout
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
            return {
                "instruction_id": "runtime-aapl-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {
                    "con_id": 265598,
                    "symbol": "AAPL",
                    "security_type": "STK",
                    "exchange": "SMART",
                    "currency": "USD",
                },
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": "LMT",
                    "time_in_force": "DAY",
                    "limit_price": str(limit_price) if limit_price is not None else None,
                    "stop_price": None,
                    "total_quantity": str(quantity),
                    "outside_rth": False,
                    "oca_group": oca_group,
                    "oca_type": oca_type,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 42,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": str(quantity),
                    "avgFillPrice": 0.0,
                    "permId": 9042,
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
                open_orders={
                    41: BrokerOpenOrder(
                        order_id=41,
                        perm_id=9041,
                        client_id=0,
                        status="PreSubmitted",
                        order_ref="runtime-aapl-1:exit:catastrophic_stop",
                        action="SELL",
                        total_quantity=Decimal("1"),
                        symbol="AAPL",
                        account="DU1234567",
                        security_type="STK",
                        exchange="SMART",
                        primary_exchange="NASDAQ",
                        currency="USD",
                        local_symbol="AAPL",
                        order_type="STP",
                        aux_price=Decimal("170.00"),
                        oca_group="OCA-test",
                        oca_type=1,
                    )
                },
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.submitted_exits), 1)
        self.assertEqual(calls[0]["order_ref"], "runtime-aapl-1:exit:take_profit")
        self.assertEqual(str(calls[0]["limit_price"]), "204.00")
        self.assertIsNone(calls[0]["stop_price"])
        self.assertIsNotNone(calls[0]["oca_group"])
        self.assertEqual(calls[0]["oca_type"], 1)
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.exit_order_id, 41)

    def test_run_runtime_cycle_marks_missing_protective_exits_stale_and_repairs(
        self,
    ) -> None:
        payload = _aapl_payload()
        payload["instruction"]["exit"]["catastrophic_stop_loss_pct"] = "0.15"
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
            broker_order_id=11,
            exit_order_id=41,
            entry_filled_quantity="1",
            entry_avg_fill_price="200.00",
        )

        session = self.session_factory()
        try:
            instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-aapl-1"
                )
            ).scalar_one()
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="DU1234567",
                base_currency="USD",
                metadata_json={},
            )
            session.add(broker_account)
            session.flush()
            session.add_all(
                [
                    BrokerOrderRecord(
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="DU1234567",
                        order_role="EXIT",
                        external_order_id="41",
                        external_perm_id="9041",
                        external_client_id="0",
                        order_ref="runtime-aapl-1:exit:catastrophic_stop",
                        symbol="AAPL",
                        exchange="SMART",
                        currency="USD",
                        security_type="STK",
                        primary_exchange="NASDAQ",
                        local_symbol="AAPL",
                        side="SELL",
                        order_type="STP",
                        time_in_force="DAY",
                        status="PreSubmitted",
                        total_quantity="1",
                        limit_price=None,
                        stop_price="170.00",
                        submitted_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    ),
                    BrokerOrderRecord(
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="DU1234567",
                        order_role="EXIT",
                        external_order_id="42",
                        external_perm_id="9042",
                        external_client_id="0",
                        order_ref="runtime-aapl-1:exit:take_profit",
                        symbol="AAPL",
                        exchange="SMART",
                        currency="USD",
                        security_type="STK",
                        primary_exchange="NASDAQ",
                        local_symbol="AAPL",
                        side="SELL",
                        order_type="LMT",
                        time_in_force="DAY",
                        status="Submitted",
                        total_quantity="1",
                        limit_price="204.00",
                        stop_price=None,
                        submitted_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    ),
                ]
            )
            session.commit()
        finally:
            session.close()

        calls: list[str] = []

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: Decimal,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: Decimal | None = None,
            stop_price: Decimal | None = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            del broker_config, instruction, timeout
            calls.append(order_ref)
            order_id = 51 if order_ref.endswith("catastrophic_stop") else 52
            order_type_code = "STP" if order_ref.endswith("catastrophic_stop") else "LMT"
            return {
                "instruction_id": "runtime-aapl-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {
                    "con_id": 265598,
                    "symbol": "AAPL",
                    "security_type": "STK",
                    "exchange": "SMART",
                    "currency": "USD",
                },
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": order_type_code,
                    "time_in_force": "DAY",
                    "limit_price": str(limit_price) if limit_price is not None else None,
                    "stop_price": str(stop_price) if stop_price is not None else None,
                    "total_quantity": str(quantity),
                    "outside_rth": False,
                    "oca_group": oca_group,
                    "oca_type": oca_type,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": order_id,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": str(quantity),
                    "avgFillPrice": 0.0,
                    "permId": 9050 + order_id,
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
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(
            calls,
            [
                "runtime-aapl-1:exit:catastrophic_stop",
                "runtime-aapl-1:exit:take_profit",
            ],
        )
        self.assertEqual(len(result.submitted_exits), 2)
        session = self.session_factory()
        try:
            statuses_by_order_id = {
                row.external_order_id: row.status
                for row in session.execute(select(BrokerOrderRecord)).scalars()
            }
            self.assertEqual(statuses_by_order_id["41"], "NOT_FOUND_AT_BROKER")
            self.assertEqual(statuses_by_order_id["42"], "NOT_FOUND_AT_BROKER")
            self.assertEqual(statuses_by_order_id["51"], "Submitted")
            self.assertEqual(statuses_by_order_id["52"], "Submitted")
        finally:
            session.close()

    def test_run_runtime_cycle_blocks_repair_when_duplicate_exit_refs_are_active(
        self,
    ) -> None:
        payload = _aapl_payload()
        payload["instruction"]["exit"]["catastrophic_stop_loss_pct"] = "0.15"
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
            broker_order_id=11,
            entry_filled_quantity="1",
            entry_avg_fill_price="200.00",
        )

        session = self.session_factory()
        try:
            instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-aapl-1"
                )
            ).scalar_one()
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="DU1234567",
                base_currency="USD",
                metadata_json={},
            )
            session.add(broker_account)
            session.flush()
            session.add_all(
                [
                    BrokerOrderRecord(
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="DU1234567",
                        order_role="EXIT",
                        external_order_id="42",
                        external_perm_id="9042",
                        external_client_id="0",
                        order_ref="runtime-aapl-1:exit:take_profit",
                        symbol="AAPL",
                        exchange="SMART",
                        currency="USD",
                        security_type="STK",
                        primary_exchange="NASDAQ",
                        local_symbol="AAPL",
                        side="SELL",
                        order_type="LMT",
                        time_in_force="DAY",
                        status="Submitted",
                        total_quantity="1",
                        limit_price="204.00",
                        submitted_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    ),
                    BrokerOrderRecord(
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="DU1234567",
                        order_role="EXIT",
                        external_order_id="43",
                        external_perm_id="9043",
                        external_client_id="0",
                        order_ref="runtime-aapl-1:exit:take_profit",
                        symbol="AAPL",
                        exchange="SMART",
                        currency="USD",
                        security_type="STK",
                        primary_exchange="NASDAQ",
                        local_symbol="AAPL",
                        side="SELL",
                        order_type="LMT",
                        time_in_force="DAY",
                        status="PreSubmitted",
                        total_quantity="1",
                        limit_price="204.00",
                        submitted_at=datetime(2026, 4, 10, 20, 1, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 10, 20, 1, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    ),
                ]
            )
            session.commit()
        finally:
            session.close()

        calls: list[str] = []

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            **kwargs: object,
        ) -> dict[str, object]:
            del broker_config, instruction
            calls.append(str(kwargs["order_ref"]))
            raise AssertionError("duplicate active exits must block repair submits")

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 20, 5, tzinfo=timezone.utc),
            exit_submitter=fake_exit_submitter,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders=_duplicate_take_profit_open_orders(),
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(calls, [])
        self.assertEqual(result.submitted_exits, ())
        session = self.session_factory()
        try:
            event_type = session.execute(
                select(InstructionEventRecord.event_type).where(
                    InstructionEventRecord.event_type
                    == "protective_exit_duplicate_blocked"
                )
            ).scalar_one()
            self.assertEqual(event_type, "protective_exit_duplicate_blocked")
        finally:
            session.close()

    def test_run_runtime_cycle_can_use_persisted_order_status_fill_without_executions(self) -> None:
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

        session = self.session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="DU1234567",
                base_currency="USD",
            )
            session.add(broker_account)
            session.flush()
            instruction_record = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-aapl-1"
                )
            ).scalar_one()
            session.add(
                BrokerOrderRecord(
                    instruction_id=instruction_record.id,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="DU1234567",
                    order_role="ENTRY",
                    external_order_id="11",
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
                    status="Filled",
                    total_quantity="1",
                    limit_price="200.00",
                    stop_price=None,
                    submitted_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={
                        "last_order_status_callback": {
                            "orderId": 11,
                            "status": "Filled",
                            "filled": "1",
                            "remaining": "0",
                            "avgFillPrice": "200.00",
                            "permId": 8001,
                            "parentId": 0,
                            "lastFillPrice": "200.00",
                            "clientId": 0,
                            "whyHeld": "",
                            "mktCapPrice": "0.0",
                        }
                    },
                )
            )
            session.commit()
        finally:
            session.close()

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: Decimal,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: Decimal | None = None,
            stop_price: Decimal | None = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            return {
                "contract": {"symbol": "AAPL", "exchange": "SMART", "currency": "USD"},
                "order": {
                    "order_id": 21,
                    "order_ref": order_ref,
                    "order_type": "LMT",
                    "action": "SELL",
                    "time_in_force": "DAY",
                    "limit_price": "204.00",
                    "total_quantity": str(quantity),
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 21,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": str(quantity),
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
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.filled_entries), 1)
        self.assertEqual(len(result.submitted_exits), 1)
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.entry_filled_quantity, "1")
        self.assertEqual(record.entry_avg_fill_price, "200.00")

    def test_run_runtime_cycle_submits_delayed_market_anchored_limit_exit(self) -> None:
        payload = _sive_payload()
        payload["instruction"]["exit"] = {
            "delayed_limit": {
                "submit_at": "2026-04-10T10:30:00+02:00",
                "limit_offset_pct": "0.05",
            }
        }
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.POSITION_OPEN.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            entry_filled_quantity="1",
        )

        market_price_calls: list[dict[str, object]] = []

        def fake_market_price_reader(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            at: datetime,
            timeout: int = 10,
        ) -> dict[str, object]:
            market_price_calls.append({"at": at, "timeout": timeout})
            return {
                "price": "20.00",
                "observed_at": "20260410 10:29:00",
                "currency": "SEK",
                "source": "test_latest_trade_price",
            }

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
            self.assertEqual(order_ref, "runtime-sive-1:exit:delayed_limit")
            self.assertEqual(order_type, OrderType.LIMIT)
            self.assertEqual(str(quantity), "1")
            self.assertEqual(limit_price, Decimal("21.00"))
            self.assertIsNone(stop_price)
            return {
                "instruction_id": "runtime-sive-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 489000, "symbol": "SIVE"},
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": "LMT",
                    "time_in_force": "DAY",
                    "limit_price": str(limit_price),
                    "total_quantity": "1",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 41,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 9141,
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
            now=datetime(2026, 4, 10, 8, 30, tzinfo=timezone.utc),
            exit_submitter=fake_exit_submitter,
            market_price_reader=fake_market_price_reader,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(market_price_calls), 1)
        self.assertEqual(len(result.submitted_exits), 1)
        record = self._read_record("runtime-sive-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.exit_order_id, 41)
        self.assertEqual(record.exit_submitted_quantity, "1")

    def test_run_runtime_cycle_does_not_resubmit_delayed_exit_when_ledger_has_open_exit(self) -> None:
        payload = _sive_payload()
        payload["instruction"]["exit"] = {
            "delayed_limit": {
                "submit_at": "2026-04-10T10:30:00+02:00",
                "limit_offset_pct": "0.05",
            }
        }
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            entry_filled_quantity="1",
            exit_order_id=41,
        )

        session = self.session_factory()
        try:
            instruction_record = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-sive-1"
                )
            ).scalar_one()
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="DU1234567",
                base_currency="SEK",
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
                    external_order_id="41",
                    external_perm_id="9141",
                    external_client_id="0",
                    order_ref="runtime-sive-1:exit:delayed_limit",
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
                    limit_price="21.00",
                    stop_price=None,
                    submitted_at=datetime(2026, 4, 10, 8, 30, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 10, 8, 30, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
            )
            session.commit()
        finally:
            session.close()

        market_price_calls: list[dict[str, object]] = []
        exit_submit_calls: list[dict[str, object]] = []

        def fake_market_price_reader(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            at: datetime,
            timeout: int = 10,
        ) -> dict[str, object]:
            market_price_calls.append({"at": at, "timeout": timeout})
            return {
                "price": "20.00",
                "observed_at": "20260410 10:29:00",
                "currency": "SEK",
                "source": "test_latest_trade_price",
            }

        def fake_exit_submitter(**kwargs: object) -> dict[str, object]:
            exit_submit_calls.append(kwargs)
            raise AssertionError("Delayed exit should not be resubmitted when a persisted open exit exists.")

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 8, 30, tzinfo=timezone.utc),
            exit_submitter=fake_exit_submitter,
            market_price_reader=fake_market_price_reader,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders=_delayed_limit_open_orders(),
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(market_price_calls, [])
        self.assertEqual(exit_submit_calls, [])
        self.assertEqual(len(result.submitted_exits), 0)
        record = self._read_record("runtime-sive-1")
        self.assertEqual(record.exit_order_id, 41)
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)

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
                now=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
                exit_submitter=fake_exit_submitter,
                submission_lead_time=timedelta(minutes=1),
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={},
                    executions=(),
                    portfolio=(),
                    positions=(),
                    account_values={},
                ),
            )

        self.assertEqual(len(result.submitted_exits), 1)
        record = self._read_record("runtime-sive-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.exit_order_id, 31)
        self.assertEqual(record.exit_submitted_quantity, "100")

    def test_run_runtime_cycle_blocks_due_entries_while_next_session_exit_is_active(self) -> None:
        exit_payload = _sive_payload()
        entry_payload = _aapl_payload()
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.POSITION_OPEN.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=exit_payload,
            entry_filled_quantity="100",
        )
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 13, 13, 0, tzinfo=timezone.utc),
            payload=entry_payload,
        )

        entry_submit_calls: list[str] = []

        def fake_entry_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            entry_submit_calls.append(instruction.instruction_id)
            raise AssertionError("due entries should be blocked while urgent exits remain active")

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
                now=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
                entry_submitter=fake_entry_submitter,
                exit_submitter=fake_exit_submitter,
                submission_lead_time=timedelta(minutes=1),
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={},
                    executions=(),
                    portfolio=(),
                    positions=(),
                    account_values={},
                ),
            )

        self.assertEqual(entry_submit_calls, [])
        self.assertEqual(len(result.submitted_exits), 1)
        self.assertEqual(result.issues, ())
        self.assertEqual(
            self._read_record("runtime-aapl-1").state,
            ExecutionState.ENTRY_PENDING.value,
        )

    def test_run_runtime_cycle_blocks_due_entries_only_for_same_account(self) -> None:
        exit_payload = _sive_payload()
        entry_payload = _aapl_payload()
        entry_payload["instruction"]["account"]["account_key"] = "OTHER_ACCOUNT"
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.POSITION_OPEN.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=exit_payload,
            entry_filled_quantity="100",
            account_key="GTW05",
        )
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 13, 13, 0, tzinfo=timezone.utc),
            payload=entry_payload,
            account_key="OTHER_ACCOUNT",
        )

        entry_submit_calls: list[str] = []

        def fake_entry_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            del broker_config, timeout
            entry_submit_calls.append(instruction.instruction_id)
            return {
                "instruction_id": "runtime-aapl-1",
                "account": "OTHER_ACCOUNT",
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
                    "orderId": 42,
                    "status": "PreSubmitted",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 9042,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                },
            }

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
            del broker_config, instruction, quantity, order_type, timeout
            del limit_price, stop_price, oca_group, oca_type
            self.assertEqual(order_ref, "runtime-sive-1:exit:forced")
            return {
                "instruction_id": "runtime-sive-1",
                "account": "GTW05",
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
                now=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
                entry_submitter=fake_entry_submitter,
                exit_submitter=fake_exit_submitter,
                submission_lead_time=timedelta(minutes=1),
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={},
                    executions=(),
                    portfolio=(),
                    positions=(),
                    account_values={},
                ),
            )

        self.assertEqual(entry_submit_calls, ["runtime-aapl-1"])
        self.assertEqual(len(result.submitted_exits), 1)
        self.assertEqual(len(result.submitted_entries), 1)
        self.assertEqual(
            self._read_record("runtime-sive-1").state,
            ExecutionState.EXIT_PENDING.value,
        )
        self.assertEqual(
            self._read_record("runtime-aapl-1").state,
            ExecutionState.ENTRY_SUBMITTED.value,
        )

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
                now=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
                exit_submitter=fake_exit_submitter,
                broker_order_canceler=fake_canceler,
                submission_lead_time=timedelta(minutes=1),
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
                            account="DU1234567",
                            security_type="STK",
                            exchange="SMART",
                            primary_exchange="SFB",
                            currency="SEK",
                            local_symbol="SIVE",
                            order_type="LMT",
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
                            account="DU1234567",
                            security_type="STK",
                            exchange="SMART",
                            primary_exchange="SFB",
                            currency="SEK",
                            local_symbol="SIVE",
                            order_type="STP",
                        ),
                    },
                    executions=(),
                    portfolio=(),
                    positions=(),
                    account_values={},
                ),
            )

        self.assertEqual(cancelled_ids, [21, 22])
        self.assertEqual(len(result.submitted_exits), 1)

    def test_run_runtime_cycle_keeps_existing_forced_exit_order(self) -> None:
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
            exit_order_id=31,
        )

        session = self.session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="DU1234567",
                base_currency="SEK",
            )
            session.add(broker_account)
            session.flush()
            instruction_record = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-sive-1"
                )
            ).scalar_one()
            session.add(
                BrokerOrderRecord(
                    instruction_id=instruction_record.id,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="DU1234567",
                    order_role="EXIT",
                    external_order_id="31",
                    external_perm_id="9101",
                    external_client_id="0",
                    order_ref="runtime-sive-1:exit:forced",
                    symbol="SIVE",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    primary_exchange="SFB",
                    local_symbol="SIVE",
                    side="SELL",
                    order_type="MKT",
                    time_in_force="DAY",
                    status="PreSubmitted",
                    total_quantity="100",
                    limit_price=None,
                    stop_price=None,
                    submitted_at=datetime(2026, 4, 13, 6, 58, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 13, 6, 58, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
            )
            session.commit()
        finally:
            session.close()

        cancelled_ids: list[int] = []
        submitted_refs: list[str] = []

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
            submitted_refs.append(order_ref)
            return {}

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
                now=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
                exit_submitter=fake_exit_submitter,
                broker_order_canceler=fake_canceler,
                submission_lead_time=timedelta(minutes=1),
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={
                        31: BrokerOpenOrder(
                            order_id=31,
                            perm_id=9101,
                            client_id=0,
                            status="PreSubmitted",
                            order_ref="runtime-sive-1:exit:forced",
                            action="SELL",
                            total_quantity=Decimal("100"),
                            symbol="SIVE",
                            account="DU1234567",
                            security_type="STK",
                            exchange="SMART",
                            primary_exchange="SFB",
                            currency="SEK",
                            local_symbol="SIVE",
                            order_type="MKT",
                        ),
                    },
                    executions=(),
                    portfolio=(),
                    positions=(),
                    account_values={},
                ),
            )

        self.assertEqual(cancelled_ids, [])
        self.assertEqual(submitted_refs, [])
        self.assertEqual(len(result.submitted_exits), 0)

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
                        account="DU1234567",
                        security_type="STK",
                        primary_exchange="NASDAQ",
                        currency="USD",
                        local_symbol="AAPL",
                    ),
                ),
                portfolio=(),
                positions=(),
                account_values={},
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
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.cancelled_entries), 1)
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.ENTRY_CANCELLED.value)

    def test_persisted_open_exit_orders_dedupes_replaced_order_lineage(self) -> None:
        payload = _sive_payload()
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            exit_order_id=3953,
            entry_filled_quantity="100",
        )

        session = self.session_factory()
        try:
            instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-sive-1"
                )
            ).scalar_one()
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="GTW05",
                base_currency="USD",
                metadata_json={},
            )
            session.add(broker_account)
            session.flush()
            session.add_all(
                [
                    BrokerOrderRecord(
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="GTW05",
                        order_role="EXIT",
                        external_order_id="3952",
                        external_perm_id="449407988",
                        order_ref="runtime-sive-1:exit:forced",
                        symbol="SIVE",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        side="SELL",
                        order_type="MKT",
                        status="PreSubmitted",
                        total_quantity="100",
                        last_status_at=datetime(2026, 4, 10, 7, 30, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    ),
                    BrokerOrderRecord(
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="GTW05",
                        order_role="EXIT",
                        external_order_id="3953",
                        external_perm_id="449407988",
                        order_ref="runtime-sive-1:exit:forced",
                        symbol="SIVE",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        side="SELL",
                        order_type="MKT",
                        status="PreSubmitted",
                        total_quantity="100",
                        last_status_at=datetime(2026, 4, 10, 7, 31, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    ),
                ]
            )
            session.commit()
            records = [instruction]
        finally:
            session.close()

        result = _persisted_open_order_ids_by_instruction(
            self.session_factory,
            records=records,
            order_role="EXIT",
        )

        self.assertEqual(result["runtime-sive-1"], (3953,))

    def test_persisted_open_order_ids_ignore_orders_with_matching_execution_fill(self) -> None:
        payload = _sive_payload()
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            exit_order_id=3953,
            entry_filled_quantity="100",
        )

        session = self.session_factory()
        try:
            instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-sive-1"
                )
            ).scalar_one()
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="GTW05",
                base_currency="USD",
                metadata_json={},
            )
            session.add(broker_account)
            session.flush()
            broker_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="GTW05",
                order_role="EXIT",
                external_order_id="3953",
                external_perm_id="449407988",
                order_ref="runtime-sive-1:exit:forced",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                side="SELL",
                order_type="MKT",
                status="PendingCancel",
                total_quantity="100",
                last_status_at=datetime(2026, 4, 10, 7, 31, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add(broker_order)
            session.flush()
            session.add(
                ExecutionFillRecord(
                    broker_order_id=broker_order.id,
                    instruction_id=instruction.id,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="GTW05",
                    external_execution_id="0001",
                    external_order_id="3953",
                    external_perm_id="449407988",
                    order_ref="runtime-sive-1:exit:forced",
                    symbol="SIVE",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    side="SLD",
                    quantity="100",
                    price="22.61",
                    executed_at=datetime(2026, 4, 10, 7, 32, tzinfo=timezone.utc),
                    raw_payload={},
                )
            )
            session.commit()
            records = [instruction]
        finally:
            session.close()

        result = _persisted_open_order_ids_by_instruction(
            self.session_factory,
            records=records,
            order_role="EXIT",
        )

        self.assertEqual(result["runtime-sive-1"], ())

    def test_persisted_open_exit_orders_require_exact_fill_lineage(self) -> None:
        payload = _sive_payload()
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
        )

        session = self.session_factory()
        try:
            instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-sive-1"
                )
            ).scalar_one()
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="GTW05",
                base_currency="USD",
                metadata_json={},
            )
            session.add(broker_account)
            session.flush()
            stop_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="GTW05",
                order_role="EXIT",
                external_order_id="3952",
                external_perm_id="449407988",
                order_ref="runtime-sive-1:exit:catastrophic_stop",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                side="SELL",
                order_type="STP",
                status="PendingCancel",
                total_quantity="100",
                last_status_at=datetime(2026, 4, 10, 7, 30, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            take_profit_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="GTW05",
                order_role="EXIT",
                external_order_id="3953",
                external_perm_id="449407989",
                order_ref="runtime-sive-1:exit:take_profit",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                side="SELL",
                order_type="LMT",
                status="PreSubmitted",
                total_quantity="100",
                last_status_at=datetime(2026, 4, 10, 7, 31, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add_all([stop_order, take_profit_order])
            session.flush()
            session.add(
                ExecutionFillRecord(
                    broker_order_id=stop_order.id,
                    instruction_id=instruction.id,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="GTW05",
                    external_execution_id="fill-stop-1",
                    external_order_id="3952",
                    external_perm_id="449407988",
                    order_ref="runtime-sive-1:exit:catastrophic_stop",
                    symbol="SIVE",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    side="SLD",
                    quantity="100",
                    price="95.00",
                    executed_at=datetime(2026, 4, 10, 7, 32, tzinfo=timezone.utc),
                    raw_payload={},
                )
            )
            session.commit()
            records = [instruction]
        finally:
            session.close()

        result = _persisted_open_order_ids_by_instruction(
            self.session_factory,
            records=records,
            order_role="EXIT",
        )

        self.assertEqual(result["runtime-sive-1"], (3953,))

    def test_run_runtime_cycle_completes_instruction_from_persisted_exit_fill(self) -> None:
        payload = _sive_payload()
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            exit_order_id=3953,
            entry_filled_quantity="100",
        )

        session = self.session_factory()
        try:
            instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-sive-1"
                )
            ).scalar_one()
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="GTW05",
                base_currency="USD",
                metadata_json={},
            )
            session.add(broker_account)
            session.flush()
            broker_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="GTW05",
                order_role="EXIT",
                external_order_id="3953",
                external_perm_id="449407988",
                order_ref="runtime-sive-1:exit:forced",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                side="SELL",
                order_type="MKT",
                status="PendingCancel",
                total_quantity="100",
                last_status_at=datetime(2026, 4, 10, 7, 31, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add(broker_order)
            session.flush()
            session.add(
                ExecutionFillRecord(
                    broker_order_id=broker_order.id,
                    instruction_id=instruction.id,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="GTW05",
                    external_execution_id="0001",
                    external_order_id="3953",
                    external_perm_id="449407988",
                    order_ref="runtime-sive-1:exit:forced",
                    symbol="SIVE",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    side="SLD",
                    quantity="100",
                    price="22.61",
                    executed_at=datetime(2026, 4, 10, 7, 32, tzinfo=timezone.utc),
                    raw_payload={},
                )
            )
            session.commit()
        finally:
            session.close()

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 8, 30, tzinfo=timezone.utc),
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.completed_instructions), 1)
        record = self._read_record("runtime-sive-1")
        self.assertEqual(record.state, ExecutionState.COMPLETED.value)
        self.assertEqual(record.exit_order_status, "Filled")
        self.assertEqual(record.exit_filled_quantity, "100")
