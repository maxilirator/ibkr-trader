from __future__ import annotations

from datetime import date
from datetime import datetime
from datetime import timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from ibkr_trader.api.server import (
    create_app,
    enforce_loopback_binding,
    is_loopback_host,
    parse_account_summary_payload,
    parse_contract_resolve_payload,
    parse_execution_batch_payload,
    parse_historical_bars_payload,
    parse_kill_switch_payload,
    parse_positive_limit,
    parse_runtime_cycle_payload,
    parse_shortability_snapshot_payload,
    parse_tick_stream_payload,
    serialize_execution_batch,
    serialize_runtime_schedule_preview,
)
from ibkr_trader.config import AppConfig
from ibkr_trader.config import ApiServerConfig
from ibkr_trader.config import IbkrConnectionConfig
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
from ibkr_trader.db.models import InstructionSetCancellationRecord
from ibkr_trader.db.models import ReconciliationIssueRecord
from ibkr_trader.db.models import ReconciliationRunRecord
from ibkr_trader.ibkr.shortability import ShortabilityMarketDataType
from ibkr_trader.ibkr.shortability import ShortabilitySource
from ibkr_trader.orchestration.scheduling import build_batch_runtime_schedule
from ibkr_trader.orchestration.operator_controls import set_kill_switch_state


def _sample_submit_payload() -> dict[str, object]:
    return {
        "schema_version": "2026-04-10",
        "source": {
            "system": "q-training",
            "batch_id": "trial_27-2026-04-10-prod-long-01",
            "generated_at": "2026-04-10T02:15:44Z",
        },
        "instructions": [
            {
                "instruction_id": "2026-04-10-GTW05-long_risk_book-SIVE-long-01",
                "account": {
                    "account_key": "GTW05",
                    "book_key": "long_risk_book",
                },
                "instrument": {
                    "symbol": "SIVE",
                    "security_type": "STK",
                    "exchange": "SMART",
                    "primary_exchange": "SFB",
                    "currency": "SEK",
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
                },
                "exit": {
                    "force_exit_next_session_open": True,
                },
                "trace": {
                    "reason_code": "risk_policy_orderbook",
                },
            }
        ],
    }


def _write_schedule_fixture(schedule_path: Path) -> None:
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


class ApiServerTests(TestCase):
    def test_is_loopback_host_accepts_loopback_names_and_ips(self) -> None:
        self.assertTrue(is_loopback_host("127.0.0.1"))
        self.assertTrue(is_loopback_host("::1"))
        self.assertTrue(is_loopback_host("localhost"))
        self.assertFalse(is_loopback_host("0.0.0.0"))
        self.assertFalse(is_loopback_host("192.168.1.15"))

    def test_enforce_loopback_binding_rejects_nonlocal_host(self) -> None:
        with self.assertRaisesRegex(ValueError, "loopback"):
            enforce_loopback_binding("0.0.0.0", require_loopback_only=True)

    def test_parse_positive_limit_validates_bounds(self) -> None:
        self.assertEqual(parse_positive_limit(5, field_name="limit", maximum=10), 5)
        with self.assertRaisesRegex(ValueError, "positive"):
            parse_positive_limit(0, field_name="limit", maximum=10)
        with self.assertRaisesRegex(ValueError, "at most 10"):
            parse_positive_limit(11, field_name="limit", maximum=10)

    def test_parse_contract_resolve_payload_normalizes_values(self) -> None:
        query = parse_contract_resolve_payload(
            {
                "symbol": "sive",
                "security_type": "stk",
                "exchange": "xsto",
                "currency": "sek",
                "primary_exchange": "xsto",
                "isin": "SE0003917798",
            }
        )

        self.assertEqual(query.symbol, "SIVE")
        self.assertEqual(query.security_type, "STK")
        self.assertEqual(query.exchange, "XSTO")
        self.assertEqual(query.currency, "SEK")
        self.assertEqual(query.primary_exchange, "XSTO")
        self.assertEqual(query.isin, "SE0003917798")

    def test_parse_account_summary_payload_accepts_defaults(self) -> None:
        tags, group, account_id = parse_account_summary_payload({})

        self.assertIn("NetLiquidation", tags)
        self.assertEqual(group, "All")
        self.assertIsNone(account_id)

    def test_parse_historical_bars_payload_normalizes_values(self) -> None:
        query = parse_historical_bars_payload(
            {
                "symbol": "sive",
                "security_type": "stk",
                "exchange": "smart",
                "currency": "sek",
                "primary_exchange": "sfb",
                "duration": "2 D",
                "bar_size": "5 mins",
                "what_to_show": "trades",
                "use_rth": True,
                "end_at": "2026-04-10T17:30:00+02:00",
            }
        )

        self.assertEqual(query.symbol, "SIVE")
        self.assertEqual(query.security_type, "STK")
        self.assertEqual(query.exchange, "SMART")
        self.assertEqual(query.currency, "SEK")
        self.assertEqual(query.primary_exchange, "SFB")
        self.assertEqual(query.duration, "2 D")
        self.assertEqual(query.bar_size, "5 mins")
        self.assertEqual(query.what_to_show, "TRADES")
        self.assertTrue(query.use_rth)
        self.assertEqual(query.end_at.isoformat(), "2026-04-10T17:30:00+02:00")

    def test_parse_tick_stream_payload_normalizes_tick_types(self) -> None:
        query = parse_tick_stream_payload(
            {
                "symbol": "aapl",
                "security_type": "stk",
                "exchange": "smart",
                "currency": "usd",
                "primary_exchange": "nasdaq",
                "tick_types": ["last", "bid_ask", "mid-point"],
                "duration_seconds": 3,
                "max_events": 100,
            }
        )

        self.assertEqual(query.symbol, "AAPL")
        self.assertEqual(query.exchange, "SMART")
        self.assertEqual(query.currency, "USD")
        self.assertEqual(query.primary_exchange, "NASDAQ")
        self.assertEqual(query.tick_types, ("Last", "BidAsk", "MidPoint"))
        self.assertEqual(query.duration_seconds, 3)
        self.assertEqual(query.max_events, 100)

    def test_parse_tick_stream_payload_rejects_empty_tick_types(self) -> None:
        with self.assertRaisesRegex(ValueError, "tick_types"):
            parse_tick_stream_payload(
                {
                    "symbol": "AAPL",
                    "exchange": "SMART",
                    "currency": "USD",
                    "tick_types": [],
                }
            )

    def test_parse_shortability_snapshot_payload_uses_stockholm_defaults(self) -> None:
        query = parse_shortability_snapshot_payload({})

        self.assertEqual(query.exchange, "SMART")
        self.assertEqual(query.primary_exchange, "SFB")
        self.assertEqual(query.currency, "SEK")
        self.assertEqual(query.security_type, "STK")
        self.assertEqual(query.source, ShortabilitySource.OFFICIAL_IBKR_PAGE)
        self.assertEqual(query.market_data_type, ShortabilityMarketDataType.LIVE)
        self.assertTrue(query.only_shortable)
        self.assertIsNone(query.as_of_date)

    def test_parse_shortability_snapshot_payload_accepts_symbols_date_source_and_delayed_type(self) -> None:
        query = parse_shortability_snapshot_payload(
            {
                "symbols": ["sive", "abb"],
                "as_of_date": "2026-04-14",
                "source": "broker_ticks",
                "market_data_type": "delayed_frozen",
                "max_symbols": 25,
                "max_concurrent": 10,
                "per_symbol_timeout_seconds": 1.5,
            }
        )

        self.assertEqual(query.symbols, ("SIVE", "ABB"))
        self.assertEqual(query.source, ShortabilitySource.BROKER_TICKS)
        self.assertEqual(
            query.market_data_type,
            ShortabilityMarketDataType.DELAYED_FROZEN,
        )
        self.assertEqual(query.as_of_date, date(2026, 4, 14))
        self.assertEqual(query.max_symbols, 25)
        self.assertEqual(query.max_concurrent, 10)
        self.assertEqual(query.per_symbol_timeout_seconds, 1.5)

    def test_operator_snapshot_endpoint_returns_durable_ledger_state(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "operator_snapshot.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            session_factory = create_session_factory(engine)
            session = session_factory()
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
                    AccountSnapshotRecord(
                        broker_account_id=broker_account.id,
                        snapshot_at=datetime(2026, 4, 19, 8, 15, tzinfo=timezone.utc),
                        source="runtime_snapshot",
                        net_liquidation="100500.00",
                        total_cash_value="55000.00",
                        buying_power="200000.00",
                        available_funds="120000.00",
                        excess_liquidity="119000.00",
                        cushion="0.91",
                        currency="SEK",
                    )
                )
                session.add(
                    InstructionRecord(
                        instruction_id="instr-001",
                        schema_version="2026-04-10",
                        source_system="q-training",
                        batch_id="batch-001",
                        account_key="U25245596",
                        book_key="long_risk_book",
                        symbol="SAAB",
                        exchange="SMART",
                        currency="SEK",
                        state="ENTRY_PENDING",
                        submit_at=datetime(2026, 4, 19, 8, 20, tzinfo=timezone.utc),
                        expire_at=datetime(2026, 4, 19, 15, 30, tzinfo=timezone.utc),
                        order_type="LIMIT",
                        side="BUY",
                        payload={"instruction": {"instruction_id": "instr-001"}},
                    )
                )
                session.commit()
            finally:
                session.close()
                engine.dispose()

            app = create_app(
                AppConfig(
                    environment="test",
                    timezone="Europe/Stockholm",
                    database_url=database_url,
                    session_calendar_path=Path("/tmp/day_sessions.parquet"),
                    stockholm_instruments_path=Path("/tmp/all.txt"),
                    stockholm_identity_path=Path("/tmp/identity.parquet"),
                    api=ApiServerConfig(
                        host="127.0.0.1",
                        port=8000,
                        require_loopback_only=False,
                    ),
                    ibkr=IbkrConnectionConfig(
                        host="127.0.0.1",
                        port=4001,
                        client_id=0,
                        diagnostic_client_id=7,
                        streaming_client_id=9,
                        account_id="U25245596",
                    ),
                )
            )

            with (
                patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
                patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
                TestClient(app) as client,
            ):
                response = client.get("/v1/read/operator-snapshot")

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertTrue(body["accepted"])
            self.assertEqual(body["operator_snapshot"]["accounts"][0]["account_key"], "U25245596")
            self.assertEqual(
                body["operator_snapshot"]["instructions"][0]["instruction_id"],
                "instr-001",
            )

    def test_ledger_snapshot_endpoint_returns_append_only_history(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "ledger_snapshot.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            session_factory = create_session_factory(engine)
            session = session_factory()
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
                    instruction_id="instr-001",
                    schema_version="2026-04-10",
                    source_system="q-training",
                    batch_id="batch-001",
                    account_key="U25245596",
                    book_key="long_risk_book",
                    symbol="SAAB",
                    exchange="SMART",
                    currency="SEK",
                    state="ENTRY_SUBMITTED",
                    submit_at=datetime(2026, 4, 19, 7, 20, tzinfo=timezone.utc),
                    expire_at=datetime(2026, 4, 19, 15, 30, tzinfo=timezone.utc),
                    order_type="LIMIT",
                    side="BUY",
                    broker_order_id=11,
                    broker_order_status="Submitted",
                    payload={},
                )
                session.add(instruction)
                session.flush()

                broker_order = BrokerOrderRecord(
                    instruction_id=instruction.id,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="U25245596",
                    order_role="ENTRY",
                    external_order_id="11",
                    external_perm_id="9001",
                    external_client_id="0",
                    order_ref="instr-001",
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
                    submitted_at=datetime(2026, 4, 19, 7, 21, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 19, 7, 22, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
                session.add(broker_order)
                session.flush()

                session.add(
                    InstructionEventRecord(
                        instruction_id=instruction.id,
                        event_type="entry_submitted",
                        source="runtime",
                        event_at=datetime(2026, 4, 19, 7, 21, tzinfo=timezone.utc),
                        state_before="ENTRY_PENDING",
                        state_after="ENTRY_SUBMITTED",
                        payload={},
                        note="Runtime submitted the entry order.",
                    )
                )
                session.add(
                    BrokerOrderEventRecord(
                        broker_order_id=broker_order.id,
                        event_type="order_error_callback",
                        event_at=datetime(2026, 4, 19, 7, 22, tzinfo=timezone.utc),
                        status_before="PreSubmitted",
                        status_after="Submitted",
                        payload={"errorCode": 201, "errorMsg": "Order held for review"},
                        note="Broker callback arrived.",
                    )
                )
                session.add(
                    ExecutionFillRecord(
                        broker_order_id=broker_order.id,
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        external_execution_id="exec-001",
                        external_order_id="11",
                        external_perm_id="9001",
                        order_ref="instr-001",
                        symbol="SAAB",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        side="BOT",
                        quantity="1",
                        price="100.50",
                        commission="1.00",
                        commission_currency="SEK",
                        executed_at=datetime(2026, 4, 19, 7, 23, tzinfo=timezone.utc),
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
                    action_count=1,
                    metadata_json={},
                )
                session.add(reconciliation_run)
                session.flush()
                session.add(
                    ReconciliationIssueRecord(
                        reconciliation_run_id=reconciliation_run.id,
                        instruction_id="instr-001",
                        stage="reconcile_instruction",
                        severity="ERROR",
                        message="Order state drift detected.",
                        observed_at=datetime(2026, 4, 19, 7, 25, 3, tzinfo=timezone.utc),
                        payload={"broker_order_id": 11},
                    )
                )
                session.add(
                    InstructionSetCancellationRecord(
                        requested_at=datetime(2026, 4, 19, 7, 26, tzinfo=timezone.utc),
                        requested_by="dashboard",
                        reason="Cancel stale row.",
                        selectors={"instruction_ids": ["instr-001"]},
                        status="COMPLETED",
                        matched_instruction_count=1,
                        cancelled_pending_count=0,
                        cancelled_submitted_count=1,
                        skipped_count=0,
                        failed_count=0,
                        result_payload={
                            "results": [
                                {
                                    "instruction_id": "instr-001",
                                    "action": "cancelled_submitted_entry",
                                }
                            ]
                        },
                    )
                )
                session.commit()
            finally:
                session.close()
                engine.dispose()

            set_kill_switch_state(
                session_factory,
                enabled=True,
                reason="Freeze new entries.",
                updated_by="test-suite",
            )

            app = create_app(
                AppConfig(
                    environment="test",
                    timezone="Europe/Stockholm",
                    database_url=database_url,
                    session_calendar_path=Path("/tmp/day_sessions.parquet"),
                    stockholm_instruments_path=Path("/tmp/all.txt"),
                    stockholm_identity_path=Path("/tmp/identity.parquet"),
                    api=ApiServerConfig(
                        host="127.0.0.1",
                        port=8000,
                        require_loopback_only=False,
                    ),
                    ibkr=IbkrConnectionConfig(
                        host="127.0.0.1",
                        port=4001,
                        client_id=0,
                        diagnostic_client_id=7,
                        streaming_client_id=9,
                        account_id="U25245596",
                    ),
                )
            )

            with (
                patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
                patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
                TestClient(app) as client,
            ):
                response = client.get("/v1/read/ledger-snapshot?focus_instruction_id=instr-001")

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertTrue(body["accepted"])
            self.assertEqual(
                body["ledger_snapshot"]["focus_instruction"]["instruction_id"],
                "instr-001",
            )
            self.assertEqual(body["ledger_snapshot"]["summary"]["instruction_count"], 1)
            self.assertEqual(
                body["ledger_snapshot"]["broker_order_events"][0]["message"],
                "[201] Order held for review",
            )

    def test_kill_switch_endpoints_round_trip(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "controls.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            engine.dispose()

            app = create_app(
                AppConfig(
                    environment="test",
                    timezone="Europe/Stockholm",
                    database_url=database_url,
                    session_calendar_path=Path(temp_dir) / "day_sessions.parquet",
                    stockholm_instruments_path=Path("/tmp/all.txt"),
                    stockholm_identity_path=Path("/tmp/identity.parquet"),
                    api=ApiServerConfig(
                        host="127.0.0.1",
                        port=8000,
                        require_loopback_only=False,
                    ),
                    ibkr=IbkrConnectionConfig(
                        host="127.0.0.1",
                        port=4001,
                        client_id=0,
                        diagnostic_client_id=7,
                        streaming_client_id=9,
                        account_id="DU1234567",
                    ),
                )
            )

            with (
                patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
                patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
                TestClient(app) as client,
            ):
                initial = client.get("/v1/controls/kill-switch")
                updated = client.post(
                    "/v1/controls/kill-switch",
                    json={
                        "enabled": True,
                        "reason": "Freeze new entries.",
                        "updated_by": "test-suite",
                    },
                )
                after = client.get("/v1/controls/kill-switch")

            self.assertEqual(initial.status_code, 200)
            self.assertFalse(initial.json()["kill_switch"]["enabled"])
            self.assertEqual(updated.status_code, 200)
            self.assertTrue(updated.json()["kill_switch"]["enabled"])
            self.assertEqual(after.status_code, 200)
            self.assertEqual(after.json()["kill_switch"]["reason"], "Freeze new entries.")

    def test_submit_endpoint_rejects_when_kill_switch_is_enabled(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "submit_kill_switch.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            engine.dispose()
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)

            app = create_app(
                AppConfig(
                    environment="test",
                    timezone="Europe/Stockholm",
                    database_url=database_url,
                    session_calendar_path=schedule_path,
                    stockholm_instruments_path=Path("/tmp/all.txt"),
                    stockholm_identity_path=Path("/tmp/identity.parquet"),
                    api=ApiServerConfig(
                        host="127.0.0.1",
                        port=8000,
                        require_loopback_only=False,
                    ),
                    ibkr=IbkrConnectionConfig(
                        host="127.0.0.1",
                        port=4001,
                        client_id=0,
                        diagnostic_client_id=7,
                        streaming_client_id=9,
                        account_id="DU1234567",
                    ),
                )
            )

            with (
                patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
                patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
                TestClient(app) as client,
            ):
                client.post(
                    "/v1/controls/kill-switch",
                    json={
                        "enabled": True,
                        "reason": "Freeze new entries.",
                        "updated_by": "test-suite",
                    },
                )
                response = client.post("/v1/instructions/submit", json=_sample_submit_payload())

            self.assertEqual(response.status_code, 409)
            self.assertIn("kill switch", response.text)

    def test_cancel_set_endpoint_cancels_pending_instructions(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "cancel_set.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            session_factory = create_session_factory(engine)
            session = session_factory()
            try:
                session.add(
                    InstructionRecord(
                        instruction_id="instr-001",
                        schema_version="2026-04-10",
                        source_system="q-training",
                        batch_id="batch-001",
                        account_key="U25245596",
                        book_key="long_risk_book",
                        symbol="SAAB",
                        exchange="SMART",
                        currency="SEK",
                        state="ENTRY_PENDING",
                        submit_at=datetime(2026, 4, 19, 8, 20, tzinfo=timezone.utc),
                        expire_at=datetime(2026, 4, 19, 15, 30, tzinfo=timezone.utc),
                        order_type="LIMIT",
                        side="BUY",
                        payload={"instruction": {"instruction_id": "instr-001"}},
                    )
                )
                session.commit()
            finally:
                session.close()
                engine.dispose()

            app = create_app(
                AppConfig(
                    environment="test",
                    timezone="Europe/Stockholm",
                    database_url=database_url,
                    session_calendar_path=Path(temp_dir) / "day_sessions.parquet",
                    stockholm_instruments_path=Path("/tmp/all.txt"),
                    stockholm_identity_path=Path("/tmp/identity.parquet"),
                    api=ApiServerConfig(
                        host="127.0.0.1",
                        port=8000,
                        require_loopback_only=False,
                    ),
                    ibkr=IbkrConnectionConfig(
                        host="127.0.0.1",
                        port=4001,
                        client_id=0,
                        diagnostic_client_id=7,
                        streaming_client_id=9,
                        account_id="DU1234567",
                    ),
                )
            )

            with (
                patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
                patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
                TestClient(app) as client,
            ):
                response = client.post(
                    "/v1/instructions/cancel-set",
                    json={
                        "batch_id": "batch-001",
                        "requested_by": "test-suite",
                    },
                )

            self.assertEqual(response.status_code, 200)
            body = response.json()["cancelled_instruction_set"]
            self.assertEqual(body["status"], "COMPLETED")
            self.assertEqual(body["cancelled_pending_count"], 1)
            self.assertEqual(body["matched_instruction_count"], 1)

    def test_ibkr_telemetry_limit_must_be_positive(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        app = create_app(
            AppConfig(
                environment="test",
                timezone="Europe/Stockholm",
                database_url="sqlite+pysqlite:///:memory:",
                session_calendar_path=Path("/tmp/day_sessions.parquet"),
                stockholm_instruments_path=Path("/tmp/all.txt"),
                stockholm_identity_path=Path("/tmp/identity.parquet"),
                api=ApiServerConfig(
                    host="127.0.0.1",
                    port=8000,
                    require_loopback_only=False,
                ),
                ibkr=IbkrConnectionConfig(
                    host="127.0.0.1",
                    port=4001,
                    client_id=0,
                    diagnostic_client_id=7,
                    streaming_client_id=9,
                    account_id="DU1234567",
                ),
            )
        )

        client = TestClient(app)
        response = client.get("/v1/ibkr/telemetry?recent_limit=0")
        self.assertEqual(response.status_code, 400)
        self.assertIn("recent_limit", response.text)

    def test_parse_runtime_cycle_payload_accepts_optional_timestamp(self) -> None:
        now_at, timeout, instruction_ids = parse_runtime_cycle_payload(
            {
                "now_at": "2026-04-13T09:00:00+02:00",
                "timeout": 15,
                "instruction_ids": ["instruction-1", "instruction-2"],
            }
        )

        self.assertEqual(now_at.isoformat(), "2026-04-13T09:00:00+02:00")
        self.assertEqual(timeout, 15)
        self.assertEqual(instruction_ids, ("instruction-1", "instruction-2"))

    def test_parse_kill_switch_payload_requires_boolean_enabled(self) -> None:
        enabled, reason, updated_by = parse_kill_switch_payload(
            {
                "enabled": True,
                "reason": "Freeze new entries.",
                "updated_by": "dashboard",
            }
        )

        self.assertTrue(enabled)
        self.assertEqual(reason, "Freeze new entries.")
        self.assertEqual(updated_by, "dashboard")

        with self.assertRaisesRegex(ValueError, "boolean"):
            parse_kill_switch_payload({"enabled": "yes"})

    def test_parse_execution_batch_payload_validates_contract(self) -> None:
        batch = parse_execution_batch_payload(
            {
                "schema_version": "2026-04-10",
                "source": {
                    "system": "q-training",
                    "batch_id": "trial_27-2026-04-10-prod-long-01",
                    "generated_at": "2026-04-10T02:15:44Z",
                    "release_id": "release-1",
                    "strategy_id": "trial_27",
                    "policy_id": "policy-1",
                },
                "instructions": [
                    {
                        "instruction_id": "2026-04-10-GTW05-long_risk_book-SIVE-long-01",
                        "account": {
                            "account_key": "GTW05",
                            "book_key": "long_risk_book",
                            "book_role": "prod",
                            "book_side": "long",
                        },
                        "instrument": {
                            "symbol": "sive",
                            "security_type": "stk",
                            "exchange": "xsto",
                            "currency": "sek",
                            "isin": "SE0003917798",
                            "aliases": ["SIVE.ST", "sivers-ima"],
                        },
                        "intent": {
                            "side": "buy",
                            "position_side": "long",
                        },
                        "sizing": {
                            "mode": "fraction_of_account_nav",
                            "target_fraction_of_account": "1.0",
                        },
                        "entry": {
                            "order_type": "limit",
                            "submit_at": "2026-04-10T09:25:00+02:00",
                            "expire_at": "2026-04-10T17:30:00+02:00",
                            "limit_price": "11.3131",
                            "time_in_force": "day",
                            "max_submit_count": 1,
                            "cancel_unfilled_at_expiry": True,
                        },
                        "exit": {
                            "take_profit_pct": "0.02",
                            "catastrophic_stop_loss_pct": "0.15",
                            "force_exit_next_session_open": True,
                        },
                        "trace": {
                            "reason_code": "risk_policy_orderbook",
                            "execution_policy": "policy-x",
                            "trade_date": "2026-04-10",
                            "data_cutoff_date": "2026-04-09",
                            "company_name": "Sivers Semiconductors",
                            "metadata": {
                                "entry_reference_type": "prev_close",
                                "entry_reference_price": "11.37",
                            },
                        },
                    }
                ],
            }
        )

        serialized = serialize_execution_batch(batch)

        self.assertEqual(serialized["schema_version"], "2026-04-10")
        self.assertEqual(serialized["instructions"][0]["instrument"]["symbol"], "SIVE")
        self.assertEqual(serialized["instructions"][0]["instrument"]["exchange"], "XSTO")
        self.assertEqual(serialized["instructions"][0]["entry"]["limit_price"], "11.3131")
        self.assertEqual(
            serialized["instructions"][0]["sizing"]["target_fraction_of_account"],
            "1.0",
        )

    def test_parse_execution_batch_payload_requires_absolute_timestamps(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone"):
            parse_execution_batch_payload(
                {
                    "schema_version": "2026-04-10",
                    "source": {
                        "system": "q-training",
                        "batch_id": "trial_27-2026-04-10-prod-long-01",
                        "generated_at": "2026-04-10T02:15:44Z",
                    },
                    "instructions": [
                        {
                            "instruction_id": "demo-1",
                            "account": {
                                "account_key": "GTW05",
                                "book_key": "long_risk_book",
                            },
                            "instrument": {
                                "symbol": "SIVE",
                                "security_type": "STK",
                                "exchange": "XSTO",
                                "currency": "SEK",
                            },
                            "intent": {
                                "side": "BUY",
                                "position_side": "LONG",
                            },
                            "sizing": {
                                "mode": "fraction_of_account_nav",
                                "target_fraction_of_account": "1.0",
                            },
                            "entry": {
                                "order_type": "LIMIT",
                                "submit_at": "2026-04-10T09:25:00",
                                "expire_at": "2026-04-10T17:30:00+02:00",
                                "limit_price": "11.3131",
                            },
                            "exit": {
                                "take_profit_pct": "0.02",
                            },
                            "trace": {
                                "reason_code": "risk_policy_orderbook",
                            },
                        }
                    ],
                }
            )

    def test_parse_execution_batch_payload_requires_single_sizing_target(self) -> None:
        with self.assertRaisesRegex(ValueError, "exactly one"):
            parse_execution_batch_payload(
                {
                    "schema_version": "2026-04-10",
                    "source": {
                        "system": "q-training",
                        "batch_id": "trial_27-2026-04-10-prod-long-01",
                        "generated_at": "2026-04-10T02:15:44Z",
                    },
                    "instructions": [
                        {
                            "instruction_id": "demo-1",
                            "account": {
                                "account_key": "GTW05",
                                "book_key": "long_risk_book",
                            },
                            "instrument": {
                                "symbol": "SIVE",
                                "security_type": "STK",
                                "exchange": "XSTO",
                                "currency": "SEK",
                            },
                            "intent": {
                                "side": "BUY",
                                "position_side": "LONG",
                            },
                            "sizing": {
                                "mode": "fraction_of_account_nav",
                                "target_fraction_of_account": "1.0",
                                "target_notional": "100000",
                            },
                            "entry": {
                                "order_type": "LIMIT",
                                "submit_at": "2026-04-10T09:25:00+02:00",
                                "expire_at": "2026-04-10T17:30:00+02:00",
                                "limit_price": "11.3131",
                            },
                            "exit": {
                                "take_profit_pct": "0.02",
                            },
                            "trace": {
                                "reason_code": "risk_policy_orderbook",
                            },
                        }
                    ],
                }
            )

    def test_serialize_runtime_schedule_preview_projects_stockholm_times(self) -> None:
        batch = parse_execution_batch_payload(
            {
                "schema_version": "2026-04-10",
                "source": {
                    "system": "q-training",
                    "batch_id": "trial_27-2026-04-10-prod-long-01",
                    "generated_at": "2026-04-10T02:15:44Z",
                },
                "instructions": [
                    {
                        "instruction_id": "demo-1",
                        "account": {
                            "account_key": "GTW05",
                            "book_key": "long_risk_book",
                        },
                        "instrument": {
                            "symbol": "SIVE",
                            "security_type": "STK",
                            "exchange": "XSTO",
                            "currency": "SEK",
                        },
                        "intent": {
                            "side": "BUY",
                            "position_side": "LONG",
                        },
                        "sizing": {
                            "mode": "fraction_of_account_nav",
                            "target_fraction_of_account": "1.0",
                        },
                        "entry": {
                            "order_type": "LIMIT",
                            "submit_at": "2026-04-10T07:25:00Z",
                            "expire_at": "2026-04-10T15:30:00Z",
                            "limit_price": "11.3131",
                        },
                        "exit": {
                            "force_exit_next_session_open": True,
                        },
                        "trace": {
                            "reason_code": "risk_policy_orderbook",
                        },
                    }
                ],
            }
        )

        preview = serialize_runtime_schedule_preview(
            build_batch_runtime_schedule(batch, runtime_timezone="Europe/Stockholm")
        )

        self.assertEqual(preview["runtime_timezone"], "Europe/Stockholm")
        self.assertEqual(
            preview["instructions"][0]["submit_at_runtime"],
            "2026-04-10T09:25:00+02:00",
        )
        self.assertEqual(
            preview["instructions"][0]["next_session_exit"]["status"],
            "calendar_required",
        )
