from __future__ import annotations

from datetime import datetime
from datetime import timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from sqlalchemy import select

from ibkr_trader.api.server import create_app
from ibkr_trader.config import AppConfig
from ibkr_trader.config import ApiServerConfig
from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.domain.execution_payloads import parse_execution_batch_payload
from ibkr_trader.orchestration.runtime_worker import run_runtime_cycle
from ibkr_trader.orchestration.rl_action_execution import execute_owned_rl_action
from ibkr_trader.orchestration.state_machine import ExecutionState
from ibkr_trader.orchestration.submission import submit_execution_batch
from ibkr_trader.rl.action_translation import ACTION_STATUS_INVALID
from ibkr_trader.rl.action_translation import ACTION_STATUS_LOGGED
from ibkr_trader.rl.action_translation import ACTION_STATUS_TRANSLATED
from ibkr_trader.rl.action_translation import FLAT
from ibkr_trader.rl.action_translation import LONG_OPEN
from ibkr_trader.rl.action_translation import SHORT_OPEN
from ibkr_trader.rl.action_translation import translate_rl_action
from ibkr_trader.virtual.execution import record_virtual_market_quote


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


def _model_routed_payload(
    *,
    instruction_id: str,
    model_id: str,
    symbol: str,
    side: str,
    account_key: str = "VIRTUALRL01",
    book_key: str = "rl_shared_virtual_01",
    notional: str = "1000",
) -> dict[str, object]:
    return {
        "schema_version": "2026-04-25",
        "source": {
            "system": "q-training",
            "batch_id": f"{instruction_id}-batch",
            "generated_at": "2026-04-27T06:50:00Z",
            "strategy_id": "rl-virtual-smoke",
        },
        "instructions": [
            {
                "instruction_id": instruction_id,
                "account": {
                    "account_key": account_key,
                    "book_key": book_key,
                    "book_role": "virtual",
                    "book_side": side,
                },
                "instrument": {
                    "symbol": symbol,
                    "security_type": "STK",
                    "exchange": "SMART",
                    "currency": "SEK",
                    "primary_exchange": "SFB",
                },
                "intent": {
                    "side": "BUY" if side == "LONG" else "SELL",
                    "position_side": side,
                },
                "sizing": {
                    "mode": "target_notional",
                    "target_notional": notional,
                    "funding_basis": "cash",
                    "allow_leverage": side == "SHORT",
                },
                "execution": {
                    "mode": "model_routed",
                    "model_id": model_id,
                    "model_family": "canonical_rl",
                    "model_version": "v1",
                    "model_artifact_id": f"{model_id}:test",
                    "window": {
                        "start_at": "2026-04-27T07:00:00Z",
                        "end_at": "2026-04-27T15:30:00Z",
                    },
                },
                "trace": {
                    "reason_code": "model_routed_selection",
                    "trade_date": "2026-04-27",
                    "metadata": {
                        "selection_source": "test",
                    },
                },
            }
        ],
    }


def _translate(
    payload: dict[str, object],
    *,
    deployment_key: str,
    action_name: str,
    previous_close: Decimal | None = Decimal("100"),
    state_before: str = FLAT,
):
    batch = parse_execution_batch_payload(payload)
    return translate_rl_action(
        batch,
        batch.instructions[0],
        deployment_key=deployment_key,
        action_name=action_name,
        state_before=state_before,
        observed_at=datetime(2026, 4, 27, 7, 5, tzinfo=timezone.utc),
        previous_close=previous_close,
        decision_id="2026-04-27T07:05:00Z",
    )


class RLActionTranslationTests(TestCase):
    def test_long_prevclose_action_maps_to_buy_limit_below_previous_close(self) -> None:
        result = _translate(
            _model_routed_payload(
                instruction_id="long-axfo-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
                book_key="rl_shared_long_trial_106_virtual_01",
            ),
            deployment_key="long_trial_106_virtual_shared_01",
            action_name="entry_prevclose_-50bp",
        )

        self.assertEqual(result.action_status, ACTION_STATUS_TRANSLATED)
        instruction = result.instruction_payload["instructions"][0]
        self.assertEqual(instruction["intent"], {"side": "BUY", "position_side": "LONG"})
        self.assertEqual(instruction["entry"]["order_type"], "LIMIT")
        self.assertEqual(instruction["entry"]["limit_price"], "99.5000")

    def test_short_prevclose_action_maps_to_sell_limit_above_previous_close(self) -> None:
        result = _translate(
            _model_routed_payload(
                instruction_id="short-aza-1",
                model_id="short_trial36_v1",
                symbol="AZA",
                side="SHORT",
                book_key="rl_shared_short_trial_36_virtual_01",
            ),
            deployment_key="short_trial_36_virtual_shared_01",
            action_name="entry_prevclose_88bp",
        )

        self.assertEqual(result.action_status, ACTION_STATUS_TRANSLATED)
        instruction = result.instruction_payload["instructions"][0]
        self.assertEqual(instruction["intent"], {"side": "SELL", "position_side": "SHORT"})
        self.assertEqual(instruction["entry"]["order_type"], "LIMIT")
        self.assertEqual(instruction["entry"]["limit_price"], "100.8800")

    def test_long_market_entry_maps_to_buy_and_has_no_limit_price(self) -> None:
        result = _translate(
            _model_routed_payload(
                instruction_id="long-market-1",
                model_id="long_trial_106_v1",
                symbol="AZN",
                side="LONG",
            ),
            deployment_key="long_trial_106_virtual_shared_01",
            action_name="market_entry",
        )

        self.assertEqual(result.action_status, ACTION_STATUS_TRANSLATED)
        instruction = result.instruction_payload["instructions"][0]
        self.assertEqual(instruction["intent"], {"side": "BUY", "position_side": "LONG"})
        self.assertEqual(instruction["entry"]["order_type"], "MARKET")
        self.assertNotIn("limit_price", instruction["entry"])

    def test_short_market_entry_maps_to_sell_and_has_no_limit_price(self) -> None:
        result = _translate(
            _model_routed_payload(
                instruction_id="short-market-1",
                model_id="short_trial36_v1",
                symbol="AZA",
                side="SHORT",
                book_key="rl_shared_short_trial_36_virtual_01",
            ),
            deployment_key="short_trial_36_virtual_shared_01",
            action_name="market_entry",
        )

        self.assertEqual(result.action_status, ACTION_STATUS_TRANSLATED)
        instruction = result.instruction_payload["instructions"][0]
        self.assertEqual(instruction["intent"], {"side": "SELL", "position_side": "SHORT"})
        self.assertEqual(instruction["entry"]["order_type"], "MARKET")
        self.assertNotIn("limit_price", instruction["entry"])

    def test_wait_and_skip_do_not_generate_instructions(self) -> None:
        for action_name in ("skip", "wait"):
            result = _translate(
                _model_routed_payload(
                    instruction_id=f"long-{action_name}-1",
                    model_id="long_trial_106_v1",
                    symbol="AXFO",
                    side="LONG",
                ),
                deployment_key="long_trial_106_virtual_shared_01",
                action_name=action_name,
            )
            self.assertEqual(result.action_status, ACTION_STATUS_LOGGED)
            self.assertIsNone(result.instruction_payload)

    def test_long_rejects_short_prevclose_direction(self) -> None:
        result = _translate(
            _model_routed_payload(
                instruction_id="long-wrong-entry-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
            ),
            deployment_key="long_trial_106_virtual_shared_01",
            action_name="entry_prevclose_88bp",
        )

        self.assertEqual(result.action_status, ACTION_STATUS_INVALID)
        self.assertIsNone(result.instruction_payload)

    def test_short_rejects_long_prevclose_direction(self) -> None:
        result = _translate(
            _model_routed_payload(
                instruction_id="short-wrong-entry-1",
                model_id="short_trial36_v1",
                symbol="AZA",
                side="SHORT",
            ),
            deployment_key="short_trial_36_virtual_shared_01",
            action_name="entry_prevclose_-50bp",
        )

        self.assertEqual(result.action_status, ACTION_STATUS_INVALID)
        self.assertIsNone(result.instruction_payload)

    def test_exit_actions_translate_to_owned_mutations_without_instruction_payload(self) -> None:
        long_result = _translate(
            _model_routed_payload(
                instruction_id="long-exit-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
            ),
            deployment_key="long_trial_106_virtual_shared_01",
            action_name="exit_tp_200bp",
            state_before=LONG_OPEN,
        )
        short_result = _translate(
            _model_routed_payload(
                instruction_id="short-exit-1",
                model_id="short_trial36_v1",
                symbol="AZA",
                side="SHORT",
            ),
            deployment_key="short_trial_36_virtual_shared_01",
            action_name="exit_tp_180bp",
            state_before=SHORT_OPEN,
        )

        self.assertEqual(long_result.action_status, ACTION_STATUS_TRANSLATED)
        self.assertEqual(long_result.state_after, "EXIT_PENDING")
        self.assertIsNone(long_result.instruction_payload)
        self.assertEqual(short_result.action_status, ACTION_STATUS_TRANSLATED)
        self.assertEqual(short_result.state_after, "EXIT_PENDING")
        self.assertIsNone(short_result.instruction_payload)

    def test_wrong_side_take_profit_actions_fail_closed(self) -> None:
        long_result = _translate(
            _model_routed_payload(
                instruction_id="long-wrong-exit-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
            ),
            deployment_key="long_trial_106_virtual_shared_01",
            action_name="exit_tp_180bp",
            state_before=LONG_OPEN,
        )
        short_result = _translate(
            _model_routed_payload(
                instruction_id="short-wrong-exit-1",
                model_id="short_trial36_v1",
                symbol="AZA",
                side="SHORT",
            ),
            deployment_key="short_trial_36_virtual_shared_01",
            action_name="exit_tp_200bp",
            state_before=SHORT_OPEN,
        )

        self.assertEqual(long_result.action_status, ACTION_STATUS_INVALID)
        self.assertIsNone(long_result.instruction_payload)
        self.assertIn("long take-profit", long_result.note)
        self.assertEqual(short_result.action_status, ACTION_STATUS_INVALID)
        self.assertIsNone(short_result.instruction_payload)
        self.assertIn("short take-profit", short_result.note)

    def test_exit_market_is_allowed_only_from_matching_open_state(self) -> None:
        long_result = _translate(
            _model_routed_payload(
                instruction_id="long-exit-market-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
            ),
            deployment_key="long_trial_106_virtual_shared_01",
            action_name="exit_market",
            state_before=LONG_OPEN,
        )
        wrong_state_result = _translate(
            _model_routed_payload(
                instruction_id="long-exit-market-wrong-state-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
            ),
            deployment_key="long_trial_106_virtual_shared_01",
            action_name="exit_market",
            state_before=SHORT_OPEN,
        )

        self.assertEqual(long_result.action_status, ACTION_STATUS_TRANSLATED)
        self.assertEqual(long_result.state_after, "EXIT_PENDING")
        self.assertIsNone(long_result.instruction_payload)
        self.assertEqual(wrong_state_result.action_status, ACTION_STATUS_INVALID)
        self.assertIsNone(wrong_state_result.instruction_payload)


class RLActionVirtualExecutionTests(TestCase):
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

    def _submit_translated(self, translated_payload: dict[str, object], schedule_path: Path) -> None:
        batch = parse_execution_batch_payload(translated_payload)
        submit_execution_batch(
            self.session_factory,
            batch,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=schedule_path,
        )

    def _run_cycle(self, schedule_path: Path, minute: int):
        return run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=schedule_path,
            now=datetime(2026, 4, 27, 7, minute, tzinfo=timezone.utc),
        )

    def _record_quote(self, *, symbol: str, price: Decimal, minute: int) -> None:
        record_virtual_market_quote(
            self.session_factory,
            account_key="VIRTUALRL01",
            symbol=symbol,
            exchange="SMART",
            currency="SEK",
            security_type="STK",
            primary_exchange="SFB",
            last_price=price,
            bid_price=price,
            ask_price=price,
            observed_at=datetime(2026, 4, 27, 7, minute, tzinfo=timezone.utc),
            source="test",
        )

    def test_long_limit_entry_fills_only_when_stream_crosses_down_to_limit(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            result = _translate(
                _model_routed_payload(
                    instruction_id="long-cross-1",
                    model_id="long_trial_106_v1",
                    symbol="AXFO",
                    side="LONG",
                    book_key="rl_shared_long_trial_106_virtual_01",
                ),
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="entry_prevclose_-50bp",
            )
            self._submit_translated(result.instruction_payload, schedule_path)

            self._record_quote(symbol="AXFO", price=Decimal("100.00"), minute=5)
            self._run_cycle(schedule_path, 6)
            self._run_cycle(schedule_path, 7)
            session = self.session_factory()
            try:
                order = session.execute(select(BrokerOrderRecord)).scalar_one()
                instruction = session.execute(select(InstructionRecord)).scalar_one()
                self.assertEqual(order.status, "Submitted")
                self.assertEqual(instruction.state, ExecutionState.ENTRY_SUBMITTED.value)
            finally:
                session.close()

            self._record_quote(symbol="AXFO", price=Decimal("99.50"), minute=8)
            self._run_cycle(schedule_path, 9)
            session = self.session_factory()
            try:
                order = session.execute(select(BrokerOrderRecord)).scalar_one()
                instruction = session.execute(select(InstructionRecord)).scalar_one()
                self.assertEqual(order.status, "FILLED")
                self.assertEqual(instruction.state, ExecutionState.POSITION_OPEN.value)
            finally:
                session.close()

    def test_cancel_entry_marks_owned_pending_instruction_cancelled(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            source_payload = _model_routed_payload(
                instruction_id="long-cancel-entry-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
                book_key="rl_shared_long_trial_106_virtual_01",
            )
            result = _translate(
                source_payload,
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="entry_prevclose_-50bp",
            )
            self._submit_translated(result.instruction_payload, schedule_path)
            source_batch = parse_execution_batch_payload(source_payload)

            execution = execute_owned_rl_action(
                self.session_factory,
                self.config,
                source_batch.instructions[0],
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="cancel_entry",
            )

            self.assertEqual(execution.state_before, ExecutionState.ENTRY_PENDING.value)
            self.assertEqual(execution.state_after, ExecutionState.ENTRY_CANCELLED.value)
            session = self.session_factory()
            try:
                instruction = session.execute(select(InstructionRecord)).scalar_one()
                self.assertEqual(instruction.state, ExecutionState.ENTRY_CANCELLED.value)
            finally:
                session.close()

    def test_long_take_profit_exit_submits_sell_limit_above_entry_fill(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            source_payload = _model_routed_payload(
                instruction_id="long-owned-exit-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
                book_key="rl_shared_long_trial_106_virtual_01",
            )
            result = _translate(
                source_payload,
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="entry_prevclose_-50bp",
            )
            self._submit_translated(result.instruction_payload, schedule_path)
            self._record_quote(symbol="AXFO", price=Decimal("99.50"), minute=5)
            self._run_cycle(schedule_path, 6)
            self._run_cycle(schedule_path, 7)
            source_batch = parse_execution_batch_payload(source_payload)

            execution = execute_owned_rl_action(
                self.session_factory,
                self.config,
                source_batch.instructions[0],
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="exit_tp_200bp",
            )

            self.assertEqual(execution.state_before, ExecutionState.POSITION_OPEN.value)
            self.assertEqual(execution.state_after, ExecutionState.EXIT_PENDING.value)
            self.assertEqual(execution.limit_price, "101.4900")
            session = self.session_factory()
            try:
                orders = session.execute(
                    select(BrokerOrderRecord).order_by(BrokerOrderRecord.id.asc())
                ).scalars().all()
                self.assertEqual(len(orders), 2)
                self.assertEqual(orders[1].order_role, "EXIT")
                self.assertEqual(orders[1].side, "SELL")
                self.assertEqual(orders[1].order_type, "LMT")
                self.assertEqual(orders[1].limit_price, "101.4900")
                instruction = session.execute(select(InstructionRecord)).scalar_one()
                self.assertEqual(instruction.state, ExecutionState.EXIT_PENDING.value)
            finally:
                session.close()

    def test_short_take_profit_exit_submits_buy_limit_below_entry_fill(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            source_payload = _model_routed_payload(
                instruction_id="short-owned-exit-1",
                model_id="short_trial36_v1",
                symbol="AZA",
                side="SHORT",
                book_key="rl_shared_short_trial_36_virtual_01",
            )
            result = _translate(
                source_payload,
                deployment_key="short_trial_36_virtual_shared_01",
                action_name="entry_prevclose_88bp",
            )
            self._submit_translated(result.instruction_payload, schedule_path)
            self._record_quote(symbol="AZA", price=Decimal("100.88"), minute=5)
            self._run_cycle(schedule_path, 6)
            self._run_cycle(schedule_path, 7)
            source_batch = parse_execution_batch_payload(source_payload)

            execution = execute_owned_rl_action(
                self.session_factory,
                self.config,
                source_batch.instructions[0],
                deployment_key="short_trial_36_virtual_shared_01",
                action_name="exit_tp_180bp",
            )

            self.assertEqual(execution.state_after, ExecutionState.EXIT_PENDING.value)
            self.assertEqual(execution.limit_price, "99.0642")
            session = self.session_factory()
            try:
                orders = session.execute(
                    select(BrokerOrderRecord).order_by(BrokerOrderRecord.id.asc())
                ).scalars().all()
                self.assertEqual(len(orders), 2)
                self.assertEqual(orders[0].side, "SELL")
                self.assertEqual(orders[1].order_role, "EXIT")
                self.assertEqual(orders[1].side, "BUY")
                self.assertEqual(orders[1].order_type, "LMT")
                self.assertEqual(orders[1].limit_price, "99.0642")
            finally:
                session.close()

    def test_clear_exit_cancels_owned_exit_and_keeps_position_open(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            source_payload = _model_routed_payload(
                instruction_id="long-clear-exit-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
                book_key="rl_shared_long_trial_106_virtual_01",
            )
            result = _translate(
                source_payload,
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="entry_prevclose_-50bp",
            )
            self._submit_translated(result.instruction_payload, schedule_path)
            self._record_quote(symbol="AXFO", price=Decimal("99.50"), minute=5)
            self._run_cycle(schedule_path, 6)
            self._run_cycle(schedule_path, 7)
            source_batch = parse_execution_batch_payload(source_payload)
            execute_owned_rl_action(
                self.session_factory,
                self.config,
                source_batch.instructions[0],
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="exit_tp_200bp",
            )

            execution = execute_owned_rl_action(
                self.session_factory,
                self.config,
                source_batch.instructions[0],
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="clear_exit",
            )

            self.assertEqual(execution.state_before, ExecutionState.EXIT_PENDING.value)
            self.assertEqual(execution.state_after, ExecutionState.POSITION_OPEN.value)
            session = self.session_factory()
            try:
                orders = session.execute(
                    select(BrokerOrderRecord).order_by(BrokerOrderRecord.id.asc())
                ).scalars().all()
                self.assertEqual(orders[1].status, "Cancelled")
                instruction = session.execute(select(InstructionRecord)).scalar_one()
                self.assertEqual(instruction.state, ExecutionState.POSITION_OPEN.value)
            finally:
                session.close()


class RLActionTranslationApiTests(TestCase):
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

    def _submit_translated(self, translated_payload: dict[str, object], schedule_path: Path) -> None:
        batch = parse_execution_batch_payload(translated_payload)
        submit_execution_batch(
            self.session_factory,
            batch,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=schedule_path,
        )

    def _run_cycle(self, schedule_path: Path, minute: int):
        return run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=schedule_path,
            now=datetime(2026, 4, 27, 7, minute, tzinfo=timezone.utc),
        )

    def _record_quote(self, *, symbol: str, price: Decimal, minute: int) -> None:
        record_virtual_market_quote(
            self.session_factory,
            account_key="VIRTUALRL01",
            symbol=symbol,
            exchange="SMART",
            currency="SEK",
            security_type="STK",
            primary_exchange="SFB",
            last_price=price,
            bid_price=price,
            ask_price=price,
            observed_at=datetime(2026, 4, 27, 7, minute, tzinfo=timezone.utc),
            source="test",
        )

    def test_translate_endpoint_submits_and_logs_deterministic_instruction(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            schedule_path = temp_path / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            database_url = f"sqlite+pysqlite:///{temp_path / 'rl_translate.db'}"
            engine = build_engine(database_url)
            create_schema(engine)
            engine.dispose()

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
                model_response = client.post(
                    "/v1/rl/models/register",
                    json={
                        "model_key": "long_trial_106_v1",
                        "display_name": "Long Trial 106 V1",
                        "strategy_family": "canonical_long",
                        "side": "LONG",
                        "action_space": [
                            "skip",
                            "wait",
                            "market_entry",
                            "entry_prevclose_-50bp",
                            "exit_tp_200bp",
                        ],
                        "observation_contract": {
                            "bar_family": "phase1_intraday_ohlc_v1",
                            "bar_interval": "5m",
                        },
                        "execution_mapping_version": "long_actions_v1",
                    },
                )
                deployment_response = client.post(
                    "/v1/rl/deployments",
                    json={
                        "deployment_key": "long_trial_106_virtual_shared_01",
                        "model_key": "long_trial_106_v1",
                        "account_key": "VIRTUALRL01",
                        "book_key": "rl_shared_long_trial_106_virtual_01",
                        "mode": "virtual",
                        "status": "running",
                        "allowed_symbols": ["AXFO"],
                        "risk_limits": {},
                        "action_constraints": {
                            "position_side": "LONG",
                            "execution_mapping_version": "long_actions_v1",
                        },
                    },
                )
                source_response = client.post(
                    "/v1/instructions/submit",
                    json=_model_routed_payload(
                        instruction_id="api-long-axfo-1",
                        model_id="long_trial_106_v1",
                        symbol="AXFO",
                        side="LONG",
                        book_key="rl_shared_long_trial_106_virtual_01",
                    ),
                )
                translate_response = client.post(
                    "/v1/rl/actions/translate",
                    json={
                        "deployment_key": "long_trial_106_virtual_shared_01",
                        "source_instruction_id": "api-long-axfo-1",
                        "action_name": "entry_prevclose_-50bp",
                        "state_before": "FLAT",
                        "observed_at": "2026-04-27T07:05:00Z",
                        "previous_close": "100",
                        "decision_id": "2026-04-27T07:05:00Z",
                        "submit": True,
                        "log_action": True,
                    },
                )

        self.assertEqual(model_response.status_code, 200)
        self.assertEqual(deployment_response.status_code, 200)
        self.assertEqual(source_response.status_code, 200)
        self.assertEqual(translate_response.status_code, 200)
        body = translate_response.json()
        self.assertTrue(body["accepted"])
        self.assertTrue(body["submitted"])
        self.assertEqual(body["translation"]["action_status"], "translated")
        instruction = body["translation"]["instruction_payload"]["instructions"][0]
        self.assertEqual(instruction["intent"], {"side": "BUY", "position_side": "LONG"})
        self.assertEqual(instruction["entry"]["limit_price"], "99.5000")
        self.assertEqual(body["submitted_batch"]["instruction_count"], 1)
        self.assertEqual(body["trader_action"]["action_status"], "translated")

    def test_translate_endpoint_executes_owned_long_take_profit_exit(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            schedule_path = temp_path / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            database_url = f"sqlite+pysqlite:///{temp_path / 'rl_exit_translate.db'}"
            engine = build_engine(database_url)
            create_schema(engine)
            engine.dispose()

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
                    "/v1/rl/models/register",
                    json={
                        "model_key": "long_trial_106_v1",
                        "display_name": "Long Trial 106 V1",
                        "strategy_family": "canonical_long",
                        "side": "LONG",
                        "action_space": [
                            "skip",
                            "wait",
                            "market_entry",
                            "entry_prevclose_-50bp",
                            "exit_tp_200bp",
                            "clear_exit",
                        ],
                        "observation_contract": {
                            "bar_family": "phase1_intraday_ohlc_v1",
                            "bar_interval": "5m",
                        },
                        "execution_mapping_version": "long_actions_v1",
                    },
                )
                client.post(
                    "/v1/rl/deployments",
                    json={
                        "deployment_key": "long_trial_106_virtual_shared_01",
                        "model_key": "long_trial_106_v1",
                        "account_key": "VIRTUALRL01",
                        "book_key": "rl_shared_long_trial_106_virtual_01",
                        "mode": "virtual",
                        "status": "running",
                        "allowed_symbols": ["AXFO"],
                        "risk_limits": {},
                        "action_constraints": {
                            "position_side": "LONG",
                            "execution_mapping_version": "long_actions_v1",
                        },
                    },
                )
                client.post(
                    "/v1/instructions/submit",
                    json=_model_routed_payload(
                        instruction_id="api-long-exit-axfo-1",
                        model_id="long_trial_106_v1",
                        symbol="AXFO",
                        side="LONG",
                        book_key="rl_shared_long_trial_106_virtual_01",
                    ),
                )
                entry_response = client.post(
                    "/v1/rl/actions/translate",
                    json={
                        "deployment_key": "long_trial_106_virtual_shared_01",
                        "source_instruction_id": "api-long-exit-axfo-1",
                        "action_name": "entry_prevclose_-50bp",
                        "state_before": "FLAT",
                        "observed_at": "2026-04-27T07:05:00Z",
                        "previous_close": "100",
                        "decision_id": "entry-decision",
                        "submit": True,
                        "log_action": True,
                    },
                )
                generated_instruction_id = entry_response.json()["translation"][
                    "instruction_payload"
                ]["instructions"][0]["instruction_id"]
                inspection_engine = build_engine(database_url)
                inspection_session_factory = create_session_factory(inspection_engine)
                session = inspection_session_factory()
                try:
                    instruction = session.execute(
                        select(InstructionRecord).where(
                            InstructionRecord.instruction_id == generated_instruction_id
                        )
                    ).scalar_one()
                    instruction.state = ExecutionState.POSITION_OPEN.value
                    instruction.entry_filled_quantity = "1"
                    instruction.entry_avg_fill_price = "99.50"
                    session.commit()
                finally:
                    session.close()
                    inspection_engine.dispose()
                exit_response = client.post(
                    "/v1/rl/actions/translate",
                    json={
                        "deployment_key": "long_trial_106_virtual_shared_01",
                        "source_instruction_id": "api-long-exit-axfo-1",
                        "action_name": "exit_tp_200bp",
                        "state_before": "LONG_OPEN",
                        "observed_at": "2026-04-27T07:10:00Z",
                        "previous_close": "100",
                        "decision_id": "exit-decision",
                        "submit": True,
                        "log_action": True,
                    },
                )

        self.assertEqual(exit_response.status_code, 200)
        body = exit_response.json()
        self.assertTrue(body["submitted"])
        self.assertEqual(body["translation"]["action_status"], "translated")
        self.assertEqual(body["action_execution"]["state_after"], "EXIT_PENDING")
        self.assertEqual(body["action_execution"]["limit_price"], "101.4900")
        self.assertEqual(body["trader_action"]["action_status"], "executed")

    def test_short_limit_entry_fills_only_when_stream_crosses_up_to_limit(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            result = _translate(
                _model_routed_payload(
                    instruction_id="short-cross-1",
                    model_id="short_trial36_v1",
                    symbol="AZA",
                    side="SHORT",
                    book_key="rl_shared_short_trial_36_virtual_01",
                ),
                deployment_key="short_trial_36_virtual_shared_01",
                action_name="entry_prevclose_88bp",
            )
            self._submit_translated(result.instruction_payload, schedule_path)

            self._record_quote(symbol="AZA", price=Decimal("100.00"), minute=5)
            self._run_cycle(schedule_path, 6)
            self._run_cycle(schedule_path, 7)
            session = self.session_factory()
            try:
                order = session.execute(select(BrokerOrderRecord)).scalar_one()
                instruction = session.execute(select(InstructionRecord)).scalar_one()
                self.assertEqual(order.status, "Submitted")
                self.assertEqual(instruction.state, ExecutionState.ENTRY_SUBMITTED.value)
            finally:
                session.close()

            self._record_quote(symbol="AZA", price=Decimal("100.88"), minute=8)
            self._run_cycle(schedule_path, 9)
            session = self.session_factory()
            try:
                order = session.execute(select(BrokerOrderRecord)).scalar_one()
                instruction = session.execute(select(InstructionRecord)).scalar_one()
                self.assertEqual(order.status, "FILLED")
                self.assertEqual(instruction.state, ExecutionState.POSITION_OPEN.value)
            finally:
                session.close()

    def test_market_entry_fills_on_next_virtual_runtime_cycle(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            result = _translate(
                _model_routed_payload(
                    instruction_id="long-market-fill-1",
                    model_id="long_trial_106_v1",
                    symbol="AZN",
                    side="LONG",
                    book_key="rl_shared_long_trial_106_virtual_01",
                ),
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="market_entry",
            )
            self._submit_translated(result.instruction_payload, schedule_path)

            self._record_quote(symbol="AZN", price=Decimal("101.25"), minute=5)
            self._run_cycle(schedule_path, 6)
            self._run_cycle(schedule_path, 7)
            session = self.session_factory()
            try:
                order = session.execute(select(BrokerOrderRecord)).scalar_one()
                instruction = session.execute(select(InstructionRecord)).scalar_one()
                self.assertEqual(order.order_type, "MKT")
                self.assertEqual(order.side, "BUY")
                self.assertEqual(order.status, "FILLED")
                self.assertEqual(instruction.state, ExecutionState.POSITION_OPEN.value)
            finally:
                session.close()
