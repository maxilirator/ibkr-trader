from __future__ import annotations

import sys
from importlib import util as importlib_util
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
from ibkr_trader.db.models import ExecutionFillRecord
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


def _load_q_training_intraday_simulator():
    for repo_root in (
        Path("/home/mattias/dev/q-training"),
        Path("/home/mattias/dev/q-training-bucket-booster"),
    ):
        simulator_path = repo_root / "src/q_train/intraday/simulator.py"
        if simulator_path.exists():
            module_name = "_q_training_intraday_simulator_for_trader_tests"
            module_spec = importlib_util.spec_from_file_location(module_name, simulator_path)
            if module_spec is None or module_spec.loader is None:
                continue
            module = importlib_util.module_from_spec(module_spec)
            sys.modules[module_name] = module
            module_spec.loader.exec_module(module)
            return module.IntradayReplaySpec, module.simulate_component_session
    return None, None


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

class RLActionTranslationTestsBase(TestCase):
    pass

class RLActionVirtualExecutionTestsBase(TestCase):
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

class RLActionTranslationApiTestsBase(TestCase):
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


__all__ = [name for name in globals() if not name.startswith("__")]
