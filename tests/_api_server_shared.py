from __future__ import annotations

from copy import deepcopy
from datetime import date
from datetime import datetime
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from ibkr_trader.api.server import (
    build_rl_runtime_state_snapshot,
    create_app,
    enforce_loopback_binding,
    enrich_operator_snapshot_with_market_stream,
    _completed_rl_bar_as_of,
    is_loopback_host,
    market_stream_contracts_for_current_holdings,
    market_stream_contracts_for_open_orders,
    market_stream_contracts_for_open_virtual_positions,
    market_stream_contracts_for_runtime_holdings,
    parse_account_summary_payload,
    parse_contract_resolve_payload,
    parse_execution_batch_payload,
    parse_historical_bars_payload,
    parse_kill_switch_payload,
    parse_market_stream_subscribe_payload,
    parse_operator_review_payload,
    parse_positive_limit,
    parse_runtime_cycle_payload,
    parse_rl_observation_build_payload,
    parse_stockholm_intraday_backfill_payload,
    parse_shortability_snapshot_payload,
    parse_tick_stream_payload,
    parse_trader_action_payload,
    parse_trader_deployment_payload,
    parse_trader_deployment_update_payload,
    parse_trader_heartbeat_payload,
    parse_trader_model_payload,
    serialize_execution_batch,
    serialize_runtime_schedule_preview,
    should_include_background_execution_recovery,
)
from ibkr_trader.config import AppConfig
from ibkr_trader.config import ApiServerConfig
from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from tests._kill_switch_test_support import disable_kill_switch_for_test
from ibkr_trader.db.models import AccountSnapshotRecord
from ibkr_trader.db.models import BrokerAccountRecord
from ibkr_trader.db.models import BrokerOrderEventRecord
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import ExecutionFillRecord
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import InstructionSetCancellationRecord
from ibkr_trader.db.models import PositionSnapshotRecord
from ibkr_trader.db.models import ReconciliationIssueRecord
from ibkr_trader.db.models import ReconciliationRunRecord
from ibkr_trader.db.models import TraderDeploymentRecord
from ibkr_trader.db.models import TraderModelRecord
from ibkr_trader.db.models import VirtualMarketQuoteRecord
from ibkr_trader.ibkr.shortability import ShortabilityMarketDataType
from ibkr_trader.ibkr.shortability import ShortabilitySource
from ibkr_trader.ibkr.runtime_snapshot import BrokerOpenOrder
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
    schedule_path.write_text(
        "\n".join(
            [
                "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
                "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
            ]
        ),
        encoding="utf-8",
    )

class ApiServerTestCase(TestCase):
    pass


__all__ = [name for name in globals() if not name.startswith("__")]
