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
    create_app,
    enforce_loopback_binding,
    enrich_operator_snapshot_with_market_stream,
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

    def test_parse_stockholm_intraday_backfill_payload_normalizes_values(self) -> None:
        query = parse_stockholm_intraday_backfill_payload(
            {
                "as_of_date": "2026-04-24",
                "bar_size": "1 min",
                "what_to_show": ["trades", "midpoint", "ask"],
                "use_rth": True,
                "max_symbols": 10,
                "start_after": "sive",
                "symbols": ["volcar-b", "sive"],
                "include_remapped": True,
                "sleep_seconds": 0.0,
                "max_runtime_seconds": 12.5,
            }
        )

        self.assertEqual(query.as_of_date.isoformat(), "2026-04-24")
        self.assertEqual(query.bar_size, "1 min")
        self.assertEqual(query.what_to_show, ("TRADES", "MIDPOINT", "ASK"))
        self.assertTrue(query.use_rth)
        self.assertEqual(query.max_symbols, 10)
        self.assertEqual(query.start_after, "sive")
        self.assertEqual(query.symbols, ("volcar-b", "sive"))
        self.assertTrue(query.include_remapped)
        self.assertEqual(query.sleep_seconds, 0.0)
        self.assertEqual(query.max_runtime_seconds, 12.5)

    def test_parse_trader_payloads_normalize_values(self) -> None:
        model_payload = parse_trader_model_payload(
            {
                "model_key": "Short_Trial36_V1",
                "display_name": "Short Trial 36 V1",
                "strategy_family": "canonical_short_live_execution_policy",
                "side": "short",
                "action_space": ["skip", "market_entry", "exit_market"],
                "observation_contract": {"bar_family": "stockholm_intraday_1m_v1"},
                "metadata": {"canonical_seed": 140},
            }
        )
        deployment_payload = parse_trader_deployment_payload(
            {
                "deployment_key": "Short_Trial36_Live_01",
                "model_key": "Short_Trial36_V1",
                "account_key": "u25245596",
                "book_key": "RL_SHORT_TRIAL36_LIVE_01",
                "mode": "live",
                "status": "running",
                "allowed_symbols": ["sive", "volv-b"],
            }
        )
        action_payload = parse_trader_action_payload(
            {
                "deployment_key": "Short_Trial36_Live_01",
                "symbol": "sive",
                "action_name": "market_entry",
                "observed_at": "2026-04-25T09:25:00+02:00",
                "state_before": "flat",
                "state_after": "entry_pending",
                "action_status": "translated",
                "payload": {"confidence": 0.73},
            }
        )
        heartbeat_payload = parse_trader_heartbeat_payload(
            {
                "status": "running",
                "last_seen_at": "2026-04-25T09:30:00+02:00",
                "last_bar_at": "2026-04-25T09:29:00+02:00",
                "metrics": {"bar_lag_seconds": 4},
            }
        )

        self.assertEqual(model_payload["model_key"], "short_trial36_v1")
        self.assertEqual(model_payload["side"], "SHORT")
        self.assertEqual(deployment_payload["deployment_key"], "short_trial36_live_01")
        self.assertEqual(deployment_payload["account_key"], "U25245596")
        self.assertEqual(deployment_payload["allowed_symbols"], ("SIVE", "VOLV-B"))
        deployment_update_payload = parse_trader_deployment_update_payload(
            {
                "status": "running",
                "allowed_symbols": ["volv-b", "sive", "volv-b"],
                "metadata": {"edited_by": "operator"},
            }
        )
        self.assertEqual(deployment_update_payload["status"], "running")
        self.assertEqual(
            deployment_update_payload["allowed_symbols"],
            ("VOLV-B", "SIVE"),
        )
        self.assertEqual(
            deployment_update_payload["metadata"]["edited_by"],
            "operator",
        )
        self.assertEqual(action_payload["symbol"], "SIVE")
        self.assertEqual(action_payload["state_before"], "FLAT")
        self.assertEqual(heartbeat_payload["status"], "running")
        self.assertEqual(
            heartbeat_payload["last_bar_at"].isoformat(),
            "2026-04-25T09:29:00+02:00",
        )

    def test_parse_trader_payloads_require_explicit_runtime_fields(self) -> None:
        with self.assertRaisesRegex(ValueError, "side is required"):
            parse_trader_model_payload(
                {
                    "model_key": "long_trial_v1",
                    "display_name": "Long Trial V1",
                    "strategy_family": "canonical_long_live_execution_policy",
                    "action_space": ["skip", "market_entry"],
                }
            )

        with self.assertRaisesRegex(ValueError, "mode is required"):
            parse_trader_deployment_payload(
                {
                    "deployment_key": "long_trial_virtual_01",
                    "model_key": "long_trial_v1",
                    "account_key": "virtual0001",
                    "book_key": "rl_long_trial_virtual_01",
                    "status": "running",
                }
            )

        with self.assertRaisesRegex(ValueError, "observed_at is required"):
            parse_trader_action_payload(
                {
                    "deployment_key": "long_trial_virtual_01",
                    "symbol": "SIVE",
                    "action_name": "market_entry",
                    "action_status": "translated",
                }
            )

        with self.assertRaisesRegex(ValueError, "last_seen_at is required"):
            parse_trader_heartbeat_payload({"status": "running"})

    def test_parse_rl_observation_build_payload_accepts_source_bars(self) -> None:
        payload = parse_rl_observation_build_payload(
            {
                "deployment_key": "Long_Trial_106_Virtual_Shared_01",
                "symbols": ["axfo", "azn"],
                "as_of": "2026-04-28T09:07:30+02:00",
                "source_bars": {"AXFO": []},
                "history_overrides": {"AXFO": {"prev_close": "100"}},
                "static_features": {
                    "AXFO": {
                        "feature_names": ["rank_score_z"],
                        "values": ["0.25"],
                    }
                },
                "include_source_bars": True,
            }
        )

        self.assertEqual(payload["deployment_key"], "long_trial_106_virtual_shared_01")
        self.assertEqual(payload["symbols"], ("AXFO", "AZN"))
        self.assertEqual(payload["as_of"].isoformat(), "2026-04-28T09:07:30+02:00")
        self.assertEqual(
            payload["static_features"]["AXFO"]["feature_names"],
            ["rank_score_z"],
        )
        self.assertTrue(payload["include_source_bars"])

    def test_stockholm_intraday_backfill_endpoint_returns_paged_batch(self) -> None:
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

        expected_payload = {
            "query": {
                "as_of_date": "2026-04-24",
                "bar_size": "1 min",
                "what_to_show": ["TRADES", "MIDPOINT"],
                "use_rth": True,
                "max_symbols": 2,
                "start_after": None,
                "symbols": None,
                "include_remapped": False,
                "sleep_seconds": 0.0,
                "max_runtime_seconds": 55.0,
            },
            "universe": {
                "stockholm_instruments_path": "/tmp/all.txt",
                "stockholm_identity_path": "/tmp/identity.parquet",
                "current_universe_size": 705,
                "page_size": 2,
                "next_cursor": "sive",
                "requested_page_next_cursor": "sive",
            },
            "summary": {
                "requested_symbol_count": 2,
                "processed_symbol_count": 2,
                "ok_count": 2,
                "lookup_error_count": 0,
                "timeout_count": 0,
                "error_count": 0,
                "partial_count": 0,
                "skipped_remapped_count": 0,
                "unsupported_series_count": 0,
                "not_requested_series_count": 0,
                "resolves_cleanly_count": 2,
                "resolves_suspiciously_remapped_count": 0,
                "budget_exhausted": False,
                "elapsed_seconds": 0.0,
            },
            "entries": [],
        }

        with (
            patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
            patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
            patch(
                "ibkr_trader.ibkr.session_manager.ManagedSyncSession.execute",
                side_effect=lambda _operation_name, callback, **_kwargs: callback(None),
            ),
            patch(
                "ibkr_trader.api.server.collect_stockholm_intraday_backfill",
                return_value=expected_payload,
            ) as collect_mock,
            TestClient(app) as client,
        ):
            response = client.post(
                "/v1/market-data/stockholm-intraday-backfill",
                json={
                    "as_of_date": "2026-04-24",
                    "what_to_show": ["trades", "midpoint"],
                    "max_symbols": 2,
                    "sleep_seconds": 0.0,
                },
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["accepted"])
        self.assertEqual(body["market"], "stockholm")
        self.assertEqual(body["series_mode"], "paged_batch")
        self.assertEqual(body["summary"]["requested_symbol_count"], 2)
        self.assertEqual(body["universe"]["next_cursor"], "sive")
        collect_mock.assert_called_once()

    def test_rl_registry_endpoints_round_trip(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "rl_registry.db"
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
                register_response = client.post(
                    "/v1/rl/models/register",
                    json={
                        "model_key": "short_trial36_v1",
                        "display_name": "Short Trial 36 V1",
                        "strategy_family": "canonical_short_live_execution_policy",
                        "side": "SHORT",
                        "action_space": ["skip", "market_entry", "exit_market"],
                        "observation_contract": {
                            "bar_family": "stockholm_intraday_1m_v1"
                        },
                    },
                )
                self.assertEqual(register_response.status_code, 200)

                upsert_response = client.post(
                    "/v1/rl/models/upsert",
                    json={
                        "model_key": "short_trial36_v1",
                        "display_name": "Short Trial 36 V1",
                        "strategy_family": "canonical_short_live_execution_policy",
                        "side": "SHORT",
                        "action_space": [
                            "skip",
                            "market_entry",
                            "exit_market",
                            "exit_tp_180bp",
                        ],
                        "observation_contract": {
                            "bar_family": "phase1_intraday_ohlc_v1",
                            "bar_interval": "5m",
                        },
                        "execution_mapping_version": "short_actions_v1",
                    },
                )
                self.assertEqual(upsert_response.status_code, 200)
                self.assertEqual(
                    upsert_response.json()["trader_model"]["observation_contract"][
                        "bar_family"
                    ],
                    "phase1_intraday_ohlc_v1",
                )

                deployment_response = client.post(
                    "/v1/rl/deployments",
                    json={
                        "deployment_key": "short_trial36_live_01",
                        "model_key": "short_trial36_v1",
                        "account_key": "U25245596",
                        "book_key": "rl_short_trial36_live_01",
                        "mode": "live",
                        "status": "running",
                        "allowed_symbols": ["SIVE", "VOLV-B"],
                    },
                )
                self.assertEqual(deployment_response.status_code, 200)

                update_deployment_response = client.patch(
                    "/v1/rl/deployments/short_trial36_live_01",
                    json={
                        "allowed_symbols": ["SIVE", "VOLV-B", "ERIC-B"],
                        "risk_limits": {"max_open_positions": 3},
                        "metadata": {"edited_by": "test"},
                    },
                )
                self.assertEqual(update_deployment_response.status_code, 200)
                updated_deployment = update_deployment_response.json()[
                    "trader_deployment"
                ]
                self.assertEqual(
                    updated_deployment["allowed_symbols"],
                    ["SIVE", "VOLV-B", "ERIC-B"],
                )
                self.assertEqual(
                    updated_deployment["risk_limits"]["max_open_positions"],
                    3,
                )

                action_response = client.post(
                    "/v1/rl/actions/log",
                    json={
                        "deployment_key": "short_trial36_live_01",
                        "symbol": "SIVE",
                        "action_name": "market_entry",
                        "observed_at": "2026-04-25T09:25:00+02:00",
                        "state_before": "FLAT",
                        "state_after": "ENTRY_PENDING",
                        "action_status": "translated",
                    },
                )
                self.assertEqual(action_response.status_code, 200)

                heartbeat_response = client.post(
                    "/v1/rl/deployments/short_trial36_live_01/heartbeat",
                    json={
                        "status": "running",
                        "last_seen_at": "2026-04-25T09:30:00+02:00",
                        "last_bar_at": "2026-04-25T09:29:00+02:00",
                        "metrics": {"bar_lag_seconds": 4},
                    },
                )
                self.assertEqual(heartbeat_response.status_code, 200)

                dashboard_response = client.get("/v1/read/rl-dashboard")
                self.assertEqual(dashboard_response.status_code, 200)
                body = dashboard_response.json()
                self.assertTrue(body["accepted"])
                self.assertEqual(body["rl_dashboard"]["summary"]["model_count"], 1)
                self.assertEqual(
                    body["rl_dashboard"]["summary"]["deployment_count"],
                    1,
                )
                self.assertEqual(
                    body["rl_dashboard"]["summary"]["recent_action_count"],
                    1,
                )
                self.assertEqual(
                    body["rl_dashboard"]["deployments"][0]["account_key"],
                    "U25245596",
                )
                self.assertEqual(
                    body["rl_dashboard"]["recent_actions"][0]["action_name"],
                    "market_entry",
                )

    def test_rl_observation_endpoint_builds_model_facing_prefix(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        def bars_for_live_day() -> list[dict[str, str]]:
            bars: list[dict[str, str]] = []
            for minute in range(8):
                bars.append(
                    {
                        "timestamp": f"20260428 09:{minute:02d}:00",
                        "open": f"{110 + minute:.2f}",
                        "high": f"{111 + minute:.2f}",
                        "low": f"{109 + minute:.2f}",
                        "close": f"{110.5 + minute:.2f}",
                    }
                )
            return bars

        history_features = {
            "prev_open_rel_close": 0.01,
            "prev_high_rel_close": 0.02,
            "prev_low_rel_close": -0.01,
            "prev_close_rel_open": 0.03,
            "prev_high_rel_low": 0.04,
            "trailing_intraday_realized_vol": 0.02,
            "trailing_session_count_norm": 0.5,
        }

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "rl_observations.db"
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
                register_response = client.post(
                    "/v1/rl/models/register",
                    json={
                        "model_key": "long_trial_106_v1",
                        "display_name": "Long Trial 106 V1",
                        "strategy_family": "canonical_long_live_execution_policy",
                        "side": "LONG",
                        "action_space": ["skip", "wait", "market_entry"],
                        "observation_contract": {
                            "bar_family": "phase1_intraday_ohlc_v1",
                            "bar_interval": "5m",
                            "session_timezone": "Europe/Stockholm",
                            "session_open_local": "09:00",
                            "session_close_local": "17:30",
                            "include_market_context": False,
                            "include_vol_normalized_intraday_state": True,
                        },
                    },
                )
                self.assertEqual(register_response.status_code, 200)
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
                    },
                )
                self.assertEqual(deployment_response.status_code, 200)
                observation_response = client.post(
                    "/v1/rl/observations/build",
                    json={
                        "deployment_key": "long_trial_106_virtual_shared_01",
                        "symbols": ["AXFO"],
                        "as_of": "2026-04-28T09:07:30+02:00",
                        "source_bars": {"AXFO": bars_for_live_day()},
                        "history_overrides": {
                            "AXFO": {
                                "prev_close": "100",
                                "history_features": history_features,
                            }
                        },
                    },
                )

        self.assertEqual(observation_response.status_code, 200)
        body = observation_response.json()
        self.assertTrue(body["accepted"])
        observation = body["rl_observation"]
        self.assertEqual(observation["input_contract"]["bar_interval"], "5m")
        self.assertEqual(observation["input_contract"]["decision_cadence"], "5m")
        self.assertEqual(
            observation["observations"]["AXFO"]["latest_bar_complete"],
            False,
        )
        self.assertEqual(
            observation["observations"]["AXFO"]["model_decision"]["usable_bar_count"],
            1,
        )
        self.assertAlmostEqual(
            observation["observations"]["AXFO"]["features"]["base_dynamic"][1][0],
            1.0 / 101.0,
        )

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

    def test_parse_market_stream_subscribe_payload_normalizes_symbols(self) -> None:
        payload = parse_market_stream_subscribe_payload(
            {
                "symbols": ["axfo", "azn"],
                "market_data_type": "delayed",
                "replace": True,
            }
        )

        self.assertEqual([item.symbol for item in payload["contracts"]], ["AXFO", "AZN"])
        self.assertEqual(payload["contracts"][0].exchange, "SMART")
        self.assertEqual(payload["contracts"][0].primary_exchange, "SFB")
        self.assertEqual(payload["market_data_type"], "DELAYED")
        self.assertTrue(payload["replace"])

    def test_market_stream_contracts_for_open_orders_uses_stockholm_stream_defaults(
        self,
    ) -> None:
        contracts = market_stream_contracts_for_open_orders(
            {
                18: BrokerOpenOrder(
                    order_id=18,
                    perm_id=10018,
                    client_id=0,
                    status="PreSubmitted",
                    order_ref="2026-04-29-U25245596-live_top1_31_seedpicker-HTRO-long-01",
                    action="SELL",
                    total_quantity=None,
                    symbol="htro",
                    security_type="STK",
                    exchange="SFB",
                    primary_exchange=None,
                    currency="SEK",
                    local_symbol="HTRO",
                ),
                19: BrokerOpenOrder(
                    order_id=19,
                    perm_id=10019,
                    client_id=0,
                    status="Cancelled",
                    order_ref=None,
                    action="SELL",
                    total_quantity=None,
                    symbol="OLD",
                    security_type="STK",
                    exchange="SMART",
                    primary_exchange="SFB",
                    currency="SEK",
                ),
            }
        )

        self.assertEqual(len(contracts), 1)
        self.assertEqual(contracts[0].symbol, "HTRO")
        self.assertEqual(contracts[0].security_type, "STK")
        self.assertEqual(contracts[0].exchange, "SMART")
        self.assertEqual(contracts[0].primary_exchange, "SFB")
        self.assertEqual(contracts[0].currency, "SEK")
        self.assertEqual(contracts[0].local_symbol, "HTRO")

    def test_market_stream_contracts_for_runtime_holdings_subscribes_positions(
        self,
    ) -> None:
        contracts = market_stream_contracts_for_runtime_holdings(
            SimpleNamespace(
                portfolio=(),
                positions=(
                    SimpleNamespace(
                        account="U25245596",
                        symbol="hm b",
                        security_type="STK",
                        exchange="SFB",
                        primary_exchange=None,
                        currency="SEK",
                        local_symbol="HM B",
                        position="2",
                    ),
                    SimpleNamespace(
                        account="U25245596",
                        symbol="OLD",
                        security_type="STK",
                        exchange="SFB",
                        primary_exchange=None,
                        currency="SEK",
                        local_symbol="OLD",
                        position="0",
                    ),
                ),
            )
        )

        self.assertEqual([contract.symbol for contract in contracts], ["HM B"])
        self.assertEqual(contracts[0].exchange, "SMART")
        self.assertEqual(contracts[0].primary_exchange, "SFB")

    def test_market_stream_contracts_for_open_virtual_positions_subscribes_holdings(
        self,
    ) -> None:
        engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(engine)
        session_factory = create_session_factory(engine)
        session = session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind="VIRTUAL",
                account_key="VIRTUALRL01",
                account_label="Virtual RL",
                base_currency="SEK",
                is_virtual=True,
            )
            session.add(broker_account)
            session.flush()
            session.add_all(
                [
                    PositionSnapshotRecord(
                        broker_account_id=broker_account.id,
                        is_virtual=True,
                        snapshot_at=datetime(2026, 4, 29, 14, 0, tzinfo=timezone.utc),
                        source="virtual_execution",
                        symbol="AZN",
                        exchange="SFB",
                        currency="SEK",
                        security_type="STK",
                        primary_exchange=None,
                        local_symbol="AZN",
                        quantity="1",
                        average_cost="1700",
                        market_price="1701",
                        market_value="1701",
                        unrealized_pnl="1",
                        realized_pnl="0",
                    ),
                    PositionSnapshotRecord(
                        broker_account_id=broker_account.id,
                        is_virtual=True,
                        snapshot_at=datetime(2026, 4, 29, 14, 1, tzinfo=timezone.utc),
                        source="virtual_execution",
                        symbol="OLD",
                        exchange="SFB",
                        currency="SEK",
                        security_type="STK",
                        primary_exchange=None,
                        local_symbol="OLD",
                        quantity="0",
                        average_cost=None,
                        market_price="10",
                        market_value="0",
                        unrealized_pnl="0",
                        realized_pnl="0",
                    ),
                ]
            )
            session.commit()
        finally:
            session.close()

        try:
            contracts = market_stream_contracts_for_current_holdings(
                session_factory,
            )
            self.assertEqual([contract.symbol for contract in contracts], ["AZN"])
            self.assertEqual(contracts[0].exchange, "SMART")
            self.assertEqual(contracts[0].primary_exchange, "SFB")
            self.assertEqual(contracts[0].local_symbol, "AZN")

            virtual_contracts = market_stream_contracts_for_open_virtual_positions(
                session_factory,
            )
            self.assertEqual(
                [contract.symbol for contract in virtual_contracts],
                ["AZN"],
            )
        finally:
            engine.dispose()

    def test_operator_snapshot_stream_overlay_marks_positions_orders_and_accounts(
        self,
    ) -> None:
        snapshot = {
            "accounts": [
                {
                    "account_key": "U25245596",
                    "net_liquidation": "100000",
                    "day_performance": {
                        "start_net_liquidation": "100000",
                        "latest_return_pct": "0.00",
                        "points": [
                            {
                                "snapshot_at": "2026-04-29T07:00:00+00:00",
                                "net_liquidation": "100000",
                                "return_pct": "0.00",
                            }
                        ],
                    },
                }
            ],
            "positions": [
                {
                    "account_key": "U25245596",
                    "symbol": "AZN",
                    "quantity": "2",
                    "average_cost": "100",
                    "market_price": "101",
                    "market_value": "202",
                    "unrealized_pnl": "2",
                }
            ],
            "open_orders": [
                {
                    "account_key": "U25245596",
                    "symbol": "AZN",
                    "working_price": "104",
                    "working_price_reference": "LIMIT",
                    "limit_price": "104",
                }
            ],
        }
        stream_snapshot = {
            "running": True,
            "desired_subscription_count": 1,
            "quote_count": 0,
            "bars_by_symbol": {
                "AZN": [
                    {
                        "timestamp": "2026-04-29T07:01:00+00:00",
                        "close": "102",
                    },
                    {
                        "timestamp": "2026-04-29T07:02:00+00:00",
                        "close": "103",
                    },
                ]
            },
        }

        enriched = enrich_operator_snapshot_with_market_stream(
            snapshot,
            stream_snapshot,
        )

        self.assertEqual(enriched["positions"][0]["market_price"], "103")
        self.assertEqual(enriched["positions"][0]["market_value"], "206")
        self.assertEqual(enriched["positions"][0]["unrealized_pnl"], "6")
        self.assertEqual(enriched["open_orders"][0]["reference_market_price"], "103")
        self.assertEqual(enriched["open_orders"][0]["last_market_price_direction"], "UP")
        self.assertEqual(enriched["open_orders"][0]["price_spread"], "+1.00")
        self.assertEqual(enriched["accounts"][0]["net_liquidation"], "100004")
        self.assertEqual(
            enriched["accounts"][0]["day_performance"]["latest_return_pct"],
            "0.00",
        )
        self.assertTrue(enriched["market_stream_overlay"]["applied"])

    def test_operator_snapshot_stream_overlay_does_not_double_count_live_position_value(
        self,
    ) -> None:
        snapshot = {
            "accounts": [
                {
                    "account_key": "U25245596",
                    "is_virtual": False,
                    "net_liquidation": "18716.12",
                    "day_performance": {"points": []},
                }
            ],
            "positions": [
                {
                    "account_key": "U25245596",
                    "symbol": "HTRO",
                    "quantity": "518",
                    "average_cost": "34.01559531",
                    "market_price": None,
                    "market_value": "0",
                    "unrealized_pnl": None,
                }
            ],
            "open_orders": [],
        }

        enriched = enrich_operator_snapshot_with_market_stream(
            snapshot,
            {
                "running": True,
                "bars_by_symbol": {
                    "HTRO": [
                        {
                            "timestamp": "2026-04-29T14:44:00+00:00",
                            "close": "32.21",
                        }
                    ]
                },
            },
        )

        self.assertEqual(enriched["positions"][0]["market_value"], "16684.78")
        self.assertEqual(enriched["accounts"][0]["net_liquidation"], "18716.12")
        self.assertEqual(enriched["market_stream_overlay"]["marked_account_count"], 0)

    def test_parse_market_stream_subscribe_payload_enriches_stockholm_identity(self) -> None:
        payload = parse_market_stream_subscribe_payload(
            {
                "symbols": ["eric-b"],
                "market_data_type": "live",
            },
            stockholm_identity_map={
                "ERIC-B": SimpleNamespace(
                    ticker_alias="ERIC B",
                    isin="SE0000108656",
                )
            },
        )

        contract = payload["contracts"][0]
        self.assertEqual(contract.symbol, "ERIC-B")
        self.assertEqual(contract.local_symbol, "ERIC B")
        self.assertEqual(contract.isin, "SE0000108656")

    def test_parse_market_stream_subscribe_payload_enriches_share_class_alias(
        self,
    ) -> None:
        payload = parse_market_stream_subscribe_payload(
            {
                "symbols": ["eric b"],
                "market_data_type": "live",
            },
            stockholm_identity_map={
                "ERIC-B": SimpleNamespace(
                    ticker_alias="ERIC B",
                    isin="SE0000108656",
                )
            },
        )

        contract = payload["contracts"][0]
        self.assertEqual(contract.symbol, "ERIC B")
        self.assertEqual(contract.local_symbol, "ERIC B")
        self.assertEqual(contract.isin, "SE0000108656")

    def test_tick_stream_sample_endpoint_returns_stream_events(self) -> None:
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

        expected_payload = {
            "query": {
                "symbol": "AAPL",
                "exchange": "SMART",
                "currency": "USD",
                "tick_types": ["Last"],
            },
            "event_count": 1,
            "events": [
                {
                    "stream": "Last",
                    "timestamp": "2026-04-27T13:31:00Z",
                    "price": "180.25",
                    "size": "100",
                }
            ],
        }

        with (
            patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
            patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
            patch(
                "ibkr_trader.api.server.collect_tick_stream_sample",
                return_value=expected_payload,
            ) as collect_mock,
            TestClient(app) as client,
        ):
            response = client.post(
                "/v1/market-data/tick-stream-sample",
                json={
                    "symbol": "aapl",
                    "exchange": "smart",
                    "currency": "usd",
                    "primary_exchange": "nasdaq",
                    "tick_types": ["last"],
                    "duration_seconds": 1,
                    "max_events": 1,
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["event_count"], 1)
        collect_mock.assert_called_once()
        query = collect_mock.call_args.args[1]
        self.assertEqual(query.symbol, "AAPL")
        self.assertEqual(query.primary_exchange, "NASDAQ")
        self.assertEqual(query.tick_types, ("Last",))

    def test_rl_observation_endpoint_reads_market_stream_by_default(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        class FakeMarketStreamService:
            def snapshot(self, *, symbols=None, bar_limit=390):
                _ = bar_limit
                return {
                    "bars_by_symbol": {
                        symbol: [
                            {
                                "timestamp": "2026-04-28T09:00:00+02:00",
                                "open": "100",
                                "high": "101",
                                "low": "99",
                                "close": "100",
                                "currency": "SEK",
                            },
                            {
                                "timestamp": "2026-04-28T09:01:00+02:00",
                                "open": "100",
                                "high": "102",
                                "low": "100",
                                "close": "101",
                                "currency": "SEK",
                            },
                            {
                                "timestamp": "2026-04-28T09:05:00+02:00",
                                "open": "101",
                                "high": "103",
                                "low": "101",
                                "close": "102",
                                "currency": "SEK",
                            },
                        ]
                        for symbol in symbols or []
                    }
                }

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "stream_observation.db"
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
                client.app.state.market_stream_service = FakeMarketStreamService()
                self.assertEqual(
                    client.post(
                        "/v1/rl/models/register",
                        json={
                            "model_key": "long_trial_106_v1",
                            "display_name": "Long Trial 106 V1",
                            "strategy_family": "canonical_long_live_execution_policy",
                            "side": "LONG",
                            "action_space": ["skip", "wait", "market_entry"],
                            "observation_contract": {
                                "bar_family": "phase1_intraday_ohlc_v1",
                                "bar_interval": "5m",
                                "session_timezone": "Europe/Stockholm",
                                "session_open_local": "09:00",
                                "session_close_local": "17:30",
                                "include_market_context": False,
                            },
                        },
                    ).status_code,
                    200,
                )
                self.assertEqual(
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
                        },
                    ).status_code,
                    200,
                )
                response = client.post(
                    "/v1/rl/observations/build",
                    json={
                        "deployment_key": "long_trial_106_virtual_shared_01",
                        "symbols": ["AXFO"],
                        "as_of": "2026-04-28T09:07:30+02:00",
                        "history_overrides": {
                            "AXFO": {
                                "prev_close": "100",
                                "history_features": {
                                    "prev_open_rel_close": 0.0,
                                    "prev_high_rel_close": 0.02,
                                    "prev_low_rel_close": -0.02,
                                    "prev_close_rel_open": 0.0,
                                    "prev_high_rel_low": 0.04,
                                    "trailing_intraday_realized_vol": 0.01,
                                    "trailing_session_count_norm": 1.0,
                                },
                            }
                        },
                    },
                )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["source_mode"], "market_stream")
        self.assertEqual(body["streamed_symbols"], ["AXFO"])
        self.assertEqual(
            body["rl_observation"]["observations"]["AXFO"]["model_decision"]["usable_bar_count"],
            1,
        )

    def test_market_stream_endpoints_use_persistent_service(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        class FakeMarketStreamService:
            def __init__(self) -> None:
                self.calls = []

            def subscribe_many(self, contracts, *, replace, market_data_type):
                self.calls.append((contracts, replace, market_data_type))
                return {
                    "running": True,
                    "subscribed_count": len(contracts),
                    "subscriptions": [],
                    "quote_count": 0,
                    "quotes": [],
                    "bars_by_symbol": {contract.symbol: [] for contract in contracts},
                    "errors": [],
                }

            def snapshot(self, *, symbols=None, bar_limit=390):
                return {
                    "running": True,
                    "subscribed_count": 2,
                    "subscriptions": [],
                    "quote_count": 0,
                    "quotes": [],
                    "bars_by_symbol": {symbol: [] for symbol in symbols or []},
                    "errors": [],
                    "bar_limit": bar_limit,
                }

            def stop(self):
                self.calls.append(("stop",))

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
        fake_service = FakeMarketStreamService()

        with (
            patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
            patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
            TestClient(app) as client,
        ):
            client.app.state.market_stream_service = fake_service
            subscribe_response = client.post(
                "/v1/market-data/stream/subscribe",
                json={"symbols": ["axfo", "azn"], "market_data_type": "delayed"},
            )
            snapshot_response = client.get(
                "/v1/market-data/stream/snapshot?symbols=AXFO,AZN&bar_limit=10"
            )

        self.assertEqual(subscribe_response.status_code, 200)
        self.assertEqual(snapshot_response.status_code, 200)
        self.assertEqual(subscribe_response.json()["stream"]["subscribed_count"], 2)
        contracts, replace, market_data_type = fake_service.calls[0]
        self.assertEqual([contract.symbol for contract in contracts], ["AXFO", "AZN"])
        self.assertTrue(replace)
        self.assertEqual(market_data_type, "DELAYED")
        self.assertEqual(
            sorted(snapshot_response.json()["stream"]["bars_by_symbol"]),
            ["AXFO", "AZN"],
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

    def test_background_execution_recovery_runs_when_instruction_is_active(self) -> None:
        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "recovery_active.db"
            engine = build_engine(f"sqlite+pysqlite:///{database_path}")
            create_schema(engine)
            session_factory = create_session_factory(engine)

            session = session_factory()
            try:
                session.add(
                    InstructionRecord(
                        instruction_id="runtime-sive-1",
                        schema_version="2026-04-10",
                        source_system="q-training",
                        batch_id="batch-1",
                        account_key="GTW05",
                        book_key="long_risk_book",
                        symbol="SIVE",
                        exchange="SMART",
                        currency="SEK",
                        state="EXIT_PENDING",
                        submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
                        expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
                        order_type="LIMIT",
                        side="BUY",
                        payload={"instruction": {"instruction_id": "runtime-sive-1"}},
                    )
                )
                session.commit()
            finally:
                session.close()

            self.assertTrue(should_include_background_execution_recovery(session_factory))

    def test_background_execution_recovery_runs_when_broker_order_is_unsettled(self) -> None:
        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "recovery_order.db"
            engine = build_engine(f"sqlite+pysqlite:///{database_path}")
            create_schema(engine)
            session_factory = create_session_factory(engine)

            session = session_factory()
            try:
                broker_account = BrokerAccountRecord(
                    broker_kind="IBKR",
                    account_key="GTW05",
                    base_currency="SEK",
                )
                session.add(broker_account)
                session.flush()
                session.add(
                    BrokerOrderRecord(
                        instruction_id=None,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="GTW05",
                        order_role="EXIT",
                        external_order_id="3953",
                        external_perm_id="449407988",
                        external_client_id="0",
                        order_ref="runtime-sive-1:exit:forced",
                        symbol="SIVE",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        side="SELL",
                        order_type="MKT",
                        status="PendingCancel",
                        total_quantity="100",
                        submitted_at=datetime(2026, 4, 10, 7, 30, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 10, 7, 31, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    )
                )
                session.commit()
            finally:
                session.close()

            self.assertTrue(should_include_background_execution_recovery(session_factory))

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

    def test_rl_candidates_endpoint_returns_model_routed_names(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "rl_candidates.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            session_factory = create_session_factory(engine)
            session = session_factory()
            try:
                trader_model = TraderModelRecord(
                    model_key="long_trial_106_v1",
                    display_name="Long Trial 106 V1",
                    strategy_family="canonical_long_live_execution_policy",
                    side="LONG",
                    action_space_json=["wait", "entry_prevclose_-50bp"],
                    observation_contract_json={"bar_family": "phase1_intraday_ohlc_v1"},
                    execution_mapping_version="long_actions_v1",
                    metadata_json={},
                )
                session.add(trader_model)
                session.flush()
                session.add(
                    TraderDeploymentRecord(
                        trader_model_id=trader_model.id,
                        deployment_key="long_trial_106_virtual_shared_01",
                        account_key="VIRTUALRL01",
                        book_key="rl_shared_long_trial_106_virtual_01",
                        mode="virtual",
                        status="running",
                        is_virtual=True,
                        allowed_symbols_json=["AXFO"],
                        risk_limits_json={},
                        action_constraints_json={},
                        metadata_json={},
                    )
                )
                session.add(
                    InstructionRecord(
                        instruction_id="candidate-AXFO",
                        schema_version="2026-04-25",
                        source_system="upstream-agent",
                        batch_id="candidate-batch-001",
                        account_key="VIRTUALRL01",
                        book_key="rl_shared_long_trial_106_virtual_01",
                        symbol="AXFO",
                        exchange="XSTO",
                        currency="SEK",
                        state="MODEL_ROUTED_PENDING",
                        submit_at=datetime(2099, 4, 28, 7, 0, tzinfo=timezone.utc),
                        expire_at=datetime(2099, 4, 28, 15, 30, tzinfo=timezone.utc),
                        order_type="MODEL_ROUTED",
                        side="BUY",
                        payload={
                            "schema_version": "2026-04-25",
                            "source": {
                                "system": "upstream-agent",
                                "batch_id": "candidate-batch-001",
                                "generated_at": "2099-04-28T06:30:00Z",
                            },
                            "instruction": {
                                "instruction_id": "candidate-AXFO",
                                "account": {
                                    "account_key": "VIRTUALRL01",
                                    "book_key": "rl_shared_long_trial_106_virtual_01",
                                },
                                "instrument": {
                                    "symbol": "AXFO",
                                    "security_type": "STK",
                                    "exchange": "XSTO",
                                    "currency": "SEK",
                                },
                                "intent": {
                                    "side": "BUY",
                                    "position_side": "LONG",
                                },
                                "sizing": {
                                    "mode": "target_notional",
                                    "target_notional": "1000",
                                },
                                "execution": {
                                    "mode": "model_routed",
                                    "model_id": "long_trial_106_v1",
                                    "model_family": (
                                        "canonical_long_live_execution_policy"
                                    ),
                                    "window": {
                                        "start_at": "2099-04-28T09:00:00+02:00",
                                        "end_at": "2099-04-28T17:30:00+02:00",
                                    },
                                },
                                "trace": {
                                    "reason_code": "rl_model_routed_candidate",
                                    "trade_date": "2099-04-28",
                                },
                            },
                        },
                    )
                )
                session.add(
                    InstructionRecord(
                        instruction_id="entry-instruction-001",
                        schema_version="2026-04-10",
                        source_system="q-training",
                        batch_id="entry-batch-001",
                        account_key="VIRTUALRL01",
                        book_key="rl_shared_long_trial_106_virtual_01",
                        symbol="AXFO",
                        exchange="XSTO",
                        currency="SEK",
                        state="ENTRY_PENDING",
                        submit_at=datetime(2026, 4, 28, 7, 0, tzinfo=timezone.utc),
                        expire_at=datetime(2026, 4, 28, 15, 30, tzinfo=timezone.utc),
                        order_type="LIMIT",
                        side="BUY",
                        payload={"instruction": {"instruction_id": "entry-instruction-001"}},
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
                response = client.get(
                    "/v1/rl/candidates",
                    params={"deployment_key": "long_trial_106_virtual_shared_01"},
                )

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertTrue(body["accepted"])
            self.assertEqual(body["candidate_count"], 1)
            candidate = body["candidates"][0]
            self.assertEqual(candidate["candidate_id"], "candidate-AXFO")
            self.assertEqual(candidate["state"], "MODEL_ROUTED_PENDING")
            self.assertEqual(candidate["model_id"], "long_trial_106_v1")
            self.assertEqual(candidate["symbol"], "AXFO")
            self.assertEqual(candidate["execution_window"]["start_at"], "2099-04-28T09:00:00+02:00")
            self.assertEqual(
                candidate["candidate"]["instruction_id"],
                "candidate-AXFO",
            )

    def test_rl_dashboard_archives_expired_candidate_sources(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "rl_dashboard_rollover.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            session_factory = create_session_factory(engine)
            session = session_factory()
            try:
                for instruction_id, expire_at in (
                    (
                        "expired-candidate",
                        datetime(2000, 1, 1, 15, 30, tzinfo=timezone.utc),
                    ),
                    (
                        "future-candidate",
                        datetime(2099, 1, 1, 15, 30, tzinfo=timezone.utc),
                    ),
                ):
                    session.add(
                        InstructionRecord(
                            instruction_id=instruction_id,
                            schema_version="2026-04-25",
                            source_system="upstream-agent",
                            batch_id="candidate-batch-001",
                            account_key="VIRTUALRL01",
                            book_key="rl_shared_long_trial_106_virtual_01",
                            is_virtual=True,
                            symbol="AXFO",
                            exchange="XSTO",
                            currency="SEK",
                            state="MODEL_ROUTED_PENDING",
                            submit_at=datetime(
                                2000,
                                1,
                                1,
                                7,
                                0,
                                tzinfo=timezone.utc,
                            ),
                            expire_at=expire_at,
                            order_type="MODEL_ROUTED",
                            side="BUY",
                            payload={
                                "instruction": {
                                    "instruction_id": instruction_id,
                                    "execution": {
                                        "mode": "model_routed",
                                        "model_id": "long_trial_106_v1",
                                    },
                                }
                            },
                        )
                    )
                session.add(
                    InstructionRecord(
                        instruction_id="generated-position",
                        schema_version="2026-04-10",
                        source_system="rl-runner",
                        batch_id="generated-batch",
                        account_key="VIRTUALRL01",
                        book_key="rl_shared_long_trial_106_virtual_01",
                        is_virtual=True,
                        symbol="AXFO",
                        exchange="XSTO",
                        currency="SEK",
                        state="POSITION_OPEN",
                        submit_at=datetime(2000, 1, 1, 7, 0, tzinfo=timezone.utc),
                        expire_at=datetime(2099, 1, 1, 7, 0, tzinfo=timezone.utc),
                        order_type="LIMIT",
                        side="BUY",
                        payload={
                            "instruction": {
                                "instruction_id": "generated-position",
                                "trace": {
                                    "metadata": {
                                        "rl_source_instruction_id": (
                                            "expired-candidate"
                                        )
                                    }
                                },
                            }
                        },
                    )
                )
                session.commit()
            finally:
                session.close()

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
                response = client.get("/v1/read/rl-dashboard")

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertEqual(body["rl_dashboard"]["summary"]["candidate_count"], 1)
            self.assertEqual(
                body["rl_dashboard"]["candidates"][0]["candidate_id"],
                "future-candidate",
            )

            session = session_factory()
            try:
                rows = {
                    row.instruction_id: row
                    for row in session.query(InstructionRecord).all()
                }
                self.assertIsNotNone(rows["expired-candidate"].archived_at)
                self.assertIsNone(rows["future-candidate"].archived_at)
                self.assertIsNone(rows["generated-position"].archived_at)
            finally:
                session.close()
                engine.dispose()

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

    def test_operator_review_endpoints_round_trip(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "operator_review.db"
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

                broker_order = BrokerOrderRecord(
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

                broker_event = BrokerOrderEventRecord(
                    broker_order_id=broker_order.id,
                    event_type="order_error_callback",
                    event_at=datetime(2026, 4, 19, 7, 22, tzinfo=timezone.utc),
                    status_before="PreSubmitted",
                    status_after="Submitted",
                    payload={"errorCode": 201, "errorMsg": "Order held for review"},
                    note="Broker callback arrived.",
                )
                session.add(broker_event)

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
                issue = ReconciliationIssueRecord(
                    reconciliation_run_id=reconciliation_run.id,
                    instruction_id="instr-001",
                    stage="reconcile_instruction",
                    severity="ERROR",
                    message="Order state drift detected.",
                    observed_at=datetime(2026, 4, 19, 7, 25, 3, tzinfo=timezone.utc),
                    payload={"broker_order_id": broker_order.id},
                )
                session.add(issue)
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
                attention_response = client.post(
                    "/v1/broker-attention/1/review",
                    json={"action": "ARCHIVE", "updated_by": "test-suite"},
                )
                issue_response = client.post(
                    "/v1/reconciliation-issues/1/review",
                    json={"action": "RESOLVE", "updated_by": "test-suite"},
                )
                archive_response = client.post(
                    "/v1/reconciliation-issues/archive-open",
                    json={"updated_by": "test-suite"},
                )

            self.assertEqual(attention_response.status_code, 200)
            self.assertEqual(
                attention_response.json()["operator_review"]["status"],
                "ARCHIVED",
            )
            self.assertEqual(issue_response.status_code, 200)
            self.assertEqual(
                issue_response.json()["operator_review"]["status"],
                "RESOLVED",
            )
            self.assertEqual(archive_response.status_code, 200)
            self.assertEqual(
                archive_response.json()["reconciliation_issue_archive"][
                    "archived_issue_count"
                ],
                1,
            )

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

    def test_submit_endpoint_accepts_exact_replay_and_rejects_changed_duplicate(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "submit_idempotency.db"
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

            changed_payload = deepcopy(_sample_submit_payload())
            changed_instruction = changed_payload["instructions"][0]
            assert isinstance(changed_instruction, dict)
            changed_entry = changed_instruction["entry"]
            assert isinstance(changed_entry, dict)
            changed_entry["limit_price"] = "11.9999"

            with (
                patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
                patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
                TestClient(app) as client,
            ):
                first = client.post("/v1/instructions/submit", json=_sample_submit_payload())
                replay = client.post("/v1/instructions/submit", json=_sample_submit_payload())
                changed = client.post("/v1/instructions/submit", json=changed_payload)

            self.assertEqual(first.status_code, 200)
            self.assertEqual(replay.status_code, 200)
            self.assertEqual(
                replay.json()["submitted"]["instructions"][0]["record_id"],
                first.json()["submitted"]["instructions"][0]["record_id"],
            )
            self.assertEqual(changed.status_code, 409)
            self.assertIn("different payload", changed.text)

    def test_virtual_account_and_market_watch_endpoints_persist_virtual_rows(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "virtual_api.db"
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
                account_response = client.post(
                    "/v1/virtual/accounts",
                    json={
                        "account_key": "virtual0001",
                        "base_currency": "SEK",
                        "account_label": "RL virtual sandbox",
                        "cash_balance": "200000",
                    },
                )
                quote_response = client.post(
                    "/v1/virtual/market-watch",
                    json={
                        "account_key": "virtual0001",
                        "observed_at": "2026-04-27T09:01:00Z",
                        "symbol": "sive",
                        "security_type": "stk",
                        "exchange": "xsto",
                        "currency": "sek",
                        "bid_price": "10.00",
                        "ask_price": "10.00",
                        "last_price": "10.00",
                        "source": "test-suite",
                    },
                )
                list_response = client.get(
                    "/v1/virtual/market-watch?account_key=virtual0001&limit=5"
                )

            self.assertEqual(account_response.status_code, 200)
            self.assertEqual(quote_response.status_code, 200)
            self.assertEqual(list_response.status_code, 200)
            account_body = account_response.json()["virtual_account"]
            self.assertEqual(account_body["account_key"], "VIRTUAL0001")
            self.assertEqual(account_body["broker_kind"], "VIRTUAL")
            self.assertTrue(account_body["is_virtual"])
            self.assertEqual(account_body["cash_balance"], "200000")

            quote_body = quote_response.json()["virtual_market_watch"]
            self.assertEqual(quote_body["quote"]["account_key"], "VIRTUAL0001")
            self.assertEqual(quote_body["quote"]["symbol"], "SIVE")
            self.assertEqual(quote_body["filled_order_count"], 0)
            self.assertEqual(list_response.json()["quote_count"], 1)

            engine = build_engine(database_url)
            session_factory = create_session_factory(engine)
            session = session_factory()
            try:
                account = (
                    session.query(BrokerAccountRecord)
                    .filter_by(broker_kind="VIRTUAL", account_key="VIRTUAL0001")
                    .one()
                )
                quote = (
                    session.query(VirtualMarketQuoteRecord)
                    .filter_by(account_key="VIRTUAL0001", symbol="SIVE")
                    .one()
                )
                snapshot_count = (
                    session.query(AccountSnapshotRecord)
                    .filter_by(broker_account_id=account.id, is_virtual=True)
                    .count()
                )
                self.assertTrue(account.is_virtual)
                self.assertEqual(quote.currency, "SEK")
                self.assertEqual(quote.ask_price, "10.00")
                self.assertGreaterEqual(snapshot_count, 1)
                latest_snapshot = (
                    session.query(AccountSnapshotRecord)
                    .filter_by(broker_account_id=account.id, is_virtual=True)
                    .order_by(AccountSnapshotRecord.snapshot_at.desc())
                    .first()
                )
                self.assertEqual(latest_snapshot.total_cash_value, "200000")
                self.assertEqual(latest_snapshot.buying_power, "200000")
                self.assertEqual(latest_snapshot.available_funds, "200000")
            finally:
                session.close()
                engine.dispose()

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

    def test_parse_operator_review_payload_requires_valid_action_and_updated_by(self) -> None:
        action, updated_by, note = parse_operator_review_payload(
            {
                "action": "ACKNOWLEDGE",
                "updated_by": "dashboard",
                "note": "Looks good.",
            }
        )

        self.assertEqual(action, "ACKNOWLEDGE")
        self.assertEqual(updated_by, "dashboard")
        self.assertEqual(note, "Looks good.")

        with self.assertRaisesRegex(ValueError, "required"):
            parse_operator_review_payload({})

        with self.assertRaisesRegex(ValueError, "updated_by"):
            parse_operator_review_payload({"action": "ACKNOWLEDGE", "updated_by": "   "})

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
