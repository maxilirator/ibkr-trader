from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone
from unittest import TestCase
from unittest.mock import patch

from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.models import TraderActionRecord
from ibkr_trader.db.models import TraderDeploymentRecord
from ibkr_trader.db.models import TraderHeartbeatRecord
from ibkr_trader.db.models import TraderModelRecord
from ibkr_trader.read_models.rl_dashboard import build_rl_trader_dashboard_snapshot


class RLTraderDashboardReadModelTests(TestCase):
    def setUp(self) -> None:
        self.engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(self.engine)
        self.session_factory = create_session_factory(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_build_rl_dashboard_snapshot_returns_models_deployments_and_actions(self) -> None:
        now_at = datetime(2026, 4, 25, 7, 30, tzinfo=timezone.utc)

        with session_scope(self.session_factory) as session:
            model = TraderModelRecord(
                model_key="short_trial36_v1",
                display_name="Short Trial 36 V1",
                strategy_family="canonical_short_live_execution_policy",
                side="SHORT",
                action_space_json=["skip", "market_entry", "exit_market"],
                observation_contract_json={"bar_family": "stockholm_intraday_1m_v1"},
            )
            deployment = TraderDeploymentRecord(
                trader_model=model,
                deployment_key="short_trial36_live_01",
                account_key="U25245596",
                book_key="rl_short_trial36_live_01",
                mode="live",
                status="running",
                allowed_symbols_json=["SIVE"],
            )
            session.add_all(
                [
                    model,
                    deployment,
                    TraderActionRecord(
                        trader_deployment=deployment,
                        observed_at=now_at - timedelta(minutes=2),
                        symbol="SIVE",
                        action_name="market_entry",
                        action_status="translated",
                        state_before="FLAT",
                        state_after="ENTRY_PENDING",
                    ),
                    TraderHeartbeatRecord(
                        trader_deployment=deployment,
                        status="running",
                        last_seen_at=now_at - timedelta(seconds=30),
                        last_bar_at=now_at - timedelta(minutes=1),
                        last_action_at=now_at - timedelta(minutes=2),
                        metrics_json={"bar_lag_seconds": 3},
                    ),
                ]
            )

        with patch("ibkr_trader.read_models.rl_dashboard.utc_now", return_value=now_at):
            snapshot = build_rl_trader_dashboard_snapshot(
                self.session_factory,
                heartbeat_stale_after_seconds=120,
            )

        self.assertEqual(snapshot.summary.model_count, 1)
        self.assertEqual(snapshot.summary.deployment_count, 1)
        self.assertEqual(snapshot.summary.live_deployment_count, 1)
        self.assertEqual(snapshot.summary.running_deployment_count, 1)
        self.assertEqual(snapshot.summary.stale_heartbeat_count, 0)
        self.assertEqual(snapshot.models[0].model_key, "short_trial36_v1")
        self.assertEqual(snapshot.deployments[0].deployment_key, "short_trial36_live_01")
        self.assertEqual(snapshot.deployments[0].heartbeat.status, "running")
        self.assertEqual(snapshot.recent_actions[0].action_name, "market_entry")

    def test_build_rl_dashboard_snapshot_marks_stale_heartbeat(self) -> None:
        now_at = datetime(2026, 4, 25, 7, 30, tzinfo=timezone.utc)

        with session_scope(self.session_factory) as session:
            model = TraderModelRecord(
                model_key="short_trial36_v1",
                display_name="Short Trial 36 V1",
                strategy_family="canonical_short_live_execution_policy",
                side="SHORT",
                action_space_json=["skip"],
                observation_contract_json={},
            )
            deployment = TraderDeploymentRecord(
                trader_model=model,
                deployment_key="short_trial36_live_01",
                account_key="U25245596",
                book_key="rl_short_trial36_live_01",
                mode="live",
                status="running",
            )
            session.add(
                TraderHeartbeatRecord(
                    trader_deployment=deployment,
                    status="running",
                    last_seen_at=now_at - timedelta(minutes=10),
                    metrics_json={},
                )
            )

        with patch("ibkr_trader.read_models.rl_dashboard.utc_now", return_value=now_at):
            snapshot = build_rl_trader_dashboard_snapshot(
                self.session_factory,
                heartbeat_stale_after_seconds=120,
            )

        self.assertTrue(snapshot.deployments[0].heartbeat.is_stale)
        self.assertEqual(snapshot.summary.stale_heartbeat_count, 1)

    def test_stopped_deployments_do_not_count_as_stale(self) -> None:
        now_at = datetime(2026, 4, 25, 7, 30, tzinfo=timezone.utc)

        with session_scope(self.session_factory) as session:
            model = TraderModelRecord(
                model_key="short_trial36_v1",
                display_name="Short Trial 36 V1",
                strategy_family="canonical_short_live_execution_policy",
                side="SHORT",
                action_space_json=["skip"],
                observation_contract_json={},
            )
            deployment = TraderDeploymentRecord(
                trader_model=model,
                deployment_key="short_trial36_virtual_01",
                account_key="VIRTUALRL01",
                book_key="rl_short_trial36_virtual_01",
                mode="virtual",
                status="stopped",
                is_virtual=True,
            )
            session.add(
                TraderHeartbeatRecord(
                    trader_deployment=deployment,
                    status="stopped",
                    last_seen_at=now_at - timedelta(minutes=10),
                    metrics_json={},
                )
            )

        with patch("ibkr_trader.read_models.rl_dashboard.utc_now", return_value=now_at):
            snapshot = build_rl_trader_dashboard_snapshot(
                self.session_factory,
                heartbeat_stale_after_seconds=120,
            )

        self.assertFalse(snapshot.deployments[0].heartbeat.is_stale)
        self.assertEqual(snapshot.summary.stale_heartbeat_count, 0)
