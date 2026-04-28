from __future__ import annotations

from datetime import datetime
from datetime import timezone
from unittest import TestCase

from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.orchestration.trader_registry import (
    TraderModelConflictError,
    TraderModelNotFoundError,
    create_trader_deployment,
    log_trader_action,
    register_trader_model,
    update_trader_deployment,
    upsert_trader_model,
    upsert_trader_heartbeat,
)


class TraderRegistryTests(TestCase):
    def setUp(self) -> None:
        self.engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(self.engine)
        self.session_factory = create_session_factory(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_register_model_create_deployment_log_action_and_heartbeat(self) -> None:
        model = register_trader_model(
            self.session_factory,
            model_key="short_trial36_v1",
            display_name="Short Trial 36 V1",
            strategy_family="canonical_short_live_execution_policy",
            side="SHORT",
            source_workflow_path="/tmp/workflow.yaml",
            promoted_checkpoint_path="/tmp/best_dqn_state.pt",
            action_space=("skip", "market_entry", "exit_market"),
            observation_contract={"bar_family": "stockholm_intraday_1m_v1"},
            execution_mapping_version="short_actions_v1",
        )
        self.assertEqual(model.model_key, "short_trial36_v1")

        deployment = create_trader_deployment(
            self.session_factory,
            deployment_key="short_trial36_live_01",
            model_key="short_trial36_v1",
            account_key="u25245596",
            book_key="rl_short_trial36_live_01",
            mode="live",
            status="running",
            allowed_symbols=("SIVE", "VOLV-B"),
            risk_limits={"max_open_positions": 8},
            action_constraints={"position_side": "SHORT"},
        )
        self.assertEqual(deployment.account_key, "U25245596")
        self.assertEqual(deployment.allowed_symbols, ("SIVE", "VOLV-B"))

        action = log_trader_action(
            self.session_factory,
            deployment_key="short_trial36_live_01",
            symbol="sive",
            action_name="market_entry",
            observed_at=datetime(2026, 4, 25, 7, 25, tzinfo=timezone.utc),
            state_before="flat",
            state_after="entry_pending",
            payload={"confidence": 0.73},
        )
        self.assertEqual(action.symbol, "SIVE")
        self.assertEqual(action.action_name, "market_entry")

        heartbeat = upsert_trader_heartbeat(
            self.session_factory,
            deployment_key="short_trial36_live_01",
            status="running",
            last_seen_at=datetime(2026, 4, 25, 7, 26, tzinfo=timezone.utc),
            last_bar_at=datetime(2026, 4, 25, 7, 25, tzinfo=timezone.utc),
            metrics={"bar_lag_seconds": 2},
        )
        self.assertEqual(heartbeat.status, "running")
        self.assertEqual(heartbeat.metrics["bar_lag_seconds"], 2)

    def test_register_model_rejects_duplicate_key(self) -> None:
        register_trader_model(
            self.session_factory,
            model_key="short_trial36_v1",
            display_name="Short Trial 36 V1",
            strategy_family="canonical_short_live_execution_policy",
            side="SHORT",
            source_workflow_path=None,
            promoted_checkpoint_path=None,
            action_space=("skip",),
        )

        with self.assertRaises(TraderModelConflictError):
            register_trader_model(
                self.session_factory,
                model_key="short_trial36_v1",
                display_name="Duplicate",
                strategy_family="canonical_short_live_execution_policy",
                side="SHORT",
                source_workflow_path=None,
                promoted_checkpoint_path=None,
                action_space=("skip",),
            )

    def test_upsert_model_updates_existing_metadata(self) -> None:
        register_trader_model(
            self.session_factory,
            model_key="short_trial36_v1",
            display_name="Short Trial 36 V1",
            strategy_family="canonical_short_live_execution_policy",
            side="SHORT",
            source_workflow_path=None,
            promoted_checkpoint_path=None,
            action_space=("skip", "market_entry"),
            observation_contract={"bar_family": "stockholm_intraday_1m_v1"},
            execution_mapping_version="short_actions_v0",
        )

        updated = upsert_trader_model(
            self.session_factory,
            model_key="short_trial36_v1",
            display_name="Short Trial 36 V1",
            strategy_family="canonical_short_live_execution_policy",
            side="SHORT",
            source_workflow_path="/tmp/workflow.yaml",
            promoted_checkpoint_path="/tmp/best_dqn_state.pt",
            action_space=("skip", "market_entry", "exit_tp_180bp"),
            observation_contract={"bar_family": "phase1_intraday_ohlc_v1"},
            execution_mapping_version="short_actions_v1",
            metadata={"feature_adapter": "ibkr_1m_to_phase1_5m_ohlc_v1"},
        )

        self.assertEqual(updated.model_key, "short_trial36_v1")
        self.assertEqual(
            updated.action_space,
            ("skip", "market_entry", "exit_tp_180bp"),
        )
        self.assertEqual(
            updated.observation_contract["bar_family"],
            "phase1_intraday_ohlc_v1",
        )
        self.assertEqual(updated.execution_mapping_version, "short_actions_v1")
        self.assertEqual(
            updated.metadata["feature_adapter"],
            "ibkr_1m_to_phase1_5m_ohlc_v1",
        )

    def test_upsert_model_creates_missing_model(self) -> None:
        created = upsert_trader_model(
            self.session_factory,
            model_key="long_trial_106_v1",
            display_name="Long Trial 106 V1",
            strategy_family="canonical_long_research_execution_policy",
            side="LONG",
            source_workflow_path=None,
            promoted_checkpoint_path=None,
            action_space=("skip", "market_entry", "entry_prevclose_-50bp"),
            observation_contract={"bar_family": "phase1_intraday_ohlc_v1"},
            execution_mapping_version="long_actions_v1",
        )

        self.assertEqual(created.model_key, "long_trial_106_v1")
        self.assertEqual(created.side, "LONG")
        self.assertEqual(created.execution_mapping_version, "long_actions_v1")

    def test_update_deployment_replaces_editable_fields(self) -> None:
        register_trader_model(
            self.session_factory,
            model_key="short_trial36_v1",
            display_name="Short Trial 36 V1",
            strategy_family="canonical_short_live_execution_policy",
            side="SHORT",
            source_workflow_path=None,
            promoted_checkpoint_path=None,
            action_space=("skip", "market_entry"),
        )
        create_trader_deployment(
            self.session_factory,
            deployment_key="short_trial36_virtual_01",
            model_key="short_trial36_v1",
            account_key="virtual0001",
            book_key="rl_short_trial36_virtual_01",
            mode="virtual",
            status="draft",
            allowed_symbols=("SIVE",),
            metadata={"created_from": "test"},
        )

        updated = update_trader_deployment(
            self.session_factory,
            deployment_key="short_trial36_virtual_01",
            status="running",
            allowed_symbols=("volv-b", "sive", "volv-b"),
            risk_limits={"max_open_positions": 2},
            metadata={"edited_by": "test"},
        )

        self.assertEqual(updated.status, "running")
        self.assertEqual(updated.account_key, "VIRTUAL0001")
        self.assertEqual(updated.allowed_symbols, ("VOLV-B", "SIVE"))
        self.assertEqual(updated.risk_limits["max_open_positions"], 2)
        self.assertEqual(updated.metadata["edited_by"], "test")
        self.assertIsNotNone(updated.started_at)

        stopped = update_trader_deployment(
            self.session_factory,
            deployment_key="short_trial36_virtual_01",
            status="stopped",
        )
        self.assertIsNotNone(stopped.stopped_at)

        restarted = update_trader_deployment(
            self.session_factory,
            deployment_key="short_trial36_virtual_01",
            status="running",
        )
        self.assertEqual(restarted.status, "running")
        self.assertIsNone(restarted.stopped_at)
        self.assertIsNone(restarted.paused_at)

    def test_create_deployment_requires_existing_model(self) -> None:
        with self.assertRaises(TraderModelNotFoundError):
            create_trader_deployment(
                self.session_factory,
                deployment_key="short_trial36_live_01",
                model_key="missing_model",
                account_key="U25245596",
                book_key="rl_short_trial36_live_01",
                mode="live",
                status="draft",
            )

    def test_create_deployment_requires_virtual_mode_to_match_virtual_account(self) -> None:
        register_trader_model(
            self.session_factory,
            model_key="long_trial_v1",
            display_name="Long Trial V1",
            strategy_family="canonical_long_live_execution_policy",
            side="LONG",
            source_workflow_path=None,
            promoted_checkpoint_path=None,
            action_space=("skip", "market_entry"),
        )

        with self.assertRaisesRegex(ValueError, "virtual account_key requires mode virtual"):
            create_trader_deployment(
                self.session_factory,
                deployment_key="long_trial_live_bad",
                model_key="long_trial_v1",
                account_key="virtual0001",
                book_key="rl_long_trial",
                mode="live",
                status="draft",
            )

        with self.assertRaisesRegex(ValueError, "mode virtual requires"):
            create_trader_deployment(
                self.session_factory,
                deployment_key="long_trial_virtual_bad",
                model_key="long_trial_v1",
                account_key="U25245596",
                book_key="rl_long_trial",
                mode="virtual",
                status="draft",
            )
