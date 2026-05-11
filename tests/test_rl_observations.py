from __future__ import annotations

from datetime import datetime
from unittest import TestCase

from ibkr_trader.rl.observations import HISTORY_FEATURE_NAMES
from ibkr_trader.rl.observations import build_history_override_from_source_bars
from ibkr_trader.rl.observations import build_phase1_observation_payload


def _bars_for_day(
    day: str,
    *,
    start_price: float,
    minutes: int,
) -> list[dict[str, str]]:
    bars: list[dict[str, str]] = []
    for minute in range(minutes):
        bars.append(
            {
                "timestamp": f"{day} 09:{minute:02d}:00",
                "open": f"{start_price + minute:.2f}",
                "high": f"{start_price + minute + 1.0:.2f}",
                "low": f"{start_price + minute - 1.0:.2f}",
                "close": f"{start_price + minute + 0.5:.2f}",
                "volume": "1000",
                "bar_count": "12",
            }
        )
    return bars


def _bars_for_minutes(
    day: str,
    *,
    start_price: float,
    minutes: list[int],
) -> list[dict[str, str]]:
    bars: list[dict[str, str]] = []
    for idx, minute in enumerate(minutes):
        bars.append(
            {
                "timestamp": f"{day} 09:{minute:02d}:00",
                "open": f"{start_price + idx:.2f}",
                "high": f"{start_price + idx + 1.0:.2f}",
                "low": f"{start_price + idx - 1.0:.2f}",
                "close": f"{start_price + idx + 0.5:.2f}",
                "volume": "1000",
                "bar_count": "12",
            }
        )
    return bars


def _history_override(prev_close: str = "100") -> dict[str, object]:
    return {
        "prev_close": prev_close,
        "history_features": {
            name: 0.01 for name in HISTORY_FEATURE_NAMES
        },
    }


class RLObservationTests(TestCase):
    def test_builds_growing_five_minute_prefix_from_one_minute_bars(self) -> None:
        payload = build_phase1_observation_payload(
            deployment_key="long_trial_106_virtual_shared_01",
            model_key="long_trial_106_v1",
            model_side="LONG",
            observation_contract={
                "bar_family": "phase1_intraday_ohlc_v1",
                "bar_interval": "5m",
                "session_timezone": "Europe/Stockholm",
                "session_open_local": "09:00",
                "session_close_local": "17:30",
                "include_market_context": True,
                "include_vol_normalized_intraday_state": True,
            },
            action_space=["skip", "wait", "market_entry"],
            as_of=datetime.fromisoformat("2026-04-28T09:07:30+02:00"),
            symbols=["AXFO", "AZN"],
            source_bars_by_symbol={
                "AXFO": _bars_for_day("20260427", start_price=100.0, minutes=10)
                + _bars_for_day("20260428", start_price=110.0, minutes=8),
                "AZN": _bars_for_day("20260427", start_price=200.0, minutes=10)
                + _bars_for_day("20260428", start_price=210.0, minutes=8),
            },
        )

        self.assertEqual(payload["input_contract"]["bar_interval"], "5m")
        self.assertEqual(payload["input_contract"]["update_cadence"], "1m")
        self.assertEqual(payload["input_contract"]["decision_cadence"], "5m")
        self.assertEqual(
            payload["input_contract"]["decision_policy"],
            "completed_5m_bar_only",
        )
        self.assertEqual(payload["input_contract"]["expected_session_bars"], 102)
        self.assertIn(
            "runtime_dynamic_from_runner_state",
            payload["feature_schema"]["model_input_component_order"],
        )
        self.assertEqual(payload["feature_schema"]["path_pad_length"], 102)
        axfo = payload["observations"]["AXFO"]
        self.assertEqual(axfo["bar_count"], 2)
        self.assertTrue(axfo["phase1_bars"][0]["complete"])
        self.assertFalse(axfo["phase1_bars"][1]["complete"])
        self.assertTrue(axfo["model_decision"]["ready"])
        self.assertEqual(axfo["model_decision"]["usable_bar_count"], 1)
        self.assertTrue(axfo["model_decision"]["ignore_trailing_incomplete_bar"])
        self.assertEqual(
            axfo["model_decision"]["latest_usable_bar_ended_at"],
            "2026-04-28T09:05:00+02:00",
        )
        self.assertEqual(axfo["phase1_bars"][0]["open"], "110.00")
        self.assertEqual(axfo["phase1_bars"][0]["close"], "114.50")
        self.assertEqual(axfo["phase1_bars"][1]["open"], "115.00")
        self.assertFalse(axfo["features"]["static_features_ready"])
        self.assertEqual(axfo["features"]["static_features_source"], "missing")
        self.assertAlmostEqual(
            axfo["features"]["base_dynamic"][1][0],
            1.0 / 101.0,
        )
        self.assertIn(
            "market_bar_norm",
            axfo["features"]["extra_dynamic_feature_names"],
        )
        self.assertEqual(payload["market_context"]["counts_by_bar"], [2, 2])

    def test_sparse_observed_bars_pause_when_coverage_is_below_threshold(self) -> None:
        payload = build_phase1_observation_payload(
            deployment_key="long_trial_106_virtual_shared_01",
            model_key="long_trial_106_v1",
            model_side="LONG",
            observation_contract={
                "bar_family": "phase1_intraday_ohlc_v1",
                "bar_interval": "5m",
                "session_timezone": "Europe/Stockholm",
                "session_open_local": "09:00",
                "session_close_local": "17:30",
                "include_market_context": False,
                "include_vol_normalized_intraday_state": True,
            },
            action_space=["skip", "wait", "market_entry"],
            as_of=datetime.fromisoformat("2026-04-28T09:20:30+02:00"),
            symbols=["AXFO"],
            source_bars_by_symbol={
                "AXFO": _bars_for_minutes(
                    "20260428",
                    start_price=110.0,
                    minutes=[0, 1, 2, 3, 4, 15, 16, 17, 18, 19],
                ),
            },
            history_overrides={"AXFO": _history_override(prev_close="100")},
        )

        axfo = payload["observations"]["AXFO"]
        self.assertEqual(axfo["bar_count"], 2)
        self.assertFalse(axfo["model_decision"]["ready"])
        self.assertEqual(
            axfo["model_decision"]["reason"],
            "paused_observed_bar_coverage_below_threshold",
        )
        self.assertEqual(
            axfo["data_quality"]["bar_sequence_policy"],
            "observed_provider_bars_only",
        )
        self.assertEqual(axfo["data_quality"]["expected_complete_bar_count"], 4)
        self.assertEqual(axfo["data_quality"]["observed_complete_bar_count"], 2)
        self.assertEqual(axfo["data_quality"]["missing_complete_bar_count"], 2)
        self.assertAlmostEqual(axfo["data_quality"]["coverage_ratio"], 0.5)

    def test_sparse_observed_bars_pass_when_coverage_threshold_allows_it(self) -> None:
        payload = build_phase1_observation_payload(
            deployment_key="long_trial_106_virtual_shared_01",
            model_key="long_trial_106_v1",
            model_side="LONG",
            observation_contract={
                "bar_family": "phase1_intraday_ohlc_v1",
                "bar_interval": "5m",
                "include_market_context": False,
                "include_vol_normalized_intraday_state": True,
            },
            action_space=["skip", "wait", "market_entry"],
            as_of=datetime.fromisoformat("2026-04-28T09:20:30+02:00"),
            symbols=["AXFO"],
            source_bars_by_symbol={
                "AXFO": _bars_for_minutes(
                    "20260428",
                    start_price=110.0,
                    minutes=[0, 1, 2, 3, 4, 15, 16, 17, 18, 19],
                ),
            },
            history_overrides={"AXFO": _history_override(prev_close="100")},
            config_overrides={"min_observed_bar_coverage_ratio": 0.5},
        )

        axfo = payload["observations"]["AXFO"]
        self.assertTrue(axfo["model_decision"]["ready"])
        self.assertEqual(
            payload["input_contract"]["bar_sequence_policy"],
            "observed_provider_bars_only",
        )

    def test_accepts_history_override_when_only_live_day_bars_are_sent(self) -> None:
        payload = build_phase1_observation_payload(
            deployment_key="long_trial_106_virtual_shared_01",
            model_key="long_trial_106_v1",
            model_side="LONG",
            observation_contract={
                "bar_family": "phase1_intraday_ohlc_v1",
                "bar_interval": "5m",
                "include_market_context": False,
                "include_vol_normalized_intraday_state": True,
            },
            action_space=["skip", "wait", "market_entry"],
            as_of=datetime.fromisoformat("2026-04-28T09:07:30+02:00"),
            symbols=["AXFO"],
            source_bars_by_symbol={
                "AXFO": _bars_for_day("20260428", start_price=110.0, minutes=8),
            },
            history_overrides={"AXFO": _history_override(prev_close="100")},
        )

        axfo = payload["observations"]["AXFO"]
        self.assertEqual(axfo["pricing_context"]["prev_close"], "100.0")
        self.assertEqual(axfo["features"]["history_features"], [0.01] * 7)
        self.assertEqual(
            axfo["features"]["extra_dynamic_feature_names"][0],
            "vol_norm_open_rel_prev_close",
        )

    def test_builds_history_override_from_prior_source_bars(self) -> None:
        override = build_history_override_from_source_bars(
            symbol="AXFO",
            target_date="2026-04-28",
            source_bars=_bars_for_day("20260426", start_price=95.0, minutes=10)
            + _bars_for_day("20260427", start_price=100.0, minutes=10),
            observation_contract={
                "bar_family": "phase1_intraday_ohlc_v1",
                "bar_interval": "5m",
                "session_timezone": "Europe/Stockholm",
                "session_open_local": "09:00",
                "session_close_local": "17:30",
            },
        )

        self.assertEqual(override["prev_close"], "109.5")
        self.assertEqual(
            set(override["history_features"]),
            set(HISTORY_FEATURE_NAMES),
        )
        self.assertGreater(
            override["history_features"]["trailing_intraday_realized_vol"],
            0.0,
        )

    def test_accepts_normalized_static_features_from_upstream_candidate_payload(self) -> None:
        payload = build_phase1_observation_payload(
            deployment_key="long_trial_106_virtual_shared_01",
            model_key="long_trial_106_v1",
            model_side="LONG",
            observation_contract={
                "bar_family": "phase1_intraday_ohlc_v1",
                "bar_interval": "5m",
                "include_market_context": False,
                "include_vol_normalized_intraday_state": True,
            },
            action_space=["skip", "wait", "market_entry"],
            as_of=datetime.fromisoformat("2026-04-28T09:07:30+02:00"),
            symbols=["AXFO"],
            source_bars_by_symbol={
                "AXFO": _bars_for_day("20260428", start_price=110.0, minutes=8),
            },
            history_overrides={"AXFO": _history_override(prev_close="100")},
            static_features_by_symbol={
                "AXFO": {
                    "feature_names": ["rank_score_z", "turnover_z"],
                    "values": ["0.25", "-1.50"],
                    "normalized": True,
                    "source": "lockbox_candidate_row",
                }
            },
        )

        features = payload["observations"]["AXFO"]["features"]
        self.assertTrue(features["static_features_ready"])
        self.assertEqual(features["static_feature_names"], ["rank_score_z", "turnover_z"])
        self.assertEqual(features["static_features"], [0.25, -1.5])
        self.assertTrue(features["static_features_normalized"])
        self.assertEqual(features["static_features_source"], "lockbox_candidate_row")

    def test_first_incomplete_bar_is_not_decision_ready(self) -> None:
        payload = build_phase1_observation_payload(
            deployment_key="long_trial_106_virtual_shared_01",
            model_key="long_trial_106_v1",
            model_side="LONG",
            observation_contract={
                "bar_family": "phase1_intraday_ohlc_v1",
                "bar_interval": "5m",
                "include_market_context": False,
                "include_vol_normalized_intraday_state": False,
            },
            action_space=["skip", "wait", "market_entry"],
            as_of=datetime.fromisoformat("2026-04-28T09:02:30+02:00"),
            symbols=["AXFO"],
            source_bars_by_symbol={
                "AXFO": _bars_for_day("20260428", start_price=110.0, minutes=3),
            },
            history_overrides={"AXFO": _history_override(prev_close="100")},
        )

        decision = payload["observations"]["AXFO"]["model_decision"]
        self.assertFalse(decision["ready"])
        self.assertEqual(decision["usable_bar_count"], 0)
        self.assertEqual(decision["next_decision_at"], "2026-04-28T09:05:00+02:00")
