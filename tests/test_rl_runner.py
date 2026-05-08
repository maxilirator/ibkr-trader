from __future__ import annotations

from types import SimpleNamespace
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

import scripts.run_rl_agents as runner
from scripts.run_rl_agents import build_historical_bars_payload
from scripts.run_rl_agents import candidate_matches_deployment
from scripts.run_rl_agents import decision_observed_at
from scripts.run_rl_agents import group_candidates_by_deployment
from scripts.run_rl_agents import LoadedDeployment
from scripts.run_rl_agents import RunnerSymbolState
from scripts.run_rl_agents import parse_reason_code_filter
from scripts.run_rl_agents import static_feature_payload


def test_runner_historical_backfill_payload_uses_candidate_instrument_metadata() -> None:
    payload = build_historical_bars_payload(
        {
            "symbol": "ERIC-B",
            "exchange": "SMART",
            "currency": "SEK",
            "candidate": {
                "instrument": {
                    "symbol": "ERIC-B",
                    "security_type": "STK",
                    "exchange": "SMART",
                    "primary_exchange": "SFB",
                    "currency": "SEK",
                    "isin": "SE0000108656",
                }
            },
        },
        trade_date="2026-04-29",
        duration="30 D",
        bar_size="1 min",
    )

    assert payload["symbol"] == "ERIC-B"
    assert payload["exchange"] == "SMART"
    assert payload["primary_exchange"] == "SFB"
    assert payload["currency"] == "SEK"
    assert payload["isin"] == "SE0000108656"
    assert payload["duration"] == "30 D"
    assert payload["bar_size"] == "1 min"
    assert payload["what_to_show"] == "TRADES"
    assert payload["end_at"] == "2026-04-29T09:00:00+02:00"


def test_runner_historical_backfill_payload_maps_payload_xsto_to_ibkr_smart_sfb() -> None:
    payload = build_historical_bars_payload(
        {
            "symbol": "ERIC B",
            "exchange": "XSTO",
            "currency": "SEK",
            "candidate": {
                "payload": {
                    "instruction": {
                        "instrument": {
                            "symbol": "ERIC B",
                            "security_type": "STK",
                            "exchange": "XSTO",
                            "currency": "SEK",
                            "isin": "SE0000108656",
                        }
                    }
                }
            },
        },
        trade_date="2026-04-29",
        duration="30 D",
        bar_size="1 min",
    )

    assert payload["symbol"] == "ERIC B"
    assert payload["exchange"] == "SMART"
    assert payload["primary_exchange"] == "SFB"
    assert payload["isin"] == "SE0000108656"


def test_runner_history_override_can_fall_back_to_candidate_yesterday_close(
    monkeypatch,
) -> None:
    def fail_post_json(*_: object, **__: object) -> dict[str, object]:
        raise runner.ApiError("historical bars unavailable")

    monkeypatch.setattr(runner, "post_json", fail_post_json)

    override = runner.history_override_payload(
        api_base="http://127.0.0.1:8000",
        loaded=SimpleNamespace(config=SimpleNamespace(model_key="long_trial_106_v1")),
        candidate={
            "symbol": "SAGA B",
            "trace": {"metadata": {"yesterday_close": "187.9"}},
        },
        trade_date="2026-05-08",
        history_cache={},
        duration="5 D",
        bar_size="1 min",
        timeout=1,
        allow_metadata_fallback=True,
    )

    assert override["prev_close"] == "187.9"
    assert override["source"] == "candidate_metadata_yesterday_close_fallback"
    assert sorted(override["history_features"]) == sorted(runner.HISTORY_FEATURE_NAMES)


def test_runner_metadata_history_only_uses_candidate_history_without_ibkr(
    monkeypatch,
) -> None:
    def fail_post_json(*_: object, **__: object) -> dict[str, object]:
        pytest.fail("metadata_history_only must not call IBKR historical bars")

    monkeypatch.setattr(runner, "post_json", fail_post_json)
    history_features = {
        name: float(index) / 10.0
        for index, name in enumerate(runner.HISTORY_FEATURE_NAMES)
    }

    override = runner.history_override_payload(
        api_base="http://127.0.0.1:8000",
        loaded=SimpleNamespace(config=SimpleNamespace(model_key="long_trial_106_v1")),
        candidate={
            "symbol": "SAGA B",
            "trace": {
                "metadata": {
                    "yesterday_close": "187.9",
                    "history_features": history_features,
                }
            },
        },
        trade_date="2026-05-08",
        history_cache={},
        duration="5 D",
        bar_size="1 min",
        timeout=1,
        metadata_history_only=True,
    )

    assert override["prev_close"] == "187.9"
    assert override["history_features"] == history_features
    assert override["source"] == "candidate_metadata.history_features"


def test_runner_metadata_history_only_can_use_yesterday_close_fallback(
    monkeypatch,
) -> None:
    def fail_post_json(*_: object, **__: object) -> dict[str, object]:
        pytest.fail("metadata_history_only must not call IBKR historical bars")

    monkeypatch.setattr(runner, "post_json", fail_post_json)

    override = runner.history_override_payload(
        api_base="http://127.0.0.1:8000",
        loaded=SimpleNamespace(config=SimpleNamespace(model_key="long_trial_106_v1")),
        candidate={
            "symbol": "SAGA B",
            "trace": {"metadata": {"yesterday_close": "187.9"}},
        },
        trade_date="2026-05-08",
        history_cache={},
        duration="5 D",
        bar_size="1 min",
        timeout=1,
        allow_metadata_fallback=True,
        metadata_history_only=True,
    )

    assert override["prev_close"] == "187.9"
    assert override["source"] == "candidate_metadata_yesterday_close_fallback"
    assert sorted(override["history_features"]) == sorted(runner.HISTORY_FEATURE_NAMES)


def test_runner_waits_for_stream_bars_before_feature_or_history_preparation(
    monkeypatch,
) -> None:
    heartbeats: list[dict[str, object]] = []

    monkeypatch.setattr(
        runner,
        "load_runtime_state_context",
        lambda **_: runner.RuntimeStateContext(
            states={},
            blocked_symbols={},
            source="runtime-state",
        ),
    )
    monkeypatch.setattr(
        runner,
        "static_feature_payload",
        lambda *_, **__: pytest.fail("feature prep must wait for stream bars"),
    )
    monkeypatch.setattr(
        runner,
        "history_override_payload",
        lambda **_: pytest.fail("history prep must wait for stream bars"),
    )

    def fake_post_json(
        url: str,
        payload: dict[str, object],
        *,
        timeout: int = 30,
    ) -> dict[str, object]:
        del timeout
        assert "/heartbeat" in url
        heartbeats.append(payload)
        return {"accepted": True}

    monkeypatch.setattr(runner, "post_json", fake_post_json)

    runner.run_model_candidates(
        api_base="http://127.0.0.1:8000",
        loaded=SimpleNamespace(
            config=SimpleNamespace(
                model_key="long_trial_106_v1",
                deployment_key="long_trial_106_virtual_shared_01",
                side="LONG",
            )
        ),
        deployment_key="long_trial_106_virtual_shared_01",
        deployment_mode="virtual",
        candidates=[
            {
                "instruction_id": "long-saga",
                "symbol": "SAGA B",
                "account_key": "VIRTUALRL02",
                "trace": {
                    "trade_date": "2026-05-08",
                    "data_cutoff_date": "2026-05-07",
                },
            }
        ],
        processed_decisions=set(),
        execute_actions=True,
        history_cache={},
        history_duration="5 D",
        history_bar_size="1 min",
        history_timeout=20,
        stream_bar_ready_symbols=set(),
        stream_plan={"stream_symbol_count": 1},
        trade_date="2026-05-08",
    )

    assert heartbeats[-1]["status"] == "degraded"
    assert (
        heartbeats[-1]["runtime_error"]
        == "market stream has no bars for active RL candidates"
    )
    assert heartbeats[-1]["metrics"]["waiting_for_stream_bar_candidate_count"] == 1
    assert (
        heartbeats[-1]["metrics"]["skipped_candidates"][0]["status"]
        == "waiting_for_stream_bars"
    )


def test_runner_observed_at_uses_latest_completed_model_bar() -> None:
    observed_at = decision_observed_at(
        {
            "latest_bar_ended_at": "2026-04-29T09:10:00+02:00",
            "model_decision": {
                "latest_usable_bar_ended_at": "2026-04-29T09:05:00+02:00",
            },
        }
    )

    assert observed_at == "2026-04-29T09:05:00+02:00"


def test_runner_action_diagnostics_include_margin_and_mask() -> None:
    diagnostics = runner.action_diagnostics(
        ["skip", "wait", "entry_prevclose_-50bp"],
        [0.1, 0.2, 0.7],
        RunnerSymbolState(),
        chosen_action="entry_prevclose_-50bp",
    )

    assert diagnostics["chosen_action"] == "entry_prevclose_-50bp"
    assert diagnostics["valid_action_mask"] == [True, True, True]
    assert diagnostics["action_margin"] == pytest.approx(0.5)
    assert diagnostics["valid_actions_ranked"][0] == {
        "action_name": "entry_prevclose_-50bp",
        "q_value": 0.7,
    }


def test_runner_action_distribution_warns_when_short_flat_names_all_skip() -> None:
    metrics = runner.action_distribution_metrics(
        [
            {
                "symbol": f"SHORT{i}",
                "action_name": "skip",
                "state_before": "FLAT",
            }
            for i in range(6)
        ],
        model_side="SHORT",
    )

    assert metrics["evaluated_action_count"] == 6
    assert metrics["flat_entry_action_count"] == 0
    assert metrics["flat_idle_action_rate"] == 1.0
    assert metrics["warning"] == "short_flat_candidates_all_skip"


def test_runner_action_distribution_does_not_warn_on_pending_waits() -> None:
    metrics = runner.action_distribution_metrics(
        [
            {
                "symbol": f"LONG{i}",
                "action_name": "wait",
                "state_before": "ENTRY_PENDING",
            }
            for i in range(6)
        ],
        model_side="LONG",
    )

    assert metrics["evaluated_action_count"] == 6
    assert metrics["flat_evaluated_action_count"] == 0
    assert metrics["warning"] is None


def test_runner_loads_market_entry_pending_as_entry_pending(monkeypatch) -> None:
    monkeypatch.setattr(
        runner,
        "get_json",
        lambda *_args, **_kwargs: {
            "instructions": [
                {
                    "source_system": "rl-runner",
                    "symbol": "SHB A",
                    "state": "ENTRY_PENDING",
                    "activity_at": "2026-05-07T13:38:20Z",
                    "payload": {
                        "instruction": {
                            "trace": {
                                "metadata": {
                                    "rl_deployment_key": "long_trial_106_virtual_shared_01",
                                    "rl_action_name": "market_entry",
                                }
                            }
                        }
                    },
                }
            ]
        },
    )

    states = runner.load_runtime_states_from_instructions(
        api_base="http://127.0.0.1:8000",
        deployment_key="long_trial_106_virtual_shared_01",
        symbols=["SHB A"],
        side="LONG",
    )

    assert states["SHB A"].pending_entry_anchor == "market"
    assert states["SHB A"].bars_since_entry_order == 1
    assert runner.translation_state_before(states["SHB A"], "LONG") == "ENTRY_PENDING"


def test_runner_prefers_runtime_state_endpoint(monkeypatch) -> None:
    monkeypatch.setattr(
        runner,
        "get_json",
        lambda *_args, **_kwargs: {
            "accepted": True,
            "runtime_state": {
                "deployment_key": "long_trial_106_virtual_shared_01",
                "symbols": [
                    {
                        "symbol": "SHB A",
                        "status": "ready",
                        "state_before": "LONG_OPEN",
                        "runner_state": {
                            "in_position": True,
                            "pending_entry_anchor": None,
                            "pending_entry_rel_bp": None,
                            "pending_exit_tp_bp": None,
                            "entry_price": "130.50",
                            "entry_bar_idx": None,
                            "bars_since_entry_order": 0,
                            "bars_since_exit_order": 0,
                        },
                    },
                    {
                        "symbol": "BALD B",
                        "status": "blocked",
                        "state_before": "INCONSISTENT",
                        "blockers": [{"reason": "duplicate_active_positions"}],
                    },
                ],
            },
        },
    )

    context = runner.load_runtime_state_context(
        api_base="http://127.0.0.1:8000",
        deployment_key="long_trial_106_virtual_shared_01",
        symbols=["SHB A", "BALD B"],
        side="LONG",
    )

    assert context.source == "runtime-state"
    assert context.states["SHB A"].in_position is True
    assert context.states["SHB A"].entry_price == pytest.approx(130.5)
    assert "BALD B" in context.blocked_symbols


def test_runner_blocks_ambiguous_runtime_state_before_features(monkeypatch) -> None:
    heartbeats: list[dict[str, object]] = []

    monkeypatch.setattr(
        runner,
        "load_runtime_state_context",
        lambda **_: runner.RuntimeStateContext(
            states={},
            blocked_symbols={
                "BALD B": {
                    "symbol": "BALD B",
                    "status": "blocked",
                    "blockers": [{"reason": "duplicate_active_positions"}],
                }
            },
            source="runtime-state",
        ),
    )
    monkeypatch.setattr(
        runner,
        "static_feature_payload",
        lambda *_, **__: pytest.fail("blocked symbols must not build features"),
    )
    monkeypatch.setattr(
        runner,
        "history_override_payload",
        lambda **_: pytest.fail("blocked symbols must not build history"),
    )

    def fake_post_json(
        url: str,
        payload: dict[str, object],
        *,
        timeout: int = 30,
    ) -> dict[str, object]:
        del timeout
        assert "/heartbeat" in url
        heartbeats.append(payload)
        return {"accepted": True}

    monkeypatch.setattr(runner, "post_json", fake_post_json)

    runner.run_model_candidates(
        api_base="http://127.0.0.1:8000",
        loaded=SimpleNamespace(
            config=SimpleNamespace(
                model_key="long_trial_106_v1",
                deployment_key="long_trial_106_virtual_shared_01",
                side="LONG",
            )
        ),
        deployment_key="long_trial_106_virtual_shared_01",
        deployment_mode="virtual",
        candidates=[
            {
                "instruction_id": "long-bald",
                "symbol": "BALD B",
                "account_key": "VIRTUALRL01",
                "trace": {
                    "trade_date": "2026-05-07",
                    "data_cutoff_date": "2026-05-06",
                },
            }
        ],
        processed_decisions=set(),
        execute_actions=True,
        history_cache={},
        history_duration="5 D",
        history_bar_size="1 min",
        history_timeout=20,
        stream_bar_ready_symbols={"BALD B"},
        stream_plan={"stream_symbol_count": 1},
        trade_date="2026-05-07",
    )

    assert heartbeats[-1]["status"] == "degraded"
    assert (
        heartbeats[-1]["runtime_error"]
        == "authoritative runtime state blocked all RL candidates"
    )
    metrics = heartbeats[-1]["metrics"]
    assert metrics["runtime_state_source"] == "runtime-state"
    assert metrics["runtime_state_blocked_symbol_count"] == 1
    assert metrics["runtime_state_blocked_candidate_count"] == 1
    assert metrics["skipped_candidates"][0]["status"] == "runtime_state_blocked"


def test_runner_expected_decision_bar_uses_completed_five_minute_boundary() -> None:
    stockholm = ZoneInfo("Europe/Stockholm")

    assert (
        runner.expected_decision_bar_ended_at(
            trade_date="2026-05-05",
            now=datetime(2026, 5, 5, 9, 4, 59, tzinfo=stockholm),
        )
        is None
    )
    assert runner.expected_decision_bar_ended_at(
        trade_date="2026-05-05",
        now=datetime(2026, 5, 5, 9, 31, 2, tzinfo=stockholm),
    ) == "2026-05-05T09:30:00+02:00"
    assert runner.expected_decision_bar_ended_at(
        trade_date="2026-05-05",
        now=datetime(2026, 5, 5, 18, 0, 0, tzinfo=stockholm),
    ) == "2026-05-05T17:30:00+02:00"


def test_runner_classifies_stale_decision_bar() -> None:
    assert runner.classify_decision_bar_freshness(
        {
            "ready": True,
            "latest_usable_bar_ended_at": "2026-05-05T09:25:00+02:00",
        },
        target_decision_bar_ended_at="2026-05-05T09:30:00+02:00",
    )["status"] == "stale_bar"


def test_runner_stream_plan_prioritizes_candidates_over_benchmarks() -> None:
    plan = runner.build_stream_symbol_plan(
        candidate_symbols=["BBB", "AAA"],
        benchmark_symbols=["OMXS30", "AAA"],
        max_stream_symbols=2,
        warning_symbols=2,
    )

    assert plan["stream_symbols"] == ["AAA", "BBB"]
    assert plan["overflow_symbols"] == ["OMXS30"]
    assert plan["overflow_candidate_symbol_count"] == 0
    assert plan["over_warning_threshold"] is True


def test_runner_publish_virtual_decision_bar_posts_phase1_bar(monkeypatch) -> None:
    posted: list[tuple[str, dict[str, object]]] = []

    def fake_post_json(
        url: str,
        payload: dict[str, object],
        *,
        timeout: int = 30,
    ) -> dict[str, object]:
        del timeout
        posted.append((url, payload))
        return {"accepted": True, "virtual_market_watch": {"filled_order_count": 1}}

    monkeypatch.setattr(runner, "post_json", fake_post_json)

    result = runner.publish_virtual_decision_bar(
        "http://127.0.0.1:8000",
        candidate={
            "instruction_id": "candidate-1",
            "account_key": "VIRTUALRL01",
            "symbol": "AXFO",
            "currency": "SEK",
            "candidate": {
                "instrument": {
                    "symbol": "AXFO",
                    "security_type": "STK",
                    "exchange": "XSTO",
                    "currency": "SEK",
                    "primary_exchange": "SFB",
                }
            },
        },
        symbol_observation={
            "model_decision": {
                "latest_usable_bar_ended_at": "2026-04-29T09:05:00+02:00",
            },
            "phase1_bars": [
                {
                    "started_at": "2026-04-29T09:00:00+02:00",
                    "ended_at": "2026-04-29T09:05:00+02:00",
                    "complete": True,
                    "open": "100",
                    "high": "101",
                    "low": "99.40",
                    "close": "100.50",
                }
            ],
        },
        deployment_key="long_trial_106_virtual_shared_01",
        action_name="entry_prevclose_-50bp",
        decision_id="decision-1",
    )

    assert result == {"accepted": True, "virtual_market_watch": {"filled_order_count": 1}}
    assert posted[0][0] == "http://127.0.0.1:8000/v1/virtual/market-watch"
    payload = posted[0][1]
    assert payload["account_key"] == "VIRTUALRL01"
    assert payload["symbol"] == "AXFO"
    assert payload["last_price"] == "100.50"
    assert payload["latest_stream_bar"]["low"] == "99.40"
    assert payload["metadata"]["purpose"] == "virtual_same_bar_fill_parity"


def test_runner_prefers_static_features_from_candidate_payload() -> None:
    loaded = SimpleNamespace(
        config=SimpleNamespace(model_key="long_trial_106_v1"),
        static_feature_names=["rank_score_z", "turnover_z"],
    )

    payload = static_feature_payload(
        loaded,
        candidate={
            "trace": {
                "metadata": {
                    "static_features": {
                        "schema_version": "rl_static_features_v1",
                        "model_key": "long_trial_106_v1",
                        "feature_names": ["rank_score_z", "turnover_z"],
                        "values": ["0.25", "-1.50"],
                        "normalized": True,
                        "source": "upstream_candidate_payload",
                    }
                }
            }
        },
        symbol="AXFO",
        trade_date="2026-03-23",
    )

    assert payload == {
        "feature_names": ["rank_score_z", "turnover_z"],
        "values": [0.25, -1.5],
        "normalized": True,
        "source": "upstream_candidate_payload",
    }


def test_runner_normalizes_raw_static_features_when_bundle_stats_exist() -> None:
    loaded = SimpleNamespace(
        config=SimpleNamespace(model_key="long_trial_106_v1"),
        static_feature_names=["rank_score", "turnover"],
        static_feature_mean=runner.np.asarray([10.0, 100.0], dtype=runner.np.float32),
        static_feature_std=runner.np.asarray([2.0, 10.0], dtype=runner.np.float32),
        static_feature_normalization_id="trial_106_seed240",
    )

    payload = static_feature_payload(
        loaded,
        candidate={
            "trace": {
                "metadata": {
                    "static_features": {
                        "schema_version": "rl_static_features_v1",
                        "model_key": "long_trial_106_v1",
                        "feature_names": ["rank_score", "turnover"],
                        "values": [12.0, 80.0],
                        "normalized": False,
                        "source": "upstream_candidate_payload",
                    }
                }
            }
        },
        symbol="AXFO",
        trade_date="2026-03-23",
    )

    assert payload["values"] == [1.0, -2.0]
    assert payload["normalized"] is True
    assert payload["source"] == "upstream_candidate_payload+trader_static_zscore"


def test_runner_normalizes_candidate_values_even_when_legacy_payload_claims_normalized() -> None:
    loaded = SimpleNamespace(
        config=SimpleNamespace(model_key="short_trial36_v1"),
        static_feature_names=["vote_sum"],
        static_feature_mean=runner.np.asarray([250.0], dtype=runner.np.float32),
        static_feature_std=runner.np.asarray([25.0], dtype=runner.np.float32),
        static_feature_normalization_id="trial_36_seed140",
    )

    payload = static_feature_payload(
        loaded,
        candidate={
            "trace": {
                "metadata": {
                    "static_features": {
                        "schema_version": "rl_static_features_v1",
                        "model_key": "short_trial36_v1",
                        "feature_names": ["vote_sum"],
                        "values": [300.0],
                        "normalized": True,
                        "source": "upstream_candidate_payload",
                    }
                }
            }
        },
        symbol="SCA B",
        trade_date="2026-05-06",
    )

    assert payload["values"] == [2.0]


def test_runner_requires_static_features_on_candidate_payload() -> None:
    loaded = SimpleNamespace(
        config=SimpleNamespace(model_key="long_trial_106_v1"),
        static_feature_names=["rank_score_z", "turnover_z"],
    )

    with pytest.raises(ValueError, match="missing required instruction static_features"):
        static_feature_payload(
            loaded,
            candidate={"trace": {"metadata": {}}},
            symbol="AXFO",
            trade_date="2026-03-23",
        )


def test_runner_reason_code_filter_accepts_current_upstream_candidate_tape_reason() -> None:
    assert "rl_model_routed_candidate_tape_selected" in parse_reason_code_filter(None)
    assert parse_reason_code_filter("") == set()
    assert parse_reason_code_filter("a,b\nc") == {"a", "b", "c"}


def test_runner_matches_candidates_to_specific_deployment_account_and_book() -> None:
    loaded = SimpleNamespace(config=SimpleNamespace(model_key="long_trial_106_v1"))
    deployment = LoadedDeployment(
        deployment_key="long_live_01",
        model_key="long_trial_106_v1",
        account_key="U123",
        book_key="rl_live_long",
        mode="live",
        loaded=loaded,
    )

    assert candidate_matches_deployment(
        {
            "model_id": "long_trial_106_v1",
            "account_key": "U123",
            "book_key": "rl_live_long",
            "is_virtual": False,
        },
        deployment,
        account_mode="live",
    )
    assert not candidate_matches_deployment(
        {
            "model_id": "long_trial_106_v1",
            "account_key": "VIRTUALRL01",
            "book_key": "rl_live_long",
            "is_virtual": True,
        },
        deployment,
        account_mode="all",
    )


def test_runner_groups_same_model_candidates_by_deployment() -> None:
    long_model = SimpleNamespace(config=SimpleNamespace(model_key="long_trial_106_v1"))
    virtual_deployment = LoadedDeployment(
        deployment_key="long_virtual_01",
        model_key="long_trial_106_v1",
        account_key="VIRTUALRL01",
        book_key="rl_virtual_long",
        mode="virtual",
        loaded=long_model,
    )
    live_deployment = LoadedDeployment(
        deployment_key="long_live_01",
        model_key="long_trial_106_v1",
        account_key="U123",
        book_key="rl_live_long",
        mode="live",
        loaded=long_model,
    )

    grouped = group_candidates_by_deployment(
        [
            {
                "instruction_id": "virtual-candidate",
                "model_id": "long_trial_106_v1",
                "account_key": "VIRTUALRL01",
                "book_key": "rl_virtual_long",
                "is_virtual": True,
            },
            {
                "instruction_id": "live-candidate",
                "model_id": "long_trial_106_v1",
                "account_key": "U123",
                "book_key": "rl_live_long",
                "is_virtual": False,
            },
        ],
        {
            virtual_deployment.deployment_key: virtual_deployment,
            live_deployment.deployment_key: live_deployment,
        },
        account_mode="all",
    )

    assert [item["instruction_id"] for item in grouped["long_virtual_01"]] == [
        "virtual-candidate"
    ]
    assert [item["instruction_id"] for item in grouped["long_live_01"]] == [
        "live-candidate"
    ]


def test_runner_subscribes_omxs30_as_index_contract(monkeypatch) -> None:
    posted: list[tuple[str, dict[str, object]]] = []

    def fake_post_json(
        url: str,
        payload: dict[str, object],
        *,
        timeout: int = 30,
    ) -> dict[str, object]:
        del timeout
        posted.append((url, payload))
        return {"accepted": True}

    monkeypatch.setattr(runner, "post_json", fake_post_json)

    runner.subscribe_symbols(
        "http://127.0.0.1:8000",
        ["AXFO", "OMXS30"],
        market_data_type="LIVE",
    )

    assert posted[0][0] == "http://127.0.0.1:8000/v1/market-data/stream/subscribe"
    assert posted[0][1]["replace"] is True
    assert posted[0][1]["market_data_type"] == "LIVE"
    assert posted[0][1]["contracts"] == [
        {
            "symbol": "AXFO",
            "security_type": "STK",
            "exchange": "SMART",
            "primary_exchange": "SFB",
            "currency": "SEK",
        },
        {
            "symbol": "OMXS30",
            "security_type": "IND",
            "exchange": "OMS",
            "currency": "SEK",
            "primary_exchange": "",
        },
    ]


def test_runner_skips_duplicate_stream_subscription_posts(monkeypatch) -> None:
    posted: list[tuple[str, dict[str, object]]] = []

    def fake_post_json(
        url: str,
        payload: dict[str, object],
        *,
        timeout: int = 30,
    ) -> dict[str, object]:
        del timeout
        posted.append((url, payload))
        return {"accepted": True}

    monkeypatch.setattr(runner, "post_json", fake_post_json)
    subscription_state: dict[str, object] = {}

    first = runner.subscribe_symbols(
        "http://127.0.0.1:8000",
        ["AXFO", "OMXS30"],
        market_data_type="LIVE",
        subscription_state=subscription_state,
    )
    second = runner.subscribe_symbols(
        "http://127.0.0.1:8000",
        ["OMXS30", "AXFO"],
        market_data_type="LIVE",
        subscription_state=subscription_state,
    )

    assert first is True
    assert second is False
    assert len(posted) == 1


def test_runner_repairs_subscription_when_api_stream_lost_state() -> None:
    assert runner.stream_subscription_needs_repair(
        {
            "desired_symbols": ["OMXS30"],
            "subscriptions": [
                {"contract": {"symbol": "OMXS30"}},
            ],
        },
        ["OMXS30", "AXFO"],
    )
    assert not runner.stream_subscription_needs_repair(
        {
            "desired_symbols": ["OMXS30", "AXFO"],
            "subscriptions": [
                {"contract": {"symbol": "OMXS30"}},
                {"contract": {"symbol": "AXFO"}},
            ],
        },
        ["AXFO", "OMXS30"],
    )


def test_runner_degrades_heartbeat_when_stream_subscribe_fails(monkeypatch) -> None:
    heartbeats: list[tuple[str, dict[str, object]]] = []

    def fake_get_json(url: str, *, timeout: int = 30) -> dict[str, object]:
        assert "/v1/rl/candidates" in url
        return {
            "candidates": [
                {
                    "instruction_id": "long-axfo",
                    "symbol": "AXFO",
                    "is_virtual": True,
                    "model_id": "long_trial_106_v1",
                    "source": {"system": "q-training"},
                    "trace": {
                        "reason_code": "rl_model_routed_selected_candidate",
                        "trade_date": "2026-04-28",
                    },
                }
            ]
        }

    def fake_post_json(
        url: str,
        payload: dict[str, object],
        *,
        timeout: int = 30,
    ) -> dict[str, object]:
        if "/v1/market-data/stream/subscribe" in url:
            raise runner.ApiError("stream subscribe failed")
        assert "/heartbeat" in url
        heartbeats.append((url, payload))
        return {"accepted": True}

    monkeypatch.setattr(runner, "get_json", fake_get_json)
    monkeypatch.setattr(runner, "post_json", fake_post_json)
    loaded_models = {
        "long_trial_106_v1": SimpleNamespace(
            config=SimpleNamespace(
                model_key="long_trial_106_v1",
                deployment_key="long_trial_106_virtual_shared_01",
            )
        ),
        "short_trial36_v1": SimpleNamespace(
            config=SimpleNamespace(
                model_key="short_trial36_v1",
                deployment_key="short_trial_36_virtual_shared_01",
            )
        ),
    }

    runner.run_once(
        api_base="http://127.0.0.1:8000",
        limit=100,
        loaded_models=loaded_models,
        processed_decisions=set(),
        execute_virtual=True,
        include_smoke=False,
        stop_stream_on_empty=False,
        market_data_type="LIVE",
        candidate_reason_codes={"rl_model_routed_selected_candidate"},
        trade_date="2026-04-28",
        history_cache={},
        history_duration="30 D",
        history_bar_size="1 min",
        history_timeout=20,
        benchmark_symbols=[],
    )

    by_url = {url: payload for url, payload in heartbeats}
    long_heartbeat = by_url[
        "http://127.0.0.1:8000/v1/rl/deployments/"
        "long_trial_106_virtual_shared_01/heartbeat"
    ]
    assert long_heartbeat["status"] == "degraded"
    assert long_heartbeat["runtime_error"] == "market stream unavailable for active RL candidates"
    assert long_heartbeat["metrics"]["candidate_count"] == 1
    assert long_heartbeat["metrics"]["symbols"] == ["AXFO"]
    assert "stream subscribe failed" in long_heartbeat["metrics"]["stream_error"]

    short_heartbeat = by_url[
        "http://127.0.0.1:8000/v1/rl/deployments/"
        "short_trial_36_virtual_shared_01/heartbeat"
    ]
    assert short_heartbeat["status"] == "running"
    assert short_heartbeat["metrics"] == {"candidate_count": 0, "runner_mode": "idle"}


def test_runner_reports_stale_bar_without_calling_model_or_translator(monkeypatch) -> None:
    heartbeats: list[dict[str, object]] = []
    translated: list[dict[str, object]] = []

    monkeypatch.setattr(runner, "load_runtime_states_from_instructions", lambda **_: {})
    monkeypatch.setattr(
        runner,
        "static_feature_payload",
        lambda *_, **__: {"feature_names": ["x"], "values": [0.0], "normalized": True},
    )
    monkeypatch.setattr(
        runner,
        "history_override_payload",
        lambda **_: {"previous_session": {"prev_close": 100}, "history_features": {}},
    )
    monkeypatch.setattr(
        runner,
        "expected_decision_bar_ended_at",
        lambda **_: "2026-05-05T09:10:00+02:00",
    )
    monkeypatch.setattr(
        runner,
        "choose_action",
        lambda *_, **__: pytest.fail("stale bars must not reach the model"),
    )

    def fake_post_json(
        url: str,
        payload: dict[str, object],
        *,
        timeout: int = 30,
    ) -> dict[str, object]:
        del timeout
        if "/v1/rl/observations/build" in url:
            return {
                "rl_observation": {
                    "feature_schema": {"path_pad_length": 102},
                    "observations": {
                        "AXFO": {
                            "latest_bar_ended_at": "2026-05-05T09:05:00+02:00",
                            "model_decision": {
                                "ready": True,
                                "decision_id": "long:AXFO:2026-05-05T09:05:00+02:00",
                                "latest_usable_bar_ended_at": "2026-05-05T09:05:00+02:00",
                            },
                        }
                    },
                },
                "fetched_symbols": [],
            }
        if "/v1/rl/actions/translate" in url:
            translated.append(payload)
            return {"accepted": True}
        assert "/heartbeat" in url
        heartbeats.append(payload)
        return {"accepted": True}

    monkeypatch.setattr(runner, "post_json", fake_post_json)

    runner.run_model_candidates(
        api_base="http://127.0.0.1:8000",
        loaded=SimpleNamespace(
            config=SimpleNamespace(
                model_key="long_trial_106_v1",
                deployment_key="long_trial_106_virtual_shared_01",
                side="LONG",
            ),
            action_names=["skip", "wait", "entry_prevclose_-50bp"],
            obs_dim=10,
        ),
        deployment_key="long_trial_106_virtual_shared_01",
        deployment_mode="virtual",
        candidates=[
            {
                "instruction_id": "long-axfo",
                "symbol": "AXFO",
                "account_key": "VIRTUALRL01",
                "trace": {
                    "trade_date": "2026-05-05",
                    "data_cutoff_date": "2026-05-04",
                },
            }
        ],
        processed_decisions=set(),
        execute_actions=True,
        history_cache={},
        history_duration="5 D",
        history_bar_size="1 min",
        history_timeout=20,
        stream_bar_ready_symbols={"AXFO"},
        stream_plan={"stream_symbol_count": 1},
        trade_date="2026-05-05",
    )

    assert translated == []
    assert heartbeats[-1]["status"] == "degraded"
    assert (
        heartbeats[-1]["runtime_error"]
        == "market stream bars are stale for all active RL candidates"
    )
    metrics = heartbeats[-1]["metrics"]
    assert metrics["stale_decision_bar_candidate_count"] == 1
    assert metrics["fresh_decision_bar_candidate_count"] == 0
    assert metrics["evaluated_candidate_count"] == 0
    assert metrics["actions"][0]["status"] == "stale_bar"
    assert metrics["timing"]["cadence_budget_seconds"] == 300.0
    assert metrics["timing"]["cadence_over_budget"] is False
    assert metrics["timing"]["total_seconds"] >= 0.0


def test_runner_records_translate_conflict_without_aborting(monkeypatch) -> None:
    heartbeats: list[dict[str, object]] = []
    processed_decisions: set[str] = set()

    monkeypatch.setattr(
        runner,
        "load_runtime_states_from_instructions",
        lambda **_: {"AXFO": RunnerSymbolState(in_position=True, entry_price=100.0)},
    )
    monkeypatch.setattr(
        runner,
        "static_feature_payload",
        lambda *_, **__: {"feature_names": ["x"], "values": [0.0], "normalized": True},
    )
    monkeypatch.setattr(
        runner,
        "history_override_payload",
        lambda **_: {"previous_session": {"prev_close": 100}, "history_features": {}},
    )
    monkeypatch.setattr(
        runner,
        "expected_decision_bar_ended_at",
        lambda **_: "2026-05-05T09:05:00+02:00",
    )
    monkeypatch.setattr(runner, "assemble_dqn_observation_vector", lambda *_, **__: object())
    monkeypatch.setattr(runner, "choose_action", lambda *_, **__: ("exit_market", [0.0, 1.0]))

    def fake_post_json(
        url: str,
        payload: dict[str, object],
        *,
        timeout: int = 30,
    ) -> dict[str, object]:
        del timeout
        if "/v1/rl/observations/build" in url:
            return {
                "rl_observation": {
                    "feature_schema": {"path_pad_length": 102},
                    "observations": {
                        "AXFO": {
                            "latest_bar_ended_at": "2026-05-05T09:05:00+02:00",
                            "pricing_context": {"prev_close": "100"},
                            "model_decision": {
                                "ready": True,
                                "decision_id": "long:AXFO:2026-05-05T09:05:00+02:00",
                                "latest_usable_bar_ended_at": "2026-05-05T09:05:00+02:00",
                            },
                        }
                    },
                },
                "fetched_symbols": [],
            }
        if "/v1/rl/actions/translate" in url:
            raise runner.ApiError("translate -> HTTP 409: duplicate active instructions")
        assert "/heartbeat" in url
        heartbeats.append(payload)
        return {"accepted": True}

    monkeypatch.setattr(runner, "post_json", fake_post_json)

    runner.run_model_candidates(
        api_base="http://127.0.0.1:8000",
        loaded=SimpleNamespace(
            config=SimpleNamespace(
                model_key="long_trial_106_v1",
                deployment_key="long_trial_106_virtual_shared_01",
                side="LONG",
            ),
            action_names=["wait", "exit_market"],
            obs_dim=10,
            model=object(),
        ),
        deployment_key="long_trial_106_virtual_shared_01",
        deployment_mode="virtual",
        candidates=[
            {
                "instruction_id": "long-axfo",
                "symbol": "AXFO",
                "account_key": "VIRTUALRL01",
                "trace": {
                    "trade_date": "2026-05-05",
                    "data_cutoff_date": "2026-05-04",
                },
            }
        ],
        processed_decisions=processed_decisions,
        execute_actions=True,
        history_cache={},
        history_duration="5 D",
        history_bar_size="1 min",
        history_timeout=20,
        stream_bar_ready_symbols={"AXFO"},
        stream_plan={"stream_symbol_count": 1},
        trade_date="2026-05-05",
    )

    assert heartbeats[-1]["status"] == "degraded"
    assert (
        heartbeats[-1]["runtime_error"]
        == "1 RL action translation request(s) failed"
    )
    action = heartbeats[-1]["metrics"]["actions"][0]
    assert action["status"] == "translate_error"
    assert action["retryable"] is False
    assert processed_decisions == {
        "long_trial_106_virtual_shared_01:long-axfo:long:AXFO:2026-05-05T09:05:00+02:00"
    }
