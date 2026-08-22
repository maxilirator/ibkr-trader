from __future__ import annotations

from tests._rl_runner_shared import *  # noqa: F401,F403


def test_runner_publishes_omxs30_as_desired_index_contract(monkeypatch) -> None:
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

    runner.publish_desired_stream_symbols(
        "http://127.0.0.1:8000",
        ["AXFO", "OMXS30"],
        market_data_type="LIVE",
    )

    assert posted[0][0] == "http://127.0.0.1:8000/v1/market-data/stream/desired"
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


def test_runner_observation_uses_deployment_row_key(monkeypatch) -> None:
    observation_payloads: list[dict[str, object]] = []
    heartbeats: list[dict[str, object]] = []

    monkeypatch.setattr(
        runner,
        "load_runtime_states_from_instructions",
        lambda **_: {"NORION": RunnerSymbolState()},
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
        lambda **_: "2026-06-04T14:30:00+02:00",
    )
    monkeypatch.setattr(
        runner,
        "classify_decision_bar_freshness",
        lambda *_args, **_kwargs: {
            "status": "stale_bar",
            "latest_usable_bar_ended_at": "2026-06-04T14:25:00+02:00",
        },
    )

    def fake_post_json(
        url: str,
        payload: dict[str, object],
        *,
        timeout: int = 30,
    ) -> dict[str, object]:
        del timeout
        if "/v1/rl/observations/build" in url:
            observation_payloads.append(payload)
            deployment_key = str(payload["deployment_key"])
            return {
                "rl_observation": {
                    "feature_schema": {"path_pad_length": 102},
                    "observations": {
                        "NORION": {
                            "latest_bar_ended_at": "2026-06-04T14:25:00+02:00",
                            "model_decision": {
                                "ready": True,
                                "decision_id": (
                                    f"{deployment_key}:NORION:"
                                    "2026-06-04T14:25:00+02:00"
                                ),
                                "latest_usable_bar_ended_at": (
                                    "2026-06-04T14:25:00+02:00"
                                ),
                            },
                        },
                    },
                },
                "fetched_symbols": [],
            }
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
        deployment_key="long_trial_106_virtual_seedpicker_01",
        deployment_mode="virtual",
        candidates=[
            {
                "instruction_id": "seedpicker-norion",
                "symbol": "NORION",
                "account_key": "VIRTUALSEEDRL01",
                "trace": {
                    "trade_date": "2026-06-04",
                    "data_cutoff_date": "2026-06-03",
                },
            }
        ],
        processed_decisions=set(),
        execute_actions=True,
        history_cache={},
        history_duration="5 D",
        history_bar_size="1 min",
        history_timeout=20,
        stream_bar_ready_symbols={"NORION"},
        stream_plan={"stream_symbol_count": 1},
        trade_date="2026-06-04",
    )

    assert observation_payloads[0]["deployment_key"] == (
        "long_trial_106_virtual_seedpicker_01"
    )
    action = heartbeats[-1]["metrics"]["actions"][0]
    assert action["decision_id"].startswith("long_trial_106_virtual_seedpicker_01:")

def test_runner_skips_duplicate_desired_stream_posts(monkeypatch) -> None:
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

    first = runner.publish_desired_stream_symbols(
        "http://127.0.0.1:8000",
        ["AXFO", "OMXS30"],
        market_data_type="LIVE",
        subscription_state=subscription_state,
    )
    second = runner.publish_desired_stream_symbols(
        "http://127.0.0.1:8000",
        ["OMXS30", "AXFO"],
        market_data_type="LIVE",
        subscription_state=subscription_state,
    )

    assert first is True
    assert second is False
    assert len(posted) == 1

def test_runner_republishes_only_when_desired_state_is_missing() -> None:
    assert runner.stream_desired_state_needs_publish(
        {
            "desired_symbols": ["OMXS30"],
            "subscriptions": [
                {"contract": {"symbol": "OMXS30"}},
            ],
        },
        ["OMXS30", "AXFO"],
    )
    assert not runner.stream_desired_state_needs_publish(
        {
            "desired_symbols": ["OMXS30", "AXFO"],
            "subscriptions": [
                {"contract": {"symbol": "OMXS30"}},
            ],
        },
        ["AXFO", "OMXS30"],
    )
    assert not runner.stream_subscription_needs_repair(
        {
            "desired_symbols": ["OMXS30", "AXFO"],
            "subscriptions": [
                {"contract": {"symbol": "OMXS30"}},
            ],
        },
        ["AXFO", "OMXS30"],
    )
    assert runner.stream_subscription_pending_symbols(
        {
            "desired_symbols": ["OMXS30", "AXFO"],
            "subscriptions": [
                {"contract": {"symbol": "OMXS30"}},
            ],
        },
        ["AXFO", "OMXS30"],
    ) == ["AXFO"]

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
        if "/v1/market-data/stream/desired" in url:
            raise runner.ApiError("stream desired failed")
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
    assert "stream desired failed" in long_heartbeat["metrics"]["stream_error"]

    short_heartbeat = by_url[
        "http://127.0.0.1:8000/v1/rl/deployments/"
        "short_trial_36_virtual_shared_01/heartbeat"
    ]
    assert short_heartbeat["status"] == "running"
    assert short_heartbeat["metrics"] == {"candidate_count": 0, "runner_mode": "idle"}

def test_runner_reports_stale_bar_without_calling_model_or_translator(monkeypatch) -> None:
    heartbeats: list[dict[str, object]] = []
    translated: list[dict[str, object]] = []
    observation_payloads: list[dict[str, object]] = []

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
            observation_payloads.append(payload)
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
    assert observation_payloads[0]["as_of"] == "2026-05-05T09:10:00+02:00"
    assert observation_payloads[0]["fetch"]["mode"] == "market_stream"
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
