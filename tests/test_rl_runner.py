from __future__ import annotations

from types import SimpleNamespace

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
    assert posted[0][1]["replace"] is False
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
