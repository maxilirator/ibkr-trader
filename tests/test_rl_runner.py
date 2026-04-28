from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

import scripts.run_rl_agents as runner
from scripts.run_rl_agents import build_historical_bars_payload
from scripts.run_rl_agents import decision_observed_at
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


def test_runner_prefers_static_features_from_candidate_payload() -> None:
    loaded = SimpleNamespace(
        config=SimpleNamespace(model_key="long_trial_106_v1"),
        static_feature_names=["rank_score_z", "turnover_z"],
        candidate_tape=pd.DataFrame(),
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
                    "source": {"system": "q-training-bucket"},
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
        candidate_reason_code="rl_model_routed_selected_candidate",
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
