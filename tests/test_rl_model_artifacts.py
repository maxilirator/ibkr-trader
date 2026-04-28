from __future__ import annotations

from ibkr_trader.rl.model_artifacts import DEFAULT_SHARED_VIRTUAL_ACCOUNT
from ibkr_trader.rl.model_artifacts import deployment_registry_payload
from ibkr_trader.rl.model_artifacts import model_registry_payload
from ibkr_trader.rl.model_artifacts import promoted_rl_models
from ibkr_trader.rl.model_artifacts import validate_promoted_artifact


def test_promoted_qtraining_artifacts_are_present_and_match_declared_actions() -> None:
    artifacts = promoted_rl_models()

    assert [artifact.model_key for artifact in artifacts] == [
        "long_trial_106_v1",
        "short_trial36_v1",
    ]
    for artifact in artifacts:
        validation = validate_promoted_artifact(artifact)
        assert validation["action_count"] == 8
        assert validation["static_feature_count"] > 0
        assert artifact.entry_action_name in artifact.action_space
        assert artifact.take_profit_action_name in artifact.action_space


def test_model_and_deployment_payloads_use_shared_virtual_account_with_side_constraints() -> None:
    payloads = [
        (
            model_registry_payload(artifact),
            deployment_registry_payload(artifact),
        )
        for artifact in promoted_rl_models()
    ]

    sides = {model["model_key"]: model["side"] for model, _ in payloads}
    assert sides == {
        "long_trial_106_v1": "LONG",
        "short_trial36_v1": "SHORT",
    }
    for model_payload, deployment_payload in payloads:
        assert model_payload["observation_contract"]["bar_interval"] == "5m"
        assert model_payload["observation_contract"]["update_cadence"] == "1m"
        assert deployment_payload["account_key"] == DEFAULT_SHARED_VIRTUAL_ACCOUNT
        assert deployment_payload["mode"] == "virtual"
        assert deployment_payload["action_constraints"]["position_side"] == model_payload["side"]
        assert deployment_payload["metadata"]["shared_capital_account"] is True
