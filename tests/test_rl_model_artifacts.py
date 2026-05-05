from __future__ import annotations

import json
from pathlib import Path

import pytest

from ibkr_trader.rl.model_artifacts import DEFAULT_SHARED_VIRTUAL_ACCOUNT
from ibkr_trader.rl.model_artifacts import deployment_registry_payload
from ibkr_trader.rl.model_artifacts import load_model_bundle_manifest
from ibkr_trader.rl.model_artifacts import model_registry_payload
from ibkr_trader.rl.model_artifacts import promoted_rl_models
from ibkr_trader.rl.model_artifacts import validate_promoted_artifact


LONG_ACTIONS = [
    "skip",
    "wait",
    "market_entry",
    "cancel_entry",
    "exit_market",
    "clear_exit",
    "entry_prevclose_-50bp",
    "exit_tp_200bp",
]


def _write_bundle(root: Path, *, model_key: str = "long_trial_106_v1") -> Path:
    bundle_dir = root / model_key / "trial_106_seed240"
    bundle_dir.mkdir(parents=True)
    (bundle_dir / "best_dqn_state.pt").write_bytes(b"checkpoint")
    (bundle_dir / "static_feature_cols.csv").write_text(
        "static_feature_cols\nrank_score_z\nturnover_z\n",
        encoding="utf-8",
    )
    (bundle_dir / "summary.json").write_text(
        json.dumps(
            {
                "action_names": LONG_ACTIONS,
                "trade_side": "LONG",
                "static_feature_count": 2,
            }
        ),
        encoding="utf-8",
    )
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "rl_model_bundle_v1",
                "model_key": model_key,
                "display_name": "Long Trial 106 V1",
                "strategy_family": "canonical_long_live_execution_policy",
                "strategy_id": "long_trial_106",
                "side": "LONG",
                "model_family": "canonical_long_live_execution_policy",
                "model_version": "v1",
                "model_artifact_id": "trial_106_seed240",
                "files": {
                    "checkpoint": "best_dqn_state.pt",
                    "summary": "summary.json",
                    "static_feature_cols": "static_feature_cols.csv",
                },
                "deployment": {
                    "deployment_key": "long_trial_106_virtual_shared_01",
                    "book_key": "rl_shared_long_trial_106_virtual_01",
                },
                "action_space": LONG_ACTIONS,
                "execution_mapping_version": "long_actions_v1",
                "state_machine_version": "long_symbol_state_v1",
                "entry_action_name": "entry_prevclose_-50bp",
                "take_profit_action_name": "exit_tp_200bp",
                "observation_contract": {
                    "feature_schema_version": "long_trial_106_v1_phase1_live_v1"
                },
                "lineage": {
                    "source_workflow_path": "/home/mattias/dev/q-training/workflows/canonical/long_research/v1/execution_policy_long_trial106_v1.yaml"
                },
            }
        ),
        encoding="utf-8",
    )
    return bundle_dir / "manifest.json"


def test_promoted_model_bundles_are_loaded_from_trader_local_root(tmp_path: Path) -> None:
    _write_bundle(tmp_path)

    artifacts = promoted_rl_models(tmp_path)

    assert [artifact.model_key for artifact in artifacts] == ["long_trial_106_v1"]
    artifact = artifacts[0]
    validation = validate_promoted_artifact(artifact)
    assert validation["action_count"] == 8
    assert validation["static_feature_count"] == 2
    assert artifact.entry_action_name in artifact.action_space
    assert artifact.take_profit_action_name in artifact.action_space
    assert artifact.promoted_checkpoint_path == artifact.bundle_dir / "best_dqn_state.pt"


def test_model_registry_payload_uses_runtime_bundle_and_lineage_metadata(tmp_path: Path) -> None:
    artifact = load_model_bundle_manifest(_write_bundle(tmp_path))

    model_payload = model_registry_payload(artifact)
    deployment_payload = deployment_registry_payload(artifact)

    assert model_payload["source_workflow_path"] is None
    assert str(tmp_path) in model_payload["promoted_checkpoint_path"]
    assert model_payload["metadata"]["bundle_dir"] == str(artifact.bundle_dir)
    assert model_payload["metadata"]["lineage"]["non_runtime"] is True
    assert "q-training" in model_payload["metadata"]["lineage"]["source_workflow_path"]
    assert model_payload["observation_contract"]["static_feature_policy"] == (
        "instruction_payload_required"
    )
    assert model_payload["observation_contract"]["static_feature_source"] == (
        "instruction.trace.metadata.static_features"
    )
    assert deployment_payload["account_key"] == DEFAULT_SHARED_VIRTUAL_ACCOUNT
    assert "allowed_symbols" not in deployment_payload
    assert deployment_payload["metadata"]["daily_universe_source"] == (
        "model_routed_candidates"
    )


def test_bundle_runtime_file_paths_must_be_relative(tmp_path: Path) -> None:
    manifest_path = _write_bundle(tmp_path)
    manifest = json.loads(manifest_path.read_text())
    manifest["files"]["checkpoint"] = "/home/mattias/dev/q-training/checkpoint.pt"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="must be relative"):
        load_model_bundle_manifest(manifest_path)
