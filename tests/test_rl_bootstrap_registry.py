from __future__ import annotations

from pathlib import Path

from ibkr_trader.rl.model_artifacts import deployment_registry_payload
from ibkr_trader.rl.model_artifacts import load_model_bundle_manifest
from tests.test_rl_model_artifacts import _write_bundle


def test_bootstrap_payload_leaves_daily_universe_to_model_routed_candidates(
    tmp_path: Path,
) -> None:
    artifact = load_model_bundle_manifest(_write_bundle(tmp_path))

    payload = deployment_registry_payload(artifact)

    assert "allowed_symbols" not in payload
    assert payload["metadata"]["daily_universe_source"] == "model_routed_candidates"


def test_bootstrap_payload_still_accepts_explicit_emergency_allow_list(
    tmp_path: Path,
) -> None:
    artifact = load_model_bundle_manifest(_write_bundle(tmp_path))

    payload = deployment_registry_payload(artifact, allowed_symbols=("axfo", "telia"))

    assert payload["allowed_symbols"] == ["AXFO", "TELIA"]
