from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


DEFAULT_RL_MODEL_BUNDLE_ROOT = Path("/home/mattias/ibkr-trader/var/rl-models")
DEFAULT_SHARED_VIRTUAL_ACCOUNT = "VIRTUALRL01"
MODEL_BUNDLE_SCHEMA_VERSION = "rl_model_bundle_v1"


@dataclass(frozen=True, slots=True)
class PromotedRLModelArtifact:
    """Trader-local, deployed RL model bundle.

    Research paths are lineage only. Runtime files must live inside the bundle
    directory beside the manifest.
    """

    model_key: str
    display_name: str
    strategy_family: str
    strategy_id: str
    side: str
    model_family: str
    model_version: str
    model_artifact_id: str
    bundle_dir: Path
    manifest_path: Path
    promoted_checkpoint_path: Path
    summary_path: Path
    static_feature_cols_path: Path
    deployment_key: str
    book_key: str
    action_space: tuple[str, ...]
    execution_mapping_version: str
    state_machine_version: str
    entry_action_name: str
    take_profit_action_name: str
    observation_contract: dict[str, Any]
    lineage: dict[str, Any]

    @property
    def side_upper(self) -> str:
        return self.side.upper()

    @property
    def book_side(self) -> str:
        return self.side_upper


def rl_model_bundle_root() -> Path:
    return Path(
        os.environ.get("RL_MODEL_BUNDLE_ROOT", str(DEFAULT_RL_MODEL_BUNDLE_ROOT))
    ).expanduser()


def promoted_rl_models(root: Path | None = None) -> tuple[PromotedRLModelArtifact, ...]:
    base = root or rl_model_bundle_root()
    manifest_paths = _discover_bundle_manifests(base)
    if not manifest_paths:
        raise FileNotFoundError(
            f"no RL model bundle manifests found under {base}; expected "
            "<root>/<model_key>/manifest.json or <root>/<model_key>/<artifact_id>/manifest.json"
        )
    return tuple(load_model_bundle_manifest(path) for path in manifest_paths)


def promoted_rl_model_by_key(
    model_key: str,
    *,
    root: Path | None = None,
) -> PromotedRLModelArtifact:
    normalized = model_key.strip().lower()
    for artifact in promoted_rl_models(root):
        if artifact.model_key.lower() == normalized:
            return artifact
    raise KeyError(f"unknown promoted RL model: {model_key}")


def load_model_bundle_manifest(path: Path) -> PromotedRLModelArtifact:
    manifest_path = path.expanduser().resolve()
    payload = json.loads(manifest_path.read_text())
    if not isinstance(payload, Mapping):
        raise ValueError(f"{manifest_path} must contain a JSON object")
    schema_version = str(payload.get("schema_version") or "")
    if schema_version and schema_version != MODEL_BUNDLE_SCHEMA_VERSION:
        raise ValueError(
            f"{manifest_path} schema_version={schema_version!r} is not supported"
        )

    bundle_dir = manifest_path.parent
    files = _mapping(payload.get("files"), "files")
    deployment = dict(payload.get("deployment") or {})
    observation_contract = _default_observation_contract(
        static_feature_policy="instruction_payload_required"
    )
    observation_contract.update(dict(payload.get("observation_contract") or {}))
    observation_contract["static_feature_policy"] = "instruction_payload_required"
    observation_contract["static_feature_source"] = "instruction.trace.metadata.static_features"
    observation_contract.setdefault("market_data_source", "trader_market_stream")

    lineage = dict(payload.get("lineage") or {})
    if lineage:
        lineage["non_runtime"] = True

    return PromotedRLModelArtifact(
        model_key=_required_str(payload, "model_key"),
        display_name=_required_str(payload, "display_name"),
        strategy_family=_required_str(payload, "strategy_family"),
        strategy_id=_required_str(payload, "strategy_id"),
        side=_required_str(payload, "side").upper(),
        model_family=_required_str(payload, "model_family"),
        model_version=_required_str(payload, "model_version"),
        model_artifact_id=_required_str(payload, "model_artifact_id"),
        bundle_dir=bundle_dir,
        manifest_path=manifest_path,
        promoted_checkpoint_path=_resolve_bundle_file(
            bundle_dir,
            _required_str(files, "checkpoint"),
            field_name="files.checkpoint",
        ),
        summary_path=_resolve_bundle_file(
            bundle_dir,
            _required_str(files, "summary"),
            field_name="files.summary",
        ),
        static_feature_cols_path=_resolve_bundle_file(
            bundle_dir,
            _required_str(files, "static_feature_cols"),
            field_name="files.static_feature_cols",
        ),
        deployment_key=str(
            deployment.get("deployment_key")
            or f"{_required_str(payload, 'model_key')}_virtual_shared_01"
        ),
        book_key=str(
            deployment.get("book_key")
            or f"rl_shared_{_required_str(payload, 'model_key')}_virtual_01"
        ),
        action_space=tuple(_required_str_list(payload, "action_space")),
        execution_mapping_version=_required_str(payload, "execution_mapping_version"),
        state_machine_version=_required_str(payload, "state_machine_version"),
        entry_action_name=_required_str(payload, "entry_action_name"),
        take_profit_action_name=_required_str(payload, "take_profit_action_name"),
        observation_contract=observation_contract,
        lineage=lineage,
    )


def read_static_feature_names(path: Path) -> tuple[str, ...]:
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != ["static_feature_cols"]:
            raise ValueError(
                f"{path} must contain a single static_feature_cols column"
            )
        names = tuple(str(row["static_feature_cols"]).strip() for row in reader)
    if not names or any(not name for name in names):
        raise ValueError(f"{path} must contain non-empty static feature names")
    return names


def read_model_summary(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def validate_promoted_artifact(
    artifact: PromotedRLModelArtifact,
) -> dict[str, Any]:
    required_paths = {
        "manifest_path": artifact.manifest_path,
        "promoted_checkpoint_path": artifact.promoted_checkpoint_path,
        "summary_path": artifact.summary_path,
        "static_feature_cols_path": artifact.static_feature_cols_path,
    }
    missing = {
        name: str(path)
        for name, path in required_paths.items()
        if not path.exists()
    }
    if missing:
        raise FileNotFoundError(f"missing deployed RL model bundle files: {missing}")

    summary = read_model_summary(artifact.summary_path)
    summary_actions = tuple(str(action) for action in summary.get("action_names", ()))
    if summary_actions != artifact.action_space:
        raise ValueError(
            f"{artifact.model_key} action space mismatch: "
            f"summary={summary_actions} expected={artifact.action_space}"
        )
    summary_side = str(summary.get("trade_side") or artifact.side).upper()
    if summary_side != artifact.side_upper:
        raise ValueError(
            f"{artifact.model_key} side mismatch: summary={summary_side} "
            f"expected={artifact.side_upper}"
        )

    static_feature_names = read_static_feature_names(artifact.static_feature_cols_path)
    summary_static_count = summary.get("static_feature_count")
    if summary_static_count is not None and int(summary_static_count) != len(static_feature_names):
        raise ValueError(
            f"{artifact.model_key} static feature count mismatch: "
            f"summary={summary_static_count} csv={len(static_feature_names)}"
        )
    return {
        "model_key": artifact.model_key,
        "action_count": len(artifact.action_space),
        "static_feature_count": len(static_feature_names),
        "summary": summary,
    }


def model_observation_contract(
    artifact: PromotedRLModelArtifact,
) -> dict[str, Any]:
    validation = validate_promoted_artifact(artifact)
    contract = dict(artifact.observation_contract)
    contract["static_feature_count"] = validation["static_feature_count"]
    contract["static_feature_policy"] = "instruction_payload_required"
    contract["static_feature_source"] = "instruction.trace.metadata.static_features"
    return contract


def model_registry_payload(artifact: PromotedRLModelArtifact) -> dict[str, Any]:
    validation = validate_promoted_artifact(artifact)
    metadata: dict[str, Any] = {
        "strategy_id": artifact.strategy_id,
        "model_family": artifact.model_family,
        "model_version": artifact.model_version,
        "model_artifact_id": artifact.model_artifact_id,
        "runtime_artifact_root": str(artifact.bundle_dir.parent),
        "bundle_dir": str(artifact.bundle_dir),
        "manifest_path": str(artifact.manifest_path),
        "summary_path": str(artifact.summary_path),
        "static_feature_cols_path": str(artifact.static_feature_cols_path),
        "static_feature_count": validation["static_feature_count"],
        "static_feature_source": "instruction.trace.metadata.static_features",
        "runner": "scripts/run_rl_agents.py",
    }
    if artifact.lineage:
        metadata["lineage"] = artifact.lineage
    return {
        "model_key": artifact.model_key,
        "display_name": artifact.display_name,
        "strategy_family": artifact.strategy_family,
        "side": artifact.side_upper,
        "source_workflow_path": None,
        "promoted_checkpoint_path": str(artifact.promoted_checkpoint_path),
        "action_space": list(artifact.action_space),
        "observation_contract": model_observation_contract(artifact),
        "execution_mapping_version": artifact.execution_mapping_version,
        "metadata": metadata,
    }


def deployment_registry_payload(
    artifact: PromotedRLModelArtifact,
    *,
    account_key: str = DEFAULT_SHARED_VIRTUAL_ACCOUNT,
    mode: str = "virtual",
    status: str = "running",
    allowed_symbols: tuple[str, ...] = (),
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "deployment_key": artifact.deployment_key,
        "model_key": artifact.model_key,
        "account_key": account_key.upper(),
        "book_key": artifact.book_key,
        "mode": mode.lower(),
        "status": status.lower(),
        "risk_limits": {
            "max_notional_per_name_sek": 1000,
        },
        "action_constraints": {
            "position_side": artifact.side_upper,
            "state_machine_version": artifact.state_machine_version,
            "execution_mapping_version": artifact.execution_mapping_version,
            "entry_action_name": artifact.entry_action_name,
            "take_profit_action_name": artifact.take_profit_action_name,
        },
        "metadata": {
            "shared_capital_account": True,
            "runner": "scripts/run_rl_agents.py",
            "model_artifact_id": artifact.model_artifact_id,
            "daily_universe_source": "model_routed_candidates",
        },
    }
    if allowed_symbols:
        payload["allowed_symbols"] = [symbol.upper() for symbol in allowed_symbols]
    return payload


def promoted_artifact_summary(
    root: Path | None = None,
) -> dict[str, Any]:
    return {
        artifact.model_key: validate_promoted_artifact(artifact)
        for artifact in promoted_rl_models(root)
    }


def _discover_bundle_manifests(root: Path) -> tuple[Path, ...]:
    if not root.exists():
        return ()
    direct = set(root.glob("*/manifest.json"))
    versioned = set(root.glob("*/*/manifest.json"))
    return tuple(sorted(direct | versioned))


def _resolve_bundle_file(
    bundle_dir: Path,
    value: str,
    *,
    field_name: str,
) -> Path:
    candidate = Path(value)
    if candidate.is_absolute():
        raise ValueError(
            f"{field_name} must be relative to the deployed model bundle, got {value!r}"
        )
    return (bundle_dir / candidate).resolve()


def _mapping(value: Any, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return value


def _required_str(payload: Mapping[str, Any], field_name: str) -> str:
    raw_value = payload.get(field_name)
    if raw_value is None:
        raise ValueError(f"{field_name} is required")
    value = str(raw_value).strip()
    if not value:
        raise ValueError(f"{field_name} must be non-empty")
    return value


def _required_str_list(payload: Mapping[str, Any], field_name: str) -> tuple[str, ...]:
    raw_value = payload.get(field_name)
    if not isinstance(raw_value, list) or not raw_value:
        raise ValueError(f"{field_name} must be a non-empty array")
    values = tuple(str(item).strip() for item in raw_value)
    if any(not value for value in values):
        raise ValueError(f"{field_name} must contain non-empty strings")
    return values


def _default_observation_contract(*, static_feature_policy: str) -> dict[str, Any]:
    return {
        "bar_family": "phase1_intraday_ohlc_v1",
        "bar_interval": "5m",
        "update_cadence": "1m",
        "decision_cadence": "5m",
        "session_timezone": "Europe/Stockholm",
        "session_open_local": "09:00",
        "session_close_local": "17:30",
        "price_inputs": ["open", "high", "low", "close"],
        "growing_day_prefix": True,
        "include_market_context": True,
        "include_vol_normalized_intraday_state": True,
        "vol_normalization_floor": 0.000001,
        "market_data_source": "trader_market_stream",
        "static_feature_policy": static_feature_policy,
        "source_market_data_contract": {
            "bar_family": "stockholm_intraday_1m_v1",
            "required_series": ["TRADES"],
            "adapter": "ibkr_1m_trades_to_phase1_5m_ohlc_v1",
        },
    }


__all__ = [
    "DEFAULT_RL_MODEL_BUNDLE_ROOT",
    "DEFAULT_SHARED_VIRTUAL_ACCOUNT",
    "MODEL_BUNDLE_SCHEMA_VERSION",
    "PromotedRLModelArtifact",
    "deployment_registry_payload",
    "load_model_bundle_manifest",
    "model_observation_contract",
    "model_registry_payload",
    "promoted_artifact_summary",
    "promoted_rl_model_by_key",
    "promoted_rl_models",
    "read_model_summary",
    "read_static_feature_names",
    "rl_model_bundle_root",
    "validate_promoted_artifact",
]
