from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


DEFAULT_QTRAINING_ROOT = Path("/home/mattias/dev/q-training-bucket-booster")
DEFAULT_SHARED_VIRTUAL_ACCOUNT = "VIRTUALRL01"


@dataclass(frozen=True, slots=True)
class PromotedRLModelArtifact:
    model_key: str
    display_name: str
    strategy_family: str
    strategy_id: str
    side: str
    model_family: str
    model_version: str
    model_artifact_id: str
    source_workflow_path: Path
    promoted_checkpoint_path: Path
    summary_path: Path
    static_feature_cols_path: Path
    candidate_tape_path: Path
    deployment_key: str
    book_key: str
    action_space: tuple[str, ...]
    execution_mapping_version: str
    state_machine_version: str
    entry_action_name: str
    take_profit_action_name: str

    @property
    def side_upper(self) -> str:
        return self.side.upper()

    @property
    def book_side(self) -> str:
        return self.side_upper


def qtraining_root() -> Path:
    return Path(os.environ.get("QTRAINING_ROOT", str(DEFAULT_QTRAINING_ROOT))).expanduser()


def promoted_rl_models(root: Path | None = None) -> tuple[PromotedRLModelArtifact, ...]:
    base = root or qtraining_root()
    return (
        PromotedRLModelArtifact(
            model_key="long_trial_106_v1",
            display_name="Long Trial 106 V1",
            strategy_family="canonical_long_live_execution_policy",
            strategy_id="long_trial_106",
            side="LONG",
            model_family="canonical_long_live_execution_policy",
            model_version="v1",
            model_artifact_id="trial_106_seed240",
            source_workflow_path=base
            / "workflows/canonical/long_research/v1/execution_policy_long_trial106_v1.yaml",
            promoted_checkpoint_path=base
            / "artifacts/analysis/long_trial_106_ex_long_true_rl_dqn_w128_oracle_notrade_dualseed_extension_v1/continuation/true_rl_dqn_w128_seed240/best_dqn_state.pt",
            summary_path=base
            / "artifacts/analysis/long_trial_106_ex_long_true_rl_dqn_w128_oracle_notrade_dualseed_extension_v1/continuation/true_rl_dqn_w128_seed240/summary.json",
            static_feature_cols_path=base
            / "artifacts/analysis/long_trial_106_ex_long_true_rl_dqn_w128_oracle_notrade_dualseed_extension_v1/continuation/true_rl_dqn_w128_seed240/static_feature_cols.csv",
            candidate_tape_path=base
            / "artifacts/analysis/long_trial_104_ex_long_true_rl_input_materialize_ranker_v1/lockbox_candidate_tape.parquet",
            deployment_key="long_trial_106_virtual_shared_01",
            book_key="rl_shared_long_trial_106_virtual_01",
            action_space=(
                "skip",
                "wait",
                "market_entry",
                "cancel_entry",
                "exit_market",
                "clear_exit",
                "entry_prevclose_-50bp",
                "exit_tp_200bp",
            ),
            execution_mapping_version="long_actions_v1",
            state_machine_version="long_symbol_state_v1",
            entry_action_name="entry_prevclose_-50bp",
            take_profit_action_name="exit_tp_200bp",
        ),
        PromotedRLModelArtifact(
            model_key="short_trial36_v1",
            display_name="Short Trial 36 V1",
            strategy_family="canonical_short_live_execution_policy",
            strategy_id="short_trial_36",
            side="SHORT",
            model_family="canonical_short_live_execution_policy",
            model_version="v1",
            model_artifact_id="trial_36_seed140",
            source_workflow_path=base
            / "workflows/canonical/short_live/v1/execution_policy_short_trial36_v1.yaml",
            promoted_checkpoint_path=base
            / "artifacts/analysis/short_trial_36_ex_short_true_rl_dqn_w128_volnorm_market_context_triseed_v1/continuation/true_rl_dqn_w128_seed140/best_dqn_state.pt",
            summary_path=base
            / "artifacts/analysis/short_trial_36_ex_short_true_rl_dqn_w128_volnorm_market_context_triseed_v1/continuation/true_rl_dqn_w128_seed140/summary.json",
            static_feature_cols_path=base
            / "artifacts/analysis/short_trial_36_ex_short_true_rl_dqn_w128_volnorm_market_context_triseed_v1/continuation/true_rl_dqn_w128_seed140/static_feature_cols.csv",
            candidate_tape_path=base
            / "artifacts/analysis/short_trial_14_replay_tape_ibkr_shortable_v1/lockbox_candidate_tape.parquet",
            deployment_key="short_trial_36_virtual_shared_01",
            book_key="rl_shared_short_trial_36_virtual_01",
            action_space=(
                "skip",
                "wait",
                "market_entry",
                "cancel_entry",
                "exit_market",
                "clear_exit",
                "entry_prevclose_88bp",
                "exit_tp_180bp",
            ),
            execution_mapping_version="short_actions_v1",
            state_machine_version="short_symbol_state_v1",
            entry_action_name="entry_prevclose_88bp",
            take_profit_action_name="exit_tp_180bp",
        ),
    )


def promoted_rl_model_by_key(
    model_key: str,
    *,
    root: Path | None = None,
) -> PromotedRLModelArtifact:
    normalized = model_key.strip().lower()
    for artifact in promoted_rl_models(root):
        if artifact.model_key == normalized:
            return artifact
    raise KeyError(f"unknown promoted RL model: {model_key}")


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
        "source_workflow_path": artifact.source_workflow_path,
        "promoted_checkpoint_path": artifact.promoted_checkpoint_path,
        "summary_path": artifact.summary_path,
        "static_feature_cols_path": artifact.static_feature_cols_path,
        "candidate_tape_path": artifact.candidate_tape_path,
    }
    missing = {
        name: str(path)
        for name, path in required_paths.items()
        if not path.exists()
    }
    if missing:
        raise FileNotFoundError(f"missing promoted RL artifact files: {missing}")

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
    return {
        "bar_family": "phase1_intraday_ohlc_v1",
        "bar_interval": "5m",
        "update_cadence": "1m",
        "decision_cadence": "5m",
        "intraday_fetch_config": str(qtraining_root() / "configs/intraday_fetch.yaml"),
        "session_timezone": "Europe/Stockholm",
        "session_open_local": "09:00",
        "session_close_local": "17:30",
        "price_inputs": ["open", "high", "low", "close"],
        "growing_day_prefix": True,
        "include_market_context": True,
        "include_vol_normalized_intraday_state": True,
        "vol_normalization_floor": 0.000001,
        "feature_schema_version": f"{artifact.model_key}_phase1_live_v1",
        "static_feature_count": validation["static_feature_count"],
        "source_market_data_contract": {
            "bar_family": "stockholm_intraday_1m_v1",
            "required_series": ["TRADES"],
            "adapter": "ibkr_1m_trades_to_phase1_5m_ohlc_v1",
        },
    }


def model_registry_payload(artifact: PromotedRLModelArtifact) -> dict[str, Any]:
    validation = validate_promoted_artifact(artifact)
    return {
        "model_key": artifact.model_key,
        "display_name": artifact.display_name,
        "strategy_family": artifact.strategy_family,
        "side": artifact.side_upper,
        "source_workflow_path": str(artifact.source_workflow_path),
        "promoted_checkpoint_path": str(artifact.promoted_checkpoint_path),
        "action_space": list(artifact.action_space),
        "observation_contract": model_observation_contract(artifact),
        "execution_mapping_version": artifact.execution_mapping_version,
        "metadata": {
            "strategy_id": artifact.strategy_id,
            "model_family": artifact.model_family,
            "model_version": artifact.model_version,
            "model_artifact_id": artifact.model_artifact_id,
            "summary_path": str(artifact.summary_path),
            "static_feature_cols_path": str(artifact.static_feature_cols_path),
            "candidate_tape_path": str(artifact.candidate_tape_path),
            "static_feature_count": validation["static_feature_count"],
            "runner": "scripts/run_rl_agents.py",
        },
    }


def deployment_registry_payload(
    artifact: PromotedRLModelArtifact,
    *,
    account_key: str = DEFAULT_SHARED_VIRTUAL_ACCOUNT,
    mode: str = "virtual",
    status: str = "running",
    allowed_symbols: tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        "deployment_key": artifact.deployment_key,
        "model_key": artifact.model_key,
        "account_key": account_key.upper(),
        "book_key": artifact.book_key,
        "mode": mode.lower(),
        "status": status.lower(),
        "allowed_symbols": [symbol.upper() for symbol in allowed_symbols],
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
        },
    }


def promoted_artifact_summary(
    root: Path | None = None,
) -> dict[str, Any]:
    return {
        artifact.model_key: validate_promoted_artifact(artifact)
        for artifact in promoted_rl_models(root)
    }


__all__ = [
    "DEFAULT_QTRAINING_ROOT",
    "DEFAULT_SHARED_VIRTUAL_ACCOUNT",
    "PromotedRLModelArtifact",
    "deployment_registry_payload",
    "model_observation_contract",
    "model_registry_payload",
    "promoted_artifact_summary",
    "promoted_rl_model_by_key",
    "promoted_rl_models",
    "qtraining_root",
    "read_model_summary",
    "read_static_feature_names",
    "validate_promoted_artifact",
]
