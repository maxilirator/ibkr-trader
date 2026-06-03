from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import numpy as np

try:
    import torch
    from torch import nn
except ModuleNotFoundError:  # pragma: no cover - exercised by runner import smoke tests.
    torch = None
    nn = None

from ibkr_trader.rl.inference_vector import RunnerSymbolState
from ibkr_trader.rl.inference_vector import valid_action_mask
from ibkr_trader.rl.model_artifacts import PromotedRLModelArtifact
from ibkr_trader.rl.model_artifacts import read_static_feature_names
from ibkr_trader.rl.runner_types import LoadedModel


def _require_torch() -> tuple[Any, Any]:
    if torch is None or nn is None:
        raise RuntimeError(
            "The promoted RL runner needs PyTorch. Install the trader RL extras "
            "plus a CPU or GPU PyTorch wheel appropriate for this host."
        )
    return torch, nn


def _q_network_class(nn_module: Any) -> type[Any]:
    class QNetwork(nn_module.Module):
        def __init__(self, obs_dim: int, action_dim: int, hidden_dim: int) -> None:
            super().__init__()
            self.net = nn_module.Sequential(
                nn_module.Linear(obs_dim, hidden_dim),
                nn_module.ReLU(),
                nn_module.Linear(hidden_dim, hidden_dim),
                nn_module.ReLU(),
                nn_module.Linear(hidden_dim, action_dim),
            )

        def forward(self, x: Any) -> Any:
            return self.net(x)

    return QNetwork


def load_model(config: PromotedRLModelArtifact) -> LoadedModel:
    torch_module, nn_module = _require_torch()
    summary = json.loads(config.summary_path.read_text())
    action_names = [str(item) for item in summary["action_names"]]
    if action_names != list(config.action_space):
        raise ValueError(
            f"{config.model_key} action space mismatch between deployed bundle summary and trader registry"
        )
    state_dict = torch_module.load(config.promoted_checkpoint_path, map_location="cpu")
    if isinstance(state_dict, dict) and "model_state_dict" in state_dict:
        state_dict = state_dict["model_state_dict"]
    if isinstance(state_dict, dict) and "state_dict" in state_dict:
        state_dict = state_dict["state_dict"]
    obs_dim = int(state_dict["net.0.weight"].shape[1])
    hidden_dim = int(state_dict["net.0.weight"].shape[0])
    action_dim = int(state_dict["net.4.weight"].shape[0])
    if action_dim != len(action_names):
        raise ValueError(
            f"{config.model_key} checkpoint action dimension {action_dim} "
            f"does not match action names {len(action_names)}"
        )
    q_network_cls = _q_network_class(nn_module)
    model = q_network_cls(obs_dim=obs_dim, action_dim=action_dim, hidden_dim=hidden_dim)
    model.load_state_dict(state_dict, strict=True)
    model.eval()
    static_feature_names = list(read_static_feature_names(config.static_feature_cols_path))
    static_feature_mean: np.ndarray | None = None
    static_feature_std: np.ndarray | None = None
    static_feature_normalization_id: str | None = None
    if config.static_feature_normalization_path is not None:
        (
            static_feature_mean,
            static_feature_std,
            static_feature_normalization_id,
        ) = load_static_feature_normalization(
            config.static_feature_normalization_path,
            expected_feature_names=static_feature_names,
        )
    expected_static_count = summary.get("static_feature_count")
    if expected_static_count is not None and int(expected_static_count) != len(static_feature_names):
        raise ValueError(
            f"{config.model_key} static feature count mismatch: "
            f"summary={expected_static_count} csv={len(static_feature_names)}"
        )
    if obs_dim <= len(static_feature_names):
        raise ValueError(
            f"{config.model_key} observation width {obs_dim} is not large enough "
            f"for {len(static_feature_names)} static features"
        )
    return LoadedModel(
        config=config,
        action_names=action_names,
        obs_dim=obs_dim,
        model=model,
        static_feature_names=static_feature_names,
        static_feature_mean=static_feature_mean,
        static_feature_std=static_feature_std,
        static_feature_normalization_id=static_feature_normalization_id,
    )


def load_static_feature_normalization(
    path: Path,
    *,
    expected_feature_names: list[str],
) -> tuple[np.ndarray, np.ndarray, str]:
    payload = json.loads(path.read_text())
    if not isinstance(payload, Mapping):
        raise ValueError(f"{path} must contain a JSON object")
    schema_version = str(payload.get("schema_version") or "")
    if schema_version != "rl_static_feature_normalization_v1":
        raise ValueError(
            f"{path} schema_version must be rl_static_feature_normalization_v1"
        )
    feature_names = payload.get("feature_names")
    if feature_names != expected_feature_names:
        raise ValueError(
            f"{path} feature_names do not match static_feature_cols.csv"
        )
    mean = _float_array(payload.get("mean"), field_name=f"{path}.mean")
    std = _float_array(payload.get("std"), field_name=f"{path}.std")
    if mean.shape != std.shape or mean.shape[0] != len(expected_feature_names):
        raise ValueError(
            f"{path} mean/std length must match {len(expected_feature_names)} features"
        )
    if not np.isfinite(mean).all() or not np.isfinite(std).all():
        raise ValueError(f"{path} mean/std contain non-finite values")
    if np.any(std <= 0.0):
        raise ValueError(f"{path} std must be positive")
    normalization_id = str(
        payload.get("normalization_id")
        or payload.get("model_artifact_id")
        or path.name
    )
    return mean.astype(np.float32), std.astype(np.float32), normalization_id


def _float_array(raw_value: Any, *, field_name: str) -> np.ndarray:
    if not isinstance(raw_value, list) or not raw_value:
        raise ValueError(f"{field_name} must be a non-empty array")
    values = np.asarray([float(value) for value in raw_value], dtype=np.float32)
    return values


def static_feature_payload(
    loaded: LoadedModel,
    *,
    candidate: Mapping[str, Any] | None = None,
    symbol: str,
    trade_date: str,
) -> dict[str, Any]:
    candidate_payload = candidate_static_feature_payload(
        loaded,
        candidate=candidate,
        symbol=symbol,
    )
    if candidate_payload is not None:
        return candidate_payload

    raise ValueError(
        f"missing required instruction static_features for {loaded.config.model_key} "
        f"{symbol} {trade_date}; production RL candidates must carry "
        "trace.metadata.static_features"
    )


def candidate_static_feature_payload(
    loaded: LoadedModel,
    *,
    candidate: Mapping[str, Any] | None,
    symbol: str,
) -> dict[str, Any] | None:
    raw_payload = extract_candidate_static_features(candidate)
    if raw_payload is None:
        return None
    if isinstance(raw_payload, str):
        try:
            raw_payload = json.loads(raw_payload)
        except json.JSONDecodeError as exc:
            raise ValueError(f"candidate static_features JSON is invalid for {symbol}") from exc
    if not isinstance(raw_payload, Mapping):
        raise ValueError(f"candidate static_features must be an object for {symbol}")

    raw_model_key = raw_payload.get("model_key")
    if raw_model_key is not None and str(raw_model_key) != loaded.config.model_key:
        raise ValueError(
            f"candidate static_features model_key mismatch for {symbol}: "
            f"{raw_model_key!r} != {loaded.config.model_key!r}"
        )

    raw_names = raw_payload.get("feature_names")
    if not isinstance(raw_names, list) or not all(
        isinstance(name, str) and name.strip() for name in raw_names
    ):
        raise ValueError(f"candidate static_features.feature_names must be strings for {symbol}")
    names = [name.strip() for name in raw_names]
    if names != loaded.static_feature_names:
        raise ValueError(
            f"candidate static feature_names mismatch for {loaded.config.model_key} {symbol}: "
            f"got {len(names)} names, expected {len(loaded.static_feature_names)}"
        )

    raw_values = (
        raw_payload.get("values")
        if raw_payload.get("values") is not None
        else raw_payload.get("static_features_norm")
        if raw_payload.get("static_features_norm") is not None
        else raw_payload.get("static_features")
    )
    if not isinstance(raw_values, list):
        raise ValueError(f"candidate static_features.values must be an array for {symbol}")
    if len(raw_values) != len(names):
        raise ValueError(
            f"candidate static feature value count mismatch for {loaded.config.model_key} {symbol}: "
            f"got {len(raw_values)}, expected {len(names)}"
        )
    values = np.asarray([float(value) for value in raw_values], dtype=np.float32)
    if not np.isfinite(values).all():
        raise ValueError(f"candidate static features contain non-finite values for {symbol}")

    normalization = raw_payload.get("normalization")
    already_model_normalized = _payload_declares_model_bundle_normalization(
        normalization,
        normalization_id=getattr(loaded, "static_feature_normalization_id", None),
    )
    mean = getattr(loaded, "static_feature_mean", None)
    std = getattr(loaded, "static_feature_std", None)
    source = str(raw_payload.get("source") or "upstream_candidate_payload")
    if mean is not None and std is not None and not already_model_normalized:
        values = (values - mean) / std
        normalized = True
        source = f"{source}+trader_static_zscore"
    else:
        normalized = bool(raw_payload.get("normalized", True))
    if not normalized:
        raise ValueError(
            f"candidate static features must already be normalized for {loaded.config.model_key} {symbol}"
        )

    return {
        "feature_names": names,
        "values": [float(value) for value in values.tolist()],
        "normalized": True,
        "source": source,
    }


def _payload_declares_model_bundle_normalization(
    raw_value: Any,
    *,
    normalization_id: str | None,
) -> bool:
    if not isinstance(raw_value, Mapping) or not normalization_id:
        return False
    method = str(raw_value.get("method") or "").strip()
    payload_id = str(
        raw_value.get("normalization_id")
        or raw_value.get("model_artifact_id")
        or ""
    ).strip()
    return method == "training_static_zscore" and payload_id == normalization_id


def extract_candidate_static_features(
    candidate: Mapping[str, Any] | None,
) -> Any | None:
    if not isinstance(candidate, Mapping):
        return None
    trace = candidate.get("trace")
    metadata = trace.get("metadata") if isinstance(trace, Mapping) else None
    if not isinstance(metadata, Mapping):
        return None
    for key in ("static_features", "rl_static_features", "model_static_features"):
        if key in metadata:
            return metadata[key]
    return None


