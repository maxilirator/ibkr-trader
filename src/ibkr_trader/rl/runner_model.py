from __future__ import annotations

import hashlib
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
from ibkr_trader.rl.model_contracts import contract_scaler_path
from ibkr_trader.rl.model_contracts import runtime_contract_for_model
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
    contract = runtime_contract_for_model(config.model_key)
    if contract is not None:
        _validate_contract_artifact_identity(config, contract)
    summary = json.loads(config.summary_path.read_text())
    action_names = [str(item) for item in summary["action_names"]]
    if action_names != list(config.action_space):
        raise ValueError(
            f"{config.model_key} action space mismatch between deployed bundle summary and trader registry"
        )
    if contract is not None and tuple(action_names) != contract.action_names:
        raise ValueError(
            f"{config.model_key} action order does not match runtime contract: "
            f"{action_names}"
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
    if contract is not None:
        _validate_contract_model_shape(
            config,
            contract,
            obs_dim=obs_dim,
            hidden_dim=hidden_dim,
            action_dim=action_dim,
        )
    q_network_cls = _q_network_class(nn_module)
    model = q_network_cls(obs_dim=obs_dim, action_dim=action_dim, hidden_dim=hidden_dim)
    model.load_state_dict(state_dict, strict=True)
    model.eval()
    static_feature_names = list(read_static_feature_names(config.static_feature_cols_path))
    static_feature_mean: np.ndarray | None = None
    static_feature_std: np.ndarray | None = None
    static_feature_normalization_id: str | None = None
    static_feature_mean_sha256: str | None = None
    static_feature_std_sha256: str | None = None
    static_normalization_path = (
        contract_scaler_path(contract)
        if contract is not None
        else config.static_feature_normalization_path
    )
    if static_normalization_path is not None:
        (
            static_feature_mean,
            static_feature_std,
            static_feature_normalization_id,
            static_feature_mean_sha256,
            static_feature_std_sha256,
        ) = load_static_feature_normalization(
            static_normalization_path,
            expected_feature_names=static_feature_names,
            expected_mean_sha256=(
                contract.static_mean_sha256 if contract is not None else None
            ),
            expected_std_sha256=(
                contract.static_std_sha256 if contract is not None else None
            ),
        )
    expected_static_count = summary.get("static_feature_count")
    if expected_static_count is not None and int(expected_static_count) != len(static_feature_names):
        raise ValueError(
            f"{config.model_key} static feature count mismatch: "
            f"summary={expected_static_count} csv={len(static_feature_names)}"
        )
    if contract is not None and len(static_feature_names) != contract.static_feature_count:
        raise ValueError(
            f"{config.model_key} static feature count does not match runtime contract: "
            f"{len(static_feature_names)} != {contract.static_feature_count}"
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
        static_feature_mean_sha256=static_feature_mean_sha256,
        static_feature_std_sha256=static_feature_std_sha256,
    )


def load_static_feature_normalization(
    path: Path,
    *,
    expected_feature_names: list[str],
    expected_mean_sha256: str | None = None,
    expected_std_sha256: str | None = None,
) -> tuple[np.ndarray, np.ndarray, str, str, str]:
    payload = json.loads(path.read_text())
    if not isinstance(payload, Mapping):
        raise ValueError(f"{path} must contain a JSON object")
    schema_version = str(payload.get("schema_version") or "")
    if schema_version == "rl_static_feature_normalization_v1":
        feature_names = payload.get("feature_names")
        if feature_names != expected_feature_names:
            raise ValueError(
                f"{path} feature_names do not match static_feature_cols.csv"
            )
        mean = _float_array(payload.get("mean"), field_name=f"{path}.mean")
        std = _float_array(payload.get("std"), field_name=f"{path}.std")
        normalization_id = str(
            payload.get("normalization_id")
            or payload.get("model_artifact_id")
            or path.name
        )
        declared_mean_sha256 = str(payload.get("mean_sha256") or "").strip() or None
        declared_std_sha256 = str(payload.get("std_sha256") or "").strip() or None
    elif _looks_like_contract_static_scaler(payload):
        (
            mean,
            std,
            normalization_id,
            declared_mean_sha256,
            declared_std_sha256,
        ) = _contract_static_scaler_arrays(
            payload,
            expected_feature_names=expected_feature_names,
            path=path,
        )
    else:
        raise ValueError(
            f"{path} must contain rl_static_feature_normalization_v1 or "
            "the runtime contract static scaler format"
        )
    if mean.shape != std.shape or mean.shape[0] != len(expected_feature_names):
        raise ValueError(
            f"{path} mean/std length must match {len(expected_feature_names)} features"
        )
    if not np.isfinite(mean).all() or not np.isfinite(std).all():
        raise ValueError(f"{path} mean/std contain non-finite values")
    if np.any(std <= 0.0):
        raise ValueError(f"{path} std must be positive")
    mean = mean.astype(np.float32)
    std = std.astype(np.float32)
    mean_sha256 = _float32_array_sha256(mean)
    std_sha256 = _float32_array_sha256(std)
    if declared_mean_sha256 is not None and declared_mean_sha256 != mean_sha256:
        raise ValueError(
            f"{path} declared mean_sha256 {declared_mean_sha256} does not match "
            f"computed {mean_sha256}"
        )
    if declared_std_sha256 is not None and declared_std_sha256 != std_sha256:
        raise ValueError(
            f"{path} declared std_sha256 {declared_std_sha256} does not match "
            f"computed {std_sha256}"
        )
    if expected_mean_sha256 is not None and mean_sha256 != expected_mean_sha256:
        raise ValueError(
            f"{path} mean_sha256 {mean_sha256} does not match runtime contract "
            f"{expected_mean_sha256}"
        )
    if expected_std_sha256 is not None and std_sha256 != expected_std_sha256:
        raise ValueError(
            f"{path} std_sha256 {std_sha256} does not match runtime contract "
            f"{expected_std_sha256}"
        )
    return mean, std, normalization_id, mean_sha256, std_sha256


def _validate_contract_artifact_identity(
    config: PromotedRLModelArtifact,
    contract: Any,
) -> None:
    if config.model_artifact_id != contract.artifact_id:
        raise ValueError(
            f"{config.model_key} artifact_id {config.model_artifact_id!r} "
            f"does not match runtime contract {contract.artifact_id!r}"
        )
    checkpoint_size = config.promoted_checkpoint_path.stat().st_size
    if checkpoint_size != contract.checkpoint_size_bytes:
        raise ValueError(
            f"{config.model_key} checkpoint size {checkpoint_size} does not match "
            f"runtime contract {contract.checkpoint_size_bytes}"
        )
    checkpoint_sha256 = _file_sha256(config.promoted_checkpoint_path)
    if checkpoint_sha256 != contract.checkpoint_sha256:
        raise ValueError(
            f"{config.model_key} checkpoint sha256 {checkpoint_sha256} does not "
            f"match runtime contract {contract.checkpoint_sha256}"
        )


def _validate_contract_model_shape(
    config: PromotedRLModelArtifact,
    contract: Any,
    *,
    obs_dim: int,
    hidden_dim: int,
    action_dim: int,
) -> None:
    if obs_dim != contract.obs_dim:
        raise ValueError(
            f"{config.model_key} obs_dim {obs_dim} does not match runtime contract "
            f"{contract.obs_dim}"
        )
    if hidden_dim != contract.hidden_dim:
        raise ValueError(
            f"{config.model_key} hidden_dim {hidden_dim} does not match runtime "
            f"contract {contract.hidden_dim}"
        )
    if action_dim != len(contract.action_names):
        raise ValueError(
            f"{config.model_key} action_dim {action_dim} does not match runtime "
            f"contract {len(contract.action_names)}"
        )


def _looks_like_contract_static_scaler(payload: Mapping[str, Any]) -> bool:
    return isinstance(payload.get("features"), list) and payload.get("feature_count") is not None


def _contract_static_scaler_arrays(
    payload: Mapping[str, Any],
    *,
    expected_feature_names: list[str],
    path: Path,
) -> tuple[np.ndarray, np.ndarray, str, str | None, str | None]:
    raw_features = payload.get("features")
    if not isinstance(raw_features, list):
        raise ValueError(f"{path} features must be an array")
    feature_count = int(payload.get("feature_count") or len(raw_features))
    if feature_count != len(expected_feature_names):
        raise ValueError(
            f"{path} feature_count {feature_count} does not match "
            f"{len(expected_feature_names)} static feature names"
        )
    features_by_index: list[Mapping[str, Any] | None] = [None] * feature_count
    for raw_feature in raw_features:
        if not isinstance(raw_feature, Mapping):
            raise ValueError(f"{path} features entries must be objects")
        index = int(raw_feature.get("index"))
        if index < 0 or index >= feature_count:
            raise ValueError(f"{path} feature index {index} is outside range")
        features_by_index[index] = raw_feature
    missing_indexes = [idx for idx, value in enumerate(features_by_index) if value is None]
    if missing_indexes:
        raise ValueError(f"{path} missing scaler feature indexes {missing_indexes}")

    mean_values: list[float] = []
    std_values: list[float] = []
    for idx, feature in enumerate(features_by_index):
        assert feature is not None
        feature_name = str(feature.get("feature_name") or "").strip()
        mean_values.append(float(feature["mean"]))
        std_values.append(float(feature["std"]))
        if feature_name != expected_feature_names[idx]:
            raise ValueError(
                f"{path} feature order mismatch at index {idx}: "
                f"{feature_name!r} != {expected_feature_names[idx]!r}"
            )
    normalization_id = str(
        payload.get("artifact_id")
        or payload.get("normalization_id")
        or path.name
    )
    return (
        np.asarray(mean_values, dtype=np.float32),
        np.asarray(std_values, dtype=np.float32),
        normalization_id,
        str(payload.get("mean_sha256") or "").strip() or None,
        str(payload.get("std_sha256") or "").strip() or None,
    )


def _float_array(raw_value: Any, *, field_name: str) -> np.ndarray:
    if not isinstance(raw_value, list) or not raw_value:
        raise ValueError(f"{field_name} must be a non-empty array")
    values = np.asarray([float(value) for value in raw_value], dtype=np.float32)
    return values


def _float32_array_sha256(values: np.ndarray) -> str:
    return hashlib.sha256(np.asarray(values, dtype=np.float32).tobytes()).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
        mean_sha256=getattr(loaded, "static_feature_mean_sha256", None),
        std_sha256=getattr(loaded, "static_feature_std_sha256", None),
    )
    mean = getattr(loaded, "static_feature_mean", None)
    std = getattr(loaded, "static_feature_std", None)
    source = str(raw_payload.get("source") or "upstream_candidate_payload")
    claims_normalized = bool(raw_payload.get("normalized", True))
    has_strict_scaler_contract = bool(
        getattr(loaded, "static_feature_mean_sha256", None)
        and getattr(loaded, "static_feature_std_sha256", None)
    )
    if (
        claims_normalized
        and mean is not None
        and std is not None
        and not already_model_normalized
        and has_strict_scaler_contract
    ):
        raise ValueError(
            f"candidate static_features for {loaded.config.model_key} {symbol} claim "
            "normalized=true but do not carry the exact runtime scaler marker/hash"
        )
    if mean is not None and std is not None and not already_model_normalized:
        values = (values - mean) / std
        normalized = True
        source = f"{source}+trader_static_zscore"
    else:
        normalized = claims_normalized
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
    mean_sha256: str | None = None,
    std_sha256: str | None = None,
) -> bool:
    if not isinstance(raw_value, Mapping) or not normalization_id:
        return False
    method = str(raw_value.get("method") or "").strip()
    payload_id = str(
        raw_value.get("normalization_id")
        or raw_value.get("model_artifact_id")
        or ""
    ).strip()
    if method != "training_static_zscore" or payload_id != normalization_id:
        return False
    if mean_sha256 and str(raw_value.get("mean_sha256") or "").strip() != mean_sha256:
        return False
    if std_sha256 and str(raw_value.get("std_sha256") or "").strip() != std_sha256:
        return False
    return True


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
