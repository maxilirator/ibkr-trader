from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
from pathlib import Path


@dataclass(frozen=True, slots=True)
class RLRuntimeContract:
    model_key: str
    artifact_id: str
    checkpoint_sha256: str
    checkpoint_size_bytes: int
    obs_dim: int
    hidden_dim: int
    action_names: tuple[str, ...]
    static_feature_count: int
    static_mean_sha256: str
    static_std_sha256: str
    scaler_package: str
    scaler_filename: str
    include_market_context: bool
    max_bars: int


LONG_TRIAL_106_V1_CONTRACT = RLRuntimeContract(
    model_key="long_trial_106_v1",
    artifact_id="trial_106_seed240",
    checkpoint_sha256=(
        "c95de9cd05ced12c18e4a59d86bdeed6c1d1c1d705ec5cc8d10977092a21e250"
    ),
    checkpoint_size_bytes=575145,
    obs_dim=979,
    hidden_dim=128,
    action_names=(
        "skip",
        "wait",
        "market_entry",
        "cancel_entry",
        "exit_market",
        "clear_exit",
        "entry_prevclose_-50bp",
        "exit_tp_200bp",
    ),
    static_feature_count=106,
    static_mean_sha256=(
        "03c526b2c8d885f54bf90dc28f6e9afe07bbdb00eac5836b465ee1f797163ad6"
    ),
    static_std_sha256=(
        "bbdd10ea7ee7a5cf4aa44eedaccfd717b3de7cd60b90940755c313530d15ee5c"
    ),
    scaler_package="ibkr_trader.rl.contracts.long_trial_106_v1",
    scaler_filename="static_scaler_long_trial_106_v1.json",
    include_market_context=True,
    max_bars=102,
)


_CONTRACTS_BY_MODEL_KEY = {
    LONG_TRIAL_106_V1_CONTRACT.model_key: LONG_TRIAL_106_V1_CONTRACT,
}


def runtime_contract_for_model(model_key: str) -> RLRuntimeContract | None:
    return _CONTRACTS_BY_MODEL_KEY.get(str(model_key).strip())


def contract_scaler_path(contract: RLRuntimeContract) -> Path:
    return Path(
        resources.files(contract.scaler_package).joinpath(contract.scaler_filename)
    )
