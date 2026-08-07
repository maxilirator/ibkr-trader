from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import numpy as np

STOCKHOLM_TZ = ZoneInfo("Europe/Stockholm")
DEFAULT_BENCHMARK_SYMBOLS = ("OMXS30",)
BENCHMARK_STREAM_CONTRACTS = {
    "OMXS30": {
        "symbol": "OMXS30",
        "security_type": "IND",
        "exchange": "OMS",
        "currency": "SEK",
        "primary_exchange": "",
    }
}
DEFAULT_CANDIDATE_REASON_CODES = (
    "rl_model_routed_selected_candidate",
    "rl_model_routed_candidate",
    "rl_model_routed_candidate_tape_selected",
)
DEFAULT_MAX_STREAM_SYMBOLS = 120
DEFAULT_STREAM_WARNING_SYMBOLS = 100


@dataclass(slots=True)
class LoadedModel:
    config: Any
    action_names: list[str]
    obs_dim: int
    model: Any
    static_feature_names: list[str]
    static_feature_mean: np.ndarray | None = None
    static_feature_std: np.ndarray | None = None
    static_feature_normalization_id: str | None = None
    static_feature_mean_sha256: str | None = None
    static_feature_std_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class LoadedDeployment:
    deployment_key: str
    model_key: str
    account_key: str
    book_key: str
    mode: str
    loaded: LoadedModel


@dataclass(frozen=True, slots=True)
class RuntimeStateContext:
    states: dict[str, Any]
    blocked_symbols: dict[str, Mapping[str, Any]]
    source: str
