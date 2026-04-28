from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from scripts.bootstrap_rl_registry import selected_candidate_symbols


def test_selected_candidate_symbols_is_uncapped_by_default() -> None:
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "candidate_tape.parquet"
        pd.DataFrame(
            [
                {
                    "instrument": "low",
                    "datetime": "2026-03-23",
                    "selected": True,
                    "meta_score": 0.5,
                },
                {
                    "instrument": "high",
                    "datetime": "2026-03-23",
                    "selected": True,
                    "meta_score": 2.0,
                },
                {
                    "instrument": "skip",
                    "datetime": "2026-03-23",
                    "selected": False,
                    "meta_score": 9.0,
                },
            ]
        ).to_parquet(path, index=False)

        symbols = selected_candidate_symbols(
            path,
            candidate_date="latest",
            limit=None,
        )

    assert symbols == ("HIGH", "LOW")


def test_selected_candidate_symbols_can_still_be_capped_explicitly() -> None:
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "candidate_tape.parquet"
        pd.DataFrame(
            [
                {
                    "instrument": "low",
                    "datetime": "2026-03-23",
                    "selected": True,
                    "meta_score": 0.5,
                },
                {
                    "instrument": "high",
                    "datetime": "2026-03-23",
                    "selected": True,
                    "meta_score": 2.0,
                },
            ]
        ).to_parquet(path, index=False)

        symbols = selected_candidate_symbols(
            path,
            candidate_date="latest",
            limit=1,
        )

    assert symbols == ("HIGH",)
