from __future__ import annotations

from datetime import date
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

import pandas as pd

from scripts.submit_rl_candidate_lists import CandidateBatchConfig
from scripts.submit_rl_candidate_lists import build_candidate_payload
from scripts.submit_rl_candidate_lists import load_identity_map
from scripts.submit_rl_candidate_lists import load_selected_rows


def test_load_selected_rows_uses_latest_selected_sorted_by_score() -> None:
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "candidate_tape.parquet"
        pd.DataFrame(
            [
                {
                    "instrument": "aaa",
                    "datetime": "2026-03-22",
                    "selected": True,
                    "meta_score": 1.0,
                },
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
                    "meta_score": 10.0,
                },
            ]
        ).to_parquet(path, index=False)

        rows, candidate_date = load_selected_rows(path, candidate_date="latest", limit=None)

    assert candidate_date == "2026-03-23"
    assert rows["instrument"].tolist() == ["high", "low"]


def test_load_selected_rows_applies_explicit_cap_only_when_requested() -> None:
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
                    "instrument": "mid",
                    "datetime": "2026-03-23",
                    "selected": True,
                    "meta_score": 1.5,
                },
                {
                    "instrument": "high",
                    "datetime": "2026-03-23",
                    "selected": True,
                    "meta_score": 2.0,
                },
            ]
        ).to_parquet(path, index=False)

        rows, candidate_date = load_selected_rows(path, candidate_date="latest", limit=2)

    assert candidate_date == "2026-03-23"
    assert rows["instrument"].tolist() == ["high", "mid"]


def test_build_candidate_payload_creates_per_symbol_model_routed_instructions() -> None:
    with TemporaryDirectory() as temp_dir:
        identity_path = Path(temp_dir) / "identity.parquet"
        pd.DataFrame(
            [
                {
                    "instrument": "eric-b",
                    "company_name": "Ericsson",
                    "isin": "SE0000108656",
                    "ticker_alias": "ERIC B",
                    "yahoo_symbol": "ERIC-B.ST",
                    "instrument_aliases_json": '["ERIC-B.ST", "ericsson"]',
                }
            ]
        ).to_parquet(identity_path, index=False)
        identity = load_identity_map(identity_path)
        candidate_path = Path(temp_dir) / "candidate_tape.parquet"
        static_feature_cols_path = Path(temp_dir) / "static_feature_cols.csv"
        static_feature_cols_path.write_text("static_feature_cols\nmeta_score\n", encoding="utf-8")
        rows = pd.DataFrame(
            [
                {
                    "instrument": "eric-b",
                    "datetime": "2026-03-23",
                    "selected": True,
                    "meta_score": 1.0,
                }
            ]
        )
        config = CandidateBatchConfig(
            side="SHORT",
            strategy_id="short_trial_36",
            model_key="short_trial36_v1",
            model_family="canonical_short_live_execution_policy",
            model_artifact_id="trial_36",
            deployment_key="short_trial_36_virtual_shared_01",
            book_key="rl_shared_short_trial_36_virtual_01",
            candidate_tape_path=candidate_path,
            static_feature_cols_path=static_feature_cols_path,
        )

        payload = build_candidate_payload(
            config,
            rows,
            identity=identity,
            account_key="VIRTUALRL01",
            trade_date=date(2026, 4, 29),
            candidate_date="2026-03-23",
            target_notional="1000",
            start_at=datetime(2026, 4, 29, 9, 0, tzinfo=ZoneInfo("Europe/Stockholm")),
            end_at=datetime(2026, 4, 29, 17, 30, tzinfo=ZoneInfo("Europe/Stockholm")),
            generated_at=datetime(2026, 4, 29, 4, 0, tzinfo=ZoneInfo("UTC")),
        )

    assert payload["schema_version"] == "2026-04-25"
    assert payload["source"]["policy_id"] == "short_trial36_v1"
    assert len(payload["instructions"]) == 1
    instruction = payload["instructions"][0]
    assert instruction["instrument"]["symbol"] == "ERIC-B"
    assert instruction["instrument"]["isin"] == "SE0000108656"
    assert instruction["instrument"]["aliases"][0] == "ERIC B"
    assert instruction["intent"] == {"side": "SELL", "position_side": "SHORT"}
    assert instruction["execution"]["mode"] == "model_routed"
    assert instruction["execution"]["model_id"] == "short_trial36_v1"
    assert instruction["trace"]["metadata"]["static_features"] == {
        "schema_version": "rl_static_features_v1",
        "model_key": "short_trial36_v1",
        "feature_names": ["meta_score"],
        "values": [1.0],
        "normalized": True,
        "source": "lockbox_candidate_row",
    }


def test_build_candidate_payload_applies_run_suffix_to_ids() -> None:
    config = CandidateBatchConfig(
        side="LONG",
        strategy_id="long_trial_106",
        model_key="long_trial_106_v1",
        model_family="canonical_long_live_execution_policy",
        model_artifact_id="trial_106",
        deployment_key="long_trial_106_virtual_shared_01",
        book_key="rl_shared_long_trial_106_virtual_01",
        candidate_tape_path=Path("/tmp/candidate_tape.parquet"),
    )

    payload = build_candidate_payload(
        config,
        pd.DataFrame(
            [
                {
                    "instrument": "axfo",
                    "datetime": "2026-03-23",
                    "selected": True,
                    "meta_score": 1.0,
                }
            ]
        ),
        identity={},
        account_key="VIRTUALRL01",
        trade_date=date(2026, 4, 29),
        candidate_date="2026-03-23",
        target_notional="1000",
        start_at=datetime(2026, 4, 29, 9, 0, tzinfo=ZoneInfo("Europe/Stockholm")),
        end_at=datetime(2026, 4, 29, 17, 30, tzinfo=ZoneInfo("Europe/Stockholm")),
        generated_at=datetime(2026, 4, 29, 4, 0, tzinfo=ZoneInfo("UTC")),
        run_suffix="prep-2",
    )

    assert payload["source"]["batch_id"] == "rl-long-2026-04-29-selected-2026-03-23-prep-2"
    assert payload["source"]["release_id"] == "rl_selected_long_2026-04-29-prep-2"
    assert (
        payload["instructions"][0]["instruction_id"]
        == "2026-04-29-VIRTUALRL01-rl-long-AXFO-model-routed-prep-2-01"
    )
