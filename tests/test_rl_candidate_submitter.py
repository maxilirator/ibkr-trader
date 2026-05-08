from __future__ import annotations

from datetime import date
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

import pandas as pd

from scripts.submit_rl_candidate_lists import CandidateBatchConfig
from scripts.submit_rl_candidate_lists import CONFIGS
from scripts.submit_rl_candidate_lists import build_candidate_payload
from scripts.submit_rl_candidate_lists import load_identity_map
from scripts.submit_rl_candidate_lists import load_selected_rows
from scripts.submit_rl_candidate_lists import resolve_capital_plan


def test_default_virtual02_configs_use_live_short_book_keys() -> None:
    configs = {config.side: config for config in CONFIGS}

    assert configs["LONG"].deployment_key == "long_trial_106_virtual_shared_01"
    assert configs["LONG"].book_key == "bb_long_02"
    assert configs["SHORT"].deployment_key == "short_trial_36_virtual_shared_01"
    assert configs["SHORT"].book_key == "bb_short_02"


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
    assert instruction["lifecycle"] == {
        "trade_date": "2026-04-29",
        "scope": "account_book_side_symbol_trade_date",
        "max_entry_orders": 1,
        "max_exit_orders": 1,
        "allow_reentry_after_exit": False,
        "allow_reentry_after_cancel": False,
        "retire_from_active_universe_when_flat": True,
    }
    assert instruction["sizing"] == {
        "mode": "target_notional",
        "target_notional": "1000",
    }
    assert instruction["trace"]["metadata"]["static_features"] == {
        "schema_version": "rl_static_features_v1",
        "model_key": "short_trial36_v1",
        "feature_names": ["meta_score"],
        "values": [1.0],
        "normalized": False,
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


def test_resolve_capital_plan_divides_side_budget_across_dynamic_candidate_count() -> None:
    target_notional, capital_plan = resolve_capital_plan(
        side="LONG",
        account_key="virtualrl01",
        candidate_count=15,
        default_target_notional="1000",
        account_equity_reference="200000",
        long_allocation_pct="0.90",
        short_allocation_pct="0.80",
        long_budget="100000",
        short_budget="100000",
        max_notional_per_name=None,
    )

    assert target_notional == "6666"
    assert capital_plan == {
        "schema_version": "rl_capital_plan_v2",
        "allocation_method": "gross_budget_equal_weight",
        "account_key": "VIRTUALRL01",
        "account_currency": "SEK",
        "account_equity_reference": "200000",
        "capital_base": "net_liquidation_value",
        "strategy_key": "bucket_booster_long",
        "strategy_side": "LONG",
        "book_allocation_pct": None,
        "max_book_gross_account_pct": "0.90",
        "strategy_gross_budget": "100000",
        "candidate_count": 15,
        "per_name_target_notional": "6666",
        "max_notional_per_name": None,
        "min_order_notional": "1000",
        "rounding": "whole_shares_down",
        "require_shortable": False,
        "require_borrow_rate_available": False,
        "short_sale_proceeds_reinvested": False,
        "allocation_guard": {
            "schema_version": "rl_allocation_guard_v1",
            "account_key": "VIRTUALRL01",
            "capital_base": "net_liquidation_value",
            "max_long_gross_account_pct": "0.90",
            "max_short_gross_account_pct": "0.80",
            "max_total_gross_account_pct": "1.70",
            "max_abs_net_exposure_account_pct": "0.25",
            "min_excess_liquidity_buffer_pct": "0.20",
            "block_if_margin_preflight_fails": True,
            "block_if_projected_maintenance_margin_exceeded": True,
        },
    }


def test_resolve_capital_plan_uses_account_pct_as_side_gross_exposure() -> None:
    target_notional, capital_plan = resolve_capital_plan(
        side="SHORT",
        account_key="virtualrl01",
        candidate_count=20,
        default_target_notional="1000",
        account_equity_reference="100000",
        long_allocation_pct="0.90",
        short_allocation_pct="0.80",
        long_budget=None,
        short_budget=None,
        max_notional_per_name=None,
    )

    assert target_notional == "4000"
    assert capital_plan["allocation_method"] == "account_pct_gross_exposure_equal_weight"
    assert capital_plan["strategy_key"] == "bucket_booster_short"
    assert capital_plan["book_allocation_pct"] == "0.8"
    assert capital_plan["max_book_gross_account_pct"] == "0.80"
    assert capital_plan["strategy_gross_budget"] == "80000"
    assert capital_plan["per_name_target_notional"] == "4000"
    assert capital_plan["require_shortable"] is True
    assert capital_plan["require_borrow_rate_available"] is True
    assert capital_plan["short_sale_proceeds_reinvested"] is False
    assert capital_plan["allocation_guard"]["max_total_gross_account_pct"] == "1.70"
