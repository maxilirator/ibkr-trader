from __future__ import annotations

from scripts.check_operational_readiness import check_action_mapping
from scripts.check_operational_readiness import check_candidate_payloads
from scripts.check_operational_readiness import check_instruction_states
from scripts.check_operational_readiness import check_healthz
from scripts.check_operational_readiness import check_rl_dashboard


def test_readiness_accepts_side_correct_long_and_short_action_spaces() -> None:
    long_check = check_action_mapping(
        model_key="long_trial_106_v1",
        side="LONG",
        action_space=[
            "skip",
            "wait",
            "market_entry",
            "cancel_entry",
            "exit_market",
            "clear_exit",
            "entry_prevclose_-50bp",
            "exit_tp_200bp",
        ],
        entry_action="entry_prevclose_-50bp",
        take_profit_action="exit_tp_200bp",
    )[0]
    short_check = check_action_mapping(
        model_key="short_trial36_v1",
        side="SHORT",
        action_space=[
            "skip",
            "wait",
            "market_entry",
            "cancel_entry",
            "exit_market",
            "clear_exit",
            "entry_prevclose_88bp",
            "exit_tp_180bp",
        ],
        entry_action="entry_prevclose_88bp",
        take_profit_action="exit_tp_180bp",
    )[0]

    assert long_check.status == "ok"
    assert short_check.status == "ok"


def test_readiness_blocks_wrong_short_action_mapping() -> None:
    check = check_action_mapping(
        model_key="short_trial36_v1",
        side="SHORT",
        action_space=["skip", "wait", "entry_prevclose_-50bp", "exit_tp_200bp"],
        entry_action="entry_prevclose_-50bp",
        take_profit_action="exit_tp_200bp",
    )[0]

    assert check.status == "blocker"
    assert "entry_prevclose_88bp" in check.details["missing_actions"]
    assert check.details["mismatches"]["entry_action"] == "entry_prevclose_-50bp"


def test_readiness_candidate_payload_requires_static_features() -> None:
    check = check_candidate_payloads(
        [
            {
                "instruction_id": "candidate-1",
                "model_id": "long_trial_106_v1",
                "trace": {
                    "trade_date": "2026-04-30",
                    "metadata": {
                        "capital_plan": {"schema_version": "rl_capital_plan_v2"}
                    },
                },
            }
        ],
        trade_date="2026-04-30",
    )[0]

    assert check.status == "blocker"
    assert check.details["missing_static_features"] == ["candidate-1"]


def test_readiness_candidate_payload_accepts_static_features_and_capital_plan() -> None:
    check = check_candidate_payloads(
        [
            {
                "instruction_id": "candidate-1",
                "model_id": "long_trial_106_v1",
                "trace": {
                    "trade_date": "2026-04-30",
                    "metadata": {
                        "static_features": {
                            "model_key": "long_trial_106_v1",
                            "feature_names": ["a", "b"],
                            "values": [0.1, 0.2],
                            "normalized": True,
                        },
                        "capital_plan": {"schema_version": "rl_capital_plan_v2"},
                    },
                },
            }
        ],
        trade_date="2026-04-30",
    )[0]

    assert check.status == "ok"
    assert check.details["by_model"] == {"long_trial_106_v1": 1}


def test_readiness_candidate_payload_allows_empty_after_trade_window() -> None:
    check = check_candidate_payloads(
        [],
        trade_date="2026-04-30",
        allow_empty=True,
    )[0]

    assert check.status == "ok"
    assert "after the trade window" in check.message


def test_readiness_dashboard_requires_two_models_and_two_deployments() -> None:
    checks = check_rl_dashboard(
        {
            "models": [],
            "deployments": [],
            "candidates": [],
        },
        trade_date="2026-04-30",
        expected_model_count=2,
        expected_deployment_count=2,
    )

    status_by_name = {check.name: check.status for check in checks}
    assert status_by_name["rl.models"] == "blocker"
    assert status_by_name["rl.deployments"] == "blocker"


def test_readiness_blocks_stale_active_instructions_from_previous_trade_date() -> None:
    check = check_instruction_states(
        {
            "instructions": [
                {
                    "instruction_id": "old-open",
                    "state": "ENTRY_PENDING",
                    "payload": {
                        "instruction": {
                            "trace": {
                                "trade_date": "2026-04-29",
                            }
                        }
                    },
                }
            ]
        },
        trade_date="2026-04-30",
    )[0]

    assert check.status == "blocker"
    assert check.details["stale_active_instruction_ids"] == ["old-open"]


def test_readiness_warns_on_fresh_snapshot_with_recent_failed_attempt() -> None:
    checks = check_healthz(
        {
            "status": "ok",
            "broker_monitor": {
                "snapshot_refresh": {
                    "ok": False,
                    "is_stale": False,
                    "last_success_at": "2026-04-30T11:36:22Z",
                    "last_failure_at": "2026-04-30T11:41:27Z",
                    "error": "Skipped broker snapshot refresh because heartbeat failed.",
                }
            },
        }
    )

    snapshot_check = {
        check.name: check for check in checks
    }["broker_monitor.snapshot_refresh"]
    assert snapshot_check.status == "warning"
