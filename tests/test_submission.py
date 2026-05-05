from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from sqlalchemy import select

from ibkr_trader.api.server import parse_execution_batch_payload
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.orchestration.operator_controls import KillSwitchActiveError
from ibkr_trader.orchestration.operator_controls import set_kill_switch_state
from ibkr_trader.orchestration.state_machine import ExecutionState
from ibkr_trader.orchestration.submission import SubmissionConflictError
from ibkr_trader.orchestration.submission import submit_execution_batch


def _sample_payload() -> dict[str, object]:
    return {
        "schema_version": "2026-04-10",
        "source": {
            "system": "q-training",
            "batch_id": "trial_27-2026-04-10-prod-long-01",
            "generated_at": "2026-04-10T02:15:44Z",
        },
        "instructions": [
            {
                "instruction_id": "2026-04-10-GTW05-long_risk_book-SIVE-long-01",
                "account": {
                    "account_key": "GTW05",
                    "book_key": "long_risk_book",
                },
                "instrument": {
                    "symbol": "SIVE",
                    "security_type": "STK",
                    "exchange": "SMART",
                    "primary_exchange": "SFB",
                    "currency": "SEK",
                    "isin": "SE0003917798",
                    "aliases": ["SIVE.ST"],
                },
                "intent": {
                    "side": "BUY",
                    "position_side": "LONG",
                },
                "sizing": {
                    "mode": "target_quantity",
                    "target_quantity": "100",
                },
                "entry": {
                    "order_type": "LIMIT",
                    "submit_at": "2026-04-10T09:25:00+02:00",
                    "expire_at": "2026-04-10T17:30:00+02:00",
                    "limit_price": "11.3131",
                },
                "exit": {
                    "force_exit_next_session_open": True,
                },
                "trace": {
                    "reason_code": "risk_policy_orderbook",
                    "company_name": "Sivers Semiconductors",
                },
            }
        ],
    }


def _model_routed_payload() -> dict[str, object]:
    return {
        "schema_version": "2026-04-25",
        "source": {
            "system": "q-training",
            "batch_id": "long-rl-smoke-2026-04-28",
            "generated_at": "2026-04-27T21:30:00Z",
            "strategy_id": "long_trial_106",
            "policy_id": "long_trial_106_v1",
        },
        "instructions": [
            {
                "instruction_id": "2026-04-28-VIRTUALRL01-long-AXFO-model-routed",
                "account": {
                    "account_key": "VIRTUALRL01",
                    "book_key": "rl_shared_long_trial_106_virtual_01",
                    "book_role": "virtual",
                    "book_side": "LONG",
                },
                "instrument": {
                    "symbol": "AXFO",
                    "security_type": "STK",
                    "exchange": "XSTO",
                    "currency": "SEK",
                },
                "intent": {
                    "side": "BUY",
                    "position_side": "LONG",
                },
                "sizing": {
                    "mode": "target_notional",
                    "target_notional": "1000",
                    "funding_basis": "cash",
                },
                "execution": {
                    "mode": "model_routed",
                    "model_id": "long_trial_106_v1",
                    "model_family": "canonical_long_live_execution_policy",
                    "model_version": "v1",
                    "model_artifact_id": "trial_106",
                    "window": {
                        "start_at": "2026-04-28T09:00:00+02:00",
                        "end_at": "2026-04-28T17:30:00+02:00",
                    },
                },
                "trace": {
                    "reason_code": "rl_model_routed_smoke_test",
                    "trade_date": "2026-04-28",
                    "data_cutoff_date": "2026-03-23",
                    "metadata": {
                        "source": "selected_approved_long_lockbox_rows",
                    },
                },
            }
        ],
    }


class SubmissionTests(TestCase):
    def setUp(self) -> None:
        self.engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(self.engine)
        self.session_factory = create_session_factory(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_submit_execution_batch_persists_instruction_and_initial_event(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            schedule_path.with_suffix(".csv").write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
                        "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
                    ]
                ),
                encoding="utf-8",
            )

            result = submit_execution_batch(
                self.session_factory,
                parse_execution_batch_payload(_sample_payload()),
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )

        self.assertEqual(result.instruction_count, 1)
        instruction = result.instructions[0]
        self.assertEqual(instruction.state, ExecutionState.ENTRY_PENDING.value)
        self.assertEqual(instruction.symbol, "SIVE")
        self.assertEqual(
            instruction.runtime_schedule["next_session_exit"]["status"],
            "resolved",
        )
        self.assertEqual(
            instruction.runtime_schedule["next_session_exit"]["next_session_open_local"],
            "2026-04-13T09:00:00+02:00",
        )
        self.assertEqual(instruction.initial_event.event_type, "instruction_submitted")
        self.assertEqual(instruction.initial_event.state_after, ExecutionState.ENTRY_PENDING.value)

    def test_submit_model_routed_batch_persists_for_rl_agent_pickup(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            schedule_path.with_suffix(".csv").write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-28,Europe/Stockholm,09:00,17:30,regular,base,override",
                    ]
                ),
                encoding="utf-8",
            )

            result = submit_execution_batch(
                self.session_factory,
                parse_execution_batch_payload(_model_routed_payload()),
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )

        self.assertEqual(result.instruction_count, 1)
        instruction = result.instructions[0]
        self.assertEqual(instruction.state, ExecutionState.MODEL_ROUTED_PENDING.value)
        self.assertEqual(instruction.order_type, "MODEL_ROUTED")
        self.assertEqual(instruction.submit_at.isoformat(), "2026-04-28T09:00:00+02:00")
        self.assertEqual(instruction.expire_at.isoformat(), "2026-04-28T17:30:00+02:00")
        self.assertEqual(
            instruction.runtime_schedule["next_session_exit"]["status"],
            "not_requested",
        )
        self.assertEqual(
            instruction.initial_event.note,
            "Model-routed instruction validated and persisted for RL agent pickup.",
        )

        with session_scope(self.session_factory) as session:
            record = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id
                    == "2026-04-28-VIRTUALRL01-long-AXFO-model-routed"
                )
            ).scalar_one()

        self.assertEqual(record.state, ExecutionState.MODEL_ROUTED_PENDING.value)
        self.assertEqual(
            record.payload["instruction"]["execution"]["model_id"],
            "long_trial_106_v1",
        )

    def test_parse_rejects_virtual_book_role_on_live_account(self) -> None:
        payload = _model_routed_payload()
        instruction_payload = payload["instructions"][0]
        assert isinstance(instruction_payload, dict)
        account_payload = instruction_payload["account"]
        assert isinstance(account_payload, dict)
        account_payload["account_key"] = "U25245596"

        with self.assertRaisesRegex(
            ValueError,
            "book_role=virtual requires a virtual account_key",
        ):
            parse_execution_batch_payload(payload)

    def test_parse_model_routed_batch_accepts_root_model_shortcut(self) -> None:
        payload = _model_routed_payload()
        instruction_payload = payload["instructions"][0]
        assert isinstance(instruction_payload, dict)
        execution_payload = instruction_payload["execution"]
        assert isinstance(execution_payload, dict)
        del execution_payload["model_id"]
        instruction_payload["model"] = "long_trial_106_v1"

        batch = parse_execution_batch_payload(payload)

        self.assertEqual(batch.instructions[0].execution.model_id, "long_trial_106_v1")

    def test_parse_model_routed_batch_accepts_nested_static_feature_metadata(self) -> None:
        payload = _model_routed_payload()
        instruction_payload = payload["instructions"][0]
        assert isinstance(instruction_payload, dict)
        trace_payload = instruction_payload["trace"]
        assert isinstance(trace_payload, dict)
        metadata = trace_payload["metadata"]
        assert isinstance(metadata, dict)
        metadata["static_features"] = {
            "schema_version": "rl_static_features_v1",
            "model_key": "long_trial_106_v1",
            "feature_names": ["rank_score_z", "turnover_z"],
            "values": [0.25, -1.5],
            "normalized": True,
            "source": "upstream_candidate_payload",
        }

        batch = parse_execution_batch_payload(payload)

        self.assertEqual(
            batch.instructions[0].trace.metadata["static_features"]["values"],
            [0.25, -1.5],
        )

    def test_submit_execution_batch_returns_existing_rows_for_exact_replay(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            schedule_path.with_suffix(".csv").write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
                        "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
                    ]
                ),
                encoding="utf-8",
            )
            batch = parse_execution_batch_payload(_sample_payload())

            first = submit_execution_batch(
                self.session_factory,
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )

            replay = submit_execution_batch(
                self.session_factory,
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )

        self.assertEqual(replay.instruction_count, 1)
        self.assertEqual(replay.instructions[0].record_id, first.instructions[0].record_id)
        self.assertEqual(
            replay.instructions[0].initial_event.event_id,
            first.instructions[0].initial_event.event_id,
        )

        with session_scope(self.session_factory) as session:
            rows = session.execute(select(InstructionRecord)).scalars().all()
        self.assertEqual(len(rows), 1)

    def test_submit_execution_batch_rejects_existing_instruction_id_with_changed_payload(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            schedule_path.with_suffix(".csv").write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
                        "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
                    ]
                ),
                encoding="utf-8",
            )
            submit_execution_batch(
                self.session_factory,
                parse_execution_batch_payload(_sample_payload()),
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )
            changed_payload = deepcopy(_sample_payload())
            changed_instruction = changed_payload["instructions"][0]
            assert isinstance(changed_instruction, dict)
            changed_entry = changed_instruction["entry"]
            assert isinstance(changed_entry, dict)
            changed_entry["limit_price"] = "11.9999"

            with self.assertRaisesRegex(SubmissionConflictError, "different payload"):
                submit_execution_batch(
                    self.session_factory,
                    parse_execution_batch_payload(changed_payload),
                    runtime_timezone="Europe/Stockholm",
                    session_calendar_path=schedule_path,
                )

    def test_submit_execution_batch_rejects_when_kill_switch_is_enabled(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            schedule_path.with_suffix(".csv").write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
                        "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
                    ]
                ),
                encoding="utf-8",
            )
            set_kill_switch_state(
                self.session_factory,
                enabled=True,
                reason="Freeze new entries.",
                updated_by="test",
            )

            with self.assertRaisesRegex(KillSwitchActiveError, "kill switch"):
                submit_execution_batch(
                    self.session_factory,
                    parse_execution_batch_payload(_sample_payload()),
                    runtime_timezone="Europe/Stockholm",
                    session_calendar_path=schedule_path,
                )


if __name__ == "__main__":
    import unittest

    unittest.main()
