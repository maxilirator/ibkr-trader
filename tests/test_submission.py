from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from ibkr_trader.api.server import parse_execution_batch_payload
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
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

    def test_submit_execution_batch_rejects_existing_instruction_id(self) -> None:
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

            submit_execution_batch(
                self.session_factory,
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )

            with self.assertRaisesRegex(SubmissionConflictError, "already exists"):
                submit_execution_batch(
                    self.session_factory,
                    batch,
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
