from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from ibkr_trader.api.server import parse_execution_batch_payload
from ibkr_trader.orchestration.scheduling import NextSessionExitStatus
from ibkr_trader.orchestration.scheduling import build_batch_runtime_schedule
from ibkr_trader.orchestration.scheduling import resolve_runtime_timezone


def _sample_batch() -> dict[str, object]:
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
                    "exchange": "XSTO",
                    "currency": "SEK",
                },
                "intent": {
                    "side": "BUY",
                    "position_side": "LONG",
                },
                "sizing": {
                    "mode": "fraction_of_account_nav",
                    "target_fraction_of_account": "1.0",
                },
                "entry": {
                    "order_type": "LIMIT",
                    "submit_at": "2026-04-10T07:25:00Z",
                    "expire_at": "2026-04-10T15:30:00Z",
                    "limit_price": "11.3131",
                },
                "exit": {
                    "force_exit_next_session_open": True,
                },
                "trace": {
                    "reason_code": "risk_policy_orderbook",
                },
            }
        ],
    }


class SchedulingTests(TestCase):
    def test_resolve_runtime_timezone_accepts_stockholm(self) -> None:
        timezone = resolve_runtime_timezone("Europe/Stockholm")
        self.assertEqual(str(timezone), "Europe/Stockholm")

    def test_build_batch_runtime_schedule_projects_into_stockholm(self) -> None:
        batch = parse_execution_batch_payload(_sample_batch())

        schedule = build_batch_runtime_schedule(
            batch,
            runtime_timezone="Europe/Stockholm",
        )

        instruction = schedule.instructions[0]
        self.assertEqual(schedule.runtime_timezone, "Europe/Stockholm")
        self.assertEqual(instruction.submit_at_utc.isoformat(), "2026-04-10T07:25:00+00:00")
        self.assertEqual(
            instruction.submit_at_runtime.isoformat(),
            "2026-04-10T09:25:00+02:00",
        )
        self.assertEqual(
            instruction.expire_at_runtime.isoformat(),
            "2026-04-10T17:30:00+02:00",
        )
        self.assertEqual(instruction.entry_window_seconds, 29100)

    def test_build_batch_runtime_schedule_flags_calendar_requirement(self) -> None:
        batch = parse_execution_batch_payload(_sample_batch())

        schedule = build_batch_runtime_schedule(
            batch,
            runtime_timezone="Europe/Stockholm",
        )

        next_session_exit = schedule.instructions[0].next_session_exit
        self.assertTrue(next_session_exit.requested)
        self.assertEqual(next_session_exit.status, NextSessionExitStatus.CALENDAR_REQUIRED)
        self.assertEqual(next_session_exit.reference_after_date.isoformat(), "2026-04-10")
        self.assertIn("exchange calendar", next_session_exit.note)

    def test_build_batch_runtime_schedule_resolves_next_stockholm_open_from_local_calendar(self) -> None:
        with TemporaryDirectory() as temp_dir:
            parquet_path = Path(temp_dir) / "day_sessions.parquet"
            parquet_path.with_suffix(".csv").write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
                        "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
                    ]
                ),
                encoding="utf-8",
            )

            batch = parse_execution_batch_payload(_sample_batch())
            schedule = build_batch_runtime_schedule(
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=parquet_path,
            )

        next_session_exit = schedule.instructions[0].next_session_exit
        self.assertEqual(next_session_exit.status, NextSessionExitStatus.RESOLVED)
        self.assertEqual(
            next_session_exit.next_session_open_local.isoformat(),
            "2026-04-13T09:00:00+02:00",
        )
        self.assertEqual(
            next_session_exit.next_session_open_utc.isoformat(),
            "2026-04-13T07:00:00+00:00",
        )
        self.assertEqual(next_session_exit.session_kind, "regular")
        self.assertTrue(next_session_exit.calendar_source.endswith("day_sessions.csv"))

    def test_resolve_runtime_timezone_rejects_unknown_name(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unknown runtime timezone"):
            resolve_runtime_timezone("Europe/NotARealPlace")


if __name__ == "__main__":
    import unittest

    unittest.main()
