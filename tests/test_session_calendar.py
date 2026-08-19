from __future__ import annotations

import unittest
from datetime import date
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from ibkr_trader.orchestration.session_calendar import find_session_for_date
from ibkr_trader.orchestration.session_calendar import find_next_session_open
from ibkr_trader.orchestration.session_calendar import load_session_calendar


_CSV_SESSIONS = "\n".join(
    [
        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
        "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
        "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
    ]
)


def _write_empty_parquet_calendar(parquet_path: Path) -> None:
    import duckdb

    duckdb.execute(
        """
        COPY (
            SELECT
                DATE '2026-04-30' AS session_date,
                'Europe/Stockholm' AS timezone,
                '09:00' AS open_time,
                '13:00' AS close_time,
                'override' AS session_kind
            WHERE false
        )
        TO ? (FORMAT PARQUET)
        """,
        [str(parquet_path)],
    )


class SessionCalendarTests(unittest.TestCase):
    def test_a_missing_calendar_does_not_fall_back_to_a_sibling_csv(self) -> None:
        """Only the resolved file is read. A neighbour nobody published is not data."""
        with TemporaryDirectory() as temp_dir:
            parquet_path = Path(temp_dir) / "day_sessions.parquet"
            parquet_path.with_suffix(".csv").write_text(_CSV_SESSIONS, encoding="utf-8")

            with self.assertRaises(FileNotFoundError):
                load_session_calendar(parquet_path)

    def test_an_empty_calendar_raises_instead_of_reading_a_sibling_csv(self) -> None:
        try:
            import duckdb  # noqa: F401
        except ModuleNotFoundError:
            self.skipTest("duckdb is required for parquet session-calendar tests")

        with TemporaryDirectory() as temp_dir:
            parquet_path = Path(temp_dir) / "day_sessions.parquet"
            _write_empty_parquet_calendar(parquet_path)
            parquet_path.with_suffix(".csv").write_text(_CSV_SESSIONS, encoding="utf-8")

            with self.assertRaises(ValueError) as caught:
                load_session_calendar(parquet_path)

        self.assertIn("no sessions", str(caught.exception))

    def test_find_next_session_open_uses_next_stockholm_session(self) -> None:
        with TemporaryDirectory() as temp_dir:
            csv_path = Path(temp_dir) / "day_sessions.csv"
            csv_path.write_text(_CSV_SESSIONS, encoding="utf-8")

            resolution = find_next_session_open(
                datetime.fromisoformat("2026-04-10T17:30:00+02:00"),
                session_calendar_path=csv_path,
            )

        self.assertIsNotNone(resolution)
        assert resolution is not None
        self.assertEqual(resolution.open_at.isoformat(), "2026-04-13T09:00:00+02:00")
        self.assertEqual(resolution.close_at.isoformat(), "2026-04-13T17:30:00+02:00")
        self.assertEqual(resolution.session_kind, "regular")

    def test_find_session_for_date_matches_parquet_date_columns(self) -> None:
        try:
            import duckdb
        except ModuleNotFoundError:
            self.skipTest("duckdb is required for parquet session-calendar tests")

        with TemporaryDirectory() as temp_dir:
            parquet_path = Path(temp_dir) / "day_sessions.parquet"
            duckdb.execute(
                """
                COPY (
                    SELECT
                        DATE '2026-04-30' AS session_date,
                        'Europe/Stockholm' AS timezone,
                        '09:00' AS open_time,
                        '13:00' AS close_time,
                        'override' AS session_kind
                )
                TO ? (FORMAT PARQUET)
                """,
                [str(parquet_path)],
            )

            resolution = find_session_for_date(
                date(2026, 4, 30),
                session_calendar_path=parquet_path,
            )

        self.assertIsNotNone(resolution)
        assert resolution is not None
        self.assertEqual(resolution.close_at.isoformat(), "2026-04-30T13:00:00+02:00")


if __name__ == "__main__":
    unittest.main()
