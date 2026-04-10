from __future__ import annotations

import unittest
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from ibkr_trader.orchestration.session_calendar import find_next_session_open
from ibkr_trader.orchestration.session_calendar import load_session_calendar


class SessionCalendarTests(unittest.TestCase):
    def test_load_session_calendar_falls_back_from_parquet_to_csv(self) -> None:
        with TemporaryDirectory() as temp_dir:
            parquet_path = Path(temp_dir) / "day_sessions.parquet"
            csv_path = parquet_path.with_suffix(".csv")
            csv_path.write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
                        "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
                    ]
                ),
                encoding="utf-8",
            )

            rows = load_session_calendar(parquet_path)

        self.assertEqual(len(rows), 2)
        self.assertTrue(rows[0].source_path.endswith("day_sessions.csv"))

    def test_find_next_session_open_uses_next_stockholm_session(self) -> None:
        with TemporaryDirectory() as temp_dir:
            parquet_path = Path(temp_dir) / "day_sessions.parquet"
            csv_path = parquet_path.with_suffix(".csv")
            csv_path.write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
                        "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
                    ]
                ),
                encoding="utf-8",
            )

            resolution = find_next_session_open(
                datetime.fromisoformat("2026-04-10T17:30:00+02:00"),
                session_calendar_path=parquet_path,
            )

        self.assertIsNotNone(resolution)
        assert resolution is not None
        self.assertEqual(resolution.open_at.isoformat(), "2026-04-13T09:00:00+02:00")
        self.assertEqual(resolution.close_at.isoformat(), "2026-04-13T17:30:00+02:00")
        self.assertEqual(resolution.session_kind, "regular")


if __name__ == "__main__":
    unittest.main()
