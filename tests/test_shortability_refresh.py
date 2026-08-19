from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from ibkr_trader.config import AppConfig
from ibkr_trader.ibkr.short_sale_validation import _stockholm_shortability_snapshot_path
from ibkr_trader.ibkr.shortability import ShortabilityMarketDataType
from ibkr_trader.ibkr.shortability import ShortabilitySource
from ibkr_trader.ibkr.shortability_refresh import build_parser
from ibkr_trader.ibkr.shortability_refresh import build_query_from_args
from ibkr_trader.ibkr.shortability_refresh import main

_SNAPSHOT = {
    "snapshot_at": "2026-04-15T23:38:05.631154+00:00",
    "universe_as_of_date": "2026-04-14",
    "entries": [{"symbol": "VOLV-B", "status": "shortable"}],
    "evaluated_entries": [{"symbol": "VOLV-B", "status": "shortable"}],
    "status_counts": {"shortable": 1},
    "requested_symbol_count": 1,
    "evaluated_symbol_count": 1,
    "returned_symbol_count": 1,
}


class ShortabilityRefreshTests(TestCase):
    def test_build_query_from_args_uses_full_universe_persistence_defaults(self) -> None:
        args = build_parser().parse_args([])

        query = build_query_from_args(args)

        self.assertIsNone(query.symbols)
        self.assertIsNone(query.as_of_date)
        self.assertEqual(query.source, ShortabilitySource.OFFICIAL_IBKR_PAGE)
        self.assertFalse(query.only_shortable)
        self.assertEqual(query.market_data_type, ShortabilityMarketDataType.LIVE)
        self.assertEqual(query.per_symbol_timeout_seconds, 2.0)
        self.assertEqual(query.max_concurrent, 25)
        self.assertIsNone(query.max_symbols)

    def test_build_query_from_args_accepts_explicit_overrides(self) -> None:
        args = build_parser().parse_args(
            [
                "--as-of-date",
                "2026-04-14",
                "--source",
                "BROKER_TICKS",
                "--market-data-type",
                "DELAYED",
                "--per-symbol-timeout-seconds",
                "3.5",
                "--max-concurrent",
                "11",
                "--max-symbols",
                "50",
            ]
        )

        query = build_query_from_args(args)

        self.assertEqual(query.as_of_date, date(2026, 4, 14))
        self.assertEqual(query.source, ShortabilitySource.BROKER_TICKS)
        self.assertEqual(query.market_data_type, ShortabilityMarketDataType.DELAYED)
        self.assertEqual(query.per_symbol_timeout_seconds, 3.5)
        self.assertEqual(query.max_concurrent, 11)
        self.assertEqual(query.max_symbols, 50)

    def test_refresh_writes_under_the_output_root_and_not_into_the_shared_dataset(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            output_root = Path(temp_dir) / "var"
            with patch.dict(
                os.environ, {"IBKR_TRADER_OUTPUT_ROOT": str(output_root)}
            ), patch(
                "ibkr_trader.ibkr.shortability_refresh.collect_shortability_snapshot",
                return_value=dict(_SNAPSHOT),
            ):
                app_config = AppConfig.from_env()
                exit_code = main([])

            self.assertEqual(exit_code, 0)
            self.assertTrue(
                (output_root / "shortability" / "shortability_latest.json").is_file()
            )
            self.assertTrue((output_root / "shortable.txt").is_file())
            # The catalog-resolved dataset directory belongs to q-data.
            self.assertFalse(
                (app_config.stockholm_identity_path.parent / "shortability").exists()
            )
            self.assertFalse(
                (app_config.stockholm_instruments_path.parent / "shortable.txt").exists()
            )

    def test_short_sale_validation_reads_the_snapshot_the_refresh_writes(self) -> None:
        """A refresh has to update the file that blocks short orders, not a second copy."""
        with TemporaryDirectory() as temp_dir:
            output_root = Path(temp_dir) / "var"
            with patch.dict(
                os.environ, {"IBKR_TRADER_OUTPUT_ROOT": str(output_root)}
            ), patch(
                "ibkr_trader.ibkr.shortability_refresh.collect_shortability_snapshot",
                return_value=dict(_SNAPSHOT),
            ):
                main([])
                reader_path = _stockholm_shortability_snapshot_path()

            self.assertEqual(
                reader_path,
                output_root / "shortability" / "shortability_latest.json",
            )
            self.assertIn("VOLV-B", reader_path.read_text(encoding="utf-8"))
