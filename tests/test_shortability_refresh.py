from __future__ import annotations

from datetime import date
from unittest import TestCase

from ibkr_trader.ibkr.shortability import ShortabilityMarketDataType
from ibkr_trader.ibkr.shortability import ShortabilitySource
from ibkr_trader.ibkr.shortability_refresh import build_parser
from ibkr_trader.ibkr.shortability_refresh import build_query_from_args


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
