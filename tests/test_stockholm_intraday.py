from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.ibkr.stockholm_intraday import (
    StockholmInstrumentIdentity,
    StockholmIntradayBackfillQuery,
    collect_stockholm_intraday_backfill,
)


class StockholmIntradayBackfillTests(TestCase):
    def test_collect_backfill_classifies_ok_remapped_and_lookup_error(self) -> None:
        universe = ["sive", "vuxen", "abera"]
        identities = {
            "sive": StockholmInstrumentIdentity(
                slug="sive",
                company_name="Sivers Semiconductors",
                share_class=None,
                isin="SE0003917798",
                ticker_alias="SIVE",
                yahoo_symbol="SIVE.ST",
            ),
            "vuxen": StockholmInstrumentIdentity(
                slug="vuxen",
                company_name="Vuxen Group",
                share_class=None,
                isin="SE0015661608",
                ticker_alias="VUXEN",
                yahoo_symbol="VUXEN.ST",
            ),
            "abera": StockholmInstrumentIdentity(
                slug="abera",
                company_name="Abera Bioscience",
                share_class=None,
                isin="SE0015245097",
                ticker_alias="ABERA",
                yahoo_symbol="ABERA.ST",
            ),
        }

        def fake_read_historical_bars(
            config: object,
            query: object,
            *,
            timeout: int = 20,
            app: object | None = None,
            contract_details_cache: object | None = None,
        ) -> dict[str, object]:
            if query.symbol == "ABERA":
                raise LookupError("IBKR rejected the contract lookup for ABERA")
            if query.symbol == "VUXEN":
                resolved_symbol = "PURE"
                local_symbol = "VUXEN"
            else:
                resolved_symbol = "SIVE"
                local_symbol = "SIVE"
            return {
                "resolved_contract": {
                    "symbol": resolved_symbol,
                    "local_symbol": local_symbol,
                    "sec_ids": {"ISIN": query.isin},
                },
                "bar_count": 2,
                "currency": "SEK",
                "bars": [
                    {"timestamp": "20260424 09:00:00 MET", "close": "10.0"},
                    {"timestamp": "20260424 09:01:00 MET", "close": "10.1"},
                ],
            }

        query = StockholmIntradayBackfillQuery(
            as_of_date=date(2026, 4, 24),
            what_to_show=("TRADES", "MIDPOINT"),
            max_symbols=3,
            include_remapped=False,
            sleep_seconds=0.0,
        )

        with patch(
            "ibkr_trader.ibkr.stockholm_intraday._load_current_stockholm_universe",
            return_value=universe,
        ), patch(
            "ibkr_trader.ibkr.stockholm_intraday._load_stockholm_identity_map",
            return_value=identities,
        ), patch(
            "ibkr_trader.ibkr.stockholm_intraday.read_historical_bars",
            side_effect=fake_read_historical_bars,
        ):
            payload = collect_stockholm_intraday_backfill(
                IbkrConnectionConfig(
                    host="127.0.0.1",
                    port=4002,
                    client_id=7,
                    diagnostic_client_id=7,
                    account_id="U1234567",
                ),
                query,
                instruments_path=Path("/tmp/all.txt"),
                identity_path=Path("/tmp/identity.parquet"),
                timeout=5,
                app=object(),
            )

        self.assertEqual(payload["universe"]["page_size"], 3)
        self.assertEqual(payload["summary"]["ok_count"], 1)
        self.assertEqual(payload["summary"]["skipped_remapped_count"], 1)
        self.assertEqual(payload["summary"]["lookup_error_count"], 1)

        statuses = {entry["slug"]: entry["status"] for entry in payload["entries"]}
        self.assertEqual(statuses["sive"], "ok")
        self.assertEqual(statuses["vuxen"], "skipped_remapped")
        self.assertEqual(statuses["abera"], "lookup_error")

    def test_collect_backfill_builds_cursor_page(self) -> None:
        with patch(
            "ibkr_trader.ibkr.stockholm_intraday._load_current_stockholm_universe",
            return_value=["beta", "alpha", "gamma"],
        ), patch(
            "ibkr_trader.ibkr.stockholm_intraday._load_stockholm_identity_map",
            return_value={},
        ), patch(
            "ibkr_trader.ibkr.stockholm_intraday.read_historical_bars",
            return_value={
                "resolved_contract": {
                    "symbol": "GAMMA",
                    "local_symbol": "GAMMA",
                    "sec_ids": {},
                },
                "bar_count": 1,
                "currency": "SEK",
                "bars": [{"timestamp": "20260424 09:00:00 MET", "close": "10.0"}],
            },
        ):
            payload = collect_stockholm_intraday_backfill(
                IbkrConnectionConfig(
                    host="127.0.0.1",
                    port=4002,
                    client_id=7,
                    diagnostic_client_id=7,
                    account_id="U1234567",
                ),
                StockholmIntradayBackfillQuery(
                    as_of_date=date(2026, 4, 24),
                    max_symbols=1,
                    start_after="beta",
                    max_runtime_seconds=None,
                    sleep_seconds=0.0,
                ),
                instruments_path=Path("/tmp/all.txt"),
                identity_path=Path("/tmp/identity.parquet"),
                timeout=5,
                app=object(),
            )

        self.assertEqual([entry["slug"] for entry in payload["entries"]], ["gamma"])
        self.assertIsNone(payload["universe"]["next_cursor"])

    def test_collect_backfill_marks_adjusted_last_unsupported_without_requesting_it(self) -> None:
        calls: list[str] = []

        def fake_read_historical_bars(
            config: object,
            query: object,
            *,
            timeout: int = 20,
            app: object | None = None,
            contract_details_cache: object | None = None,
        ) -> dict[str, object]:
            calls.append(query.what_to_show)
            return {
                "resolved_contract": {
                    "symbol": "SIVE",
                    "local_symbol": "SIVE",
                    "sec_ids": {"ISIN": query.isin},
                },
                "bar_count": 1,
                "currency": "SEK",
                "bars": [{"timestamp": "20260424 09:00:00 MET", "close": "10.0"}],
            }

        with patch(
            "ibkr_trader.ibkr.stockholm_intraday._load_current_stockholm_universe",
            return_value=["sive"],
        ), patch(
            "ibkr_trader.ibkr.stockholm_intraday._load_stockholm_identity_map",
            return_value={
                "sive": StockholmInstrumentIdentity(
                    slug="sive",
                    company_name="Sivers Semiconductors",
                    share_class=None,
                    isin="SE0003917798",
                    ticker_alias="SIVE",
                    yahoo_symbol="SIVE.ST",
                )
            },
        ), patch(
            "ibkr_trader.ibkr.stockholm_intraday.read_historical_bars",
            side_effect=fake_read_historical_bars,
        ):
            payload = collect_stockholm_intraday_backfill(
                IbkrConnectionConfig(
                    host="127.0.0.1",
                    port=4002,
                    client_id=7,
                    diagnostic_client_id=7,
                    account_id="U1234567",
                ),
                StockholmIntradayBackfillQuery(
                    as_of_date=date(2026, 4, 24),
                    what_to_show=("TRADES", "ADJUSTED_LAST"),
                    max_symbols=1,
                    max_runtime_seconds=None,
                    sleep_seconds=0.0,
                ),
                instruments_path=Path("/tmp/all.txt"),
                identity_path=Path("/tmp/identity.parquet"),
                timeout=5,
                app=object(),
            )

        self.assertEqual(calls, ["TRADES"])
        self.assertEqual(payload["entries"][0]["series"]["ADJUSTED_LAST"]["status"], "unsupported")
        self.assertEqual(payload["summary"]["unsupported_series_count"], 1)

    def test_collect_backfill_budget_returns_resumable_partial_page(self) -> None:
        monotonic_now = 0.0

        def fake_monotonic() -> float:
            return monotonic_now

        def fake_read_historical_bars(
            config: object,
            query: object,
            *,
            timeout: int = 20,
            app: object | None = None,
            contract_details_cache: object | None = None,
        ) -> dict[str, object]:
            nonlocal monotonic_now
            monotonic_now += 2.0
            return {
                "resolved_contract": {
                    "symbol": query.symbol,
                    "local_symbol": query.symbol,
                    "sec_ids": {},
                },
                "bar_count": 1,
                "currency": "SEK",
                "bars": [{"timestamp": "20260424 09:00:00 MET", "close": "10.0"}],
            }

        with patch(
            "ibkr_trader.ibkr.stockholm_intraday.runtime_time.monotonic",
            side_effect=fake_monotonic,
        ), patch(
            "ibkr_trader.ibkr.stockholm_intraday._load_current_stockholm_universe",
            return_value=["alpha", "beta", "gamma", "delta"],
        ), patch(
            "ibkr_trader.ibkr.stockholm_intraday._load_stockholm_identity_map",
            return_value={},
        ), patch(
            "ibkr_trader.ibkr.stockholm_intraday.read_historical_bars",
            side_effect=fake_read_historical_bars,
        ):
            payload = collect_stockholm_intraday_backfill(
                IbkrConnectionConfig(
                    host="127.0.0.1",
                    port=4002,
                    client_id=7,
                    diagnostic_client_id=7,
                    account_id="U1234567",
                ),
                StockholmIntradayBackfillQuery(
                    as_of_date=date(2026, 4, 24),
                    what_to_show=("TRADES",),
                    max_symbols=4,
                    max_runtime_seconds=3.0,
                    sleep_seconds=0.0,
                ),
                instruments_path=Path("/tmp/all.txt"),
                identity_path=Path("/tmp/identity.parquet"),
                timeout=10,
                app=object(),
            )

        self.assertEqual([entry["slug"] for entry in payload["entries"]], ["alpha", "beta"])
        self.assertEqual(payload["summary"]["processed_symbol_count"], 2)
        self.assertTrue(payload["summary"]["budget_exhausted"])
        self.assertEqual(payload["universe"]["next_cursor"], "beta")
        self.assertEqual(payload["universe"]["requested_page_next_cursor"], None)
