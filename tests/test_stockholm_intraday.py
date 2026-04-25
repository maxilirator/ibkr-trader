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
                    sleep_seconds=0.0,
                ),
                instruments_path=Path("/tmp/all.txt"),
                identity_path=Path("/tmp/identity.parquet"),
                timeout=5,
                app=object(),
            )

        self.assertEqual([entry["slug"] for entry in payload["entries"]], ["gamma"])
        self.assertIsNone(payload["universe"]["next_cursor"])
