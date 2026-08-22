from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.ibkr.historical_bars import CachedContractLookupError
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
            negative_contract_cache_ttl_seconds: float = 0.0,
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
            negative_contract_cache_ttl_seconds: float = 0.0,
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
            negative_contract_cache_ttl_seconds: float = 0.0,
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

    def _run_explicit_symbols_page(
        self,
        *,
        symbols: tuple[str, ...],
        max_symbols: int,
        start_after: str | None,
    ) -> dict[str, object]:
        return collect_stockholm_intraday_backfill(
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
                max_symbols=max_symbols,
                start_after=start_after,
                symbols=symbols,
                max_runtime_seconds=None,
                sleep_seconds=0.0,
            ),
            instruments_path=Path("/tmp/all.txt"),
            identity_path=Path("/tmp/identity.parquet"),
            timeout=5,
            app=object(),
        )

    def test_explicit_symbols_longer_than_max_symbols_pages_to_completion(self) -> None:
        requested = ("delta", "alpha", "echo", "bravo", "charlie")

        with patch(
            "ibkr_trader.ibkr.stockholm_intraday._load_current_stockholm_universe",
            return_value=[],
        ), patch(
            "ibkr_trader.ibkr.stockholm_intraday._load_stockholm_identity_map",
            return_value={},
        ), patch(
            "ibkr_trader.ibkr.stockholm_intraday.read_historical_bars",
            return_value={
                "resolved_contract": {"symbol": "X", "local_symbol": "X", "sec_ids": {}},
                "bar_count": 1,
                "currency": "SEK",
                "bars": [{"timestamp": "20260424 09:00:00 MET", "close": "10.0"}],
            },
        ):
            seen_slugs: list[str] = []
            cursor: str | None = None
            seen_cursors: list[str | None] = []
            for _ in range(len(requested) + 1):
                payload = self._run_explicit_symbols_page(
                    symbols=requested,
                    max_symbols=2,
                    start_after=cursor,
                )
                page_slugs = [entry["slug"] for entry in payload["entries"]]
                self.assertTrue(page_slugs, "expected a non-empty page while cursor advances")
                seen_slugs.extend(page_slugs)
                next_cursor = payload["universe"]["next_cursor"]
                if next_cursor is None:
                    break
                self.assertNotIn(next_cursor, seen_cursors, "cursor must not repeat")
                seen_cursors.append(next_cursor)
                cursor = next_cursor
            else:
                self.fail("paging did not terminate within the expected number of pages")

            self.assertIsNone(payload["universe"]["next_cursor"])
            self.assertEqual(sorted(seen_slugs), sorted(requested))
            self.assertEqual(len(seen_slugs), len(set(seen_slugs)))

    def test_explicit_symbols_shorter_than_max_symbols_returns_single_page(self) -> None:
        with patch(
            "ibkr_trader.ibkr.stockholm_intraday._load_current_stockholm_universe",
            return_value=[],
        ), patch(
            "ibkr_trader.ibkr.stockholm_intraday._load_stockholm_identity_map",
            return_value={},
        ), patch(
            "ibkr_trader.ibkr.stockholm_intraday.read_historical_bars",
            return_value={
                "resolved_contract": {"symbol": "X", "local_symbol": "X", "sec_ids": {}},
                "bar_count": 1,
                "currency": "SEK",
                "bars": [{"timestamp": "20260424 09:00:00 MET", "close": "10.0"}],
            },
        ):
            payload = self._run_explicit_symbols_page(
                symbols=("beta", "alpha"),
                max_symbols=5,
                start_after=None,
            )

        self.assertEqual(
            sorted(entry["slug"] for entry in payload["entries"]),
            ["alpha", "beta"],
        )
        self.assertIsNone(payload["universe"]["next_cursor"])

    def test_explicit_symbols_not_in_instrument_file_are_reported_without_a_broker_call(
        self,
    ) -> None:
        # "gamma" is requested but absent from both the universe list and the
        # identity map. It must be reported as unresolvable without ever
        # reaching IBKR - an unresolvable explicit symbol used to fall back to
        # a fabricated ticker (its own slug, upper-cased) and burn a full
        # broker timeout on every page that contained it.
        calls: list[str] = []

        def fake_read_historical_bars(
            config: object,
            query: object,
            *,
            timeout: int = 20,
            app: object | None = None,
            contract_details_cache: object | None = None,
            negative_contract_cache_ttl_seconds: float = 0.0,
        ) -> dict[str, object]:
            calls.append(query.symbol)
            return {
                "resolved_contract": {
                    "symbol": query.symbol,
                    "local_symbol": query.symbol,
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
            payload = self._run_explicit_symbols_page(
                symbols=("sive", "gamma"),
                max_symbols=2,
                start_after=None,
            )

        self.assertEqual(
            sorted(entry["slug"] for entry in payload["entries"]),
            ["gamma", "sive"],
        )
        entries_by_slug = {entry["slug"]: entry for entry in payload["entries"]}
        gamma_entry = entries_by_slug["gamma"]
        self.assertIsNone(gamma_entry["company_name"])
        self.assertIsNone(gamma_entry["ticker_alias"])
        self.assertEqual(gamma_entry["status"], "unknown_instrument")
        self.assertEqual(entries_by_slug["sive"]["status"], "ok")
        self.assertEqual(calls, ["SIVE"])
        self.assertEqual(payload["summary"]["unknown_instrument_count"], 1)
        self.assertIsNone(payload["universe"]["next_cursor"])

    def test_reference_data_is_loaded_once_across_multiple_requests(self) -> None:
        universe_calls: list[Path] = []
        identity_calls: list[Path] = []

        def counting_load_universe(path: Path) -> list[str]:
            universe_calls.append(path)
            return ["sive"]

        def counting_load_identity(path: Path) -> dict[str, StockholmInstrumentIdentity]:
            identity_calls.append(path)
            return {
                "sive": StockholmInstrumentIdentity(
                    slug="sive",
                    company_name="Sivers Semiconductors",
                    share_class=None,
                    isin="SE0003917798",
                    ticker_alias="SIVE",
                    yahoo_symbol="SIVE.ST",
                )
            }

        with TemporaryDirectory() as tmp_dir:
            instruments_path = Path(tmp_dir) / "universe.parquet"
            identity_path = Path(tmp_dir) / "identity.parquet"
            instruments_path.write_bytes(b"universe")
            identity_path.write_bytes(b"identity")

            with patch(
                "ibkr_trader.ibkr.stockholm_intraday._load_current_stockholm_universe",
                side_effect=counting_load_universe,
            ), patch(
                "ibkr_trader.ibkr.stockholm_intraday._load_stockholm_identity_map",
                side_effect=counting_load_identity,
            ), patch(
                "ibkr_trader.ibkr.stockholm_intraday.read_historical_bars",
                return_value={
                    "resolved_contract": {"symbol": "SIVE", "local_symbol": "SIVE", "sec_ids": {}},
                    "bar_count": 1,
                    "currency": "SEK",
                    "bars": [{"timestamp": "20260424 09:00:00 MET", "close": "10.0"}],
                },
            ):
                for _ in range(3):
                    collect_stockholm_intraday_backfill(
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
                            max_symbols=1,
                            max_runtime_seconds=None,
                            sleep_seconds=0.0,
                        ),
                        instruments_path=instruments_path,
                        identity_path=identity_path,
                        timeout=5,
                        app=object(),
                    )

        self.assertEqual(len(universe_calls), 1)
        self.assertEqual(len(identity_calls), 1)

    def test_explicit_symbols_budget_truncates_page_with_resumable_cursor(self) -> None:
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
            negative_contract_cache_ttl_seconds: float = 0.0,
        ) -> dict[str, object]:
            nonlocal monotonic_now
            monotonic_now += 2.0
            return {
                "resolved_contract": {"symbol": query.symbol, "local_symbol": query.symbol, "sec_ids": {}},
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
                    start_after=None,
                    symbols=("delta", "gamma", "beta", "alpha"),
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
        self.assertLessEqual(payload["summary"]["elapsed_seconds"], 4.0)

    def test_cached_negative_lookup_still_appears_in_entries_and_summary(self) -> None:
        # A cached-negative contract lookup (the read_historical_bars negative
        # cache short-circuiting a broker call) must be visible the same way a
        # fresh lookup failure is - reported in `entries` with status
        # "lookup_error" - but distinguishable via a dedicated summary counter
        # and the entry's `cache_hit` flag, so an operator can tell "IBKR said
        # no, again" apart from "IBKR said no, right now".
        identities = {
            "abera": StockholmInstrumentIdentity(
                slug="abera",
                company_name="Abera Bioscience",
                share_class=None,
                isin="SE0015245097",
                ticker_alias="ABERA",
                yahoo_symbol="ABERA.ST",
            ),
            "abig": StockholmInstrumentIdentity(
                slug="abig",
                company_name="Abigroup",
                share_class=None,
                isin="SE0015245098",
                ticker_alias="ABIG",
                yahoo_symbol="ABIG.ST",
            ),
        }

        def fake_read_historical_bars(
            config: object,
            query: object,
            *,
            timeout: int = 20,
            app: object | None = None,
            contract_details_cache: object | None = None,
            negative_contract_cache_ttl_seconds: float = 0.0,
        ) -> dict[str, object]:
            if query.symbol == "ABERA":
                raise CachedContractLookupError(
                    "IBKR rejected the contract lookup for ABERA: [200] No "
                    "security definition has been found for the request "
                    "(cached, no broker call made)"
                )
            if query.symbol == "ABIG":
                raise LookupError(
                    "IBKR rejected the contract lookup for ABIG: [200] No "
                    "security definition has been found for the request"
                )
            raise AssertionError(f"unexpected symbol {query.symbol}")

        query = StockholmIntradayBackfillQuery(
            as_of_date=date(2026, 4, 24),
            what_to_show=("TRADES",),
            max_symbols=2,
            include_remapped=False,
            sleep_seconds=0.0,
        )

        with patch(
            "ibkr_trader.ibkr.stockholm_intraday._load_current_stockholm_universe",
            return_value=["abera", "abig"],
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

        entries_by_slug = {entry["slug"]: entry for entry in payload["entries"]}
        self.assertEqual(entries_by_slug["abera"]["status"], "lookup_error")
        self.assertTrue(entries_by_slug["abera"]["cache_hit"])
        self.assertEqual(entries_by_slug["abig"]["status"], "lookup_error")
        self.assertFalse(entries_by_slug["abig"]["cache_hit"])

        self.assertEqual(payload["summary"]["lookup_error_count"], 2)
        self.assertEqual(payload["summary"]["cached_lookup_error_count"], 1)
