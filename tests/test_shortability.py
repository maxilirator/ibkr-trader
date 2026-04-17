from __future__ import annotations

from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

import pandas as pd

from ibkr_trader.ibkr.shortability import (
    OfficialIbkrShortableRow,
    StockholmInstrumentIdentity,
    ShortabilityEntry,
    ShortabilitySource,
    ShortabilityStatus,
    ShortabilitySnapshotQuery,
    _PendingShortabilityRequest,
    _build_shortability_snapshot_from_official_rows,
    _coerce_decimal,
    _build_contract_attempt_queries,
    _finalize_request,
    interpret_shortability_status,
    load_stockholm_identity_map,
    load_stockholm_symbols_from_instruments_file,
    parse_official_ibkr_shortable_rows,
    persist_shortability_snapshot,
)


class ShortabilityTests(TestCase):
    def test_parse_official_ibkr_shortable_rows_extracts_stockholm_symbols(self) -> None:
        last_updated_text, rows = parse_official_ibkr_shortable_rows(
            """
            <h4>Shortable Stocks for Sweden </h4>
            <p><em>Last updated: Wed, 15.Apr.26 19:30EDT</em></p>
            <table class='table'>
              <tbody>
                <tr>
                  <td class='text-center'><a href="javascript:NewWindow('http://www1.interactivebrokers.ch/contract_info/index.php?site=IB&action=Details&conid=123','VOLV.B','600','600','custom','front');">VOLV.B</a></td>
                  <td class='text-center'>SEK</td>
                  <td>VOLVO AB-B SHS</td>
                  <td class='text-center'><a href='/sso/Login?RL=1&action=CS_SLB'> Log In to Check Availability </a></td>
                </tr>
                <tr>
                  <td class='text-center'><a href="javascript:NewWindow('http://www1.interactivebrokers.ch/contract_info/index.php?site=IB&action=Details&conid=456','SIVE','600','600','custom','front');">SIVE</a></td>
                  <td class='text-center'>SEK</td>
                  <td>SIVERS SEMICONDUCTORS AB</td>
                  <td class='text-center'><a href='/sso/Login?RL=1&action=CS_SLB'> Log In to Check Availability </a></td>
                </tr>
              </tbody>
            </table>
            """
        )

        self.assertEqual(last_updated_text, "Wed, 15.Apr.26 19:30EDT")
        self.assertEqual([row.normalized_symbol for row in rows], ["VOLV-B", "SIVE"])
        self.assertEqual(rows[0].broker_conid, "123")
        self.assertEqual(rows[1].long_name, "SIVERS SEMICONDUCTORS AB")

    def test_build_shortability_snapshot_from_official_rows_marks_rest_of_universe_not_shortable(self) -> None:
        query = ShortabilitySnapshotQuery(
            source=ShortabilitySource.OFFICIAL_IBKR_PAGE,
            only_shortable=False,
        )
        snapshot = _build_shortability_snapshot_from_official_rows(
            query,
            all_symbols=("SIVE", "VOLV-B", "ABB"),
            universe_source="/tmp/all.txt",
            universe_as_of_date=date(2026, 4, 15),
            shortable_rows=(
                OfficialIbkrShortableRow(
                    symbol="SIVE",
                    normalized_symbol="SIVE",
                    currency="SEK",
                    long_name="SIVERS SEMICONDUCTORS AB",
                    broker_conid="456",
                    details_url="http://example.com/sive",
                ),
                OfficialIbkrShortableRow(
                    symbol="VOLV.B",
                    normalized_symbol="VOLV-B",
                    currency="SEK",
                    long_name="VOLVO AB-B SHS",
                    broker_conid="123",
                    details_url="http://example.com/volv",
                ),
            ),
            source_updated_text="Wed, 15.Apr.26 19:30EDT",
        )

        self.assertEqual(snapshot["source"], "OFFICIAL_IBKR_PAGE")
        self.assertEqual(snapshot["source_updated_text"], "Wed, 15.Apr.26 19:30EDT")
        self.assertEqual(snapshot["status_counts"], {"shortable": 2, "not_shortable": 1})
        self.assertEqual(
            [(entry["symbol"], entry["status"]) for entry in snapshot["entries"]],
            [
                ("SIVE", "shortable"),
                ("VOLV-B", "shortable"),
                ("ABB", "not_shortable"),
            ],
        )
        self.assertEqual(snapshot["entries"][0]["broker_conid"], "456")

    def test_coerce_decimal_parses_numeric_values(self) -> None:
        self.assertEqual(str(_coerce_decimal(3.0)), "3.0")
        self.assertEqual(str(_coerce_decimal("2.5")), "2.5")
        self.assertIsNone(_coerce_decimal(None))

    def test_interpret_shortability_status_from_indicator(self) -> None:
        self.assertEqual(
            interpret_shortability_status(shortable_value=3),
            ShortabilityStatus.SHORTABLE,
        )
        self.assertEqual(
            interpret_shortability_status(shortable_value=2),
            ShortabilityStatus.LOCATE_REQUIRED,
        )
        self.assertEqual(
            interpret_shortability_status(shortable_value=1),
            ShortabilityStatus.NOT_SHORTABLE,
        )

    def test_interpret_shortability_status_can_fallback_to_shares(self) -> None:
        self.assertEqual(
            interpret_shortability_status(shortable_value=None, shortable_shares=100),
            ShortabilityStatus.SHORTABLE,
        )
        self.assertEqual(
            interpret_shortability_status(shortable_value=None, shortable_shares=0),
            ShortabilityStatus.NOT_SHORTABLE,
        )
        self.assertEqual(
            interpret_shortability_status(shortable_value=None, shortable_shares=None),
            ShortabilityStatus.UNKNOWN_STATUS,
        )

    def test_load_stockholm_symbols_from_instruments_file_filters_by_as_of_date(self) -> None:
        with TemporaryDirectory() as temp_dir:
            instruments_path = Path(temp_dir) / "all.txt"
            instruments_path.write_text(
                "\n".join(
                    [
                        "sive\t2020-01-01\t2026-04-14",
                        "abb\t2006-01-10\t2026-04-14",
                        "gone\t2010-01-01\t2020-01-01",
                    ]
                ),
                encoding="utf-8",
            )

            symbols, effective_date = load_stockholm_symbols_from_instruments_file(
                instruments_path,
                as_of_date=date(2026, 4, 14),
            )

        self.assertEqual(symbols, ("ABB", "SIVE"))
        self.assertEqual(effective_date, date(2026, 4, 14))

    def test_load_stockholm_symbols_from_instruments_file_uses_latest_available_date(self) -> None:
        with TemporaryDirectory() as temp_dir:
            instruments_path = Path(temp_dir) / "all.txt"
            instruments_path.write_text(
                "\n".join(
                    [
                        "sive\t2020-01-01\t2026-04-14",
                        "abb\t2006-01-10\t2026-04-14",
                        "future\t2026-04-15\t2026-04-20",
                    ]
                ),
                encoding="utf-8",
            )

            symbols, effective_date = load_stockholm_symbols_from_instruments_file(
                instruments_path,
                today=date(2026, 4, 16),
            )

        self.assertEqual(symbols, ("ABB", "SIVE"))
        self.assertEqual(effective_date, date(2026, 4, 14))

    def test_load_stockholm_symbols_from_instruments_file_rejects_missing_path(self) -> None:
        with self.assertRaisesRegex(FileNotFoundError, "not found"):
            load_stockholm_symbols_from_instruments_file(
                Path("/tmp/does-not-exist-shortability")
            )

    def test_build_contract_attempt_queries_adds_share_class_fallbacks(self) -> None:
        query = ShortabilitySnapshotQuery()

        attempts = _build_contract_attempt_queries(query, "VOLV-B")

        self.assertEqual(
            [(attempt.symbol, attempt.local_symbol) for attempt in attempts],
            [
                ("VOLV-B", None),
                ("VOLV", "VOLV B"),
                ("VOLV B", None),
                ("VOLVB", None),
            ],
        )

    def test_build_contract_attempt_queries_prefers_identity_isin(self) -> None:
        query = ShortabilitySnapshotQuery()
        identity = StockholmInstrumentIdentity(
            symbol="VOLV-B",
            isin="SE0000115446",
            ticker_alias="VOLV B",
            yahoo_symbol="VOLV-B.ST",
        )

        attempts = _build_contract_attempt_queries(query, "VOLV-B", identity=identity)

        self.assertIn(
            ("VOLV-B", None, "SE0000115446"),
            [(attempt.symbol, attempt.local_symbol, attempt.isin) for attempt in attempts],
        )
        self.assertIn(
            ("VOLV B", "VOLV B", "SE0000115446"),
            [(attempt.symbol, attempt.local_symbol, attempt.isin) for attempt in attempts],
        )

    def test_load_stockholm_identity_map_reads_parquet(self) -> None:
        with TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "instrument_identity.parquet"
            pd.DataFrame(
                [
                    {
                        "instrument": "volv-b",
                        "isin": "SE0000115446",
                        "ticker_alias": "VOLV B",
                        "yahoo_symbol": "VOLV-B.ST",
                    },
                    {
                        "instrument": "sive",
                        "isin": "SE0003917798",
                        "ticker_alias": "SIVE",
                        "yahoo_symbol": "SIVE.ST",
                    },
                ]
            ).to_parquet(identity_path, index=False)

            identity_map = load_stockholm_identity_map(
                identity_path,
                symbols=("VOLV-B",),
            )

        self.assertEqual(set(identity_map.keys()), {"VOLV-B"})
        self.assertEqual(identity_map["VOLV-B"].isin, "SE0000115446")
        self.assertEqual(identity_map["VOLV-B"].ticker_alias, "VOLV B")

    def test_finalize_request_maps_not_found(self) -> None:
        entry = _finalize_request(
            _PendingShortabilityRequest(
                req_id=1,
                symbol="MISSING",
                exchange="SMART",
                primary_exchange="SFB",
                currency="SEK",
                security_type="STK",
                started_at=0.0,
                errors=[
                    {
                        "error_code": 200,
                        "error_string": "No security definition has been found for the request",
                    }
                ],
                completed_reason="error",
            )
        )

        self.assertEqual(entry.status, ShortabilityStatus.NOT_FOUND)

    def test_finalize_request_maps_timeout(self) -> None:
        entry = _finalize_request(
            _PendingShortabilityRequest(
                req_id=1,
                symbol="SLOW",
                exchange="SMART",
                primary_exchange="SFB",
                currency="SEK",
                security_type="STK",
                started_at=0.0,
                errors=[],
                completed_reason=None,
            )
        )

        self.assertEqual(entry.status, ShortabilityStatus.TIMEOUT)

    def test_finalize_request_maps_unknown_status_when_data_is_unusable(self) -> None:
        entry = _finalize_request(
            _PendingShortabilityRequest(
                req_id=1,
                symbol="WEIRD",
                exchange="SMART",
                primary_exchange="SFB",
                currency="SEK",
                security_type="STK",
                started_at=0.0,
                errors=[],
                completed_reason="shortable_value",
            )
        )

        self.assertEqual(entry.status, ShortabilityStatus.UNKNOWN_STATUS)

    def test_persist_shortability_snapshot_writes_expected_files(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            result = persist_shortability_snapshot(
                {
                    "snapshot_at": "2026-04-15T23:38:05.631154+00:00",
                    "universe_as_of_date": "2026-04-14",
                    "entries": [
                        {"symbol": "VOLV-B", "status": "shortable"},
                    ],
                    "evaluated_entries": [
                        {"symbol": "VOLV-B", "status": "shortable"},
                        {"symbol": "ERIC-B", "status": "locate_required"},
                        {"symbol": "MISSING", "status": "not_found"},
                    ],
                },
                instruments_dir=root / "instruments",
                meta_dir=root / "meta" / "shortability",
            )

            self.assertEqual(result["as_of_date"], "2026-04-14")
            self.assertEqual(result["shortable_count"], 1)
            self.assertEqual(result["shortable_or_locate_count"], 2)
            self.assertEqual(
                (root / "instruments" / "shortable.txt").read_text(encoding="utf-8"),
                "volv-b\n",
            )
            self.assertEqual(
                (root / "instruments" / "shortable_or_locate.txt").read_text(
                    encoding="utf-8"
                ),
                "eric-b\nvolv-b\n",
            )
            snapshot_text = (root / "meta" / "shortability" / "shortability_snapshot_2026-04-14.json").read_text(
                encoding="utf-8"
            )
            self.assertIn('"snapshot_at": "2026-04-15T23:38:05.631154+00:00"', snapshot_text)
            self.assertIn('"evaluated_entries"', snapshot_text)
