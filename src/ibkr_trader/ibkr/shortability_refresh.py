from __future__ import annotations

import argparse
import json
from datetime import date

from ibkr_trader.config import AppConfig
from ibkr_trader.ibkr.shortability import (
    ShortabilityMarketDataType,
    ShortabilitySource,
    ShortabilitySnapshotQuery,
    collect_shortability_snapshot,
    persist_shortability_snapshot,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh the Stockholm shortability universe from IBKR and persist "
            "the resulting symbol lists and JSON snapshot into q-data."
        )
    )
    parser.add_argument(
        "--as-of-date",
        type=str,
        default=None,
        help="Listing-universe date to scan in YYYY-MM-DD format. Defaults to the latest available date.",
    )
    parser.add_argument(
        "--source",
        type=str,
        default=ShortabilitySource.OFFICIAL_IBKR_PAGE.value,
        choices=[item.value for item in ShortabilitySource],
        help="Shortability source to use.",
    )
    parser.add_argument(
        "--market-data-type",
        type=str,
        default=ShortabilityMarketDataType.LIVE.value,
        choices=[item.value for item in ShortabilityMarketDataType],
        help="IBKR market-data mode to request when using BROKER_TICKS.",
    )
    parser.add_argument(
        "--per-symbol-timeout-seconds",
        type=float,
        default=2.0,
        help="How long to wait for each symbol's shortability response.",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=25,
        help="Maximum concurrent market-data requests.",
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=None,
        help="Optional cap for testing smaller slices instead of the full universe.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="Overall IBKR connect/setup timeout in seconds.",
    )
    return parser


def build_query_from_args(args: argparse.Namespace) -> ShortabilitySnapshotQuery:
    as_of_date = date.fromisoformat(args.as_of_date) if args.as_of_date else None
    query = ShortabilitySnapshotQuery(
        as_of_date=as_of_date,
        source=ShortabilitySource(args.source),
        only_shortable=False,
        market_data_type=ShortabilityMarketDataType(args.market_data_type),
        per_symbol_timeout_seconds=args.per_symbol_timeout_seconds,
        max_concurrent=args.max_concurrent,
        max_symbols=args.max_symbols,
    )
    query.validate()
    return query


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    app_config = AppConfig.from_env()

    snapshot = collect_shortability_snapshot(
        app_config.ibkr.streaming_session(),
        build_query_from_args(args),
        instruments_path=app_config.stockholm_instruments_path,
        identity_path=app_config.stockholm_identity_path,
        timeout=args.timeout,
    )
    persisted = persist_shortability_snapshot(
        snapshot,
        instruments_dir=app_config.stockholm_instruments_path.parent,
        meta_dir=app_config.stockholm_identity_path.parent / "shortability",
    )
    print(
        json.dumps(
            {
                "persisted_artifacts": persisted,
                "status_counts": snapshot["status_counts"],
                "requested_symbol_count": snapshot["requested_symbol_count"],
                "evaluated_symbol_count": snapshot["evaluated_symbol_count"],
                "returned_symbol_count": snapshot["returned_symbol_count"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
