#!/usr/bin/env python3
"""Enqueue 1-minute backfill for XSTO sessions that are complete but missing.

Run nightly. Rather than assuming "yesterday", it asks which recent sessions
have no backfill request yet and enqueues those - so a night that is skipped,
or a host that was down, is picked up automatically instead of leaving a hole.

Enqueue only: no broker connection, and nothing is written to q-data. The
trader's backfill worker drains the queue on the historical client under the
pacing governor; publishing into q-data remains q-data-ops' job.
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from enqueue_xsto_minute_backfill import (  # noqa: E402
    trading_sessions,
    universe_active_on,
)

#: How far back to look for gaps. Wide enough to cover a long outage, narrow
#: enough that a nightly run stays cheap.
DEFAULT_LOOKBACK_DAYS = 10


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--reason", default="xsto-1min-nightly")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    from sqlalchemy import text

    from ibkr_trader.config import AppConfig
    from ibkr_trader.db.base import build_engine, create_session_factory, session_scope
    from ibkr_trader.ibkr.market_data_backfill import (
        enqueue_market_data_backfill_request,
    )

    config = AppConfig.from_env()
    today = datetime.now(UTC).date()
    sessions = trading_sessions(
        config.session_calendar_path, today - timedelta(days=args.lookback_days), today
    )

    # Only sessions that have actually closed. Enqueuing the current session
    # before its close would cache a partial day under a key that then looks
    # complete.
    now = datetime.now(UTC)
    sessions = [(d, c) for d, c in sessions if c <= now]
    if not sessions:
        print("no closed sessions in the lookback window")
        return 0

    engine = build_engine(config.database_url)
    factory = create_session_factory(engine)
    try:
        with session_scope(factory) as db:
            already = {
                row[0]
                for row in db.execute(
                    text(
                        "select distinct trade_date from market_data_backfill_request "
                        "where trade_date = any(:dates)"
                    ),
                    {"dates": [d.isoformat() for d, _ in sessions]},
                )
            }

        missing = [(d, c) for d, c in sessions if d.isoformat() not in already]
        if not missing:
            print(f"all {len(sessions)} closed session(s) already enqueued; nothing to do")
            return 0

        print(f"sessions needing backfill: {[d.isoformat() for d, _ in missing]}")
        total = 0
        stale_universe = False
        for day, close_at in missing:
            symbols = universe_active_on(config.stockholm_instruments_path, day)
            if not symbols:
                # The universe dataset does not yet cover this session. Enqueuing
                # nothing is correct - guessing an instrument list would be worse -
                # but it must not look like success, or the dataset silently stops
                # growing the day q-data's universe publish falls behind.
                print(
                    f"  {day}: 0 instruments active - the universe dataset does not "
                    f"cover this session yet ({config.stockholm_instruments_path.name}); "
                    "nothing enqueued"
                )
                stale_universe = True
                continue
            if args.dry_run:
                print(f"  {day}: would enqueue {len(symbols)} (dry run)")
                total += len(symbols)
                continue
            for symbol in symbols:
                enqueue_market_data_backfill_request(
                    factory,
                    symbol=symbol,
                    trade_date=day.isoformat(),
                    requested_until=close_at,
                    reason=args.reason,
                )
            total += len(symbols)
            print(f"  {day}: enqueued {len(symbols)}")
        print(f"total {total} request(s)")
        if stale_universe:
            print(
                "WARNING: at least one closed session had no active instruments. "
                "The nightly backfill cannot extend past the universe dataset, so "
                "this will keep recurring until q-data republishes it."
            )
            return 2
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
