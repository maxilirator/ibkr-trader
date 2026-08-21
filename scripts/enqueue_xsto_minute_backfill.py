#!/usr/bin/env python3
"""Enqueue whole-market XSTO 1-minute backfill requests, one day at a time.

The universe is a living list, not a fixed set. ``xsto.world.universe`` carries
``instrument``, ``start`` and ``end`` per row, so the instruments that existed on
a given date are a property of that date. Taking the current universe and
crossing it with a date range would request names before they listed and miss
names that have since gone - and would spend the pacing budget discovering that
one request at a time.

So requests are generated per trading day from the universe active *on that day*.

This enqueues only. The trader's backfill worker performs the IBKR calls on the
historical client id (8), under the shared pacing governor, and persists the
bars. Publishing into q-data is q-data-ops' job: the trader mounts q-data
read-only and must not write there.

Read-only with respect to IBKR: it opens no broker connection.

Usage::

    python scripts/enqueue_xsto_minute_backfill.py --start 2026-08-04 --end 2026-08-08
    python scripts/enqueue_xsto_minute_backfill.py --start 2026-08-04 --days 5 --dry-run
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

#: One request per (symbol, month) instead of per (symbol, day).
#:
#: Measured against the live Gateway on AZN@XSTO, 1-minute bars:
#:
#:     1 D    510 bars    0s        1 M   11,220 bars    1s
#:     1 W  2,550 bars    0s        2 M   21,927 bars   51s
#:     2 W  5,100 bars    1s        3 M   32,127 bars   81s
#:                                  6 M   timed out at 181s
#:
#: Bar counts are exactly linear at 510 per session, so nothing is truncated.
#: The knee is at one month: 22x the data for the same second. Past it, response
#: time explodes. Twelve months of the whole market is ~11,400 requests monthly
#: against ~238,500 daily - the difference between under two days and a month.
#:
#: `build_backfill_request_key` already hashes `duration`, so monthly and daily
#: requests for the same symbol never collide.
MONTHLY_DURATION = "1 M"

#: Bound on a single enqueue run. A whole-market year is ~235k requests; at the
#: pacing ceiling of 300/hour that is weeks of work, and materialising it all as
#: PENDING rows hides how little of it is reachable in any one window.
DEFAULT_MAX_REQUESTS = 20_000


def trading_sessions(
    calendar_path: Path, start: date, end: date
) -> list[tuple[date, datetime]]:
    """Real XSTO sessions with their true close instant, in UTC.

    Uses the calendar's own ``close_time`` and ``timezone`` per row rather than
    a constant. XSTO has half-days, and 17:30 Europe/Stockholm is 15:30 UTC in
    summer and 16:30 in winter - stamping local time as UTC would bound every
    request two hours past the close for half the year.
    """
    import pandas as pd

    frame = pd.read_parquet(calendar_path)
    for required in ("session_date", "close_time", "timezone"):
        if required not in frame.columns:
            raise SystemExit(
                f"calendar {calendar_path} lacks {required!r}; found {list(frame.columns)}"
            )

    sessions: list[tuple[date, datetime]] = []
    for row in frame.itertuples(index=False):
        day = pd.Timestamp(row.session_date).date()
        if not (start <= day <= end):
            continue
        close = time.fromisoformat(str(row.close_time))
        local = pd.Timestamp(datetime.combine(day, close)).tz_localize(str(row.timezone))
        sessions.append((day, local.tz_convert("UTC").to_pydatetime()))
    return sorted(sessions, key=lambda item: item[0])


def month_ends(sessions: list[tuple[date, datetime]]) -> list[tuple[date, datetime]]:
    """Collapse sessions to the last session of each calendar month.

    A ``1 M`` request is anchored at its *end*, so the anchor must be a real
    session close - IBKR walks back from it. Using a calendar month-end that is
    not a trading day would shift the whole window.
    """
    last: dict[tuple[int, int], tuple[date, datetime]] = {}
    for day, close_at in sessions:
        last[(day.year, day.month)] = (day, close_at)
    return [last[key] for key in sorted(last)]


def universe_active_on(universe_path: Path, day: date) -> list[str]:
    """Instruments listed on ``day``, from the universe's own active windows."""
    import pandas as pd

    frame = pd.read_parquet(universe_path)
    stamp = pd.Timestamp(day)
    start = pd.to_datetime(frame["start"])
    end = pd.to_datetime(frame["end"])
    active = frame[(start <= stamp) & (end >= stamp)]
    return sorted(str(s) for s in active["instrument"].tolist())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", required=True, help="first trade date, YYYY-MM-DD")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--end", help="last trade date, YYYY-MM-DD")
    group.add_argument("--days", type=int, help="number of trading days from --start")
    parser.add_argument(
        "--granularity",
        choices=("day", "month"),
        default="day",
        help="one request per trading day, or per month (~22x fewer requests)",
    )
    parser.add_argument("--max-requests", type=int, default=DEFAULT_MAX_REQUESTS)
    parser.add_argument("--reason", default="xsto-1min-backfill")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    from ibkr_trader.config import AppConfig
    from ibkr_trader.db.base import build_engine, create_session_factory
    from ibkr_trader.ibkr.market_data_backfill import (
        enqueue_market_data_backfill_request,
    )

    config = AppConfig.from_env()
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else date.today()

    sessions = trading_sessions(config.session_calendar_path, start, end)
    if args.days:
        sessions = sessions[: args.days]
    if not sessions:
        print("no trading days in range")
        return 1
    if args.granularity == "month":
        sessions = month_ends(sessions)
    days = [d for d, _ in sessions]

    print(f"calendar : {config.session_calendar_path}")
    print(f"universe : {config.stockholm_instruments_path}")
    unit = "month-end sessions" if args.granularity == "month" else "trading days"
    print(f"{unit:9}: {len(days)}  ({days[0]} .. {days[-1]})")
    if args.granularity == "month":
        print(f"duration : {MONTHLY_DURATION} per request (~22x fewer than daily)")

    engine = build_engine(config.database_url)
    session_factory = create_session_factory(engine)

    enqueued = existing = 0
    try:
        for day, close_at in sessions:
            symbols = universe_active_on(config.stockholm_instruments_path, day)
            print(
                f"  {day}: {len(symbols)} instruments active "
                f"(close {close_at:%H:%M}Z)",
                end="",
            )
            if args.dry_run:
                print(" (dry run)")
                enqueued += len(symbols)
                if enqueued >= args.max_requests:
                    print(f"  stopping at --max-requests={args.max_requests}")
                    break
                continue

            day_new = 0
            for symbol in symbols:
                if enqueued + existing >= args.max_requests:
                    break
                row = enqueue_market_data_backfill_request(
                    session_factory,
                    symbol=symbol,
                    trade_date=day.isoformat(),
                    requested_until=close_at,
                    reason=args.reason,
                    **(
                        {"duration": MONTHLY_DURATION}
                        if args.granularity == "month"
                        else {}
                    ),
                )
                # request_key is unique, so re-running is idempotent.
                if str(row.get("status", "")).upper() == "PENDING" and row.get("attempt_count", 0) == 0:
                    day_new += 1
                else:
                    existing += 1
            enqueued += day_new
            print(f" -> {day_new} new")
            if enqueued + existing >= args.max_requests:
                print(f"  stopping at --max-requests={args.max_requests}")
                break
    finally:
        engine.dispose()

    print(f"\nenqueued {enqueued} new request(s); {existing} already present")
    if not args.dry_run:
        print("The backfill worker will drain these under the pacing governor.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
