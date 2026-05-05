from __future__ import annotations

from datetime import datetime, timezone

from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.ibkr.market_stream_store import list_market_stream_bars
from ibkr_trader.ibkr.market_stream_store import merge_bar_lists
from ibkr_trader.ibkr.market_stream_store import persist_market_stream_snapshot_bars


def test_persists_and_reads_stream_snapshot_bars() -> None:
    engine = build_engine("sqlite+pysqlite:///:memory:")
    create_schema(engine)
    session_factory = create_session_factory(engine)
    try:
        result = persist_market_stream_snapshot_bars(
            session_factory,
            stream_snapshot={
                "quotes": [
                    {
                        "symbol": "AXFO",
                        "exchange": "SMART",
                        "currency": "SEK",
                        "security_type": "STK",
                        "primary_exchange": "SFB",
                    }
                ],
                "bars_by_symbol": {
                    "AXFO": [
                        {
                            "timestamp": "2026-04-29T07:00:00+00:00",
                            "open": "100",
                            "high": "101",
                            "low": "99",
                            "close": "100.50",
                            "bar_count": "4",
                        }
                    ]
                },
            },
        )

        assert result["inserted_count"] == 1
        bars = list_market_stream_bars(
            session_factory,
            symbols=["AXFO"],
            started_at=datetime(2026, 4, 29, 7, 0, tzinfo=timezone.utc),
            ended_at=datetime(2026, 4, 29, 7, 1, tzinfo=timezone.utc),
        )
        assert bars["AXFO"] == [
            {
                "timestamp": "2026-04-29T07:00:00+00:00",
                "open": "100",
                "high": "101",
                "low": "99",
                "close": "100.50",
                "volume": None,
                "bar_count": "4",
                "currency": "SEK",
                "source": "ibkr_live_market_stream_1m",
            }
        ]
    finally:
        engine.dispose()


def test_merge_bar_lists_prefers_newer_payload_for_same_timestamp() -> None:
    merged = merge_bar_lists(
        [
            {
                "timestamp": "2026-04-29T07:00:00+00:00",
                "open": "100",
                "high": "101",
                "low": "99",
                "close": "100",
            }
        ],
        [
            {
                "timestamp": "2026-04-29T07:00:00+00:00",
                "open": "100",
                "high": "102",
                "low": "98",
                "close": "101",
            }
        ],
        limit=10,
    )

    assert merged == [
        {
            "timestamp": "2026-04-29T07:00:00+00:00",
            "open": "100",
            "high": "102",
            "low": "98",
            "close": "101",
        }
    ]

