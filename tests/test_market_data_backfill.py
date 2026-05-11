from __future__ import annotations

from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.ibkr.market_data_backfill import (
    enqueue_market_data_backfill_request,
    list_market_data_backfill_requests,
    run_due_market_data_backfills,
)
from ibkr_trader.ibkr.market_stream_store import list_market_stream_bars


def test_backfill_request_coalesces_symbol_day_until_latest_request() -> None:
    engine = build_engine("sqlite+pysqlite:///:memory:")
    create_schema(engine)
    session_factory = create_session_factory(engine)
    try:
        first = enqueue_market_data_backfill_request(
            session_factory,
            symbol="AXFO",
            trade_date="2026-04-28",
            requested_until=datetime.fromisoformat("2026-04-28T09:10:00+02:00"),
            instrument={"exchange": "SMART", "currency": "SEK"},
            reason="missing_bars",
        )
        second = enqueue_market_data_backfill_request(
            session_factory,
            symbol="AXFO",
            trade_date="2026-04-28",
            requested_until=datetime.fromisoformat("2026-04-28T09:30:00+02:00"),
            instrument={"exchange": "SMART", "currency": "SEK"},
            reason="coverage_low",
        )

        rows = list_market_data_backfill_requests(session_factory)
        assert first["id"] == second["id"]
        assert len(rows) == 1
        assert rows[0]["requested_until"] == "2026-04-28T09:30:00+02:00"
        assert second["enqueue_action"] == "extended"
    finally:
        engine.dispose()


def test_backfill_worker_persists_bars_and_marks_coverage_succeeded() -> None:
    with TemporaryDirectory() as temp_dir:
        database_path = Path(temp_dir) / "backfill.db"
        engine = build_engine(f"sqlite+pysqlite:///{database_path}")
        create_schema(engine)
        session_factory = create_session_factory(engine)
        try:
            enqueue_market_data_backfill_request(
                session_factory,
                symbol="AXFO",
                trade_date="2026-04-28",
                requested_until=datetime.fromisoformat("2026-04-28T09:10:00+02:00"),
                instrument={"exchange": "SMART", "currency": "SEK"},
                reason="missing_bars",
            )

            def fake_execute(operation_name, operation):
                _ = operation
                assert operation_name == "market_data_backfill_today"
                return {
                    "bar_count": 1,
                    "bars": [
                        {
                            "timestamp": "20260428 09:00:00",
                            "open": "100",
                            "high": "101",
                            "low": "99",
                            "close": "100.5",
                            "currency": "SEK",
                        }
                    ],
                }

            result = run_due_market_data_backfills(
                session_factory,
                broker_config=IbkrConnectionConfig(
                    host="127.0.0.1",
                    port=4001,
                    client_id=8,
                    diagnostic_client_id=7,
                    account_id="DU1234567",
                ),
                execute_historical=fake_execute,
                limit=5,
                timeout=10,
            )

            assert result["completed_count"] == 1
            row = list_market_data_backfill_requests(session_factory)[0]
            assert row["status"] == "SUCCEEDED"
            assert row["covered_until"].startswith("2026-04-28T09:10:00")
            bars = list_market_stream_bars(
                session_factory,
                symbols=["AXFO"],
                started_at=datetime.fromisoformat("2026-04-28T09:00:00+02:00"),
                ended_at=datetime.fromisoformat("2026-04-28T09:01:00+02:00"),
            )
            assert bars["AXFO"][0]["timestamp"] == "2026-04-28T07:00:00+00:00"
        finally:
            engine.dispose()
