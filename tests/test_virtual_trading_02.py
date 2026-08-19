from __future__ import annotations

from tests._virtual_trading_shared import *  # noqa: F401,F403


class VirtualTradingTests02(VirtualTradingTestsBase):
    def test_virtual_orders_fill_from_stream_snapshot_when_limit_crosses(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            _write_schedule_fixture(schedule_path)
            batch = parse_execution_batch_payload(_virtual_payload())
            submit_execution_batch(
                self.session_factory,
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )

            first_cycle = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 1, tzinfo=timezone.utc),
            )
            self.assertEqual(len(first_cycle.submitted_entries), 1)
            self.assertEqual(len(first_cycle.filled_entries), 0)

            entry_stream_snapshot = {
                "running": True,
                "quotes": [
                    {
                        "symbol": "SIVE",
                        "exchange": "SMART",
                        "currency": "SEK",
                        "security_type": "STK",
                        "primary_exchange": "SFB",
                        "bid_price": None,
                        "ask_price": None,
                        "last_price": None,
                        "updated_at": "2026-04-27T07:02:00Z",
                    }
                ],
                "bars_by_symbol": {
                    "SIVE": [
                        {
                            "timestamp": "2026-04-27T07:02:00Z",
                            "open": "10.70",
                            "high": "10.80",
                            "low": "10.40",
                            "close": "10.70",
                        }
                    ]
                },
            }

            second_cycle = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 2, tzinfo=timezone.utc),
                virtual_market_sync=lambda at: record_virtual_market_quotes_from_stream_snapshot(
                    self.session_factory,
                    stream_snapshot=entry_stream_snapshot,
                    observed_at=at,
                ),
            )
            self.assertEqual(len(second_cycle.filled_entries), 1)
            self.assertEqual(len(second_cycle.submitted_exits), 1)

            exit_stream_snapshot = {
                "running": True,
                "quotes": [
                    {
                        "symbol": "SIVE",
                        "exchange": "SMART",
                        "currency": "SEK",
                        "security_type": "STK",
                        "primary_exchange": "SFB",
                        "bid_price": None,
                        "ask_price": None,
                        "last_price": None,
                        "updated_at": "2026-04-27T07:03:00Z",
                    }
                ],
                "bars_by_symbol": {
                    "SIVE": [
                        {
                            "timestamp": "2026-04-27T07:03:00Z",
                            "open": "11.40",
                            "high": "11.60",
                            "low": "11.35",
                            "close": "11.45",
                        }
                    ]
                },
            }
            third_cycle = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 3, tzinfo=timezone.utc),
                virtual_market_sync=lambda at: record_virtual_market_quotes_from_stream_snapshot(
                    self.session_factory,
                    stream_snapshot=exit_stream_snapshot,
                    observed_at=at,
                ),
            )
            self.assertEqual(len(third_cycle.completed_instructions), 1)

        session = self.session_factory()
        try:
            instruction = session.execute(select(InstructionRecord)).scalar_one()
            orders = session.execute(
                select(BrokerOrderRecord).order_by(BrokerOrderRecord.id)
            ).scalars().all()
            fills = session.execute(
                select(ExecutionFillRecord).order_by(ExecutionFillRecord.id)
            ).scalars().all()

            self.assertEqual(instruction.state, ExecutionState.COMPLETED.value)
            self.assertEqual([order.status for order in orders], ["FILLED", "FILLED"])
            self.assertEqual([fill.price for fill in fills], ["10.50", "11.55"])
            self.assertTrue(
                all(
                    "STREAM_BAR" in fill.raw_payload.get("condition_code", "")
                    for fill in fills
                )
            )
        finally:
            session.close()

    def test_virtual_limit_fill_uses_quote_price_when_market_is_better_than_limit(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            _write_schedule_fixture(schedule_path)
            payload = _virtual_payload()
            payload["instructions"][0]["entry"]["limit_price"] = "55.6802"  # type: ignore[index]
            payload["instructions"][0]["sizing"] = {  # type: ignore[index]
                "mode": "target_quantity",
                "target_quantity": "38",
            }
            batch = parse_execution_batch_payload(payload)
            submit_execution_batch(
                self.session_factory,
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )

            first_cycle = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 1, tzinfo=timezone.utc),
            )
            self.assertEqual(len(first_cycle.submitted_entries), 1)
            self.assertEqual(len(first_cycle.filled_entries), 0)

            record_virtual_market_quote(
                self.session_factory,
                account_key="virtual0001",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                bid_price=Decimal("51.95"),
                ask_price=Decimal("52.05"),
                last_price=Decimal("51.95"),
                observed_at=datetime(2026, 4, 27, 7, 2, tzinfo=timezone.utc),
                source="ibkr_live_market_stream_virtual_bridge",
                raw_payload={
                    "latest_stream_bar": {
                        "timestamp": "2026-04-27T07:02:00Z",
                        "open": "52.10",
                        "high": "52.20",
                        "low": "51.95",
                        "close": "51.95",
                    }
                },
            )

        session = self.session_factory()
        try:
            fill = session.execute(select(ExecutionFillRecord)).scalar_one()
            self.assertEqual(fill.price, "52.05")
            self.assertEqual(
                fill.raw_payload["condition_code"],
                "BUY_LIMIT_MET:STREAM_BAR_LOW",
            )
        finally:
            session.close()
