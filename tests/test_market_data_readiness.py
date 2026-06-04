from __future__ import annotations

from datetime import datetime
from datetime import timezone
from unittest import TestCase

from ibkr_trader.orchestration.market_data_readiness import (
    build_market_stream_readiness_checker,
)
from ibkr_trader.orchestration.market_data_readiness import (
    evaluate_market_stream_snapshot,
)


def _instruction_payload() -> dict[str, object]:
    return {
        "instruction": {
            "instruction_id": "runtime-aapl-1",
            "account": {
                "account_key": "GTW05",
                "book_key": "long_risk_book",
            },
            "instrument": {
                "symbol": "AAPL",
                "security_type": "STK",
                "exchange": "SMART",
                "currency": "USD",
                "primary_exchange": "NASDAQ",
            },
            "intent": {
                "side": "BUY",
                "position_side": "LONG",
            },
            "sizing": {
                "mode": "target_quantity",
                "target_quantity": "1",
            },
            "entry": {
                "order_type": "LIMIT",
                "submit_at": "2026-04-10T15:55:00-04:00",
                "expire_at": "2026-04-10T15:59:00-04:00",
                "limit_price": "200.00",
                "time_in_force": "DAY",
            },
            "exit": {
                "take_profit_pct": "0.02",
            },
            "trace": {
                "reason_code": "readiness-test",
            },
        }
    }


class MarketDataReadinessTests(TestCase):
    def test_checker_subscribes_target_and_accepts_fresh_stream_evidence(self) -> None:
        class FakeMarketStreamService:
            def __init__(self) -> None:
                self.subscriptions: list[tuple[str, bool, str | None]] = []
                self.snapshot_calls: list[tuple[list[str] | None, int]] = []

            def subscribe_many(self, contracts: list[object], *, replace: bool, market_data_type: str | None) -> None:
                contract = contracts[0]
                self.subscriptions.append((contract.symbol, replace, market_data_type))

            def snapshot(self, *, symbols: list[str] | None = None, bar_limit: int = 390) -> dict[str, object]:
                self.snapshot_calls.append((symbols, bar_limit))
                return {
                    "running": True,
                    "last_error": None,
                    "desired_symbols": ["AAPL"],
                    "subscriptions": [
                        {
                            "status": "subscribed",
                            "contract": {"symbol": "AAPL"},
                        }
                    ],
                    "quotes": [
                        {
                            "symbol": "AAPL",
                            "updated_at": "2026-04-10T19:59:59+00:00",
                            "last_trade_at": None,
                            "bid_price": "199.99",
                            "ask_price": "200.01",
                        }
                    ],
                    "bars_by_symbol": {"AAPL": []},
                }

        service = FakeMarketStreamService()
        checker = build_market_stream_readiness_checker(
            service,
            max_age_seconds=180,
            market_data_type="LIVE",
        )

        result = checker(
            "runtime-aapl-1",
            _instruction_payload(),
            datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
        )

        self.assertTrue(result.ready)
        self.assertEqual(result.reason, "market_stream_ready")
        self.assertEqual(service.subscriptions, [("AAPL", False, "LIVE")])
        self.assertEqual(service.snapshot_calls, [(["AAPL"], 1)])
        self.assertEqual(result.evidence["latest_market_data_age_seconds"], 1)

    def test_checker_waits_for_first_tick_after_new_subscription(self) -> None:
        class FakeMarketStreamService:
            def __init__(self) -> None:
                self.snapshot_calls = 0

            def subscribe_many(
                self,
                contracts: list[object],
                *,
                replace: bool,
                market_data_type: str | None,
            ) -> None:
                self.contract = contracts[0]
                self.replace = replace
                self.market_data_type = market_data_type

            def snapshot(
                self,
                *,
                symbols: list[str] | None = None,
                bar_limit: int = 390,
            ) -> dict[str, object]:
                del symbols, bar_limit
                self.snapshot_calls += 1
                quote = {
                    "symbol": "AAPL",
                    "updated_at": None,
                    "last_trade_at": None,
                }
                if self.snapshot_calls >= 3:
                    quote["updated_at"] = "2026-04-10T19:59:58+00:00"
                return {
                    "running": True,
                    "last_error": None,
                    "desired_symbols": ["AAPL"],
                    "subscriptions": [
                        {
                            "status": "subscribed",
                            "contract": {"symbol": "AAPL"},
                        }
                    ],
                    "quotes": [quote],
                    "bars_by_symbol": {"AAPL": []},
                }

        sleeps: list[float] = []
        service = FakeMarketStreamService()
        checker = build_market_stream_readiness_checker(
            service,
            max_age_seconds=180,
            market_data_type="LIVE",
            first_data_wait_seconds=2.0,
            first_data_poll_seconds=0.5,
            sleep_fn=sleeps.append,
        )

        result = checker(
            "runtime-aapl-1",
            _instruction_payload(),
            datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
        )

        self.assertTrue(result.ready)
        self.assertEqual(result.reason, "market_stream_ready")
        self.assertEqual(service.snapshot_calls, 3)
        self.assertEqual(sleeps, [0.5, 0.5])

    def test_snapshot_without_fresh_symbol_data_is_not_ready(self) -> None:
        result = evaluate_market_stream_snapshot(
            {
                "running": True,
                "last_error": None,
                "desired_symbols": ["AAPL"],
                "subscriptions": [
                    {
                        "status": "subscribed",
                        "contract": {"symbol": "AAPL"},
                    }
                ],
                "quotes": [
                    {
                        "symbol": "AAPL",
                        "updated_at": "2026-04-10T19:56:00+00:00",
                    }
                ],
                "bars_by_symbol": {"AAPL": []},
            },
            symbol="AAPL",
            reference_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
            max_age_seconds=180,
        )

        self.assertFalse(result.ready)
        self.assertEqual(result.reason, "market_stream_data_stale")
        self.assertEqual(result.evidence["latest_market_data_age_seconds"], 240)
