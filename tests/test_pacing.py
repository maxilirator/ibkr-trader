from __future__ import annotations

from unittest import TestCase

from ibkr_trader.ibkr.pacing import BrokerApiPacingGovernor
from ibkr_trader.ibkr.pacing import BrokerPacingConfig
from ibkr_trader.ibkr.pacing import BrokerPacingLimitExceeded


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class BrokerApiPacingGovernorTests(TestCase):
    def test_api_request_pacing_waits_for_next_second(self) -> None:
        clock = FakeClock()
        governor = BrokerApiPacingGovernor(
            BrokerPacingConfig(
                max_requests_per_second=2,
                request_acquire_timeout_seconds=2,
            ),
            monotonic=clock.monotonic,
            sleeper=clock.sleep,
        )

        governor.acquire_api_request("one")
        governor.acquire_api_request("two")
        governor.acquire_api_request("three")

        self.assertEqual(clock.sleeps, [1.0])
        snapshot = governor.snapshot()
        self.assertEqual(snapshot["api_requests_last_second"], 1)
        self.assertEqual(snapshot["total_api_request_permits"], 3)
        self.assertEqual(snapshot["last_api_operation"], "three")

    def test_api_request_pacing_raises_when_timeout_is_too_short(self) -> None:
        clock = FakeClock()
        governor = BrokerApiPacingGovernor(
            BrokerPacingConfig(
                max_requests_per_second=1,
                request_acquire_timeout_seconds=0,
            ),
            monotonic=clock.monotonic,
            sleeper=clock.sleep,
        )

        governor.acquire_api_request("one")

        with self.assertRaisesRegex(BrokerPacingLimitExceeded, "request permits/second"):
            governor.acquire_api_request("two")

        self.assertEqual(clock.sleeps, [])
        self.assertIn("pacing limit exceeded", str(governor.snapshot()["last_rejection"]))

    def test_historical_request_limit_is_enforced_before_api_permit(self) -> None:
        clock = FakeClock()
        governor = BrokerApiPacingGovernor(
            BrokerPacingConfig(
                max_requests_per_second=10,
                max_historical_requests_per_10_minutes=1,
                historical_window_seconds=600,
            ),
            monotonic=clock.monotonic,
            sleeper=clock.sleep,
        )

        governor.acquire_historical_request("history")

        with self.assertRaisesRegex(BrokerPacingLimitExceeded, "historical pacing limit"):
            governor.acquire_historical_request("history")

        snapshot = governor.snapshot()
        self.assertEqual(snapshot["total_historical_request_permits"], 1)
        self.assertEqual(snapshot["total_api_request_permits"], 2)

    def test_market_data_line_limit_rejects_oversized_subscription_set(self) -> None:
        governor = BrokerApiPacingGovernor(
            BrokerPacingConfig(max_market_data_lines=2)
        )

        governor.check_market_data_line_limit(
            requested_line_count=2,
            operation_name="stream",
        )

        with self.assertRaisesRegex(BrokerPacingLimitExceeded, "market data line limit"):
            governor.check_market_data_line_limit(
                requested_line_count=3,
                operation_name="stream",
            )

