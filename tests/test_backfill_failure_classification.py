"""A permanently-unresolvable backfill request must not re-enter the queue.

Both failure call sites used to pass retryable=True unconditionally, so
BACKFILL_STATUS_FAILED_FINAL was unreachable. On a whole-market backfill the
names that can never resolve - delisted, not yet listed on that date, wrong
venue - are numerous enough to crowd out real work: the live host logs ~250
"No security definition" errors a day against a budget of 50 requests per
10 minutes.
"""

from __future__ import annotations

from unittest import TestCase

from ibkr_trader.ibkr.broker_circuit import BrokerCircuitOpen
from ibkr_trader.ibkr.market_data_backfill import (
    TERMINAL_CONTRACT_ERROR_MARKERS,
    _is_terminal_backfill_failure,
)


class TerminalFailureTests(TestCase):
    def test_ibkr_error_200_is_terminal(self) -> None:
        """The exact text IBKR returns for a contract that does not exist."""
        exc = LookupError(
            "IBKR rejected the contract lookup for ACUVI: "
            "No security definition has been found for the request"
        )
        self.assertTrue(_is_terminal_backfill_failure(exc))

    def test_no_contract_matched_is_terminal(self) -> None:
        exc = LookupError("No IBKR contract matched FOO on SFB SEK.")
        self.assertTrue(_is_terminal_backfill_failure(exc))

    def test_hmds_no_data_is_terminal(self) -> None:
        """IBKR error 162. Requests are per (symbol, trade_date), so "no data"
        is permanent for that pair. Measured as the dominant failure on the
        live 48h pilot, where it drove attempt_count to 261."""
        exc = LookupError(
            "IBKR rejected the historical data request for 36GRP: "
            "162 Historical Market Data Service error message:"
            "HMDS query returned no data: 36GRP@SMART Trades"
        )
        self.assertTrue(_is_terminal_backfill_failure(exc))

    def test_a_malformed_request_is_terminal(self) -> None:
        """It will be exactly as malformed on the next attempt."""
        self.assertTrue(_is_terminal_backfill_failure(ValueError("duration is required")))

    def test_transport_failures_stay_retryable(self) -> None:
        for exc in (
            ConnectionError("Failed to connect to IBKR at 127.0.0.1:4002"),
            TimeoutError("Timed out while resolving ERIC-B on SFB."),
            BrokerCircuitOpen("circuit open"),
        ):
            with self.subTest(exc=type(exc).__name__):
                self.assertFalse(_is_terminal_backfill_failure(exc))

    def test_a_lookup_failure_during_a_gateway_restart_stays_retryable(self) -> None:
        """LookupError alone is not enough: it also covers incidental failures
        while the Gateway is restarting, which do resolve on their own."""
        exc = LookupError("IBKR rejected the contract lookup for ERIC-B: 1100 Connectivity lost")
        self.assertFalse(_is_terminal_backfill_failure(exc))

    def test_an_unexpected_error_stays_retryable(self) -> None:
        """Unknown failures must not be silently dropped from the queue."""
        self.assertFalse(_is_terminal_backfill_failure(RuntimeError("something odd")))

    def test_matching_is_case_insensitive(self) -> None:
        self.assertTrue(
            _is_terminal_backfill_failure(LookupError("NO SECURITY DEFINITION has been found"))
        )

    def test_every_marker_is_lowercase(self) -> None:
        """Matching lowercases the message, so an uppercase marker never fires."""
        for marker in TERMINAL_CONTRACT_ERROR_MARKERS:
            with self.subTest(marker=marker):
                self.assertEqual(marker, marker.lower())


class TerminalFailureLeavesTheQueueTests(TestCase):
    """The property that matters: a terminal failure is not re-claimed."""

    def setUp(self) -> None:
        from ibkr_trader.db.base import build_engine, create_schema, create_session_factory

        self.engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(self.engine)
        self.session_factory = create_session_factory(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    def _enqueue(self, symbol: str):
        from datetime import UTC, datetime

        from ibkr_trader.ibkr.market_data_backfill import (
            enqueue_market_data_backfill_request,
        )

        return enqueue_market_data_backfill_request(
            self.session_factory,
            symbol=symbol,
            trade_date="2026-05-04",
            requested_until=datetime(2026, 5, 4, 17, 30, tzinfo=UTC),
            reason="test",
        )

    def test_a_terminal_failure_is_never_claimed_again(self) -> None:
        from ibkr_trader.ibkr.market_data_backfill import (
            BACKFILL_STATUS_FAILED_FINAL,
            claim_due_market_data_backfill_requests,
            mark_market_data_backfill_failed,
        )

        row = self._enqueue("DEADNAME")
        claimed = claim_due_market_data_backfill_requests(self.session_factory, limit=5)
        self.assertEqual(len(claimed), 1)

        mark_market_data_backfill_failed(
            self.session_factory,
            request_id=int(row["id"]),
            error="No security definition has been found for the request",
            retryable=False,
        )
        # Not re-claimable now, and not after any amount of waiting.
        self.assertEqual(claim_due_market_data_backfill_requests(self.session_factory, limit=5), [])

        from ibkr_trader.ibkr.market_data_backfill import list_market_data_backfill_requests
        rows = list_market_data_backfill_requests(self.session_factory, symbols=["DEADNAME"])
        self.assertEqual(rows[0]["status"], BACKFILL_STATUS_FAILED_FINAL)
        self.assertIsNone(rows[0]["next_retry_at"])

    def test_a_retryable_failure_does_come_back(self) -> None:
        from ibkr_trader.ibkr.market_data_backfill import (
            claim_due_market_data_backfill_requests,
            mark_market_data_backfill_failed,
        )

        row = self._enqueue("ERIC-B")
        claim_due_market_data_backfill_requests(self.session_factory, limit=5)
        mark_market_data_backfill_failed(
            self.session_factory,
            request_id=int(row["id"]),
            error="Failed to connect to IBKR",
            retryable=True,
            retry_after_seconds=0.0,
        )
        self.assertEqual(len(claim_due_market_data_backfill_requests(self.session_factory, limit=5)), 1)
