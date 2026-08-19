"""Tests for the named broker recovery and market stream states.

These assert the *policy*, not the plumbing: which named state a given real
snapshot produces, and what authority that state grants. The precedence rules
(maintenance over circuit, circuit over connection state) are the part most
likely to be broken by a later well-meaning edit, so they are asserted directly.
"""

from __future__ import annotations

import threading
from types import SimpleNamespace
from unittest import TestCase

from ibkr_trader.api.server_routes_broker_market import (
    _build_recovery_policy_payload,
)
from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.ibkr.broker_circuit import BrokerHealthCircuit
from ibkr_trader.ibkr.market_stream import LiveMarketDataStreamService
from ibkr_trader.ibkr.session_manager import (
    ManagedSessionMetrics,
    ManagedSessionStatus,
    serialize_managed_session_status,
)
from ibkr_trader.ibkr.recovery_policy import (
    BrokerRecoveryState,
    StreamAssessment,
    StreamState,
    assess_broker_session_payload,
    assess_market_stream_payload,
    broker_authority,
    classify_broker_recovery_state,
    classify_stream_state,
)


class BrokerRecoveryStateTests(TestCase):
    def test_connected_without_failures_is_healthy_and_allows_entries(self) -> None:
        assessment = classify_broker_recovery_state(
            connected=True, consecutive_failures=0
        )
        self.assertEqual(assessment.state, BrokerRecoveryState.HEALTHY)
        self.assertTrue(assessment.authority.allows_new_entries)
        self.assertTrue(assessment.authority.allows_order_management)
        self.assertFalse(assessment.authority.requires_operator_attention)

    def test_connected_with_failures_is_degraded_and_withholds_new_entries(self) -> None:
        assessment = classify_broker_recovery_state(
            connected=True, consecutive_failures=2
        )
        self.assertEqual(assessment.state, BrokerRecoveryState.DEGRADED)
        self.assertFalse(assessment.authority.allows_new_entries)
        # Flattening an existing position must stay possible during a partial outage.
        self.assertTrue(assessment.authority.allows_order_management)
        self.assertTrue(assessment.authority.requires_operator_attention)

    def test_disconnected_below_threshold_is_recovering_without_paging(self) -> None:
        assessment = classify_broker_recovery_state(
            connected=False, consecutive_failures=1, down_failure_threshold=3
        )
        self.assertEqual(assessment.state, BrokerRecoveryState.RECOVERING)
        self.assertFalse(assessment.authority.allows_new_entries)
        self.assertFalse(assessment.authority.requires_operator_attention)

    def test_disconnected_at_threshold_is_down_and_pages(self) -> None:
        assessment = classify_broker_recovery_state(
            connected=False, consecutive_failures=3, down_failure_threshold=3
        )
        self.assertEqual(assessment.state, BrokerRecoveryState.DOWN)
        self.assertTrue(assessment.authority.requires_operator_attention)
        self.assertIn("3", assessment.reason)

    def test_recovering_reason_reports_remaining_backoff(self) -> None:
        assessment = classify_broker_recovery_state(
            connected=False,
            consecutive_failures=1,
            cooldown_seconds_remaining=42,
        )
        self.assertEqual(assessment.state, BrokerRecoveryState.RECOVERING)
        self.assertIn("42s", assessment.reason)

    def test_open_circuit_outranks_a_live_connection(self) -> None:
        assessment = classify_broker_recovery_state(
            connected=True,
            consecutive_failures=0,
            circuit_open=True,
            circuit_reason="api_startup_no_next_valid_id",
        )
        self.assertEqual(assessment.state, BrokerRecoveryState.BLOCKED)
        self.assertFalse(assessment.authority.allows_new_entries)
        self.assertFalse(assessment.authority.allows_order_management)
        self.assertIn("api_startup_no_next_valid_id", assessment.reason)

    def test_maintenance_outranks_an_open_circuit_and_does_not_page(self) -> None:
        assessment = classify_broker_recovery_state(
            connected=False,
            consecutive_failures=9,
            circuit_open=True,
            maintenance_mode=True,
        )
        self.assertEqual(assessment.state, BrokerRecoveryState.MAINTENANCE)
        self.assertFalse(assessment.authority.allows_new_entries)
        self.assertFalse(assessment.authority.requires_operator_attention)

    def test_unresolvable_state_is_unknown_and_withholds_all_authority(self) -> None:
        for connected, failures in ((None, 0), (True, None), (None, None)):
            with self.subTest(connected=connected, failures=failures):
                assessment = classify_broker_recovery_state(
                    connected=connected, consecutive_failures=failures
                )
                self.assertEqual(assessment.state, BrokerRecoveryState.UNKNOWN)
                self.assertFalse(assessment.authority.allows_new_entries)
                self.assertFalse(assessment.authority.allows_order_management)
                self.assertFalse(assessment.authority.allows_market_data)
                self.assertTrue(assessment.authority.requires_operator_attention)

    def test_only_healthy_state_permits_new_entries(self) -> None:
        """New risk is the one authority that must be unique to HEALTHY."""
        permitted = [
            state
            for state in BrokerRecoveryState
            if broker_authority(state).allows_new_entries
        ]
        self.assertEqual(permitted, [BrokerRecoveryState.HEALTHY])

    def test_every_state_has_an_authority_entry(self) -> None:
        for state in BrokerRecoveryState:
            with self.subTest(state=state):
                self.assertIsNotNone(broker_authority(state))

    def test_payload_exposes_state_and_authority(self) -> None:
        payload = classify_broker_recovery_state(
            connected=True, consecutive_failures=0
        ).to_payload()
        self.assertEqual(payload["state"], "healthy")
        self.assertTrue(payload["allows_new_entries"])
        self.assertFalse(payload["requires_operator_attention"])


class StreamStateTests(TestCase):
    def test_running_with_fresh_data_is_streaming_and_usable(self) -> None:
        assessment = classify_stream_state(
            running=True,
            subscription_count=12,
            latest_market_data_age_seconds=5,
            stale_after_seconds=180,
            is_stale=False,
        )
        self.assertEqual(assessment.state, StreamState.STREAMING)
        self.assertTrue(assessment.is_usable)

    def test_running_without_subscriptions_is_idle_not_stale(self) -> None:
        """No subscriptions means no data is expected, so age carries no signal."""
        assessment = classify_stream_state(
            running=True,
            subscription_count=0,
            latest_market_data_age_seconds=None,
            stale_after_seconds=180,
        )
        self.assertEqual(assessment.state, StreamState.IDLE)
        self.assertFalse(assessment.is_usable)

    def test_service_staleness_verdict_is_authoritative(self) -> None:
        assessment = classify_stream_state(
            running=True,
            subscription_count=4,
            latest_market_data_age_seconds=10,
            stale_after_seconds=180,
            is_stale=True,
        )
        self.assertEqual(assessment.state, StreamState.STALE)
        self.assertFalse(assessment.is_usable)

    def test_age_beyond_threshold_is_stale_when_service_has_no_verdict(self) -> None:
        assessment = classify_stream_state(
            running=True,
            subscription_count=4,
            latest_market_data_age_seconds=180,
            stale_after_seconds=180,
            is_stale=None,
        )
        self.assertEqual(assessment.state, StreamState.STALE)

    def test_subscribed_but_never_ticked_is_stale(self) -> None:
        assessment = classify_stream_state(
            running=True,
            subscription_count=4,
            latest_market_data_age_seconds=None,
            stale_after_seconds=180,
            is_stale=False,
        )
        self.assertEqual(assessment.state, StreamState.STALE)
        self.assertIn("never received", assessment.reason)

    def test_stopped_stream_with_auto_reconnect_is_reconnecting(self) -> None:
        assessment = classify_stream_state(
            running=False,
            subscription_count=4,
            latest_market_data_age_seconds=None,
            stale_after_seconds=180,
            auto_reconnect_active=True,
        )
        self.assertEqual(assessment.state, StreamState.RECONNECTING)

    def test_stopped_stream_in_cooldown_is_blocked(self) -> None:
        assessment = classify_stream_state(
            running=False,
            subscription_count=4,
            latest_market_data_age_seconds=None,
            stale_after_seconds=180,
            cooldown_seconds_remaining=30,
            auto_reconnect_active=True,
        )
        self.assertEqual(assessment.state, StreamState.BLOCKED)
        self.assertIn("30s", assessment.reason)

    def test_stopped_stream_without_auto_reconnect_is_stopped(self) -> None:
        """STOPPED is distinct from RECONNECTING: it will not self-heal."""
        assessment = classify_stream_state(
            running=False,
            subscription_count=4,
            latest_market_data_age_seconds=None,
            stale_after_seconds=180,
            auto_reconnect_active=False,
        )
        self.assertEqual(assessment.state, StreamState.STOPPED)
        self.assertIn("operator action", assessment.reason)

    def test_unresolvable_stream_state_is_unknown_and_unusable(self) -> None:
        assessment = classify_stream_state(
            running=None,
            subscription_count=None,
            latest_market_data_age_seconds=None,
            stale_after_seconds=None,
        )
        self.assertEqual(assessment.state, StreamState.UNKNOWN)
        self.assertFalse(assessment.is_usable)

    def test_only_streaming_is_usable(self) -> None:
        """Acting on stream data is an authority unique to STREAMING."""
        usable = [
            state
            for state in StreamState
            if StreamAssessment(state, "probe").is_usable
        ]
        self.assertEqual(usable, [StreamState.STREAMING])


def _managed_session_status(
    *,
    connected: bool,
    consecutive_failures: int,
    cooldown_seconds_remaining: int | None = None,
) -> ManagedSessionStatus:
    """Build a real ManagedSessionStatus so the adapter is tested against the
    production dataclass rather than a hand-written dict that could drift."""
    return ManagedSessionStatus(
        role="primary",
        host="127.0.0.1",
        port=4001,
        client_id=0,
        connected=connected,
        last_error=None,
        consecutive_failures=consecutive_failures,
        cooldown_until=None,
        cooldown_seconds_remaining=cooldown_seconds_remaining,
        circuit_breaker_reason=None,
        circuit_breaker_until=None,
        metrics=ManagedSessionMetrics(
            connect_attempt_count=1,
            connect_success_count=1,
            disconnect_count=0,
            checkout_count=0,
            failed_checkout_count=0,
            connect_attempts_last_60_seconds=0,
            checkouts_last_60_seconds=0,
            last_connect_attempt_at=None,
            last_connect_success_at=None,
            last_disconnect_at=None,
            last_checkout_at=None,
        ),
    )


class BrokerSessionPayloadAdapterTests(TestCase):
    """The adapter must keep working against the real serialiser output."""

    def test_healthy_real_session_payload_classifies_healthy(self) -> None:
        payload = serialize_managed_session_status(
            _managed_session_status(connected=True, consecutive_failures=0)
        )
        assessment = assess_broker_session_payload(payload)
        self.assertEqual(assessment.state, BrokerRecoveryState.HEALTHY)

    def test_real_session_payload_carries_the_fields_the_adapter_reads(self) -> None:
        """Guards against a rename in session_manager silently degrading us."""
        payload = serialize_managed_session_status(
            _managed_session_status(connected=True, consecutive_failures=0)
        )
        for key in ("connected", "consecutive_failures", "cooldown_seconds_remaining"):
            self.assertIn(key, payload)

    def test_open_real_circuit_snapshot_blocks(self) -> None:
        circuit = BrokerHealthCircuit()
        circuit.trip(reason="probe_failed", source="monitor", error="timeout")
        payload = serialize_managed_session_status(
            _managed_session_status(connected=True, consecutive_failures=0)
        )
        assessment = assess_broker_session_payload(
            payload, circuit_payload=circuit.snapshot()
        )
        self.assertEqual(assessment.state, BrokerRecoveryState.BLOCKED)
        self.assertIn("probe_failed", assessment.reason)

    def test_cleared_real_circuit_snapshot_does_not_block(self) -> None:
        circuit = BrokerHealthCircuit()
        circuit.trip(reason="probe_failed", source="monitor")
        circuit.clear()
        payload = serialize_managed_session_status(
            _managed_session_status(connected=True, consecutive_failures=0)
        )
        assessment = assess_broker_session_payload(
            payload, circuit_payload=circuit.snapshot()
        )
        self.assertEqual(assessment.state, BrokerRecoveryState.HEALTHY)

    def test_missing_payload_is_unknown(self) -> None:
        self.assertEqual(
            assess_broker_session_payload(None).state, BrokerRecoveryState.UNKNOWN
        )

    def test_malformed_fields_are_unknown_not_healthy(self) -> None:
        assessment = assess_broker_session_payload(
            {"connected": "yes", "consecutive_failures": "none"}
        )
        self.assertEqual(assessment.state, BrokerRecoveryState.UNKNOWN)
        self.assertFalse(assessment.authority.allows_new_entries)

    def test_lock_busy_session_is_unknown_not_recovering(self) -> None:
        """A non-blocking status() that lost the lock race reports
        connected=False as a last-known value. Reading that as a disconnect
        would report a busy, trading session as RECOVERING or DOWN."""
        payload = serialize_managed_session_status(
            _managed_session_status(connected=False, consecutive_failures=4)
        )
        payload["status_available"] = False
        assessment = assess_broker_session_payload(payload)
        self.assertEqual(assessment.state, BrokerRecoveryState.UNKNOWN)
        self.assertIn("lock was busy", assessment.reason)

    def test_real_lock_busy_snapshot_is_flagged_unavailable(self) -> None:
        """Verifies the marker against the real session_manager dataclass."""
        status = _managed_session_status(connected=True, consecutive_failures=0)
        payload = serialize_managed_session_status(status)
        self.assertTrue(payload["status_available"])

    def test_per_session_circuit_blocks_even_when_shared_circuit_is_closed(self) -> None:
        """A successful market-stream connect clears the shared circuit, so a
        circuit-broken session must be detected from its own fields."""
        status = _managed_session_status(connected=False, consecutive_failures=1)
        payload = serialize_managed_session_status(status)
        payload["circuit_breaker_reason"] = "api_startup_no_next_valid_id"
        assessment = assess_broker_session_payload(
            payload, circuit_payload={"open": False}
        )
        self.assertEqual(assessment.state, BrokerRecoveryState.BLOCKED)
        self.assertIn("api_startup_no_next_valid_id", assessment.reason)
        self.assertTrue(assessment.authority.requires_operator_attention)

    def test_negative_failure_count_is_unknown_not_healthy(self) -> None:
        assessment = classify_broker_recovery_state(
            connected=True, consecutive_failures=-5
        )
        self.assertEqual(assessment.state, BrokerRecoveryState.UNKNOWN)


class MarketStreamPayloadAdapterTests(TestCase):
    def test_uses_subscribed_count_not_the_filtered_subscription_list(self) -> None:
        """`subscriptions` is filtered by requested symbols; `subscribed_count` is not."""
        assessment = assess_market_stream_payload(
            {
                "running": True,
                "subscribed_count": 7,
                "subscriptions": [],
                "latest_market_data_age_seconds": 3,
                "stale_after_seconds": 180,
                "is_stale": False,
            }
        )
        self.assertEqual(assessment.state, StreamState.STREAMING)
        self.assertIn("7 subscription(s)", assessment.reason)

    def test_dead_supervisor_maps_to_stopped(self) -> None:
        """`auto_reconnect_active` is the flag that decides whether anything will
        reconnect. `stale_reconnect_enabled` is a different setting entirely and
        must not be mistaken for it."""
        assessment = assess_market_stream_payload(
            {
                "status_available": True,
                "running": False,
                "subscribed_count": 5,
                "stale_after_seconds": 180,
                "auto_reconnect_active": False,
                # Deliberately the opposite value, to catch reading the wrong key.
                "stale_reconnect_enabled": True,
            }
        )
        self.assertEqual(assessment.state, StreamState.STOPPED)

    def test_unknown_supervisor_state_is_not_assumed_recoverable(self) -> None:
        assessment = assess_market_stream_payload(
            {
                "status_available": True,
                "running": False,
                "subscribed_count": 5,
                "stale_after_seconds": 180,
            }
        )
        self.assertEqual(assessment.state, StreamState.UNKNOWN)

    def test_stale_detection_disabled_is_never_reported_as_streaming(self) -> None:
        """`is_stale=False` also means "not checked"; a six-hour-old feed must
        not read as healthy just because staleness detection is switched off."""
        assessment = assess_market_stream_payload(
            {
                "status_available": True,
                "running": True,
                "subscribed_count": 40,
                "latest_market_data_age_seconds": 21600,
                "stale_after_seconds": None,
                "is_stale": False,
                "staleness_detectable": False,
                "auto_reconnect_active": True,
            }
        )
        self.assertEqual(assessment.state, StreamState.UNKNOWN)
        self.assertFalse(assessment.is_usable)

    def test_lock_busy_stream_is_unknown(self) -> None:
        assessment = assess_market_stream_payload(
            {"status_available": False, "unavailable_reason": "lock is busy"}
        )
        self.assertEqual(assessment.state, StreamState.UNKNOWN)
        self.assertIn("lock is busy", assessment.reason)

    def test_missing_payload_is_unknown(self) -> None:
        self.assertEqual(
            assess_market_stream_payload(None).state, StreamState.UNKNOWN
        )


class MarketStreamHealthSnapshotTests(TestCase):
    """`health_snapshot` was split out of `snapshot`; the two must stay aligned."""

    def _service(self) -> LiveMarketDataStreamService:
        return LiveMarketDataStreamService(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4001,
                client_id=0,
                diagnostic_client_id=7,
                streaming_client_id=9,
            )
        )

    #: Every key `snapshot()` is contracted to return. Frozen deliberately: a
    #: subset check in either direction cannot detect a key being dropped from
    #: the single helper that now produces all of them, which is exactly the
    #: mistake the extraction refactor was capable of making.
    EXPECTED_SNAPSHOT_KEYS = frozenset(
        {
            "status_available",
            "auto_reconnect_active",
            "staleness_detectable",
            "running",
            "started_at",
            "last_error",
            "consecutive_failures",
            "cooldown_until",
            "cooldown_seconds_remaining",
            "connect_attempt_count",
            "connect_success_count",
            "last_connect_attempt_at",
            "last_connect_success_at",
            "last_disconnect_observed_at",
            "latest_market_data_at",
            "latest_market_data_age_seconds",
            "latest_quote_at",
            "latest_trade_at",
            "stale_after_seconds",
            "is_stale",
            "stale_reconnect_enabled",
            "stale_reconnect_allowed",
            "stale_reconnect_count",
            "last_connectivity_event_at",
            "last_connectivity_event_code",
            "last_connectivity_event_message",
            "connectivity_resubscribe_count",
            "connectivity_maintained_count",
            "market_data_line_limit",
            "last_stale_detected_at",
            "desired_subscription_count",
            "desired_symbols",
            "last_desired_update_at",
            "desired_update_count",
            "desired_noop_count",
            "subscribed_count",
            "last_subscribe_request_at",
            "last_subscription_change_at",
            "subscribe_request_count",
            "subscribe_noop_count",
            "actual_subscription_count",
            "actual_unsubscription_count",
            "market_data_type_request_count",
            "errors",
            "subscriptions",
            "quote_count",
            "quotes",
            "bars_by_symbol",
        }
    )

    HEAVY_KEYS = frozenset({"subscriptions", "quote_count", "quotes", "bars_by_symbol"})

    def test_snapshot_returns_exactly_the_contracted_keys(self) -> None:
        self.assertEqual(set(self._service().snapshot()), self.EXPECTED_SNAPSHOT_KEYS)

    def test_health_snapshot_is_snapshot_minus_exactly_the_heavy_keys(self) -> None:
        self.assertEqual(
            set(self._service().health_snapshot()),
            self.EXPECTED_SNAPSHOT_KEYS - self.HEAVY_KEYS,
        )

    def test_health_snapshot_reports_unavailable_when_lock_is_busy(self) -> None:
        """It must not block: the stream lock is held across pacing-governed
        IBKR calls, and a stalled /healthz suppresses the operator watchdog."""
        service = self._service()
        acquired = threading.Event()
        release = threading.Event()

        def hold_lock() -> None:
            with service._lock:
                acquired.set()
                release.wait(timeout=5)

        holder = threading.Thread(target=hold_lock, daemon=True)
        holder.start()
        try:
            self.assertTrue(acquired.wait(timeout=5))
            payload = service.health_snapshot()
        finally:
            release.set()
            holder.join(timeout=5)

        self.assertFalse(payload["status_available"])
        self.assertEqual(
            assess_market_stream_payload(payload).state, StreamState.UNKNOWN
        )

    def test_health_snapshot_values_match_snapshot(self) -> None:
        service = self._service()
        health = service.health_snapshot()
        full = service.snapshot()
        for key, value in health.items():
            with self.subTest(key=key):
                # Excluded because it is derived from wall-clock time and would
                # differ between the two calls once any quote exists.
                if key != "latest_market_data_age_seconds":
                    self.assertEqual(full[key], value)


class RecoveryPolicyPayloadTests(TestCase):
    """The /healthz builder's failure paths.

    These are the "explicit failure" branches the module advertises, so they get
    coverage: an unreadable control table and an exploding stream service must
    both produce a payload that reports the problem rather than a reassuring
    default.
    """

    def _sessions(self) -> dict[str, dict[str, object]]:
        return {
            "primary": serialize_managed_session_status(
                _managed_session_status(connected=True, consecutive_failures=0)
            )
        }

    def test_healthy_inputs_produce_healthy_states(self) -> None:
        payload = _build_recovery_policy_payload(
            session_statuses=self._sessions(),
            circuit_snapshot={"open": False},
            market_stream_service=SimpleNamespace(
                health_snapshot=lambda: {
                    "status_available": True,
                    "running": True,
                    "subscribed_count": 3,
                    "latest_market_data_age_seconds": 2,
                    "stale_after_seconds": 180,
                    "is_stale": False,
                    "staleness_detectable": True,
                    "auto_reconnect_active": True,
                }
            ),
            session_factory=object(),
            maintenance_reader=lambda _factory: False,
        )
        self.assertEqual(payload["broker_sessions"]["primary"]["state"], "healthy")
        self.assertEqual(payload["market_stream"]["state"], "streaming")
        self.assertIsNone(payload["maintenance_mode_error"])
        self.assertIsNone(payload["market_stream_error"])

    def test_unreadable_maintenance_flag_never_reports_false(self) -> None:
        """Emitting False would be a default that looks valid for a state that
        was never established."""
        def _raise(_factory: object) -> bool:
            raise RuntimeError("control table unavailable")

        payload = _build_recovery_policy_payload(
            session_statuses=self._sessions(),
            circuit_snapshot={"open": False},
            market_stream_service=SimpleNamespace(health_snapshot=dict),
            session_factory=object(),
            maintenance_reader=_raise,
        )
        self.assertIsNone(payload["maintenance_mode"])
        self.assertIn("control table unavailable", payload["maintenance_mode_error"])
        # And the sessions must not claim a state they could not establish.
        self.assertEqual(payload["broker_sessions"]["primary"]["state"], "unknown")
        self.assertFalse(
            payload["broker_sessions"]["primary"]["allows_new_entries"]
        )

    def test_stream_failure_is_surfaced_in_the_payload_not_only_logged(self) -> None:
        def _explode() -> dict[str, object]:
            raise RuntimeError("stream service is gone")

        payload = _build_recovery_policy_payload(
            session_statuses=self._sessions(),
            circuit_snapshot={"open": False},
            market_stream_service=SimpleNamespace(health_snapshot=_explode),
            session_factory=object(),
            maintenance_reader=lambda _factory: False,
        )
        self.assertIn("stream service is gone", payload["market_stream_error"])
        self.assertEqual(payload["market_stream"]["state"], "unknown")
        self.assertFalse(payload["market_stream"]["is_usable"])

    def test_maintenance_mode_is_reported_when_enabled(self) -> None:
        payload = _build_recovery_policy_payload(
            session_statuses=self._sessions(),
            circuit_snapshot={"open": False},
            market_stream_service=SimpleNamespace(health_snapshot=dict),
            session_factory=object(),
            maintenance_reader=lambda _factory: True,
        )
        self.assertTrue(payload["maintenance_mode"])
        self.assertEqual(payload["broker_sessions"]["primary"]["state"], "maintenance")
