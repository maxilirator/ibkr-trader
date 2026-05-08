from __future__ import annotations

import threading
import time
from unittest import TestCase

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.ibkr.session_manager import ManagedSyncSession


class _FakeSyncWrapper:
    instances: list["_FakeSyncWrapper"] = []

    def __init__(self, timeout: int) -> None:
        self.timeout = timeout
        self.connected = False
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.connection_args: tuple[str, int, int] | None = None
        self.__class__.instances.append(self)

    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool:
        self.connect_calls += 1
        self.connected = True
        self.connection_args = (host, port, client_id)
        return True

    def disconnect_and_stop(self) -> None:
        self.disconnect_calls += 1
        self.connected = False

    def isConnected(self) -> bool:  # noqa: N802
        return self.connected

    def drain_broker_callback_events(self) -> list[dict[str, object]]:
        return [{"event_type": "order_status"}]


class _FailingSyncWrapper(_FakeSyncWrapper):
    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool:
        self.connect_calls += 1
        self.connected = False
        self.connection_args = (host, port, client_id)
        return False


class _FailingSyncWrapperWithReason(_FailingSyncWrapper):
    last_connect_failure_reason = "socket connected but API startup did not return nextValidId"


class SessionManagerTests(TestCase):
    def setUp(self) -> None:
        _FakeSyncWrapper.instances = []

    def test_checkout_reuses_single_connected_wrapper(self) -> None:
        session = ManagedSyncSession(
            "diagnostic",
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4001,
                client_id=7,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            wrapper_cls=_FakeSyncWrapper,
        )

        with session.checkout() as first_app:
            self.assertTrue(first_app.isConnected())
        with session.checkout() as second_app:
            self.assertIs(first_app, second_app)

        self.assertEqual(len(_FakeSyncWrapper.instances), 1)
        self.assertEqual(_FakeSyncWrapper.instances[0].connect_calls, 1)
        self.assertEqual(_FakeSyncWrapper.instances[0].disconnect_calls, 0)

        session.disconnect()
        self.assertEqual(_FakeSyncWrapper.instances[0].disconnect_calls, 1)

    def test_status_reports_connection_state(self) -> None:
        session = ManagedSyncSession(
            "primary",
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4001,
                client_id=0,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            wrapper_cls=_FakeSyncWrapper,
        )

        disconnected = session.status()
        self.assertFalse(disconnected.connected)

        session.warmup()
        connected = session.status()
        self.assertTrue(connected.connected)
        self.assertEqual(connected.client_id, 0)
        self.assertEqual(connected.metrics.connect_attempt_count, 1)
        self.assertEqual(connected.metrics.connect_success_count, 1)

    def test_execute_tracks_operation_telemetry(self) -> None:
        from ibkr_trader.ibkr.session_manager import BrokerActivityTracker

        tracker = BrokerActivityTracker()
        session = ManagedSyncSession(
            "diagnostic",
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4001,
                client_id=7,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            wrapper_cls=_FakeSyncWrapper,
            activity_tracker=tracker,
        )

        result = session.execute("probe", lambda app: app.connection_args)

        self.assertEqual(result, ("127.0.0.1", 4001, 7))
        status = session.status()
        self.assertEqual(status.metrics.checkout_count, 1)
        self.assertEqual(status.metrics.failed_checkout_count, 0)
        telemetry = tracker.snapshot(recent_limit=5)
        self.assertEqual(telemetry["total_operations"], 1)
        self.assertEqual(telemetry["successful_operations"], 1)
        self.assertEqual(telemetry["per_operation"]["probe"]["total"], 1)

    def test_execute_tracks_failures(self) -> None:
        from ibkr_trader.ibkr.session_manager import BrokerActivityTracker

        tracker = BrokerActivityTracker()
        session = ManagedSyncSession(
            "diagnostic",
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4001,
                client_id=7,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            wrapper_cls=_FakeSyncWrapper,
            activity_tracker=tracker,
        )

        with self.assertRaisesRegex(RuntimeError, "boom"):
            session.execute("probe", lambda app: (_ for _ in ()).throw(RuntimeError("boom")))

        status = session.status()
        self.assertEqual(status.metrics.checkout_count, 1)
        self.assertEqual(status.metrics.failed_checkout_count, 1)
        telemetry = tracker.snapshot(recent_limit=5)
        self.assertEqual(telemetry["failed_operations"], 1)
        self.assertEqual(telemetry["per_operation"]["probe"]["failure"], 1)

    def test_broker_timeout_disconnects_poisoned_session(self) -> None:
        session = ManagedSyncSession(
            "diagnostic",
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4001,
                client_id=7,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            wrapper_cls=_FakeSyncWrapper,
            initial_connect_backoff_seconds=60,
            max_connect_backoff_seconds=60,
        )

        with self.assertRaisesRegex(TimeoutError, "stuck"):
            session.execute(
                "historical_bars",
                lambda app: (_ for _ in ()).throw(TimeoutError("stuck")),
            )

        wrapper = _FakeSyncWrapper.instances[0]
        self.assertEqual(wrapper.disconnect_calls, 1)
        status = session.status()
        self.assertFalse(status.connected)
        self.assertEqual(status.consecutive_failures, 1)
        self.assertIsNotNone(status.cooldown_until)
        self.assertEqual(status.last_error, "stuck")

    def test_drain_broker_callback_events_uses_managed_session(self) -> None:
        session = ManagedSyncSession(
            "primary",
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4001,
                client_id=0,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            wrapper_cls=_FakeSyncWrapper,
        )

        events = session.drain_broker_callback_events()

        self.assertEqual(events, [{"event_type": "order_status"}])
        status = session.status()
        self.assertEqual(status.metrics.checkout_count, 1)

    def test_passive_callback_drain_does_not_connect_disconnected_session(self) -> None:
        session = ManagedSyncSession(
            "primary",
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4001,
                client_id=0,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            wrapper_cls=_FakeSyncWrapper,
        )

        events = session.drain_broker_callback_events(connect_if_needed=False)

        self.assertEqual(events, [])
        self.assertEqual(_FakeSyncWrapper.instances, [])
        status = session.status()
        self.assertEqual(status.metrics.connect_attempt_count, 0)
        self.assertEqual(status.metrics.checkout_count, 0)

    def test_passive_callback_drain_uses_existing_connected_session(self) -> None:
        session = ManagedSyncSession(
            "primary",
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4001,
                client_id=0,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            wrapper_cls=_FakeSyncWrapper,
        )
        session.warmup()

        events = session.drain_broker_callback_events(connect_if_needed=False)

        self.assertEqual(events, [{"event_type": "order_status"}])
        self.assertEqual(len(_FakeSyncWrapper.instances), 1)
        self.assertEqual(_FakeSyncWrapper.instances[0].connect_calls, 1)
        status = session.status()
        self.assertEqual(status.metrics.checkout_count, 0)

    def test_failed_connect_enters_cooldown_without_repeated_socket_attempts(self) -> None:
        session = ManagedSyncSession(
            "primary",
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4001,
                client_id=0,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            wrapper_cls=_FailingSyncWrapper,
            initial_connect_backoff_seconds=60,
            max_connect_backoff_seconds=60,
        )

        with self.assertRaisesRegex(ConnectionError, "Failed to connect"):
            session.execute("probe", lambda app: app.connection_args)
        with self.assertRaisesRegex(ConnectionError, "cooling down"):
            session.execute("probe", lambda app: app.connection_args)

        status = session.status()
        self.assertFalse(status.connected)
        self.assertEqual(status.metrics.connect_attempt_count, 1)
        self.assertEqual(status.consecutive_failures, 1)
        self.assertIsNotNone(status.cooldown_until)
        self.assertIsNotNone(status.cooldown_seconds_remaining)
        self.assertIn("Failed to connect", status.last_error or "")
        self.assertNotIn("cooling down after", status.last_error or "")

    def test_failed_connect_reports_wrapper_failure_reason(self) -> None:
        session = ManagedSyncSession(
            "diagnostic",
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4001,
                client_id=7,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            wrapper_cls=_FailingSyncWrapperWithReason,
            initial_connect_backoff_seconds=60,
            max_connect_backoff_seconds=60,
            gateway_diagnostics_reader=lambda: {},
        )

        with self.assertRaisesRegex(ConnectionError, "nextValidId"):
            session.execute("heartbeat_probe", lambda app: app.connection_args)

        status = session.status()
        self.assertEqual(
            status.last_error,
            "socket connected but API startup did not return nextValidId",
        )

    def test_next_valid_id_failure_uses_stuck_gateway_slow_probe_circuit(self) -> None:
        session = ManagedSyncSession(
            "diagnostic",
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4001,
                client_id=7,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            wrapper_cls=_FailingSyncWrapperWithReason,
            initial_connect_backoff_seconds=5,
            max_connect_backoff_seconds=60,
            api_startup_failure_slow_probe_seconds=900,
            gateway_diagnostics_reader=lambda: {
                "status": "stuck_shutdown_after_existing_session",
                "severity": "bad",
                "summary": (
                    "IB Gateway is stuck shutting down after an "
                    "existing-session conflict."
                ),
                "latest_dialog": "Shutdown progress",
                "latest_event_at": "2026-05-08T13:50:57+00:00",
                "existing_session_detected_at": "2026-05-08T13:50:57+00:00",
                "existing_session_action": "Click button: Cancel",
                "configured_existing_session_action": "primaryoverride",
            },
        )

        with self.assertRaisesRegex(ConnectionError, "IB Gateway diagnostic"):
            session.execute("heartbeat_probe", lambda app: app.connection_args)

        status = session.status()
        self.assertFalse(status.connected)
        self.assertIn("IB Gateway diagnostic", status.last_error or "")
        self.assertIn("stuck_shutdown_after_existing_session", status.circuit_breaker_reason or "")
        self.assertIsNotNone(status.circuit_breaker_until)
        self.assertIsNotNone(status.cooldown_seconds_remaining)
        self.assertGreaterEqual(status.cooldown_seconds_remaining or 0, 800)

    def test_execute_can_ignore_cooldown_for_explicit_health_checks(self) -> None:
        session = ManagedSyncSession(
            "diagnostic",
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4001,
                client_id=7,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            wrapper_cls=_FailingSyncWrapper,
            initial_connect_backoff_seconds=60,
            max_connect_backoff_seconds=60,
        )

        with self.assertRaisesRegex(ConnectionError, "Failed to connect"):
            session.execute("probe", lambda app: app.connection_args)
        with self.assertRaisesRegex(ConnectionError, "cooling down"):
            session.execute("probe", lambda app: app.connection_args)
        with self.assertRaisesRegex(ConnectionError, "Failed to connect"):
            session.execute(
                "heartbeat_probe",
                lambda app: app.connection_args,
                ignore_cooldown=True,
            )

        status = session.status()
        self.assertEqual(status.metrics.connect_attempt_count, 2)
        self.assertEqual(status.consecutive_failures, 2)
        self.assertIn("Failed to connect", status.last_error or "")
        self.assertNotIn("cooling down after", status.last_error or "")

    def test_execute_serializes_access_to_shared_session(self) -> None:
        session = ManagedSyncSession(
            "primary",
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4001,
                client_id=0,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            wrapper_cls=_FakeSyncWrapper,
        )

        state_lock = threading.Lock()
        inflight = 0
        max_inflight = 0
        start_second = threading.Event()
        finish_first = threading.Event()

        def operation_one(app: object) -> str:
            nonlocal inflight, max_inflight
            with state_lock:
                inflight += 1
                max_inflight = max(max_inflight, inflight)
            start_second.set()
            finish_first.wait(timeout=2)
            with state_lock:
                inflight -= 1
            return "first"

        def operation_two(app: object) -> str:
            nonlocal inflight, max_inflight
            with state_lock:
                inflight += 1
                max_inflight = max(max_inflight, inflight)
            with state_lock:
                inflight -= 1
            return "second"

        first_result: list[str] = []
        second_result: list[str] = []

        first_thread = threading.Thread(
            target=lambda: first_result.append(session.execute("first", operation_one))
        )
        second_thread = threading.Thread(
            target=lambda: second_result.append(session.execute("second", operation_two))
        )

        first_thread.start()
        start_second.wait(timeout=2)
        second_thread.start()
        time.sleep(0.1)
        finish_first.set()

        first_thread.join(timeout=2)
        second_thread.join(timeout=2)

        self.assertEqual(first_result, ["first"])
        self.assertEqual(second_result, ["second"])
        self.assertEqual(max_inflight, 1)
