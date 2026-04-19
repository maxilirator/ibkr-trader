from __future__ import annotations

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
