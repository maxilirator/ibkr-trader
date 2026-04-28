from __future__ import annotations

from datetime import timedelta
from unittest import TestCase

from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import RuntimeServiceRecord
from ibkr_trader.orchestration.runtime_service_state import (
    RuntimeServiceLeaseError,
    acquire_runtime_service_lease,
    mark_runtime_service_disabled,
    mark_runtime_service_stopped,
    read_runtime_service_status,
    renew_runtime_service_lease,
)


class RuntimeServiceStateTests(TestCase):
    def setUp(self) -> None:
        self.engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(self.engine)
        self.session_factory = create_session_factory(self.engine)

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_runtime_service_lease_round_trips(self) -> None:
        snapshot = acquire_runtime_service_lease(
            self.session_factory,
            runtime_key="EXECUTION_RUNTIME",
            service_type="execution",
            owner_token="token-1",
            owner_label="quant:1001",
            hostname="quant",
            pid=1001,
            runtime_timezone="Europe/Stockholm",
            broker_kind="IBKR",
            broker_client_id=0,
            lease_seconds=30,
            metadata_json={"interval_seconds": 5},
        )

        self.assertEqual(snapshot.status, "STARTING")
        self.assertEqual(snapshot.effective_status, "STARTING")
        self.assertFalse(snapshot.is_stale)
        renewed = renew_runtime_service_lease(
            self.session_factory,
            runtime_key="EXECUTION_RUNTIME",
            owner_token="token-1",
            lease_seconds=30,
        )
        self.assertEqual(renewed.owner_label, "quant:1001")
        self.assertIsNotNone(renewed.heartbeat_age_seconds)
        self.assertGreaterEqual(renewed.lease_seconds_remaining or 0, 0)

        stopped = mark_runtime_service_stopped(
            self.session_factory,
            runtime_key="EXECUTION_RUNTIME",
            owner_token="token-1",
            note="Stopped from test.",
        )
        self.assertEqual(stopped.status, "STOPPED")
        self.assertIsNone(stopped.owner_token)

        latest = read_runtime_service_status(
            self.session_factory,
            runtime_key="EXECUTION_RUNTIME",
        )
        self.assertEqual(latest.status, "STOPPED")
        self.assertEqual(latest.effective_status, "STOPPED")

    def test_runtime_service_status_marks_expired_active_lease_stale(self) -> None:
        acquire_runtime_service_lease(
            self.session_factory,
            runtime_key="EXECUTION_RUNTIME",
            service_type="execution",
            owner_token="token-1",
            owner_label="quant:1001",
            hostname="quant",
            pid=1001,
            runtime_timezone="Europe/Stockholm",
            broker_kind="IBKR",
            broker_client_id=0,
            lease_seconds=30,
        )
        with session_scope(self.session_factory) as session:
            record = session.query(RuntimeServiceRecord).filter_by(
                runtime_key="EXECUTION_RUNTIME"
            ).one()
            record.status = "RUNNING"
            record.lease_expires_at = utc_now() - timedelta(seconds=1)

        latest = read_runtime_service_status(
            self.session_factory,
            runtime_key="EXECUTION_RUNTIME",
        )

        self.assertEqual(latest.status, "RUNNING")
        self.assertEqual(latest.effective_status, "STALE")
        self.assertTrue(latest.is_stale)
        self.assertLess(latest.lease_seconds_remaining or 0, 0)

    def test_runtime_service_rejects_conflicting_live_owner(self) -> None:
        acquire_runtime_service_lease(
            self.session_factory,
            runtime_key="EXECUTION_RUNTIME",
            service_type="execution",
            owner_token="token-1",
            owner_label="quant:1001",
            hostname="quant",
            pid=1001,
            runtime_timezone="Europe/Stockholm",
            broker_kind="IBKR",
            broker_client_id=0,
            lease_seconds=30,
        )

        with self.assertRaises(RuntimeServiceLeaseError):
            acquire_runtime_service_lease(
                self.session_factory,
                runtime_key="EXECUTION_RUNTIME",
                service_type="execution",
                owner_token="token-2",
                owner_label="quant:1002",
                hostname="quant",
                pid=1002,
                runtime_timezone="Europe/Stockholm",
                broker_kind="IBKR",
                broker_client_id=0,
                lease_seconds=30,
            )

    def test_runtime_service_can_be_marked_disabled_without_live_owner(self) -> None:
        acquire_runtime_service_lease(
            self.session_factory,
            runtime_key="EXECUTION_RUNTIME",
            service_type="execution",
            owner_token="token-1",
            owner_label="quant:1001",
            hostname="quant",
            pid=1001,
            runtime_timezone="Europe/Stockholm",
            broker_kind="IBKR",
            broker_client_id=0,
            lease_seconds=30,
        )

        disabled = mark_runtime_service_disabled(
            self.session_factory,
            runtime_key="EXECUTION_RUNTIME",
            note="Disabled from test.",
        )

        self.assertEqual(disabled.status, "DISABLED")
        self.assertIsNone(disabled.owner_token)
        self.assertIsNone(disabled.last_error)
        self.assertTrue(disabled.metadata_json["disabled"])
