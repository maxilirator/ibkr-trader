from __future__ import annotations

from unittest import TestCase

from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.orchestration.runtime_service_state import (
    RuntimeServiceLeaseError,
    acquire_runtime_service_lease,
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
        renewed = renew_runtime_service_lease(
            self.session_factory,
            runtime_key="EXECUTION_RUNTIME",
            owner_token="token-1",
            lease_seconds=30,
        )
        self.assertEqual(renewed.owner_label, "quant:1001")

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
