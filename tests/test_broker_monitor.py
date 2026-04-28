from __future__ import annotations

import unittest
from datetime import UTC
from datetime import datetime
from time import sleep

from ibkr_trader.api.broker_monitor import BrokerMonitorService
from ibkr_trader.ibkr.probe import GatewayProbeResult
from ibkr_trader.ibkr.runtime_snapshot import BrokerRuntimeSnapshot


class BrokerMonitorTests(unittest.TestCase):
    def test_run_cycle_records_heartbeat_and_snapshot_success(self) -> None:
        persisted: list[tuple[BrokerRuntimeSnapshot, datetime]] = []
        snapshot = BrokerRuntimeSnapshot(
            open_orders={},
            executions=(),
            portfolio=(),
            positions=(),
            account_values={
                "U25245596": {
                    "NetLiquidation": {"value": "100000.00", "currency": "USD"}
                }
            },
        )
        service = BrokerMonitorService(
            heartbeat_probe=lambda: GatewayProbeResult(
                host="127.0.0.1",
                port=4002,
                client_id=7,
                broker_current_time=datetime(2026, 4, 19, 21, 0, tzinfo=UTC),
                next_valid_order_id=101,
            ),
            snapshot_fetcher=lambda: snapshot,
            snapshot_persister=lambda current_snapshot, captured_at: persisted.append(
                (current_snapshot, captured_at)
            ),
            heartbeat_interval_seconds=30.0,
            snapshot_refresh_interval_seconds=60.0,
        )

        service.run_cycle()
        status = service.status()

        self.assertTrue(status.heartbeat.ok)
        self.assertEqual(
            status.heartbeat.broker_current_time,
            datetime(2026, 4, 19, 21, 0, tzinfo=UTC),
        )
        self.assertTrue(status.snapshot_refresh.ok)
        self.assertEqual(status.snapshot_refresh.account_count, 1)
        self.assertFalse(status.heartbeat.is_stale)
        self.assertFalse(status.snapshot_refresh.is_stale)
        self.assertIsNotNone(status.heartbeat.last_attempt_age_seconds)
        self.assertIsNotNone(status.snapshot_refresh.next_check_due_at)
        self.assertEqual(len(persisted), 1)

    def test_run_cycle_records_failures_without_fake_success(self) -> None:
        snapshot_calls = 0

        def snapshot_fetcher() -> BrokerRuntimeSnapshot:
            nonlocal snapshot_calls
            snapshot_calls += 1
            raise RuntimeError("snapshot failed")

        service = BrokerMonitorService(
            heartbeat_probe=lambda: (_ for _ in ()).throw(ConnectionError("heartbeat down")),
            snapshot_fetcher=snapshot_fetcher,
            snapshot_persister=lambda current_snapshot, captured_at: None,
            heartbeat_interval_seconds=30.0,
            snapshot_refresh_interval_seconds=60.0,
        )

        service.run_cycle()
        status = service.status()

        self.assertFalse(status.heartbeat.ok)
        self.assertEqual(status.heartbeat.error, "heartbeat down")
        self.assertFalse(status.snapshot_refresh.ok)
        self.assertEqual(
            status.snapshot_refresh.error,
            "Skipped broker snapshot refresh because the heartbeat probe failed.",
        )
        self.assertFalse(status.heartbeat.is_stale)
        self.assertFalse(status.snapshot_refresh.is_stale)
        self.assertEqual(snapshot_calls, 0)

    def test_request_cycle_if_due_throttles_non_blocking_refresh(self) -> None:
        call_count = 0
        snapshot = BrokerRuntimeSnapshot(
            open_orders={},
            executions=(),
            portfolio=(),
            positions=(),
            account_values={},
        )

        def heartbeat_probe() -> GatewayProbeResult:
            nonlocal call_count
            call_count += 1
            return GatewayProbeResult(
                host="127.0.0.1",
                port=4002,
                client_id=7,
                broker_current_time=datetime(2026, 4, 19, 21, 0, tzinfo=UTC),
                next_valid_order_id=101,
            )

        service = BrokerMonitorService(
            heartbeat_probe=heartbeat_probe,
            snapshot_fetcher=lambda: snapshot,
            snapshot_persister=lambda current_snapshot, captured_at: None,
            heartbeat_interval_seconds=30.0,
            snapshot_refresh_interval_seconds=60.0,
        )

        self.assertTrue(service.request_cycle_if_due(min_interval_seconds=60.0))
        for _ in range(20):
            if call_count > 0 and not service.status().refresh_in_flight:
                break
            sleep(0.01)

        self.assertEqual(call_count, 1)
        self.assertFalse(service.request_cycle_if_due(min_interval_seconds=60.0))
        self.assertEqual(call_count, 1)


if __name__ == "__main__":
    unittest.main()
