from __future__ import annotations

import unittest
from datetime import UTC
from datetime import datetime

from ibkr_trader.api.broker_monitor import BrokerMonitorService
from ibkr_trader.config import IbkrConnectionConfig
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
        self.assertEqual(len(persisted), 1)

    def test_run_cycle_records_failures_without_fake_success(self) -> None:
        service = BrokerMonitorService(
            heartbeat_probe=lambda: (_ for _ in ()).throw(ConnectionError("heartbeat down")),
            snapshot_fetcher=lambda: (_ for _ in ()).throw(RuntimeError("snapshot failed")),
            snapshot_persister=lambda current_snapshot, captured_at: None,
            heartbeat_interval_seconds=30.0,
            snapshot_refresh_interval_seconds=60.0,
        )

        service.run_cycle()
        status = service.status()

        self.assertFalse(status.heartbeat.ok)
        self.assertEqual(status.heartbeat.error, "heartbeat down")
        self.assertFalse(status.snapshot_refresh.ok)
        self.assertEqual(status.snapshot_refresh.error, "snapshot failed")


if __name__ == "__main__":
    unittest.main()
