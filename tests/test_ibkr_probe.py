from __future__ import annotations

from datetime import UTC, datetime
from unittest import TestCase

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.ibkr.probe import GatewayProbeResult, probe_gateway


class _FakeSyncWrapper:
    def __init__(self, timeout: int) -> None:
        self.timeout = timeout
        self.connected = False
        self.disconnected = False
        self.connection_args: tuple[str, int, int] | None = None

    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool:
        self.connected = True
        self.connection_args = (host, port, client_id)
        return True

    def disconnect_and_stop(self) -> None:
        self.disconnected = True

    def get_current_time(self, *, timeout: int | None = None) -> int:
        return 1_710_000_000

    def get_next_valid_id(self, *, timeout: int | None = None) -> int:
        return 42


class ProbeTests(TestCase):
    def test_probe_gateway_uses_config_and_returns_snapshot(self) -> None:
        config = IbkrConnectionConfig(
            host="127.0.0.1",
            port=4002,
            client_id=0,
            account_id="DU1234567",
        )

        result = probe_gateway(
            config,
            timeout=7,
            sync_wrapper_cls=_FakeSyncWrapper,
        )

        self.assertEqual(result.host, "127.0.0.1")
        self.assertEqual(result.port, 4002)
        self.assertEqual(result.client_id, 0)
        self.assertEqual(result.next_valid_order_id, 42)
        self.assertEqual(
            result.broker_current_time,
            datetime.fromtimestamp(1_710_000_000, tz=UTC),
        )

    def test_probe_result_serializes_iso_timestamp(self) -> None:
        result = GatewayProbeResult(
            host="127.0.0.1",
            port=4002,
            client_id=0,
            broker_current_time=datetime(2026, 4, 10, 12, 30, tzinfo=UTC),
            next_valid_order_id=100,
        )

        payload = result.to_json()

        self.assertIn('"client_id": 0', payload)
        self.assertIn('"next_valid_order_id": 100', payload)
        self.assertIn("2026-04-10T12:30:00+00:00", payload)

