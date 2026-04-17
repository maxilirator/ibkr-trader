from __future__ import annotations

import time
from unittest import TestCase

from ibkr_trader.ibkr.sync_wrapper import load_sync_wrapper_class


class SyncWrapperTests(TestCase):
    def test_connect_and_start_waits_for_next_valid_id(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)
        connect_calls: list[tuple[str, int, int]] = []

        def fake_connect(host: str, port: int, client_id: int) -> None:
            connect_calls.append((host, port, client_id))

        def fake_is_connected() -> bool:
            return True

        def fake_run() -> None:
            time.sleep(0.05)
            app.next_valid_id_value = 101

        app.connect = fake_connect  # type: ignore[method-assign]
        app.isConnected = fake_is_connected  # type: ignore[method-assign]
        app.run = fake_run  # type: ignore[method-assign]

        connected = app.connect_and_start("127.0.0.1", 4001, 7)

        self.assertTrue(connected)
        self.assertEqual(connect_calls, [("127.0.0.1", 4001, 7)])
        self.assertEqual(app.next_valid_id_value, 101)

        app.disconnect_and_stop()

    def test_connect_and_start_fails_when_next_valid_id_never_arrives(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)
        disconnect_calls = 0

        def fake_connect(host: str, port: int, client_id: int) -> None:
            return None

        def fake_is_connected() -> bool:
            return True

        def fake_run() -> None:
            time.sleep(0.05)

        def fake_disconnect_and_stop() -> None:
            nonlocal disconnect_calls
            disconnect_calls += 1

        app.connect = fake_connect  # type: ignore[method-assign]
        app.isConnected = fake_is_connected  # type: ignore[method-assign]
        app.run = fake_run  # type: ignore[method-assign]
        app.disconnect_and_stop = fake_disconnect_and_stop  # type: ignore[method-assign]

        connected = app.connect_and_start("127.0.0.1", 4001, 7)

        self.assertFalse(connected)
        self.assertEqual(disconnect_calls, 1)
