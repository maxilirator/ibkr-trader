from __future__ import annotations

import time
from types import SimpleNamespace
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

    def test_drain_broker_callback_events_records_live_order_callbacks(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)

        app.openOrder(
            11,
            SimpleNamespace(
                symbol="AAPL",
                localSymbol="AAPL",
                secType="STK",
                exchange="SMART",
                primaryExchange="NASDAQ",
                currency="USD",
            ),
            SimpleNamespace(
                permId=9001,
                clientId=0,
                account="DU1234567",
                orderRef="sync-aapl-1",
                action="BUY",
                totalQuantity="1",
                orderType="LMT",
                lmtPrice="200.00",
                auxPrice="",
                outsideRth=False,
                transmit=True,
            ),
            SimpleNamespace(
                status="PreSubmitted",
                warningText="Held in TWS.",
                rejectReason="",
                completedStatus="",
                completedTime="",
            ),
        )
        app.orderStatus(
            11,
            "Submitted",
            "0",
            "1",
            0.0,
            9001,
            0,
            0.0,
            0,
            "",
            0.0,
        )
        app.error(11, 0, 202, "Rejected by exchange", '{"reason":"test"}')

        events = app.drain_broker_callback_events()

        self.assertEqual(
            [item["event_type"] for item in events],
            ["open_order", "order_status", "order_error"],
        )
        self.assertEqual(events[0]["order"]["order_ref"], "sync-aapl-1")
        self.assertEqual(events[1]["order_status"]["status"], "Submitted")
        self.assertEqual(events[2]["error"]["errorCode"], 202)
        self.assertEqual(app.drain_broker_callback_events(), [])

    def test_suppressed_open_order_callbacks_do_not_hit_journal(self) -> None:
        wrapper_cls = load_sync_wrapper_class()
        app = wrapper_cls(timeout=1)

        with app._suppress_broker_callback_events("open_order", "order_status"):
            app.openOrder(
                11,
                SimpleNamespace(
                    symbol="AAPL",
                    localSymbol="AAPL",
                    secType="STK",
                    exchange="SMART",
                    primaryExchange="NASDAQ",
                    currency="USD",
                ),
                SimpleNamespace(
                    permId=9001,
                    clientId=0,
                    account="DU1234567",
                    orderRef="sync-aapl-1",
                    action="BUY",
                    totalQuantity="1",
                    orderType="LMT",
                    lmtPrice="200.00",
                    auxPrice="",
                    outsideRth=False,
                    transmit=True,
                ),
                SimpleNamespace(
                    status="PreSubmitted",
                    warningText="Held in TWS.",
                    rejectReason="",
                    completedStatus="",
                    completedTime="",
                ),
            )
            app.orderStatus(
                11,
                "Submitted",
                "0",
                "1",
                0.0,
                9001,
                0,
                0.0,
                0,
                "",
                0.0,
            )

        self.assertEqual(app.drain_broker_callback_events(), [])
