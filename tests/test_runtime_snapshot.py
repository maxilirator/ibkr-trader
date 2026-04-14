from __future__ import annotations

from types import SimpleNamespace
from unittest import TestCase

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.ibkr.runtime_snapshot import fetch_broker_runtime_snapshot
from ibkr_trader.ibkr.runtime_snapshot import serialize_broker_runtime_snapshot


class _FakeRuntimeSnapshotSyncWrapper:
    def __init__(self, timeout: int) -> None:
        self.timeout = timeout
        self.connected = False
        self.disconnected = False

    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool:
        self.connected = True
        self.connection_args = (host, port, client_id)
        return True

    def disconnect_and_stop(self) -> None:
        self.disconnected = True

    def get_open_orders(self, timeout: int = 3) -> dict[int, object]:
        return {
            17: {
                "orderId": 17,
                "contract": SimpleNamespace(
                    symbol="MSFT",
                    localSymbol="MSFT",
                    secType="STK",
                    exchange="SMART",
                    primaryExchange="NASDAQ",
                    currency="USD",
                ),
                "order": SimpleNamespace(
                    permId=9001,
                    clientId=0,
                    account="U25245596",
                    orderRef="runtime-msft-1",
                    action="BUY",
                    totalQuantity="1",
                    orderType="LMT",
                    lmtPrice="405.00",
                    auxPrice="",
                    outsideRth=False,
                    transmit=True,
                ),
                "orderState": SimpleNamespace(
                    status="Inactive",
                    warningText="Order held in TWS pending manual transmit.",
                    rejectReason="",
                    completedStatus="",
                    completedTime="",
                ),
            }
        }

    def get_executions(self, exec_filter: object | None = None, timeout: int = 10) -> list[object]:
        return [
            {
                "contract": SimpleNamespace(symbol="SAAB.B"),
                "execution": SimpleNamespace(
                    execId="00014800.69ddd749.01.01",
                    orderId=2,
                    permId=600952471,
                    clientId=0,
                    orderRef="live-saab-buy-20260414-2",
                    side="BOT",
                    shares="1",
                    price="615.5",
                    exchange="SFB",
                    time="20260414  14:47:29",
                ),
            }
        ]


class RuntimeSnapshotTests(TestCase):
    def test_fetch_broker_runtime_snapshot_serializes_open_orders_and_executions(self) -> None:
        config = IbkrConnectionConfig(
            host="127.0.0.1",
            port=7496,
            client_id=0,
            diagnostic_client_id=7,
            account_id="U25245596",
        )

        snapshot = fetch_broker_runtime_snapshot(
            config,
            sync_wrapper_cls=_FakeRuntimeSnapshotSyncWrapper,
            response_timeout_cls=TimeoutError,
        )
        serialized = serialize_broker_runtime_snapshot(snapshot)

        self.assertEqual(len(serialized["open_orders"]), 1)
        self.assertEqual(serialized["open_orders"][0]["order_ref"], "runtime-msft-1")
        self.assertEqual(serialized["open_orders"][0]["warning_text"], "Order held in TWS pending manual transmit.")
        self.assertEqual(serialized["open_orders"][0]["transmit"], True)
        self.assertEqual(len(serialized["executions"]), 1)
        self.assertEqual(serialized["executions"][0]["order_ref"], "live-saab-buy-20260414-2")
        self.assertEqual(serialized["executions"][0]["price"], "615.5")
