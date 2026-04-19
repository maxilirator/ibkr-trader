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

    def get_account_summary(
        self,
        tags: str,
        group: str = "All",
        timeout: int = 5,
    ) -> dict[str, dict[str, dict[str, str]]]:
        self.account_summary_args = (tags, group, timeout)
        return {
            "U25245596": {
                "NetLiquidation": {"value": "100000.00", "currency": "USD"},
                "TotalCashValue": {"value": "45000.00", "currency": "USD"},
                "BuyingPower": {"value": "200000.00", "currency": "USD"},
                "AvailableFunds": {"value": "150000.00", "currency": "USD"},
                "ExcessLiquidity": {"value": "149000.00", "currency": "USD"},
                "Cushion": {"value": "0.91", "currency": "USD"},
                "Currency": {"value": "USD", "currency": "USD"},
            },
            "U11111111": {
                "NetLiquidation": {"value": "50000.00", "currency": "SEK"},
                "TotalCashValue": {"value": "21000.00", "currency": "SEK"},
                "BuyingPower": {"value": "100000.00", "currency": "SEK"},
                "AvailableFunds": {"value": "80000.00", "currency": "SEK"},
                "ExcessLiquidity": {"value": "79000.00", "currency": "SEK"},
                "Cushion": {"value": "0.88", "currency": "SEK"},
                "Currency": {"value": "SEK", "currency": "SEK"},
            },
        }

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
                "contract": SimpleNamespace(
                    symbol="SAAB.B",
                    localSymbol="SAAB-B",
                    secType="STK",
                    exchange="SFB",
                    primaryExchange="SFB",
                    currency="SEK",
                ),
                "execution": SimpleNamespace(
                    execId="00014800.69ddd749.01.01",
                    orderId=2,
                    permId=600952471,
                    clientId=0,
                    acctNumber="U25245596",
                    orderRef="live-saab-buy-20260414-2",
                    side="BOT",
                    shares="1",
                    price="615.5",
                    exchange="SFB",
                    time="20260414  14:47:29",
                ),
            }
        ]

    def get_account_updates(self, account_code: str = "", timeout: int = 10) -> dict[str, object]:
        return {
            "portfolio": [
                {
                    "contract": SimpleNamespace(
                        symbol="MSFT",
                        localSymbol="MSFT",
                        secType="STK",
                        exchange="SMART",
                        primaryExchange="NASDAQ",
                        currency="USD",
                    ),
                    "accountName": "U25245596",
                    "position": "2",
                    "marketPrice": "410.50",
                    "marketValue": "821.00",
                    "averageCost": "401.25",
                    "unrealizedPNL": "18.50",
                    "realizedPNL": "0",
                }
            ],
            "account_values": {
                "U25245596": {
                    "NetLiquidation": {"value": "100000.00", "currency": "USD"},
                    "BuyingPower": {"value": "200000.00", "currency": "USD"},
                }
            },
        }

    def get_positions(self, timeout: int = 10) -> dict[str, list[object]]:
        return {
            "U25245596": [
                {
                    "contract": SimpleNamespace(
                        symbol="MSFT",
                        localSymbol="MSFT",
                        secType="STK",
                        exchange="SMART",
                        primaryExchange="NASDAQ",
                        currency="USD",
                    ),
                    "position": "2",
                    "avgCost": "401.25",
                }
            ]
        }


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
        self.assertEqual(serialized["executions"][0]["account"], "U25245596")
        self.assertEqual(serialized["executions"][0]["order_ref"], "live-saab-buy-20260414-2")
        self.assertEqual(serialized["executions"][0]["price"], "615.5")
        self.assertEqual(serialized["executions"][0]["currency"], "SEK")
        self.assertEqual(len(serialized["portfolio"]), 1)
        self.assertEqual(serialized["portfolio"][0]["market_value"], "821.00")
        self.assertEqual(len(serialized["positions"]), 1)
        self.assertEqual(serialized["positions"][0]["position"], "2")
        self.assertEqual(
            serialized["account_values"]["U25245596"]["NetLiquidation"]["value"],
            "100000.00",
        )
        self.assertEqual(
            serialized["account_values"]["U25245596"]["TotalCashValue"]["value"],
            "45000.00",
        )
        self.assertEqual(
            serialized["account_values"]["U11111111"]["NetLiquidation"]["currency"],
            "SEK",
        )
