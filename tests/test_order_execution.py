from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase

from ibkr_trader.api.server import parse_execution_batch_payload
from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.ibkr.order_execution import cancel_broker_order
from ibkr_trader.ibkr.order_execution import submit_order_from_batch


class _FakeContract:
    def __init__(self) -> None:
        self.symbol = ""
        self.secType = ""
        self.exchange = ""
        self.currency = ""
        self.includeExpired = False
        self.primaryExchange = ""
        self.localSymbol = ""
        self.secIdType = ""
        self.secId = ""


class _FakeOrder:
    def __init__(self) -> None:
        self.account = ""
        self.action = ""
        self.orderType = ""
        self.totalQuantity = Decimal("0")
        self.tif = ""
        self.outsideRth = False
        self.transmit = False
        self.orderRef = ""
        self.lmtPrice = None


class _FakeOrderExecutionSyncWrapper:
    def __init__(self, timeout: int) -> None:
        self.timeout = timeout
        self.connected = False
        self.disconnected = False
        self.placed_orders: list[tuple[object, object, int | None]] = []
        self.cancelled_orders: list[tuple[int, int]] = []
        self.open_orders: dict[int, object] = {}

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
        return {
            "DU1234567": {
                "NetLiquidation": {"value": "100000.00", "currency": "USD"},
                "BuyingPower": {"value": "200000.00", "currency": "USD"},
                "AvailableFunds": {"value": "100000.00", "currency": "USD"},
                "ExcessLiquidity": {"value": "100000.00", "currency": "USD"},
                "AccountType": {"value": "INDIVIDUAL", "currency": ""},
            }
        }

    def get_contract_details(self, contract: _FakeContract, timeout: int | None = None) -> list[object]:
        return [
            SimpleNamespace(
                contract=SimpleNamespace(
                    conId=265598,
                    symbol=contract.symbol,
                    localSymbol=contract.symbol,
                    secType=contract.secType,
                    exchange="SMART",
                    primaryExchange=contract.primaryExchange or "NASDAQ",
                    currency=contract.currency,
                    tradingClass=contract.symbol,
                ),
                marketName="NMS",
                minTick=0.01,
                validExchanges="SMART,NASDAQ",
                orderTypes="ACTIVETIM,ADJUST,ALERT,LMT,MKT",
                timeZoneId="US/Eastern",
                tradingHours="20260410:093000-160000",
                liquidHours="20260410:093000-160000",
                stockType="COMMON",
                industry="Technology",
                category="Computers",
                subcategory="Computers",
                longName="APPLE INC",
                secIdList=[SimpleNamespace(tag="ISIN", value="US0378331005")],
            )
        ]

    def get_historical_data(
        self,
        contract: _FakeContract,
        end_date_time: str,
        duration_str: str,
        bar_size_setting: str,
        what_to_show: str,
        use_rth: bool = True,
        format_date: int = 1,
        timeout: int | None = None,
    ) -> list[object]:
        raise AssertionError("FX data should not be requested for target_quantity sizing.")

    def place_order_sync(self, contract: object, order: object, timeout: int | None = None) -> dict[str, object]:
        self.placed_orders.append((contract, order, timeout))
        order.orderId = 17
        self.open_orders[17] = {
            "orderId": 17,
            "contract": contract,
            "order": order,
            "orderState": SimpleNamespace(
                status="Inactive",
                warningText="Order held in TWS pending manual transmit.",
                rejectReason="",
                completedStatus="",
                completedTime="",
            ),
        }
        return {
            "orderId": 17,
            "status": "Submitted",
            "filled": "0",
            "remaining": str(order.totalQuantity),
            "avgFillPrice": "0",
            "permId": 9001,
            "parentId": 0,
            "lastFillPrice": "0",
            "clientId": 0,
            "whyHeld": "",
            "mktCapPrice": "0",
        }

    def cancel_order_sync(
        self,
        order_id: int,
        orderCancel: object | None = None,
        timeout: int = 3,
    ) -> dict[str, object]:
        self.cancelled_orders.append((order_id, timeout))
        return {
            "orderId": order_id,
            "status": "Cancelled",
            "filled": "0",
            "remaining": "10",
            "avgFillPrice": "0",
            "permId": 9001,
            "parentId": 0,
            "lastFillPrice": "0",
            "clientId": 0,
            "whyHeld": "",
            "mktCapPrice": "0",
        }


def _base_payload() -> dict[str, object]:
    return {
        "schema_version": "2026-04-10",
        "source": {
            "system": "q-training",
            "batch_id": "batch-1",
            "generated_at": "2026-04-10T02:15:44Z",
        },
        "instructions": [
            {
                "instruction_id": "ny-paper-1",
                "account": {
                    "account_key": "GTW05",
                    "book_key": "long_risk_book",
                },
                "instrument": {
                    "symbol": "AAPL",
                    "security_type": "STK",
                    "exchange": "SMART",
                    "currency": "USD",
                    "primary_exchange": "NASDAQ",
                },
                "intent": {
                    "side": "BUY",
                    "position_side": "LONG",
                },
                "sizing": {
                    "mode": "target_quantity",
                    "target_quantity": "10",
                },
                "entry": {
                    "order_type": "LIMIT",
                    "submit_at": "2026-04-10T09:35:00-04:00",
                    "expire_at": "2026-04-10T15:55:00-04:00",
                    "limit_price": "120.00",
                },
                "exit": {
                    "take_profit_pct": "0.02",
                },
                "trace": {
                    "reason_code": "manual-paper-smoke",
                },
            }
        ],
    }


class OrderExecutionTests(TestCase):
    def setUp(self) -> None:
        self.config = IbkrConnectionConfig(
            host="127.0.0.1",
            port=7497,
            client_id=0,
            diagnostic_client_id=7,
            account_id="DU1234567",
        )

    def test_submit_order_from_batch_builds_limit_order(self) -> None:
        batch = parse_execution_batch_payload(_base_payload())

        result = submit_order_from_batch(
            self.config,
            batch,
            sync_wrapper_cls=_FakeOrderExecutionSyncWrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
            order_cls=_FakeOrder,
        )

        self.assertEqual(result["instruction_id"], "ny-paper-1")
        self.assertEqual(result["account"], "DU1234567")
        self.assertEqual(result["resolved_contract"]["con_id"], 265598)
        self.assertEqual(result["order"]["action"], "BUY")
        self.assertEqual(result["order"]["order_type"], "LMT")
        self.assertEqual(result["order"]["time_in_force"], "DAY")
        self.assertEqual(result["order"]["limit_price"], "120.00")
        self.assertEqual(result["order"]["total_quantity"], "10")
        self.assertEqual(result["broker_order_status"]["status"], "Submitted")
        self.assertEqual(result["tws_submission"]["source"], "openOrder")
        self.assertEqual(result["tws_submission"]["order_state"]["status"], "Inactive")
        self.assertEqual(
            result["tws_submission"]["order_state"]["warning_text"],
            "Order held in TWS pending manual transmit.",
        )

    def test_submit_order_from_batch_rejects_fractional_stock_quantity(self) -> None:
        payload = _base_payload()
        payload["instructions"][0]["sizing"]["target_quantity"] = "10.5"
        batch = parse_execution_batch_payload(payload)

        with self.assertRaisesRegex(ValueError, "integral share quantity"):
            submit_order_from_batch(
                self.config,
                batch,
                sync_wrapper_cls=_FakeOrderExecutionSyncWrapper,
                response_timeout_cls=TimeoutError,
                contract_cls=_FakeContract,
                order_cls=_FakeOrder,
            )

    def test_cancel_broker_order_returns_cancel_status(self) -> None:
        result = cancel_broker_order(
            self.config,
            17,
            sync_wrapper_cls=_FakeOrderExecutionSyncWrapper,
            response_timeout_cls=TimeoutError,
        )

        self.assertEqual(result["broker_order_status"]["orderId"], 17)
        self.assertEqual(result["broker_order_status"]["status"], "Cancelled")
