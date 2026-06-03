from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase

from ibkr_trader.api.server import parse_execution_batch_payload
from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.domain.execution_contract import OrderType
from ibkr_trader.ibkr.order_execution import cancel_broker_order
from ibkr_trader.ibkr.order_execution import submit_exit_order_from_instruction
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
        self.auxPrice = None
        self.ocaGroup = None
        self.ocaType = None
        self.whatIf = False


class _FakeOrderExecutionSyncWrapper:
    total_cash_value = "100000.00"

    def __init__(self, timeout: int) -> None:
        self.timeout = timeout
        self.connected = False
        self.disconnected = False
        self.placed_orders: list[tuple[object, object, int | None]] = []
        self.previewed_orders: list[tuple[object, object, int | None]] = []
        self.cancelled_orders: list[tuple[int, int]] = []
        self.open_orders: dict[int, object] = {}
        self.errors: dict[int, list[dict[str, object]]] = {}
        self.wire_audit_events: list[dict[str, object]] = []
        self._next_order_id = 17

    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool:
        self.connected = True
        self.connection_args = (host, port, client_id)
        return True

    def disconnect_and_stop(self) -> None:
        self.disconnected = True

    def get_account_updates(
        self,
        account_code: str = "",
        timeout: int = 10,
    ) -> dict[str, object]:
        return {
            "portfolio": [],
            "account_values": {
                "DU1234567": {
                    "NetLiquidation": {"value": "100000.00", "currency": "USD"},
                    "TotalCashValue": {"value": self.total_cash_value, "currency": "USD"},
                    "BuyingPower": {"value": "200000.00", "currency": "USD"},
                    "AvailableFunds": {"value": "100000.00", "currency": "USD"},
                    "ExcessLiquidity": {"value": "100000.00", "currency": "USD"},
                    "AccountType": {"value": "INDIVIDUAL", "currency": ""},
                }
            },
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
                marketRuleIds="26,26",
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

    def get_market_rule(self, market_rule_id: int, timeout: int = 5) -> list[object]:
        return [SimpleNamespace(lowEdge=0, increment=0.01)]

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

    def get_positions(self, timeout: int = 10) -> dict[str, list[object]]:
        return {"DU1234567": []}

    def broker_wire_audit_event_count(self) -> int:
        return len(self.wire_audit_events)

    def broker_wire_audit_events_since(self, offset: int = 0) -> list[dict[str, object]]:
        return self.wire_audit_events[offset:]

    def _append_wire_audit(self, stage: str, contract: object, order: object) -> None:
        self.wire_audit_events.append(
            {
                "event_type": "outbound_order_request",
                "event_at": "2026-04-10T13:35:00+00:00",
                "request": {
                    "api_method": "placeOrder",
                    "stage": stage,
                    "order_id": getattr(order, "orderId", None),
                    "contract": {
                        "symbol": getattr(contract, "symbol", None),
                        "security_type": getattr(contract, "secType", None),
                        "exchange": getattr(contract, "exchange", None),
                        "primary_exchange": getattr(contract, "primaryExchange", None),
                        "currency": getattr(contract, "currency", None),
                    },
                    "order": {
                        "order_id": getattr(order, "orderId", None),
                        "account": getattr(order, "account", None),
                        "order_ref": getattr(order, "orderRef", None),
                        "action": getattr(order, "action", None),
                        "order_type": getattr(order, "orderType", None),
                        "total_quantity": str(getattr(order, "totalQuantity", "")),
                        "limit_price": (
                            str(getattr(order, "lmtPrice"))
                            if getattr(order, "lmtPrice", None) is not None
                            else None
                        ),
                        "time_in_force": getattr(order, "tif", None),
                        "outside_rth": getattr(order, "outsideRth", None),
                        "transmit": getattr(order, "transmit", None),
                        "what_if": getattr(order, "whatIf", None),
                    },
                },
            }
        )

    def preview_order_sync(
        self,
        contract: object,
        order: object,
        timeout: int | None = None,
    ) -> dict[str, object]:
        self.previewed_orders.append((contract, order, timeout))
        order_id = self._next_order_id
        self._next_order_id += 1
        order.orderId = order_id
        self._append_wire_audit("what_if_preflight", contract, order)
        return {
            "orderId": order_id,
            "contract": contract,
            "order": order,
            "orderState": SimpleNamespace(
                status="PreSubmitted",
                warningText="",
                rejectReason="",
                completedStatus="",
                completedTime="",
                equityWithLoanBefore="100000.00",
                equityWithLoanChange=str(-Decimal(str(order.totalQuantity)) * Decimal(str(order.lmtPrice or 0))),
                equityWithLoanAfter=str(
                    Decimal("100000.00")
                    - (Decimal(str(order.totalQuantity)) * Decimal(str(order.lmtPrice or 0)))
                ),
                initMarginBefore="0.00",
                initMarginChange="0.00",
                initMarginAfter="0.00",
                commission="1.00",
                minCommission="1.00",
                maxCommission="1.00",
                commissionCurrency="USD",
            ),
        }

    def place_order_sync(self, contract: object, order: object, timeout: int | None = None) -> dict[str, object]:
        self.placed_orders.append((contract, order, timeout))
        order_id = self._next_order_id
        self._next_order_id += 1
        order.orderId = order_id
        self._append_wire_audit("live_order_submit", contract, order)
        self.open_orders[order_id] = {
            "orderId": order_id,
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
            "orderId": order_id,
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


class _FakeMissingCancelOrderExecutionSyncWrapper(_FakeOrderExecutionSyncWrapper):
    def cancel_order_sync(
        self,
        order_id: int,
        orderCancel: object | None = None,
        timeout: int = 3,
    ) -> dict[str, object]:
        self.cancelled_orders.append((order_id, timeout))
        self.errors[order_id] = [
            {
                "errorCode": 10147,
                "errorString": f"OrderId {order_id} that needs to be cancelled is not found.",
            }
        ]
        raise TimeoutError()


class _FakeAlreadyCancelledOrderExecutionSyncWrapper(_FakeOrderExecutionSyncWrapper):
    def cancel_order_sync(
        self,
        order_id: int,
        orderCancel: object | None = None,
        timeout: int = 3,
    ) -> dict[str, object]:
        self.cancelled_orders.append((order_id, timeout))
        self.errors[order_id] = [
            {
                "errorCode": 10148,
                "errorString": (
                    f"OrderId {order_id} that needs to be cancelled cannot be "
                    "cancelled, state: Cancelled."
                ),
            }
        ]
        raise TimeoutError()


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

class OrderExecutionTestsBase(TestCase):
    def setUp(self) -> None:
        self.config = IbkrConnectionConfig(
            host="127.0.0.1",
            port=7497,
            client_id=0,
            diagnostic_client_id=7,
            account_id="DU1234567",
        )


__all__ = [name for name in globals() if not name.startswith("__")]
