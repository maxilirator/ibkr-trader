from __future__ import annotations

from typing import Any

from ibkr_trader.ibkr.errors import IbkrDependencyError


class _LocalResponseTimeout(TimeoutError):
    """Fallback timeout class for tests and fake wrappers without ibapi installed."""


class _FallbackExecutionFilter:
    """Small stand-in for ibapi.execution.ExecutionFilter used by local tests."""

    def __init__(self) -> None:
        self.acctCode = ""
        self.lastNDays = 0


class _FallbackOrderCancel:
    """Small stand-in for ibapi.order_cancel.OrderCancel used by local tests."""


class _FallbackTWSSyncWrapper:
    """A non-networking base with the buffers RepoSyncWrapper extends in tests.

    Production IBKR work still requires the official package: the default
    ``connect`` raises clearly if somebody tries to use this fallback against
    a real Gateway. Unit tests monkeypatch connection methods and exercise the
    repo-owned callback/request logic without needing the proprietary module.
    """

    def __init__(self, timeout: int = 30) -> None:
        self.timeout = timeout
        self.contract_details: dict[int, Any] = {}
        self.account_summary: dict[int, Any] = {}
        self.open_orders: dict[int, Any] = {}
        self.order_status: dict[int, Any] = {}
        self.historical_data: dict[int, Any] = {}
        self.market_rule: dict[int, Any] = {}
        self.executions: dict[int, list[Any]] = {}
        self.portfolio: list[Any] = []
        self.next_valid_id_value: int | None = None

    def connect(self, host: str, port: int, client_id: int) -> None:
        raise IbkrDependencyError(
            "The official IBKR Python client is not installed. "
            "Install the current TWS API package from IBKR and make sure "
            "the `ibapi` module is available in this environment."
        )

    def isConnected(self) -> bool:  # noqa: N802
        return False

    def run(self) -> None:
        return None

    def disconnect(self) -> None:
        return None

    def get_next_valid_id(self, timeout: int | None = None) -> int:
        del timeout
        if self.next_valid_id_value is None:
            raise _LocalResponseTimeout("nextValidId callback was not received")
        return self.next_valid_id_value

    def _set_event(self, req_id: int, response_name: str, payload: Any) -> None:
        getattr(self, response_name)[req_id] = payload

    def _wait_for_response(
        self,
        req_id: int,
        response_name: str,
        timeout: int | None = None,
    ) -> Any:
        del timeout
        payload = getattr(self, response_name)
        if isinstance(payload, dict):
            if req_id in payload:
                return payload[req_id]
            if req_id == 0:
                return payload
        raise _LocalResponseTimeout(f"{response_name} response was not received")

    def reqContractDetails(self, req_id: int, contract: Any) -> None:  # noqa: N802
        del req_id, contract

    def placeOrder(self, orderId: int, contract: Any, order: Any) -> None:  # noqa: N802
        del orderId, contract, order

    def sendMsg(self, msgId: int, msg: str) -> None:  # noqa: N802
        del msgId, msg

    def sendMsgProtoBuf(self, msgId: int, msg: bytes) -> None:  # noqa: N802
        del msgId, msg

    def reqAccountSummary(self, req_id: int, group: str, tags: str) -> None:  # noqa: N802
        del req_id, group, tags

    def cancelAccountSummary(self, req_id: int) -> None:  # noqa: N802
        del req_id

    def reqAllOpenOrders(self) -> None:  # noqa: N802
        return None

    def reqHistoricalData(self, *args: Any, **kwargs: Any) -> None:  # noqa: N802
        del args, kwargs

    def reqMarketRule(self, market_rule_id: int) -> None:  # noqa: N802
        del market_rule_id

    def reqExecutions(self, req_id: int, exec_filter: Any) -> None:  # noqa: N802
        del req_id, exec_filter

    def reqAccountUpdates(self, subscribe: bool, account_code: str) -> None:  # noqa: N802
        del subscribe, account_code

    def cancelOrder(self, order_id: int, orderCancel: Any | None = None) -> None:  # noqa: N802
        del order_id, orderCancel

    def updateAccountValue(
        self,
        key: str,
        value: str,
        currency: str,
        accountName: str,
    ) -> None:
        del key, value, currency, accountName

    def commissionAndFeesReport(self, commissionAndFeesReport: Any) -> None:  # noqa: N802
        del commissionAndFeesReport

    def error(  # noqa: N802
        self,
        reqId: int,
        errorTime: int,
        errorCode: int,
        errorString: str,
        advancedOrderRejectJson: str = "",
    ) -> None:
        del reqId, errorTime, errorCode, errorString, advancedOrderRejectJson

    def orderStatus(  # noqa: N802
        self,
        orderId: int,
        status: str,
        filled: Any,
        remaining: Any,
        avgFillPrice: float,
        permId: int,
        parentId: int,
        lastFillPrice: float,
        clientId: int,
        whyHeld: str,
        mktCapPrice: float,
    ) -> None:
        self.order_status[orderId] = {
            "orderId": orderId,
            "status": status,
            "filled": filled,
            "remaining": remaining,
            "avgFillPrice": avgFillPrice,
            "permId": permId,
            "parentId": parentId,
            "lastFillPrice": lastFillPrice,
            "clientId": clientId,
            "whyHeld": whyHeld,
            "mktCapPrice": mktCapPrice,
        }

    def execDetails(self, reqId: int, contract: Any, execution: Any) -> None:  # noqa: N802
        self.executions.setdefault(reqId, []).append(
            {
                "contract": contract,
                "execution": execution,
            }
        )

    def execDetailsEnd(self, reqId: int) -> None:  # noqa: N802
        del reqId

    def openOrder(  # noqa: N802
        self,
        orderId: int,
        contract: Any,
        order: Any,
        orderState: Any,
    ) -> None:
        self.open_orders[orderId] = {
            "orderId": orderId,
            "contract": contract,
            "order": order,
            "orderState": orderState,
        }



__all__ = [
    "_FallbackExecutionFilter",
    "_FallbackOrderCancel",
    "_FallbackTWSSyncWrapper",
    "_LocalResponseTimeout",
]
