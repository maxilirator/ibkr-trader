from __future__ import annotations

from collections import deque
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC
from datetime import datetime
import threading
import time
from typing import Any

from ibkr_trader.ibkr.errors import IbkrDependencyError


def load_response_timeout_class() -> type[Exception]:
    try:
        from ibapi.sync_wrapper import ResponseTimeout
    except ModuleNotFoundError as exc:
        raise IbkrDependencyError(
            "The official IBKR Python client is not installed. "
            "Install the current TWS API package from IBKR and make sure "
            "the `ibapi` module is available in this environment."
        ) from exc

    return ResponseTimeout


def load_sync_wrapper_class() -> type[Any]:
    try:
        from ibapi.execution import ExecutionFilter
        from ibapi.order_cancel import OrderCancel
        from ibapi.sync_wrapper import TWSSyncWrapper
    except ModuleNotFoundError as exc:
        raise IbkrDependencyError(
            "The official IBKR Python client is not installed. "
            "Install the current TWS API package from IBKR and make sure "
            "the `ibapi` module is available in this environment."
        ) from exc

    class RepoSyncWrapper(TWSSyncWrapper):
        def __init__(self, timeout: int = 30) -> None:
            super().__init__(timeout=timeout)
            self._local_request_id = 1
            self._local_request_id_lock = threading.Lock()
            self._broker_callback_events: deque[dict[str, Any]] = deque(maxlen=5000)
            self._broker_callback_events_lock = threading.Lock()
            self._suppressed_callback_kinds: dict[str, int] = {}
            self._known_order_ids: set[int] = set()
            self.account_values: dict[str, dict[str, dict[str, str | None]]] = {}

        def connect_and_start(self, host: str, port: int, client_id: int) -> bool:
            self.next_valid_id_value = None
            self.connect(host, port, client_id)

            timeout_at = time.time() + 5
            while not self.isConnected() and time.time() < timeout_at:
                time.sleep(0.1)

            if not self.isConnected():
                return False

            self.api_thread = threading.Thread(
                target=self.run,
                name=f"ibkr-sync-wrapper-{client_id}",
                daemon=True,
            )
            self.api_thread.start()

            timeout_at = time.time() + 5
            while self.next_valid_id_value is None and time.time() < timeout_at:
                time.sleep(0.1)

            if self.next_valid_id_value is None:
                self.disconnect_and_stop()
                return False

            return self.isConnected()

        def disconnect_and_stop(self) -> None:
            self.disconnect()
            if hasattr(self, "api_thread") and self.api_thread.is_alive():
                self.api_thread.join(timeout=2)

        def _next_local_request_id(self) -> int:
            with self._local_request_id_lock:
                req_id = self._local_request_id
                self._local_request_id += 1
            return req_id

        def _record_known_order_id(self, order_id: Any) -> None:
            if order_id in (None, ""):
                return
            try:
                normalized_order_id = int(order_id)
            except (TypeError, ValueError):
                return
            self._known_order_ids.add(normalized_order_id)

        def _is_callback_suppressed(self, kind: str) -> bool:
            return self._suppressed_callback_kinds.get(kind, 0) > 0

        @contextmanager
        def _suppress_broker_callback_events(self, *kinds: str):
            for kind in kinds:
                self._suppressed_callback_kinds[kind] = (
                    self._suppressed_callback_kinds.get(kind, 0) + 1
                )
            try:
                yield
            finally:
                for kind in kinds:
                    remaining = self._suppressed_callback_kinds.get(kind, 0) - 1
                    if remaining > 0:
                        self._suppressed_callback_kinds[kind] = remaining
                    else:
                        self._suppressed_callback_kinds.pop(kind, None)

        def _append_broker_callback_event(self, payload: dict[str, Any]) -> None:
            with self._broker_callback_events_lock:
                self._broker_callback_events.append(payload)

        def drain_broker_callback_events(self) -> list[dict[str, Any]]:
            with self._broker_callback_events_lock:
                events = list(self._broker_callback_events)
                self._broker_callback_events.clear()
            return events

        def _serialize_open_order_callback(
            self,
            orderId: int,
            contract: Any,
            order: Any,
            orderState: Any,
        ) -> dict[str, Any]:
            return {
                "event_type": "open_order",
                "event_at": datetime.now(UTC),
                "order": {
                    "order_id": orderId,
                    "perm_id": (
                        int(getattr(order, "permId"))
                        if getattr(order, "permId", None) not in (None, "")
                        else None
                    ),
                    "client_id": (
                        int(getattr(order, "clientId"))
                        if getattr(order, "clientId", None) not in (None, "")
                        else None
                    ),
                    "status": (
                        str(getattr(orderState, "status"))
                        if getattr(orderState, "status", None) not in (None, "")
                        else None
                    ),
                    "order_ref": (
                        str(getattr(order, "orderRef"))
                        if getattr(order, "orderRef", None) not in (None, "")
                        else None
                    ),
                    "action": (
                        str(getattr(order, "action"))
                        if getattr(order, "action", None) not in (None, "")
                        else None
                    ),
                    "total_quantity": (
                        str(getattr(order, "totalQuantity"))
                        if getattr(order, "totalQuantity", None) not in (None, "")
                        else None
                    ),
                    "symbol": (
                        str(getattr(contract, "symbol"))
                        if getattr(contract, "symbol", None) not in (None, "")
                        else None
                    ),
                    "account": (
                        str(getattr(order, "account"))
                        if getattr(order, "account", None) not in (None, "")
                        else None
                    ),
                    "security_type": (
                        str(getattr(contract, "secType"))
                        if getattr(contract, "secType", None) not in (None, "")
                        else None
                    ),
                    "exchange": (
                        str(getattr(contract, "exchange"))
                        if getattr(contract, "exchange", None) not in (None, "")
                        else None
                    ),
                    "primary_exchange": (
                        str(getattr(contract, "primaryExchange"))
                        if getattr(contract, "primaryExchange", None) not in (None, "")
                        else None
                    ),
                    "currency": (
                        str(getattr(contract, "currency"))
                        if getattr(contract, "currency", None) not in (None, "")
                        else None
                    ),
                    "local_symbol": (
                        str(getattr(contract, "localSymbol"))
                        if getattr(contract, "localSymbol", None) not in (None, "")
                        else None
                    ),
                    "order_type": (
                        str(getattr(order, "orderType"))
                        if getattr(order, "orderType", None) not in (None, "")
                        else None
                    ),
                    "limit_price": (
                        str(getattr(order, "lmtPrice"))
                        if getattr(order, "lmtPrice", None) not in (None, "")
                        else None
                    ),
                    "aux_price": (
                        str(getattr(order, "auxPrice"))
                        if getattr(order, "auxPrice", None) not in (None, "")
                        else None
                    ),
                    "outside_rth": (
                        bool(getattr(order, "outsideRth"))
                        if getattr(order, "outsideRth", None) is not None
                        else None
                    ),
                    "oca_group": (
                        str(getattr(order, "ocaGroup"))
                        if getattr(order, "ocaGroup", None) not in (None, "")
                        else None
                    ),
                    "oca_type": (
                        int(getattr(order, "ocaType"))
                        if getattr(order, "ocaType", None) not in (None, "")
                        else None
                    ),
                    "transmit": (
                        bool(getattr(order, "transmit"))
                        if getattr(order, "transmit", None) is not None
                        else None
                    ),
                    "warning_text": (
                        str(getattr(orderState, "warningText"))
                        if getattr(orderState, "warningText", None) not in (None, "")
                        else None
                    ),
                    "reject_reason": (
                        str(getattr(orderState, "rejectReason"))
                        if getattr(orderState, "rejectReason", None) not in (None, "")
                        else None
                    ),
                    "completed_status": (
                        str(getattr(orderState, "completedStatus"))
                        if getattr(orderState, "completedStatus", None) not in (None, "")
                        else None
                    ),
                    "completed_time": (
                        str(getattr(orderState, "completedTime"))
                        if getattr(orderState, "completedTime", None) not in (None, "")
                        else None
                    ),
                },
            }

        def _serialize_order_status_callback(
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
        ) -> dict[str, Any]:
            return {
                "event_type": "order_status",
                "event_at": datetime.now(UTC),
                "order_status": {
                    "orderId": orderId,
                    "status": status,
                    "filled": str(filled),
                    "remaining": str(remaining),
                    "avgFillPrice": str(avgFillPrice),
                    "permId": permId,
                    "parentId": parentId,
                    "lastFillPrice": str(lastFillPrice),
                    "clientId": clientId,
                    "whyHeld": whyHeld,
                    "mktCapPrice": str(mktCapPrice),
                },
            }

        def _serialize_order_error_callback(
            self,
            reqId: int,
            errorTime: int,
            errorCode: int,
            errorString: str,
            advancedOrderRejectJson: str,
        ) -> dict[str, Any]:
            return {
                "event_type": "order_error",
                "event_at": datetime.now(UTC),
                "error": {
                    "orderId": reqId,
                    "errorTime": errorTime,
                    "errorCode": errorCode,
                    "errorString": errorString,
                    "advancedOrderRejectJson": advancedOrderRejectJson or "",
                },
            }

        def placeOrder(self, orderId: int, contract: Any, order: Any) -> None:  # noqa: N802
            self._record_known_order_id(orderId)
            super().placeOrder(orderId, contract, order)

        def get_contract_details(self, contract: Any, timeout: int = 5) -> list[Any]:
            req_id = self._next_local_request_id()
            if req_id in self.contract_details:
                del self.contract_details[req_id]
            self.reqContractDetails(req_id, contract)
            return self._wait_for_response(req_id, "contract_details", timeout)

        def place_order_sync(
            self,
            contract: Any,
            order: Any,
            timeout: int | None = None,
        ) -> dict[str, Any]:
            if timeout is None:
                timeout = 5 if getattr(order, "orderType", None) in ["LMT", "MKT"] else 2

            order_id = self.get_next_valid_id()
            order.orderId = order_id
            self._record_known_order_id(order_id)

            if order_id in self.order_status:
                del self.order_status[order_id]

            self.placeOrder(order_id, contract, order)
            return self._wait_for_response(order_id, "order_status", timeout)

        def get_account_summary(
            self,
            tags: str,
            group: str = "All",
            timeout: int = 5,
        ) -> dict[str, dict[str, dict[str, str]]]:
            req_id = self._next_local_request_id()
            if req_id in self.account_summary:
                del self.account_summary[req_id]
            self.reqAccountSummary(req_id, group, tags)
            try:
                return self._wait_for_response(req_id, "account_summary", timeout)
            finally:
                self.cancelAccountSummary(req_id)

        def get_open_orders(self, timeout: int = 3) -> dict[int, Any]:
            self.open_orders = {}
            with self._suppress_broker_callback_events("open_order", "order_status"):
                self.reqOpenOrders()
                return self._wait_for_response(0, "open_orders", timeout)

        def get_historical_data(
            self,
            contract: Any,
            end_date_time: str,
            duration_str: str,
            bar_size_setting: str,
            what_to_show: str,
            use_rth: bool = True,
            format_date: int = 1,
            timeout: int = 30,
        ) -> list[Any]:
            req_id = self._next_local_request_id()
            if req_id in self.historical_data:
                del self.historical_data[req_id]
            self.reqHistoricalData(
                req_id,
                contract,
                end_date_time,
                duration_str,
                bar_size_setting,
                what_to_show,
                use_rth,
                format_date,
                False,
                [],
            )
            return self._wait_for_response(req_id, "historical_data", timeout)

        def get_executions(self, exec_filter: Any | None = None, timeout: int = 10) -> list[Any]:
            if exec_filter is None:
                exec_filter = ExecutionFilter()

            req_id = self._next_local_request_id()
            if req_id in self.executions:
                del self.executions[req_id]
            self.reqExecutions(req_id, exec_filter)
            return self._wait_for_response(req_id, "executions", timeout)

        def updateAccountValue(
            self,
            key: str,
            value: str,
            currency: str,
            accountName: str,
        ) -> None:
            account_values = self.account_values.setdefault(accountName, {})
            account_values[key] = {
                "value": value,
                "currency": currency or None,
            }
            super().updateAccountValue(key, value, currency, accountName)

        def get_account_updates(
            self,
            account_code: str = "",
            timeout: int = 10,
        ) -> dict[str, Any]:
            self.portfolio = []
            self.account_values = {}
            self.reqAccountUpdates(True, account_code)
            try:
                portfolio = self._wait_for_response(0, "portfolio", timeout)
                account_values = deepcopy(self.account_values)
            finally:
                self.reqAccountUpdates(False, account_code)
            return {
                "portfolio": portfolio,
                "account_values": account_values,
            }

        def error(  # noqa: N802
            self,
            reqId: int,
            errorTime: int,
            errorCode: int,
            errorString: str,
            advancedOrderRejectJson: str = "",
        ) -> None:
            super().error(reqId, errorTime, errorCode, errorString, advancedOrderRejectJson)
            if self._is_callback_suppressed("order_error"):
                return
            if reqId < 0 or reqId not in self._known_order_ids:
                return
            self._append_broker_callback_event(
                self._serialize_order_error_callback(
                    reqId,
                    errorTime,
                    errorCode,
                    errorString,
                    advancedOrderRejectJson,
                )
            )

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
            super().orderStatus(
                orderId,
                status,
                filled,
                remaining,
                avgFillPrice,
                permId,
                parentId,
                lastFillPrice,
                clientId,
                whyHeld,
                mktCapPrice,
            )
            self._record_known_order_id(orderId)
            if self._is_callback_suppressed("order_status"):
                return
            self._append_broker_callback_event(
                self._serialize_order_status_callback(
                    orderId,
                    status,
                    filled,
                    remaining,
                    avgFillPrice,
                    permId,
                    parentId,
                    lastFillPrice,
                    clientId,
                    whyHeld,
                    mktCapPrice,
                )
            )

        def openOrder(  # noqa: N802
            self,
            orderId: int,
            contract: Any,
            order: Any,
            orderState: Any,
        ) -> None:
            super().openOrder(orderId, contract, order, orderState)
            self._record_known_order_id(orderId)
            if self._is_callback_suppressed("open_order"):
                return
            self._append_broker_callback_event(
                self._serialize_open_order_callback(orderId, contract, order, orderState)
            )

        def cancel_order_sync(
            self,
            order_id: int,
            orderCancel: Any | None = None,
            timeout: int = 3,
        ) -> dict[str, Any]:
            if orderCancel is None:
                orderCancel = OrderCancel()
            self._record_known_order_id(order_id)
            self.cancelOrder(order_id, orderCancel)
            return self._wait_for_response(order_id, "order_status", timeout)

    return RepoSyncWrapper
