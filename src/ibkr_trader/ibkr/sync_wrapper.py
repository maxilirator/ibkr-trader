from __future__ import annotations

import base64
from collections import deque
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC
from datetime import datetime
import threading
import time
from typing import Any

from ibkr_trader.ibkr.errors import IbkrDependencyError


IBKR_INFORMATIONAL_MESSAGE_CODES = {2104, 2106, 2107, 2108, 2119, 2158}
IBKR_ORDER_OUTBOUND_MESSAGE_NAMES = {
    3: "PLACE_ORDER",
    4: "CANCEL_ORDER",
    203: "PLACE_ORDER_PROTOBUF",
    204: "CANCEL_ORDER_PROTOBUF",
}


from ibkr_trader.ibkr.sync_wrapper_fallback import _FallbackExecutionFilter
from ibkr_trader.ibkr.sync_wrapper_fallback import _FallbackOrderCancel
from ibkr_trader.ibkr.sync_wrapper_fallback import _FallbackTWSSyncWrapper
from ibkr_trader.ibkr.sync_wrapper_fallback import _LocalResponseTimeout


def load_response_timeout_class() -> type[Exception]:
    try:
        from ibapi.sync_wrapper import ResponseTimeout
    except ModuleNotFoundError:
        return _LocalResponseTimeout

    return ResponseTimeout


def load_sync_wrapper_class() -> type[Any]:
    try:
        from ibapi.execution import ExecutionFilter
        from ibapi.order_cancel import OrderCancel
        from ibapi.sync_wrapper import TWSSyncWrapper
    except ModuleNotFoundError:
        ExecutionFilter = _FallbackExecutionFilter
        OrderCancel = _FallbackOrderCancel
        TWSSyncWrapper = _FallbackTWSSyncWrapper

    class RepoSyncWrapper(TWSSyncWrapper):
        def __init__(self, timeout: int = 30) -> None:
            super().__init__(timeout=timeout)
            self._local_request_id = 1
            self._local_request_id_lock = threading.Lock()
            self._broker_callback_events: deque[dict[str, Any]] = deque(maxlen=5000)
            self._broker_callback_events_lock = threading.Lock()
            self._broker_wire_audit_events: deque[dict[str, Any]] = deque(maxlen=5000)
            self._broker_wire_audit_events_lock = threading.Lock()
            self._suppressed_callback_kinds: dict[str, int] = {}
            self._known_order_ids: set[int] = set()
            self._next_order_id_floor: int | None = None
            self._next_order_id_lock = threading.Lock()
            self._closed_execution_request_ids: deque[int] = deque(maxlen=512)
            self._closed_execution_request_ids_lookup: set[int] = set()
            self.account_values: dict[str, dict[str, dict[str, str | None]]] = {}
            self.execution_commissions: dict[str, Any] = {}

        def connect_and_start(self, host: str, port: int, client_id: int) -> bool:
            self.last_connect_failure_reason = None
            self.next_valid_id_value = None
            self.connect(host, port, client_id)

            timeout_at = time.time() + 5
            while not self.isConnected() and time.time() < timeout_at:
                time.sleep(0.1)

            if not self.isConnected():
                self.last_connect_failure_reason = (
                    f"Failed to open an IBKR API socket at {host}:{port} "
                    f"with client_id={client_id}."
                )
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
                self.last_connect_failure_reason = (
                    f"Connected to IBKR API socket at {host}:{port} with "
                    f"client_id={client_id}, but Gateway did not complete API startup "
                    "(no nextValidId callback). If the Gateway UI is green, check for "
                    "a pending API connection approval prompt and verify API Settings: "
                    "Enable ActiveX and Socket Clients, Socket port, and trusted "
                    "localhost connections."
                )
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

        def _wait_for_response(
            self,
            req_id: int,
            response_name: str,
            timeout: int | None = None,
        ) -> Any:
            event_key = f"{response_name}_{req_id}"
            response_data = getattr(self, "response_data", None)
            if isinstance(response_data, dict) and event_key in response_data:
                return response_data.pop(event_key)
            return super()._wait_for_response(req_id, response_name, timeout)

        def _request_next_order_id_from_broker(self, timeout: int | None = None) -> int:
            return super().get_next_valid_id(timeout)

        def _allocate_order_id(self, timeout: int | None = None) -> int:
            broker_order_id = int(self._request_next_order_id_from_broker(timeout))
            with self._next_order_id_lock:
                if (
                    self._next_order_id_floor is None
                    or broker_order_id > self._next_order_id_floor
                ):
                    order_id = broker_order_id
                else:
                    order_id = self._next_order_id_floor
                self._next_order_id_floor = order_id + 1
                if (
                    self.next_valid_id_value is None
                    or self.next_valid_id_value <= order_id
                ):
                    self.next_valid_id_value = order_id + 1
                return order_id

        def _record_known_order_id(self, order_id: Any) -> None:
            if order_id in (None, ""):
                return
            try:
                normalized_order_id = int(order_id)
            except (TypeError, ValueError):
                return
            self._known_order_ids.add(normalized_order_id)
            with self._next_order_id_lock:
                if (
                    self._next_order_id_floor is None
                    or self._next_order_id_floor <= normalized_order_id
                ):
                    self._next_order_id_floor = normalized_order_id + 1

        def _mark_execution_request_closed(self, req_id: int) -> None:
            if req_id in self._closed_execution_request_ids_lookup:
                return
            if len(self._closed_execution_request_ids) == self._closed_execution_request_ids.maxlen:
                dropped_req_id = self._closed_execution_request_ids.popleft()
                self._closed_execution_request_ids_lookup.discard(dropped_req_id)
            self._closed_execution_request_ids.append(req_id)
            self._closed_execution_request_ids_lookup.add(req_id)

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

        def _append_broker_wire_audit_event(self, payload: dict[str, Any]) -> None:
            with self._broker_wire_audit_events_lock:
                self._broker_wire_audit_events.append(payload)

        def drain_broker_callback_events(self) -> list[dict[str, Any]]:
            with self._broker_callback_events_lock:
                events = list(self._broker_callback_events)
                self._broker_callback_events.clear()
            return events

        def broker_wire_audit_event_count(self) -> int:
            with self._broker_wire_audit_events_lock:
                return len(self._broker_wire_audit_events)

        def broker_wire_audit_events_since(self, offset: int = 0) -> list[dict[str, Any]]:
            with self._broker_wire_audit_events_lock:
                events = list(self._broker_wire_audit_events)
            if offset <= 0:
                return events
            return events[offset:]

        def _serialize_public_fields(self, payload: Any) -> Any:
            if payload in (None, ""):
                return None
            if isinstance(payload, datetime):
                return payload.isoformat()
            if isinstance(payload, (str, int, float, bool)):
                return payload
            if isinstance(payload, list | tuple):
                return [
                    serialized
                    for item in payload
                    if (serialized := self._serialize_public_fields(item)) is not None
                ]
            if isinstance(payload, dict):
                return {
                    str(key): serialized
                    for key, value in payload.items()
                    if (serialized := self._serialize_public_fields(value)) is not None
                }
            fields = getattr(payload, "__dict__", None)
            if isinstance(fields, dict):
                return {
                    key: serialized
                    for key, value in fields.items()
                    if not key.startswith("_")
                    and not callable(value)
                    and (serialized := self._serialize_public_fields(value)) is not None
                }
            return str(payload)

        def _serialize_contract_for_wire_audit(self, contract: Any) -> dict[str, Any]:
            return {
                "con_id": (
                    int(getattr(contract, "conId"))
                    if getattr(contract, "conId", None) not in (None, "")
                    else None
                ),
                "symbol": (
                    str(getattr(contract, "symbol"))
                    if getattr(contract, "symbol", None) not in (None, "")
                    else None
                ),
                "local_symbol": (
                    str(getattr(contract, "localSymbol"))
                    if getattr(contract, "localSymbol", None) not in (None, "")
                    else None
                ),
                "trading_class": (
                    str(getattr(contract, "tradingClass"))
                    if getattr(contract, "tradingClass", None) not in (None, "")
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
                "sec_id_type": (
                    str(getattr(contract, "secIdType"))
                    if getattr(contract, "secIdType", None) not in (None, "")
                    else None
                ),
                "sec_id": (
                    str(getattr(contract, "secId"))
                    if getattr(contract, "secId", None) not in (None, "")
                    else None
                ),
                "raw_nonempty_fields": self._serialize_public_fields(contract),
            }

        def _serialize_order_for_wire_audit(self, order_id: int, order: Any) -> dict[str, Any]:
            return {
                "order_id": order_id,
                "account": (
                    str(getattr(order, "account"))
                    if getattr(order, "account", None) not in (None, "")
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
                "order_type": (
                    str(getattr(order, "orderType"))
                    if getattr(order, "orderType", None) not in (None, "")
                    else None
                ),
                "total_quantity": (
                    str(getattr(order, "totalQuantity"))
                    if getattr(order, "totalQuantity", None) not in (None, "")
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
                "time_in_force": (
                    str(getattr(order, "tif"))
                    if getattr(order, "tif", None) not in (None, "")
                    else None
                ),
                "outside_rth": (
                    bool(getattr(order, "outsideRth"))
                    if getattr(order, "outsideRth", None) is not None
                    else None
                ),
                "transmit": (
                    bool(getattr(order, "transmit"))
                    if getattr(order, "transmit", None) is not None
                    else None
                ),
                "what_if": (
                    bool(getattr(order, "whatIf"))
                    if getattr(order, "whatIf", None) is not None
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
                "parent_id": (
                    int(getattr(order, "parentId"))
                    if getattr(order, "parentId", None) not in (None, "")
                    else None
                ),
                "raw_nonempty_fields": self._serialize_public_fields(order),
            }

        def _serialize_outbound_order_request(
            self,
            orderId: int,
            contract: Any,
            order: Any,
        ) -> dict[str, Any]:
            return {
                "event_type": "outbound_order_request",
                "event_at": datetime.now(UTC),
                "request": {
                    "api_method": "placeOrder",
                    "stage": (
                        "what_if_preflight"
                        if bool(getattr(order, "whatIf", False))
                        else "live_order_submit"
                    ),
                    "order_id": orderId,
                    "contract": self._serialize_contract_for_wire_audit(contract),
                    "order": self._serialize_order_for_wire_audit(orderId, order),
                },
            }

        def _serialize_outbound_cancel_request(
            self,
            order_id: int,
            orderCancel: Any | None,
        ) -> dict[str, Any]:
            return {
                "event_type": "outbound_cancel_request",
                "event_at": datetime.now(UTC),
                "request": {
                    "api_method": "cancelOrder",
                    "order_id": order_id,
                    "order_cancel": self._serialize_public_fields(orderCancel),
                },
            }

        def _serialize_outbound_ibapi_message(
            self,
            api_method: str,
            msg_id: int,
            msg: str | bytes,
        ) -> dict[str, Any] | None:
            try:
                normalized_msg_id = int(msg_id)
            except (TypeError, ValueError):
                return None
            message_name = IBKR_ORDER_OUTBOUND_MESSAGE_NAMES.get(normalized_msg_id)
            if message_name is None:
                return None

            payload: dict[str, Any]
            if isinstance(msg, bytes | bytearray):
                raw_bytes = bytes(msg)
                payload = {
                    "encoding": "protobuf_bytes_base64",
                    "byte_length": len(raw_bytes),
                    "base64": base64.b64encode(raw_bytes).decode("ascii"),
                }
            else:
                raw_text = str(msg)
                payload = {
                    "encoding": "ibapi_field_string",
                    "char_length": len(raw_text),
                    "fields": raw_text.split("\0"),
                    "repr": repr(raw_text),
                }

            return {
                "event_type": "outbound_ibapi_message",
                "event_at": datetime.now(UTC),
                "request": {
                    "api_method": api_method,
                    "message_id": normalized_msg_id,
                    "message_name": message_name,
                    "payload": payload,
                },
            }

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

        def _merge_execution_commissions(
            self,
            raw_executions: list[dict[str, Any]],
        ) -> list[dict[str, Any]]:
            merged_executions: list[dict[str, Any]] = []
            for execution_payload in raw_executions:
                if not isinstance(execution_payload, dict):
                    merged_executions.append(execution_payload)
                    continue
                execution = execution_payload.get("execution")
                exec_id = (
                    str(getattr(execution, "execId"))
                    if getattr(execution, "execId", None) not in (None, "")
                    else None
                )
                if exec_id is None:
                    merged_executions.append(execution_payload)
                    continue
                commission_report = self.execution_commissions.get(exec_id)
                if commission_report is None:
                    merged_executions.append(execution_payload)
                    continue
                merged_executions.append(
                    {
                        **execution_payload,
                        "commission_and_fees_report": commission_report,
                    }
                )
            return merged_executions

        def placeOrder(self, orderId: int, contract: Any, order: Any) -> None:  # noqa: N802
            self._record_known_order_id(orderId)
            self._append_broker_wire_audit_event(
                self._serialize_outbound_order_request(orderId, contract, order)
            )
            super().placeOrder(orderId, contract, order)

        def sendMsg(self, msgId: int, msg: str) -> None:  # noqa: N802
            wire_event = self._serialize_outbound_ibapi_message("sendMsg", msgId, msg)
            if wire_event is not None:
                self._append_broker_wire_audit_event(wire_event)
            super().sendMsg(msgId, msg)

        def sendMsgProtoBuf(self, msgId: int, msg: bytes) -> None:  # noqa: N802
            wire_event = self._serialize_outbound_ibapi_message(
                "sendMsgProtoBuf",
                msgId,
                msg,
            )
            if wire_event is not None:
                self._append_broker_wire_audit_event(wire_event)
            super().sendMsgProtoBuf(msgId, msg)

        def get_contract_details(self, contract: Any, timeout: int = 5) -> list[Any]:
            req_id = self._next_local_request_id()
            if req_id in self.contract_details:
                del self.contract_details[req_id]
            self.reqContractDetails(req_id, contract)
            try:
                return self._wait_for_response(req_id, "contract_details", timeout)
            finally:
                # `req_id` is never reused (it's an ever-incrementing counter), so
                # nothing else will ever read or clear this entry - without this,
                # every contract lookup leaves its result in `self.contract_details`
                # for the lifetime of the long-lived historical session.
                self.contract_details.pop(req_id, None)

        def place_order_sync(
            self,
            contract: Any,
            order: Any,
            timeout: int | None = None,
        ) -> dict[str, Any]:
            if timeout is None:
                timeout = 5 if getattr(order, "orderType", None) in ["LMT", "MKT"] else 2

            order_id = self._allocate_order_id()
            order.orderId = order_id
            self._record_known_order_id(order_id)

            if order_id in self.order_status:
                del self.order_status[order_id]

            self.placeOrder(order_id, contract, order)
            return self._wait_for_response(order_id, "order_status", timeout)

        def preview_order_sync(
            self,
            contract: Any,
            order: Any,
            timeout: int | None = None,
        ) -> dict[str, Any]:
            if timeout is None:
                timeout = 5

            order_id = self._allocate_order_id()
            order.orderId = order_id
            order.whatIf = True
            order.transmit = True
            self._record_known_order_id(order_id)

            if order_id in self.open_orders:
                del self.open_orders[order_id]
            if hasattr(self, "errors") and order_id in self.errors:
                del self.errors[order_id]

            with self._suppress_broker_callback_events(
                "open_order",
                "order_status",
                "order_error",
            ):
                self.placeOrder(order_id, contract, order)
                return self._wait_for_response(order_id, "open_orders", timeout)

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
                # Same retention bug as get_historical_data/get_contract_details
                # had before 8a630bb: the entry is deleted before the request,
                # which is dead code because req_id never repeats, and never
                # after the response is read. Cancelling the subscription stops
                # further callbacks but leaves the accumulated result behind for
                # the lifetime of this long-lived session. Lower call volume than
                # the historical path, so it was deprioritised then; closing it
                # now so no known per-request retention mechanism remains.
                self.account_summary.pop(req_id, None)

        def get_open_orders(self, timeout: int = 3) -> dict[int, Any]:
            self.open_orders = {}
            with self._suppress_broker_callback_events("open_order", "order_status"):
                # Use the API-wide open-order query instead of reqOpenOrders()
                # so client_id=0 does not try to bind manual TWS orders.
                self.reqAllOpenOrders()
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
            try:
                return self._wait_for_response(req_id, "historical_data", timeout)
            finally:
                # Same reasoning as `get_contract_details`: `req_id` is never
                # reused, so the bar list for every historical request would
                # otherwise sit in `self.historical_data` for the process
                # lifetime of the long-lived historical session - this is what
                # grew the API process by ~300MB/hour under sustained backfill.
                self.historical_data.pop(req_id, None)

        def marketRule(self, marketRuleId: int, priceIncrements: list[Any]) -> None:  # noqa: N802
            self._set_event(marketRuleId, "market_rule", list(priceIncrements or []))
            super().marketRule(marketRuleId, priceIncrements)

        def get_market_rule(
            self,
            market_rule_id: int,
            timeout: int = 5,
        ) -> list[Any]:
            self.reqMarketRule(market_rule_id)
            return self._wait_for_response(market_rule_id, "market_rule", timeout)

        def get_executions(self, exec_filter: Any | None = None, timeout: int = 10) -> list[Any]:
            if exec_filter is None:
                exec_filter = ExecutionFilter()

            req_id = self._next_local_request_id()
            self._closed_execution_request_ids_lookup.discard(req_id)
            if req_id in self.executions:
                del self.executions[req_id]
            self.reqExecutions(req_id, exec_filter)
            try:
                raw_executions = self._wait_for_response(req_id, "executions", timeout)
            finally:
                self._mark_execution_request_closed(req_id)
                self.executions.pop(req_id, None)
            return self._merge_execution_commissions(raw_executions)

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

        def commissionAndFeesReport(  # noqa: N802
            self,
            commissionAndFeesReport: Any,
        ) -> None:
            exec_id = (
                str(getattr(commissionAndFeesReport, "execId"))
                if getattr(commissionAndFeesReport, "execId", None) not in (None, "")
                else None
            )
            if exec_id is not None:
                self.execution_commissions[exec_id] = deepcopy(commissionAndFeesReport)
            super().commissionAndFeesReport(commissionAndFeesReport)

        def error(  # noqa: N802
            self,
            reqId: int,
            errorTime: int,
            errorCode: int,
            errorString: str,
            advancedOrderRejectJson: str = "",
        ) -> None:
            if errorCode == 2100 and "unsubscribed from account data" in errorString.lower():
                return
            if errorCode not in IBKR_INFORMATIONAL_MESSAGE_CODES:
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

        def execDetails(self, reqId: int, contract: Any, execution: Any) -> None:  # noqa: N802
            if reqId in self._closed_execution_request_ids_lookup:
                return
            super().execDetails(reqId, contract, execution)

        def execDetailsEnd(self, reqId: int) -> None:  # noqa: N802
            if reqId in self._closed_execution_request_ids_lookup:
                return
            super().execDetailsEnd(reqId)

        def openOrder(  # noqa: N802
            self,
            orderId: int,
            contract: Any,
            order: Any,
            orderState: Any,
        ) -> None:
            super().openOrder(orderId, contract, order, orderState)
            self._record_known_order_id(orderId)
            if orderId in self.open_orders:
                self._set_event(orderId, "open_orders", self.open_orders[orderId])
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

        def cancelOrder(self, order_id: int, orderCancel: Any | None = None) -> None:  # noqa: N802
            self._record_known_order_id(order_id)
            self._append_broker_wire_audit_event(
                self._serialize_outbound_cancel_request(order_id, orderCancel)
            )
            super().cancelOrder(order_id, orderCancel)

    return RepoSyncWrapper
