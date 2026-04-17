from __future__ import annotations

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

        def get_contract_details(self, contract: Any, timeout: int = 5) -> list[Any]:
            req_id = self._next_local_request_id()
            if req_id in self.contract_details:
                del self.contract_details[req_id]
            self.reqContractDetails(req_id, contract)
            return self._wait_for_response(req_id, "contract_details", timeout)

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
            summary = self._wait_for_response(req_id, "account_summary", timeout)
            self.cancelAccountSummary(req_id)
            return summary

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

        def cancel_order_sync(
            self,
            order_id: int,
            orderCancel: Any | None = None,
            timeout: int = 3,
        ) -> dict[str, Any]:
            if orderCancel is None:
                orderCancel = OrderCancel()
            self.cancelOrder(order_id, orderCancel)
            return self._wait_for_response(order_id, "order_status", timeout)

    return RepoSyncWrapper
