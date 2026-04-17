from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.ibkr.contracts import _extract_broker_error_message
from ibkr_trader.ibkr.errors import IbkrDependencyError
from ibkr_trader.ibkr.sync_wrapper import (
    load_response_timeout_class as _load_response_timeout_class,
)
from ibkr_trader.ibkr.sync_wrapper import load_sync_wrapper_class as _load_sync_wrapper_class


DEFAULT_ACCOUNT_SUMMARY_TAGS: tuple[str, ...] = (
    "AccountType",
    "NetLiquidation",
    "BuyingPower",
    "AvailableFunds",
    "ExcessLiquidity",
    "FullAvailableFunds",
    "FullExcessLiquidity",
    "MaintMarginReq",
    "InitMarginReq",
    "LookAheadAvailableFunds",
    "LookAheadExcessLiquidity",
    "Currency",
)


@runtime_checkable
class AccountSummarySyncWrapperProtocol(Protocol):
    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool: ...

    def disconnect_and_stop(self) -> None: ...

    def get_account_summary(
        self,
        tags: str,
        group: str = "All",
        timeout: int = 5,
    ) -> dict[str, dict[str, dict[str, str]]]: ...


def normalize_account_summary_payload(
    raw_summary: dict[str, dict[str, dict[str, str]]],
    *,
    requested_tags: tuple[str, ...],
    account_id: str | None = None,
    group: str = "All",
) -> dict[str, Any]:
    filtered_accounts: dict[str, dict[str, dict[str, str | None]]] = {}

    for current_account_id, values in raw_summary.items():
        if account_id is not None and current_account_id != account_id:
            continue

        filtered_accounts[current_account_id] = {}
        for tag in requested_tags:
            if tag not in values:
                continue
            filtered_accounts[current_account_id][tag] = {
                "value": values[tag].get("value"),
                "currency": values[tag].get("currency"),
            }

    return {
        "group": group,
        "requested_tags": list(requested_tags),
        "account_filter": account_id,
        "accounts": filtered_accounts,
    }


def read_account_summary(
    config: IbkrConnectionConfig,
    *,
    tags: tuple[str, ...] = DEFAULT_ACCOUNT_SUMMARY_TAGS,
    group: str = "All",
    account_id: str | None = None,
    timeout: int = 10,
    sync_wrapper_cls: type[AccountSummarySyncWrapperProtocol] | None = None,
    response_timeout_cls: type[Exception] | None = None,
    app: AccountSummarySyncWrapperProtocol | None = None,
) -> dict[str, Any]:
    timeout_cls = response_timeout_cls or _load_response_timeout_class()
    runtime_app = app
    owns_connection = runtime_app is None
    if runtime_app is None:
        wrapper_cls = sync_wrapper_cls or _load_sync_wrapper_class()
        runtime_app = wrapper_cls(timeout=timeout)
        if not runtime_app.connect_and_start(
            host=config.host,
            port=config.port,
            client_id=config.client_id,
        ):
            raise ConnectionError(
                f"Failed to connect to IBKR at {config.host}:{config.port} "
                f"with client_id={config.client_id}."
            )

    try:
        try:
            raw_summary = runtime_app.get_account_summary(
                tags=",".join(tags),
                group=group,
                timeout=timeout,
            )
        except timeout_cls as exc:
            broker_error = _extract_broker_error_message(runtime_app)
            if broker_error is not None:
                raise LookupError(
                    f"IBKR rejected the account summary request: {broker_error}"
                ) from exc
            raise TimeoutError("Timed out while requesting IBKR account summary.") from exc
    finally:
        if owns_connection:
            runtime_app.disconnect_and_stop()

    return normalize_account_summary_payload(
        raw_summary,
        requested_tags=tags,
        account_id=account_id,
        group=group,
    )
