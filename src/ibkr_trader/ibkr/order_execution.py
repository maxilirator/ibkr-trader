from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal
from decimal import ROUND_DOWN
from typing import Any, Protocol, runtime_checkable

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.domain.contract_resolution import ContractResolveQuery
from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.domain.execution_contract import ExecutionInstructionBatch
from ibkr_trader.domain.execution_contract import OrderType
from ibkr_trader.ibkr.account_summary import DEFAULT_ACCOUNT_SUMMARY_TAGS
from ibkr_trader.ibkr.account_summary import normalize_account_summary_payload
from ibkr_trader.ibkr.contracts import _extract_broker_error_message
from ibkr_trader.ibkr.contracts import build_ibkr_contract
from ibkr_trader.ibkr.contracts import serialize_contract_details
from ibkr_trader.ibkr.order_preview import _load_contract_class
from ibkr_trader.ibkr.order_preview import _load_response_timeout_class
from ibkr_trader.ibkr.order_preview import _load_sync_wrapper_class
from ibkr_trader.ibkr.order_preview import _resolve_sizing_preview
from ibkr_trader.ibkr.order_preview import _select_broker_account_id
from ibkr_trader.ibkr.probe import IbkrDependencyError


@runtime_checkable
class OrderExecutionSyncWrapperProtocol(Protocol):
    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool: ...

    def disconnect_and_stop(self) -> None: ...

    def get_account_summary(
        self,
        tags: str,
        group: str = "All",
        timeout: int = 5,
    ) -> dict[str, dict[str, dict[str, str]]]: ...

    def get_contract_details(self, contract: Any, timeout: int | None = None) -> list[Any]: ...

    def get_historical_data(
        self,
        contract: Any,
        end_date_time: str,
        duration_str: str,
        bar_size_setting: str,
        what_to_show: str,
        use_rth: bool = True,
        format_date: int = 1,
        timeout: int | None = None,
    ) -> list[Any]: ...

    def place_order_sync(self, contract: Any, order: Any, timeout: int | None = None) -> dict[str, Any]: ...

    def cancel_order_sync(
        self,
        order_id: int,
        orderCancel: Any | None = None,
        timeout: int = 3,
    ) -> dict[str, Any]: ...


def _load_order_class() -> type[Any]:
    try:
        from ibapi.order import Order
    except ModuleNotFoundError as exc:
        raise IbkrDependencyError(
            "The official IBKR Python client is not installed. "
            "Install the current TWS API package from IBKR and make sure "
            "the `ibapi` module is available in this environment."
        ) from exc

    return Order


def _serialize_for_json(payload: Any) -> Any:
    if isinstance(payload, Decimal):
        return str(payload)
    if isinstance(payload, dict):
        return {key: _serialize_for_json(value) for key, value in payload.items()}
    if isinstance(payload, list):
        return [_serialize_for_json(value) for value in payload]
    if isinstance(payload, tuple):
        return [_serialize_for_json(value) for value in payload]
    return payload


def _serialize_tws_submission(raw_payload: Any) -> dict[str, Any] | None:
    if not isinstance(raw_payload, dict):
        return None

    order_id = raw_payload.get("orderId")
    order = raw_payload.get("order")
    contract = raw_payload.get("contract")
    order_state = raw_payload.get("orderState")

    payload = {
        "source": "openOrder",
        "order_id": int(order_id) if order_id not in (None, "") else None,
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
        "contract": {
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
        },
        "order_state": {
            "status": (
                str(getattr(order_state, "status"))
                if getattr(order_state, "status", None) not in (None, "")
                else None
            ),
            "warning_text": (
                str(getattr(order_state, "warningText"))
                if getattr(order_state, "warningText", None) not in (None, "")
                else None
            ),
            "reject_reason": (
                str(getattr(order_state, "rejectReason"))
                if getattr(order_state, "rejectReason", None) not in (None, "")
                else None
            ),
            "completed_status": (
                str(getattr(order_state, "completedStatus"))
                if getattr(order_state, "completedStatus", None) not in (None, "")
                else None
            ),
            "completed_time": (
                str(getattr(order_state, "completedTime"))
                if getattr(order_state, "completedTime", None) not in (None, "")
                else None
            ),
        },
    }
    return _serialize_for_json(payload)


def _extract_tws_submission(
    app: OrderExecutionSyncWrapperProtocol,
    broker_status: dict[str, Any],
) -> dict[str, Any] | None:
    raw_open_orders = getattr(app, "open_orders", None)
    if not isinstance(raw_open_orders, dict):
        return None

    order_id = broker_status.get("orderId")
    if order_id in (None, ""):
        return None

    return _serialize_tws_submission(raw_open_orders.get(int(order_id)))


def _require_single_instruction(batch: ExecutionInstructionBatch) -> ExecutionInstruction:
    if len(batch.instructions) != 1:
        raise ValueError("Paper order submit currently supports exactly one instruction.")
    return batch.instructions[0]


def _normalize_quantity_for_stock(quantity: Decimal | None) -> Decimal:
    if quantity is None:
        raise ValueError("Order quantity could not be resolved.")
    if quantity <= 0:
        raise ValueError("Order quantity must be positive.")

    normalized = quantity.quantize(Decimal("1"), rounding=ROUND_DOWN)
    if normalized <= 0:
        raise ValueError("Order quantity rounds down to zero.")
    if normalized != quantity:
        raise ValueError(
            "Paper order submit currently requires an integral share quantity. "
            f"Resolved quantity was {quantity}."
        )
    return normalized


def _build_ibkr_order(
    *,
    action: str,
    order_ref: str,
    order_type: OrderType,
    broker_account_id: str,
    quantity: Decimal,
    time_in_force: str,
    limit_price: Decimal | None,
    order_cls: type[Any] | None = None,
) -> Any:
    runtime_order_cls = order_cls or _load_order_class()
    order = runtime_order_cls()
    order.account = broker_account_id
    order.action = action
    order.orderType = "LMT" if order_type is OrderType.LIMIT else "MKT"
    order.totalQuantity = int(quantity)
    order.tif = time_in_force
    order.outsideRth = False
    order.transmit = True
    order.orderRef = order_ref

    if limit_price is not None:
        order.lmtPrice = float(limit_price)

    return order


def _opposite_action(action: str) -> str:
    if action == "BUY":
        return "SELL"
    if action == "SELL":
        return "BUY"
    raise ValueError(f"Unsupported action: {action}")


def _resolve_instruction_contract_and_account(
    app: OrderExecutionSyncWrapperProtocol,
    config: IbkrConnectionConfig,
    instruction: ExecutionInstruction,
    *,
    timeout: int,
    timeout_cls: type[Exception],
    contract_cls: type[Any] | None,
) -> tuple[str, list[str], dict[str, Any], Any, dict[str, Any]]:
    runtime_contract_cls = contract_cls or _load_contract_class()
    raw_summary = app.get_account_summary(
        tags=",".join(DEFAULT_ACCOUNT_SUMMARY_TAGS),
        group="All",
        timeout=timeout,
    )
    normalized_summary = normalize_account_summary_payload(
        raw_summary,
        requested_tags=DEFAULT_ACCOUNT_SUMMARY_TAGS,
        account_id=None,
        group="All",
    )
    broker_account_id, account_warnings = _select_broker_account_id(
        configured_account_id=config.account_id,
        normalized_summary=normalized_summary,
    )
    if broker_account_id is None:
        raise LookupError("; ".join(account_warnings) or "No broker account could be selected.")

    raw_contract = build_ibkr_contract(
        ContractResolveQuery(
            symbol=instruction.instrument.symbol,
            security_type=instruction.instrument.security_type.value,
            exchange=instruction.instrument.exchange,
            currency=instruction.instrument.currency,
            primary_exchange=instruction.instrument.primary_exchange,
            isin=instruction.instrument.isin,
        ),
        contract_cls=runtime_contract_cls,
    )
    try:
        contract_matches = app.get_contract_details(raw_contract, timeout=timeout)
    except timeout_cls as exc:
        broker_error = _extract_broker_error_message(app)
        if broker_error is not None:
            raise LookupError(
                f"IBKR rejected the contract lookup: {broker_error}"
            ) from exc
        raise TimeoutError(
            f"Timed out while resolving {instruction.instrument.symbol}."
        ) from exc

    if len(contract_matches) != 1:
        raise LookupError(
            f"Expected exactly one resolved contract, got {len(contract_matches)}."
        )
    resolved_contract = _serialize_resolved_contract(contract_matches[0])
    return (
        broker_account_id,
        account_warnings,
        normalized_summary,
        contract_matches[0].contract,
        resolved_contract,
    )


def _serialize_resolved_contract(raw_detail: Any) -> dict[str, Any]:
    serialized = asdict(serialize_contract_details(raw_detail))
    serialized["min_tick"] = (
        str(serialized["min_tick"]) if serialized["min_tick"] is not None else None
    )
    serialized["valid_exchanges"] = list(serialized["valid_exchanges"])
    serialized["order_types"] = list(serialized["order_types"])
    serialized["sec_ids"] = dict(serialized["sec_ids"])
    return serialized


def submit_order_from_batch(
    config: IbkrConnectionConfig,
    batch: ExecutionInstructionBatch,
    *,
    timeout: int = 10,
    sync_wrapper_cls: type[OrderExecutionSyncWrapperProtocol] | None = None,
    response_timeout_cls: type[Exception] | None = None,
    contract_cls: type[Any] | None = None,
    order_cls: type[Any] | None = None,
) -> dict[str, Any]:
    instruction = _require_single_instruction(batch)
    return submit_order_from_instruction(
        config,
        instruction,
        timeout=timeout,
        sync_wrapper_cls=sync_wrapper_cls,
        response_timeout_cls=response_timeout_cls,
        contract_cls=contract_cls,
        order_cls=order_cls,
    )


def submit_order_from_instruction(
    config: IbkrConnectionConfig,
    instruction: ExecutionInstruction,
    *,
    timeout: int = 10,
    sync_wrapper_cls: type[OrderExecutionSyncWrapperProtocol] | None = None,
    response_timeout_cls: type[Exception] | None = None,
    contract_cls: type[Any] | None = None,
    order_cls: type[Any] | None = None,
) -> dict[str, Any]:
    if instruction.entry.order_type is not OrderType.LIMIT:
        raise ValueError("Manual paper order submit currently supports LIMIT orders only.")

    wrapper_cls = sync_wrapper_cls or _load_sync_wrapper_class()
    timeout_cls = response_timeout_cls or _load_response_timeout_class()
    runtime_contract_cls = contract_cls or _load_contract_class()
    runtime_order_cls = order_cls or _load_order_class()
    app = wrapper_cls(timeout=timeout)

    if not app.connect_and_start(
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
            (
                broker_account_id,
                account_warnings,
                normalized_summary,
                resolved_ibkr_contract,
                resolved_contract,
            ) = _resolve_instruction_contract_and_account(
                app,
                config,
                instruction,
                timeout=timeout,
                timeout_cls=timeout_cls,
                contract_cls=runtime_contract_cls,
            )
        except timeout_cls as exc:
            broker_error = _extract_broker_error_message(app)
            if broker_error is not None:
                raise LookupError(
                    f"IBKR rejected the account summary request: {broker_error}"
                ) from exc
            raise TimeoutError("Timed out while requesting IBKR account summary.") from exc

        sizing_preview = _resolve_sizing_preview(
            instruction,
            broker_account_id=broker_account_id,
            normalized_summary=normalized_summary,
            app=app,
            timeout=timeout,
            timeout_cls=timeout_cls,
            contract_cls=runtime_contract_cls,
            fx_rate_cache={},
        )
        if sizing_preview["issues"]:
            raise ValueError("; ".join(sizing_preview["issues"]))

        normalized_quantity = _normalize_quantity_for_stock(
            sizing_preview["estimated_quantity"]
        )
        order = _build_ibkr_order(
            action=instruction.intent.side,
            order_ref=instruction.instruction_id,
            order_type=instruction.entry.order_type,
            broker_account_id=broker_account_id,
            quantity=normalized_quantity,
            time_in_force=instruction.entry.time_in_force.value,
            limit_price=instruction.entry.limit_price,
            order_cls=runtime_order_cls,
        )

        try:
            order_status = app.place_order_sync(
                resolved_ibkr_contract,
                order,
                timeout=timeout,
            )
        except timeout_cls as exc:
            broker_error = _extract_broker_error_message(app)
            if broker_error is not None:
                raise LookupError(
                    f"IBKR rejected the order submission: {broker_error}"
                ) from exc
            raise TimeoutError("Timed out while placing the IBKR order.") from exc
        tws_submission = _extract_tws_submission(app, order_status)
    finally:
        app.disconnect_and_stop()

    return _serialize_for_json(
        {
            "instruction_id": instruction.instruction_id,
            "account": broker_account_id,
            "warnings": list(account_warnings),
            "resolved_contract": resolved_contract,
            "order": {
                "order_ref": instruction.instruction_id,
                "action": instruction.intent.side,
                "order_type": order.orderType,
                "time_in_force": order.tif,
                "limit_price": (
                    str(instruction.entry.limit_price)
                    if instruction.entry.limit_price is not None
                    else None
                ),
                "total_quantity": str(normalized_quantity),
                "outside_rth": order.outsideRth,
                "transmit": order.transmit,
            },
            "broker_order_status": order_status,
            "tws_submission": tws_submission,
        }
    )


def submit_exit_order_from_instruction(
    config: IbkrConnectionConfig,
    instruction: ExecutionInstruction,
    *,
    quantity: Decimal,
    order_type: OrderType,
    order_ref: str,
    timeout: int = 10,
    limit_price: Decimal | None = None,
    sync_wrapper_cls: type[OrderExecutionSyncWrapperProtocol] | None = None,
    response_timeout_cls: type[Exception] | None = None,
    contract_cls: type[Any] | None = None,
    order_cls: type[Any] | None = None,
) -> dict[str, Any]:
    normalized_quantity = _normalize_quantity_for_stock(quantity)
    if order_type is OrderType.LIMIT and limit_price is None:
        raise ValueError("limit_price is required for LIMIT exit orders.")
    if order_type is OrderType.MARKET and limit_price is not None:
        raise ValueError("limit_price must be omitted for MARKET exit orders.")

    wrapper_cls = sync_wrapper_cls or _load_sync_wrapper_class()
    timeout_cls = response_timeout_cls or _load_response_timeout_class()
    runtime_contract_cls = contract_cls or _load_contract_class()
    runtime_order_cls = order_cls or _load_order_class()
    app = wrapper_cls(timeout=timeout)

    if not app.connect_and_start(
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
            (
                broker_account_id,
                account_warnings,
                _normalized_summary,
                resolved_ibkr_contract,
                resolved_contract,
            ) = _resolve_instruction_contract_and_account(
                app,
                config,
                instruction,
                timeout=timeout,
                timeout_cls=timeout_cls,
                contract_cls=runtime_contract_cls,
            )
        except timeout_cls as exc:
            broker_error = _extract_broker_error_message(app)
            if broker_error is not None:
                raise LookupError(
                    f"IBKR rejected the account summary request: {broker_error}"
                ) from exc
            raise TimeoutError("Timed out while requesting IBKR account summary.") from exc

        order = _build_ibkr_order(
            action=_opposite_action(instruction.intent.side),
            order_ref=order_ref,
            order_type=order_type,
            broker_account_id=broker_account_id,
            quantity=normalized_quantity,
            time_in_force=instruction.entry.time_in_force.value,
            limit_price=limit_price,
            order_cls=runtime_order_cls,
        )

        try:
            order_status = app.place_order_sync(
                resolved_ibkr_contract,
                order,
                timeout=timeout,
            )
        except timeout_cls as exc:
            broker_error = _extract_broker_error_message(app)
            if broker_error is not None:
                raise LookupError(
                    f"IBKR rejected the order submission: {broker_error}"
                ) from exc
            raise TimeoutError("Timed out while placing the IBKR order.") from exc
    finally:
        app.disconnect_and_stop()

    return _serialize_for_json(
        {
            "instruction_id": instruction.instruction_id,
            "account": broker_account_id,
            "warnings": list(account_warnings),
            "resolved_contract": resolved_contract,
            "order": {
                "order_ref": order_ref,
                "action": order.action,
                "order_type": order.orderType,
                "time_in_force": order.tif,
                "limit_price": str(limit_price) if limit_price is not None else None,
                "total_quantity": str(normalized_quantity),
                "outside_rth": order.outsideRth,
                "transmit": order.transmit,
            },
            "broker_order_status": order_status,
        }
    )


def cancel_broker_order(
    config: IbkrConnectionConfig,
    order_id: int,
    *,
    timeout: int = 10,
    sync_wrapper_cls: type[OrderExecutionSyncWrapperProtocol] | None = None,
    response_timeout_cls: type[Exception] | None = None,
) -> dict[str, Any]:
    wrapper_cls = sync_wrapper_cls or _load_sync_wrapper_class()
    timeout_cls = response_timeout_cls or _load_response_timeout_class()
    app = wrapper_cls(timeout=timeout)

    if not app.connect_and_start(
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
            order_status = app.cancel_order_sync(order_id, timeout=timeout)
        except timeout_cls as exc:
            broker_error = _extract_broker_error_message(app)
            if broker_error is not None:
                raise LookupError(
                    f"IBKR rejected the order cancel request: {broker_error}"
                ) from exc
            raise TimeoutError("Timed out while cancelling the IBKR order.") from exc
    finally:
        app.disconnect_and_stop()

    return _serialize_for_json({"broker_order_status": order_status})
