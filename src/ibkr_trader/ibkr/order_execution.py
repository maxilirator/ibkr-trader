from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal
from decimal import ROUND_DOWN
import re
import time
from typing import Any, Protocol, runtime_checkable

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.domain.contract_resolution import ContractResolveQuery
from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.domain.execution_contract import ExecutionInstructionBatch
from ibkr_trader.domain.execution_contract import OrderType
from ibkr_trader.domain.execution_contract import SizingMode
from ibkr_trader.ibkr.account_summary import DEFAULT_ACCOUNT_SUMMARY_TAGS
from ibkr_trader.ibkr.account_summary import read_account_summary
from ibkr_trader.ibkr.contracts import _extract_broker_error_message
from ibkr_trader.ibkr.contracts import build_ibkr_contract
from ibkr_trader.ibkr.contracts import serialize_contract_details
from ibkr_trader.ibkr.order_preview import _load_contract_class
from ibkr_trader.ibkr.order_preview import _load_response_timeout_class
from ibkr_trader.ibkr.order_preview import _load_sync_wrapper_class
from ibkr_trader.ibkr.order_preview import _resolve_sizing_preview
from ibkr_trader.ibkr.order_preview import _select_broker_account_id
from ibkr_trader.ibkr.price_rules import normalize_order_price
from ibkr_trader.ibkr.price_rules import resolve_price_increment
from ibkr_trader.ibkr.errors import IbkrDependencyError

_ORDER_CANCEL_NOT_FOUND_CODE = 10147


@runtime_checkable
class OrderExecutionSyncWrapperProtocol(Protocol):
    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool: ...

    def disconnect_and_stop(self) -> None: ...

    def get_account_updates(
        self,
        account_code: str = "",
        timeout: int = 10,
    ) -> dict[str, Any]: ...

    def get_contract_details(self, contract: Any, timeout: int | None = None) -> list[Any]: ...

    def get_market_rule(
        self,
        market_rule_id: int,
        timeout: int = 5,
    ) -> list[Any]: ...

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


def _normalize_quantity_for_stock(
    quantity: Decimal | None,
    *,
    allow_round_down: bool,
) -> tuple[Decimal, list[str]]:
    warnings: list[str] = []
    if quantity is None:
        raise ValueError("Order quantity could not be resolved.")
    if quantity <= 0:
        raise ValueError("Order quantity must be positive.")

    normalized = quantity.quantize(Decimal("1"), rounding=ROUND_DOWN)
    if normalized <= 0:
        raise ValueError("Order quantity rounds down to zero.")
    if normalized != quantity:
        if not allow_round_down:
            raise ValueError(
                "Paper order submit currently requires an integral share quantity. "
                f"Resolved quantity was {quantity}."
            )
        warnings.append(
            "Resolved stock quantity was rounded down to a whole share for execution."
        )
    return normalized, warnings


_INSUFFICIENT_FUNDS_PATTERN = re.compile(
    r"Loan Value \[([0-9.,]+)\s+[A-Z]{3}\].*Initial Margin of\s+\[([0-9.,]+)\s+[A-Z]{3}\]",
    re.IGNORECASE,
)


def _extract_latest_order_error(
    app: Any,
    *,
    order_id: int | None,
) -> dict[str, Any] | None:
    if order_id is None:
        return None
    raw_errors = getattr(app, "errors", None)
    if not isinstance(raw_errors, dict):
        return None
    order_errors = raw_errors.get(order_id)
    if not isinstance(order_errors, list) or not order_errors:
        return None
    latest_error = order_errors[-1]
    if not isinstance(latest_error, dict):
        return None
    return latest_error


def _wait_for_immediate_order_error(
    app: Any,
    *,
    order_id: int | None,
    timeout_seconds: float,
) -> dict[str, Any] | None:
    if order_id is None:
        return None
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    while time.monotonic() < deadline:
        latest_error = _extract_latest_order_error(app, order_id=order_id)
        if latest_error is not None:
            return latest_error
        time.sleep(0.05)
    return _extract_latest_order_error(app, order_id=order_id)


def _parse_broker_decimal(raw_value: str) -> Decimal | None:
    normalized = raw_value.strip().replace(",", "")
    if not normalized:
        return None
    try:
        return Decimal(normalized)
    except Exception:
        return None


def _resize_for_insufficient_funds(
    *,
    quantity: Decimal,
    error_payload: dict[str, Any],
) -> Decimal | None:
    if error_payload.get("errorCode") != 201:
        return None

    error_string = str(error_payload.get("errorString") or "")
    match = _INSUFFICIENT_FUNDS_PATTERN.search(error_string)
    if match is None:
        return None

    available_equity = _parse_broker_decimal(match.group(1))
    required_margin = _parse_broker_decimal(match.group(2))
    if (
        available_equity is None
        or required_margin is None
        or available_equity <= 0
        or required_margin <= 0
    ):
        return None

    resized_quantity = (quantity * available_equity / required_margin).quantize(
        Decimal("1"),
        rounding=ROUND_DOWN,
    )
    if resized_quantity >= quantity:
        resized_quantity = quantity - Decimal("1")
    if resized_quantity <= 0:
        return None
    return resized_quantity


def _build_ibkr_order(
    *,
    action: str,
    order_ref: str,
    ibkr_order_type: str,
    broker_account_id: str,
    quantity: Decimal,
    time_in_force: str,
    limit_price: Decimal | None,
    stop_price: Decimal | None = None,
    oca_group: str | None = None,
    oca_type: int | None = None,
    order_cls: type[Any] | None = None,
) -> Any:
    runtime_order_cls = order_cls or _load_order_class()
    order = runtime_order_cls()
    order.account = broker_account_id
    order.action = action
    order.orderType = ibkr_order_type
    order.totalQuantity = int(quantity)
    order.tif = time_in_force
    order.outsideRth = False
    order.transmit = True
    order.orderRef = order_ref

    if limit_price is not None:
        order.lmtPrice = float(limit_price)
    if stop_price is not None:
        order.auxPrice = float(stop_price)
    if oca_group is not None:
        order.ocaGroup = oca_group
    if oca_type is not None:
        order.ocaType = int(oca_type)

    return order


def _opposite_action(action: str) -> str:
    if action == "BUY":
        return "SELL"
    if action == "SELL":
        return "BUY"
    raise ValueError(f"Unsupported action: {action}")


def _resolve_ibkr_order_type(
    order_type: OrderType | str,
    *,
    limit_price: Decimal | None,
    stop_price: Decimal | None,
) -> str:
    if order_type is OrderType.LIMIT or order_type == "STOP_LIMIT":
        if limit_price is None:
            raise ValueError("limit_price is required for LIMIT orders.")
        if stop_price is not None:
            raise ValueError("stop_price must be omitted for LIMIT orders.")
        return "LMT"

    if order_type is OrderType.MARKET:
        if limit_price is not None:
            raise ValueError("limit_price must be omitted for MARKET orders.")
        if stop_price is not None:
            raise ValueError("stop_price must be omitted for MARKET orders.")
        return "MKT"

    if order_type == "STOP":
        if stop_price is None:
            raise ValueError("stop_price is required for STOP orders.")
        if limit_price is not None:
            raise ValueError("limit_price must be omitted for STOP orders.")
        return "STP"

    raise ValueError(f"Unsupported order type: {order_type}")


def _resolve_instruction_contract_and_account(
    app: OrderExecutionSyncWrapperProtocol,
    config: IbkrConnectionConfig,
    instruction: ExecutionInstruction,
    *,
    timeout: int,
    timeout_cls: type[Exception],
    contract_cls: type[Any] | None,
) -> tuple[str, list[str], dict[str, Any], Any, dict[str, Any], Any]:
    runtime_contract_cls = contract_cls or _load_contract_class()
    normalized_summary = read_account_summary(
        config,
        tags=DEFAULT_ACCOUNT_SUMMARY_TAGS,
        group="All",
        account_id=None,
        timeout=timeout,
        response_timeout_cls=timeout_cls,
        app=app,
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
        contract_matches[0],
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


def _normalize_price_for_order(
    app: OrderExecutionSyncWrapperProtocol,
    raw_detail: Any,
    *,
    exchange: str,
    price: Decimal | None,
    action: str,
    order_type: str,
    timeout: int,
    timeout_cls: type[Exception],
) -> tuple[Decimal | None, Decimal | None]:
    if price is None:
        return None, None

    increment = resolve_price_increment(
        app,
        raw_detail,
        exchange=exchange,
        price=price,
        timeout=timeout,
        timeout_cls=timeout_cls,
    )
    normalized_price = normalize_order_price(
        price=price,
        increment=increment,
        action=action,
        order_type=order_type,
    )
    return normalized_price, increment


def submit_order_from_batch(
    config: IbkrConnectionConfig,
    batch: ExecutionInstructionBatch,
    *,
    timeout: int = 10,
    sync_wrapper_cls: type[OrderExecutionSyncWrapperProtocol] | None = None,
    response_timeout_cls: type[Exception] | None = None,
    contract_cls: type[Any] | None = None,
    order_cls: type[Any] | None = None,
    app: OrderExecutionSyncWrapperProtocol | None = None,
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
        app=app,
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
    app: OrderExecutionSyncWrapperProtocol | None = None,
) -> dict[str, Any]:
    if instruction.entry.order_type is not OrderType.LIMIT:
        raise ValueError("Manual broker order submit currently supports LIMIT orders only.")

    timeout_cls = response_timeout_cls or _load_response_timeout_class()
    runtime_contract_cls = contract_cls or _load_contract_class()
    runtime_order_cls = order_cls or _load_order_class()
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
        (
            broker_account_id,
            account_warnings,
            normalized_summary,
            resolved_ibkr_contract,
            resolved_contract,
            raw_contract_detail,
        ) = _resolve_instruction_contract_and_account(
            runtime_app,
            config,
            instruction,
            timeout=timeout,
            timeout_cls=timeout_cls,
            contract_cls=runtime_contract_cls,
        )

        sizing_preview = _resolve_sizing_preview(
            instruction,
            broker_account_id=broker_account_id,
            normalized_summary=normalized_summary,
            app=runtime_app,
            timeout=timeout,
            timeout_cls=timeout_cls,
            contract_cls=runtime_contract_cls,
            fx_rate_cache={},
        )
        if sizing_preview["issues"]:
            raise ValueError("; ".join(sizing_preview["issues"]))

        normalized_quantity, quantity_warnings = _normalize_quantity_for_stock(
            sizing_preview["estimated_quantity"],
            allow_round_down=instruction.sizing.mode is not SizingMode.TARGET_QUANTITY,
        )
        normalized_limit_price, limit_increment = _normalize_price_for_order(
            runtime_app,
            raw_contract_detail,
            exchange=(
                str(getattr(resolved_ibkr_contract, "exchange", ""))
                or instruction.instrument.exchange
            ),
            price=instruction.entry.limit_price,
            action=instruction.intent.side,
            order_type="LMT",
            timeout=timeout,
            timeout_cls=timeout_cls,
        )
        price_warnings: list[str] = []
        if (
            instruction.entry.limit_price is not None
            and normalized_limit_price is not None
            and normalized_limit_price != instruction.entry.limit_price
        ):
            price_warnings.append(
                "Entry limit price was normalized to the nearest valid IBKR tick increment."
            )
        ibkr_order_type = _resolve_ibkr_order_type(
            instruction.entry.order_type,
            limit_price=normalized_limit_price,
            stop_price=None,
        )
        resize_warnings: list[str] = []
        submitted_quantity = normalized_quantity
        for _ in range(3):
            order = _build_ibkr_order(
                action=instruction.intent.side,
                order_ref=instruction.instruction_id,
                ibkr_order_type=ibkr_order_type,
                broker_account_id=broker_account_id,
                quantity=submitted_quantity,
                time_in_force=instruction.entry.time_in_force.value,
                limit_price=normalized_limit_price,
                order_cls=runtime_order_cls,
            )

            try:
                order_status = runtime_app.place_order_sync(
                    resolved_ibkr_contract,
                    order,
                    timeout=timeout,
                )
            except timeout_cls as exc:
                broker_error = _extract_broker_error_message(runtime_app)
                if broker_error is not None:
                    raise LookupError(
                        f"IBKR rejected the order submission: {broker_error}"
                    ) from exc
                raise TimeoutError("Timed out while placing the IBKR order.") from exc

            immediate_error = _wait_for_immediate_order_error(
                runtime_app,
                order_id=(
                    int(order_status["orderId"])
                    if order_status.get("orderId") not in (None, "")
                    else None
                ),
                timeout_seconds=min(float(timeout), 1.0),
            )
            resized_quantity = (
                _resize_for_insufficient_funds(
                    quantity=submitted_quantity,
                    error_payload=immediate_error,
                )
                if immediate_error is not None
                else None
            )
            if resized_quantity is not None:
                resize_warnings.append(
                    "Entry quantity was reduced after an IBKR insufficient-funds reject."
                )
                submitted_quantity = resized_quantity
                continue
            if immediate_error is not None:
                error_code = immediate_error.get("errorCode")
                error_string = immediate_error.get("errorString") or "Unknown broker error."
                raise LookupError(
                    f"IBKR rejected the order submission: [{error_code}] {error_string}"
                )

            tws_submission = _extract_tws_submission(runtime_app, order_status)
            break
        else:
            raise ValueError(
                "IBKR rejected the order submission after repeated insufficient-funds quantity reductions."
            )
    finally:
        if owns_connection:
            runtime_app.disconnect_and_stop()

    return _serialize_for_json(
        {
            "instruction_id": instruction.instruction_id,
            "account": broker_account_id,
            "warnings": [*list(account_warnings), *quantity_warnings, *price_warnings, *resize_warnings],
            "resolved_contract": resolved_contract,
            "order": {
                "order_ref": instruction.instruction_id,
                "action": instruction.intent.side,
                "order_type": order.orderType,
                "time_in_force": order.tif,
                "limit_price": (
                    str(normalized_limit_price)
                    if normalized_limit_price is not None
                    else None
                ),
                "price_increment": str(limit_increment) if limit_increment is not None else None,
                "total_quantity": str(submitted_quantity),
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
    order_type: OrderType | str,
    order_ref: str,
    timeout: int = 10,
    limit_price: Decimal | None = None,
    stop_price: Decimal | None = None,
    oca_group: str | None = None,
    oca_type: int | None = None,
    sync_wrapper_cls: type[OrderExecutionSyncWrapperProtocol] | None = None,
    response_timeout_cls: type[Exception] | None = None,
    contract_cls: type[Any] | None = None,
    order_cls: type[Any] | None = None,
    app: OrderExecutionSyncWrapperProtocol | None = None,
) -> dict[str, Any]:
    normalized_quantity, quantity_warnings = _normalize_quantity_for_stock(
        quantity,
        allow_round_down=False,
    )
    ibkr_order_type = _resolve_ibkr_order_type(
        order_type,
        limit_price=limit_price,
        stop_price=stop_price,
    )

    timeout_cls = response_timeout_cls or _load_response_timeout_class()
    runtime_contract_cls = contract_cls or _load_contract_class()
    runtime_order_cls = order_cls or _load_order_class()
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
        (
            broker_account_id,
            account_warnings,
            _normalized_summary,
            resolved_ibkr_contract,
            resolved_contract,
            raw_contract_detail,
        ) = _resolve_instruction_contract_and_account(
            runtime_app,
            config,
            instruction,
            timeout=timeout,
            timeout_cls=timeout_cls,
            contract_cls=runtime_contract_cls,
        )

        action = _opposite_action(instruction.intent.side)
        normalized_limit_price, limit_increment = _normalize_price_for_order(
            runtime_app,
            raw_contract_detail,
            exchange=(
                str(getattr(resolved_ibkr_contract, "exchange", ""))
                or instruction.instrument.exchange
            ),
            price=limit_price,
            action=action,
            order_type=ibkr_order_type,
            timeout=timeout,
            timeout_cls=timeout_cls,
        )
        normalized_stop_price, stop_increment = _normalize_price_for_order(
            runtime_app,
            raw_contract_detail,
            exchange=(
                str(getattr(resolved_ibkr_contract, "exchange", ""))
                or instruction.instrument.exchange
            ),
            price=stop_price,
            action=action,
            order_type=ibkr_order_type,
            timeout=timeout,
            timeout_cls=timeout_cls,
        )
        price_warnings: list[str] = []
        if limit_price is not None and normalized_limit_price is not None and normalized_limit_price != limit_price:
            price_warnings.append(
                "Exit limit price was normalized to the nearest valid IBKR tick increment."
            )
        if stop_price is not None and normalized_stop_price is not None and normalized_stop_price != stop_price:
            price_warnings.append(
                "Exit stop price was normalized to the nearest valid IBKR tick increment."
            )

        order = _build_ibkr_order(
            action=action,
            order_ref=order_ref,
            ibkr_order_type=ibkr_order_type,
            broker_account_id=broker_account_id,
            quantity=normalized_quantity,
            time_in_force=instruction.entry.time_in_force.value,
            limit_price=normalized_limit_price,
            stop_price=normalized_stop_price,
            oca_group=oca_group,
            oca_type=oca_type,
            order_cls=runtime_order_cls,
        )

        try:
            order_status = runtime_app.place_order_sync(
                resolved_ibkr_contract,
                order,
                timeout=timeout,
            )
        except timeout_cls as exc:
            broker_error = _extract_broker_error_message(runtime_app)
            if broker_error is not None:
                raise LookupError(
                    f"IBKR rejected the order submission: {broker_error}"
                ) from exc
            raise TimeoutError("Timed out while placing the IBKR order.") from exc
        tws_submission = _extract_tws_submission(runtime_app, order_status)
    finally:
        if owns_connection:
            runtime_app.disconnect_and_stop()

    return _serialize_for_json(
        {
            "instruction_id": instruction.instruction_id,
            "account": broker_account_id,
            "warnings": [*list(account_warnings), *quantity_warnings, *price_warnings],
            "resolved_contract": resolved_contract,
            "order": {
                "order_ref": order_ref,
                "action": order.action,
                "order_type": order.orderType,
                "time_in_force": order.tif,
                "limit_price": str(normalized_limit_price) if normalized_limit_price is not None else None,
                "stop_price": str(normalized_stop_price) if normalized_stop_price is not None else None,
                "limit_price_increment": (
                    str(limit_increment) if limit_increment is not None else None
                ),
                "stop_price_increment": (
                    str(stop_increment) if stop_increment is not None else None
                ),
                "total_quantity": str(normalized_quantity),
                "outside_rth": order.outsideRth,
                "oca_group": oca_group,
                "oca_type": oca_type,
                "transmit": order.transmit,
            },
            "broker_order_status": order_status,
            "tws_submission": tws_submission,
        }
    )


def cancel_broker_order(
    config: IbkrConnectionConfig,
    order_id: int,
    *,
    timeout: int = 10,
    sync_wrapper_cls: type[OrderExecutionSyncWrapperProtocol] | None = None,
    response_timeout_cls: type[Exception] | None = None,
    app: OrderExecutionSyncWrapperProtocol | None = None,
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
            order_status = runtime_app.cancel_order_sync(order_id, timeout=timeout)
        except timeout_cls as exc:
            broker_error = _extract_broker_error_message(runtime_app)
            if broker_error is not None:
                if f"[{_ORDER_CANCEL_NOT_FOUND_CODE}]" in broker_error:
                    return _serialize_for_json(
                        {
                            "broker_order_status": {
                                "orderId": order_id,
                                "status": "NOT_FOUND_AT_BROKER",
                            },
                            "warning": (
                                "IBKR reported that the order was already absent at cancel time."
                            ),
                        }
                    )
                raise LookupError(
                    f"IBKR rejected the order cancel request: {broker_error}"
                ) from exc
            raise TimeoutError("Timed out while cancelling the IBKR order.") from exc
    finally:
        if owns_connection:
            runtime_app.disconnect_and_stop()

    return _serialize_for_json({"broker_order_status": order_status})
