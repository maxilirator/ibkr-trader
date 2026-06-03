from __future__ import annotations

from dataclasses import asdict
from decimal import Decimal
from decimal import ROUND_CEILING
from decimal import ROUND_DOWN
import re
import time
from typing import Any, Protocol, runtime_checkable

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.domain.contract_resolution import ContractResolveQuery
from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.domain.execution_contract import ExecutionInstructionBatch
from ibkr_trader.domain.execution_contract import FundingBasis
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
from ibkr_trader.ibkr.order_preview import _default_cash_entry_reserve
from ibkr_trader.ibkr.order_preview import _resolve_sizing_preview
from ibkr_trader.ibkr.order_preview import _select_broker_account_id
from ibkr_trader.ibkr.price_rules import normalize_order_price
from ibkr_trader.ibkr.price_rules import resolve_price_increment
from ibkr_trader.ibkr.short_sale_validation import ShortSaleValidationError
from ibkr_trader.ibkr.short_sale_validation import validate_short_sale_entry
from ibkr_trader.ibkr.errors import IbkrDependencyError

_ORDER_CANCEL_NOT_FOUND_CODE = 10147
_ORDER_CANCEL_ALREADY_CANCELLED_CODE = 10148
_NON_FATAL_ORDER_WARNING_CODES = {399}
_ORDER_ALREADY_CANCELLED_RE = re.compile(
    rf"\[{_ORDER_CANCEL_ALREADY_CANCELLED_CODE}\].*state:\s*Cancelled\b",
    re.IGNORECASE,
)
_DEFERRED_EXCHANGE_WARNING_RE = re.compile(
    r"will\s+not\s+be\s+placed\s+at\s+the\s+exchange\s+until\s+"
    r"(?P<date>\d{4}-\d{2}-\d{2})",
    re.IGNORECASE,
)
_BLANK_WHATIF_CANCEL_RE = re.compile(
    r"^\s*Order\s+Canceled\s+-\s+reason:\s*$",
    re.IGNORECASE,
)


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

    def get_positions(self, timeout: int = 10) -> dict[str, list[Any]]: ...

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

    def preview_order_sync(
        self,
        contract: Any,
        order: Any,
        timeout: int | None = None,
    ) -> dict[str, Any]: ...

    def place_order_sync(self, contract: Any, order: Any, timeout: int | None = None) -> dict[str, Any]: ...

    def cancel_order_sync(
        self,
        order_id: int,
        orderCancel: Any | None = None,
        timeout: int = 3,
    ) -> dict[str, Any]: ...


def _latest_broker_error_for_request(
    app: Any,
    request_id: Any,
) -> dict[str, Any] | None:
    if request_id in (None, ""):
        return None
    try:
        normalized_request_id = int(request_id)
    except (TypeError, ValueError):
        return None

    raw_errors = getattr(app, "errors", {})
    if not isinstance(raw_errors, dict):
        return None
    request_errors = raw_errors.get(normalized_request_id) or []
    if not request_errors:
        return None
    latest_error = request_errors[-1]
    if not isinstance(latest_error, dict):
        return None
    return latest_error


def _format_broker_error(error_payload: dict[str, Any] | None) -> str | None:
    if error_payload is None:
        return None
    error_code = error_payload.get("errorCode")
    error_string = error_payload.get("errorString")
    if error_code is None or not error_string:
        return None
    return f"[{error_code}] {error_string}"


def _is_blank_whatif_cancel_error(error_payload: dict[str, Any] | None) -> bool:
    if error_payload is None:
        return False
    if error_payload.get("errorCode") != 202:
        return False
    error_string = str(error_payload.get("errorString") or "")
    advanced_reject = str(error_payload.get("advancedOrderRejectJson") or "").strip()
    return not advanced_reject and _BLANK_WHATIF_CANCEL_RE.match(error_string) is not None


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
        "what_if": (
            bool(getattr(order, "whatIf"))
            if getattr(order, "whatIf", None) is not None
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
            "equity_with_loan_before": (
                str(getattr(order_state, "equityWithLoanBefore"))
                if getattr(order_state, "equityWithLoanBefore", None) not in (None, "")
                else None
            ),
            "equity_with_loan_change": (
                str(getattr(order_state, "equityWithLoanChange"))
                if getattr(order_state, "equityWithLoanChange", None) not in (None, "")
                else None
            ),
            "equity_with_loan_after": (
                str(getattr(order_state, "equityWithLoanAfter"))
                if getattr(order_state, "equityWithLoanAfter", None) not in (None, "")
                else None
            ),
            "initial_margin_before": (
                str(getattr(order_state, "initMarginBefore"))
                if getattr(order_state, "initMarginBefore", None) not in (None, "")
                else None
            ),
            "initial_margin_change": (
                str(getattr(order_state, "initMarginChange"))
                if getattr(order_state, "initMarginChange", None) not in (None, "")
                else None
            ),
            "initial_margin_after": (
                str(getattr(order_state, "initMarginAfter"))
                if getattr(order_state, "initMarginAfter", None) not in (None, "")
                else None
            ),
            "commission": (
                str(getattr(order_state, "commission"))
                if getattr(order_state, "commission", None) not in (None, "")
                else None
            ),
            "minimum_commission": (
                str(getattr(order_state, "minCommission"))
                if getattr(order_state, "minCommission", None) not in (None, "")
                else None
            ),
            "maximum_commission": (
                str(getattr(order_state, "maxCommission"))
                if getattr(order_state, "maxCommission", None) not in (None, "")
                else None
            ),
            "commission_currency": (
                str(getattr(order_state, "commissionCurrency"))
                if getattr(order_state, "commissionCurrency", None) not in (None, "")
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


def _broker_wire_audit_event_count(app: Any) -> int | None:
    counter = getattr(app, "broker_wire_audit_event_count", None)
    if not callable(counter):
        return None
    try:
        return int(counter())
    except Exception:
        return None


def _broker_wire_audit_events_since(
    app: Any,
    offset: int | None,
    *,
    order_ref: str | None = None,
) -> list[dict[str, Any]]:
    reader = getattr(app, "broker_wire_audit_events_since", None)
    if not callable(reader) or offset is None:
        return []
    try:
        raw_events = reader(offset)
    except Exception:
        return []
    if not isinstance(raw_events, list):
        return []
    events = [
        event
        for event in raw_events
        if isinstance(event, dict)
    ]
    if order_ref is None:
        return _serialize_for_json(events)
    filtered_events: list[dict[str, Any]] = []
    for event in events:
        request = event.get("request")
        if not isinstance(request, dict):
            continue
        order = request.get("order")
        if not isinstance(order, dict):
            if request.get("api_method") == "cancelOrder":
                filtered_events.append(event)
            continue
        if order.get("order_ref") == order_ref:
            filtered_events.append(event)
    return _serialize_for_json(filtered_events)


def _attach_broker_wire_audit(
    exc: Exception,
    app: Any,
    offset: int | None,
    *,
    order_ref: str | None,
) -> None:
    wire_audit = _broker_wire_audit_events_since(app, offset, order_ref=order_ref)
    if wire_audit:
        setattr(exc, "ibkr_wire_audit", wire_audit)


def _require_single_instruction(batch: ExecutionInstructionBatch) -> ExecutionInstruction:
    if len(batch.instructions) != 1:
        raise ValueError("Paper order submit currently supports exactly one instruction.")
    return batch.instructions[0]


def _is_unacceptable_deferred_entry_warning(
    warning_text: str,
    instruction: ExecutionInstruction,
) -> bool:
    match = _DEFERRED_EXCHANGE_WARNING_RE.search(warning_text)
    if match is None:
        return False
    warning_date = match.group("date")
    entry_date = instruction.entry.submit_at.date().isoformat()
    return warning_date != entry_date


def _cancel_after_unacceptable_entry_warning(
    app: OrderExecutionSyncWrapperProtocol,
    *,
    order_status: dict[str, Any],
    timeout: int,
) -> None:
    order_id = order_status.get("orderId")
    if order_id in (None, ""):
        return
    app.cancel_order_sync(
        int(order_id),
        timeout=max(1, min(timeout, 5)),
    )


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


def _parse_optional_decimal(raw_value: Any) -> Decimal | None:
    if raw_value in (None, ""):
        return None
    return _parse_broker_decimal(str(raw_value))


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
    what_if: bool = False,
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
    order.whatIf = what_if

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


def _is_already_cancelled_order_error(broker_error: str) -> bool:
    return _ORDER_ALREADY_CANCELLED_RE.search(broker_error) is not None


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
        broker_error = _extract_broker_error_message(
            app,
            include_known_order_ids=True,
        )
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


def _is_cash_backed_long_entry(
    instruction: ExecutionInstruction,
    *,
    funding_basis: FundingBasis,
) -> bool:
    return (
        instruction.intent.side == "BUY"
        and instruction.intent.position_side.value == "LONG"
        and funding_basis is FundingBasis.CASH
    )


def _extract_whatif_metric(raw_preview: dict[str, Any], field_name: str) -> Decimal | None:
    order_state = raw_preview.get("orderState")
    if order_state is None:
        return None
    return _parse_optional_decimal(getattr(order_state, field_name, None))


def _extract_whatif_commission_estimate(raw_preview: dict[str, Any]) -> Decimal | None:
    commission_values = [
        _extract_whatif_metric(raw_preview, "commission"),
        _extract_whatif_metric(raw_preview, "minCommission"),
        _extract_whatif_metric(raw_preview, "maxCommission"),
    ]
    positive_values = [value for value in commission_values if value is not None and value > 0]
    if not positive_values:
        return None
    return max(positive_values)


def _extract_whatif_headroom_after(raw_preview: dict[str, Any]) -> Decimal | None:
    equity_with_loan_after = _extract_whatif_metric(raw_preview, "equityWithLoanAfter")
    initial_margin_after = _extract_whatif_metric(raw_preview, "initMarginAfter")
    if equity_with_loan_after is None or initial_margin_after is None:
        return None
    return equity_with_loan_after - initial_margin_after


def _required_cash_entry_buffer(
    *,
    sizing_preview: dict[str, Any],
    raw_preview: dict[str, Any],
) -> Decimal:
    configured_reserve = sizing_preview.get("cash_reserve")
    cash_currency = sizing_preview.get("cash_currency") or sizing_preview.get("account_currency")
    required_buffer = (
        configured_reserve
        if isinstance(configured_reserve, Decimal) and configured_reserve > 0
        else _default_cash_entry_reserve(cash_currency)
    )
    commission_estimate = _extract_whatif_commission_estimate(raw_preview)
    if commission_estimate is not None and commission_estimate > required_buffer:
        required_buffer = commission_estimate
    return required_buffer


def _price_per_share_in_cash_currency(
    *,
    limit_price: Decimal | None,
    instruction: ExecutionInstruction,
    sizing_preview: dict[str, Any],
) -> Decimal | None:
    if limit_price is None:
        return None
    cash_currency = sizing_preview.get("cash_currency") or sizing_preview.get("account_currency")
    if cash_currency == instruction.instrument.currency:
        return limit_price
    fx_conversion = sizing_preview.get("fx_conversion")
    if fx_conversion is None:
        return None
    conversion_rate = getattr(fx_conversion, "rate", None)
    if not isinstance(conversion_rate, Decimal) or conversion_rate <= 0:
        return None
    return limit_price / conversion_rate


def _resize_quantity_for_cash_buffer(
    *,
    quantity: Decimal,
    headroom_after: Decimal,
    required_buffer: Decimal,
    limit_price: Decimal | None,
    instruction: ExecutionInstruction,
    sizing_preview: dict[str, Any],
) -> Decimal | None:
    shortfall = required_buffer - headroom_after
    if shortfall <= 0:
        return quantity

    per_share_cost = _price_per_share_in_cash_currency(
        limit_price=limit_price,
        instruction=instruction,
        sizing_preview=sizing_preview,
    )
    if per_share_cost is None or per_share_cost <= 0:
        resized_quantity = quantity - Decimal("1")
    else:
        shares_to_free = (shortfall / per_share_cost).to_integral_value(
            rounding=ROUND_CEILING,
        )
        if shares_to_free <= 0:
            shares_to_free = Decimal("1")
        resized_quantity = quantity - shares_to_free

    if resized_quantity >= quantity:
        resized_quantity = quantity - Decimal("1")
    if resized_quantity <= 0:
        return None
    return resized_quantity


def _preflight_cash_backed_entry_quantity(
    app: OrderExecutionSyncWrapperProtocol,
    *,
    instruction: ExecutionInstruction,
    resolved_ibkr_contract: Any,
    broker_account_id: str,
    quantity: Decimal,
    ibkr_order_type: str,
    normalized_limit_price: Decimal | None,
    timeout: int,
    timeout_cls: type[Exception],
    sizing_preview: dict[str, Any],
    order_cls: type[Any],
) -> tuple[Decimal, list[str], dict[str, Any] | None]:
    warnings: list[str] = []
    last_preview: dict[str, Any] | None = None
    preview_quantity = quantity

    for _ in range(5):
        preview_order = _build_ibkr_order(
            action=instruction.intent.side,
            order_ref=instruction.instruction_id,
            ibkr_order_type=ibkr_order_type,
            broker_account_id=broker_account_id,
            quantity=preview_quantity,
            time_in_force=instruction.entry.time_in_force.value,
            limit_price=normalized_limit_price,
            what_if=True,
            order_cls=order_cls,
        )
        try:
            raw_preview = app.preview_order_sync(
                resolved_ibkr_contract,
                preview_order,
                timeout=timeout,
            )
        except timeout_cls as exc:
            broker_error_payload = _latest_broker_error_for_request(
                app,
                getattr(preview_order, "orderId", None),
            )
            broker_error = _format_broker_error(broker_error_payload)
            if _is_blank_whatif_cancel_error(broker_error_payload):
                warnings.append(
                    "IBKR emitted a blank 202 cancellation notice during the WhatIf preflight; continuing with cash-reserve sizing."
                )
                return preview_quantity, warnings, last_preview
            if broker_error is not None:
                warnings.append(
                    f"IBKR rejected the WhatIf preflight ({broker_error}); continuing with cash-reserve sizing."
                )
                return preview_quantity, warnings, last_preview
            warnings.append(
                "Timed out while requesting the IBKR WhatIf order preview; continuing with cash-reserve sizing."
            )
            return preview_quantity, warnings, last_preview

        last_preview = _serialize_tws_submission(raw_preview)
        warning_text = str(
            getattr(raw_preview.get("orderState"), "warningText", "") or ""
        ).strip()
        if warning_text:
            warnings.append(f"IBKR WhatIf warning: {warning_text}")

        headroom_after = _extract_whatif_headroom_after(raw_preview)
        required_buffer = _required_cash_entry_buffer(
            sizing_preview=sizing_preview,
            raw_preview=raw_preview,
        )
        if headroom_after is None:
            warnings.append(
                "IBKR WhatIf preview did not return post-trade headroom metrics; relying on cash-reserve sizing."
            )
            return preview_quantity, warnings, last_preview
        if headroom_after >= required_buffer:
            if preview_quantity != quantity:
                warnings.append(
                    "Entry quantity was reduced after the IBKR WhatIf preview left too little post-trade cash cushion."
                )
            return preview_quantity, warnings, last_preview

        resized_quantity = _resize_quantity_for_cash_buffer(
            quantity=preview_quantity,
            headroom_after=headroom_after,
            required_buffer=required_buffer,
            limit_price=normalized_limit_price,
            instruction=instruction,
            sizing_preview=sizing_preview,
        )
        if resized_quantity is None:
            raise ValueError(
                "IBKR WhatIf preview indicates there is no safe cash-backed quantity after reserve and commission buffers."
            )
        preview_quantity = resized_quantity

    raise ValueError(
        "IBKR WhatIf preview could not find a quantity that leaves enough post-trade cash cushion."
    )



__all__ = [name for name in globals() if not name.startswith("__")]
