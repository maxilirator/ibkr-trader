from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol, runtime_checkable

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.ibkr.contracts import _extract_broker_error_message
from ibkr_trader.ibkr.order_preview import _load_response_timeout_class
from ibkr_trader.ibkr.order_preview import _load_sync_wrapper_class


@runtime_checkable
class RuntimeSnapshotSyncWrapperProtocol(Protocol):
    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool: ...

    def disconnect_and_stop(self) -> None: ...

    def get_open_orders(self, timeout: int = 3) -> dict[int, Any]: ...

    def get_executions(self, exec_filter: Any | None = None, timeout: int = 10) -> list[Any]: ...


@dataclass(slots=True)
class BrokerOpenOrder:
    order_id: int
    perm_id: int | None
    client_id: int | None
    status: str | None
    order_ref: str | None
    action: str | None
    total_quantity: Decimal | None
    symbol: str | None
    account: str | None = None
    security_type: str | None = None
    exchange: str | None = None
    primary_exchange: str | None = None
    currency: str | None = None
    local_symbol: str | None = None
    order_type: str | None = None
    limit_price: Decimal | None = None
    aux_price: Decimal | None = None
    outside_rth: bool | None = None
    oca_group: str | None = None
    oca_type: int | None = None
    transmit: bool | None = None
    warning_text: str | None = None
    reject_reason: str | None = None
    completed_status: str | None = None
    completed_time: str | None = None


@dataclass(slots=True)
class BrokerExecution:
    exec_id: str | None
    order_id: int | None
    perm_id: int | None
    client_id: int | None
    order_ref: str | None
    side: str | None
    shares: Decimal | None
    price: Decimal | None
    exchange: str | None
    executed_at: datetime | None
    symbol: str | None


@dataclass(slots=True)
class BrokerRuntimeSnapshot:
    open_orders: dict[int, BrokerOpenOrder]
    executions: tuple[BrokerExecution, ...]


def _serialize_for_json(payload: Any) -> Any:
    if isinstance(payload, Decimal):
        return str(payload)
    if isinstance(payload, datetime):
        return payload.isoformat()
    if isinstance(payload, dict):
        return {key: _serialize_for_json(value) for key, value in payload.items()}
    if isinstance(payload, list):
        return [_serialize_for_json(value) for value in payload]
    if isinstance(payload, tuple):
        return [_serialize_for_json(value) for value in payload]
    return payload


def serialize_broker_runtime_snapshot(snapshot: BrokerRuntimeSnapshot) -> dict[str, Any]:
    return _serialize_for_json(
        {
            "open_orders": [asdict(order) for order in snapshot.open_orders.values()],
            "executions": [asdict(execution) for execution in snapshot.executions],
        }
    )


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _to_optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_ibkr_execution_time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    raw_value = str(value).strip()
    for fmt in ("%Y%m%d  %H:%M:%S", "%Y%m%d-%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw_value, fmt)
        except ValueError:
            continue
    return None


def _serialize_open_order(raw_payload: Any) -> BrokerOpenOrder | None:
    if not isinstance(raw_payload, dict):
        return None
    order_id = _to_optional_int(raw_payload.get("orderId"))
    if order_id is None:
        return None

    order = raw_payload.get("order")
    contract = raw_payload.get("contract")
    order_state = raw_payload.get("orderState")
    return BrokerOpenOrder(
        order_id=order_id,
        perm_id=_to_optional_int(getattr(order, "permId", None)),
        client_id=_to_optional_int(getattr(order, "clientId", None)),
        status=(
            str(getattr(order_state, "status"))
            if getattr(order_state, "status", None) not in (None, "")
            else None
        ),
        order_ref=(
            str(getattr(order, "orderRef"))
            if getattr(order, "orderRef", None) not in (None, "")
            else None
        ),
        action=(
            str(getattr(order, "action"))
            if getattr(order, "action", None) not in (None, "")
            else None
        ),
        total_quantity=_to_decimal(getattr(order, "totalQuantity", None)),
        symbol=(
            str(getattr(contract, "symbol"))
            if getattr(contract, "symbol", None) not in (None, "")
            else None
        ),
        account=(
            str(getattr(order, "account"))
            if getattr(order, "account", None) not in (None, "")
            else None
        ),
        security_type=(
            str(getattr(contract, "secType"))
            if getattr(contract, "secType", None) not in (None, "")
            else None
        ),
        exchange=(
            str(getattr(contract, "exchange"))
            if getattr(contract, "exchange", None) not in (None, "")
            else None
        ),
        primary_exchange=(
            str(getattr(contract, "primaryExchange"))
            if getattr(contract, "primaryExchange", None) not in (None, "")
            else None
        ),
        currency=(
            str(getattr(contract, "currency"))
            if getattr(contract, "currency", None) not in (None, "")
            else None
        ),
        local_symbol=(
            str(getattr(contract, "localSymbol"))
            if getattr(contract, "localSymbol", None) not in (None, "")
            else None
        ),
        order_type=(
            str(getattr(order, "orderType"))
            if getattr(order, "orderType", None) not in (None, "")
            else None
        ),
        limit_price=_to_decimal(getattr(order, "lmtPrice", None)),
        aux_price=_to_decimal(getattr(order, "auxPrice", None)),
        outside_rth=(
            bool(getattr(order, "outsideRth"))
            if getattr(order, "outsideRth", None) is not None
            else None
        ),
        oca_group=(
            str(getattr(order, "ocaGroup"))
            if getattr(order, "ocaGroup", None) not in (None, "")
            else None
        ),
        oca_type=_to_optional_int(getattr(order, "ocaType", None)),
        transmit=(
            bool(getattr(order, "transmit"))
            if getattr(order, "transmit", None) is not None
            else None
        ),
        warning_text=(
            str(getattr(order_state, "warningText"))
            if getattr(order_state, "warningText", None) not in (None, "")
            else None
        ),
        reject_reason=(
            str(getattr(order_state, "rejectReason"))
            if getattr(order_state, "rejectReason", None) not in (None, "")
            else None
        ),
        completed_status=(
            str(getattr(order_state, "completedStatus"))
            if getattr(order_state, "completedStatus", None) not in (None, "")
            else None
        ),
        completed_time=(
            str(getattr(order_state, "completedTime"))
            if getattr(order_state, "completedTime", None) not in (None, "")
            else None
        ),
    )


def _serialize_execution(raw_payload: Any) -> BrokerExecution | None:
    if not isinstance(raw_payload, dict):
        return None
    execution = raw_payload.get("execution")
    contract = raw_payload.get("contract")
    if execution is None:
        return None
    return BrokerExecution(
        exec_id=(
            str(getattr(execution, "execId"))
            if getattr(execution, "execId", None) not in (None, "")
            else None
        ),
        order_id=_to_optional_int(getattr(execution, "orderId", None)),
        perm_id=_to_optional_int(getattr(execution, "permId", None)),
        client_id=_to_optional_int(getattr(execution, "clientId", None)),
        order_ref=(
            str(getattr(execution, "orderRef"))
            if getattr(execution, "orderRef", None) not in (None, "")
            else None
        ),
        side=(
            str(getattr(execution, "side"))
            if getattr(execution, "side", None) not in (None, "")
            else None
        ),
        shares=_to_decimal(getattr(execution, "shares", None)),
        price=_to_decimal(getattr(execution, "price", None)),
        exchange=(
            str(getattr(execution, "exchange"))
            if getattr(execution, "exchange", None) not in (None, "")
            else None
        ),
        executed_at=_parse_ibkr_execution_time(getattr(execution, "time", None)),
        symbol=(
            str(getattr(contract, "symbol"))
            if getattr(contract, "symbol", None) not in (None, "")
            else None
        ),
    )


def fetch_broker_runtime_snapshot(
    config: IbkrConnectionConfig,
    *,
    timeout: int = 10,
    sync_wrapper_cls: type[RuntimeSnapshotSyncWrapperProtocol] | None = None,
    response_timeout_cls: type[Exception] | None = None,
) -> BrokerRuntimeSnapshot:
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
            raw_open_orders = app.get_open_orders(timeout=timeout)
            raw_executions = app.get_executions(timeout=timeout)
        except timeout_cls as exc:
            broker_error = _extract_broker_error_message(app)
            if broker_error is not None:
                raise LookupError(
                    f"IBKR rejected the runtime snapshot request: {broker_error}"
                ) from exc
            raise TimeoutError("Timed out while requesting the IBKR runtime snapshot.") from exc
    finally:
        app.disconnect_and_stop()

    open_orders: dict[int, BrokerOpenOrder] = {}
    for raw_order in (raw_open_orders or {}).values():
        serialized = _serialize_open_order(raw_order)
        if serialized is not None:
            open_orders[serialized.order_id] = serialized

    executions: list[BrokerExecution] = []
    for raw_execution in raw_executions or []:
        serialized = _serialize_execution(raw_execution)
        if serialized is not None:
            executions.append(serialized)

    return BrokerRuntimeSnapshot(
        open_orders=open_orders,
        executions=tuple(executions),
    )
