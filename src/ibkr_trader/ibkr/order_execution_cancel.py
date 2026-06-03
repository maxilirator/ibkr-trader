from __future__ import annotations

from ibkr_trader.ibkr.order_execution_common import *

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
    wire_audit_start = _broker_wire_audit_event_count(runtime_app)

    try:
        try:
            order_status = runtime_app.cancel_order_sync(order_id, timeout=timeout)
        except timeout_cls as exc:
            broker_error = _extract_broker_error_message(
                runtime_app,
                include_known_order_ids=True,
            )
            if broker_error is not None:
                if f"[{_ORDER_CANCEL_NOT_FOUND_CODE}]" in broker_error:
                    return _serialize_for_json(
                        {
                            "broker_order_status": {
                                "orderId": order_id,
                                "status": "NOT_FOUND_AT_BROKER",
                            },
                            "ibkr_wire_audit": _broker_wire_audit_events_since(
                                runtime_app,
                                wire_audit_start,
                            ),
                            "warning": (
                                "IBKR reported that the order was already absent at cancel time."
                            ),
                        }
                    )
                if _is_already_cancelled_order_error(broker_error):
                    return _serialize_for_json(
                        {
                            "broker_order_status": {
                                "orderId": order_id,
                                "status": "Cancelled",
                            },
                            "ibkr_wire_audit": _broker_wire_audit_events_since(
                                runtime_app,
                                wire_audit_start,
                            ),
                            "warning": (
                                "IBKR reported that the order was already cancelled at cancel time."
                            ),
                        }
                    )
                raise LookupError(
                    f"IBKR rejected the order cancel request: {broker_error}"
                ) from exc
            raise TimeoutError("Timed out while cancelling the IBKR order.") from exc
    except Exception as exc:
        _attach_broker_wire_audit(
            exc,
            runtime_app,
            wire_audit_start,
            order_ref=None,
        )
        raise
    finally:
        if owns_connection:
            runtime_app.disconnect_and_stop()

    return _serialize_for_json(
        {
            "broker_order_status": order_status,
            "ibkr_wire_audit": _broker_wire_audit_events_since(
                runtime_app,
                wire_audit_start,
            ),
        }
    )

__all__ = [name for name in globals() if not name.startswith("__")]
