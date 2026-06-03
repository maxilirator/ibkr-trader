from __future__ import annotations

from ibkr_trader.ibkr.order_execution_common import *

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
    wire_audit_start = _broker_wire_audit_event_count(runtime_app)

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
        short_sale_validation = validate_short_sale_entry(
            app=runtime_app,
            instruction=instruction,
            broker_account_id=broker_account_id,
            normalized_summary=normalized_summary,
            requested_quantity=normalized_quantity,
            timeout=timeout,
            timeout_cls=timeout_cls,
            contract_cls=runtime_contract_cls,
        )
        if short_sale_validation.issues:
            raise ShortSaleValidationError("; ".join(short_sale_validation.issues))
        normalized_limit_price = instruction.entry.limit_price
        limit_increment = None
        if instruction.entry.order_type is OrderType.LIMIT:
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
        broker_warnings: list[str] = []
        submitted_quantity = normalized_quantity
        whatif_preview = None
        funding_basis = sizing_preview["funding_basis"]
        if _is_cash_backed_long_entry(
            instruction,
            funding_basis=funding_basis,
        ):
            submitted_quantity, whatif_warnings, whatif_preview = _preflight_cash_backed_entry_quantity(
                runtime_app,
                instruction=instruction,
                resolved_ibkr_contract=resolved_ibkr_contract,
                broker_account_id=broker_account_id,
                quantity=submitted_quantity,
                ibkr_order_type=ibkr_order_type,
                normalized_limit_price=normalized_limit_price,
                timeout=timeout,
                timeout_cls=timeout_cls,
                sizing_preview=sizing_preview,
                order_cls=runtime_order_cls,
            )
            broker_warnings.extend(whatif_warnings)
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
                broker_error = _extract_broker_error_message(
                    runtime_app,
                    include_known_order_ids=True,
                )
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
                if error_code in _NON_FATAL_ORDER_WARNING_CODES:
                    if _is_unacceptable_deferred_entry_warning(
                        error_string,
                        instruction,
                    ):
                        try:
                            _cancel_after_unacceptable_entry_warning(
                                runtime_app,
                                order_status=order_status,
                                timeout=timeout,
                            )
                        except Exception as cancel_exc:
                            raise LookupError(
                                "IBKR accepted the order but deferred exchange "
                                "activation beyond the entry trade date, and the "
                                f"automatic cancel failed: {cancel_exc}; "
                                f"original warning [{error_code}]: {error_string}"
                            ) from cancel_exc
                        raise LookupError(
                            "IBKR accepted the order but deferred exchange activation "
                            "beyond the entry trade date; the order was cancelled. "
                            f"Original warning [{error_code}]: {error_string}"
                        )
                    broker_warnings.append(
                        f"IBKR order warning [{error_code}]: {error_string}"
                    )
                    tws_submission = _extract_tws_submission(runtime_app, order_status)
                    break
                raise LookupError(
                    f"IBKR rejected the order submission: [{error_code}] {error_string}"
                )

            tws_submission = _extract_tws_submission(runtime_app, order_status)
            break
        else:
            raise ValueError(
                "IBKR rejected the order submission after repeated insufficient-funds quantity reductions."
            )
    except Exception as exc:
        _attach_broker_wire_audit(
            exc,
            runtime_app,
            wire_audit_start,
            order_ref=instruction.instruction_id,
        )
        raise
    finally:
        if owns_connection:
            runtime_app.disconnect_and_stop()

    ibkr_wire_audit = _broker_wire_audit_events_since(
        runtime_app,
        wire_audit_start,
        order_ref=instruction.instruction_id,
    )
    return _serialize_for_json(
        {
            "instruction_id": instruction.instruction_id,
            "account": broker_account_id,
            "warnings": [
                *list(account_warnings),
                *list(sizing_preview["warnings"]),
                *quantity_warnings,
                *list(short_sale_validation.warnings),
                *price_warnings,
                *resize_warnings,
                *broker_warnings,
            ],
            "short_sale_validation": {
                "is_short_sale": short_sale_validation.is_short_sale,
                "current_position_quantity": short_sale_validation.current_position_quantity,
                "requested_quantity": short_sale_validation.requested_quantity,
                "account_type": short_sale_validation.account_type,
                "leverage": short_sale_validation.leverage,
                "net_liquidation": short_sale_validation.net_liquidation,
                "net_liquidation_currency": short_sale_validation.net_liquidation_currency,
                "net_liquidation_eur": short_sale_validation.net_liquidation_eur,
                "stockholm_shortability_status": short_sale_validation.stockholm_shortability_status,
                "stockholm_shortability_as_of_date": short_sale_validation.stockholm_shortability_as_of_date,
            },
            "resolved_contract": resolved_contract,
            "whatif_preview": whatif_preview,
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
            "ibkr_wire_audit": ibkr_wire_audit,
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
    wire_audit_start = _broker_wire_audit_event_count(runtime_app)

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
            broker_error = _extract_broker_error_message(
                runtime_app,
                include_known_order_ids=True,
            )
            if broker_error is not None:
                raise LookupError(
                    f"IBKR rejected the order submission: {broker_error}"
                ) from exc
            raise TimeoutError("Timed out while placing the IBKR order.") from exc
        tws_submission = _extract_tws_submission(runtime_app, order_status)
    except Exception as exc:
        _attach_broker_wire_audit(
            exc,
            runtime_app,
            wire_audit_start,
            order_ref=order_ref,
        )
        raise
    finally:
        if owns_connection:
            runtime_app.disconnect_and_stop()

    ibkr_wire_audit = _broker_wire_audit_events_since(
        runtime_app,
        wire_audit_start,
        order_ref=order_ref,
    )
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
            "ibkr_wire_audit": ibkr_wire_audit,
        }
    )



__all__ = [name for name in globals() if not name.startswith("__")]
