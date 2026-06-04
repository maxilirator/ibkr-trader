from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from datetime import timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from ibkr_trader.db.models import AccountSnapshotRecord
from ibkr_trader.db.models import BrokerAccountRecord
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import ExecutionFillRecord
from ibkr_trader.db.models import PositionSnapshotRecord
from ibkr_trader.ibkr.runtime_snapshot import BrokerPortfolioItem
from ibkr_trader.ibkr.runtime_snapshot import BrokerPosition
from ibkr_trader.ibkr.runtime_snapshot import BrokerRuntimeSnapshot
from ibkr_trader.ledger.persistence_shared import _broker_order_lineage_changed
from ibkr_trader.ledger.persistence_shared import _decimal_to_string
from ibkr_trader.ledger.persistence_shared import _derive_account_base_currency
from ibkr_trader.ledger.persistence_shared import _execution_side_is_exit_for_instruction
from ibkr_trader.ledger.persistence_shared import _find_active_exit_instruction_for_execution
from ibkr_trader.ledger.persistence_shared import _find_broker_order
from ibkr_trader.ledger.persistence_shared import _find_instruction_record_for_order
from ibkr_trader.ledger.persistence_shared import _get_or_create_broker_account
from ibkr_trader.ledger.persistence_shared import _is_virtual_ledger_identity
from ibkr_trader.ledger.persistence_shared import _normalize_text
from ibkr_trader.ledger.persistence_shared import _order_role_for_execution
from ibkr_trader.ledger.persistence_shared import _record_broker_order_event
from ibkr_trader.ledger.persistence_shared import _require_text
from ibkr_trader.ledger.persistence_shared import _resolve_account_key
from ibkr_trader.ledger.persistence_shared import _retire_reused_external_order_id
from ibkr_trader.ledger.persistence_shared import _serialize_for_json
from ibkr_trader.ledger.persistence_shared import _to_decimal

def _persist_account_snapshots(
    session: Session,
    *,
    broker_kind: str,
    snapshot: BrokerRuntimeSnapshot,
    captured_at: datetime,
    default_account_key: str | None,
) -> dict[str, BrokerAccountRecord]:
    broker_accounts: dict[str, BrokerAccountRecord] = {}
    for account_key, values in snapshot.account_values.items():
        normalized_account_key = _resolve_account_key(
            account_key,
            default_account_key=default_account_key,
            context="Account snapshot",
        )
        if not isinstance(values, dict):
            raise ValueError(
                f"Account snapshot for '{normalized_account_key}' was not a mapping."
            )
        broker_account = _get_or_create_broker_account(
            session,
            broker_kind=broker_kind,
            account_key=normalized_account_key,
            base_currency=_derive_account_base_currency(values),
        )
        broker_accounts[normalized_account_key] = broker_account

        def read_value(tag: str) -> str | None:
            payload = values.get(tag)
            if not isinstance(payload, dict):
                return None
            value = payload.get("value")
            return str(value) if value not in (None, "") else None

        currency = None
        for payload in values.values():
            if isinstance(payload, dict) and payload.get("currency") not in (None, ""):
                currency = str(payload["currency"])
                break

        session.add(
            AccountSnapshotRecord(
                broker_account_id=broker_account.id,
                is_virtual=_is_virtual_ledger_identity(
                    broker_kind=broker_kind,
                    account_key=normalized_account_key,
                ),
                snapshot_at=captured_at,
                source="runtime_snapshot",
                net_liquidation=read_value("NetLiquidation"),
                total_cash_value=read_value("TotalCashValue"),
                buying_power=read_value("BuyingPower"),
                available_funds=read_value("AvailableFunds"),
                excess_liquidity=read_value("ExcessLiquidity"),
                cushion=read_value("Cushion"),
                currency=currency,
                raw_payload=_serialize_for_json(values),
            )
        )
    return broker_accounts


def _build_position_union(
    snapshot: BrokerRuntimeSnapshot,
    *,
    default_account_key: str | None,
) -> dict[
    tuple[str, str, str, str, str | None, str | None],
    tuple[BrokerPosition | None, BrokerPortfolioItem | None],
]:
    positions_by_key: dict[
        tuple[str, str, str, str, str | None, str | None],
        tuple[BrokerPosition | None, BrokerPortfolioItem | None],
    ] = {}

    def merge_position(
        existing: BrokerPosition,
        incoming: BrokerPosition,
    ) -> BrokerPosition:
        return BrokerPosition(
            account=incoming.account or existing.account,
            symbol=incoming.symbol or existing.symbol,
            local_symbol=incoming.local_symbol or existing.local_symbol,
            security_type=incoming.security_type or existing.security_type,
            exchange=incoming.exchange or existing.exchange,
            primary_exchange=incoming.primary_exchange or existing.primary_exchange,
            currency=incoming.currency or existing.currency,
            position=(
                incoming.position
                if incoming.position is not None
                else existing.position
            ),
            average_cost=(
                incoming.average_cost
                if incoming.average_cost is not None
                else existing.average_cost
            ),
        )

    def merge_portfolio_item(
        existing: BrokerPortfolioItem,
        incoming: BrokerPortfolioItem,
    ) -> BrokerPortfolioItem:
        return BrokerPortfolioItem(
            account=incoming.account or existing.account,
            symbol=incoming.symbol or existing.symbol,
            local_symbol=incoming.local_symbol or existing.local_symbol,
            security_type=incoming.security_type or existing.security_type,
            exchange=incoming.exchange or existing.exchange,
            primary_exchange=incoming.primary_exchange or existing.primary_exchange,
            currency=incoming.currency or existing.currency,
            position=(
                incoming.position
                if incoming.position is not None
                else existing.position
            ),
            market_price=(
                incoming.market_price
                if incoming.market_price is not None
                else existing.market_price
            ),
            market_value=(
                incoming.market_value
                if incoming.market_value is not None
                else existing.market_value
            ),
            average_cost=(
                incoming.average_cost
                if incoming.average_cost is not None
                else existing.average_cost
            ),
            unrealized_pnl=(
                incoming.unrealized_pnl
                if incoming.unrealized_pnl is not None
                else existing.unrealized_pnl
            ),
            realized_pnl=(
                incoming.realized_pnl
                if incoming.realized_pnl is not None
                else existing.realized_pnl
            ),
        )

    for position in snapshot.positions:
        account_key = _resolve_account_key(
            position.account,
            default_account_key=default_account_key,
            context=f"Position for symbol {position.symbol or '<missing>'}",
        )
        key = (
            account_key,
            _require_text(position.symbol, context="Position symbol"),
            _require_text(position.exchange, context="Position exchange"),
            _require_text(position.currency, context="Position currency"),
            _normalize_text(position.security_type),
            _normalize_text(position.local_symbol),
        )
        previous_position, previous_portfolio = positions_by_key.get(key, (None, None))
        positions_by_key[key] = (
            merge_position(previous_position, position)
            if previous_position is not None
            else position,
            previous_portfolio,
        )

    for portfolio_item in snapshot.portfolio:
        account_key = _resolve_account_key(
            portfolio_item.account,
            default_account_key=default_account_key,
            context=f"Portfolio item for symbol {portfolio_item.symbol or '<missing>'}",
        )
        key = (
            account_key,
            _require_text(portfolio_item.symbol, context="Portfolio symbol"),
            _require_text(portfolio_item.exchange, context="Portfolio exchange"),
            _require_text(portfolio_item.currency, context="Portfolio currency"),
            _normalize_text(portfolio_item.security_type),
            _normalize_text(portfolio_item.local_symbol),
        )
        previous_position, previous_portfolio = positions_by_key.get(key, (None, None))
        positions_by_key[key] = (
            previous_position,
            merge_portfolio_item(previous_portfolio, portfolio_item)
            if previous_portfolio is not None
            else portfolio_item,
        )

    return positions_by_key


def _persist_position_snapshots(
    session: Session,
    *,
    broker_kind: str,
    snapshot: BrokerRuntimeSnapshot,
    captured_at: datetime,
    default_account_key: str | None,
    broker_accounts: dict[str, BrokerAccountRecord],
) -> None:
    for key, (position, portfolio_item) in _build_position_union(
        snapshot,
        default_account_key=default_account_key,
    ).items():
        account_key, symbol, exchange, currency, security_type, local_symbol = key
        quantity_candidates = [
            item
            for item in (
                position.position if position is not None else None,
                portfolio_item.position if portfolio_item is not None else None,
            )
            if item is not None
        ]
        if not quantity_candidates:
            raise ValueError(
                f"Position snapshot for {account_key}:{symbol}:{exchange} did not include a quantity."
            )
        if len(quantity_candidates) == 2 and quantity_candidates[0] != quantity_candidates[1]:
            raise ValueError(
                f"Position quantity mismatch for {account_key}:{symbol}:{exchange}: "
                f"{quantity_candidates[0]} != {quantity_candidates[1]}."
            )
        quantity = quantity_candidates[0]

        broker_account = broker_accounts.get(account_key)
        if broker_account is None:
            broker_account = _get_or_create_broker_account(
                session,
                broker_kind=broker_kind,
                account_key=account_key,
                base_currency=currency,
            )
            broker_accounts[account_key] = broker_account

        session.add(
            PositionSnapshotRecord(
                broker_account_id=broker_account.id,
                is_virtual=_is_virtual_ledger_identity(
                    broker_kind=broker_kind,
                    account_key=account_key,
                ),
                snapshot_at=captured_at,
                source="runtime_snapshot",
                symbol=symbol,
                exchange=exchange,
                currency=currency,
                security_type=_require_text(
                    security_type,
                    context=f"Position security type for {account_key}:{symbol}:{exchange}",
                ),
                primary_exchange=(
                    _normalize_text(position.primary_exchange)
                    if position is not None
                    else _normalize_text(
                        portfolio_item.primary_exchange if portfolio_item is not None else None
                    )
                ),
                local_symbol=local_symbol,
                quantity=str(quantity),
                average_cost=_decimal_to_string(
                    position.average_cost if position is not None else None
                )
                or _decimal_to_string(
                    portfolio_item.average_cost if portfolio_item is not None else None
                ),
                market_price=_decimal_to_string(
                    portfolio_item.market_price if portfolio_item is not None else None
                ),
                market_value=_decimal_to_string(
                    portfolio_item.market_value if portfolio_item is not None else None
                ),
                unrealized_pnl=_decimal_to_string(
                    portfolio_item.unrealized_pnl if portfolio_item is not None else None
                ),
                realized_pnl=_decimal_to_string(
                    portfolio_item.realized_pnl if portfolio_item is not None else None
                ),
                raw_payload=_serialize_for_json(
                    {
                        "position": asdict(position) if position is not None else None,
                        "portfolio": (
                            asdict(portfolio_item) if portfolio_item is not None else None
                        ),
                    }
                ),
            )
        )


def _execution_time_from_order_status_fill(
    broker_order: BrokerOrderRecord | None,
    execution_shares: Decimal | None,
) -> datetime | None:
    if broker_order is None or broker_order.last_status_at is None:
        return None
    status_payload = broker_order.metadata_json.get("last_order_status_callback")
    if not isinstance(status_payload, dict):
        return None
    status = _normalize_text(
        str(status_payload.get("status"))
        if status_payload.get("status") not in (None, "")
        else None
    )
    if status is None or status.upper() != "FILLED":
        return None
    filled_quantity = _to_decimal(status_payload.get("filled")) or Decimal("0")
    if filled_quantity <= 0:
        return None
    if execution_shares is not None and execution_shares > 0:
        remaining_quantity = _to_decimal(status_payload.get("remaining"))
        if filled_quantity < execution_shares and remaining_quantity != Decimal("0"):
            return None
    if broker_order.last_status_at.tzinfo is None:
        return broker_order.last_status_at.replace(tzinfo=timezone.utc)
    return broker_order.last_status_at.astimezone(timezone.utc)


def _resolve_execution_time(
    execution: Any,
    *,
    broker_order: BrokerOrderRecord | None,
    captured_at: datetime,
) -> tuple[datetime, dict[str, Any]]:
    if execution.executed_at is not None:
        return execution.executed_at, {}
    order_status_fill_at = _execution_time_from_order_status_fill(
        broker_order,
        execution.shares,
    )
    if order_status_fill_at is not None:
        return order_status_fill_at, {
            "executed_at_inferred_from_order_status_callback": True,
            "order_status_callback_at": order_status_fill_at.isoformat(),
            "snapshot_captured_at": captured_at.isoformat(),
        }
    # IBKR occasionally omits execution.time on fills that are otherwise
    # complete. Use the snapshot capture time only as the final fallback, while
    # retaining raw broker payload provenance so operators know it is not the
    # exchange execution timestamp.
    return captured_at, {
        "executed_at_inferred_from_snapshot_capture": True,
        "snapshot_captured_at": captured_at.isoformat(),
    }


def _persist_executions(
    session: Session,
    *,
    broker_kind: str,
    snapshot: BrokerRuntimeSnapshot,
    captured_at: datetime,
    default_account_key: str | None,
    broker_accounts: dict[str, BrokerAccountRecord],
) -> None:
    for execution in snapshot.executions:
        exec_id = _require_text(
            execution.exec_id,
            context="Execution identifier",
        )
        account_key = _resolve_account_key(
            execution.account,
            default_account_key=default_account_key,
            context=f"Execution {exec_id}",
        )
        broker_account = broker_accounts.get(account_key)
        if broker_account is None:
            broker_account = _get_or_create_broker_account(
                session,
                broker_kind=broker_kind,
                account_key=account_key,
                base_currency=_normalize_text(execution.currency),
            )
            broker_accounts[account_key] = broker_account

        existing_fill = session.execute(
            select(ExecutionFillRecord).where(
                ExecutionFillRecord.broker_kind == broker_kind,
                ExecutionFillRecord.account_key == account_key,
                ExecutionFillRecord.external_execution_id == exec_id,
            )
        ).scalar_one_or_none()
        if existing_fill is not None:
            if execution.commission is not None:
                existing_fill.commission = _decimal_to_string(execution.commission)
            if _normalize_text(execution.commission_currency) is not None:
                existing_fill.commission_currency = _normalize_text(
                    execution.commission_currency
                )
            existing_fill.raw_payload = {
                **existing_fill.raw_payload,
                **_serialize_for_json(asdict(execution)),
            }
            continue

        external_order_id = (
            str(execution.order_id) if execution.order_id is not None else None
        )
        external_perm_id = (
            str(execution.perm_id) if execution.perm_id is not None else None
        )
        instruction_record = _find_instruction_record_for_order(
            session,
            order_ref=execution.order_ref,
            external_order_id=external_order_id,
            external_perm_id=external_perm_id,
        )
        broker_order = _find_broker_order(
            session,
            broker_kind=broker_kind,
            account_key=account_key,
            external_order_id=external_order_id,
            external_perm_id=external_perm_id,
            order_ref=execution.order_ref,
        )
        if broker_order is not None and _broker_order_lineage_changed(
            broker_order,
            external_perm_id=external_perm_id,
            order_ref=execution.order_ref,
            symbol=execution.symbol,
            local_symbol=execution.local_symbol,
        ):
            _retire_reused_external_order_id(
                session,
                broker_order=broker_order,
                retired_at=execution.executed_at or captured_at,
                replacement_external_order_id=external_order_id,
                replacement_external_perm_id=external_perm_id,
                replacement_order_ref=execution.order_ref,
                replacement_symbol=execution.symbol,
                replacement_local_symbol=execution.local_symbol,
            )
            broker_order = None

        executed_at, execution_time_metadata = _resolve_execution_time(
            execution,
            broker_order=broker_order,
            captured_at=captured_at,
        )
        symbol = _normalize_text(execution.symbol) or (
            broker_order.symbol if broker_order is not None else None
        )
        currency = _normalize_text(execution.currency) or (
            broker_order.currency if broker_order is not None else None
        )
        security_type = _normalize_text(execution.security_type) or (
            broker_order.security_type if broker_order is not None else None
        )
        exchange = _normalize_text(execution.exchange) or (
            broker_order.exchange if broker_order is not None else None
        )
        local_symbol = _normalize_text(execution.local_symbol) or (
            broker_order.local_symbol if broker_order is not None else None
        )
        primary_exchange = _normalize_text(execution.primary_exchange) or (
            broker_order.primary_exchange if broker_order is not None else None
        )
        if instruction_record is None:
            instruction_record = _find_active_exit_instruction_for_execution(
                session,
                account_key=account_key,
                symbol=symbol,
                local_symbol=local_symbol,
                currency=currency,
                security_type=security_type,
                execution_side=execution.side,
            )
        resolved_order_role = _order_role_for_execution(
            execution_order_ref=execution.order_ref,
            execution_side=execution.side,
            instruction_record=instruction_record,
        )

        if broker_order is None:
            if external_order_id is None:
                raise ValueError(
                    f"Execution {exec_id} could not be linked to a broker order and did not expose an order id."
                )
            broker_order = BrokerOrderRecord(
                instruction_id=instruction_record.id if instruction_record is not None else None,
                broker_account_id=broker_account.id,
                broker_kind=broker_kind,
                account_key=account_key,
                is_virtual=_is_virtual_ledger_identity(
                    broker_kind=broker_kind,
                    account_key=account_key,
                ),
                order_role=resolved_order_role,
                external_order_id=external_order_id,
                external_perm_id=external_perm_id,
                external_client_id=(
                    str(execution.client_id) if execution.client_id is not None else None
                ),
                order_ref=_normalize_text(execution.order_ref),
                symbol=_require_text(
                    symbol,
                    context=f"Execution {exec_id} symbol",
                ),
                exchange=_require_text(
                    exchange,
                    context=f"Execution {exec_id} exchange",
                ),
                currency=_require_text(
                    currency,
                    context=f"Execution {exec_id} currency",
                ),
                security_type=_require_text(
                    security_type,
                    context=f"Execution {exec_id} security type",
                ),
                primary_exchange=primary_exchange,
                local_symbol=local_symbol,
                side=_normalize_text(execution.side) or "UNKNOWN",
                order_type="UNKNOWN",
                time_in_force=None,
                status="FILLED",
                total_quantity=_decimal_to_string(execution.shares),
                limit_price=None,
                stop_price=None,
                submitted_at=executed_at,
                last_status_at=executed_at,
                raw_payload=_serialize_for_json(asdict(execution)),
                metadata_json={},
            )
            session.add(broker_order)
            session.flush()
            _record_broker_order_event(
                session,
                broker_order=broker_order,
                event_type="execution_observed_without_open_order",
                event_at=executed_at,
                status_before=None,
                status_after="FILLED",
                payload=_serialize_for_json(asdict(execution)),
                note="Created broker order record from execution because no open-order ledger row existed.",
            )
        else:
            if instruction_record is not None:
                broker_order.instruction_id = instruction_record.id
            if broker_order.order_role != resolved_order_role:
                metadata = dict(broker_order.metadata_json or {})
                metadata["order_role_reclassified_at"] = captured_at.isoformat()
                metadata["order_role_reclassified_from"] = broker_order.order_role
                metadata["order_role_reclassified_to"] = resolved_order_role
                metadata["order_role_reclassified_reason"] = (
                    "Execution matched an active instruction exit side."
                )
                broker_order.metadata_json = _serialize_for_json(metadata)
                broker_order.order_role = resolved_order_role
            previous_status = broker_order.status
            broker_order.status = "FILLED"
            broker_order.last_status_at = executed_at
            if previous_status != broker_order.status:
                _record_broker_order_event(
                    session,
                    broker_order=broker_order,
                    event_type="execution_fill_observed",
                    event_at=executed_at,
                    status_before=previous_status,
                    status_after=broker_order.status,
                    payload=_serialize_for_json(asdict(execution)),
                    note="Observed broker execution and marked the durable order as filled.",
                )
        fill_raw_payload = {
            **_serialize_for_json(asdict(execution)),
            **execution_time_metadata,
        }

        session.add(
            ExecutionFillRecord(
                broker_order_id=broker_order.id,
                instruction_id=instruction_record.id if instruction_record is not None else None,
                broker_account_id=broker_account.id,
                broker_kind=broker_kind,
                account_key=account_key,
                is_virtual=_is_virtual_ledger_identity(
                    broker_kind=broker_kind,
                    account_key=account_key,
                ),
                external_execution_id=exec_id,
                external_order_id=external_order_id,
                external_perm_id=external_perm_id,
                order_ref=_normalize_text(execution.order_ref),
                symbol=_require_text(symbol, context=f"Execution {exec_id} symbol"),
                exchange=exchange,
                currency=_require_text(currency, context=f"Execution {exec_id} currency"),
                security_type=_require_text(
                    security_type,
                    context=f"Execution {exec_id} security type",
                ),
                side=_normalize_text(execution.side),
                quantity=_require_text(
                    _decimal_to_string(execution.shares),
                    context=f"Execution {exec_id} quantity",
                ),
                price=_require_text(
                    _decimal_to_string(execution.price),
                    context=f"Execution {exec_id} price",
                ),
                commission=_decimal_to_string(execution.commission),
                commission_currency=_normalize_text(execution.commission_currency),
                executed_at=executed_at,
                raw_payload=fill_raw_payload,
            )
        )
