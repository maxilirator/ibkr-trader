from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import ExecutionFillRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.ibkr.runtime_snapshot import BrokerExecution
from ibkr_trader.orchestration.runtime_types import ExecutionAggregate
from ibkr_trader.orchestration.runtime_types import ensure_utc
from ibkr_trader.orchestration.runtime_types import parse_decimal


def aggregate_executions(
    executions: tuple[BrokerExecution, ...],
    *,
    order_id: int | None = None,
    order_ref_exact: str | None = None,
    order_ref_prefix: str | None = None,
) -> ExecutionAggregate:
    """Combine raw broker execution callbacks into one deduplicated fill view."""

    seen_exec_ids: set[str] = set()
    matched: list[BrokerExecution] = []
    for execution in executions:
        if order_id is not None and execution.order_id == order_id:
            pass
        elif order_ref_exact is not None and execution.order_ref == order_ref_exact:
            pass
        elif (
            order_ref_prefix is not None
            and execution.order_ref is not None
            and execution.order_ref.startswith(order_ref_prefix)
        ):
            pass
        else:
            continue

        dedupe_key = execution.exec_id or (
            f"{execution.order_id}:{execution.executed_at}:{execution.shares}:{execution.price}"
        )
        if dedupe_key in seen_exec_ids:
            continue
        seen_exec_ids.add(dedupe_key)
        matched.append(execution)

    if not matched:
        return ExecutionAggregate()

    total_quantity = Decimal("0")
    weighted_notional = Decimal("0")
    last_execution_at: datetime | None = None
    for execution in matched:
        shares = parse_decimal(str(execution.shares) if execution.shares is not None else None)
        if shares <= 0:
            continue
        total_quantity += shares
        price = parse_decimal(str(execution.price) if execution.price is not None else None)
        if price > 0:
            weighted_notional += price * shares
        if execution.executed_at is not None and (
            last_execution_at is None or execution.executed_at > last_execution_at
        ):
            last_execution_at = execution.executed_at

    average_price = None
    if total_quantity > 0 and weighted_notional > 0:
        average_price = weighted_notional / total_quantity

    return ExecutionAggregate(
        quantity=total_quantity,
        average_price=average_price,
        executed_at=ensure_utc(last_execution_at),
        execution_count=len(matched),
    )


def aggregate_broker_order_status_fill(
    session_factory: sessionmaker[Session],
    *,
    record: InstructionRecord,
    order_role: str,
    external_order_id: int | None = None,
) -> ExecutionAggregate:
    """Use terminal broker order-status callbacks as fill evidence.

    IBKR can report orderStatus=Filled before reqExecutions returns data. This
    fallback keeps runtime state moving while preserving the execution ledger for
    true execution rows.
    """

    with session_scope(session_factory) as session:
        statement = select(BrokerOrderRecord).where(
            BrokerOrderRecord.instruction_id == record.id,
            BrokerOrderRecord.order_role == order_role,
        )
        if external_order_id is not None:
            statement = statement.where(
                BrokerOrderRecord.external_order_id == str(external_order_id)
            )
        broker_orders = session.execute(statement).scalars().all()

    seen_order_keys: set[str] = set()
    total_quantity = Decimal("0")
    weighted_notional = Decimal("0")
    last_execution_at: datetime | None = None
    matched_count = 0

    for broker_order in broker_orders:
        order_key = (
            broker_order.external_order_id
            or broker_order.external_perm_id
            or f"broker-order:{broker_order.id}"
        )
        if order_key in seen_order_keys:
            continue
        seen_order_keys.add(order_key)

        status_payload = broker_order.metadata_json.get("last_order_status_callback")
        if not isinstance(status_payload, dict):
            continue

        filled_quantity = parse_decimal(
            str(status_payload.get("filled"))
            if status_payload.get("filled") not in (None, "")
            else None
        )
        if filled_quantity <= 0:
            continue

        average_fill_price = parse_decimal(
            str(status_payload.get("avgFillPrice"))
            if status_payload.get("avgFillPrice") not in (None, "")
            else None
        )
        if average_fill_price <= 0:
            average_fill_price = parse_decimal(
                str(status_payload.get("lastFillPrice"))
                if status_payload.get("lastFillPrice") not in (None, "")
                else None
            )

        total_quantity += filled_quantity
        if average_fill_price > 0:
            weighted_notional += average_fill_price * filled_quantity
        matched_count += 1
        if broker_order.last_status_at is not None and (
            last_execution_at is None or broker_order.last_status_at > last_execution_at
        ):
            last_execution_at = broker_order.last_status_at

    average_price = None
    if total_quantity > 0 and weighted_notional > 0:
        average_price = weighted_notional / total_quantity

    return ExecutionAggregate(
        quantity=total_quantity,
        average_price=average_price,
        executed_at=ensure_utc(last_execution_at),
        execution_count=matched_count,
    )


def aggregate_persisted_execution_fill(
    session_factory: sessionmaker[Session],
    *,
    record: InstructionRecord,
    order_role: str,
    external_order_id: int | None = None,
) -> ExecutionAggregate:
    """Aggregate already-persisted execution fill rows for one instruction role."""

    with session_scope(session_factory) as session:
        statement = select(ExecutionFillRecord).where(
            ExecutionFillRecord.instruction_id == record.id,
        )
        if external_order_id is not None:
            statement = statement.where(
                ExecutionFillRecord.external_order_id == str(external_order_id)
            )
        elif order_role == "ENTRY":
            entry_predicates = [
                ExecutionFillRecord.order_ref == record.instruction_id,
            ]
            if record.broker_order_id is not None:
                entry_predicates.append(
                    ExecutionFillRecord.external_order_id == str(record.broker_order_id)
                )
            statement = statement.where(or_(*entry_predicates))
        else:
            exit_predicates = [
                ExecutionFillRecord.order_ref.like(f"{record.instruction_id}:exit:%"),
            ]
            if record.exit_order_id is not None:
                exit_predicates.append(
                    ExecutionFillRecord.external_order_id == str(record.exit_order_id)
                )
            statement = statement.where(or_(*exit_predicates))

        fills = session.execute(statement).scalars().all()

    seen_execution_ids: set[str] = set()
    total_quantity = Decimal("0")
    weighted_notional = Decimal("0")
    last_execution_at: datetime | None = None
    matched_count = 0

    for fill in fills:
        execution_id = fill.external_execution_id or f"execution-fill:{fill.id}"
        if execution_id in seen_execution_ids:
            continue
        seen_execution_ids.add(execution_id)

        quantity = parse_decimal(fill.quantity)
        if quantity <= 0:
            continue

        price = parse_decimal(fill.price)
        total_quantity += quantity
        if price > 0:
            weighted_notional += price * quantity
        matched_count += 1
        if fill.executed_at is not None and (
            last_execution_at is None or fill.executed_at > last_execution_at
        ):
            last_execution_at = fill.executed_at

    average_price = None
    if total_quantity > 0 and weighted_notional > 0:
        average_price = weighted_notional / total_quantity

    return ExecutionAggregate(
        quantity=total_quantity,
        average_price=average_price,
        executed_at=ensure_utc(last_execution_at),
        execution_count=matched_count,
    )
