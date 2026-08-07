from __future__ import annotations

from ibkr_trader.read_models.operator_dashboard_common import *

def _derive_order_purpose(broker_order: BrokerOrderRecord) -> str | None:
    order_ref = (broker_order.order_ref or "").strip()
    if ":exit:" in order_ref:
        suffix = order_ref.rsplit(":exit:", 1)[1].strip()
        if suffix == "take_profit":
            return "Take Profit"
        if suffix == "catastrophic_stop":
            return "Catastrophic Stop"
        if suffix == "delayed_limit":
            return "Delayed Limit"
        if suffix == "manual_flatten":
            return "Manual Flatten"
        if suffix == "force_exit_next_session_open":
            return "Next Open Exit"
        return suffix.replace("_", " ").title()

    normalized_role = (broker_order.order_role or "").strip().upper()
    if normalized_role == "ENTRY":
        return "Entry"
    if normalized_role == "EXIT":
        return "Exit"
    return broker_order.order_role


def _resolve_working_price(
    broker_order: BrokerOrderRecord,
) -> tuple[Decimal | None, str | None]:
    order_type = (broker_order.order_type or "").strip().upper()
    limit_price = _meaningful_decimal(broker_order.limit_price)
    stop_price = _meaningful_decimal(broker_order.stop_price)

    if order_type.startswith("STP"):
        if stop_price is not None:
            return stop_price, "STOP"
        if limit_price is not None:
            return limit_price, "LIMIT"
        return None, None

    if limit_price is not None:
        return limit_price, "LIMIT"
    if stop_price is not None:
        return stop_price, "STOP"
    return None, None


def _exit_fill_basis(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
) -> tuple[str | None, datetime | None, str | None, str | None]:
    if broker_order.instruction_id is None:
        return None, None, None, None
    if (broker_order.order_role or "").strip().upper() != "EXIT":
        return None, None, None, None

    rows = session.execute(
        select(ExecutionFillRecord)
        .where(ExecutionFillRecord.instruction_id == broker_order.instruction_id)
        .order_by(
            ExecutionFillRecord.executed_at.asc(),
            ExecutionFillRecord.id.asc(),
        )
    ).scalars()

    total_quantity = Decimal("0")
    weighted_notional = Decimal("0")
    latest_fill_at: datetime | None = None

    for fill in rows:
        order_ref = (fill.order_ref or "").strip()
        if ":exit:" in order_ref:
            continue

        quantity = _meaningful_decimal(fill.quantity)
        price = _meaningful_decimal(fill.price)
        if quantity is None or price is None:
            continue

        total_quantity += quantity
        weighted_notional += quantity * price
        latest_fill_at = fill.executed_at

    if total_quantity <= 0:
        return None, None, None, None

    basis_price = weighted_notional / total_quantity
    working_price, _ = _resolve_working_price(broker_order)
    if working_price is None:
        return _format_decimal(basis_price, places="0.00000001"), latest_fill_at, None, None

    fill_spread = working_price - basis_price
    fill_spread_pct = (fill_spread / basis_price) * Decimal("100") if basis_price != 0 else None
    return (
        _format_decimal(basis_price, places="0.00000001"),
        latest_fill_at,
        _format_signed_decimal(fill_spread, places="0.01"),
        _format_signed_decimal(fill_spread_pct, places="0.01")
        if fill_spread_pct is not None
        else None,
    )


def _fill_order_role(
    fill: ExecutionFillRecord,
    broker_order: BrokerOrderRecord | None,
) -> str | None:
    broker_order_role = (broker_order.order_role or "").strip().upper() if broker_order else ""
    if broker_order_role:
        return broker_order_role
    order_ref = (fill.order_ref or "").strip().lower()
    if ":exit:" in order_ref:
        return "EXIT"
    if order_ref:
        return "ENTRY"
    return None


def _instruction_position_side(instruction: InstructionRecord | None) -> str | None:
    if instruction is None:
        return None
    payload = instruction.payload if isinstance(instruction.payload, dict) else {}
    instruction_payload = payload.get("instruction", {})
    if isinstance(instruction_payload, dict):
        intent_payload = instruction_payload.get("intent", {})
        if isinstance(intent_payload, dict):
            position_side = str(intent_payload.get("position_side") or "").strip().upper()
            if position_side in {"LONG", "SHORT"}:
                return position_side

    normalized_side = (instruction.side or "").strip().upper()
    if normalized_side == "BUY":
        return "LONG"
    if normalized_side == "SELL":
        return "SHORT"
    return None


def _fill_position_side(
    fill: ExecutionFillRecord,
    broker_order: BrokerOrderRecord | None,
    instruction: InstructionRecord | None,
) -> str | None:
    instruction_side = _instruction_position_side(instruction)
    if instruction_side is not None:
        return instruction_side

    order_role = _fill_order_role(fill, broker_order)
    normalized_side = (fill.side or (broker_order.side if broker_order else "") or "").strip().upper()
    if order_role == "EXIT":
        if normalized_side in {"SLD", "SELL"}:
            return "LONG"
        if normalized_side in {"BOT", "BUY"}:
            return "SHORT"
    else:
        if normalized_side in {"BOT", "BUY"}:
            return "LONG"
        if normalized_side in {"SLD", "SELL"}:
            return "SHORT"
    return None


def _commission_cost(
    value: str | None,
    *,
    commission_currency: str | None,
    pnl_currency: str,
) -> Decimal:
    if (commission_currency or "").strip().upper() != pnl_currency.upper():
        return Decimal("0")
    parsed = _to_decimal(value)
    return abs(parsed) if parsed is not None else Decimal("0")


def _entry_fill_basis_for_instruction(
    session: Session,
    *,
    instruction_id: int,
    pnl_currency: str,
) -> tuple[Decimal | None, Decimal, Decimal, str | None]:
    instruction = session.get(InstructionRecord, instruction_id)
    entry_action = (instruction.side or "").strip().upper() if instruction else ""
    expected_entry_sides: set[str] | None = None
    inferred_entry_side: str | None = None
    if entry_action == "BUY":
        expected_entry_sides = {"BOT", "BUY"}
        inferred_entry_side = "LONG"
    elif entry_action == "SELL":
        expected_entry_sides = {"SLD", "SELL"}
        inferred_entry_side = "SHORT"

    rows = session.execute(
        select(ExecutionFillRecord, BrokerOrderRecord)
        .outerjoin(
            BrokerOrderRecord,
            BrokerOrderRecord.id == ExecutionFillRecord.broker_order_id,
        )
        .where(ExecutionFillRecord.instruction_id == instruction_id)
        .order_by(
            ExecutionFillRecord.executed_at.asc(),
            ExecutionFillRecord.id.asc(),
        )
    ).all()

    total_quantity = Decimal("0")
    weighted_notional = Decimal("0")
    commission_total = Decimal("0")
    entry_side: str | None = inferred_entry_side
    for entry_fill, broker_order in rows:
        normalized_side = (entry_fill.side or "").strip().upper()
        if expected_entry_sides is not None:
            if normalized_side not in expected_entry_sides:
                continue
        elif _fill_order_role(entry_fill, broker_order) == "EXIT":
            continue
        quantity = _meaningful_decimal(entry_fill.quantity)
        price = _meaningful_decimal(entry_fill.price)
        if quantity is None or price is None:
            continue

        abs_quantity = abs(quantity)
        total_quantity += abs_quantity
        weighted_notional += abs_quantity * price
        commission_total += _commission_cost(
            entry_fill.commission,
            commission_currency=entry_fill.commission_currency,
            pnl_currency=pnl_currency,
        )
        if normalized_side in {"BOT", "BUY"}:
            entry_side = entry_side or "LONG"
        elif normalized_side in {"SLD", "SELL"}:
            entry_side = entry_side or "SHORT"

    if total_quantity <= 0:
        return None, Decimal("0"), Decimal("0"), entry_side
    return weighted_notional / total_quantity, total_quantity, commission_total, entry_side


def _position_snapshot_basis_for_exit_fill(
    session: Session,
    *,
    fill: ExecutionFillRecord,
    broker_order: BrokerOrderRecord | None,
) -> tuple[Decimal | None, str | None]:
    normalized_side = (
        fill.side or (broker_order.side if broker_order else "") or ""
    ).strip().upper()
    if normalized_side in {"SLD", "SELL"}:
        position_side = "LONG"
    elif normalized_side in {"BOT", "BUY"}:
        position_side = "SHORT"
    else:
        return None, None

    rows = session.execute(
        select(PositionSnapshotRecord)
        .where(
            PositionSnapshotRecord.broker_account_id == fill.broker_account_id,
            PositionSnapshotRecord.symbol == fill.symbol,
            PositionSnapshotRecord.currency == fill.currency,
            PositionSnapshotRecord.security_type == fill.security_type,
            PositionSnapshotRecord.snapshot_at <= fill.executed_at,
        )
        .order_by(
            PositionSnapshotRecord.snapshot_at.desc(),
            PositionSnapshotRecord.id.desc(),
        )
        .limit(16)
    ).scalars()

    for position_snapshot in rows:
        if broker_order is not None and not _position_snapshot_matches_order(
            position_snapshot,
            broker_order=broker_order,
        ):
            continue

        position_quantity = _to_decimal(position_snapshot.quantity)
        if position_quantity is None:
            continue
        if position_side == "LONG" and position_quantity <= 0:
            continue
        if position_side == "SHORT" and position_quantity >= 0:
            continue

        average_cost = _meaningful_decimal(position_snapshot.average_cost)
        if average_cost is None:
            continue
        return abs(average_cost), position_side

    return None, position_side


def _fill_realized_pnl(
    session: Session,
    *,
    fill: ExecutionFillRecord,
    broker_order: BrokerOrderRecord | None,
    instruction: InstructionRecord | None,
) -> tuple[str | None, str | None, str | None, str | None]:
    if _fill_order_role(fill, broker_order) != "EXIT":
        return None, None, None, None

    instruction_id = (
        fill.instruction_id
        or (broker_order.instruction_id if broker_order is not None else None)
    )
    pnl_currency = fill.currency
    basis_price: Decimal | None = None
    entry_quantity = Decimal("0")
    entry_commission = Decimal("0")
    entry_side: str | None = None
    if instruction_id is not None:
        basis_price, entry_quantity, entry_commission, entry_side = _entry_fill_basis_for_instruction(
            session,
            instruction_id=instruction_id,
            pnl_currency=pnl_currency,
        )

    exit_quantity = _meaningful_decimal(fill.quantity)
    exit_price = _meaningful_decimal(fill.price)
    if exit_quantity is None or exit_price is None:
        return None, None, pnl_currency, None

    position_side = _instruction_position_side(instruction) or entry_side
    if basis_price is None or position_side is None:
        snapshot_basis_price, snapshot_position_side = _position_snapshot_basis_for_exit_fill(
            session,
            fill=fill,
            broker_order=broker_order,
        )
        if basis_price is None:
            basis_price = snapshot_basis_price
            entry_quantity = abs(exit_quantity)
            entry_commission = Decimal("0")
        position_side = position_side or snapshot_position_side

    if basis_price is None:
        return None, None, pnl_currency, None

    if position_side == "LONG":
        gross_pnl = (exit_price - basis_price) * abs(exit_quantity)
    elif position_side == "SHORT":
        gross_pnl = (basis_price - exit_price) * abs(exit_quantity)
    else:
        return None, None, pnl_currency, _format_decimal(
            basis_price,
            places="0.00000001",
        )

    prorated_entry_commission = (
        entry_commission * (abs(exit_quantity) / entry_quantity)
        if entry_quantity > 0
        else Decimal("0")
    )
    exit_commission = _commission_cost(
        fill.commission,
        commission_currency=fill.commission_currency,
        pnl_currency=pnl_currency,
    )
    net_pnl = gross_pnl - prorated_entry_commission - exit_commission
    return (
        _format_signed_decimal(net_pnl, places="0.01"),
        _format_signed_decimal(gross_pnl, places="0.01"),
        pnl_currency,
        _format_decimal(basis_price, places="0.00000001"),
    )


def _position_snapshot_matches_order(
    position_snapshot: PositionSnapshotRecord,
    *,
    broker_order: BrokerOrderRecord,
) -> bool:
    if position_snapshot.broker_account_id != broker_order.broker_account_id:
        return False
    if position_snapshot.symbol != broker_order.symbol:
        return False
    if position_snapshot.currency != broker_order.currency:
        return False
    if position_snapshot.security_type != broker_order.security_type:
        return False
    if (
        broker_order.local_symbol not in (None, "")
        and position_snapshot.local_symbol not in (None, "")
        and position_snapshot.local_symbol != broker_order.local_symbol
    ):
        return False
    if (
        broker_order.primary_exchange not in (None, "")
        and position_snapshot.primary_exchange not in (None, "")
        and position_snapshot.primary_exchange != broker_order.primary_exchange
    ):
        return False
    return True


def _latest_matching_position_quantity(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
) -> Decimal | None:
    rows = session.execute(
        select(PositionSnapshotRecord)
        .where(
            PositionSnapshotRecord.broker_account_id == broker_order.broker_account_id,
            PositionSnapshotRecord.symbol == broker_order.symbol,
            PositionSnapshotRecord.currency == broker_order.currency,
            PositionSnapshotRecord.security_type == broker_order.security_type,
        )
        .order_by(
            PositionSnapshotRecord.snapshot_at.desc(),
            PositionSnapshotRecord.id.desc(),
        )
        .limit(8)
    ).scalars()

    for position_snapshot in rows:
        if not _position_snapshot_matches_order(
            position_snapshot,
            broker_order=broker_order,
        ):
            continue
        return _to_decimal(position_snapshot.quantity)
    return None


def _matching_execution_fill_quantity(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
) -> Decimal:
    lineage_clauses = []
    if broker_order.external_perm_id not in (None, ""):
        lineage_clauses.append(
            ExecutionFillRecord.external_perm_id == broker_order.external_perm_id
        )
    if broker_order.external_order_id not in (None, ""):
        lineage_clauses.append(
            ExecutionFillRecord.external_order_id == broker_order.external_order_id
        )
    if broker_order.order_ref not in (None, ""):
        lineage_clauses.append(ExecutionFillRecord.order_ref == broker_order.order_ref)
    if not lineage_clauses and broker_order.instruction_id is not None and (
        broker_order.order_role or ""
    ).strip().upper() == "EXIT":
        lineage_clauses.append(
            and_(
                ExecutionFillRecord.instruction_id == broker_order.instruction_id,
                ExecutionFillRecord.order_ref.is_not(None),
                ExecutionFillRecord.order_ref.like("%:exit:%"),
            )
        )

    if not lineage_clauses:
        return Decimal("0")

    rows = session.execute(
        select(ExecutionFillRecord.quantity)
        .where(
            ExecutionFillRecord.broker_account_id == broker_order.broker_account_id,
            or_(*lineage_clauses),
        )
    ).scalars()
    filled_quantity = Decimal("0")
    for quantity in rows:
        parsed_quantity = _meaningful_decimal(quantity)
        if parsed_quantity is None:
            continue
        filled_quantity += abs(parsed_quantity)
    return filled_quantity


def _is_fully_filled_order(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
) -> bool:
    total_quantity = _meaningful_decimal(broker_order.total_quantity)
    if total_quantity is None or total_quantity <= 0:
        return False
    filled_quantity = _matching_execution_fill_quantity(session, broker_order=broker_order)
    return filled_quantity >= abs(total_quantity)


def _is_effectively_closed_open_order(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
) -> bool:
    if _is_fully_filled_order(session, broker_order=broker_order):
        return True

    normalized_role = (broker_order.order_role or "").strip().upper()
    if normalized_role != "EXIT":
        return False

    latest_position_quantity = _latest_matching_position_quantity(
        session,
        broker_order=broker_order,
    )
    return latest_position_quantity == Decimal("0")


def _open_order_market_context(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
) -> tuple[str | None, datetime | None, str | None, str | None, str | None, str | None]:
    matching_snapshots = []
    rows = session.execute(
        select(PositionSnapshotRecord)
        .where(
            PositionSnapshotRecord.broker_account_id == broker_order.broker_account_id,
            PositionSnapshotRecord.symbol == broker_order.symbol,
            PositionSnapshotRecord.currency == broker_order.currency,
            PositionSnapshotRecord.security_type == broker_order.security_type,
        )
        .order_by(
            PositionSnapshotRecord.snapshot_at.desc(),
            PositionSnapshotRecord.id.desc(),
        )
    ).scalars()

    for row in rows:
        if not _position_snapshot_matches_order(row, broker_order=broker_order):
            continue
        if row.market_price in (None, ""):
            continue
        matching_snapshots.append(row)
        if len(matching_snapshots) >= 2:
            break

    if not matching_snapshots:
        return None, None, None, None, None, None

    latest_snapshot = matching_snapshots[0]
    previous_snapshot = matching_snapshots[1] if len(matching_snapshots) > 1 else None

    latest_market_price = _to_decimal(latest_snapshot.market_price)
    previous_market_price = _to_decimal(
        previous_snapshot.market_price if previous_snapshot is not None else None
    )

    if latest_market_price is None:
        return None, latest_snapshot.snapshot_at, None, None, None, None

    direction: str | None = None
    if previous_market_price is not None:
        if latest_market_price > previous_market_price:
            direction = "UP"
        elif latest_market_price < previous_market_price:
            direction = "DOWN"
        else:
            direction = "UNCHANGED"

    working_price, spread_reference = _resolve_working_price(broker_order)

    if working_price is None:
        return (
            latest_snapshot.market_price,
            latest_snapshot.snapshot_at,
            direction,
            None,
            None,
            None,
        )

    spread = working_price - latest_market_price
    spread_pct = None
    if latest_market_price != 0:
        spread_pct = (spread / latest_market_price) * Decimal("100")

    return (
        latest_snapshot.market_price,
        latest_snapshot.snapshot_at,
        direction,
        _format_signed_decimal(spread, places="0.01"),
        _format_signed_decimal(spread_pct, places="0.01") if spread_pct is not None else None,
        spread_reference,
    )



__all__ = [name for name in globals() if not name.startswith("__")]
