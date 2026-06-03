from __future__ import annotations

from ibkr_trader.virtual.execution_core import *
from ibkr_trader.virtual.execution_orders import *
from ibkr_trader.virtual.execution_quotes import *

def ensure_virtual_account_record(
    session_factory: sessionmaker[Session],
    *,
    account_key: str,
    base_currency: str = "SEK",
    account_label: str | None = None,
    cash_balance: Decimal | None = None,
    snapshot_at: datetime | None = None,
) -> dict[str, Any]:
    with session_scope(session_factory) as session:
        broker_account = ensure_virtual_account(
            session,
            account_key=account_key,
            base_currency=base_currency,
            account_label=account_label,
            cash_balance=cash_balance,
        )
        snapshot = _persist_virtual_account_snapshot(
            session,
            broker_account=broker_account,
            snapshot_at=snapshot_at or utc_now(),
        )
        session.flush()
        return {
            "account_key": broker_account.account_key,
            "broker_kind": broker_account.broker_kind,
            "account_label": broker_account.account_label,
            "base_currency": broker_account.base_currency,
            "is_virtual": broker_account.is_virtual,
            "cash_balance": broker_account.metadata_json.get(
                _VIRTUAL_CASH_BALANCE_METADATA_KEY
            ),
            "snapshot_id": snapshot.id,
        }


def _latest_position_quantity(
    session: Session,
    *,
    broker_account_id: int,
    symbol: str,
    currency: str,
    security_type: str,
) -> Decimal:
    row = session.execute(
        select(PositionSnapshotRecord)
        .where(
            PositionSnapshotRecord.broker_account_id == broker_account_id,
            PositionSnapshotRecord.symbol == symbol,
            PositionSnapshotRecord.currency == currency,
            PositionSnapshotRecord.security_type == security_type,
        )
        .order_by(
            PositionSnapshotRecord.snapshot_at.desc(),
            PositionSnapshotRecord.id.desc(),
        )
        .limit(1)
    ).scalar_one_or_none()
    if row is None or row.quantity in (None, ""):
        return Decimal("0")
    return _to_decimal(row.quantity) or Decimal("0")


def _latest_position_snapshot(
    session: Session,
    *,
    broker_account_id: int,
    symbol: str,
    currency: str,
    security_type: str,
    owner_instruction_id: str | None = None,
    owner_deployment_key: str | None = None,
) -> PositionSnapshotRecord | None:
    return session.execute(
        select(PositionSnapshotRecord)
        .where(
            PositionSnapshotRecord.broker_account_id == broker_account_id,
            PositionSnapshotRecord.symbol == symbol,
            PositionSnapshotRecord.currency == currency,
            PositionSnapshotRecord.security_type == security_type,
            PositionSnapshotRecord.owner_instruction_id.is_(None)
            if owner_instruction_id is None
            else PositionSnapshotRecord.owner_instruction_id == owner_instruction_id,
            PositionSnapshotRecord.owner_deployment_key.is_(None)
            if owner_deployment_key is None
            else PositionSnapshotRecord.owner_deployment_key == owner_deployment_key,
        )
        .order_by(
            PositionSnapshotRecord.snapshot_at.desc(),
            PositionSnapshotRecord.id.desc(),
        )
        .limit(1)
    ).scalar_one_or_none()


def _latest_virtual_position_snapshots_for_account(
    session: Session,
    *,
    broker_account_id: int,
) -> tuple[PositionSnapshotRecord, ...]:
    rows = session.execute(
        select(PositionSnapshotRecord)
        .where(
            PositionSnapshotRecord.broker_account_id == broker_account_id,
            PositionSnapshotRecord.is_virtual.is_(True),
        )
        .order_by(
            PositionSnapshotRecord.symbol.asc(),
            PositionSnapshotRecord.currency.asc(),
            PositionSnapshotRecord.security_type.asc(),
            PositionSnapshotRecord.snapshot_at.desc(),
            PositionSnapshotRecord.id.desc(),
        )
    ).scalars()

    latest_by_identity: dict[
        tuple[str, str, str, str | None, str | None, str | None],
        PositionSnapshotRecord,
    ] = {}
    for row in rows:
        identity = (
            row.symbol,
            row.currency,
            row.security_type,
            row.local_symbol,
            row.owner_instruction_id,
            row.owner_deployment_key,
        )
        latest_by_identity.setdefault(identity, row)
    return tuple(latest_by_identity.values())


def _position_unrealized_pnl(position: PositionSnapshotRecord) -> Decimal:
    quantity = _to_decimal(position.quantity) or Decimal("0")
    average_cost = _to_decimal(position.average_cost)
    market_price = _to_decimal(position.market_price)
    if quantity == 0 or average_cost is None or market_price is None:
        return Decimal("0")
    return quantity * (market_price - average_cost)


def _instruction_trace_metadata(record: InstructionRecord | None) -> dict[str, Any]:
    if record is None:
        return {}
    payload = record.payload if isinstance(record.payload, dict) else {}
    instruction = payload.get("instruction")
    if not isinstance(instruction, dict):
        return {}
    trace = instruction.get("trace")
    if not isinstance(trace, dict):
        return {}
    metadata = trace.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def _owner_instruction_id_from_order_ref(order_ref: str | None) -> str | None:
    normalized = str(order_ref or "").strip()
    if not normalized:
        return None
    if ":exit:" in normalized:
        return normalized.split(":exit:", 1)[0] or None
    return normalized


def _virtual_position_owner_from_order(
    broker_order: BrokerOrderRecord,
) -> dict[str, str | None]:
    instruction = broker_order.instruction
    metadata = _instruction_trace_metadata(instruction)
    return {
        "owner_instruction_id": (
            instruction.instruction_id
            if instruction is not None
            else _owner_instruction_id_from_order_ref(broker_order.order_ref)
        ),
        "owner_source_instruction_id": (
            str(metadata.get("rl_source_instruction_id"))
            if metadata.get("rl_source_instruction_id") not in (None, "")
            else None
        ),
        "owner_deployment_key": (
            str(metadata.get("rl_deployment_key"))
            if metadata.get("rl_deployment_key") not in (None, "")
            else None
        ),
        "owner_book_key": instruction.book_key if instruction is not None else None,
    }


def _position_owner_payload(position: PositionSnapshotRecord) -> dict[str, str | None]:
    return {
        "owner_instruction_id": position.owner_instruction_id,
        "owner_source_instruction_id": position.owner_source_instruction_id,
        "owner_deployment_key": position.owner_deployment_key,
        "owner_book_key": position.owner_book_key,
    }


def _quote_matches_position_owner(
    quote: VirtualMarketQuoteRecord,
    position: PositionSnapshotRecord,
) -> bool:
    quote_scope = _quote_owner_scope(quote)
    if quote_scope is None:
        return True
    return _owner_scope_matches_quote_scope(
        {
            "deployment_key": position.owner_deployment_key,
            "source_instruction_id": position.owner_source_instruction_id,
        },
        quote_scope,
    )


def _fill_signed_quantity(fill: ExecutionFillRecord) -> Decimal:
    quantity = _to_decimal(fill.quantity) or Decimal("0")
    side = str(fill.side or "").strip().upper()
    if side in {"BOT", "BUY"}:
        return quantity
    if side in {"SLD", "SELL"}:
        return -quantity
    return quantity


def _sum_virtual_realized_pnl(
    session: Session,
    *,
    broker_account_id: int,
    account_key: str,
) -> Decimal:
    fills = session.execute(
        select(ExecutionFillRecord)
        .where(
            ExecutionFillRecord.broker_account_id == broker_account_id,
            ExecutionFillRecord.account_key == account_key,
            ExecutionFillRecord.is_virtual.is_(True),
        )
        .order_by(
            ExecutionFillRecord.symbol.asc(),
            ExecutionFillRecord.currency.asc(),
            ExecutionFillRecord.security_type.asc(),
            ExecutionFillRecord.executed_at.asc(),
            ExecutionFillRecord.id.asc(),
        )
    ).scalars()

    positions: dict[tuple[str, str, str], tuple[Decimal, Decimal | None]] = {}
    realized = Decimal("0")
    for fill in fills:
        identity = (fill.symbol, fill.currency, fill.security_type)
        current_quantity, average_cost = positions.get(identity, (Decimal("0"), None))
        fill_quantity = _fill_signed_quantity(fill)
        fill_price = _to_decimal(fill.price)
        if fill_quantity == 0 or fill_price is None:
            continue

        current_sign = 1 if current_quantity > 0 else -1 if current_quantity < 0 else 0
        fill_sign = 1 if fill_quantity > 0 else -1
        if current_sign == 0 or current_sign == fill_sign:
            new_quantity = current_quantity + fill_quantity
            if new_quantity == 0:
                positions[identity] = (Decimal("0"), None)
                continue
            current_notional = abs(current_quantity) * (average_cost or fill_price)
            fill_notional = abs(fill_quantity) * fill_price
            positions[identity] = (
                new_quantity,
                (current_notional + fill_notional) / abs(new_quantity),
            )
            continue

        close_quantity = min(abs(current_quantity), abs(fill_quantity))
        if average_cost is not None:
            realized += (
                close_quantity * (fill_price - average_cost)
                if current_quantity > 0
                else close_quantity * (average_cost - fill_price)
            )

        new_quantity = current_quantity + fill_quantity
        if new_quantity == 0:
            positions[identity] = (Decimal("0"), None)
        elif (new_quantity > 0 and current_quantity > 0) or (
            new_quantity < 0 and current_quantity < 0
        ):
            positions[identity] = (new_quantity, average_cost)
        else:
            positions[identity] = (new_quantity, fill_price)
    return realized


def _sum_virtual_commissions(
    session: Session,
    *,
    broker_account_id: int,
    account_key: str,
) -> Decimal:
    fills = session.execute(
        select(ExecutionFillRecord).where(
            ExecutionFillRecord.broker_account_id == broker_account_id,
            ExecutionFillRecord.account_key == account_key,
            ExecutionFillRecord.is_virtual.is_(True),
        )
    ).scalars()
    total = Decimal("0")
    for fill in fills:
        commission = _to_decimal(fill.commission)
        if commission is not None:
            total += commission
    return total


def _virtual_cash_balance(broker_account: BrokerAccountRecord) -> Decimal:
    metadata = broker_account.metadata_json or {}
    raw_value = metadata.get(_VIRTUAL_CASH_BALANCE_METADATA_KEY)
    return _to_decimal(raw_value) or Decimal("0")


def _persist_virtual_account_snapshot(
    session: Session,
    *,
    broker_account: BrokerAccountRecord,
    snapshot_at: datetime,
) -> AccountSnapshotRecord:
    total_commissions = _sum_virtual_commissions(
        session,
        broker_account_id=broker_account.id,
        account_key=broker_account.account_key,
    )
    cash_balance = _virtual_cash_balance(broker_account)
    realized_pnl = _sum_virtual_realized_pnl(
        session,
        broker_account_id=broker_account.id,
        account_key=broker_account.account_key,
    )
    position_unrealized_pnl = Decimal("0")
    position_count = 0
    for position in _latest_virtual_position_snapshots_for_account(
        session,
        broker_account_id=broker_account.id,
    ):
        quantity = _to_decimal(position.quantity) or Decimal("0")
        if quantity == 0:
            continue
        position_count += 1
        position_unrealized_pnl += _position_unrealized_pnl(position)

    cash_value = cash_balance + realized_pnl - total_commissions
    net_liquidation = cash_value + position_unrealized_pnl
    snapshot = AccountSnapshotRecord(
        broker_account_id=broker_account.id,
        is_virtual=True,
        snapshot_at=snapshot_at,
        source="virtual_execution",
        net_liquidation=str(net_liquidation),
        total_cash_value=str(cash_value),
        buying_power=str(net_liquidation),
        available_funds=str(cash_value),
        excess_liquidity=str(net_liquidation),
        cushion="1" if net_liquidation >= 0 else "0",
        currency="SEK",
        raw_payload={
            "virtual_account": True,
            "cash_balance_sek": str(cash_balance),
            "realized_pnl_sek": str(realized_pnl),
            "unrealized_pnl_sek": str(position_unrealized_pnl),
            "total_commissions_sek": str(total_commissions),
            "open_position_count": position_count,
        },
    )
    session.add(snapshot)
    return snapshot


def _apply_virtual_fill_to_position(
    *,
    current_quantity: Decimal,
    current_average_cost: Decimal | None,
    current_realized_pnl: Decimal,
    fill_delta: Decimal,
    fill_price: Decimal,
) -> tuple[Decimal, Decimal | None, Decimal]:
    if fill_delta == 0:
        return current_quantity, current_average_cost, current_realized_pnl

    current_sign = 1 if current_quantity > 0 else -1 if current_quantity < 0 else 0
    fill_sign = 1 if fill_delta > 0 else -1
    if current_sign == 0 or current_sign == fill_sign:
        new_quantity = current_quantity + fill_delta
        if new_quantity == 0:
            return Decimal("0"), None, current_realized_pnl
        current_notional = abs(current_quantity) * (current_average_cost or fill_price)
        fill_notional = abs(fill_delta) * fill_price
        return (
            new_quantity,
            (current_notional + fill_notional) / abs(new_quantity),
            current_realized_pnl,
        )

    close_quantity = min(abs(current_quantity), abs(fill_delta))
    if current_average_cost is not None:
        current_realized_pnl += (
            close_quantity * (fill_price - current_average_cost)
            if current_quantity > 0
            else close_quantity * (current_average_cost - fill_price)
        )

    new_quantity = current_quantity + fill_delta
    if new_quantity == 0:
        return Decimal("0"), None, current_realized_pnl
    if (new_quantity > 0 and current_quantity > 0) or (
        new_quantity < 0 and current_quantity < 0
    ):
        return new_quantity, current_average_cost, current_realized_pnl
    return new_quantity, fill_price, current_realized_pnl


def _persist_virtual_position_snapshot(
    session: Session,
    *,
    broker_account: BrokerAccountRecord,
    broker_order: BrokerOrderRecord,
    fill_payload: dict[str, Any],
) -> None:
    fill_quantity = _to_decimal(fill_payload.get("quantity")) or Decimal("1")
    fill_price = _to_decimal(fill_payload.get("price")) or Decimal("0")
    owner = _virtual_position_owner_from_order(broker_order)
    if (
        broker_order.instruction is not None
        and broker_order.instruction.source_system == "rl-runner"
        and not owner["owner_deployment_key"]
    ):
        raise ValueError(
            "RL virtual position fills must carry owner_deployment_key before "
            "a position snapshot can be written."
        )
    latest_snapshot = _latest_position_snapshot(
        session,
        broker_account_id=broker_account.id,
        symbol=broker_order.symbol,
        currency=broker_order.currency,
        security_type=broker_order.security_type,
        owner_instruction_id=owner["owner_instruction_id"],
        owner_deployment_key=owner["owner_deployment_key"],
    )
    current_quantity = (
        _to_decimal(latest_snapshot.quantity)
        if latest_snapshot is not None
        else None
    ) or Decimal("0")
    current_average_cost = (
        _to_decimal(latest_snapshot.average_cost)
        if latest_snapshot is not None
        else None
    )
    current_realized_pnl = (
        _to_decimal(latest_snapshot.realized_pnl)
        if latest_snapshot is not None
        else None
    ) or Decimal("0")
    delta = fill_quantity if broker_order.side == "BUY" else -fill_quantity
    new_quantity, average_cost, realized_pnl = _apply_virtual_fill_to_position(
        current_quantity=current_quantity,
        current_average_cost=current_average_cost,
        current_realized_pnl=current_realized_pnl,
        fill_delta=delta,
        fill_price=fill_price,
    )
    market_value = new_quantity * fill_price
    unrealized_pnl = (
        new_quantity * (fill_price - average_cost)
        if new_quantity != 0 and average_cost is not None
        else Decimal("0")
    )
    snapshot = PositionSnapshotRecord(
        broker_account_id=broker_account.id,
        is_virtual=True,
        snapshot_at=_parse_datetime_value(fill_payload["executed_at"]),
        source="virtual_execution",
        symbol=broker_order.symbol,
        exchange=broker_order.exchange,
        currency=broker_order.currency,
        security_type=broker_order.security_type,
        primary_exchange=broker_order.primary_exchange,
        local_symbol=broker_order.local_symbol,
        quantity=str(new_quantity),
        average_cost=str(average_cost) if average_cost is not None else None,
        market_price=str(fill_price),
        market_value=str(market_value),
        unrealized_pnl=str(unrealized_pnl),
        realized_pnl=str(realized_pnl),
        owner_instruction_id=owner["owner_instruction_id"],
        owner_source_instruction_id=owner["owner_source_instruction_id"],
        owner_deployment_key=owner["owner_deployment_key"],
        owner_book_key=owner["owner_book_key"],
        raw_payload=_serialize_for_json({
            "virtual_execution": fill_payload,
            "owner": owner,
            "previous_quantity": str(current_quantity),
            "delta_quantity": str(delta),
        }),
    )
    session.add(snapshot)


def _quote_mark_price(quote: VirtualMarketQuoteRecord) -> Decimal | None:
    last = _to_decimal(quote.last_price)
    midpoint = _to_decimal(quote.midpoint_price)
    bid = _to_decimal(quote.bid_price)
    ask = _to_decimal(quote.ask_price)
    if midpoint is None and bid is not None and ask is not None:
        midpoint = (bid + ask) / Decimal("2")
    for candidate in (last, midpoint, bid, ask):
        if candidate is not None and candidate > 0:
            return candidate
    return None


def _latest_position_snapshots_for_quote(
    session: Session,
    *,
    broker_account_id: int,
    quote: VirtualMarketQuoteRecord,
) -> tuple[PositionSnapshotRecord, ...]:
    rows = session.execute(
        select(PositionSnapshotRecord)
        .where(
            PositionSnapshotRecord.broker_account_id == broker_account_id,
            PositionSnapshotRecord.symbol == quote.symbol,
            PositionSnapshotRecord.currency == quote.currency,
            PositionSnapshotRecord.security_type == quote.security_type,
        )
        .order_by(
            PositionSnapshotRecord.snapshot_at.desc(),
            PositionSnapshotRecord.id.desc(),
        )
    ).scalars()
    latest_by_owner: dict[
        tuple[str | None, str | None, str | None, str | None],
        PositionSnapshotRecord,
    ] = {}
    for row in rows:
        owner_key = (
            row.owner_instruction_id,
            row.owner_deployment_key,
            row.owner_source_instruction_id,
            row.owner_book_key,
        )
        latest_by_owner.setdefault(owner_key, row)
    return tuple(latest_by_owner.values())


def _persist_virtual_position_mark_snapshots(
    session: Session,
    *,
    broker_account: BrokerAccountRecord,
    quote: VirtualMarketQuoteRecord,
) -> tuple[PositionSnapshotRecord, ...]:
    marked_snapshots: list[PositionSnapshotRecord] = []
    for latest_snapshot in _latest_position_snapshots_for_quote(
        session,
        broker_account_id=broker_account.id,
        quote=quote,
    ):
        if not _quote_matches_position_owner(quote, latest_snapshot):
            continue
        quantity = _to_decimal(latest_snapshot.quantity) or Decimal("0")
        if quantity == 0:
            continue
        mark_price = _quote_mark_price(quote)
        if mark_price is None:
            continue
        average_cost = _to_decimal(latest_snapshot.average_cost)
        market_value = quantity * mark_price
        unrealized_pnl = (
            quantity * (mark_price - average_cost)
            if average_cost is not None
            else Decimal("0")
        )
        owner = _position_owner_payload(latest_snapshot)
        snapshot = PositionSnapshotRecord(
            broker_account_id=broker_account.id,
            is_virtual=True,
            snapshot_at=quote.observed_at,
            source="virtual_market_mark",
            symbol=latest_snapshot.symbol,
            exchange=latest_snapshot.exchange,
            currency=latest_snapshot.currency,
            security_type=latest_snapshot.security_type,
            primary_exchange=latest_snapshot.primary_exchange,
            local_symbol=latest_snapshot.local_symbol,
            quantity=str(quantity),
            average_cost=str(average_cost) if average_cost is not None else None,
            market_price=str(mark_price),
            market_value=str(market_value),
            unrealized_pnl=str(unrealized_pnl),
            realized_pnl=latest_snapshot.realized_pnl,
            owner_instruction_id=owner["owner_instruction_id"],
            owner_source_instruction_id=owner["owner_source_instruction_id"],
            owner_deployment_key=owner["owner_deployment_key"],
            owner_book_key=owner["owner_book_key"],
            raw_payload=_serialize_for_json(
                {
                    "virtual_market_mark": True,
                    "quote": serialize_virtual_quote(quote),
                    "previous_snapshot_id": latest_snapshot.id,
                    "owner": owner,
                }
            ),
        )
        session.add(snapshot)
        marked_snapshots.append(snapshot)
    return tuple(marked_snapshots)


def _cancel_open_virtual_exit_siblings_after_fill(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
    observed_at: datetime,
) -> None:
    if broker_order.order_role != "EXIT" or broker_order.instruction_id is None:
        return

    filled_oca_group = _normalize_text(
        (broker_order.metadata_json or {}).get("oca_group")
    )
    rows = session.execute(
        select(BrokerOrderRecord)
        .where(
            BrokerOrderRecord.broker_kind == BROKER_KIND_VIRTUAL,
            BrokerOrderRecord.is_virtual.is_(True),
            BrokerOrderRecord.account_key == broker_order.account_key,
            BrokerOrderRecord.instruction_id == broker_order.instruction_id,
            BrokerOrderRecord.order_role == "EXIT",
            BrokerOrderRecord.id != broker_order.id,
            _open_virtual_order_status_clause(),
        )
        .order_by(BrokerOrderRecord.id.asc())
    ).scalars()

    for sibling_order in rows:
        sibling_oca_group = _normalize_text(
            (sibling_order.metadata_json or {}).get("oca_group")
        )
        if filled_oca_group is not None and sibling_oca_group != filled_oca_group:
            continue
        status_before = sibling_order.status
        sibling_order.status = "Cancelled"
        sibling_order.last_status_at = observed_at
        metadata = dict(sibling_order.metadata_json or {})
        metadata["cancelled_by_virtual_exit_fill"] = {
            "filled_broker_order_id": broker_order.id,
            "filled_external_order_id": broker_order.external_order_id,
            "filled_order_ref": broker_order.order_ref,
            "oca_group": filled_oca_group,
        }
        sibling_order.metadata_json = _serialize_for_json(metadata)
        session.add(
            BrokerOrderEventRecord(
                broker_order_id=sibling_order.id,
                event_type="virtual_exit_sibling_cancelled",
                event_at=observed_at,
                status_before=status_before,
                status_after=sibling_order.status,
                payload=_serialize_for_json(
                    {
                        "filled_broker_order_id": broker_order.id,
                        "filled_external_order_id": broker_order.external_order_id,
                        "filled_order_ref": broker_order.order_ref,
                        "oca_group": filled_oca_group,
                    }
                ),
                note=(
                    "Virtual exit fill cancelled the sibling exit order so the "
                    "simulated OCA state matches broker behaviour."
                ),
            )
        )


def persist_virtual_execution_fill(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
    instruction_record: InstructionRecord | None,
    fill_payload: dict[str, Any],
    observed_at: datetime,
    event_type: str = "virtual_execution_fill",
    note: str = "Virtual order filled from virtual market-watch price.",
) -> ExecutionFillRecord | None:
    external_execution_id = str(fill_payload["external_execution_id"])
    executed_at = _parse_datetime_value(fill_payload["executed_at"])
    normalized_fill_payload = {**fill_payload, "executed_at": executed_at}
    existing = session.execute(
        select(ExecutionFillRecord).where(
            ExecutionFillRecord.broker_kind == BROKER_KIND_VIRTUAL,
            ExecutionFillRecord.account_key == broker_order.account_key,
            ExecutionFillRecord.external_execution_id == external_execution_id,
        )
    ).scalar_one_or_none()
    if existing is not None:
        return None

    broker_account = broker_order.broker_account
    broker_account.is_virtual = True
    broker_order.is_virtual = True
    previous_status = broker_order.status
    broker_order.status = "FILLED"
    broker_order.last_status_at = observed_at
    metadata = dict(broker_order.metadata_json or {})
    metadata["virtual_execution"] = fill_payload
    metadata["last_order_status_callback"] = {
        "orderId": broker_order.external_order_id,
        "status": "Filled",
        "filled": fill_payload.get("quantity"),
        "remaining": "0",
        "avgFillPrice": fill_payload.get("price"),
        "lastFillPrice": fill_payload.get("price"),
    }
    broker_order.metadata_json = _serialize_for_json(metadata)

    fill = ExecutionFillRecord(
        broker_order_id=broker_order.id,
        instruction_id=instruction_record.id if instruction_record is not None else None,
        broker_account_id=broker_account.id,
        broker_kind=BROKER_KIND_VIRTUAL,
        account_key=broker_order.account_key,
        is_virtual=True,
        external_execution_id=external_execution_id,
        external_order_id=broker_order.external_order_id,
        external_perm_id=broker_order.external_perm_id,
        order_ref=broker_order.order_ref,
        symbol=broker_order.symbol,
        exchange=broker_order.exchange,
        currency=broker_order.currency,
        security_type=broker_order.security_type,
        side=fill_payload.get("side"),
        quantity=str(fill_payload["quantity"]),
        price=str(fill_payload["price"]),
        commission=str(fill_payload["commission"]),
        commission_currency=str(fill_payload["commission_currency"]),
        executed_at=executed_at,
        raw_payload=_serialize_for_json(normalized_fill_payload),
    )
    session.add(fill)
    session.flush()

    session.add(
        BrokerOrderEventRecord(
            broker_order_id=broker_order.id,
            event_type=event_type,
            event_at=observed_at,
            status_before=previous_status,
            status_after="FILLED",
            payload=_serialize_for_json(normalized_fill_payload),
            note=note,
        )
    )
    _persist_virtual_position_snapshot(
        session,
        broker_account=broker_account,
        broker_order=broker_order,
        fill_payload=normalized_fill_payload,
    )
    _cancel_open_virtual_exit_siblings_after_fill(
        session,
        broker_order=broker_order,
        observed_at=observed_at,
    )
    _persist_virtual_account_snapshot(
        session,
        broker_account=broker_account,
        snapshot_at=executed_at or observed_at,
    )
    return fill


def persist_virtual_execution_from_submission(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
    instruction_record: InstructionRecord | None,
    broker_submission: dict[str, Any],
    observed_at: datetime,
) -> ExecutionFillRecord | None:
    virtual_execution = broker_submission.get("virtual_execution")
    if not isinstance(virtual_execution, dict):
        return None
    fill_payload = virtual_execution.get("fill")
    if not isinstance(fill_payload, dict):
        return None
    return persist_virtual_execution_fill(
        session,
        broker_order=broker_order,
        instruction_record=instruction_record,
        fill_payload=fill_payload,
        observed_at=observed_at,
    )



__all__ = [name for name in globals() if not name.startswith("__")]
