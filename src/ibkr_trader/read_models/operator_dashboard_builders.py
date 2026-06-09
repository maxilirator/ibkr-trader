from __future__ import annotations

from ibkr_trader.read_models.operator_dashboard_common import *
from ibkr_trader.read_models.operator_dashboard_orders import *

def _build_account_snapshots(
    session: Session,
) -> tuple[OperatorAccountSnapshot, ...]:
    latest_snapshot_ids = (
        select(func.max(AccountSnapshotRecord.id).label("snapshot_id"))
        .group_by(AccountSnapshotRecord.broker_account_id)
        .subquery()
    )
    rows = session.execute(
        select(AccountSnapshotRecord, BrokerAccountRecord)
        .join(
            latest_snapshot_ids,
            AccountSnapshotRecord.id == latest_snapshot_ids.c.snapshot_id,
        )
        .join(
            BrokerAccountRecord,
            BrokerAccountRecord.id == AccountSnapshotRecord.broker_account_id,
        )
        .order_by(
            AccountSnapshotRecord.snapshot_at.desc(),
            AccountSnapshotRecord.id.desc(),
        )
    ).all()

    latest_by_account_id: dict[int, OperatorAccountSnapshot] = {}
    day_performance_by_account_id = _build_account_day_performance_by_account_id(
        session,
        account_ids={broker_account.id for _, broker_account in rows},
        reference_at=utc_now(),
    )
    for account_snapshot, broker_account in rows:
        if broker_account.id in latest_by_account_id:
            continue
        latest_by_account_id[broker_account.id] = OperatorAccountSnapshot(
            broker_kind=broker_account.broker_kind,
            account_key=broker_account.account_key,
            account_label=broker_account.account_label,
            base_currency=broker_account.base_currency,
            is_virtual=account_snapshot.is_virtual or broker_account.is_virtual,
            snapshot_at=account_snapshot.snapshot_at,
            source=account_snapshot.source,
            currency=account_snapshot.currency,
            net_liquidation=account_snapshot.net_liquidation,
            total_cash_value=account_snapshot.total_cash_value,
            buying_power=account_snapshot.buying_power,
            available_funds=account_snapshot.available_funds,
            excess_liquidity=account_snapshot.excess_liquidity,
            cushion=account_snapshot.cushion,
            day_performance=day_performance_by_account_id.get(
                broker_account.id,
                _build_empty_account_day_performance(),
            ),
        )

    return tuple(
        sorted(
            latest_by_account_id.values(),
            key=lambda row: (row.account_key, row.snapshot_at),
            reverse=False,
        )
    )


def _build_kill_switch(session: Session) -> OperatorKillSwitch:
    record = session.execute(
        select(OperatorControlRecord).where(
            OperatorControlRecord.control_key == KILL_SWITCH_CONTROL_KEY
        )
    ).scalar_one_or_none()
    if record is None:
        return OperatorKillSwitch(
            enabled=False,
            reason=None,
            updated_by=None,
            last_changed_at=None,
            latest_event_at=None,
        )

    latest_event = session.execute(
        select(OperatorControlEventRecord)
        .where(OperatorControlEventRecord.operator_control_id == record.id)
        .order_by(OperatorControlEventRecord.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    return OperatorKillSwitch(
        enabled=record.enabled,
        reason=record.reason,
        updated_by=record.updated_by,
        last_changed_at=record.last_changed_at,
        latest_event_at=latest_event.event_at if latest_event is not None else None,
    )


def _build_position_snapshots(
    session: Session,
    *,
    include_flat_positions: bool,
) -> tuple[OperatorPositionSnapshot, ...]:
    latest_snapshot_ids = (
        select(func.max(PositionSnapshotRecord.id).label("snapshot_id"))
        .group_by(
            PositionSnapshotRecord.broker_account_id,
            PositionSnapshotRecord.symbol,
            PositionSnapshotRecord.exchange,
            PositionSnapshotRecord.currency,
            PositionSnapshotRecord.security_type,
            PositionSnapshotRecord.local_symbol,
        )
        .subquery()
    )
    rows = session.execute(
        select(PositionSnapshotRecord, BrokerAccountRecord)
        .join(
            latest_snapshot_ids,
            PositionSnapshotRecord.id == latest_snapshot_ids.c.snapshot_id,
        )
        .join(
            BrokerAccountRecord,
            BrokerAccountRecord.id == PositionSnapshotRecord.broker_account_id,
        )
        .order_by(
            PositionSnapshotRecord.snapshot_at.desc(),
            PositionSnapshotRecord.id.desc(),
        )
    ).all()

    latest_by_identity: dict[
        tuple[int, str, str, str, str, str | None],
        OperatorPositionSnapshot,
    ] = {}
    for position_snapshot, broker_account in rows:
        identity = (
            broker_account.id,
            position_snapshot.symbol,
            position_snapshot.exchange,
            position_snapshot.currency,
            position_snapshot.security_type,
            position_snapshot.local_symbol,
        )
        if identity in latest_by_identity:
            continue
        if not include_flat_positions and not _is_non_zero_quantity(position_snapshot.quantity):
            continue
        latest_by_identity[identity] = OperatorPositionSnapshot(
            broker_kind=broker_account.broker_kind,
            account_key=broker_account.account_key,
            account_label=broker_account.account_label,
            is_virtual=position_snapshot.is_virtual or broker_account.is_virtual,
            snapshot_at=position_snapshot.snapshot_at,
            source=position_snapshot.source,
            symbol=position_snapshot.symbol,
            exchange=position_snapshot.exchange,
            currency=position_snapshot.currency,
            security_type=position_snapshot.security_type,
            primary_exchange=position_snapshot.primary_exchange,
            local_symbol=position_snapshot.local_symbol,
            quantity=position_snapshot.quantity,
            average_cost=position_snapshot.average_cost,
            market_price=position_snapshot.market_price,
            market_value=position_snapshot.market_value,
            unrealized_pnl=position_snapshot.unrealized_pnl,
            realized_pnl=position_snapshot.realized_pnl,
        )

    return tuple(
        sorted(
            latest_by_identity.values(),
            key=lambda row: (
                row.account_key,
                row.symbol,
                row.exchange,
                row.snapshot_at,
            ),
            reverse=False,
        )
    )


def _build_open_orders(
    session: Session,
    *,
    limit: int,
) -> tuple[OperatorOpenOrder, ...]:
    rows = session.execute(
        select(BrokerOrderRecord, BrokerAccountRecord)
        .join(
            BrokerAccountRecord,
            BrokerAccountRecord.id == BrokerOrderRecord.broker_account_id,
        )
        .where(_open_order_status_clause())
        .order_by(
            BrokerOrderRecord.last_status_at.desc(),
            BrokerOrderRecord.updated_at.desc(),
            BrokerOrderRecord.id.desc(),
        )
        .limit(max(limit * 8, limit))
    ).all()

    open_orders: list[OperatorOpenOrder] = []
    seen_lineages: set[tuple[str, str, str]] = set()
    for broker_order, broker_account in rows:
        if _normalize_order_status(broker_order.status) in _CLOSED_ORDER_STATUSES:
            continue
        if _is_effectively_closed_open_order(session, broker_order=broker_order):
            continue
        lineage_key = (
            broker_order.account_key,
            str(broker_order.external_perm_id or "").strip(),
            str(broker_order.order_ref or broker_order.external_order_id or "").strip(),
        )
        if lineage_key in seen_lineages:
            continue
        metadata_json = broker_order.metadata_json or {}
        (
            reference_market_price,
            reference_market_price_at,
            last_market_price_direction,
            price_spread,
            price_spread_pct,
            spread_reference,
        ) = _open_order_market_context(session, broker_order=broker_order)
        (
            fill_basis_price,
            fill_basis_at,
            fill_price_spread,
            fill_price_spread_pct,
        ) = _exit_fill_basis(session, broker_order=broker_order)
        working_price, working_price_reference = _resolve_working_price(broker_order)
        open_orders.append(
            OperatorOpenOrder(
                broker_order_id=broker_order.id,
                instruction_record_id=broker_order.instruction_id,
                broker_kind=broker_order.broker_kind,
                account_key=broker_order.account_key,
                account_label=broker_account.account_label,
                is_virtual=broker_order.is_virtual or broker_account.is_virtual,
                order_role=broker_order.order_role,
                external_order_id=broker_order.external_order_id,
                external_perm_id=broker_order.external_perm_id,
                external_client_id=broker_order.external_client_id,
                order_ref=broker_order.order_ref,
                order_purpose=_derive_order_purpose(broker_order),
                symbol=broker_order.symbol,
                exchange=broker_order.exchange,
                currency=broker_order.currency,
                security_type=broker_order.security_type,
                primary_exchange=broker_order.primary_exchange,
                local_symbol=broker_order.local_symbol,
                side=broker_order.side,
                order_type=broker_order.order_type,
                time_in_force=broker_order.time_in_force,
                status=broker_order.status,
                total_quantity=broker_order.total_quantity,
                limit_price=broker_order.limit_price,
                stop_price=broker_order.stop_price,
                submitted_at=broker_order.submitted_at,
                last_status_at=broker_order.last_status_at,
                warning_text=(
                    str(metadata_json.get("warning_text"))
                    if metadata_json.get("warning_text") not in (None, "")
                    else None
                ),
                reject_reason=(
                    str(metadata_json.get("reject_reason"))
                    if metadata_json.get("reject_reason") not in (None, "")
                    else None
                ),
                working_price=_format_decimal(working_price, places="0.00000001"),
                working_price_reference=working_price_reference,
                fill_basis_price=fill_basis_price,
                fill_basis_at=fill_basis_at,
                fill_price_spread=fill_price_spread,
                fill_price_spread_pct=fill_price_spread_pct,
                reference_market_price=reference_market_price,
                reference_market_price_at=reference_market_price_at,
                last_market_price_direction=last_market_price_direction,
                price_spread=price_spread,
                price_spread_pct=price_spread_pct,
                spread_reference=spread_reference,
            )
        )
        seen_lineages.add(lineage_key)
        if len(open_orders) >= limit:
            break
    return tuple(open_orders)


def _build_recent_fills(
    session: Session,
    *,
    limit: int,
) -> tuple[OperatorExecutionFill, ...]:
    rows = session.execute(
        select(ExecutionFillRecord, BrokerAccountRecord, BrokerOrderRecord, InstructionRecord)
        .join(
            BrokerAccountRecord,
            BrokerAccountRecord.id == ExecutionFillRecord.broker_account_id,
        )
        .outerjoin(BrokerOrderRecord, BrokerOrderRecord.id == ExecutionFillRecord.broker_order_id)
        .outerjoin(
            InstructionRecord,
            InstructionRecord.id
            == func.coalesce(
                ExecutionFillRecord.instruction_id,
                BrokerOrderRecord.instruction_id,
            ),
        )
        .order_by(
            ExecutionFillRecord.executed_at.desc(),
            ExecutionFillRecord.id.desc(),
        )
        .limit(limit)
    ).all()
    recent_fills: list[OperatorExecutionFill] = []
    for fill, broker_account, broker_order, instruction in rows:
        order_role = _fill_order_role(fill, broker_order)
        realized_pnl, realized_pnl_gross, realized_pnl_currency, realized_pnl_basis_price = (
            _fill_realized_pnl(
                session,
                fill=fill,
                broker_order=broker_order,
                instruction=instruction,
            )
        )
        recent_fills.append(
            OperatorExecutionFill(
                fill_id=fill.id,
                broker_order_id=fill.broker_order_id,
                instruction_record_id=fill.instruction_id,
                order_role=order_role,
                broker_kind=fill.broker_kind,
                account_key=fill.account_key,
                account_label=broker_account.account_label,
                is_virtual=fill.is_virtual or broker_account.is_virtual,
                executed_at=fill.executed_at,
                symbol=fill.symbol,
                exchange=fill.exchange,
                currency=fill.currency,
                security_type=fill.security_type,
                side=fill.side,
                position_side=_fill_position_side(fill, broker_order, instruction),
                quantity=fill.quantity,
                price=fill.price,
                commission=fill.commission,
                commission_currency=fill.commission_currency,
                realized_pnl=realized_pnl,
                realized_pnl_gross=realized_pnl_gross,
                realized_pnl_currency=realized_pnl_currency,
                realized_pnl_basis_price=realized_pnl_basis_price,
                order_ref=fill.order_ref,
                external_execution_id=fill.external_execution_id,
                external_order_id=fill.external_order_id,
                external_perm_id=fill.external_perm_id,
            )
        )
    return tuple(recent_fills)


def _build_review_status_map(
    session: Session,
    *,
    target_kind: str,
    target_ids: list[int],
) -> dict[int, OperatorReviewStatus]:
    if not target_ids:
        return {}

    rows = session.execute(
        select(OperatorReviewActionRecord)
        .where(
            OperatorReviewActionRecord.target_kind == target_kind,
            OperatorReviewActionRecord.target_id.in_(target_ids),
        )
        .order_by(
            OperatorReviewActionRecord.target_id.asc(),
            OperatorReviewActionRecord.event_at.desc(),
            OperatorReviewActionRecord.id.desc(),
        )
    ).scalars()

    review_status_by_target_id: dict[int, OperatorReviewStatus] = {}
    for row in rows:
        if row.target_id in review_status_by_target_id:
            continue
        review_status_by_target_id[row.target_id] = build_operator_review_status(row)

    return review_status_by_target_id


def _build_recent_broker_attention(
    session: Session,
    *,
    limit: int,
) -> tuple[OperatorBrokerAttention, ...]:
    rows = session.execute(
        select(BrokerOrderEventRecord, BrokerOrderRecord, BrokerAccountRecord)
        .join(
            BrokerOrderRecord,
            BrokerOrderRecord.id == BrokerOrderEventRecord.broker_order_id,
        )
        .join(
            BrokerAccountRecord,
            BrokerAccountRecord.id == BrokerOrderRecord.broker_account_id,
        )
        .where(BrokerOrderEventRecord.archived_at.is_(None))
        .order_by(
            BrokerOrderEventRecord.event_at.desc(),
            BrokerOrderEventRecord.id.desc(),
        )
        .limit(max(limit * 6, limit))
    ).all()

    attention_rows: list[OperatorBrokerAttention] = []
    for broker_order_event, broker_order, broker_account in rows:
        message = extract_broker_attention_message(broker_order_event, broker_order)
        if message is None:
            continue
        if _is_auto_recovered_entry_reject(
            session,
            broker_order_event=broker_order_event,
            broker_order=broker_order,
        ):
            continue
        if _is_expected_oca_sibling_cancel(
            session,
            broker_order_event=broker_order_event,
            broker_order=broker_order,
        ):
            continue
        if _is_expected_forced_exit_cleanup_cancel(
            session,
            broker_order_event=broker_order_event,
            broker_order=broker_order,
        ):
            continue
        if _is_expected_unfilled_entry_expiry_cancel(
            session,
            broker_order_event=broker_order_event,
            broker_order=broker_order,
        ):
            continue
        attention_rows.append(
            OperatorBrokerAttention(
                event_id=broker_order_event.id,
                broker_order_id=broker_order.id,
                account_key=broker_order.account_key,
                account_label=broker_account.account_label,
                symbol=broker_order.symbol,
                order_ref=broker_order.order_ref,
                event_type=(
                    "broker_warning"
                    if _is_price_collar_warning_callback(broker_order_event)
                    else broker_order_event.event_type
                ),
                status_after=broker_order_event.status_after,
                event_at=broker_order_event.event_at,
                message=message,
                note=broker_order_event.note,
                operator_review=build_operator_review_status(None),
            )
        )
        if len(attention_rows) >= limit:
            break

    review_status_by_target_id = _build_review_status_map(
        session,
        target_kind=BROKER_ATTENTION_TARGET_KIND,
        target_ids=[row.event_id for row in attention_rows],
    )
    return tuple(
        OperatorBrokerAttention(
            event_id=row.event_id,
            broker_order_id=row.broker_order_id,
            account_key=row.account_key,
            account_label=row.account_label,
            symbol=row.symbol,
            order_ref=row.order_ref,
            event_type=row.event_type,
            status_after=row.status_after,
            event_at=row.event_at,
            message=row.message,
            note=row.note,
            operator_review=review_status_by_target_id.get(
                row.event_id,
                build_operator_review_status(None),
            ),
        )
        for row in attention_rows
    )


def _build_recent_reconciliation_runs(
    session: Session,
    *,
    limit: int,
    include_clean_runs: bool,
) -> tuple[OperatorReconciliationRun, ...]:
    query = select(ReconciliationRunRecord)
    if not include_clean_runs:
        query = query.where(
            ReconciliationRunRecord.issue_count > 0,
            ReconciliationRunRecord.issues.any(
                ReconciliationIssueRecord.archived_at.is_(None)
            ),
        )

    reconciliation_runs = list(
        session.execute(
            query.order_by(
                ReconciliationRunRecord.started_at.desc(),
                ReconciliationRunRecord.id.desc(),
            ).limit(limit)
        ).scalars()
    )
    if not reconciliation_runs:
        return ()

    issues = list(
        session.execute(
            select(ReconciliationIssueRecord)
            .where(
                ReconciliationIssueRecord.reconciliation_run_id.in_(
                    [run.id for run in reconciliation_runs]
                ),
                ReconciliationIssueRecord.archived_at.is_(None),
            )
            .order_by(
                ReconciliationIssueRecord.observed_at.desc(),
                ReconciliationIssueRecord.id.desc(),
            )
        ).scalars()
    )
    issues_by_run_id: dict[int, list[OperatorReconciliationIssue]] = {}
    review_status_by_target_id = _build_review_status_map(
        session,
        target_kind=RECONCILIATION_ISSUE_TARGET_KIND,
        target_ids=[issue.id for issue in issues],
    )
    for issue in issues:
        issues_by_run_id.setdefault(issue.reconciliation_run_id, []).append(
            OperatorReconciliationIssue(
                issue_id=issue.id,
                instruction_id=issue.instruction_id,
                stage=issue.stage,
                severity=issue.severity,
                message=issue.message,
                observed_at=issue.observed_at,
                payload=issue.payload,
                operator_review=review_status_by_target_id.get(
                    issue.id,
                    build_operator_review_status(None),
                ),
            )
        )

    visible_runs = (
        reconciliation_runs
        if include_clean_runs
        else [run for run in reconciliation_runs if issues_by_run_id.get(run.id)]
    )

    return tuple(
        OperatorReconciliationRun(
            run_id=run.id,
            run_kind=run.run_kind,
            broker_kind=run.broker_kind,
            account_key=run.account_key,
            runtime_timezone=run.runtime_timezone,
            started_at=run.started_at,
            completed_at=run.completed_at,
            status=run.status,
            issue_count=run.issue_count,
            action_count=run.action_count,
            metadata_json=run.metadata_json,
            issues=tuple(issues_by_run_id.get(run.id, ())),
        )
        for run in visible_runs
    )


def build_operator_dashboard_snapshot(
    session_factory: sessionmaker[Session],
    *,
    include_flat_positions: bool = False,
    order_limit: int = 50,
    fill_limit: int = 50,
    attention_limit: int = 25,
    reconciliation_run_limit: int = 20,
    include_clean_reconciliation_runs: bool = False,
) -> OperatorDashboardSnapshot:
    """Return a durable operator-facing snapshot built only from persisted ledger rows."""

    with session_scope(session_factory) as session:
        return OperatorDashboardSnapshot(
            generated_at=utc_now(),
            kill_switch=_build_kill_switch(session),
            accounts=_build_account_snapshots(session),
            positions=_build_position_snapshots(
                session,
                include_flat_positions=include_flat_positions,
            ),
            open_orders=_build_open_orders(session, limit=order_limit),
            recent_fills=_build_recent_fills(session, limit=fill_limit),
            recent_broker_attention=_build_recent_broker_attention(
                session,
                limit=attention_limit,
            ),
            recent_reconciliation_runs=_build_recent_reconciliation_runs(
                session,
                limit=reconciliation_run_limit,
                include_clean_runs=include_clean_reconciliation_runs,
            ),
        )

__all__ = [name for name in globals() if not name.startswith("__")]
