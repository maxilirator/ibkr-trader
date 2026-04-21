from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from decimal import Decimal
from decimal import InvalidOperation
from enum import Enum
import re
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import AccountSnapshotRecord
from ibkr_trader.db.models import BrokerAccountRecord
from ibkr_trader.db.models import BrokerOrderEventRecord
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import ExecutionFillRecord
from ibkr_trader.db.models import OperatorControlEventRecord
from ibkr_trader.db.models import OperatorControlRecord
from ibkr_trader.db.models import OperatorReviewActionRecord
from ibkr_trader.db.models import PositionSnapshotRecord
from ibkr_trader.db.models import ReconciliationIssueRecord
from ibkr_trader.db.models import ReconciliationRunRecord
from ibkr_trader.orchestration.operator_controls import KILL_SWITCH_CONTROL_KEY
from ibkr_trader.orchestration.operator_reviews import (
    BROKER_ATTENTION_TARGET_KIND,
    RECONCILIATION_ISSUE_TARGET_KIND,
    OperatorReviewStatus,
    build_operator_review_status,
    extract_broker_attention_message,
)

_CLOSED_ORDER_STATUSES = {
    "API_CANCELLED",
    "CANCELLED",
    "ERROR",
    "FILLED",
    "INACTIVE",
    "NOT_FOUND_AT_BROKER",
    "REJECTED",
}

_RECOVERED_RETRY_STATUSES = {
    "PENDINGSUBMIT",
    "PRESUBMITTED",
    "SUBMITTED",
    "PARTIALLYFILLED",
    "FILLED",
}


@dataclass(slots=True)
class OperatorAccountSnapshot:
    broker_kind: str
    account_key: str
    account_label: str | None
    base_currency: str | None
    snapshot_at: datetime
    source: str
    currency: str | None
    net_liquidation: str | None
    total_cash_value: str | None
    buying_power: str | None
    available_funds: str | None
    excess_liquidity: str | None
    cushion: str | None


@dataclass(slots=True)
class OperatorPositionSnapshot:
    broker_kind: str
    account_key: str
    account_label: str | None
    snapshot_at: datetime
    source: str
    symbol: str
    exchange: str
    currency: str
    security_type: str
    primary_exchange: str | None
    local_symbol: str | None
    quantity: str
    average_cost: str | None
    market_price: str | None
    market_value: str | None
    unrealized_pnl: str | None
    realized_pnl: str | None


@dataclass(slots=True)
class OperatorOpenOrder:
    broker_order_id: int
    instruction_record_id: int | None
    broker_kind: str
    account_key: str
    account_label: str | None
    order_role: str
    external_order_id: str | None
    external_perm_id: str | None
    external_client_id: str | None
    order_ref: str | None
    order_purpose: str | None
    symbol: str
    exchange: str
    currency: str
    security_type: str
    primary_exchange: str | None
    local_symbol: str | None
    side: str
    order_type: str
    time_in_force: str | None
    status: str
    total_quantity: str | None
    limit_price: str | None
    stop_price: str | None
    submitted_at: datetime | None
    last_status_at: datetime | None
    warning_text: str | None
    reject_reason: str | None
    working_price: str | None
    working_price_reference: str | None
    fill_basis_price: str | None
    fill_basis_at: datetime | None
    fill_price_spread: str | None
    fill_price_spread_pct: str | None
    reference_market_price: str | None
    reference_market_price_at: datetime | None
    last_market_price_direction: str | None
    price_spread: str | None
    price_spread_pct: str | None
    spread_reference: str | None


@dataclass(slots=True)
class OperatorExecutionFill:
    fill_id: int
    broker_order_id: int | None
    instruction_record_id: int | None
    broker_kind: str
    account_key: str
    account_label: str | None
    executed_at: datetime
    symbol: str
    exchange: str | None
    currency: str
    security_type: str
    side: str | None
    quantity: str
    price: str
    commission: str | None
    commission_currency: str | None
    order_ref: str | None
    external_execution_id: str
    external_order_id: str | None
    external_perm_id: str | None


@dataclass(slots=True)
class OperatorBrokerAttention:
    event_id: int
    broker_order_id: int
    account_key: str
    account_label: str | None
    symbol: str
    order_ref: str | None
    event_type: str
    status_after: str | None
    event_at: datetime
    message: str
    note: str | None
    operator_review: OperatorReviewStatus


@dataclass(slots=True)
class OperatorReconciliationIssue:
    issue_id: int
    instruction_id: str | None
    stage: str
    severity: str
    message: str
    observed_at: datetime
    payload: dict[str, Any]
    operator_review: OperatorReviewStatus


@dataclass(slots=True)
class OperatorReconciliationRun:
    run_id: int
    run_kind: str
    broker_kind: str
    account_key: str | None
    runtime_timezone: str | None
    started_at: datetime
    completed_at: datetime
    status: str
    issue_count: int
    action_count: int
    metadata_json: dict[str, Any]
    issues: tuple[OperatorReconciliationIssue, ...]


@dataclass(slots=True)
class OperatorKillSwitch:
    enabled: bool
    reason: str | None
    updated_by: str | None
    last_changed_at: datetime | None
    latest_event_at: datetime | None


@dataclass(slots=True)
class OperatorDashboardSnapshot:
    generated_at: datetime
    kill_switch: OperatorKillSwitch
    accounts: tuple[OperatorAccountSnapshot, ...]
    positions: tuple[OperatorPositionSnapshot, ...]
    open_orders: tuple[OperatorOpenOrder, ...]
    recent_fills: tuple[OperatorExecutionFill, ...]
    recent_broker_attention: tuple[OperatorBrokerAttention, ...]
    recent_reconciliation_runs: tuple[OperatorReconciliationRun, ...]


def _normalized_payload_error_message(payload: dict[str, Any]) -> str | None:
    raw_message = payload.get("errorString") or payload.get("errorMsg") or payload.get("message")
    if raw_message in (None, ""):
        return None
    normalized = str(raw_message)
    normalized = (
        normalized.replace("<br />", " ")
        .replace("<br/>", " ")
        .replace("<br>", " ")
    )
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized or None


def _is_insufficient_funds_order_error(payload: dict[str, Any]) -> bool:
    error_code = str(payload.get("errorCode") or "").strip()
    if error_code != "201":
        return False
    normalized_message = (_normalized_payload_error_message(payload) or "").lower()
    return "available funds" in normalized_message and "margin" in normalized_message


def _has_recovered_replacement_order(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
    event_at: datetime,
) -> bool:
    if broker_order.instruction_id is None:
        return False

    candidate_orders = session.execute(
        select(BrokerOrderRecord).where(
            BrokerOrderRecord.instruction_id == broker_order.instruction_id,
            BrokerOrderRecord.order_role == broker_order.order_role,
            BrokerOrderRecord.id != broker_order.id,
        )
    ).scalars()

    for candidate_order in candidate_orders:
        candidate_status = (candidate_order.status or "").upper()
        if candidate_status not in _RECOVERED_RETRY_STATUSES:
            continue
        if (
            candidate_order.last_status_at is not None
            and candidate_order.last_status_at < event_at
        ):
            continue
        return True
    return False


def _is_auto_recovered_entry_reject(
    session: Session,
    *,
    broker_order_event: BrokerOrderEventRecord,
    broker_order: BrokerOrderRecord,
) -> bool:
    if broker_order_event.event_type != "order_error_callback":
        return False
    if broker_order.order_role != "ENTRY":
        return False
    payload = broker_order_event.payload if isinstance(broker_order_event.payload, dict) else {}
    if not _is_insufficient_funds_order_error(payload):
        return False
    return _has_recovered_replacement_order(
        session,
        broker_order=broker_order,
        event_at=broker_order_event.event_at,
    )


def _serialize_for_json(payload: Any) -> Any:
    if isinstance(payload, Enum):
        return payload.value
    if isinstance(payload, Decimal):
        return str(payload)
    if isinstance(payload, datetime):
        return payload.isoformat()
    if isinstance(payload, date):
        return payload.isoformat()
    if isinstance(payload, list):
        return [_serialize_for_json(item) for item in payload]
    if isinstance(payload, tuple):
        return [_serialize_for_json(item) for item in payload]
    if isinstance(payload, dict):
        return {key: _serialize_for_json(value) for key, value in payload.items()}
    return payload


def serialize_operator_dashboard_snapshot(
    snapshot: OperatorDashboardSnapshot,
) -> dict[str, Any]:
    return _serialize_for_json(asdict(snapshot))


def _normalize_order_status(status: str | None) -> str | None:
    if status is None:
        return None
    normalized = status.strip()
    if not normalized:
        return None
    return normalized.upper()


def _is_non_zero_quantity(value: str | None) -> bool:
    if value in (None, ""):
        return False
    return Decimal(str(value)) != Decimal("0")


def _to_decimal(value: str | None) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _meaningful_decimal(value: str | None) -> Decimal | None:
    decimal_value = _to_decimal(value)
    if decimal_value is None or decimal_value == 0:
        return None
    return decimal_value


def _format_signed_decimal(value: Decimal | None, *, places: str) -> str | None:
    if value is None:
        return None
    quantized = value.quantize(Decimal(places))
    prefix = "+" if quantized > 0 else ""
    return f"{prefix}{quantized}"


def _format_decimal(value: Decimal | None, *, places: str) -> str | None:
    if value is None:
        return None
    quantized = value.quantize(Decimal(places))
    formatted = format(quantized, "f")
    if "." in formatted:
        formatted = formatted.rstrip("0").rstrip(".")
    if formatted in {"-0", ""}:
        return "0"
    return formatted


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


def _build_account_snapshots(
    session: Session,
) -> tuple[OperatorAccountSnapshot, ...]:
    rows = session.execute(
        select(AccountSnapshotRecord, BrokerAccountRecord)
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
    for account_snapshot, broker_account in rows:
        if broker_account.id in latest_by_account_id:
            continue
        latest_by_account_id[broker_account.id] = OperatorAccountSnapshot(
            broker_kind=broker_account.broker_kind,
            account_key=broker_account.account_key,
            account_label=broker_account.account_label,
            base_currency=broker_account.base_currency,
            snapshot_at=account_snapshot.snapshot_at,
            source=account_snapshot.source,
            currency=account_snapshot.currency,
            net_liquidation=account_snapshot.net_liquidation,
            total_cash_value=account_snapshot.total_cash_value,
            buying_power=account_snapshot.buying_power,
            available_funds=account_snapshot.available_funds,
            excess_liquidity=account_snapshot.excess_liquidity,
            cushion=account_snapshot.cushion,
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
    rows = session.execute(
        select(PositionSnapshotRecord, BrokerAccountRecord)
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
        .order_by(
            BrokerOrderRecord.last_status_at.desc(),
            BrokerOrderRecord.updated_at.desc(),
            BrokerOrderRecord.id.desc(),
        )
        .limit(max(limit * 4, limit))
    ).all()

    open_orders: list[OperatorOpenOrder] = []
    for broker_order, broker_account in rows:
        if _normalize_order_status(broker_order.status) in _CLOSED_ORDER_STATUSES:
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
        if len(open_orders) >= limit:
            break
    return tuple(open_orders)


def _build_recent_fills(
    session: Session,
    *,
    limit: int,
) -> tuple[OperatorExecutionFill, ...]:
    rows = session.execute(
        select(ExecutionFillRecord, BrokerAccountRecord)
        .join(
            BrokerAccountRecord,
            BrokerAccountRecord.id == ExecutionFillRecord.broker_account_id,
        )
        .order_by(
            ExecutionFillRecord.executed_at.desc(),
            ExecutionFillRecord.id.desc(),
        )
        .limit(limit)
    ).all()
    return tuple(
        OperatorExecutionFill(
            fill_id=fill.id,
            broker_order_id=fill.broker_order_id,
            instruction_record_id=fill.instruction_id,
            broker_kind=fill.broker_kind,
            account_key=fill.account_key,
            account_label=broker_account.account_label,
            executed_at=fill.executed_at,
            symbol=fill.symbol,
            exchange=fill.exchange,
            currency=fill.currency,
            security_type=fill.security_type,
            side=fill.side,
            quantity=fill.quantity,
            price=fill.price,
            commission=fill.commission,
            commission_currency=fill.commission_currency,
            order_ref=fill.order_ref,
            external_execution_id=fill.external_execution_id,
            external_order_id=fill.external_order_id,
            external_perm_id=fill.external_perm_id,
        )
        for fill, broker_account in rows
    )


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
        attention_rows.append(
            OperatorBrokerAttention(
                event_id=broker_order_event.id,
                broker_order_id=broker_order.id,
                account_key=broker_order.account_key,
                account_label=broker_account.account_label,
                symbol=broker_order.symbol,
                order_ref=broker_order.order_ref,
                event_type=broker_order_event.event_type,
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
        query = query.where(ReconciliationRunRecord.issue_count > 0)

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
                )
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
        for run in reconciliation_runs
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
