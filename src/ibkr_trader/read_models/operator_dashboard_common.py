from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import time
from datetime import timezone
from decimal import Decimal
from decimal import InvalidOperation
from enum import Enum
import re
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import and_
from sqlalchemy import func
from sqlalchemy import or_
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
from ibkr_trader.db.models import InstructionRecord
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

_STOCKHOLM_TZ = ZoneInfo("Europe/Stockholm")
_ACCOUNT_PERFORMANCE_SESSION_OPEN = time(9, 0)
_ACCOUNT_PERFORMANCE_SESSION_CLOSE = time(17, 30)
_MAX_ACCOUNT_DAY_PERFORMANCE_POINTS = 96


@dataclass(slots=True)
class OperatorAccountPerformancePoint:
    snapshot_at: datetime
    net_liquidation: str
    return_pct: str


@dataclass(slots=True)
class OperatorAccountDayPerformance:
    started_at: datetime | None
    latest_at: datetime | None
    currency: str | None
    start_net_liquidation: str | None
    latest_net_liquidation: str | None
    latest_return_pct: str | None
    points: tuple[OperatorAccountPerformancePoint, ...]


@dataclass(slots=True)
class OperatorAccountSnapshot:
    broker_kind: str
    account_key: str
    account_label: str | None
    base_currency: str | None
    is_virtual: bool
    snapshot_at: datetime
    source: str
    currency: str | None
    net_liquidation: str | None
    total_cash_value: str | None
    buying_power: str | None
    available_funds: str | None
    excess_liquidity: str | None
    cushion: str | None
    day_performance: OperatorAccountDayPerformance


@dataclass(slots=True)
class OperatorPositionSnapshot:
    broker_kind: str
    account_key: str
    account_label: str | None
    is_virtual: bool
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
    is_virtual: bool
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
    order_role: str | None
    broker_kind: str
    account_key: str
    account_label: str | None
    is_virtual: bool
    executed_at: datetime
    symbol: str
    exchange: str | None
    currency: str
    security_type: str
    side: str | None
    position_side: str | None
    quantity: str
    price: str
    commission: str | None
    commission_currency: str | None
    realized_pnl: str | None
    realized_pnl_gross: str | None
    realized_pnl_currency: str | None
    realized_pnl_basis_price: str | None
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


def _broker_order_oca_group(broker_order: BrokerOrderRecord) -> str | None:
    for payload in (broker_order.metadata_json, broker_order.raw_payload):
        if not isinstance(payload, dict):
            continue
        raw_value = payload.get("oca_group") or payload.get("ocaGroup")
        if raw_value in (None, ""):
            continue
        normalized = str(raw_value).strip()
        if normalized:
            return normalized
    return None


def _is_order_cancelled_error(payload: dict[str, Any]) -> bool:
    if str(payload.get("errorCode") or "").strip() != "202":
        return False
    message = str(
        payload.get("errorString") or payload.get("errorMsg") or payload.get("message") or ""
    ).lower()
    return "order canceled" in message or "order cancelled" in message


def _is_expected_oca_sibling_cancel(
    session: Session,
    *,
    broker_order_event: BrokerOrderEventRecord,
    broker_order: BrokerOrderRecord,
) -> bool:
    if broker_order_event.event_type != "order_error_callback":
        return False
    if broker_order.order_role != "EXIT":
        return False
    if broker_order.instruction_id is None:
        return False
    payload = broker_order_event.payload if isinstance(broker_order_event.payload, dict) else {}
    if not _is_order_cancelled_error(payload):
        return False
    if _normalize_order_status(broker_order_event.status_after) != "CANCELLED":
        return False

    oca_group = _broker_order_oca_group(broker_order)
    if oca_group is None:
        return False

    sibling_orders = session.execute(
        select(BrokerOrderRecord).where(
            BrokerOrderRecord.instruction_id == broker_order.instruction_id,
            BrokerOrderRecord.order_role == "EXIT",
            BrokerOrderRecord.id != broker_order.id,
        )
    ).scalars()

    for sibling_order in sibling_orders:
        if _broker_order_oca_group(sibling_order) != oca_group:
            continue
        if _normalize_order_status(sibling_order.status) == "FILLED":
            return True
    return False


def _has_entry_fill(
    session: Session,
    *,
    broker_order: BrokerOrderRecord,
    instruction: InstructionRecord,
) -> bool:
    if _is_non_zero_quantity(instruction.entry_filled_quantity):
        return True
    return (
        session.execute(
            select(ExecutionFillRecord.id)
            .where(
                or_(
                    ExecutionFillRecord.broker_order_id == broker_order.id,
                    ExecutionFillRecord.instruction_id == instruction.id,
                )
            )
            .limit(1)
        ).first()
        is not None
    )


def _is_expected_unfilled_entry_expiry_cancel(
    session: Session,
    *,
    broker_order_event: BrokerOrderEventRecord,
    broker_order: BrokerOrderRecord,
) -> bool:
    if broker_order_event.event_type != "order_error_callback":
        return False
    if broker_order.order_role != "ENTRY":
        return False
    if broker_order.instruction_id is None:
        return False
    if (broker_order.order_type or "").strip().upper() != "LMT":
        return False
    if (broker_order.time_in_force or "").strip().upper() != "DAY":
        return False

    payload = broker_order_event.payload if isinstance(broker_order_event.payload, dict) else {}
    if not _is_order_cancelled_error(payload):
        return False
    if _normalize_order_status(broker_order_event.status_after) != "CANCELLED":
        return False

    instruction = session.get(InstructionRecord, broker_order.instruction_id)
    if instruction is None:
        return False
    if _has_entry_fill(session, broker_order=broker_order, instruction=instruction):
        return False

    return _aware_utc(broker_order_event.event_at) >= _aware_utc(instruction.expire_at)


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


def _open_order_status_clause():
    return or_(
        BrokerOrderRecord.status.is_(None),
        func.upper(BrokerOrderRecord.status).not_in(_CLOSED_ORDER_STATUSES),
    )


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


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _account_day_window(reference_at: datetime) -> tuple[datetime, datetime]:
    local_reference = _aware_utc(reference_at).astimezone(_STOCKHOLM_TZ)
    session_date = local_reference.date()
    if local_reference.time() < _ACCOUNT_PERFORMANCE_SESSION_OPEN:
        session_date -= timedelta(days=1)
    local_start = datetime.combine(
        session_date,
        _ACCOUNT_PERFORMANCE_SESSION_OPEN,
        tzinfo=_STOCKHOLM_TZ,
    )
    local_close = datetime.combine(
        session_date,
        _ACCOUNT_PERFORMANCE_SESSION_CLOSE,
        tzinfo=_STOCKHOLM_TZ,
    )
    return local_start.astimezone(timezone.utc), local_close.astimezone(timezone.utc)


def _downsample_account_performance_points(
    points: tuple[OperatorAccountPerformancePoint, ...],
    *,
    limit: int = _MAX_ACCOUNT_DAY_PERFORMANCE_POINTS,
) -> tuple[OperatorAccountPerformancePoint, ...]:
    if len(points) <= limit:
        return points
    if limit <= 2:
        return (points[0], points[-1])

    step = (len(points) - 1) / (limit - 1)
    selected_indexes = sorted(
        {round(index * step) for index in range(limit)}
    )
    return tuple(points[index] for index in selected_indexes)


def _build_empty_account_day_performance() -> OperatorAccountDayPerformance:
    return OperatorAccountDayPerformance(
        started_at=None,
        latest_at=None,
        currency=None,
        start_net_liquidation=None,
        latest_net_liquidation=None,
        latest_return_pct=None,
        points=(),
    )


def _build_account_day_performance_by_account_id(
    session: Session,
    *,
    account_ids: set[int],
    reference_at: datetime,
) -> dict[int, OperatorAccountDayPerformance]:
    if not account_ids:
        return {}

    day_start_at, day_end_at = _account_day_window(reference_at)
    rows = session.execute(
        select(AccountSnapshotRecord)
        .where(
            AccountSnapshotRecord.broker_account_id.in_(account_ids),
            AccountSnapshotRecord.snapshot_at >= day_start_at,
            AccountSnapshotRecord.snapshot_at <= day_end_at,
        )
        .order_by(
            AccountSnapshotRecord.broker_account_id.asc(),
            AccountSnapshotRecord.snapshot_at.asc(),
            AccountSnapshotRecord.id.asc(),
        )
    ).scalars()

    snapshots_by_account_id: dict[int, list[AccountSnapshotRecord]] = {
        account_id: [] for account_id in account_ids
    }
    for row in rows:
        snapshots_by_account_id.setdefault(row.broker_account_id, []).append(row)

    performance_by_account_id: dict[int, OperatorAccountDayPerformance] = {}
    for account_id, snapshots in snapshots_by_account_id.items():
        start_value: Decimal | None = None
        start_at: datetime | None = None
        latest_value: Decimal | None = None
        latest_at: datetime | None = None
        latest_currency: str | None = None
        points: list[OperatorAccountPerformancePoint] = []

        for snapshot in snapshots:
            net_liquidation = _meaningful_decimal(snapshot.net_liquidation)
            if net_liquidation is None:
                continue
            if start_value is None:
                start_value = net_liquidation
                start_at = snapshot.snapshot_at
            if start_value is None or start_value == 0:
                continue

            return_pct = ((net_liquidation - start_value) / start_value) * Decimal("100")
            latest_value = net_liquidation
            latest_at = snapshot.snapshot_at
            latest_currency = snapshot.currency or latest_currency
            points.append(
                OperatorAccountPerformancePoint(
                    snapshot_at=snapshot.snapshot_at,
                    net_liquidation=_format_decimal(net_liquidation, places="0.01")
                    or str(net_liquidation),
                    return_pct=_format_signed_decimal(return_pct, places="0.01")
                    or "0.00",
                )
            )

        if not points or start_value is None or latest_value is None:
            performance_by_account_id[account_id] = _build_empty_account_day_performance()
            continue

        latest_return_pct = (
            ((latest_value - start_value) / start_value) * Decimal("100")
            if start_value != 0
            else None
        )
        performance_by_account_id[account_id] = OperatorAccountDayPerformance(
            started_at=start_at,
            latest_at=latest_at,
            currency=latest_currency,
            start_net_liquidation=_format_decimal(start_value, places="0.01"),
            latest_net_liquidation=_format_decimal(latest_value, places="0.01"),
            latest_return_pct=_format_signed_decimal(latest_return_pct, places="0.01"),
            points=_downsample_account_performance_points(tuple(points)),
        )

    return performance_by_account_id



__all__ = [name for name in globals() if not name.startswith("__")]
