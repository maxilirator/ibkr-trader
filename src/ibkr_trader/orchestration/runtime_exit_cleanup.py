from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, sessionmaker

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import session_scope, utc_now
from ibkr_trader.db.models import BrokerOrderRecord, ExecutionFillRecord, InstructionEventRecord, InstructionRecord
from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.ibkr.runtime_snapshot import BrokerOpenOrder, BrokerRuntimeSnapshot
from ibkr_trader.ledger.persistence import BROKER_KIND_IBKR, persist_broker_order_cancellation_result
from ibkr_trader.orchestration.runtime_broker_errors import run_with_broker_retries as _run_with_broker_retries
from ibkr_trader.orchestration.runtime_broker_matching import _broker_account_candidates
from ibkr_trader.orchestration.runtime_broker_matching import _exit_side_for_instruction
from ibkr_trader.orchestration.runtime_broker_matching import _is_market_broker_order
from ibkr_trader.orchestration.runtime_broker_matching import _is_open_broker_order_status
from ibkr_trader.orchestration.runtime_broker_matching import _matches_exit_cleanup_instrument
from ibkr_trader.orchestration.runtime_broker_matching import _matches_optional_identity
from ibkr_trader.orchestration.runtime_broker_matching import _normalize_broker_identity
from ibkr_trader.orchestration.runtime_broker_matching import _normalize_broker_order_status
from ibkr_trader.orchestration.runtime_types import ExecutionAggregate
from ibkr_trader.orchestration.runtime_types import parse_decimal as _parse_decimal
from ibkr_trader.orchestration.runtime_types import serialize_for_json as _serialize_for_json


FORCED_EXIT_TERMINAL_RETRY_SUPPRESSION = timedelta(hours=12)
FORCED_EXIT_POSITION_BLOCK_LOG_COOLDOWN = timedelta(minutes=5)
_FORCED_EXIT_TERMINAL_STATUSES = {
    "ERROR",
    "INACTIVE",
    "REJECTED",
}
_CLOSED_BROKER_ORDER_STATUSES = {
    "API_CANCELLED",
    "CANCELLED",
    "ERROR",
    "FILLED",
    "INACTIVE",
    "NOT_FOUND_AT_BROKER",
    "REJECTED",
}


def _open_order_ids_with_ref_prefix(
    snapshot: BrokerRuntimeSnapshot,
    *,
    order_ref_prefix: str,
) -> tuple[int, ...]:
    return tuple(
        order_id
        for order_id, open_order in snapshot.open_orders.items()
        if open_order.order_ref is not None
        and open_order.order_ref.startswith(order_ref_prefix)
    )


def _open_order_refs_with_ref_prefix(
    snapshot: BrokerRuntimeSnapshot,
    *,
    order_ref_prefix: str,
) -> set[str]:
    return {
        open_order.order_ref
        for open_order in snapshot.open_orders.values()
        if open_order.order_ref is not None
        and open_order.order_ref.startswith(order_ref_prefix)
    }


def _has_live_open_forced_exit_order(
    snapshot: BrokerRuntimeSnapshot,
    *,
    instruction_id: str,
) -> bool:
    expected_order_ref = f"{instruction_id}:exit:forced"
    return any(
        open_order.order_ref == expected_order_ref
        and _is_open_broker_order_status(open_order.status)
        for open_order in snapshot.open_orders.values()
    )


def _has_live_matching_exit_order(
    snapshot: BrokerRuntimeSnapshot,
    broker_config: IbkrConnectionConfig,
    *,
    record: InstructionRecord,
    instruction: ExecutionInstruction,
    remaining_quantity: Decimal,
) -> bool:
    account_candidates = _broker_account_candidates(
        broker_config,
        record=record,
        instruction=instruction,
    )
    expected_exit_side = _exit_side_for_instruction(instruction)
    required_quantity = remaining_quantity.copy_abs()

    for open_order in snapshot.open_orders.values():
        if not _is_open_broker_order_status(open_order.status):
            continue
        if not _is_market_broker_order(open_order.order_type):
            continue
        if _normalize_broker_identity(open_order.action) != expected_exit_side:
            continue
        if not _matches_optional_identity(
            open_order.account,
            account_candidates,
            default_when_missing=True,
        ):
            continue
        if not _matches_exit_cleanup_instrument(
            symbol=open_order.symbol,
            local_symbol=open_order.local_symbol,
            currency=open_order.currency,
            security_type=open_order.security_type,
            record=record,
            instruction=instruction,
        ):
            continue
        if open_order.total_quantity is not None:
            try:
                open_quantity = _parse_decimal(str(open_order.total_quantity))
            except ValueError:
                open_quantity = Decimal("0")
            if open_quantity < required_quantity:
                continue
        return True
    return False



def _record_forced_exit_position_blocked(
    session_factory: sessionmaker[Session],
    *,
    instruction_id: str,
    position_block: dict[str, Any],
    cycle_at: datetime,
) -> None:
    cutoff = cycle_at.astimezone(timezone.utc) - FORCED_EXIT_POSITION_BLOCK_LOG_COOLDOWN
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == instruction_id)
            .with_for_update()
        ).scalar_one_or_none()
        if record is None:
            return
        recent_event = session.execute(
            select(InstructionEventRecord.id).where(
                InstructionEventRecord.instruction_id == record.id,
                InstructionEventRecord.event_type
                == "forced_exit_blocked_broker_position_mismatch",
                InstructionEventRecord.event_at >= cutoff,
            )
        ).first()
        if recent_event is not None:
            return
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type="forced_exit_blocked_broker_position_mismatch",
                source="runtime_cycle",
                event_at=cycle_at,
                state_before=record.state,
                state_after=record.state,
                payload=position_block,
                note=(
                    "Runtime skipped a forced market exit because the live broker "
                    "position snapshot did not match the instruction quantity."
                ),
            )
        )


def _merge_forced_exit_conflict_detail(
    details: dict[int, dict[str, Any]],
    *,
    order_id: int,
    source: str,
    order_ref: str | None,
    status: str | None,
    symbol: str | None,
    local_symbol: str | None,
    action: str | None,
) -> None:
    detail = details.setdefault(
        order_id,
        {
            "broker_order_id": order_id,
            "sources": [],
        },
    )
    sources = detail.setdefault("sources", [])
    if source not in sources:
        sources.append(source)
    for key, value in {
        "order_ref": order_ref,
        "status": status,
        "symbol": symbol,
        "local_symbol": local_symbol,
        "action": action,
    }.items():
        if value not in (None, "") and key not in detail:
            detail[key] = value


def _open_order_is_forced_exit_for_instruction(
    open_order: BrokerOpenOrder,
    *,
    instruction_id: str,
) -> bool:
    return open_order.order_ref == f"{instruction_id}:exit:forced"


def _live_conflicting_exit_order_details_for_forced_exit(
    snapshot: BrokerRuntimeSnapshot,
    broker_config: IbkrConnectionConfig,
    *,
    record: InstructionRecord,
    instruction: ExecutionInstruction,
) -> dict[int, dict[str, Any]]:
    account_candidates = _broker_account_candidates(
        broker_config,
        record=record,
        instruction=instruction,
    )
    expected_exit_side = _exit_side_for_instruction(instruction)
    details: dict[int, dict[str, Any]] = {}

    for order_id, open_order in snapshot.open_orders.items():
        if not _is_open_broker_order_status(open_order.status):
            continue
        if _open_order_is_forced_exit_for_instruction(
            open_order,
            instruction_id=record.instruction_id,
        ):
            continue
        if _normalize_broker_identity(open_order.action) != expected_exit_side:
            continue
        if not _matches_optional_identity(
            open_order.account,
            account_candidates,
            default_when_missing=True,
        ):
            continue
        if not _matches_exit_cleanup_instrument(
            symbol=open_order.symbol,
            local_symbol=open_order.local_symbol,
            currency=open_order.currency,
            security_type=open_order.security_type,
            record=record,
            instruction=instruction,
        ):
            continue
        if open_order.order_ref in (None, "") or ":exit:" not in str(open_order.order_ref):
            continue
        _merge_forced_exit_conflict_detail(
            details,
            order_id=order_id,
            source="broker_snapshot",
            order_ref=open_order.order_ref,
            status=open_order.status,
            symbol=open_order.symbol,
            local_symbol=open_order.local_symbol,
            action=open_order.action,
        )

    return details


def _persisted_conflicting_exit_order_details_for_forced_exit(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    *,
    record: InstructionRecord,
    instruction: ExecutionInstruction,
) -> dict[int, dict[str, Any]]:
    account_candidates = _broker_account_candidates(
        broker_config,
        record=record,
        instruction=instruction,
    )
    expected_exit_side = _exit_side_for_instruction(instruction)
    details: dict[int, dict[str, Any]] = {}

    with session_scope(session_factory) as session:
        broker_orders = session.execute(
            select(BrokerOrderRecord).where(
                BrokerOrderRecord.order_role == "EXIT",
                or_(
                    BrokerOrderRecord.status.is_(None),
                    func.upper(BrokerOrderRecord.status).not_in(
                        _CLOSED_BROKER_ORDER_STATUSES
                    ),
                ),
                BrokerOrderRecord.external_order_id.is_not(None),
                BrokerOrderRecord.external_order_id != "",
            )
        ).scalars().all()

    for broker_order in broker_orders:
        if broker_order.order_ref == f"{record.instruction_id}:exit:forced":
            continue
        if _normalize_broker_identity(broker_order.side) != expected_exit_side:
            continue
        if not _matches_optional_identity(
            broker_order.account_key,
            account_candidates,
            default_when_missing=False,
        ):
            continue
        if not _matches_exit_cleanup_instrument(
            symbol=broker_order.symbol,
            local_symbol=broker_order.local_symbol,
            currency=broker_order.currency,
            security_type=broker_order.security_type,
            record=record,
            instruction=instruction,
        ):
            continue
        if _broker_order_has_matching_execution_fill(
            session_factory,
            broker_order=broker_order,
        ):
            continue
        try:
            order_id = int(str(broker_order.external_order_id))
        except ValueError:
            continue
        _merge_forced_exit_conflict_detail(
            details,
            order_id=order_id,
            source="persisted_open_exit",
            order_ref=broker_order.order_ref,
            status=broker_order.status,
            symbol=broker_order.symbol,
            local_symbol=broker_order.local_symbol,
            action=broker_order.side,
        )

    return details


def _conflicting_exit_order_details_for_forced_exit(
    session_factory: sessionmaker[Session],
    snapshot: BrokerRuntimeSnapshot,
    broker_config: IbkrConnectionConfig,
    *,
    record: InstructionRecord,
    instruction: ExecutionInstruction,
) -> dict[int, dict[str, Any]]:
    details = _live_conflicting_exit_order_details_for_forced_exit(
        snapshot,
        broker_config,
        record=record,
        instruction=instruction,
    )
    persisted_details = _persisted_conflicting_exit_order_details_for_forced_exit(
        session_factory,
        broker_config,
        record=record,
        instruction=instruction,
    )
    for order_id, persisted_detail in persisted_details.items():
        for source in persisted_detail.get("sources", []):
            _merge_forced_exit_conflict_detail(
                details,
                order_id=order_id,
                source=str(source),
                order_ref=persisted_detail.get("order_ref"),
                status=persisted_detail.get("status"),
                symbol=persisted_detail.get("symbol"),
                local_symbol=persisted_detail.get("local_symbol"),
                action=persisted_detail.get("action"),
            )
    return details


def _live_obsolete_exit_order_details_for_current_intent(
    snapshot: BrokerRuntimeSnapshot,
    *,
    instruction_id: str,
    desired_order_refs: set[str],
) -> dict[int, dict[str, Any]]:
    details: dict[int, dict[str, Any]] = {}
    order_ref_prefix = f"{instruction_id}:exit:"
    for order_id, open_order in snapshot.open_orders.items():
        if not _is_open_broker_order_status(open_order.status):
            continue
        order_ref = str(open_order.order_ref or "").strip()
        if not order_ref.startswith(order_ref_prefix):
            continue
        if order_ref in desired_order_refs:
            continue
        _merge_forced_exit_conflict_detail(
            details,
            order_id=order_id,
            source="broker_snapshot",
            order_ref=open_order.order_ref,
            status=open_order.status,
            symbol=open_order.symbol,
            local_symbol=open_order.local_symbol,
            action=open_order.action,
        )
    return details


def _persisted_obsolete_exit_order_details_for_current_intent(
    session_factory: sessionmaker[Session],
    *,
    record: InstructionRecord,
    desired_order_refs: set[str],
) -> dict[int, dict[str, Any]]:
    details: dict[int, dict[str, Any]] = {}
    with session_scope(session_factory) as session:
        broker_orders = session.execute(
            select(BrokerOrderRecord).where(
                BrokerOrderRecord.instruction_id == record.id,
                BrokerOrderRecord.order_role == "EXIT",
                or_(
                    BrokerOrderRecord.status.is_(None),
                    func.upper(BrokerOrderRecord.status).not_in(
                        _CLOSED_BROKER_ORDER_STATUSES
                    ),
                ),
                BrokerOrderRecord.external_order_id.is_not(None),
                BrokerOrderRecord.external_order_id != "",
            )
        ).scalars().all()

    for broker_order in broker_orders:
        order_ref = str(broker_order.order_ref or "").strip()
        if order_ref in desired_order_refs:
            continue
        if _broker_order_has_matching_execution_fill(
            session_factory,
            broker_order=broker_order,
        ):
            continue
        try:
            order_id = int(str(broker_order.external_order_id))
        except ValueError:
            continue
        _merge_forced_exit_conflict_detail(
            details,
            order_id=order_id,
            source="persisted_open_exit",
            order_ref=broker_order.order_ref,
            status=broker_order.status,
            symbol=broker_order.symbol,
            local_symbol=broker_order.local_symbol,
            action=broker_order.side,
        )
    return details


def _obsolete_exit_order_details_for_current_intent(
    session_factory: sessionmaker[Session],
    snapshot: BrokerRuntimeSnapshot,
    *,
    record: InstructionRecord,
    desired_order_refs: set[str],
) -> dict[int, dict[str, Any]]:
    details = _live_obsolete_exit_order_details_for_current_intent(
        snapshot,
        instruction_id=record.instruction_id,
        desired_order_refs=desired_order_refs,
    )
    persisted_details = _persisted_obsolete_exit_order_details_for_current_intent(
        session_factory,
        record=record,
        desired_order_refs=desired_order_refs,
    )
    for order_id, persisted_detail in persisted_details.items():
        for source in persisted_detail.get("sources", []):
            _merge_forced_exit_conflict_detail(
                details,
                order_id=order_id,
                source=str(source),
                order_ref=persisted_detail.get("order_ref"),
                status=persisted_detail.get("status"),
                symbol=persisted_detail.get("symbol"),
                local_symbol=persisted_detail.get("local_symbol"),
                action=persisted_detail.get("action"),
            )
    return details


def _record_exit_intent_cleanup_started(
    session_factory: sessionmaker[Session],
    *,
    instruction_id: str,
    desired_order_refs: set[str],
    obsolete_details: dict[int, dict[str, Any]],
    cycle_at: datetime,
) -> None:
    if not obsolete_details:
        return
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == instruction_id)
            .with_for_update()
        ).scalar_one_or_none()
        if record is None:
            return
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type="exit_intent_obsolete_orders_cleanup_started",
                source="runtime_cycle",
                event_at=cycle_at,
                state_before=record.state,
                state_after=record.state,
                payload={
                    "desired_order_refs": sorted(desired_order_refs),
                    "obsolete_orders": [
                        obsolete_details[order_id]
                        for order_id in sorted(obsolete_details)
                    ],
                },
                note=(
                    "Runtime found open exit orders that do not match the current "
                    "exit intent and will cancel them before submitting replacement "
                    "orders."
                ),
            )
        )


def _record_forced_exit_conflict_cleanup_started(
    session_factory: sessionmaker[Session],
    *,
    instruction_id: str,
    conflict_details: dict[int, dict[str, Any]],
    cycle_at: datetime,
) -> None:
    if not conflict_details:
        return
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == instruction_id)
            .with_for_update()
        ).scalar_one_or_none()
        if record is None:
            return
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type="forced_exit_conflicting_orders_cleanup_started",
                source="runtime_cycle",
                event_at=cycle_at,
                state_before=record.state,
                state_after=record.state,
                payload={
                    "conflicting_orders": [
                        conflict_details[order_id]
                        for order_id in sorted(conflict_details)
                    ],
                },
                note=(
                    "Runtime found open close-side exit orders for this account and "
                    "symbol before submitting the forced next-session exit."
                ),
            )
        )


def _persisted_open_order_ids_by_instruction(
    session_factory: sessionmaker[Session],
    *,
    records: list[InstructionRecord],
    order_role: str,
) -> dict[str, tuple[int, ...]]:
    if not records:
        return {}

    instruction_ids_by_record_id = {
        record.id: record.instruction_id
        for record in records
    }

    persisted_order_ids: dict[str, list[int]] = {
        record.instruction_id: []
        for record in records
    }

    with session_scope(session_factory) as session:
        rows = session.execute(
            select(BrokerOrderRecord).where(
                BrokerOrderRecord.instruction_id.in_(tuple(instruction_ids_by_record_id.keys())),
                BrokerOrderRecord.order_role == order_role,
            ).order_by(
                BrokerOrderRecord.last_status_at.desc(),
                BrokerOrderRecord.id.desc(),
            )
        ).scalars().all()

    seen_lineages: dict[str, set[tuple[str, str]]] = {
        record.instruction_id: set()
        for record in records
    }

    for broker_order in rows:
        public_instruction_id = instruction_ids_by_record_id.get(broker_order.instruction_id)
        if public_instruction_id is None:
            continue
        if _normalize_broker_order_status(broker_order.status) in _CLOSED_BROKER_ORDER_STATUSES:
            continue
        if broker_order.external_order_id in (None, ""):
            continue
        if _broker_order_has_matching_execution_fill(
            session_factory,
            broker_order=broker_order,
        ):
            continue
        lineage_key = (
            (
                str(broker_order.external_perm_id).strip()
                if broker_order.external_perm_id not in (None, "")
                else ""
            ),
            (
                str(broker_order.order_ref).strip()
                if broker_order.order_ref not in (None, "")
                else str(broker_order.external_order_id)
            ),
        )
        if lineage_key in seen_lineages[public_instruction_id]:
            continue
        try:
            persisted_order_ids[public_instruction_id].append(
                int(str(broker_order.external_order_id))
            )
        except ValueError:
            continue
        seen_lineages[public_instruction_id].add(lineage_key)

    return {
        instruction_id: tuple(sorted(set(order_ids)))
        for instruction_id, order_ids in persisted_order_ids.items()
    }


def _has_persisted_open_forced_exit_order(
    session_factory: sessionmaker[Session],
    *,
    record: InstructionRecord,
) -> bool:
    return _has_persisted_open_exit_order_ref(
        session_factory,
        record=record,
        order_ref=f"{record.instruction_id}:exit:forced",
    )


def _has_persisted_open_exit_order_ref(
    session_factory: sessionmaker[Session],
    *,
    record: InstructionRecord,
    order_ref: str,
) -> bool:
    with session_scope(session_factory) as session:
        exit_order = session.execute(
            select(BrokerOrderRecord.id).where(
                BrokerOrderRecord.instruction_id == record.id,
                BrokerOrderRecord.order_role == "EXIT",
                BrokerOrderRecord.order_ref == order_ref,
                or_(
                    BrokerOrderRecord.status.is_(None),
                    func.upper(BrokerOrderRecord.status).not_in(
                        _CLOSED_BROKER_ORDER_STATUSES
                    ),
                ),
            )
        ).first()
    return exit_order is not None


def _recent_terminal_forced_exit_failure(
    session_factory: sessionmaker[Session],
    *,
    record: InstructionRecord,
    cycle_at: datetime,
) -> dict[str, Any] | None:
    cutoff = cycle_at.astimezone(timezone.utc) - FORCED_EXIT_TERMINAL_RETRY_SUPPRESSION
    with session_scope(session_factory) as session:
        broker_order = session.execute(
            select(BrokerOrderRecord).where(
                BrokerOrderRecord.instruction_id == record.id,
                BrokerOrderRecord.order_role == "EXIT",
                BrokerOrderRecord.order_ref == f"{record.instruction_id}:exit:forced",
                func.upper(BrokerOrderRecord.status).in_(
                    _FORCED_EXIT_TERMINAL_STATUSES
                ),
                BrokerOrderRecord.last_status_at.is_not(None),
                BrokerOrderRecord.last_status_at >= cutoff,
            ).order_by(
                BrokerOrderRecord.last_status_at.desc(),
                BrokerOrderRecord.id.desc(),
            )
        ).scalars().first()

    if broker_order is None:
        return None
    return {
        "broker_order_id": broker_order.external_order_id,
        "broker_order_status": broker_order.status,
        "last_status_at": (
            broker_order.last_status_at.isoformat()
            if broker_order.last_status_at is not None
            else None
        ),
    }


def _record_forced_exit_retry_blocked(
    session_factory: sessionmaker[Session],
    *,
    instruction_id: str,
    failure: dict[str, Any],
    cycle_at: datetime,
) -> None:
    cutoff = cycle_at.astimezone(timezone.utc) - FORCED_EXIT_TERMINAL_RETRY_SUPPRESSION
    with session_scope(session_factory) as session:
        record = session.execute(
            select(InstructionRecord)
            .where(InstructionRecord.instruction_id == instruction_id)
            .with_for_update()
        ).scalar_one_or_none()
        if record is None:
            return
        recent_event = session.execute(
            select(InstructionEventRecord.id).where(
                InstructionEventRecord.instruction_id == record.id,
                InstructionEventRecord.event_type
                == "forced_exit_retry_blocked_terminal_failure",
                InstructionEventRecord.event_at >= cutoff,
            )
        ).first()
        if recent_event is not None:
            return
        session.add(
            InstructionEventRecord(
                instruction_id=record.id,
                event_type="forced_exit_retry_blocked_terminal_failure",
                source="runtime_cycle",
                event_at=cycle_at,
                state_before=record.state,
                state_after=record.state,
                payload={
                    "terminal_failure": failure,
                    "suppression_seconds": int(
                        FORCED_EXIT_TERMINAL_RETRY_SUPPRESSION.total_seconds()
                    ),
                },
                note=(
                    "Runtime skipped a forced exit retry because a recent forced "
                    "market exit reached a terminal broker status."
                ),
            )
        )


def _broker_order_has_matching_execution_fill(
    session_factory: sessionmaker[Session],
    *,
    broker_order: BrokerOrderRecord,
) -> bool:
    lineage_predicates = []
    if broker_order.external_perm_id not in (None, ""):
        lineage_predicates.append(
            ExecutionFillRecord.external_perm_id == broker_order.external_perm_id
        )
    if broker_order.external_order_id not in (None, ""):
        lineage_predicates.append(
            ExecutionFillRecord.external_order_id == broker_order.external_order_id
        )
    if broker_order.order_ref not in (None, ""):
        lineage_predicates.append(ExecutionFillRecord.order_ref == broker_order.order_ref)

    if not lineage_predicates:
        return False

    with session_scope(session_factory) as session:
        row = session.execute(
            select(ExecutionFillRecord.id)
            .where(
                ExecutionFillRecord.broker_account_id == broker_order.broker_account_id,
                or_(*lineage_predicates),
            )
            .limit(1)
        ).first()
    return row is not None


def _is_virtual_broker_order_id(
    session_factory: sessionmaker[Session],
    *,
    order_id: int,
) -> bool:
    with session_scope(session_factory) as session:
        row = session.execute(
            select(BrokerOrderRecord.is_virtual).where(
                BrokerOrderRecord.external_order_id == str(order_id)
            )
        ).scalar_one_or_none()
    return bool(row)


def _remaining_position_quantity(
    record: InstructionRecord,
    exit_fill: ExecutionAggregate,
) -> Decimal:
    entry_filled = _parse_decimal(record.entry_filled_quantity)
    if entry_filled <= 0:
        return Decimal("0")
    remaining = entry_filled - exit_fill.quantity
    return remaining if remaining > 0 else Decimal("0")


def _cancel_broker_order_and_persist(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    *,
    order_id: int,
    timeout: int,
    canceler: Callable[..., dict[str, Any]],
    event_type: str,
    note: str,
) -> dict[str, Any]:
    broker_cancellation = canceler(
        broker_config,
        order_id,
        timeout=timeout,
    )
    broker_kind = str(broker_cancellation.get("broker_kind") or BROKER_KIND_IBKR)
    fallback_account_key = (
        str(broker_cancellation["account"])
        if broker_cancellation.get("account") not in (None, "")
        else broker_config.account_id
    )
    persist_broker_order_cancellation_result(
        session_factory,
        broker_kind=broker_kind,
        broker_cancellation=broker_cancellation,
        observed_at=utc_now(),
        fallback_account_key=fallback_account_key,
        event_type=event_type,
        note=note,
    )
    return broker_cancellation


def _require_confirmed_forced_exit_cleanup_cancel(
    broker_cancellation: dict[str, Any],
    *,
    order_id: int,
) -> None:
    broker_status = broker_cancellation.get("broker_order_status")
    if not isinstance(broker_status, dict):
        raise RuntimeError(
            "Forced exit cleanup could not confirm cancellation of broker "
            f"order {order_id}: missing broker order status."
        )
    status = _normalize_broker_order_status(
        str(broker_status.get("status"))
        if broker_status.get("status") not in (None, "")
        else None
    )
    if status not in _CLOSED_BROKER_ORDER_STATUSES:
        raise RuntimeError(
            "Forced exit cleanup could not confirm cancellation of broker "
            f"order {order_id}; broker status was {status or 'UNKNOWN'}."
        )


def _require_confirmed_exit_intent_cleanup_cancel(
    broker_cancellation: dict[str, Any],
    *,
    order_id: int,
) -> None:
    broker_status = broker_cancellation.get("broker_order_status")
    if not isinstance(broker_status, dict):
        raise RuntimeError(
            "Exit intent cleanup could not confirm cancellation of broker "
            f"order {order_id}: missing broker order status."
        )
    status = _normalize_broker_order_status(
        str(broker_status.get("status"))
        if broker_status.get("status") not in (None, "")
        else None
    )
    if status not in _CLOSED_BROKER_ORDER_STATUSES:
        raise RuntimeError(
            "Exit intent cleanup could not confirm cancellation of broker "
            f"order {order_id}; broker status was {status or 'UNKNOWN'}."
        )


def _cancel_obsolete_exit_orders_for_current_intent(
    session_factory: sessionmaker[Session],
    snapshot: BrokerRuntimeSnapshot,
    broker_config: IbkrConnectionConfig,
    *,
    record: InstructionRecord,
    desired_order_refs: set[str],
    cycle_at: datetime,
    timeout: int,
    canceler: Callable[..., dict[str, Any]],
    broker_retry_delays: tuple[float, ...],
    sleep_fn: Callable[[float], None],
) -> tuple[int, ...]:
    obsolete_details = _obsolete_exit_order_details_for_current_intent(
        session_factory,
        snapshot,
        record=record,
        desired_order_refs=desired_order_refs,
    )
    if not obsolete_details:
        return ()

    _record_exit_intent_cleanup_started(
        session_factory,
        instruction_id=record.instruction_id,
        desired_order_refs=desired_order_refs,
        obsolete_details=obsolete_details,
        cycle_at=cycle_at,
    )
    cancelled_order_ids: list[int] = []
    for obsolete_order_id in sorted(obsolete_details):
        broker_cancellation = _run_with_broker_retries(
            lambda order_id=obsolete_order_id: _cancel_broker_order_and_persist(
                session_factory,
                broker_config,
                order_id=order_id,
                timeout=timeout,
                canceler=canceler,
                event_type="exit_order_cancelled_for_current_intent",
                note=(
                    "Persisted broker cancellation for an obsolete exit order "
                    "before submitting the current exit intent."
                ),
            ),
            retry_delays=broker_retry_delays,
            sleep_fn=sleep_fn,
        )
        _require_confirmed_exit_intent_cleanup_cancel(
            broker_cancellation,
            order_id=obsolete_order_id,
        )
        cancelled_order_ids.append(obsolete_order_id)

    return tuple(cancelled_order_ids)
