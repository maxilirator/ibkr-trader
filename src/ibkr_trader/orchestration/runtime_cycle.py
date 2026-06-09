from __future__ import annotations
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker
from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.ibkr.order_execution import cancel_broker_order
from ibkr_trader.ibkr.runtime_snapshot import fetch_broker_runtime_snapshot
from ibkr_trader.ibkr.runtime_snapshot import BrokerRuntimeSnapshot
from ibkr_trader.ledger.persistence import BROKER_KIND_IBKR, persist_broker_runtime_snapshot
from ibkr_trader.orchestration.entry_submission import cancel_persisted_instruction_entry
from ibkr_trader.orchestration.entry_submission import submit_persisted_instruction_entry
from ibkr_trader.orchestration.operator_controls import read_kill_switch_state
from ibkr_trader.orchestration.runtime_audit import broker_snapshot_unavailable_message as _broker_snapshot_unavailable_message
from ibkr_trader.orchestration.runtime_audit import finalize_runtime_cycle_result as _finalize_runtime_cycle_result
from ibkr_trader.orchestration.runtime_broker_errors import broker_exception_payload as _broker_exception_payload
from ibkr_trader.orchestration.runtime_broker_errors import is_retryable_broker_error as _is_retryable_broker_error
from ibkr_trader.orchestration.runtime_broker_errors import run_with_broker_retries as _run_with_broker_retries
from ibkr_trader.orchestration.runtime_broker_matching import _forced_exit_broker_position_block
from ibkr_trader.orchestration.runtime_cycle_support import _append_issue
from ibkr_trader.orchestration.runtime_cycle_support import _instruction_payload
from ibkr_trader.orchestration.runtime_cycle_support import _persist_drained_broker_callbacks
from ibkr_trader.orchestration.runtime_cycle_support import _record_runtime_note
from ibkr_trader.orchestration.runtime_entries import _mark_pending_entry_failed, _submit_due_pending_entries
from ibkr_trader.orchestration.market_data_readiness import MarketDataReadinessChecker
from ibkr_trader.orchestration.runtime_exit_cleanup import _cancel_broker_order_and_persist
from ibkr_trader.orchestration.runtime_exit_cleanup import _cancel_obsolete_exit_orders_for_current_intent
from ibkr_trader.orchestration.runtime_exit_cleanup import _conflicting_exit_order_details_for_forced_exit
from ibkr_trader.orchestration.runtime_exit_cleanup import _has_live_matching_exit_order
from ibkr_trader.orchestration.runtime_exit_cleanup import _has_live_open_forced_exit_order
from ibkr_trader.orchestration.runtime_exit_cleanup import _has_persisted_open_exit_order_ref
from ibkr_trader.orchestration.runtime_exit_cleanup import _has_persisted_open_forced_exit_order
from ibkr_trader.orchestration.runtime_exit_cleanup import _is_virtual_broker_order_id
from ibkr_trader.orchestration.runtime_exit_cleanup import _open_order_ids_with_ref_prefix
from ibkr_trader.orchestration.runtime_exit_cleanup import _open_order_refs_with_ref_prefix
from ibkr_trader.orchestration.runtime_exit_cleanup import _persisted_open_order_ids_by_instruction
from ibkr_trader.orchestration.runtime_exit_cleanup import _recent_terminal_forced_exit_failure
from ibkr_trader.orchestration.runtime_exit_cleanup import _record_forced_exit_conflict_cleanup_started
from ibkr_trader.orchestration.runtime_exit_cleanup import _record_forced_exit_position_blocked
from ibkr_trader.orchestration.runtime_exit_cleanup import _record_forced_exit_retry_blocked
from ibkr_trader.orchestration.runtime_exit_cleanup import _remaining_position_quantity
from ibkr_trader.orchestration.runtime_exit_cleanup import _require_confirmed_forced_exit_cleanup_cancel
from ibkr_trader.orchestration.runtime_exit_pricing import _is_delayed_limit_exit_due
from ibkr_trader.orchestration.runtime_fills import aggregate_broker_order_status_fill as _aggregate_broker_order_status_fill
from ibkr_trader.orchestration.runtime_fills import aggregate_executions as _aggregate_executions
from ibkr_trader.orchestration.runtime_fills import aggregate_persisted_execution_fill as _aggregate_persisted_execution_fill
from ibkr_trader.orchestration.runtime_planning import fetch_due_entry_instruction_ids as _fetch_due_entry_instruction_ids
from ibkr_trader.orchestration.runtime_planning import fetch_expired_submitted_entry_instruction_ids as _fetch_expired_submitted_entry_instruction_ids
from ibkr_trader.orchestration.runtime_planning import fetch_instruction_account_keys as _fetch_instruction_account_keys
from ibkr_trader.orchestration.runtime_planning import fetch_instruction_ids as _fetch_instruction_ids
from ibkr_trader.orchestration.runtime_planning import has_virtual_runtime_work as _has_virtual_runtime_work
from ibkr_trader.orchestration.runtime_planning import promote_due_reentry_waiting_for_flat as _promote_due_reentry_waiting_for_flat
from ibkr_trader.orchestration.runtime_planning import should_reconcile_active_runtime_instructions as _should_reconcile_active_runtime_instructions
from ibkr_trader.orchestration.runtime_planning import should_touch_real_broker_for_runtime_cycle as _should_touch_real_broker_for_runtime_cycle
from ibkr_trader.orchestration.runtime_planning import split_instruction_ids_by_virtual as _split_instruction_ids_by_virtual
from ibkr_trader.orchestration.runtime_position_lifecycle import _has_due_real_forced_exit_candidate
from ibkr_trader.orchestration.runtime_position_lifecycle import _is_next_session_exit_due
from ibkr_trader.orchestration.runtime_position_lifecycle import _is_resubmittable_entry_order_status
from ibkr_trader.orchestration.runtime_position_lifecycle import _latest_entry_broker_order_snapshot
from ibkr_trader.orchestration.runtime_position_lifecycle import _mark_entry_requeued_for_resubmit
from ibkr_trader.orchestration.runtime_position_lifecycle import _mark_unfilled_entry_cancelled
from ibkr_trader.orchestration.runtime_position_lifecycle import _record_entry_fill_and_optional_exit
from ibkr_trader.orchestration.runtime_position_lifecycle import _record_exit_fill_and_complete
from ibkr_trader.orchestration.runtime_position_lifecycle import _submit_delayed_limit_exit
from ibkr_trader.orchestration.runtime_position_lifecycle import _submit_forced_exit
from ibkr_trader.orchestration.runtime_protective_exits import _desired_protective_exit_order_refs, _submit_missing_protective_exits
from ibkr_trader.orchestration.runtime_types import ExecutionAggregate, RuntimeCycleAction, RuntimeCycleIssue, RuntimeCycleResult
from ibkr_trader.orchestration.runtime_types import ensure_utc as _ensure_utc
from ibkr_trader.orchestration.runtime_types import parse_decimal as _parse_decimal
from ibkr_trader.orchestration.runtime_types import serialize_for_json as _serialize_for_json
from ibkr_trader.orchestration.state_machine import ExecutionState
from ibkr_trader.virtual.accounts import is_virtual_account_key
from ibkr_trader.virtual.execution import cancel_virtual_order
from ibkr_trader.virtual.execution import has_real_broker_work
from ibkr_trader.virtual.execution import read_virtual_market_price
DEFAULT_BROKER_RETRY_DELAYS: tuple[float, ...] = (1.0, 2.0)
DEFAULT_SUBMISSION_LEAD_TIME = timedelta(seconds=60)


def _merge_instruction_ids(base: list[str], extra: list[str]) -> list[str]:
    return list(dict.fromkeys([*base, *extra]))


def run_runtime_cycle(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    *,
    run_kind: str = "runtime_cycle",
    runtime_timezone: str,
    session_calendar_path: Path,
    now: datetime | None = None,
    timeout: int = 10,
    instruction_ids: tuple[str, ...] | None = None,
    submit_due_entries: bool = True,
    entry_submitter: Callable[..., Any] | None = None,
    entry_canceler: Callable[..., Any] | None = None,
    exit_submitter: Callable[..., dict[str, Any]] | None = None,
    market_price_reader: Callable[..., dict[str, Any]] | None = None,
    broker_snapshot_fetcher: Callable[..., BrokerRuntimeSnapshot] | None = None,
    broker_callback_fetcher: Callable[[], list[dict[str, Any]]] | None = None,
    broker_order_canceler: Callable[..., dict[str, Any]] | None = None,
    virtual_market_sync: Callable[[datetime], Any] | None = None,
    market_data_readiness_checker: MarketDataReadinessChecker | None = None,
    broker_retry_delays: tuple[float, ...] = DEFAULT_BROKER_RETRY_DELAYS,
    submission_lead_time: timedelta = DEFAULT_SUBMISSION_LEAD_TIME,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> RuntimeCycleResult:
    cycle_started_at = now.astimezone(timezone.utc) if now is not None else utc_now()
    runtime_snapshot_fetch = broker_snapshot_fetcher or fetch_broker_runtime_snapshot
    if broker_order_canceler is None:
        def runtime_order_canceler(
            broker_config: IbkrConnectionConfig,
            order_id: int,
            *,
            timeout: int = 10,
        ) -> dict[str, Any]:
            if _is_virtual_broker_order_id(session_factory, order_id=order_id):
                return cancel_virtual_order(
                    session_factory,
                    broker_config,
                    order_id,
                    timeout=timeout,
                )
            return cancel_broker_order(
                broker_config,
                order_id,
                timeout=timeout,
            )
    else:
        runtime_order_canceler = broker_order_canceler
    runtime_entry_canceler = entry_canceler or runtime_order_canceler
    due_instruction_count = 0
    active_instruction_count = 0
    snapshot: BrokerRuntimeSnapshot | None = None
    submitted_entries: list[RuntimeCycleAction] = []
    cancelled_entries: list[RuntimeCycleAction] = []
    filled_entries: list[RuntimeCycleAction] = []
    submitted_exits: list[RuntimeCycleAction] = []
    completed_instructions: list[RuntimeCycleAction] = []
    issues: list[RuntimeCycleIssue] = []
    broker_snapshot_unavailable = False
    def _finish() -> RuntimeCycleResult:
        return _finalize_runtime_cycle_result(
            session_factory,
            broker_config,
            run_kind=run_kind,
            runtime_timezone=runtime_timezone,
            cycle_started_at=cycle_started_at,
            submit_due_entries=submit_due_entries,
            due_instruction_count=due_instruction_count,
            active_instruction_count=active_instruction_count,
            snapshot=snapshot,
            submitted_entries=submitted_entries,
            cancelled_entries=cancelled_entries,
            filled_entries=filled_entries,
            submitted_exits=submitted_exits,
            completed_instructions=completed_instructions,
            issues=issues,
        )
    kill_switch_state = read_kill_switch_state(session_factory)
    kill_switch_enabled = kill_switch_state.enabled
    due_instruction_ids = _fetch_due_entry_instruction_ids(
        session_factory,
        cycle_at=cycle_started_at,
        session_calendar_path=session_calendar_path,
        submission_lead_time=submission_lead_time,
        instruction_ids=instruction_ids,
    )
    if submit_due_entries:
        due_instruction_ids = _merge_instruction_ids(
            due_instruction_ids,
            _promote_due_reentry_waiting_for_flat(
                session_factory,
                cycle_at=cycle_started_at,
                session_calendar_path=session_calendar_path,
                submission_lead_time=submission_lead_time,
                instruction_ids=instruction_ids,
            ),
        )
    expired_submitted_entry_instruction_ids = (
        _fetch_expired_submitted_entry_instruction_ids(
            session_factory,
            cycle_at=cycle_started_at,
            session_calendar_path=session_calendar_path,
            instruction_ids=instruction_ids,
        )
    )
    due_instruction_count = len(due_instruction_ids)
    active_instruction_ids = _fetch_instruction_ids(
        session_factory,
        states=(
            ExecutionState.ENTRY_SUBMITTED.value,
            ExecutionState.POSITION_OPEN.value,
            ExecutionState.EXIT_PENDING.value,
        ),
        instruction_ids=instruction_ids,
    )
    active_instruction_count = len(active_instruction_ids)
    active_reconciliation_enabled = _should_reconcile_active_runtime_instructions(
        run_kind=run_kind,
        cycle_at=cycle_started_at,
        runtime_timezone=runtime_timezone,
        session_calendar_path=session_calendar_path,
        due_instruction_ids=due_instruction_ids,
        expired_submitted_entry_instruction_ids=expired_submitted_entry_instruction_ids,
    )
    if virtual_market_sync is not None and active_reconciliation_enabled:
        try:
            virtual_market_sync(cycle_started_at)
        except Exception as exc:  # pragma: no cover - broad by design for runtime safety
            _append_issue(
                issues,
                instruction_id=None,
                stage="virtual_market_sync",
                message=str(exc),
            )
    real_broker_cycle_enabled = _should_touch_real_broker_for_runtime_cycle(
        session_factory,
        run_kind=run_kind,
        cycle_at=cycle_started_at,
        runtime_timezone=runtime_timezone,
        session_calendar_path=session_calendar_path,
        instruction_ids=instruction_ids,
        due_instruction_ids=due_instruction_ids,
        expired_submitted_entry_instruction_ids=expired_submitted_entry_instruction_ids,
    )
    if broker_callback_fetcher is not None and real_broker_cycle_enabled:
        try:
            _persist_drained_broker_callbacks(
                session_factory,
                broker_config=broker_config,
                callback_events=broker_callback_fetcher(),
            )
        except Exception as exc:  # pragma: no cover - broad by design for runtime safety
            _append_issue(
                issues,
                instruction_id=None,
                stage="broker_callbacks_pre_cycle",
                message=str(exc),
            )
            return _finish()
    if not active_instruction_ids:
        if submit_due_entries:
            _submit_due_pending_entries(
                session_factory,
                broker_config,
                due_instruction_ids=due_instruction_ids,
                cycle_started_at=cycle_started_at,
                session_calendar_path=session_calendar_path,
                timeout=timeout,
                kill_switch_enabled=kill_switch_enabled,
                entry_submitter=entry_submitter,
                broker_retry_delays=broker_retry_delays,
                sleep_fn=sleep_fn,
                submitted_entries=submitted_entries,
                cancelled_entries=cancelled_entries,
                issues=issues,
                market_data_readiness_checker=market_data_readiness_checker,
            )
        if broker_callback_fetcher is not None and real_broker_cycle_enabled:
            try:
                _persist_drained_broker_callbacks(
                    session_factory,
                    broker_config=broker_config,
                    callback_events=broker_callback_fetcher(),
                )
            except Exception as exc:  # pragma: no cover - broad by design for runtime safety
                _append_issue(
                    issues,
                    instruction_id=None,
                    stage="broker_callbacks_post_cycle",
                    message=str(exc),
                )
        return _finish()
    if active_instruction_ids and not active_reconciliation_enabled:
        return _finish()
    with session_scope(session_factory) as session:
        records = session.execute(
            select(InstructionRecord).where(
                InstructionRecord.instruction_id.in_(active_instruction_ids)
            )
        ).scalars().all()
    records_by_instruction_id = {
        record.instruction_id: record for record in records
    }
    forced_exit_snapshot_required = _has_due_real_forced_exit_candidate(
        records,
        runtime_timezone=runtime_timezone,
        session_calendar_path=session_calendar_path,
        cycle_at=cycle_started_at,
        submission_lead_time=submission_lead_time,
    )
    open_orders_snapshot_required = bool(records)
    # The active trading loop must not depend on reqExecutions completing. Fills
    # are recovered first from the durable callback ledger and order-status
    # records; execution-history snapshots are a slow background repair path.
    executions_snapshot_required = False
    real_broker_work = has_real_broker_work(
        session_factory,
        instruction_ids=instruction_ids,
    )
    if real_broker_work and real_broker_cycle_enabled:
        try:
            snapshot = runtime_snapshot_fetch(
                broker_config,
                timeout=timeout,
                include_open_orders=open_orders_snapshot_required,
                include_executions=executions_snapshot_required,
                include_account_updates=False,
                include_positions=forced_exit_snapshot_required,
            )
        except Exception as exc:  # pragma: no cover - broad by design for runtime safety
            broker_snapshot_unavailable = True
            _append_issue(
                issues,
                instruction_id=None,
                stage="broker_snapshot",
                message=_broker_snapshot_unavailable_message(exc),
            )
            if broker_callback_fetcher is not None:
                try:
                    _persist_drained_broker_callbacks(
                        session_factory,
                        broker_config=broker_config,
                        callback_events=broker_callback_fetcher(),
                    )
                except Exception as callback_exc:  # pragma: no cover - broad by design for runtime safety
                    _append_issue(
                        issues,
                        instruction_id=None,
                        stage="broker_callbacks_post_cycle",
                        message=str(callback_exc),
                    )
            snapshot = BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            )
    elif real_broker_work:
        broker_snapshot_unavailable = True
        snapshot = BrokerRuntimeSnapshot(
            open_orders={},
            executions=(),
            portfolio=(),
            positions=(),
            account_values={},
        )
    else:
        snapshot = BrokerRuntimeSnapshot(
            open_orders={},
            executions=(),
            portfolio=(),
            positions=(),
            account_values={},
        )
    try:
        persist_broker_runtime_snapshot(
            session_factory,
            snapshot,
            broker_kind=BROKER_KIND_IBKR,
            captured_at=cycle_started_at,
            default_account_key=broker_config.account_id,
            close_missing_open_orders=(
                real_broker_work
                and real_broker_cycle_enabled
                and not broker_snapshot_unavailable
                and open_orders_snapshot_required
            ),
            empty_open_orders_authoritative=open_orders_snapshot_required,
        )
    except Exception as exc:  # pragma: no cover - broad by design for runtime safety
        _append_issue(
            issues,
            instruction_id=None,
            stage="ledger_persist",
            message=str(exc),
        )
        if broker_callback_fetcher is not None and real_broker_cycle_enabled:
            try:
                _persist_drained_broker_callbacks(
                    session_factory,
                    broker_config=broker_config,
                    callback_events=broker_callback_fetcher(),
                )
            except Exception as callback_exc:  # pragma: no cover - broad by design for runtime safety
                _append_issue(
                    issues,
                    instruction_id=None,
                    stage="broker_callbacks_post_cycle",
                    message=str(callback_exc),
                    )
        return _finish()
    persisted_open_exit_order_ids_by_instruction = _persisted_open_order_ids_by_instruction(
        session_factory,
        records=records,
        order_role="EXIT",
    )
    persisted_open_entry_order_ids_by_instruction = _persisted_open_order_ids_by_instruction(
        session_factory,
        records=records,
        order_role="ENTRY",
    )
    blocking_due_exit_instruction_ids: list[str] = []
    blocking_due_exit_account_keys: set[str] = set()
    for instruction_id in active_instruction_ids:
        record = records_by_instruction_id.get(instruction_id)
        if record is None:
            continue
        try:
            instruction = _instruction_payload(record)
            entry_fill = _aggregate_persisted_execution_fill(
                session_factory,
                record=record,
                order_role="ENTRY",
                external_order_id=record.broker_order_id,
            )
            if not entry_fill.has_fill:
                entry_fill = _aggregate_executions(
                    snapshot.executions,
                    order_id=record.broker_order_id,
                    order_ref_exact=instruction.instruction_id,
                )
            if not entry_fill.has_fill:
                entry_fill = _aggregate_broker_order_status_fill(
                    session_factory,
                    record=record,
                    order_role="ENTRY",
                    external_order_id=record.broker_order_id,
                )
            exit_fill = _aggregate_persisted_execution_fill(
                session_factory,
                record=record,
                order_role="EXIT",
            )
            if not exit_fill.has_fill:
                exit_fill = _aggregate_executions(
                    snapshot.executions,
                    order_ref_prefix=f"{instruction.instruction_id}:exit:",
                )
            if not exit_fill.has_fill:
                exit_fill = _aggregate_broker_order_status_fill(
                    session_factory,
                    record=record,
                    order_role="EXIT",
                )
            persisted_entry_open_order_ids = persisted_open_entry_order_ids_by_instruction.get(
                instruction_id,
                (),
            )
            entry_open = (
                (
                    record.broker_order_id is not None
                    and record.broker_order_id in snapshot.open_orders
                )
                or bool(persisted_entry_open_order_ids)
            )
            exit_open_order_ids = _open_order_ids_with_ref_prefix(
                snapshot,
                order_ref_prefix=f"{instruction.instruction_id}:exit:",
            )
            exit_open_order_refs = _open_order_refs_with_ref_prefix(
                snapshot,
                order_ref_prefix=f"{instruction.instruction_id}:exit:",
            )
            persisted_exit_open_order_ids = persisted_open_exit_order_ids_by_instruction.get(
                instruction_id,
                (),
            )
            combined_exit_open_order_ids = tuple(
                sorted(set(exit_open_order_ids) | set(persisted_exit_open_order_ids))
            )
            exit_open = bool(combined_exit_open_order_ids)
            try:
                effective_expire_at = resolve_effective_entry_expire_at_for_schedule(
                    instruction,
                    submit_at=_ensure_utc(record.submit_at) or record.submit_at,
                    expire_at=_ensure_utc(record.expire_at) or record.expire_at,
                    session_calendar_path=session_calendar_path,
                )
            except Exception:
                effective_expire_at = record.expire_at
            expire_at = _ensure_utc(effective_expire_at) or effective_expire_at
            durable_position_quantity = _parse_decimal(record.entry_filled_quantity)
            if (
                broker_snapshot_unavailable
                and not record.is_virtual
                and not entry_fill.has_fill
                and not exit_fill.has_fill
                and not (
                    record.state
                    in {
                        ExecutionState.POSITION_OPEN.value,
                        ExecutionState.EXIT_PENDING.value,
                    }
                    and durable_position_quantity > 0
                )
            ):
                continue
            if record.state == ExecutionState.ENTRY_SUBMITTED.value:
                if kill_switch_enabled:
                    if entry_fill.has_fill:
                        if entry_open:
                            _run_with_broker_retries(
                                lambda: _cancel_broker_order_and_persist(
                                    session_factory,
                                    broker_config,
                                    order_id=record.broker_order_id,
                                    timeout=timeout,
                                    canceler=runtime_order_canceler,
                                    event_type="entry_order_cancelled_after_fill",
                                    note=(
                                        "Persisted broker cancellation after the entry "
                                        "fill was already observed."
                                    ),
                                ),
                                retry_delays=broker_retry_delays,
                                sleep_fn=sleep_fn,
                            )
                        entry_action, exit_actions = _run_with_broker_retries(
                            lambda: _record_entry_fill_and_optional_exit(
                                session_factory,
                                broker_config,
                                instruction_id,
                                entry_fill=entry_fill,
                                cycle_at=cycle_started_at,
                                timeout=timeout,
                                exit_submitter=exit_submitter,
                            ),
                            retry_delays=broker_retry_delays,
                            sleep_fn=sleep_fn,
                        )
                        filled_entries.append(entry_action)
                        submitted_exits.extend(exit_actions)
                        continue
                    if entry_open:
                        cancellation = _run_with_broker_retries(
                            lambda: cancel_persisted_instruction_entry(
                                session_factory,
                                broker_config,
                                instruction_id,
                                timeout=timeout,
                                canceler=runtime_entry_canceler,
                            ),
                            retry_delays=broker_retry_delays,
                            sleep_fn=sleep_fn,
                        )
                        cancelled_entries.append(
                            RuntimeCycleAction(
                                instruction_id=instruction_id,
                                action="entry_cancelled_by_kill_switch",
                                state=cancellation.state,
                                detail={
                                    "broker_order_id": cancellation.broker_order_id,
                                    "broker_order_status": cancellation.broker_order_status,
                                    "reason": kill_switch_state.reason,
                                },
                            )
                        )
                    else:
                        cancelled_entries.append(
                            _mark_unfilled_entry_cancelled(
                                session_factory,
                                instruction_id,
                                note=(
                                    "Global kill switch was enabled and no open broker "
                                    "entry order remained."
                                ),
                                event_type="entry_order_cancelled_by_kill_switch",
                                action="entry_cancelled_by_kill_switch",
                            )
                        )
                    continue
                if entry_fill.has_fill:
                    if entry_open and cycle_started_at < expire_at:
                        continue
                    if entry_open and instruction.entry.cancel_unfilled_at_expiry:
                        _run_with_broker_retries(
                            lambda: _cancel_broker_order_and_persist(
                                session_factory,
                                broker_config,
                                order_id=record.broker_order_id,
                                timeout=timeout,
                                canceler=runtime_order_canceler,
                                event_type="entry_order_cancelled_post_expiry_fill",
                                note=(
                                    "Persisted broker cancellation after an entry fill "
                                    "arrived beyond the entry expiry window."
                                ),
                            ),
                            retry_delays=broker_retry_delays,
                            sleep_fn=sleep_fn,
                        )
                    entry_action, exit_actions = _run_with_broker_retries(
                        lambda: _record_entry_fill_and_optional_exit(
                            session_factory,
                            broker_config,
                            instruction_id,
                            entry_fill=entry_fill,
                            cycle_at=cycle_started_at,
                            timeout=timeout,
                            exit_submitter=exit_submitter,
                        ),
                        retry_delays=broker_retry_delays,
                        sleep_fn=sleep_fn,
                    )
                    filled_entries.append(entry_action)
                    submitted_exits.extend(exit_actions)
                    continue
                if (
                    run_kind == "runtime_cycle"
                    and not entry_open
                    and cycle_started_at < expire_at
                ):
                    with session_scope(session_factory) as session:
                        latest_record = session.execute(
                            select(InstructionRecord).where(
                                InstructionRecord.instruction_id == instruction_id
                            )
                        ).scalar_one()
                        latest_entry_broker_order = _latest_entry_broker_order_snapshot(
                            session,
                            latest_record,
                        )
                        latest_entry_status = (
                            latest_entry_broker_order.status
                            if latest_entry_broker_order is not None
                            else latest_record.broker_order_status
                        )
                    if _is_resubmittable_entry_order_status(latest_entry_status):
                        requeue_action = _mark_entry_requeued_for_resubmit(
                            session_factory,
                            instruction_id,
                            latest_broker_order=latest_entry_broker_order,
                            note=(
                                "Broker entry order disappeared or reached a terminal "
                                "status before any fill; runtime requeued it for an "
                                "immediate fresh submit attempt."
                            ),
                        )
                        if requeue_action.state != ExecutionState.ENTRY_PENDING.value:
                            submitted_entries.append(requeue_action)
                            continue
                        try:
                            submission = _run_with_broker_retries(
                                lambda: submit_persisted_instruction_entry(
                                    session_factory,
                                    broker_config,
                                    instruction_id,
                                    timeout=timeout,
                                    submitter=entry_submitter,
                                ),
                                retry_delays=broker_retry_delays,
                                sleep_fn=sleep_fn,
                            )
                            submitted_entries.append(
                                RuntimeCycleAction(
                                    instruction_id=instruction_id,
                                    action="entry_resubmitted_after_broker_cancel",
                                    state=submission.state,
                                    detail={
                                        "previous_broker_order": requeue_action.detail,
                                        "broker_order_id": submission.broker_order_id,
                                        "broker_order_status": submission.broker_order_status,
                                    },
                                )
                            )
                        except Exception as exc:  # pragma: no cover - broad by design for runtime safety
                            _append_issue(
                                issues,
                                instruction_id=instruction_id,
                                stage="entry_resubmit",
                                message=str(exc),
                            )
                            if _is_retryable_broker_error(exc):
                                _record_runtime_note(
                                    session_factory,
                                    instruction_id=instruction_id,
                                    event_type="runtime_entry_resubmit_failed",
                                    note=(
                                        "Runtime requeued a broker-cancelled entry but "
                                        "the immediate resubmit attempt hit a retryable "
                                        "broker error; it will remain pending for the "
                                        "next cycle."
                                    ),
                                    payload=_broker_exception_payload(exc),
                                )
                                submitted_entries.append(requeue_action)
                                continue
                            _mark_pending_entry_failed(
                                session_factory,
                                instruction_id,
                                note=(
                                    "Runtime marked the requeued entry as failed after "
                                    "a terminal broker resubmit error."
                                ),
                                payload=_broker_exception_payload(exc),
                                event_type="entry_resubmit_failed",
                            )
                        continue
                if (
                    instruction.entry.cancel_unfilled_at_expiry
                    and cycle_started_at >= expire_at
                ):
                    if entry_open:
                        cancellation = _run_with_broker_retries(
                            lambda: cancel_persisted_instruction_entry(
                                session_factory,
                                broker_config,
                                instruction_id,
                                timeout=timeout,
                                canceler=runtime_entry_canceler,
                            ),
                            retry_delays=broker_retry_delays,
                            sleep_fn=sleep_fn,
                        )
                        cancelled_entries.append(
                            RuntimeCycleAction(
                                instruction_id=instruction_id,
                                action="entry_cancelled_at_expiry",
                                state=cancellation.state,
                                detail={
                                    "broker_order_id": cancellation.broker_order_id,
                                    "broker_order_status": cancellation.broker_order_status,
                                },
                            )
                        )
                    else:
                        cancelled_entries.append(
                            _mark_unfilled_entry_cancelled(
                                session_factory,
                                instruction_id,
                                note=(
                                    "Entry window expired without fills and no open broker "
                                    "entry order remained."
                                ),
                            )
                        )
                continue
            remaining_quantity = _remaining_position_quantity(record, exit_fill)
            if record.state in {
                ExecutionState.POSITION_OPEN.value,
                ExecutionState.EXIT_PENDING.value,
            }:
                if exit_fill.has_fill and exit_fill.quantity > 0:
                    with session_scope(session_factory) as session:
                        writable_record = session.execute(
                            select(InstructionRecord)
                            .where(InstructionRecord.instruction_id == instruction_id)
                            .with_for_update()
                        ).scalar_one()
                        writable_record.exit_filled_quantity = str(exit_fill.quantity)
                        writable_record.exit_avg_fill_price = (
                            str(exit_fill.average_price)
                            if exit_fill.average_price is not None
                            else None
                        )
                        writable_record.exit_filled_at = exit_fill.executed_at
                if remaining_quantity <= 0 and not exit_open:
                    completed_instructions.append(
                        _record_exit_fill_and_complete(
                            session_factory,
                            instruction_id,
                            exit_fill=exit_fill,
                        )
                    )
                    continue
                next_session_exit_due = (
                    remaining_quantity > 0
                    and _is_next_session_exit_due(
                        instruction,
                        runtime_timezone=runtime_timezone,
                        session_calendar_path=session_calendar_path,
                        cycle_at=cycle_started_at,
                        submission_lead_time=submission_lead_time,
                    )
                )
                if next_session_exit_due:
                    blocking_due_exit_instruction_ids.append(instruction_id)
                    blocking_due_exit_account_keys.add(record.account_key)
                    if _has_live_matching_exit_order(
                        snapshot,
                        broker_config,
                        record=record,
                        instruction=instruction,
                        remaining_quantity=remaining_quantity,
                    ):
                        continue
                    terminal_forced_exit = _recent_terminal_forced_exit_failure(
                        session_factory,
                        record=record,
                        cycle_at=cycle_started_at,
                    )
                    if terminal_forced_exit is not None:
                        _append_issue(
                            issues,
                            instruction_id=instruction_id,
                            stage="forced_exit_retry",
                            message=(
                                "Skipped forced exit retry because a recent forced "
                                "market exit reached terminal broker status "
                                f"{terminal_forced_exit['broker_order_status']}."
                            ),
                        )
                        _record_forced_exit_retry_blocked(
                            session_factory,
                            instruction_id=instruction_id,
                            failure=terminal_forced_exit,
                            cycle_at=cycle_started_at,
                        )
                        continue
                    if _has_persisted_open_forced_exit_order(
                        session_factory,
                        record=record,
                    ) or _has_live_open_forced_exit_order(
                        snapshot,
                        instruction_id=instruction_id,
                    ):
                        continue
                    position_block = _forced_exit_broker_position_block(
                        snapshot,
                        broker_config,
                        record=record,
                        instruction=instruction,
                        remaining_quantity=remaining_quantity,
                    )
                    if position_block is not None:
                        _append_issue(
                            issues,
                            instruction_id=instruction_id,
                            stage="forced_exit_position_check",
                            message=(
                                f"{position_block['message']} "
                                f"Required {position_block['required_quantity']}; "
                                f"observed {position_block['observed_quantity'] or 'none'}."
                            ),
                        )
                        _record_forced_exit_position_blocked(
                            session_factory,
                            instruction_id=instruction_id,
                            position_block=position_block,
                            cycle_at=cycle_started_at,
                        )
                        continue
                    forced_exit_conflicts = (
                        _conflicting_exit_order_details_for_forced_exit(
                            session_factory,
                            snapshot,
                            broker_config,
                            record=record,
                            instruction=instruction,
                        )
                    )
                    for open_exit_order_id in combined_exit_open_order_ids:
                        forced_exit_conflicts.setdefault(
                            open_exit_order_id,
                            {
                                "broker_order_id": open_exit_order_id,
                                "sources": ["current_instruction_open_exit"],
                            },
                        )
                    _record_forced_exit_conflict_cleanup_started(
                        session_factory,
                        instruction_id=instruction_id,
                        conflict_details=forced_exit_conflicts,
                        cycle_at=cycle_started_at,
                    )
                    for open_exit_order_id in sorted(forced_exit_conflicts):
                        broker_cancellation = _run_with_broker_retries(
                            lambda order_id=open_exit_order_id: _cancel_broker_order_and_persist(
                                session_factory,
                                broker_config,
                                order_id=order_id,
                                timeout=timeout,
                                canceler=runtime_order_canceler,
                                event_type="exit_order_cancelled_before_forced_exit",
                                note=(
                                    "Persisted broker cancellation before submitting the "
                                    "next-session forced exit."
                                ),
                            ),
                            retry_delays=broker_retry_delays,
                            sleep_fn=sleep_fn,
                        )
                        _require_confirmed_forced_exit_cleanup_cancel(
                            broker_cancellation,
                            order_id=open_exit_order_id,
                        )
                    submitted_exits.append(
                        _run_with_broker_retries(
                            lambda: _submit_forced_exit(
                                session_factory,
                                broker_config,
                                instruction_id,
                                quantity=remaining_quantity,
                                timeout=timeout,
                                exit_submitter=exit_submitter,
                            ),
                            retry_delays=broker_retry_delays,
                            sleep_fn=sleep_fn,
                        )
                    )
                    continue
                if remaining_quantity > 0:
                    protective_order_refs = _desired_protective_exit_order_refs(
                        instruction_id=instruction_id,
                        instruction=instruction,
                    )
                    if protective_order_refs:
                        _cancel_obsolete_exit_orders_for_current_intent(
                            session_factory,
                            snapshot,
                            broker_config,
                            record=record,
                            desired_order_refs=protective_order_refs,
                            cycle_at=cycle_started_at,
                            timeout=timeout,
                            canceler=runtime_order_canceler,
                            broker_retry_delays=broker_retry_delays,
                            sleep_fn=sleep_fn,
                        )
                    repaired_exits = _run_with_broker_retries(
                        lambda: _submit_missing_protective_exits(
                            session_factory,
                            broker_config,
                            instruction_id,
                            quantity=remaining_quantity,
                            cycle_at=cycle_started_at,
                            timeout=timeout,
                            exit_submitter=exit_submitter,
                            existing_exit_order_refs=exit_open_order_refs,
                        ),
                        retry_delays=broker_retry_delays,
                        sleep_fn=sleep_fn,
                    )
                    if repaired_exits:
                        submitted_exits.extend(repaired_exits)
                        continue
                if (
                    _is_delayed_limit_exit_due(
                        instruction,
                        cycle_at=cycle_started_at,
                        session_calendar_path=session_calendar_path,
                        submission_lead_time=submission_lead_time,
                    )
                    and remaining_quantity > 0
                ):
                    delayed_exit_order_ref = f"{instruction_id}:exit:delayed_limit"
                    delayed_exit_order_refs = {delayed_exit_order_ref}
                    _cancel_obsolete_exit_orders_for_current_intent(
                        session_factory,
                        snapshot,
                        broker_config,
                        record=record,
                        desired_order_refs=delayed_exit_order_refs,
                        cycle_at=cycle_started_at,
                        timeout=timeout,
                        canceler=runtime_order_canceler,
                        broker_retry_delays=broker_retry_delays,
                        sleep_fn=sleep_fn,
                    )
                    if (
                        delayed_exit_order_ref in exit_open_order_refs
                        or _has_persisted_open_exit_order_ref(
                            session_factory,
                            record=record,
                            order_ref=delayed_exit_order_ref,
                        )
                    ):
                        continue
                    runtime_market_price_reader = market_price_reader
                    if (
                        runtime_market_price_reader is None
                        and is_virtual_account_key(instruction.account.account_key)
                    ):
                        def _read_virtual_market_price(
                            _broker_config: IbkrConnectionConfig,
                            runtime_instruction: ExecutionInstruction,
                            *,
                            at: datetime,
                            timeout: int = 10,
                        ) -> dict[str, Any]:
                            del at, timeout
                            return read_virtual_market_price(
                                session_factory,
                                runtime_instruction,
                            )
                        runtime_market_price_reader = _read_virtual_market_price
                    if runtime_market_price_reader is None:
                        raise ValueError(
                            "Delayed limit exits require a market_price_reader."
                        )
                    market_reference = _run_with_broker_retries(
                        lambda: runtime_market_price_reader(
                            broker_config,
                            instruction,
                            at=cycle_started_at,
                            timeout=timeout,
                        ),
                        retry_delays=broker_retry_delays,
                        sleep_fn=sleep_fn,
                    )
                    submitted_exits.append(
                        _run_with_broker_retries(
                            lambda: _submit_delayed_limit_exit(
                                session_factory,
                                broker_config,
                                instruction_id,
                                quantity=remaining_quantity,
                                market_reference=market_reference,
                                timeout=timeout,
                                exit_submitter=exit_submitter,
                            ),
                            retry_delays=broker_retry_delays,
                            sleep_fn=sleep_fn,
                        )
                    )
                    continue
        except Exception as exc:  # pragma: no cover - broad by design for runtime safety
            _append_issue(
                issues,
                instruction_id=instruction_id,
                stage="reconcile_instruction",
                message=str(exc),
            )
            _record_runtime_note(
                session_factory,
                instruction_id=instruction_id,
                event_type="runtime_reconcile_failed",
                note="Runtime cycle could not reconcile the instruction cleanly.",
                payload={"error": str(exc)},
            )
    if submit_due_entries:
        due_instruction_ids = _merge_instruction_ids(
            due_instruction_ids,
            _promote_due_reentry_waiting_for_flat(
                session_factory,
                cycle_at=cycle_started_at,
                session_calendar_path=session_calendar_path,
                submission_lead_time=submission_lead_time,
                instruction_ids=instruction_ids,
            ),
        )
        due_instruction_count = len(due_instruction_ids)
        # Active exit workflows take priority over fresh entries so we do not
        # size or submit new risk before urgent carry-over positions are handled.
        # The gate is account-scoped: a virtual account cleanup issue must not
        # prevent an unrelated live account from submitting its own due entry.
        due_instruction_ids_to_submit = due_instruction_ids
        if blocking_due_exit_account_keys and due_instruction_ids:
            due_account_keys = _fetch_instruction_account_keys(
                session_factory,
                due_instruction_ids,
            )
            due_instruction_ids_to_submit = [
                instruction_id
                for instruction_id in due_instruction_ids
                if due_account_keys.get(instruction_id) not in blocking_due_exit_account_keys
            ]
        if broker_snapshot_unavailable and due_instruction_ids_to_submit:
            _append_issue(
                issues,
                instruction_id=None,
                stage="entry_submit",
                message=(
                    "Broker snapshot was unavailable; submitting due entries from "
                    "durable instruction state instead of dropping the trade window."
                ),
            )
        if due_instruction_ids_to_submit:
            _submit_due_pending_entries(
                session_factory,
                broker_config,
                due_instruction_ids=due_instruction_ids_to_submit,
                cycle_started_at=cycle_started_at,
                session_calendar_path=session_calendar_path,
                timeout=timeout,
                kill_switch_enabled=kill_switch_enabled,
                entry_submitter=entry_submitter,
                broker_retry_delays=broker_retry_delays,
                sleep_fn=sleep_fn,
                submitted_entries=submitted_entries,
                cancelled_entries=cancelled_entries,
                issues=issues,
                market_data_readiness_checker=market_data_readiness_checker,
            )
    if broker_callback_fetcher is not None and real_broker_cycle_enabled:
        try:
            _persist_drained_broker_callbacks(
                session_factory,
                broker_config=broker_config,
                callback_events=broker_callback_fetcher(),
            )
        except Exception as exc:  # pragma: no cover - broad by design for runtime safety
            _append_issue(
                issues,
                instruction_id=None,
                stage="broker_callbacks_post_cycle",
                message=str(exc),
            )
    return _finish()
