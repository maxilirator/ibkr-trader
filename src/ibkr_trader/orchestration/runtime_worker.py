from __future__ import annotations

import argparse
import os
import socket
import sys
import time
import uuid
from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from decimal import Decimal
from pathlib import Path
from threading import Event
from threading import Thread
from typing import Any
from typing import Callable

from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.config import AppConfig
from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import BrokerOrderRecord
from ibkr_trader.db.models import InstructionEventRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.domain.execution_contract import ExecutionInstruction
from ibkr_trader.domain.execution_contract import OrderType
from ibkr_trader.domain.execution_payloads import parse_execution_instruction_payload
from ibkr_trader.ibkr.historical_bars import read_latest_trade_price
from ibkr_trader.ibkr.order_execution import cancel_broker_order
from ibkr_trader.ibkr.order_execution import submit_order_from_instruction
from ibkr_trader.ibkr.order_execution import submit_exit_order_from_instruction
from ibkr_trader.ibkr.runtime_snapshot import BrokerRuntimeSnapshot
from ibkr_trader.ibkr.runtime_snapshot import fetch_broker_runtime_snapshot
from ibkr_trader.ibkr.session_manager import CanonicalSyncSessions
from ibkr_trader.ledger.persistence import BROKER_KIND_IBKR
from ibkr_trader.ledger.persistence import persist_broker_callback_events
from ibkr_trader.ledger.persistence import persist_broker_order_cancellation_result
from ibkr_trader.ledger.persistence import persist_broker_order_submission
from ibkr_trader.ledger.persistence import persist_broker_runtime_snapshot
from ibkr_trader.orchestration.entry_submission import (
    cancel_persisted_instruction_entry,
    submit_persisted_instruction_entry,
)
from ibkr_trader.orchestration.operator_controls import read_kill_switch_state
from ibkr_trader.orchestration.runtime_audit import (
    broker_snapshot_unavailable_message as _broker_snapshot_unavailable_message,
)
from ibkr_trader.orchestration.runtime_audit import (
    finalize_runtime_cycle_result as _finalize_runtime_cycle_result,
)
from ibkr_trader.orchestration.runtime_broker_errors import (
    broker_exception_payload as _broker_exception_payload,
)
from ibkr_trader.orchestration.runtime_broker_errors import (
    is_retryable_broker_error as _is_retryable_broker_error,
)
from ibkr_trader.orchestration.runtime_broker_errors import (
    run_with_broker_retries as _run_with_broker_retries,
)
from ibkr_trader.orchestration.runtime_broker_matching import (
    _broker_account_candidates,
)
from ibkr_trader.orchestration.runtime_broker_matching import (
    _exit_side_for_instruction,
)
from ibkr_trader.orchestration.runtime_broker_matching import (
    _forced_exit_broker_position_block,
)
from ibkr_trader.orchestration.runtime_broker_matching import (
    _is_market_broker_order,
)
from ibkr_trader.orchestration.runtime_broker_matching import (
    _is_open_broker_order_status,
)
from ibkr_trader.orchestration.runtime_broker_matching import (
    _matches_exit_cleanup_instrument,
)
from ibkr_trader.orchestration.runtime_broker_matching import (
    _matches_optional_identity,
)
from ibkr_trader.orchestration.runtime_broker_matching import (
    _normalize_broker_identity,
)
from ibkr_trader.orchestration.runtime_broker_matching import (
    _normalize_broker_order_status,
)
from ibkr_trader.orchestration.runtime_cycle import run_runtime_cycle
from ibkr_trader.orchestration.runtime_exit_pricing import (
    _is_delayed_limit_exit_due,
)
from ibkr_trader.orchestration.runtime_entries import _mark_pending_entry_failed
from ibkr_trader.orchestration.runtime_entries import _submit_due_pending_entries
from ibkr_trader.orchestration.runtime_exit_cleanup import (
    _cancel_broker_order_and_persist,
)
from ibkr_trader.orchestration.runtime_exit_cleanup import (
    _cancel_obsolete_exit_orders_for_current_intent,
)
from ibkr_trader.orchestration.runtime_exit_cleanup import (
    _conflicting_exit_order_details_for_forced_exit,
)
from ibkr_trader.orchestration.runtime_exit_cleanup import (
    _has_live_matching_exit_order,
)
from ibkr_trader.orchestration.runtime_exit_cleanup import (
    _has_live_open_forced_exit_order,
)
from ibkr_trader.orchestration.runtime_exit_cleanup import (
    _has_persisted_open_exit_order_ref,
)
from ibkr_trader.orchestration.runtime_exit_cleanup import (
    _has_persisted_open_forced_exit_order,
)
from ibkr_trader.orchestration.runtime_exit_cleanup import (
    _is_virtual_broker_order_id,
)
from ibkr_trader.orchestration.runtime_exit_cleanup import (
    _open_order_ids_with_ref_prefix,
)
from ibkr_trader.orchestration.runtime_exit_cleanup import (
    _open_order_refs_with_ref_prefix,
)
from ibkr_trader.orchestration.runtime_exit_cleanup import (
    _persisted_open_order_ids_by_instruction,
)
from ibkr_trader.orchestration.runtime_exit_cleanup import (
    _recent_terminal_forced_exit_failure,
)
from ibkr_trader.orchestration.runtime_exit_cleanup import (
    _record_forced_exit_conflict_cleanup_started,
)
from ibkr_trader.orchestration.runtime_exit_cleanup import (
    _record_forced_exit_position_blocked,
)
from ibkr_trader.orchestration.runtime_exit_cleanup import (
    _record_forced_exit_retry_blocked,
)
from ibkr_trader.orchestration.runtime_exit_cleanup import (
    _remaining_position_quantity,
)
from ibkr_trader.orchestration.runtime_exit_cleanup import (
    _require_confirmed_forced_exit_cleanup_cancel,
)
from ibkr_trader.orchestration.runtime_position_lifecycle import (
    _has_due_real_forced_exit_candidate,
)
from ibkr_trader.orchestration.runtime_position_lifecycle import (
    _is_next_session_exit_due,
)
from ibkr_trader.orchestration.runtime_position_lifecycle import (
    _is_resubmittable_entry_order_status,
)
from ibkr_trader.orchestration.runtime_position_lifecycle import (
    _latest_entry_broker_order_snapshot,
)
from ibkr_trader.orchestration.runtime_position_lifecycle import (
    _mark_entry_requeued_for_resubmit,
)
from ibkr_trader.orchestration.runtime_position_lifecycle import (
    _mark_unfilled_entry_cancelled,
)
from ibkr_trader.orchestration.runtime_position_lifecycle import (
    _record_entry_fill_and_optional_exit,
)
from ibkr_trader.orchestration.runtime_position_lifecycle import (
    _record_exit_fill_and_complete,
)
from ibkr_trader.orchestration.runtime_position_lifecycle import (
    _submit_delayed_limit_exit,
)
from ibkr_trader.orchestration.runtime_position_lifecycle import (
    _submit_forced_exit,
)
from ibkr_trader.orchestration.scheduling import (
    resolve_effective_entry_expire_at_for_schedule,
    resolve_scheduled_submission_due_at,
)
from ibkr_trader.orchestration.runtime_planning import (
    fetch_due_entry_instruction_ids as _fetch_due_entry_instruction_ids,
)
from ibkr_trader.orchestration.runtime_planning import (
    fetch_expired_submitted_entry_instruction_ids as _fetch_expired_submitted_entry_instruction_ids,
)
from ibkr_trader.orchestration.runtime_planning import (
    fetch_instruction_account_keys as _fetch_instruction_account_keys,
)
from ibkr_trader.orchestration.runtime_planning import (
    fetch_instruction_ids as _fetch_instruction_ids,
)
from ibkr_trader.orchestration.runtime_planning import (
    has_virtual_runtime_work as _has_virtual_runtime_work,
)
from ibkr_trader.orchestration.runtime_planning import (
    is_pending_entry_expired as _is_pending_entry_expired,
)
from ibkr_trader.orchestration.runtime_planning import (
    should_reconcile_active_runtime_instructions as _should_reconcile_active_runtime_instructions,
)
from ibkr_trader.orchestration.runtime_planning import (
    should_touch_real_broker_for_runtime_cycle as _should_touch_real_broker_for_runtime_cycle,
)
from ibkr_trader.orchestration.runtime_planning import (
    split_instruction_ids_by_virtual as _split_instruction_ids_by_virtual,
)
from ibkr_trader.orchestration.runtime_protective_exits import (
    _build_protective_exit_specs,
)
from ibkr_trader.orchestration.runtime_protective_exits import (
    _desired_protective_exit_order_refs,
)
from ibkr_trader.orchestration.runtime_protective_exits import (
    _submit_missing_protective_exits,
)
from ibkr_trader.orchestration.runtime_service_state import (
    EXECUTION_RUNTIME_KEY,
    RuntimeServiceLeaseError,
    acquire_runtime_service_lease,
    mark_runtime_service_failed,
    mark_runtime_service_startup_blocked,
    mark_runtime_service_stopped,
    read_runtime_service_status,
    record_runtime_cycle_completed,
    record_runtime_cycle_started,
    serialize_runtime_service_status,
)
from ibkr_trader.orchestration.state_machine import ExecutionState
from ibkr_trader.orchestration.runtime_fills import (
    aggregate_broker_order_status_fill as _aggregate_broker_order_status_fill,
)
from ibkr_trader.orchestration.runtime_fills import (
    aggregate_executions as _aggregate_executions,
)
from ibkr_trader.orchestration.runtime_fills import (
    aggregate_persisted_execution_fill as _aggregate_persisted_execution_fill,
)
from ibkr_trader.orchestration.runtime_types import ExecutionAggregate
from ibkr_trader.orchestration.runtime_types import RuntimeCycleAction
from ibkr_trader.orchestration.runtime_types import RuntimeCycleIssue
from ibkr_trader.orchestration.runtime_types import RuntimeCycleResult
from ibkr_trader.orchestration.runtime_types import emit_runtime_cycle_result as _emit_runtime_cycle_result
from ibkr_trader.orchestration.runtime_types import ensure_utc as _ensure_utc
from ibkr_trader.orchestration.runtime_types import parse_decimal as _parse_decimal
from ibkr_trader.orchestration.runtime_types import serialize_runtime_cycle_result
from ibkr_trader.orchestration.runtime_types import serialize_for_json as _serialize_for_json
from ibkr_trader.virtual.accounts import is_virtual_account_key
from ibkr_trader.virtual.execution import cancel_virtual_order
from ibkr_trader.virtual.execution import has_real_broker_work
from ibkr_trader.virtual.execution import read_virtual_market_price
from ibkr_trader.virtual.execution import submit_virtual_exit_order

DEFAULT_BROKER_RETRY_DELAYS: tuple[float, ...] = (1.0, 2.0)
DEFAULT_SUBMISSION_LEAD_TIME = timedelta(seconds=60)
_CLOSED_BROKER_ORDER_STATUSES = {
    "API_CANCELLED",
    "CANCELLED",
    "ERROR",
    "FILLED",
    "INACTIVE",
    "NOT_FOUND_AT_BROKER",
    "REJECTED",
}



def run_startup_reconciliation(
    session_factory: sessionmaker[Session],
    broker_config: IbkrConnectionConfig,
    *,
    runtime_timezone: str,
    session_calendar_path: Path,
    now: datetime | None = None,
    timeout: int = 10,
    instruction_ids: tuple[str, ...] | None = None,
    exit_submitter: Callable[..., dict[str, Any]] | None = None,
    market_price_reader: Callable[..., dict[str, Any]] | None = None,
    broker_snapshot_fetcher: Callable[..., BrokerRuntimeSnapshot] | None = None,
    broker_callback_fetcher: Callable[[], list[dict[str, Any]]] | None = None,
    broker_order_canceler: Callable[..., dict[str, Any]] | None = None,
    virtual_market_sync: Callable[[datetime], Any] | None = None,
    broker_retry_delays: tuple[float, ...] = DEFAULT_BROKER_RETRY_DELAYS,
    submission_lead_time: timedelta = DEFAULT_SUBMISSION_LEAD_TIME,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> RuntimeCycleResult:
    """Reconcile live broker state on startup without submitting new entry orders."""

    return run_runtime_cycle(
        session_factory,
        broker_config,
        run_kind="startup_reconciliation",
        runtime_timezone=runtime_timezone,
        session_calendar_path=session_calendar_path,
        now=now,
        timeout=timeout,
        instruction_ids=instruction_ids,
        submit_due_entries=False,
        exit_submitter=exit_submitter,
        market_price_reader=market_price_reader,
        broker_snapshot_fetcher=broker_snapshot_fetcher,
        broker_callback_fetcher=broker_callback_fetcher,
        broker_order_canceler=broker_order_canceler,
        virtual_market_sync=virtual_market_sync,
        broker_retry_delays=broker_retry_delays,
        submission_lead_time=submission_lead_time,
        sleep_fn=sleep_fn,
    )


@dataclass(slots=True)
class RuntimeBrokerOperations:
    submit_entry: Callable[..., dict[str, Any]]
    submit_exit: Callable[..., dict[str, Any]]
    read_market_price: Callable[..., dict[str, Any]]
    fetch_snapshot: Callable[..., BrokerRuntimeSnapshot]
    fetch_reconciliation_snapshot: Callable[..., BrokerRuntimeSnapshot]
    drain_callbacks: Callable[[], list[dict[str, Any]]]
    cancel_order: Callable[..., dict[str, Any]]


def _build_runtime_broker_operations(
    broker_sessions: CanonicalSyncSessions,
    session_factory: sessionmaker[Session] | None = None,
) -> RuntimeBrokerOperations:
    def submit_entry_with_primary(
        broker_config: IbkrConnectionConfig,
        instruction: ExecutionInstruction,
        *,
        timeout: int = 10,
    ) -> dict[str, Any]:
        if session_factory is not None and is_virtual_account_key(
            instruction.account.account_key
        ):
            from ibkr_trader.virtual.execution import submit_virtual_entry_order

            return submit_virtual_entry_order(
                session_factory,
                broker_config,
                instruction,
                timeout=timeout,
            )
        return broker_sessions.primary.execute(
            "runtime_entry_submit",
            lambda broker_app: submit_order_from_instruction(
                broker_config,
                instruction,
                timeout=timeout,
                app=broker_app,
            ),
        )

    def submit_exit_with_primary(
        broker_config: IbkrConnectionConfig,
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
    ) -> dict[str, Any]:
        if session_factory is not None and is_virtual_account_key(
            instruction.account.account_key
        ):
            return submit_virtual_exit_order(
                session_factory,
                broker_config,
                instruction,
                quantity=quantity,
                order_type=order_type,
                order_ref=order_ref,
                timeout=timeout,
                limit_price=limit_price,
                stop_price=stop_price,
                oca_group=oca_group,
                oca_type=oca_type,
            )
        return broker_sessions.primary.execute(
            "runtime_exit_submit",
            lambda broker_app: submit_exit_order_from_instruction(
                broker_config,
                instruction,
                quantity=quantity,
                order_type=order_type,
                order_ref=order_ref,
                timeout=timeout,
                limit_price=limit_price,
                stop_price=stop_price,
                oca_group=oca_group,
                oca_type=oca_type,
                app=broker_app,
            ),
        )

    def read_market_price_with_primary(
        broker_config: IbkrConnectionConfig,
        instruction: ExecutionInstruction,
        *,
        at: datetime,
        timeout: int = 10,
    ) -> dict[str, Any]:
        if session_factory is not None and is_virtual_account_key(
            instruction.account.account_key
        ):
            return read_virtual_market_price(session_factory, instruction)
        return broker_sessions.primary.execute(
            "runtime_market_reference",
            lambda broker_app: read_latest_trade_price(
                broker_config,
                symbol=instruction.instrument.symbol,
                exchange=instruction.instrument.exchange,
                currency=instruction.instrument.currency,
                security_type=instruction.instrument.security_type.value,
                primary_exchange=instruction.instrument.primary_exchange,
                isin=instruction.instrument.isin,
                end_at=at,
                timeout=timeout,
                app=broker_app,
            ),
        )

    def fetch_snapshot_with_primary(
        broker_config: IbkrConnectionConfig,
        *,
        timeout: int = 10,
        include_open_orders: bool = False,
        include_executions: bool = False,
        include_account_updates: bool = False,
        include_positions: bool = False,
    ) -> BrokerRuntimeSnapshot:
        if session_factory is not None and not has_real_broker_work(session_factory):
            return BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            )
        return broker_sessions.primary.execute(
            "runtime_snapshot",
            lambda broker_app: fetch_broker_runtime_snapshot(
                broker_config,
                timeout=timeout,
                include_open_orders=include_open_orders,
                include_executions=include_executions,
                include_account_updates=include_account_updates,
                include_positions=include_positions,
                app=broker_app,
            ),
        )

    def fetch_reconciliation_snapshot_with_primary(
        broker_config: IbkrConnectionConfig,
        *,
        timeout: int = 10,
        include_open_orders: bool = True,
        include_executions: bool = True,
        include_account_updates: bool = False,
        include_positions: bool = True,
    ) -> BrokerRuntimeSnapshot:
        if session_factory is not None and not has_real_broker_work(session_factory):
            return BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            )
        return broker_sessions.primary.execute(
            "runtime_reconciliation_snapshot",
            lambda broker_app: fetch_broker_runtime_snapshot(
                broker_config,
                timeout=timeout,
                include_open_orders=True,
                include_executions=True,
                include_account_updates=include_account_updates,
                include_positions=True,
                app=broker_app,
            ),
        )

    def drain_callbacks_with_primary() -> list[dict[str, Any]]:
        if session_factory is not None and not has_real_broker_work(session_factory):
            return []
        return broker_sessions.primary.drain_broker_callback_events(
            connect_if_needed=False,
        )

    def cancel_order_with_primary(
        broker_config: IbkrConnectionConfig,
        order_id: int,
        *,
        timeout: int = 10,
    ) -> dict[str, Any]:
        if session_factory is not None and _is_virtual_broker_order_id(
            session_factory,
            order_id=order_id,
        ):
            return cancel_virtual_order(
                session_factory,
                broker_config,
                order_id,
                timeout=timeout,
            )
        return broker_sessions.primary.execute(
            "runtime_cancel",
            lambda broker_app: cancel_broker_order(
                broker_config,
                order_id,
                timeout=timeout,
                app=broker_app,
            ),
        )

    return RuntimeBrokerOperations(
        submit_entry=submit_entry_with_primary,
        submit_exit=submit_exit_with_primary,
        read_market_price=read_market_price_with_primary,
        fetch_snapshot=fetch_snapshot_with_primary,
        fetch_reconciliation_snapshot=fetch_reconciliation_snapshot_with_primary,
        drain_callbacks=drain_callbacks_with_primary,
        cancel_order=cancel_order_with_primary,
    )


def _runtime_owner_label() -> tuple[str, str, int]:
    hostname = socket.gethostname()
    pid = os.getpid()
    return f"{hostname}:{pid}", hostname, pid


def run_persistent_execution_runtime(
    session_factory: sessionmaker[Session],
    app_config: AppConfig,
    broker_sessions: CanonicalSyncSessions,
    *,
    interval_seconds: float,
    timeout: int,
    once: bool = False,
    skip_startup_reconciliation: bool = False,
    allow_startup_issues: bool = False,
    runtime_key: str = EXECUTION_RUNTIME_KEY,
    lease_seconds: float = 30.0,
    stop_event: Event | None = None,
    emit_results: bool = True,
    shutdown_sessions_on_exit: bool = True,
    virtual_market_sync: Callable[[datetime], Any] | None = None,
) -> int:
    runtime_stop_event = stop_event or Event()
    owner_token = uuid.uuid4().hex
    owner_label, hostname, pid = _runtime_owner_label()
    broker_config = app_config.ibkr.primary_session()
    broker_ops = _build_runtime_broker_operations(broker_sessions, session_factory)
    submission_lead_time = timedelta(
        seconds=app_config.execution_runtime_submission_lead_seconds
    )

    acquire_runtime_service_lease(
        session_factory,
        runtime_key=runtime_key,
        service_type="execution",
        owner_token=owner_token,
        owner_label=owner_label,
        hostname=hostname,
        pid=pid,
        runtime_timezone=app_config.timezone,
        broker_kind=BROKER_KIND_IBKR,
        broker_client_id=broker_config.client_id,
        lease_seconds=lease_seconds,
        metadata_json={
            "interval_seconds": interval_seconds,
            "timeout_seconds": timeout,
            "submission_lead_seconds": app_config.execution_runtime_submission_lead_seconds,
            "allow_startup_issues": allow_startup_issues,
        },
    )

    broker_sessions.warmup()
    runtime_released = False
    try:
        if not skip_startup_reconciliation:
            record_runtime_cycle_started(
                session_factory,
                runtime_key=runtime_key,
                owner_token=owner_token,
                lease_seconds=lease_seconds,
            )
            startup_result = run_startup_reconciliation(
                session_factory,
                broker_config,
                runtime_timezone=app_config.timezone,
                session_calendar_path=app_config.session_calendar_path,
                timeout=timeout,
                exit_submitter=broker_ops.submit_exit,
                market_price_reader=broker_ops.read_market_price,
                broker_snapshot_fetcher=broker_ops.fetch_reconciliation_snapshot,
                broker_callback_fetcher=broker_ops.drain_callbacks,
                broker_order_canceler=broker_ops.cancel_order,
                virtual_market_sync=virtual_market_sync,
                submission_lead_time=submission_lead_time,
            )
            record_runtime_cycle_completed(
                session_factory,
                runtime_key=runtime_key,
                owner_token=owner_token,
                lease_seconds=lease_seconds,
                result=startup_result,
            )
            if emit_results:
                _emit_runtime_cycle_result(startup_result)
            if (
                startup_result.issues
                and not allow_startup_issues
                and not _has_virtual_runtime_work(session_factory)
            ):
                mark_runtime_service_startup_blocked(
                    session_factory,
                    runtime_key=runtime_key,
                    owner_token=owner_token,
                    result=startup_result,
                )
                runtime_released = True
                print(
                    (
                        "Startup reconciliation reported issues; refusing to start the "
                        "runtime loop. Re-run with --allow-startup-issues to override."
                    ),
                    file=sys.stderr,
                )
                return 2
            if startup_result.issues and not allow_startup_issues:
                print(
                    (
                        "Startup reconciliation reported issues; continuing the "
                        "runtime loop for virtual work. Real broker mutations remain "
                        "blocked until broker state is readable."
                    ),
                    file=sys.stderr,
                )

        while True:
            if runtime_stop_event.is_set():
                break

            lease_snapshot = record_runtime_cycle_started(
                session_factory,
                runtime_key=runtime_key,
                owner_token=owner_token,
                lease_seconds=lease_seconds,
            )
            if lease_snapshot.stop_requested:
                break

            result = run_runtime_cycle(
                session_factory,
                broker_config,
                runtime_timezone=app_config.timezone,
                session_calendar_path=app_config.session_calendar_path,
                timeout=timeout,
                entry_submitter=broker_ops.submit_entry,
                exit_submitter=broker_ops.submit_exit,
                market_price_reader=broker_ops.read_market_price,
                broker_snapshot_fetcher=broker_ops.fetch_snapshot,
                broker_callback_fetcher=broker_ops.drain_callbacks,
                broker_order_canceler=broker_ops.cancel_order,
                virtual_market_sync=virtual_market_sync,
                submission_lead_time=submission_lead_time,
            )
            record_runtime_cycle_completed(
                session_factory,
                runtime_key=runtime_key,
                owner_token=owner_token,
                lease_seconds=lease_seconds,
                result=result,
            )
            if emit_results:
                _emit_runtime_cycle_result(result)
            if once:
                break
            if runtime_stop_event.wait(interval_seconds):
                break

        stop_note = (
            "Completed the requested one-shot execution-runtime cycle."
            if once
            else "Execution runtime stopped cleanly."
        )
        mark_runtime_service_stopped(
            session_factory,
            runtime_key=runtime_key,
            owner_token=owner_token,
            note=stop_note,
        )
        runtime_released = True
        return 0
    except Exception as exc:
        if not runtime_released:
            try:
                mark_runtime_service_failed(
                    session_factory,
                    runtime_key=runtime_key,
                    owner_token=owner_token,
                    error=str(exc),
                )
                runtime_released = True
            except RuntimeServiceLeaseError:
                pass
        raise
    finally:
        if shutdown_sessions_on_exit:
            broker_sessions.shutdown()


class BackgroundExecutionRuntimeService:
    """Run the execution runtime loop inside the long-lived API host process."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        app_config: AppConfig,
        broker_sessions: CanonicalSyncSessions,
        virtual_market_sync: Callable[[datetime], Any] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._app_config = app_config
        self._broker_sessions = broker_sessions
        self._virtual_market_sync = virtual_market_sync
        self._stop_event = Event()
        self._thread: Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        thread = Thread(
            target=self._run,
            name="execution-runtime-service",
            daemon=True,
        )
        thread.start()
        self._thread = thread

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=max(5.0, self._app_config.execution_runtime_interval_seconds + 5.0))
        self._thread = None

    def status(self) -> dict[str, Any] | None:
        return serialize_runtime_service_status(
            read_runtime_service_status(
                self._session_factory,
                runtime_key=EXECUTION_RUNTIME_KEY,
            )
        )

    def _run(self) -> None:
        restart_delay = max(
            0.0,
            self._app_config.execution_runtime_restart_backoff_initial_seconds,
        )
        max_restart_delay = max(
            restart_delay,
            self._app_config.execution_runtime_restart_backoff_max_seconds,
        )
        while not self._stop_event.is_set():
            try:
                exit_code = run_persistent_execution_runtime(
                    self._session_factory,
                    self._app_config,
                    self._broker_sessions,
                    interval_seconds=self._app_config.execution_runtime_interval_seconds,
                    timeout=self._app_config.execution_runtime_timeout_seconds,
                    allow_startup_issues=self._app_config.execution_runtime_allow_startup_issues,
                    lease_seconds=self._app_config.execution_runtime_lease_seconds,
                    stop_event=self._stop_event,
                    emit_results=False,
                    shutdown_sessions_on_exit=False,
                    virtual_market_sync=self._virtual_market_sync,
                )
            except RuntimeServiceLeaseError:
                if self._stop_event.wait(restart_delay):
                    return
                continue
            except Exception:
                if self._stop_event.wait(restart_delay):
                    return
                restart_delay = min(max_restart_delay, restart_delay * 2)
                continue

            if self._stop_event.is_set() or exit_code == 0:
                return
            if self._stop_event.wait(restart_delay):
                return
            restart_delay = min(max_restart_delay, restart_delay * 2)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the IBKR Trader MVP runtime loop."
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=5.0,
        help="Seconds to sleep between runtime cycles.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run exactly one runtime cycle and exit.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Broker request timeout in seconds.",
    )
    parser.add_argument(
        "--skip-startup-reconciliation",
        action="store_true",
        help=(
            "Skip the startup reconciliation pass. This is not recommended for the "
            "persistent runtime."
        ),
    )
    parser.add_argument(
        "--allow-startup-issues",
        action="store_true",
        help=(
            "Continue into the runtime loop even if startup reconciliation reports issues."
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    app_config = AppConfig.from_env()
    session_factory = create_session_factory(build_engine(app_config.database_url))
    broker_sessions = CanonicalSyncSessions(app_config.ibkr)
    try:
        return run_persistent_execution_runtime(
            session_factory,
            app_config,
            broker_sessions,
            interval_seconds=args.interval_seconds,
            timeout=args.timeout,
            once=args.once,
            skip_startup_reconciliation=args.skip_startup_reconciliation,
            allow_startup_issues=args.allow_startup_issues,
            lease_seconds=app_config.execution_runtime_lease_seconds,
        )
    except RuntimeServiceLeaseError as exc:
        print(str(exc), file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
