from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Boolean
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship

from ibkr_trader.db.base import Base
from ibkr_trader.db.base import utc_now


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
        nullable=False,
    )


class InstrumentRecord(TimestampMixin, Base):
    """Canonical instrument identity used by execution and ledger rows."""

    __tablename__ = "instrument"
    __table_args__ = (
        UniqueConstraint(
            "symbol",
            "exchange",
            "currency",
            "security_type",
            name="uq_instrument_identity",
        ),
        Index("ix_instrument_ibkr_con_id", "ibkr_con_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    exchange: Mapped[str] = mapped_column(String(32), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    security_type: Mapped[str] = mapped_column(String(16), nullable=False)
    primary_exchange: Mapped[str | None] = mapped_column(String(32))
    company_name: Mapped[str | None] = mapped_column(String(256))
    isin: Mapped[str | None] = mapped_column(String(32))
    ibkr_con_id: Mapped[int | None] = mapped_column(Integer)
    local_symbol: Mapped[str | None] = mapped_column(String(64))
    trading_class: Mapped[str | None] = mapped_column(String(64))
    aliases: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)


class InstructionRecord(TimestampMixin, Base):
    """Strategy intent persisted before any specific broker orders are emitted."""

    __tablename__ = "instruction"
    __table_args__ = (
        UniqueConstraint("instruction_id", name="uq_instruction_instruction_id"),
        Index("ix_instruction_batch_id", "batch_id"),
        Index("ix_instruction_state", "state"),
        Index("ix_instruction_account", "account_key"),
        Index("ix_instruction_broker_order_id", "broker_order_id"),
        Index("ix_instruction_broker_perm_id", "broker_perm_id"),
        Index("ix_instruction_exit_order_id", "exit_order_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instruction_id: Mapped[str] = mapped_column(String(128), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(32), nullable=False)
    source_system: Mapped[str] = mapped_column(String(128), nullable=False)
    batch_id: Mapped[str] = mapped_column(String(128), nullable=False)
    account_key: Mapped[str] = mapped_column(String(64), nullable=False)
    book_key: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    exchange: Mapped[str] = mapped_column(String(32), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    submit_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expire_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    order_type: Mapped[str] = mapped_column(String(16), nullable=False)
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    broker_order_id: Mapped[int | None] = mapped_column(Integer)
    broker_perm_id: Mapped[int | None] = mapped_column(Integer)
    broker_client_id: Mapped[int | None] = mapped_column(Integer)
    broker_order_status: Mapped[str | None] = mapped_column(String(32))
    entry_submitted_quantity: Mapped[str | None] = mapped_column(String(64))
    entry_filled_quantity: Mapped[str | None] = mapped_column(String(64))
    entry_avg_fill_price: Mapped[str | None] = mapped_column(String(64))
    entry_filled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    exit_order_id: Mapped[int | None] = mapped_column(Integer)
    exit_perm_id: Mapped[int | None] = mapped_column(Integer)
    exit_client_id: Mapped[int | None] = mapped_column(Integer)
    exit_order_status: Mapped[str | None] = mapped_column(String(32))
    exit_submitted_quantity: Mapped[str | None] = mapped_column(String(64))
    exit_filled_quantity: Mapped[str | None] = mapped_column(String(64))
    exit_avg_fill_price: Mapped[str | None] = mapped_column(String(64))
    exit_filled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    events: Mapped[list["InstructionEventRecord"]] = relationship(
        back_populates="instruction",
        cascade="all, delete-orphan",
    )


class InstructionEventRecord(Base):
    """Append-only instruction state transition and audit trail."""

    __tablename__ = "instruction_event"
    __table_args__ = (
        Index("ix_instruction_event_instruction_id", "instruction_id"),
        Index("ix_instruction_event_event_at", "event_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instruction_id: Mapped[int] = mapped_column(
        ForeignKey("instruction.id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    event_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    state_before: Mapped[str | None] = mapped_column(String(32))
    state_after: Mapped[str | None] = mapped_column(String(32))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    note: Mapped[str | None] = mapped_column(Text)

    instruction: Mapped[InstructionRecord] = relationship(back_populates="events")


class OperatorControlRecord(TimestampMixin, Base):
    """Current durable operator control state, such as the global kill switch."""

    __tablename__ = "operator_control"
    __table_args__ = (
        UniqueConstraint("control_key", name="uq_operator_control_control_key"),
        Index("ix_operator_control_enabled", "enabled"),
        Index("ix_operator_control_last_changed_at", "last_changed_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    control_key: Mapped[str] = mapped_column(String(64), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    reason: Mapped[str | None] = mapped_column(Text)
    updated_by: Mapped[str | None] = mapped_column(String(64))
    last_changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    events: Mapped[list["OperatorControlEventRecord"]] = relationship(
        back_populates="operator_control",
        cascade="all, delete-orphan",
    )


class OperatorControlEventRecord(Base):
    """Append-only operator control audit trail."""

    __tablename__ = "operator_control_event"
    __table_args__ = (
        Index("ix_operator_control_event_control_id", "operator_control_id"),
        Index("ix_operator_control_event_event_at", "event_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    operator_control_id: Mapped[int] = mapped_column(
        ForeignKey("operator_control.id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    event_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    reason: Mapped[str | None] = mapped_column(Text)
    updated_by: Mapped[str | None] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    note: Mapped[str | None] = mapped_column(Text)

    operator_control: Mapped[OperatorControlRecord] = relationship(back_populates="events")


class InstructionSetCancellationRecord(TimestampMixin, Base):
    """Durable audit row for operator-triggered instruction-set cancellation requests."""

    __tablename__ = "instruction_set_cancellation"
    __table_args__ = (
        Index("ix_instruction_set_cancellation_requested_at", "requested_at"),
        Index("ix_instruction_set_cancellation_status", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    requested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    requested_by: Mapped[str] = mapped_column(String(64), nullable=False)
    reason: Mapped[str | None] = mapped_column(Text)
    selectors: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    matched_instruction_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    cancelled_pending_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    cancelled_submitted_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    skipped_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    failed_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    result_payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)


class BrokerAccountRecord(TimestampMixin, Base):
    """Broker account registry shared across execution, ledger, and UI projections."""

    __tablename__ = "broker_account"
    __table_args__ = (
        UniqueConstraint(
            "broker_kind",
            "account_key",
            name="uq_broker_account_identity",
        ),
        Index("ix_broker_account_broker_kind", "broker_kind"),
        Index("ix_broker_account_account_key", "account_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    broker_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    account_key: Mapped[str] = mapped_column(String(64), nullable=False)
    account_label: Mapped[str | None] = mapped_column(String(256))
    base_currency: Mapped[str | None] = mapped_column(String(8))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    orders: Mapped[list["BrokerOrderRecord"]] = relationship(back_populates="broker_account")
    account_snapshots: Mapped[list["AccountSnapshotRecord"]] = relationship(
        back_populates="broker_account",
        cascade="all, delete-orphan",
    )
    position_snapshots: Mapped[list["PositionSnapshotRecord"]] = relationship(
        back_populates="broker_account",
        cascade="all, delete-orphan",
    )


class BrokerOrderRecord(TimestampMixin, Base):
    """Broker-native order envelope emitted from an instruction."""

    __tablename__ = "broker_order"
    __table_args__ = (
        UniqueConstraint(
            "broker_kind",
            "account_key",
            "external_order_id",
            name="uq_broker_order_external_order_id",
        ),
        Index("ix_broker_order_instruction_id", "instruction_id"),
        Index("ix_broker_order_broker_account_id", "broker_account_id"),
        Index("ix_broker_order_external_perm_id", "external_perm_id"),
        Index("ix_broker_order_status", "status"),
        Index("ix_broker_order_order_ref", "order_ref"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instruction_id: Mapped[int | None] = mapped_column(
        ForeignKey("instruction.id", ondelete="SET NULL")
    )
    broker_account_id: Mapped[int] = mapped_column(
        ForeignKey("broker_account.id", ondelete="CASCADE"),
        nullable=False,
    )
    broker_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    account_key: Mapped[str] = mapped_column(String(64), nullable=False)
    order_role: Mapped[str] = mapped_column(String(32), nullable=False)
    external_order_id: Mapped[str | None] = mapped_column(String(64))
    external_perm_id: Mapped[str | None] = mapped_column(String(64))
    external_client_id: Mapped[str | None] = mapped_column(String(64))
    order_ref: Mapped[str | None] = mapped_column(String(128))
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    exchange: Mapped[str] = mapped_column(String(32), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    security_type: Mapped[str] = mapped_column(String(16), nullable=False)
    primary_exchange: Mapped[str | None] = mapped_column(String(32))
    local_symbol: Mapped[str | None] = mapped_column(String(64))
    side: Mapped[str] = mapped_column(String(8), nullable=False)
    order_type: Mapped[str] = mapped_column(String(16), nullable=False)
    time_in_force: Mapped[str | None] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    total_quantity: Mapped[str | None] = mapped_column(String(64))
    limit_price: Mapped[str | None] = mapped_column(String(64))
    stop_price: Mapped[str | None] = mapped_column(String(64))
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_status_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    instruction: Mapped[InstructionRecord | None] = relationship()
    broker_account: Mapped[BrokerAccountRecord] = relationship(back_populates="orders")
    events: Mapped[list["BrokerOrderEventRecord"]] = relationship(
        back_populates="broker_order",
        cascade="all, delete-orphan",
    )
    fills: Mapped[list["ExecutionFillRecord"]] = relationship(
        back_populates="broker_order",
        cascade="all, delete-orphan",
    )


class BrokerOrderEventRecord(Base):
    """Append-only broker order lifecycle events and rejects."""

    __tablename__ = "broker_order_event"
    __table_args__ = (
        Index("ix_broker_order_event_broker_order_id", "broker_order_id"),
        Index("ix_broker_order_event_event_at", "event_at"),
        Index("ix_broker_order_event_event_type", "event_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    broker_order_id: Mapped[int] = mapped_column(
        ForeignKey("broker_order.id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    event_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    status_before: Mapped[str | None] = mapped_column(String(32))
    status_after: Mapped[str | None] = mapped_column(String(32))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    note: Mapped[str | None] = mapped_column(Text)

    broker_order: Mapped[BrokerOrderRecord] = relationship(back_populates="events")


class ExecutionFillRecord(TimestampMixin, Base):
    """Durable fill ledger row independent of instruction summary state."""

    __tablename__ = "execution_fill"
    __table_args__ = (
        UniqueConstraint(
            "broker_kind",
            "account_key",
            "external_execution_id",
            name="uq_execution_fill_external_execution_id",
        ),
        Index("ix_execution_fill_broker_order_id", "broker_order_id"),
        Index("ix_execution_fill_instruction_id", "instruction_id"),
        Index("ix_execution_fill_executed_at", "executed_at"),
        Index("ix_execution_fill_symbol", "symbol"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    broker_order_id: Mapped[int | None] = mapped_column(
        ForeignKey("broker_order.id", ondelete="SET NULL")
    )
    instruction_id: Mapped[int | None] = mapped_column(
        ForeignKey("instruction.id", ondelete="SET NULL")
    )
    broker_account_id: Mapped[int] = mapped_column(
        ForeignKey("broker_account.id", ondelete="CASCADE"),
        nullable=False,
    )
    broker_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    account_key: Mapped[str] = mapped_column(String(64), nullable=False)
    external_execution_id: Mapped[str] = mapped_column(String(128), nullable=False)
    external_order_id: Mapped[str | None] = mapped_column(String(64))
    external_perm_id: Mapped[str | None] = mapped_column(String(64))
    order_ref: Mapped[str | None] = mapped_column(String(128))
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    exchange: Mapped[str | None] = mapped_column(String(32))
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    security_type: Mapped[str] = mapped_column(String(16), nullable=False)
    side: Mapped[str | None] = mapped_column(String(8))
    quantity: Mapped[str] = mapped_column(String(64), nullable=False)
    price: Mapped[str] = mapped_column(String(64), nullable=False)
    commission: Mapped[str | None] = mapped_column(String(64))
    commission_currency: Mapped[str | None] = mapped_column(String(8))
    executed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    broker_order: Mapped[BrokerOrderRecord | None] = relationship(back_populates="fills")
    instruction: Mapped[InstructionRecord | None] = relationship()
    broker_account: Mapped[BrokerAccountRecord] = relationship()


class AccountSnapshotRecord(TimestampMixin, Base):
    """Point-in-time broker account values used for operators and reconciliation."""

    __tablename__ = "account_snapshot"
    __table_args__ = (
        Index("ix_account_snapshot_broker_account_id", "broker_account_id"),
        Index("ix_account_snapshot_snapshot_at", "snapshot_at"),
        Index("ix_account_snapshot_source", "source"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    broker_account_id: Mapped[int] = mapped_column(
        ForeignKey("broker_account.id", ondelete="CASCADE"),
        nullable=False,
    )
    snapshot_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    net_liquidation: Mapped[str | None] = mapped_column(String(64))
    total_cash_value: Mapped[str | None] = mapped_column(String(64))
    buying_power: Mapped[str | None] = mapped_column(String(64))
    available_funds: Mapped[str | None] = mapped_column(String(64))
    excess_liquidity: Mapped[str | None] = mapped_column(String(64))
    cushion: Mapped[str | None] = mapped_column(String(64))
    currency: Mapped[str | None] = mapped_column(String(8))
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    broker_account: Mapped[BrokerAccountRecord] = relationship(
        back_populates="account_snapshots"
    )


class PositionSnapshotRecord(TimestampMixin, Base):
    """Point-in-time broker position snapshot used for restart reconciliation and UI."""

    __tablename__ = "position_snapshot"
    __table_args__ = (
        Index("ix_position_snapshot_broker_account_id", "broker_account_id"),
        Index("ix_position_snapshot_snapshot_at", "snapshot_at"),
        Index("ix_position_snapshot_symbol", "symbol"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    broker_account_id: Mapped[int] = mapped_column(
        ForeignKey("broker_account.id", ondelete="CASCADE"),
        nullable=False,
    )
    snapshot_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)
    exchange: Mapped[str] = mapped_column(String(32), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False)
    security_type: Mapped[str] = mapped_column(String(16), nullable=False)
    primary_exchange: Mapped[str | None] = mapped_column(String(32))
    local_symbol: Mapped[str | None] = mapped_column(String(64))
    quantity: Mapped[str] = mapped_column(String(64), nullable=False)
    average_cost: Mapped[str | None] = mapped_column(String(64))
    market_price: Mapped[str | None] = mapped_column(String(64))
    market_value: Mapped[str | None] = mapped_column(String(64))
    unrealized_pnl: Mapped[str | None] = mapped_column(String(64))
    realized_pnl: Mapped[str | None] = mapped_column(String(64))
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    broker_account: Mapped[BrokerAccountRecord] = relationship(
        back_populates="position_snapshots"
    )


class ReconciliationRunRecord(TimestampMixin, Base):
    """Durable record of each runtime or startup reconciliation pass."""

    __tablename__ = "reconciliation_run"
    __table_args__ = (
        Index("ix_reconciliation_run_started_at", "started_at"),
        Index("ix_reconciliation_run_run_kind", "run_kind"),
        Index("ix_reconciliation_run_status", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    broker_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    account_key: Mapped[str | None] = mapped_column(String(64))
    runtime_timezone: Mapped[str | None] = mapped_column(String(64))
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    issue_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    action_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    issues: Mapped[list["ReconciliationIssueRecord"]] = relationship(
        back_populates="reconciliation_run",
        cascade="all, delete-orphan",
    )


class ReconciliationIssueRecord(Base):
    """Append-only issue rows emitted while a reconciliation run is executing."""

    __tablename__ = "reconciliation_issue"
    __table_args__ = (
        Index("ix_reconciliation_issue_run_id", "reconciliation_run_id"),
        Index("ix_reconciliation_issue_instruction_id", "instruction_id"),
        Index("ix_reconciliation_issue_stage", "stage"),
        Index("ix_reconciliation_issue_observed_at", "observed_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    reconciliation_run_id: Mapped[int] = mapped_column(
        ForeignKey("reconciliation_run.id", ondelete="CASCADE"),
        nullable=False,
    )
    instruction_id: Mapped[str | None] = mapped_column(String(128))
    stage: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False, default="ERROR")
    message: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    reconciliation_run: Mapped[ReconciliationRunRecord] = relationship(
        back_populates="issues"
    )


class RuntimeServiceRecord(TimestampMixin, Base):
    """Durable lifecycle row for long-lived local services such as execution runtime."""

    __tablename__ = "runtime_service"
    __table_args__ = (
        UniqueConstraint("runtime_key", name="uq_runtime_service_runtime_key"),
        Index("ix_runtime_service_status", "status"),
        Index("ix_runtime_service_heartbeat_at", "heartbeat_at"),
        Index("ix_runtime_service_lease_expires_at", "lease_expires_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    runtime_key: Mapped[str] = mapped_column(String(64), nullable=False)
    service_type: Mapped[str] = mapped_column(String(32), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    owner_token: Mapped[str | None] = mapped_column(String(64))
    owner_label: Mapped[str | None] = mapped_column(String(256))
    hostname: Mapped[str | None] = mapped_column(String(256))
    pid: Mapped[int | None] = mapped_column(Integer)
    runtime_timezone: Mapped[str | None] = mapped_column(String(64))
    broker_kind: Mapped[str | None] = mapped_column(String(32))
    broker_client_id: Mapped[int | None] = mapped_column(Integer)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_cycle_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_cycle_completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_successful_cycle_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    stop_requested: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)

    events: Mapped[list["RuntimeServiceEventRecord"]] = relationship(
        back_populates="runtime_service",
        cascade="all, delete-orphan",
    )


class RuntimeServiceEventRecord(Base):
    """Append-only event history for runtime-service lifecycle changes."""

    __tablename__ = "runtime_service_event"
    __table_args__ = (
        Index("ix_runtime_service_event_runtime_service_id", "runtime_service_id"),
        Index("ix_runtime_service_event_event_at", "event_at"),
        Index("ix_runtime_service_event_event_type", "event_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    runtime_service_id: Mapped[int] = mapped_column(
        ForeignKey("runtime_service.id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    event_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        nullable=False,
    )
    status_before: Mapped[str | None] = mapped_column(String(32))
    status_after: Mapped[str | None] = mapped_column(String(32))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    note: Mapped[str | None] = mapped_column(Text)

    runtime_service: Mapped[RuntimeServiceRecord] = relationship(
        back_populates="events"
    )
