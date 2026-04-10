from __future__ import annotations

from datetime import datetime
from typing import Any

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
