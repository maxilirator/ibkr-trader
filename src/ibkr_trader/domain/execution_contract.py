from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Mapping


class SecurityType(StrEnum):
    STOCK = "STK"


class PositionSide(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class SizingMode(StrEnum):
    FRACTION_OF_ACCOUNT_NAV = "fraction_of_account_nav"
    TARGET_NOTIONAL = "target_notional"
    TARGET_QUANTITY = "target_quantity"


class OrderType(StrEnum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"


class TimeInForce(StrEnum):
    DAY = "DAY"
    GTC = "GTC"


@dataclass(slots=True)
class SourceContext:
    system: str
    batch_id: str
    generated_at: datetime
    release_id: str | None = None
    strategy_id: str | None = None
    policy_id: str | None = None

    def validate(self) -> None:
        if self.generated_at.tzinfo is None:
            raise ValueError("source.generated_at must include timezone information")


@dataclass(slots=True)
class AccountRef:
    account_key: str
    book_key: str
    book_role: str | None = None
    book_side: PositionSide | None = None

    def validate(self) -> None:
        if not self.account_key:
            raise ValueError("account.account_key is required")
        if not self.book_key:
            raise ValueError("account.book_key is required")


@dataclass(slots=True)
class InstrumentRef:
    symbol: str
    exchange: str
    currency: str
    security_type: SecurityType = SecurityType.STOCK
    isin: str | None = None
    primary_exchange: str | None = None
    aliases: tuple[str, ...] = ()

    def validate(self) -> None:
        if not self.symbol:
            raise ValueError("instrument.symbol is required")
        if not self.exchange:
            raise ValueError("instrument.exchange is required")
        if not self.currency:
            raise ValueError("instrument.currency is required")


@dataclass(slots=True)
class IntentSpec:
    side: str
    position_side: PositionSide

    def validate(self) -> None:
        if self.side not in {"BUY", "SELL"}:
            raise ValueError("intent.side must be BUY or SELL")


@dataclass(slots=True)
class SizingSpec:
    mode: SizingMode
    target_fraction_of_account: Decimal | None = None
    target_notional: Decimal | None = None
    target_quantity: Decimal | None = None

    def validate(self) -> None:
        populated_targets = [
            self.target_fraction_of_account is not None,
            self.target_notional is not None,
            self.target_quantity is not None,
        ]
        if sum(populated_targets) != 1:
            raise ValueError(
                "sizing must define exactly one of "
                "target_fraction_of_account, target_notional, or target_quantity"
            )

        if self.mode is SizingMode.FRACTION_OF_ACCOUNT_NAV:
            if self.target_fraction_of_account is None:
                raise ValueError(
                    "sizing.target_fraction_of_account is required for "
                    "fraction_of_account_nav mode"
                )
            if self.target_fraction_of_account <= 0:
                raise ValueError("sizing.target_fraction_of_account must be positive")

        if self.mode is SizingMode.TARGET_NOTIONAL:
            if self.target_notional is None:
                raise ValueError("sizing.target_notional is required for target_notional mode")
            if self.target_notional <= 0:
                raise ValueError("sizing.target_notional must be positive")

        if self.mode is SizingMode.TARGET_QUANTITY:
            if self.target_quantity is None:
                raise ValueError("sizing.target_quantity is required for target_quantity mode")
            if self.target_quantity <= 0:
                raise ValueError("sizing.target_quantity must be positive")


@dataclass(slots=True)
class EntrySpec:
    order_type: OrderType
    submit_at: datetime
    expire_at: datetime
    limit_price: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.DAY
    max_submit_count: int = 1
    cancel_unfilled_at_expiry: bool = True

    def validate(self) -> None:
        if self.submit_at.tzinfo is None:
            raise ValueError("entry.submit_at must include timezone information")
        if self.expire_at.tzinfo is None:
            raise ValueError("entry.expire_at must include timezone information")
        if self.expire_at <= self.submit_at:
            raise ValueError("entry.expire_at must be after entry.submit_at")
        if self.max_submit_count <= 0:
            raise ValueError("entry.max_submit_count must be positive")
        if self.order_type is OrderType.LIMIT and self.limit_price is None:
            raise ValueError("entry.limit_price is required for LIMIT orders")
        if self.order_type is OrderType.MARKET and self.limit_price is not None:
            raise ValueError("entry.limit_price must be omitted for MARKET orders")
        if self.limit_price is not None and self.limit_price <= 0:
            raise ValueError("entry.limit_price must be positive")


@dataclass(slots=True)
class ExitSpec:
    take_profit_pct: Decimal | None = None
    stop_loss_pct: Decimal | None = None
    catastrophic_stop_loss_pct: Decimal | None = None
    force_exit_next_session_open: bool = False

    def validate(self) -> None:
        for field_name, value in (
            ("take_profit_pct", self.take_profit_pct),
            ("stop_loss_pct", self.stop_loss_pct),
            ("catastrophic_stop_loss_pct", self.catastrophic_stop_loss_pct),
        ):
            if value is not None and value <= 0:
                raise ValueError(f"exit.{field_name} must be positive")


@dataclass(slots=True)
class TraceSpec:
    reason_code: str
    execution_policy: str | None = None
    trade_date: date | None = None
    data_cutoff_date: date | None = None
    company_name: str | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class ExecutionInstruction:
    instruction_id: str
    account: AccountRef
    instrument: InstrumentRef
    intent: IntentSpec
    sizing: SizingSpec
    entry: EntrySpec
    exit: ExitSpec
    trace: TraceSpec

    def validate(self) -> None:
        if not self.instruction_id:
            raise ValueError("instruction_id is required")
        self.account.validate()
        self.instrument.validate()
        self.intent.validate()
        self.sizing.validate()
        self.entry.validate()
        self.exit.validate()


@dataclass(slots=True)
class ExecutionInstructionBatch:
    schema_version: str
    source: SourceContext
    instructions: tuple[ExecutionInstruction, ...]

    def validate(self) -> None:
        if not self.schema_version:
            raise ValueError("schema_version is required")
        self.source.validate()
        if not self.instructions:
            raise ValueError("instructions must not be empty")
        for instruction in self.instructions:
            instruction.validate()
