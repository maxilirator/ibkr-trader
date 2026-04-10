from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Mapping


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class EntryOrderType(StrEnum):
    LIMIT = "LIMIT"
    MARKET = "MARKET"


@dataclass(slots=True)
class TimedEntry:
    symbol: str
    side: Side
    quantity: Decimal
    order_type: EntryOrderType
    activate_at: datetime
    limit_price: Decimal | None = None

    def validate(self) -> None:
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")
        if self.order_type is EntryOrderType.LIMIT and self.limit_price is None:
            raise ValueError("limit orders require limit_price")
        if self.order_type is EntryOrderType.MARKET and self.limit_price is not None:
            raise ValueError("market orders cannot define limit_price")


@dataclass(slots=True)
class ExitPolicy:
    take_profit_pct: Decimal | None = None
    stop_loss_pct: Decimal | None = None
    force_exit_next_session_open: bool = False

    def validate(self) -> None:
        if self.take_profit_pct is not None and self.take_profit_pct <= 0:
            raise ValueError("take_profit_pct must be positive")
        if self.stop_loss_pct is not None and self.stop_loss_pct <= 0:
            raise ValueError("stop_loss_pct must be positive")


@dataclass(slots=True)
class TradeInstruction:
    instruction_id: str
    created_at: datetime
    entry: TimedEntry
    exit_policy: ExitPolicy
    metadata: Mapping[str, str] = field(default_factory=dict)

    def validate(self) -> None:
        self.entry.validate()
        self.exit_policy.validate()

