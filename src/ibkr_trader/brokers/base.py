from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum


class BrokerOrderStatus(StrEnum):
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


@dataclass(slots=True)
class BrokerOrderAck:
    broker_order_id: str
    status: BrokerOrderStatus


class BrokerAdapter(ABC):
    @abstractmethod
    def connect(self) -> None:
        """Connect to the broker session."""

    @abstractmethod
    def place_limit_order(
        self,
        *,
        symbol: str,
        quantity: Decimal,
        limit_price: Decimal,
        side: str,
    ) -> BrokerOrderAck:
        """Submit a limit order."""

    @abstractmethod
    def cancel_order(self, broker_order_id: str) -> None:
        """Cancel an order."""

