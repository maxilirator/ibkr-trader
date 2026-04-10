from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from ibkr_trader.brokers.base import BrokerAdapter, BrokerOrderAck
from ibkr_trader.config import IbkrConnectionConfig


@dataclass(slots=True)
class IbkrGatewayAdapter(BrokerAdapter):
    """Thin adapter boundary for a future IB Gateway / TWS API implementation."""

    config: IbkrConnectionConfig

    def connect(self) -> None:
        raise NotImplementedError(
            "Implement TWS API connectivity here using the official IBKR client."
        )

    def place_limit_order(
        self,
        *,
        symbol: str,
        quantity: Decimal,
        limit_price: Decimal,
        side: str,
    ) -> BrokerOrderAck:
        raise NotImplementedError(
            "Translate internal order intent into IBKR order placement here."
        )

    def cancel_order(self, broker_order_id: str) -> None:
        raise NotImplementedError("Translate internal cancel request to IBKR here.")

