from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Mapping


@dataclass(slots=True)
class ContractResolveQuery:
    symbol: str
    security_type: str
    exchange: str
    currency: str
    primary_exchange: str | None = None
    local_symbol: str | None = None
    include_expired: bool = False
    isin: str | None = None

    def validate(self) -> None:
        if not self.symbol:
            raise ValueError("symbol is required")
        if not self.security_type:
            raise ValueError("security_type is required")
        if not self.exchange:
            raise ValueError("exchange is required")
        if not self.currency:
            raise ValueError("currency is required")


@dataclass(slots=True)
class ResolvedContract:
    con_id: int
    symbol: str
    local_symbol: str
    security_type: str
    exchange: str
    primary_exchange: str
    currency: str
    trading_class: str
    market_name: str | None = None
    long_name: str | None = None
    min_tick: Decimal | None = None
    valid_exchanges: tuple[str, ...] = ()
    order_types: tuple[str, ...] = ()
    time_zone_id: str | None = None
    trading_hours: str | None = None
    liquid_hours: str | None = None
    stock_type: str | None = None
    industry: str | None = None
    category: str | None = None
    subcategory: str | None = None
    sec_ids: Mapping[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class ContractResolveResult:
    query: ContractResolveQuery
    matches: tuple[ResolvedContract, ...]

    @property
    def match_count(self) -> int:
        return len(self.matches)

    @property
    def is_unique(self) -> bool:
        return self.match_count == 1
