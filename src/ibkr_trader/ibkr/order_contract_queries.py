from __future__ import annotations

import re

from ibkr_trader.domain.contract_resolution import ContractResolveQuery
from ibkr_trader.domain.execution_contract import ExecutionInstruction


_STOCKHOLM_PRIMARY_EXCHANGES = {"SFB", "XSTO"}
_STOCKHOLM_SHARE_CLASS_RE = re.compile(r"^(.+?)[ .-]([A-Z]{1,2})$")


def _is_stockholm_stock_order(instruction: ExecutionInstruction) -> bool:
    instrument = instruction.instrument
    security_type = instrument.security_type.value.upper()
    currency = instrument.currency.upper()
    exchange = instrument.exchange.upper()
    primary_exchange = (instrument.primary_exchange or "").upper()
    return (
        security_type == "STK"
        and currency == "SEK"
        and (
            exchange in {"SMART", *_STOCKHOLM_PRIMARY_EXCHANGES}
            or primary_exchange in _STOCKHOLM_PRIMARY_EXCHANGES
        )
    )


def _stockholm_share_class_identity(symbol: str) -> tuple[str, str] | None:
    normalized_symbol = symbol.strip().upper()
    if not normalized_symbol:
        return None
    match = _STOCKHOLM_SHARE_CLASS_RE.match(normalized_symbol)
    if match is None:
        return None

    root = match.group(1).strip(" .-")
    share_class = match.group(2).strip()
    if not root or not share_class.isalpha():
        return None

    ibkr_symbol = f"{root}.{share_class}"
    local_symbol = f"{root} {share_class}"
    return ibkr_symbol, local_symbol


def build_order_contract_query(instruction: ExecutionInstruction) -> ContractResolveQuery:
    instrument = instruction.instrument
    symbol = instrument.symbol.strip().upper()
    local_symbol: str | None = None

    if _is_stockholm_stock_order(instruction):
        stockholm_identity = _stockholm_share_class_identity(symbol)
        if stockholm_identity is not None:
            symbol, local_symbol = stockholm_identity

    return ContractResolveQuery(
        symbol=symbol,
        security_type=instrument.security_type.value,
        exchange=instrument.exchange,
        currency=instrument.currency,
        primary_exchange=instrument.primary_exchange,
        local_symbol=local_symbol,
        isin=instrument.isin,
    )
