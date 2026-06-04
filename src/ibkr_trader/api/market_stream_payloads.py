from __future__ import annotations

from decimal import Decimal
from typing import Any
from typing import Mapping

from sqlalchemy import select

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import BrokerAccountRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import PositionSnapshotRecord
from ibkr_trader.ibkr.market_stream import MarketStreamContract
from ibkr_trader.virtual.accounts import BROKER_KIND_VIRTUAL


BACKGROUND_RECOVERY_CLOSED_ORDER_STATUSES = {
    "API_CANCELLED",
    "CANCELLED",
    "ERROR",
    "FILLED",
    "INACTIVE",
    "NOT_FOUND_AT_BROKER",
    "REJECTED",
}
ENTRY_STREAM_STATES = {"ENTRY_PENDING", "ENTRY_SUBMITTED"}
MODEL_ROUTED_STREAM_STATES = {"MODEL_ROUTED_PENDING"}
INTENT_STREAM_STATES = ENTRY_STREAM_STATES | MODEL_ROUTED_STREAM_STATES
OPERATOR_BENCHMARK_STREAM_CONTRACTS = (
    MarketStreamContract(
        symbol="OMXS30",
        security_type="IND",
        exchange="OMS",
        primary_exchange="",
        currency="SEK",
    ),
)


def parse_market_stream_subscribe_payload(
    payload: Mapping[str, Any],
    *,
    stockholm_identity_map: Mapping[str, Any] | None = None,
    max_contracts: int = 120,
) -> dict[str, Any]:
    """Parse a desired market-stream subscription payload into contracts."""

    raw_contracts = payload.get("contracts") or payload.get("instruments")
    raw_symbols = payload.get("symbols")
    contracts: list[MarketStreamContract] = []

    if raw_contracts is not None:
        if not isinstance(raw_contracts, list) or not raw_contracts:
            raise ValueError("contracts must be a non-empty array")
        for item in raw_contracts:
            if not isinstance(item, Mapping):
                raise ValueError("contracts entries must be objects")
            local_symbol = (
                str(item["local_symbol"]).strip()
                if item.get("local_symbol") is not None
                else None
            )
            isin = str(item["isin"]).strip() if item.get("isin") is not None else None
            contract = _market_stream_contract_for_symbol(
                symbol=str(item.get("symbol", "")).strip().upper(),
                security_type=str(item.get("security_type", "STK")).strip().upper(),
                exchange=str(item.get("exchange", payload.get("exchange", "SMART"))).strip().upper(),
                currency=str(item.get("currency", payload.get("currency", "SEK"))).strip().upper(),
                primary_exchange=(
                    str(item["primary_exchange"]).strip().upper()
                    if item.get("primary_exchange") is not None
                    else (
                        str(payload["primary_exchange"]).strip().upper()
                        if payload.get("primary_exchange") is not None
                        else "SFB"
                    )
                ),
                local_symbol=local_symbol,
                isin=isin,
                stockholm_identity_map=stockholm_identity_map,
            )
            contract.validate()
            contracts.append(contract)
    else:
        if not isinstance(raw_symbols, list) or not raw_symbols:
            raise ValueError("symbols must be a non-empty array of strings")
        symbols = tuple(str(symbol).strip().upper() for symbol in raw_symbols)
        if not all(symbols):
            raise ValueError("symbols must contain only non-empty strings")
        if len(set(symbols)) != len(symbols):
            raise ValueError("symbols must not contain duplicates")
        contracts = [
            _market_stream_contract_for_symbol(
                symbol=symbol,
                security_type=str(payload.get("security_type", "STK")).strip().upper(),
                exchange=str(payload.get("exchange", "SMART")).strip().upper(),
                currency=str(payload.get("currency", "SEK")).strip().upper(),
                primary_exchange=(
                    str(payload["primary_exchange"]).strip().upper()
                    if payload.get("primary_exchange") is not None
                    else "SFB"
                ),
                local_symbol=None,
                isin=None,
                stockholm_identity_map=stockholm_identity_map,
            )
            for symbol in symbols
        ]

    if len(contracts) > max_contracts:
        raise ValueError(
            f"market stream subscriptions are limited to {max_contracts} symbols"
        )

    market_data_type = (
        str(payload["market_data_type"]).strip().upper()
        if payload.get("market_data_type") is not None
        else None
    )
    return {
        "contracts": contracts,
        "replace": bool(payload.get("replace", True)),
        "market_data_type": market_data_type,
    }


def parse_market_stream_symbols(raw_value: str | None) -> list[str] | None:
    """Parse comma/newline separated symbols from query params."""

    if raw_value is None or not raw_value.strip():
        return None
    symbols = [
        item.strip().upper()
        for item in raw_value.replace("\n", ",").split(",")
        if item.strip()
    ]
    return sorted(set(symbols)) or None


def market_stream_contracts_for_open_orders(
    open_orders: Mapping[Any, Any],
) -> list[MarketStreamContract]:
    """Build additive market-data subscriptions for currently open broker orders."""

    contracts_by_key: dict[str, MarketStreamContract] = {}
    for open_order in open_orders.values():
        status = str(getattr(open_order, "status", "") or "").strip().upper()
        completed_status = (
            str(getattr(open_order, "completed_status", "") or "").strip().upper()
        )
        if (
            status in BACKGROUND_RECOVERY_CLOSED_ORDER_STATUSES
            or completed_status in BACKGROUND_RECOVERY_CLOSED_ORDER_STATUSES
        ):
            continue

        symbol = str(
            getattr(open_order, "symbol", None)
            or getattr(open_order, "local_symbol", None)
            or ""
        ).strip().upper()
        if not symbol:
            continue

        security_type = str(
            getattr(open_order, "security_type", None) or "STK"
        ).strip().upper()
        raw_exchange = str(getattr(open_order, "exchange", None) or "").strip().upper()
        exchange = raw_exchange or "SMART"
        raw_primary_exchange = getattr(open_order, "primary_exchange", None)
        primary_exchange = (
            str(raw_primary_exchange).strip().upper()
            if raw_primary_exchange not in (None, "")
            else None
        )
        if security_type == "STK":
            exchange = "SMART"
            if not primary_exchange or primary_exchange == "SMART":
                primary_exchange = "SFB"
        currency = str(getattr(open_order, "currency", None) or "SEK").strip().upper()
        local_symbol = getattr(open_order, "local_symbol", None)
        local_symbol = (
            str(local_symbol).strip() if local_symbol not in (None, "") else None
        )

        contract = MarketStreamContract(
            symbol=symbol,
            exchange=exchange,
            currency=currency,
            security_type=security_type,
            primary_exchange=primary_exchange,
            local_symbol=local_symbol,
        )
        contracts_by_key[contract.key] = contract

    return sorted(contracts_by_key.values(), key=lambda contract: contract.symbol)


def market_stream_contracts_for_runtime_holdings(
    snapshot: Any,
) -> list[MarketStreamContract]:
    """Build additive subscriptions for live holdings seen in broker snapshots."""

    contracts_by_key: dict[str, MarketStreamContract] = {}
    for source_name in ("portfolio", "positions"):
        for holding in getattr(snapshot, source_name, ()) or ():
            quantity = getattr(holding, "position", None)
            if quantity is None:
                continue
            try:
                if Decimal(str(quantity)) == 0:
                    continue
            except Exception:
                continue
            contract = _market_stream_contract_from_instrument_fields(
                symbol=(
                    str(getattr(holding, "symbol", None) or "")
                    or str(getattr(holding, "local_symbol", None) or "")
                ),
                security_type=getattr(holding, "security_type", None),
                exchange=getattr(holding, "exchange", None),
                currency=getattr(holding, "currency", None),
                primary_exchange=getattr(holding, "primary_exchange", None),
                local_symbol=getattr(holding, "local_symbol", None),
            )
            if contract is not None:
                contracts_by_key[contract.key] = contract
    return sorted(contracts_by_key.values(), key=lambda contract: contract.symbol)


def market_stream_contracts_for_current_holdings(
    session_factory: Any,
    *,
    virtual_only: bool = False,
) -> list[MarketStreamContract]:
    """Build additive subscriptions for latest persisted non-zero holdings."""

    contracts_by_key: dict[str, MarketStreamContract] = {}
    with session_scope(session_factory) as session:
        statement = (
            select(PositionSnapshotRecord, BrokerAccountRecord)
            .join(
                BrokerAccountRecord,
                BrokerAccountRecord.id == PositionSnapshotRecord.broker_account_id,
            )
            .order_by(
                BrokerAccountRecord.account_key.asc(),
                PositionSnapshotRecord.symbol.asc(),
                PositionSnapshotRecord.currency.asc(),
                PositionSnapshotRecord.security_type.asc(),
                PositionSnapshotRecord.snapshot_at.desc(),
                PositionSnapshotRecord.id.desc(),
            )
        )
        if virtual_only:
            statement = statement.where(
                PositionSnapshotRecord.is_virtual.is_(True),
                BrokerAccountRecord.broker_kind == BROKER_KIND_VIRTUAL,
            )
        rows = session.execute(statement).all()

        seen_keys: set[tuple[int, str, str, str, str | None]] = set()
        for position, broker_account in rows:
            identity = (
                broker_account.id,
                position.symbol,
                position.currency,
                position.security_type,
                position.local_symbol,
            )
            if identity in seen_keys:
                continue
            seen_keys.add(identity)
            try:
                quantity = Decimal(str(position.quantity))
            except Exception:
                continue
            if quantity == 0:
                continue

            contract = _market_stream_contract_from_instrument_fields(
                symbol=position.symbol,
                security_type=position.security_type,
                exchange=position.exchange,
                currency=position.currency,
                primary_exchange=position.primary_exchange,
                local_symbol=position.local_symbol,
            )
            if contract is not None:
                contracts_by_key[contract.key] = contract

    return sorted(contracts_by_key.values(), key=lambda contract: contract.symbol)


def market_stream_contracts_for_open_virtual_positions(
    session_factory: Any,
) -> list[MarketStreamContract]:
    """Build additive subscriptions for virtual holdings that need mark-to-market."""

    return market_stream_contracts_for_current_holdings(
        session_factory,
        virtual_only=True,
    )


def market_stream_contracts_for_pending_entries(
    session_factory: Any,
) -> list[MarketStreamContract]:
    """Build stream subscriptions for active instructions before broker orders exist."""

    contracts_by_key: dict[str, MarketStreamContract] = {}
    now = utc_now()
    with session_scope(session_factory) as session:
        records = list(
            session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.state.in_(INTENT_STREAM_STATES),
                    InstructionRecord.archived_at.is_(None),
                    InstructionRecord.expire_at >= now,
                )
            ).scalars()
        )

    for record in records:
        contract = _market_stream_contract_from_instrument_fields(
            symbol=record.symbol,
            security_type="STK",
            exchange=record.exchange,
            currency=record.currency,
            primary_exchange=None,
            local_symbol=None,
        )
        if contract is not None:
            contracts_by_key[contract.key] = contract

    return sorted(contracts_by_key.values(), key=lambda contract: contract.symbol)


def market_stream_contracts_for_operator_benchmarks() -> list[MarketStreamContract]:
    """Build subscriptions for dashboard/operator benchmark context."""

    return list(OPERATOR_BENCHMARK_STREAM_CONTRACTS)


def subscribe_open_order_market_streams(
    market_stream_service: Any,
    snapshot: Any,
    session_factory: Any | None = None,
    *,
    include_operator_benchmarks: bool = True,
) -> list[str]:
    """Synchronize live streams needed by current runtime targets."""

    contracts = market_stream_contracts_for_open_orders(
        getattr(snapshot, "open_orders", {}) or {}
    )
    contracts_by_key = {contract.key: contract for contract in contracts}
    if include_operator_benchmarks:
        for contract in market_stream_contracts_for_operator_benchmarks():
            contracts_by_key[contract.key] = contract
    for contract in market_stream_contracts_for_runtime_holdings(snapshot):
        contracts_by_key[contract.key] = contract
    if session_factory is not None:
        for contract in market_stream_contracts_for_current_holdings(
            session_factory,
        ):
            contracts_by_key[contract.key] = contract
        for contract in market_stream_contracts_for_pending_entries(
            session_factory,
        ):
            contracts_by_key[contract.key] = contract
    contracts = sorted(
        contracts_by_key.values(),
        key=lambda contract: contract.symbol,
    )
    market_data_type = "LIVE" if contracts else None
    market_stream_service.subscribe_many(
        contracts,
        replace=True,
        market_data_type=market_data_type,
    )
    return [contract.symbol for contract in contracts]


def _identity_lookup_key(symbol: str) -> str:
    return symbol.strip().upper()


def _identity_lookup_candidates(symbol: str) -> tuple[str, ...]:
    normalized = _identity_lookup_key(symbol)
    candidates = [normalized]
    if " " in normalized:
        candidates.append(normalized.replace(" ", "-"))
    if "-" in normalized:
        candidates.append(normalized.replace("-", " "))
    return tuple(dict.fromkeys(candidates))


def _identity_value(identity: Any, field_name: str) -> str | None:
    if identity is None:
        return None
    raw_value = getattr(identity, field_name, None)
    if raw_value is None:
        return None
    value = str(raw_value).strip()
    return value or None


def _market_stream_contract_for_symbol(
    *,
    symbol: str,
    security_type: str,
    exchange: str,
    currency: str,
    primary_exchange: str | None,
    local_symbol: str | None,
    isin: str | None,
    stockholm_identity_map: Mapping[str, Any] | None,
) -> MarketStreamContract:
    normalized_symbol = symbol.strip().upper()
    identity = None
    identity_map = stockholm_identity_map or {}
    for candidate in _identity_lookup_candidates(normalized_symbol):
        identity = identity_map.get(candidate)
        if identity is not None:
            break
    if identity is None:
        for candidate_identity in identity_map.values():
            ticker_alias = _identity_value(candidate_identity, "ticker_alias")
            if ticker_alias is not None and ticker_alias.upper() == normalized_symbol:
                identity = candidate_identity
                break
    enriched_local_symbol = local_symbol or _identity_value(identity, "ticker_alias")
    enriched_isin = isin or _identity_value(identity, "isin")
    return MarketStreamContract(
        symbol=normalized_symbol,
        security_type=security_type,
        exchange=exchange,
        currency=currency,
        primary_exchange=primary_exchange,
        local_symbol=enriched_local_symbol,
        isin=enriched_isin,
    )


def _market_stream_contract_from_instrument_fields(
    *,
    symbol: str,
    security_type: str | None,
    exchange: str | None,
    currency: str | None,
    primary_exchange: str | None,
    local_symbol: str | None,
) -> MarketStreamContract | None:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        return None
    normalized_security_type = str(security_type or "STK").strip().upper()
    normalized_exchange = str(exchange or "SMART").strip().upper() or "SMART"
    normalized_primary_exchange = (
        str(primary_exchange).strip().upper()
        if primary_exchange not in (None, "")
        else None
    )
    if normalized_security_type == "STK":
        normalized_exchange = "SMART"
        if not normalized_primary_exchange or normalized_primary_exchange == "SMART":
            normalized_primary_exchange = "SFB"
    return MarketStreamContract(
        symbol=normalized_symbol,
        exchange=normalized_exchange,
        currency=str(currency or "SEK").strip().upper(),
        security_type=normalized_security_type,
        primary_exchange=normalized_primary_exchange,
        local_symbol=(
            str(local_symbol).strip()
            if local_symbol not in (None, "")
            else None
        ),
    )
