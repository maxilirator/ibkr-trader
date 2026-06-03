from __future__ import annotations

from ibkr_trader.virtual.execution_core import *
from ibkr_trader.virtual.execution_orders import *
from ibkr_trader.virtual.execution_positions import *
from ibkr_trader.virtual.execution_quotes import *

def _quote_matches_order(
    quote: VirtualMarketQuoteRecord,
    broker_order: BrokerOrderRecord,
) -> bool:
    if quote.account_key != broker_order.account_key:
        return False
    if quote.symbol != broker_order.symbol:
        return False
    if quote.currency != broker_order.currency:
        return False
    if quote.security_type != broker_order.security_type:
        return False
    quote_scope = _quote_owner_scope(quote)
    if quote_scope is not None:
        owner = _virtual_position_owner_from_order(broker_order)
        if not _owner_scope_matches_quote_scope(
            {
                "deployment_key": owner["owner_deployment_key"],
                "source_instruction_id": owner["owner_source_instruction_id"],
            },
            quote_scope,
        ):
            return False
    return True


def _quote_matches_position_owner(
    quote: VirtualMarketQuoteRecord,
    position: PositionSnapshotRecord,
) -> bool:
    quote_scope = _quote_owner_scope(quote)
    if quote_scope is None:
        return True
    return _owner_scope_matches_quote_scope(
        {
            "deployment_key": position.owner_deployment_key,
            "source_instruction_id": position.owner_source_instruction_id,
        },
        quote_scope,
    )


def _build_fill_from_quote_for_order(
    *,
    quote: VirtualMarketQuoteRecord,
    broker_order: BrokerOrderRecord,
) -> dict[str, Any] | None:
    price, price_source = _virtual_condition_price_for_order(
        quote,
        action=broker_order.side,
        order_type=broker_order.order_type,
    )
    limit_price = _to_decimal(broker_order.limit_price)
    stop_price = _to_decimal(broker_order.stop_price)
    price_met, condition_code = _virtual_price_condition(
        action=broker_order.side,
        order_type=broker_order.order_type,
        market_price=price,
        limit_price=limit_price,
        stop_price=stop_price,
    )
    if not price_met or price is None:
        return None
    execution_price = _virtual_execution_price_for_order(
        quote=quote,
        action=broker_order.side,
        order_type=broker_order.order_type,
        condition_price=price,
        condition_source=price_source,
        limit_price=limit_price,
        stop_price=stop_price,
    )
    if execution_price is None:
        return None
    if price_source not in (None, "QUOTE"):
        condition_code = f"{condition_code}:{price_source}"
    order_id = int(str(broker_order.external_order_id))
    perm_id = (
        int(str(broker_order.external_perm_id))
        if broker_order.external_perm_id not in (None, "")
        else _new_virtual_perm_id(order_id)
    )
    return _build_virtual_fill_payload(
        order_id=order_id,
        perm_id=perm_id,
        order_ref=broker_order.order_ref or str(order_id),
        action=broker_order.side,
        quantity=_to_decimal(broker_order.total_quantity) or Decimal("1"),
        price=execution_price,
        quote=quote,
        condition_code=condition_code,
    )


def process_virtual_quote_fills(
    session: Session,
    *,
    quote: VirtualMarketQuoteRecord,
) -> list[dict[str, Any]]:
    broker_orders = session.execute(
        select(BrokerOrderRecord)
        .where(
            BrokerOrderRecord.broker_kind == BROKER_KIND_VIRTUAL,
            BrokerOrderRecord.is_virtual.is_(True),
            BrokerOrderRecord.account_key == quote.account_key,
            BrokerOrderRecord.symbol == quote.symbol,
            BrokerOrderRecord.currency == quote.currency,
            BrokerOrderRecord.security_type == quote.security_type,
            _open_virtual_order_status_clause(),
        )
        .order_by(BrokerOrderRecord.submitted_at.asc(), BrokerOrderRecord.id.asc())
    ).scalars().all()

    filled_orders: list[dict[str, Any]] = []
    for broker_order in broker_orders:
        if _is_closed_status(broker_order.status) or not _quote_matches_order(
            quote,
            broker_order,
        ):
            continue
        fill_payload = _build_fill_from_quote_for_order(
            quote=quote,
            broker_order=broker_order,
        )
        if fill_payload is None:
            continue
        fill = persist_virtual_execution_fill(
            session,
            broker_order=broker_order,
            instruction_record=broker_order.instruction,
            fill_payload=fill_payload,
            observed_at=quote.observed_at,
        )
        if fill is None:
            continue
        filled_orders.append(
            _serialize_for_json(
                {
                    "broker_order_id": broker_order.id,
                    "external_order_id": broker_order.external_order_id,
                    "order_ref": broker_order.order_ref,
                    "symbol": broker_order.symbol,
                    "side": broker_order.side,
                    "order_type": broker_order.order_type,
                    "status": broker_order.status,
                    "fill_id": fill.id,
                    "execution_id": fill.external_execution_id,
                    "price": fill.price,
                    "commission": fill.commission,
                    "commission_currency": fill.commission_currency,
                }
            )
        )
    return filled_orders


def record_virtual_market_quote(
    session_factory: sessionmaker[Session],
    *,
    account_key: str,
    symbol: str,
    exchange: str,
    currency: str,
    security_type: str = "STK",
    observed_at: datetime | None = None,
    primary_exchange: str | None = None,
    local_symbol: str | None = None,
    bid_price: Decimal | None = None,
    ask_price: Decimal | None = None,
    last_price: Decimal | None = None,
    midpoint_price: Decimal | None = None,
    source: str | None = None,
    raw_payload: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_account_key = normalize_virtual_account_key(account_key)
    quote_observed_at = observed_at or utc_now()
    with session_scope(session_factory) as session:
        broker_account = ensure_virtual_account(
            session,
            account_key=normalized_account_key,
            base_currency="SEK",
        )
        quote = VirtualMarketQuoteRecord(
            account_key=normalized_account_key,
            observed_at=quote_observed_at,
            symbol=symbol.strip().upper(),
            exchange=exchange.strip().upper(),
            currency=currency.strip().upper(),
            security_type=security_type.strip().upper(),
            primary_exchange=primary_exchange.strip().upper()
            if primary_exchange
            else None,
            local_symbol=local_symbol.strip() if local_symbol else None,
            bid_price=_decimal_to_string(bid_price),
            ask_price=_decimal_to_string(ask_price),
            last_price=_decimal_to_string(last_price),
            midpoint_price=_decimal_to_string(midpoint_price),
            source=source.strip() if source else None,
            raw_payload=_serialize_for_json(raw_payload or {}),
            metadata_json=_serialize_for_json(metadata or {}),
        )
        session.add(quote)
        session.flush()
        _persist_virtual_position_mark_snapshots(
            session,
            broker_account=broker_account,
            quote=quote,
        )
        filled_orders = process_virtual_quote_fills(session, quote=quote)
        if not filled_orders:
            _persist_virtual_account_snapshot(
                session,
                broker_account=broker_account,
                snapshot_at=quote_observed_at,
            )
        return {
            "quote": serialize_virtual_quote(quote),
            "filled_order_count": len(filled_orders),
            "filled_orders": filled_orders,
        }


def _normalize_stream_symbol(value: Any) -> str | None:
    normalized = _normalize_text(value)
    if normalized is None:
        return None
    return normalized.upper()


def _stream_payload(stream_snapshot: dict[str, Any]) -> dict[str, Any]:
    nested = stream_snapshot.get("stream")
    if isinstance(nested, dict):
        return nested
    return stream_snapshot


def _latest_stream_bar(
    stream_payload: dict[str, Any],
    *,
    symbol: str,
) -> dict[str, Any] | None:
    bars_by_symbol = stream_payload.get("bars_by_symbol")
    if not isinstance(bars_by_symbol, dict):
        return None
    bars = bars_by_symbol.get(symbol.upper())
    if not bars and "-" in symbol:
        bars = bars_by_symbol.get(symbol.replace("-", " ").upper())
    if not isinstance(bars, list) or not bars:
        return None
    latest = bars[-1]
    return latest if isinstance(latest, dict) else None


def _quote_observed_at_from_stream(
    *,
    quote_payload: dict[str, Any] | None,
    bar_payload: dict[str, Any] | None,
    fallback: datetime,
) -> datetime:
    if quote_payload is not None:
        observed_at = _parse_optional_datetime_value(
            quote_payload.get("last_trade_at") or quote_payload.get("updated_at")
        )
        if observed_at is not None:
            return observed_at
    if bar_payload is not None:
        observed_at = _parse_optional_datetime_value(bar_payload.get("timestamp"))
        if observed_at is not None:
            return observed_at
    return fallback


def record_virtual_market_quotes_from_stream_snapshot(
    session_factory: sessionmaker[Session],
    *,
    stream_snapshot: dict[str, Any],
    observed_at: datetime | None = None,
    account_key: str | None = None,
) -> dict[str, Any]:
    """Mirror live stream prices into the virtual quote tape for open virtual orders."""

    stream = _stream_payload(stream_snapshot)
    quote_payloads = stream.get("quotes")
    quotes_by_symbol: dict[str, dict[str, Any]] = {}
    if isinstance(quote_payloads, list):
        for quote_payload in quote_payloads:
            if not isinstance(quote_payload, dict):
                continue
            symbol = _normalize_stream_symbol(quote_payload.get("symbol"))
            if symbol is not None:
                quotes_by_symbol[symbol] = quote_payload

    fallback_observed_at = observed_at or utc_now()
    normalized_account_filter = (
        normalize_virtual_account_key(account_key) if account_key is not None else None
    )

    with session_scope(session_factory) as session:
        statement = (
            select(BrokerOrderRecord)
            .where(
                BrokerOrderRecord.broker_kind == BROKER_KIND_VIRTUAL,
                BrokerOrderRecord.is_virtual.is_(True),
                _open_virtual_order_status_clause(),
            )
            .order_by(BrokerOrderRecord.account_key.asc(), BrokerOrderRecord.symbol.asc())
        )
        if normalized_account_filter is not None:
            statement = statement.where(
                BrokerOrderRecord.account_key == normalized_account_filter
            )
        broker_orders = session.execute(statement).scalars().all()

        target_by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for broker_order in broker_orders:
            key = (
                broker_order.account_key,
                broker_order.symbol,
                broker_order.currency,
                broker_order.security_type,
            )
            target_by_key[key] = {
                "account_key": broker_order.account_key,
                "symbol": broker_order.symbol,
                "exchange": broker_order.exchange,
                "currency": broker_order.currency,
                "security_type": broker_order.security_type,
                "primary_exchange": broker_order.primary_exchange,
                "local_symbol": broker_order.local_symbol,
            }

        position_statement = (
            select(PositionSnapshotRecord, BrokerAccountRecord)
            .join(
                BrokerAccountRecord,
                BrokerAccountRecord.id == PositionSnapshotRecord.broker_account_id,
            )
            .where(
                PositionSnapshotRecord.is_virtual.is_(True),
                BrokerAccountRecord.broker_kind == BROKER_KIND_VIRTUAL,
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
        if normalized_account_filter is not None:
            position_statement = position_statement.where(
                BrokerAccountRecord.account_key == normalized_account_filter
            )
        position_rows = session.execute(position_statement).all()

        seen_position_keys: set[tuple[str, str, str, str]] = set()
        for position_snapshot, broker_account in position_rows:
            key = (
                broker_account.account_key,
                position_snapshot.symbol,
                position_snapshot.currency,
                position_snapshot.security_type,
            )
            if key in seen_position_keys:
                continue
            seen_position_keys.add(key)
            quantity = _to_decimal(position_snapshot.quantity) or Decimal("0")
            if quantity == 0:
                continue
            target_by_key.setdefault(
                key,
                {
                    "account_key": broker_account.account_key,
                    "symbol": position_snapshot.symbol,
                    "exchange": position_snapshot.exchange,
                    "currency": position_snapshot.currency,
                    "security_type": position_snapshot.security_type,
                    "primary_exchange": position_snapshot.primary_exchange,
                    "local_symbol": position_snapshot.local_symbol,
                },
            )

        filled_orders: list[dict[str, Any]] = []
        quotes_recorded: list[dict[str, Any]] = []
        skipped_count = 0

        for key, target in target_by_key.items():
            stream_symbol = str(target["symbol"]).upper()
            quote_payload = quotes_by_symbol.get(stream_symbol)
            bar_payload = _latest_stream_bar(stream, symbol=stream_symbol)
            if quote_payload is None and bar_payload is None:
                skipped_count += 1
                continue

            bid_price = (
                _optional_decimal(quote_payload.get("bid_price"))
                if quote_payload is not None
                else None
            )
            ask_price = (
                _optional_decimal(quote_payload.get("ask_price"))
                if quote_payload is not None
                else None
            )
            last_price = (
                _optional_decimal(quote_payload.get("last_price"))
                if quote_payload is not None
                else None
            )
            if last_price is None and bar_payload is not None:
                last_price = _optional_decimal(bar_payload.get("close"))

            if (
                bid_price is None
                and ask_price is None
                and last_price is None
                and bar_payload is None
            ):
                skipped_count += 1
                continue

            broker_account = ensure_virtual_account(
                session,
                account_key=target["account_key"],
                base_currency=target["currency"] or "SEK",
            )
            quote = VirtualMarketQuoteRecord(
                account_key=target["account_key"],
                observed_at=_quote_observed_at_from_stream(
                    quote_payload=quote_payload,
                    bar_payload=bar_payload,
                    fallback=fallback_observed_at,
                ),
                symbol=target["symbol"],
                exchange=(
                    _normalize_text(
                        quote_payload.get("exchange") if quote_payload else None
                    )
                    or target["exchange"]
                ).upper(),
                currency=target["currency"],
                security_type=target["security_type"],
                primary_exchange=(
                    _normalize_text(
                        quote_payload.get("primary_exchange") if quote_payload else None
                    )
                    or target["primary_exchange"]
                ),
                local_symbol=target["local_symbol"],
                bid_price=_decimal_to_string(bid_price),
                ask_price=_decimal_to_string(ask_price),
                last_price=_decimal_to_string(last_price),
                midpoint_price=None,
                source=_STREAM_VIRTUAL_QUOTE_SOURCE,
                raw_payload=_serialize_for_json(
                    {
                        "stream_quote": quote_payload or {},
                        "latest_stream_bar": bar_payload or {},
                    }
                ),
                metadata_json={
                    "virtual_stream_bridge": True,
                    "broker_order_ids_seen": [
                        order.id
                        for order in broker_orders
                        if order.account_key == target["account_key"]
                        and order.symbol == target["symbol"]
                        and order.currency == target["currency"]
                        and order.security_type == target["security_type"]
                    ],
                },
            )
            session.add(quote)
            session.flush()
            _persist_virtual_position_mark_snapshots(
                session,
                broker_account=broker_account,
                quote=quote,
            )
            quote_fills = process_virtual_quote_fills(session, quote=quote)
            filled_orders.extend(quote_fills)
            if not quote_fills:
                _persist_virtual_account_snapshot(
                    session,
                    broker_account=broker_account,
                    snapshot_at=quote.observed_at,
                )
            quotes_recorded.append(serialize_virtual_quote(quote))

        return {
            "source": _STREAM_VIRTUAL_QUOTE_SOURCE,
            "open_virtual_order_count": len(broker_orders),
            "virtual_market_target_count": len(target_by_key),
            "quote_count": len(quotes_recorded),
            "quotes": quotes_recorded,
            "skipped_order_count": skipped_count,
            "filled_order_count": len(filled_orders),
            "filled_orders": filled_orders,
        }


def list_virtual_market_quotes(
    session_factory: sessionmaker[Session],
    *,
    account_key: str | None = None,
    limit: int = 100,
) -> tuple[dict[str, Any], ...]:
    with session_scope(session_factory) as session:
        statement = select(VirtualMarketQuoteRecord).order_by(
            VirtualMarketQuoteRecord.observed_at.desc(),
            VirtualMarketQuoteRecord.id.desc(),
        )
        if account_key is not None:
            statement = statement.where(
                VirtualMarketQuoteRecord.account_key == normalize_virtual_account_key(
                    account_key
                )
            )
        rows = session.execute(statement.limit(limit)).scalars().all()
        return tuple(serialize_virtual_quote(row) for row in rows)


def read_virtual_market_price(
    session_factory: sessionmaker[Session],
    instruction: ExecutionInstruction,
) -> dict[str, Any]:
    account_key = normalize_virtual_account_key(instruction.account.account_key)
    with session_scope(session_factory) as session:
        quote = _latest_virtual_quote(
            session,
            account_key=account_key,
            symbol=instruction.instrument.symbol,
            currency=instruction.instrument.currency,
            security_type=instruction.instrument.security_type.value,
        )
        if quote is None:
            raise LookupError(
                f"No virtual market-watch quote is available for {account_key} "
                f"{instruction.instrument.symbol}.{instruction.instrument.currency}."
            )
        action = "BUY" if instruction.intent.side == "SELL" else "SELL"
        price = _quote_price_for_action(quote, action=action)
        if price is None:
            raise LookupError(
                f"Virtual market-watch quote for {instruction.instrument.symbol} "
                "does not contain a usable price."
            )
        return {
            "price": str(price),
            "observed_at": quote.observed_at.isoformat(),
            "source": "virtual_market_watch",
            "quote": serialize_virtual_quote(quote),
        }



__all__ = [name for name in globals() if not name.startswith("__")]
