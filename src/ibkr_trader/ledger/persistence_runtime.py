from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.ibkr.runtime_snapshot import BrokerRuntimeSnapshot
from ibkr_trader.ledger.persistence_order_records import _upsert_open_order
from ibkr_trader.ledger.persistence_shared import _get_or_create_broker_account
from ibkr_trader.ledger.persistence_shared import _mark_missing_open_orders_closed
from ibkr_trader.ledger.persistence_shared import _normalize_text
from ibkr_trader.ledger.persistence_shared import _resolve_account_key
from ibkr_trader.ledger.persistence_snapshots import _persist_account_snapshots
from ibkr_trader.ledger.persistence_snapshots import _persist_executions
from ibkr_trader.ledger.persistence_snapshots import _persist_position_snapshots

def persist_broker_runtime_snapshot(
    session_factory: sessionmaker[Session],
    snapshot: BrokerRuntimeSnapshot,
    *,
    broker_kind: str,
    captured_at: datetime,
    default_account_key: str | None = None,
    close_missing_open_orders: bool = False,
    empty_open_orders_authoritative: bool = False,
) -> None:
    """Persist a real broker runtime snapshot into durable ledger tables."""

    with session_scope(session_factory) as session:
        broker_accounts = _persist_account_snapshots(
            session,
            broker_kind=broker_kind,
            snapshot=snapshot,
            captured_at=captured_at,
            default_account_key=default_account_key,
        )

        _persist_position_snapshots(
            session,
            broker_kind=broker_kind,
            snapshot=snapshot,
            captured_at=captured_at,
            default_account_key=default_account_key,
            broker_accounts=broker_accounts,
        )

        for open_order in snapshot.open_orders.values():
            account_key = _resolve_account_key(
                open_order.account,
                default_account_key=default_account_key,
                context=f"Open order {open_order.order_id}",
            )
            broker_account = broker_accounts.get(account_key)
            if broker_account is None:
                broker_account = _get_or_create_broker_account(
                    session,
                    broker_kind=broker_kind,
                    account_key=account_key,
                    base_currency=_normalize_text(open_order.currency),
                )
                broker_accounts[account_key] = broker_account
            _upsert_open_order(
                session,
                broker_kind=broker_kind,
                broker_account=broker_account,
                open_order=open_order,
                observed_at=captured_at,
                default_account_key=default_account_key,
            )

        if close_missing_open_orders:
            _mark_missing_open_orders_closed(
                session,
                broker_kind=broker_kind,
                snapshot=snapshot,
                observed_at=captured_at,
                default_account_key=default_account_key,
                empty_open_orders_authoritative=empty_open_orders_authoritative,
            )

        _persist_executions(
            session,
            broker_kind=broker_kind,
            snapshot=snapshot,
            captured_at=captured_at,
            default_account_key=default_account_key,
            broker_accounts=broker_accounts,
        )
