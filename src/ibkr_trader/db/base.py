from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone

from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


def normalize_database_url(database_url: str) -> str:
    if database_url.startswith("postgresql://"):
        return database_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return database_url


def build_engine(database_url: str, *, echo: bool = False) -> Engine:
    return create_engine(normalize_database_url(database_url), echo=echo)


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)


@contextmanager
def session_scope(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def create_schema(engine: Engine) -> None:
    from ibkr_trader.db import models  # noqa: F401

    Base.metadata.create_all(engine)
    _upgrade_control_plane_schema(engine)


def _upgrade_control_plane_schema(engine: Engine) -> None:
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    if "instruction" not in table_names:
        return

    existing_columns = {
        column["name"] for column in inspector.get_columns("instruction")
    }
    upgrade_statements: list[str] = []

    def add_boolean_if_missing(table_name: str, column_name: str) -> None:
        if table_name not in table_names:
            return
        table_columns = {
            column["name"] for column in inspector.get_columns(table_name)
        }
        if column_name not in table_columns:
            upgrade_statements.append(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} BOOLEAN NOT NULL DEFAULT FALSE"
            )

    def add_archive_columns_if_missing(table_name: str) -> None:
        if table_name not in table_names:
            return
        table_columns = {
            column["name"] for column in inspector.get_columns(table_name)
        }
        if "archived_at" not in table_columns:
            upgrade_statements.append(
                f"ALTER TABLE {table_name} ADD COLUMN archived_at TIMESTAMP WITH TIME ZONE"
            )
        if "archived_by" not in table_columns:
            upgrade_statements.append(
                f"ALTER TABLE {table_name} ADD COLUMN archived_by VARCHAR(64)"
            )
        if "archive_reason" not in table_columns:
            upgrade_statements.append(
                f"ALTER TABLE {table_name} ADD COLUMN archive_reason TEXT"
            )

    def add_varchar_if_missing(
        table_name: str,
        column_name: str,
        length: int,
    ) -> None:
        if table_name not in table_names:
            return
        table_columns = {
            column["name"] for column in inspector.get_columns(table_name)
        }
        if column_name not in table_columns:
            upgrade_statements.append(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} VARCHAR({length})"
            )

    def widen_varchar_if_short(
        table_name: str,
        column_name: str,
        min_length: int,
    ) -> None:
        if engine.dialect.name != "postgresql" or table_name not in table_names:
            return
        table_columns = {
            column["name"]: column for column in inspector.get_columns(table_name)
        }
        column = table_columns.get(column_name)
        if column is None:
            return
        current_length = getattr(column["type"], "length", None)
        if current_length is not None and current_length < min_length:
            upgrade_statements.append(
                f"ALTER TABLE {table_name} ALTER COLUMN {column_name} "
                f"TYPE VARCHAR({min_length})"
            )

    def promote_integer_to_bigint(table_name: str, column_name: str) -> None:
        if engine.dialect.name != "postgresql" or table_name not in table_names:
            return
        table_columns = {
            column["name"]: column for column in inspector.get_columns(table_name)
        }
        column = table_columns.get(column_name)
        if column is None:
            return
        if str(column["type"]).upper() == "INTEGER":
            upgrade_statements.append(
                f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE BIGINT"
            )

    add_boolean_if_missing("trader_deployment", "is_virtual")
    add_boolean_if_missing("instruction", "is_virtual")
    add_boolean_if_missing("broker_account", "is_virtual")
    add_boolean_if_missing("broker_order", "is_virtual")
    add_boolean_if_missing("execution_fill", "is_virtual")
    add_boolean_if_missing("account_snapshot", "is_virtual")
    add_boolean_if_missing("position_snapshot", "is_virtual")
    add_varchar_if_missing("position_snapshot", "owner_instruction_id", 128)
    add_varchar_if_missing("position_snapshot", "owner_source_instruction_id", 128)
    add_varchar_if_missing("position_snapshot", "owner_deployment_key", 128)
    add_varchar_if_missing("position_snapshot", "owner_book_key", 64)
    add_archive_columns_if_missing("broker_order_event")
    add_archive_columns_if_missing("reconciliation_issue")
    widen_varchar_if_short("broker_order", "order_ref", 512)
    widen_varchar_if_short("execution_fill", "order_ref", 512)
    promote_integer_to_bigint("instruction", "broker_order_id")
    promote_integer_to_bigint("instruction", "broker_perm_id")
    promote_integer_to_bigint("instruction", "exit_order_id")
    promote_integer_to_bigint("instruction", "exit_perm_id")

    if "broker_order_id" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN broker_order_id BIGINT"
        )
    if "broker_perm_id" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN broker_perm_id BIGINT"
        )
    if "broker_client_id" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN broker_client_id INTEGER"
        )
    if "broker_order_status" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN broker_order_status VARCHAR(32)"
        )
    if "entry_submitted_quantity" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN entry_submitted_quantity VARCHAR(64)"
        )
    if "entry_filled_quantity" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN entry_filled_quantity VARCHAR(64)"
        )
    if "entry_avg_fill_price" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN entry_avg_fill_price VARCHAR(64)"
        )
    if "entry_filled_at" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN entry_filled_at TIMESTAMP WITH TIME ZONE"
        )
    if "exit_order_id" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN exit_order_id BIGINT"
        )
    if "exit_perm_id" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN exit_perm_id BIGINT"
        )
    if "exit_client_id" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN exit_client_id INTEGER"
        )
    if "exit_order_status" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN exit_order_status VARCHAR(32)"
        )
    if "exit_submitted_quantity" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN exit_submitted_quantity VARCHAR(64)"
        )
    if "exit_filled_quantity" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN exit_filled_quantity VARCHAR(64)"
        )
    if "exit_avg_fill_price" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN exit_avg_fill_price VARCHAR(64)"
        )
    if "exit_filled_at" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN exit_filled_at TIMESTAMP WITH TIME ZONE"
        )
    if "archived_at" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN archived_at TIMESTAMP WITH TIME ZONE"
        )
    if "archived_by" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN archived_by VARCHAR(64)"
        )
    if "archive_reason" not in existing_columns:
        upgrade_statements.append(
            "ALTER TABLE instruction ADD COLUMN archive_reason TEXT"
        )

    with engine.begin() as connection:
        for statement in upgrade_statements:
            connection.execute(text(statement))
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_instruction_broker_order_id "
                "ON instruction (broker_order_id)"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_instruction_broker_perm_id "
                "ON instruction (broker_perm_id)"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_instruction_exit_order_id "
                "ON instruction (exit_order_id)"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_instruction_archived_at "
                "ON instruction (archived_at)"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_broker_order_event_archived_at "
                "ON broker_order_event (archived_at)"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_reconciliation_issue_archived_at "
                "ON reconciliation_issue (archived_at)"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_broker_account_is_virtual "
                "ON broker_account (is_virtual)"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_broker_order_is_virtual "
                "ON broker_order (is_virtual)"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_execution_fill_is_virtual "
                "ON execution_fill (is_virtual)"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_position_snapshot_owner_deployment_key "
                "ON position_snapshot (owner_deployment_key)"
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_position_snapshot_owner_instruction_id "
                "ON position_snapshot (owner_instruction_id)"
            )
        )
