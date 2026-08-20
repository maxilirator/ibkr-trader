from __future__ import annotations

from sqlalchemy.orm import sessionmaker

from ibkr_trader.config import AppConfig
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.orchestration.operator_controls import seed_kill_switch_if_absent


def main() -> None:
    config = AppConfig.from_env()
    engine = build_engine(config.database_url)
    create_schema(engine)

    # Seeded here so a newly created database has a visible, audited kill switch
    # rather than one whose state has to be inferred from the absence of a row.
    # Never touches an existing record.
    seeded = seed_kill_switch_if_absent(sessionmaker(bind=engine, expire_on_commit=False))
    if seeded is not None:
        print("Seeded the global kill switch as enabled (no operator decision recorded).")

    print("Database schema is ready.")


if __name__ == "__main__":
    main()
