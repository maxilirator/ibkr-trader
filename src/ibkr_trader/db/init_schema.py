from __future__ import annotations

from ibkr_trader.config import AppConfig
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema


def main() -> None:
    config = AppConfig.from_env()
    engine = build_engine(config.database_url)
    create_schema(engine)
    print("Database schema is ready.")


if __name__ == "__main__":
    main()
