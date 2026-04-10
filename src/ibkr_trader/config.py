from __future__ import annotations

from dataclasses import dataclass
from os import getenv


@dataclass(slots=True)
class IbkrConnectionConfig:
    host: str
    port: int
    client_id: int
    account_id: str

    @classmethod
    def from_env(cls) -> "IbkrConnectionConfig":
        return cls(
            host=getenv("IBKR_HOST", "127.0.0.1"),
            port=int(getenv("IBKR_PORT", "4002")),
            client_id=int(getenv("IBKR_CLIENT_ID", "101")),
            account_id=getenv("IBKR_ACCOUNT_ID", ""),
        )


@dataclass(slots=True)
class AppConfig:
    environment: str
    timezone: str
    database_url: str
    ibkr: IbkrConnectionConfig

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            environment=getenv("APP_ENV", "dev"),
            timezone=getenv("APP_TIMEZONE", "America/New_York"),
            database_url=getenv(
                "DATABASE_URL",
                "postgresql://postgres:postgres@localhost:5432/ibkr_trader",
            ),
            ibkr=IbkrConnectionConfig.from_env(),
        )

