from __future__ import annotations

from dataclasses import replace
from dataclasses import dataclass
from os import environ, getenv
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"


def _parse_env_line(raw_line: str) -> tuple[str, str] | None:
    line = raw_line.strip()
    if not line or line.startswith("#"):
        return None

    if line.startswith("export "):
        line = line[7:].strip()

    if "=" not in line:
        return None

    key, value = line.split("=", 1)
    key = key.strip()
    value = value.strip()

    if not key:
        return None

    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1]

    return key, value


def load_dotenv_file(path: Path = DEFAULT_ENV_FILE) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        parsed = _parse_env_line(raw_line)
        if parsed is None:
            continue

        key, value = parsed
        environ.setdefault(key, value)


@dataclass(slots=True)
class IbkrConnectionConfig:
    host: str
    port: int
    client_id: int
    diagnostic_client_id: int
    account_id: str

    def primary_session(self) -> "IbkrConnectionConfig":
        return replace(self, client_id=self.client_id)

    def diagnostic_session(self) -> "IbkrConnectionConfig":
        return replace(self, client_id=self.diagnostic_client_id)

    @classmethod
    def from_env(cls) -> "IbkrConnectionConfig":
        return cls(
            host=getenv("IBKR_HOST", "127.0.0.1"),
            port=int(getenv("IBKR_PORT", "7497")),
            client_id=int(getenv("IBKR_CLIENT_ID", "0")),
            diagnostic_client_id=int(getenv("IBKR_DIAGNOSTIC_CLIENT_ID", "7")),
            account_id=getenv("IBKR_ACCOUNT_ID", ""),
        )


@dataclass(slots=True)
class ApiServerConfig:
    host: str
    port: int
    require_loopback_only: bool

    @classmethod
    def from_env(cls) -> "ApiServerConfig":
        return cls(
            host=getenv("API_HOST", "127.0.0.1"),
            port=int(getenv("API_PORT", "8000")),
            require_loopback_only=getenv(
                "API_REQUIRE_LOOPBACK_ONLY",
                "true",
            ).lower() not in {"0", "false", "no"},
        )


@dataclass(slots=True)
class AppConfig:
    environment: str
    timezone: str
    database_url: str
    api: ApiServerConfig
    ibkr: IbkrConnectionConfig

    @classmethod
    def from_env(cls) -> "AppConfig":
        load_dotenv_file()
        return cls(
            environment=getenv("APP_ENV", "dev"),
            timezone=getenv("APP_TIMEZONE", "America/New_York"),
            database_url=getenv(
                "DATABASE_URL",
                "postgresql://postgres:postgres@localhost:5432/ibkr_trader",
            ),
            api=ApiServerConfig.from_env(),
            ibkr=IbkrConnectionConfig.from_env(),
        )
