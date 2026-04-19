from __future__ import annotations

from dataclasses import replace
from dataclasses import dataclass
from os import environ, getenv
from pathlib import Path

from ibkr_trader.ibkr.client_ids import DIAGNOSTIC_CLIENT_ID
from ibkr_trader.ibkr.client_ids import PRIMARY_RUNTIME_CLIENT_ID
from ibkr_trader.ibkr.client_ids import STREAMING_CLIENT_ID


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


def _resolve_project_path(raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def _parse_env_list(raw_value: str) -> tuple[str, ...]:
    return tuple(
        value.strip()
        for value in raw_value.split(",")
        if value.strip()
    )


@dataclass(slots=True)
class IbkrConnectionConfig:
    host: str
    port: int
    client_id: int
    diagnostic_client_id: int
    streaming_client_id: int = STREAMING_CLIENT_ID
    account_id: str = ""
    account_ids: tuple[str, ...] = ()

    def primary_session(self) -> "IbkrConnectionConfig":
        return replace(self, client_id=self.client_id)

    def diagnostic_session(self) -> "IbkrConnectionConfig":
        return replace(self, client_id=self.diagnostic_client_id)

    def streaming_session(self) -> "IbkrConnectionConfig":
        return replace(self, client_id=self.streaming_client_id)

    @classmethod
    def from_env(cls) -> "IbkrConnectionConfig":
        configured_account_ids = _parse_env_list(getenv("IBKR_ACCOUNT_IDS", ""))
        configured_account_id = getenv("IBKR_ACCOUNT_ID", "").strip()
        if not configured_account_ids and configured_account_id:
            configured_account_ids = (configured_account_id,)
        if not configured_account_id and configured_account_ids:
            configured_account_id = configured_account_ids[0]
        return cls(
            host=getenv("IBKR_HOST", "127.0.0.1"),
            port=int(getenv("IBKR_PORT", "7497")),
            client_id=int(getenv("IBKR_CLIENT_ID", str(PRIMARY_RUNTIME_CLIENT_ID))),
            diagnostic_client_id=int(
                getenv("IBKR_DIAGNOSTIC_CLIENT_ID", str(DIAGNOSTIC_CLIENT_ID))
            ),
            streaming_client_id=int(
                getenv("IBKR_STREAMING_CLIENT_ID", str(STREAMING_CLIENT_ID))
            ),
            account_id=configured_account_id,
            account_ids=configured_account_ids,
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
    session_calendar_path: Path
    stockholm_instruments_path: Path
    stockholm_identity_path: Path
    api: ApiServerConfig
    ibkr: IbkrConnectionConfig
    broker_monitor_enabled: bool = True
    broker_heartbeat_interval_seconds: float = 30.0
    broker_heartbeat_timeout_seconds: int = 5
    broker_snapshot_refresh_interval_seconds: float = 60.0
    broker_snapshot_refresh_timeout_seconds: int = 10
    execution_runtime_enabled: bool = False
    execution_runtime_interval_seconds: float = 5.0
    execution_runtime_timeout_seconds: int = 10
    execution_runtime_allow_startup_issues: bool = False
    execution_runtime_lease_seconds: float = 30.0

    @classmethod
    def from_env(cls) -> "AppConfig":
        load_dotenv_file()
        return cls(
            environment=getenv("APP_ENV", "dev"),
            timezone=getenv("APP_TIMEZONE", "Europe/Stockholm"),
            database_url=getenv(
                "DATABASE_URL",
                "postgresql://postgres:postgres@localhost:5432/ibkr_trader",
            ),
            session_calendar_path=_resolve_project_path(
                getenv(
                    "SESSION_CALENDAR_PATH",
                    "../q-data/xsto/calendars/day_sessions.parquet",
                )
            ),
            stockholm_instruments_path=_resolve_project_path(
                getenv(
                    "XSTO_INSTRUMENTS_PATH",
                    "../q-data/xsto/instruments/all.txt",
                )
            ),
            stockholm_identity_path=_resolve_project_path(
                getenv(
                    "XSTO_IDENTITY_PATH",
                    "../q-data/xsto/meta/instrument_identity.parquet",
                )
            ),
            api=ApiServerConfig.from_env(),
            ibkr=IbkrConnectionConfig.from_env(),
            broker_monitor_enabled=getenv(
                "BROKER_MONITOR_ENABLED",
                "true",
            ).lower()
            not in {"0", "false", "no"},
            broker_heartbeat_interval_seconds=float(
                getenv("BROKER_HEARTBEAT_INTERVAL_SECONDS", "30")
            ),
            broker_heartbeat_timeout_seconds=int(
                getenv("BROKER_HEARTBEAT_TIMEOUT_SECONDS", "5")
            ),
            broker_snapshot_refresh_interval_seconds=float(
                getenv("BROKER_SNAPSHOT_REFRESH_INTERVAL_SECONDS", "60")
            ),
            broker_snapshot_refresh_timeout_seconds=int(
                getenv("BROKER_SNAPSHOT_REFRESH_TIMEOUT_SECONDS", "10")
            ),
            execution_runtime_enabled=getenv(
                "EXECUTION_RUNTIME_ENABLED",
                "false",
            ).lower()
            not in {"0", "false", "no"},
            execution_runtime_interval_seconds=float(
                getenv("EXECUTION_RUNTIME_INTERVAL_SECONDS", "5")
            ),
            execution_runtime_timeout_seconds=int(
                getenv("EXECUTION_RUNTIME_TIMEOUT_SECONDS", "10")
            ),
            execution_runtime_allow_startup_issues=getenv(
                "EXECUTION_RUNTIME_ALLOW_STARTUP_ISSUES",
                "false",
            ).lower()
            not in {"0", "false", "no"},
            execution_runtime_lease_seconds=float(
                getenv("EXECUTION_RUNTIME_LEASE_SECONDS", "30")
            ),
        )
