from __future__ import annotations

from dataclasses import replace
from dataclasses import dataclass
from os import environ, getenv
from pathlib import Path

from ibkr_trader.bootstrap import is_production_environment
from ibkr_trader.bootstrap import load_runtime_environment
from ibkr_trader.bootstrap import require_production_value
from ibkr_trader.q_data import resolve
from ibkr_trader.ibkr.client_ids import DIAGNOSTIC_CLIENT_ID
from ibkr_trader.ibkr.client_ids import HISTORICAL_CLIENT_ID
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




def _q_data_path(dataset_id: str) -> Path:
    """Resolve a shared dataset through the q-data catalog, and only that way.

    There used to be a path fallback here. It is gone on purpose: a fallback
    turns "the catalog is unreachable" into "read whatever is at this path",
    which is exactly how a six-month-stale file was once mistaken for current
    data. Failing here is loud and recoverable; reading the wrong file is not.
    """
    return resolve(dataset_id)


def _output_root() -> Path:
    """Where this service writes its own artefacts.

    Never derived from a resolved q-data path. Shared datasets are immutable,
    versioned and owned by q-data; a consumer writing into a sibling of one of
    them writes into someone else's contract.
    """
    return _resolve_project_path(getenv("IBKR_TRADER_OUTPUT_ROOT", "var/shortability"))

#: Values that switch a boolean environment flag off. Anything else - including
#: an unrecognised word - reads as enabled.
FALSE_ENV_FLAG_VALUES = frozenset({"0", "false", "no"})


def env_flag_is_enabled(raw_value: str) -> bool:
    """Coerce a boolean environment flag exactly as :class:`AppConfig` does.

    Exported so the settings registry can report the same answer the runtime
    acts on. A second, stricter implementation elsewhere would disagree on real
    inputs - ``off`` reads as *enabled* here, because it is not in the false set
    - and a dashboard that says "disabled" while the supervisor is running is
    worse than no dashboard.
    """
    return raw_value.strip().lower() not in FALSE_ENV_FLAG_VALUES


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
    historical_client_id: int = HISTORICAL_CLIENT_ID
    streaming_client_id: int = STREAMING_CLIENT_ID
    account_id: str = ""
    account_ids: tuple[str, ...] = ()

    def primary_session(self) -> "IbkrConnectionConfig":
        return replace(self, client_id=self.client_id)

    def diagnostic_session(self) -> "IbkrConnectionConfig":
        return replace(self, client_id=self.diagnostic_client_id)

    def historical_session(self) -> "IbkrConnectionConfig":
        return replace(self, client_id=self.historical_client_id)

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

        # In production these must come from the protected bootstrap file. The
        # defaults below are development conveniences and are actively unsafe
        # live: 7497 is the paper-trading port, so silently defaulting to it
        # would point the live runtime at the wrong Gateway.
        if is_production_environment(getenv("APP_ENV")):
            host = require_production_value("IBKR_HOST", getenv("IBKR_HOST"))
            port = int(require_production_value("IBKR_PORT", getenv("IBKR_PORT")))
        else:
            host = getenv("IBKR_HOST", "127.0.0.1")
            port = int(getenv("IBKR_PORT", "7497"))

        return cls(
            host=host,
            port=port,
            client_id=int(getenv("IBKR_CLIENT_ID", str(PRIMARY_RUNTIME_CLIENT_ID))),
            diagnostic_client_id=int(
                getenv("IBKR_DIAGNOSTIC_CLIENT_ID", str(DIAGNOSTIC_CLIENT_ID))
            ),
            historical_client_id=int(
                getenv("IBKR_HISTORICAL_CLIENT_ID", str(HISTORICAL_CLIENT_ID))
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
    access_log_enabled: bool = False

    @classmethod
    def from_env(cls) -> "ApiServerConfig":
        return cls(
            host=getenv("API_HOST", "127.0.0.1"),
            port=int(getenv("API_PORT", "8000")),
            require_loopback_only=getenv(
                "API_REQUIRE_LOOPBACK_ONLY",
                "true",
            ).lower() not in {"0", "false", "no"},
            access_log_enabled=getenv(
                "API_ACCESS_LOG_ENABLED",
                "false",
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
    #: Where this service writes its own artefacts. Defaulted so existing
    #: callers keep working; never derived from a resolved q-data path.
    output_root: Path = PROJECT_ROOT / "var"
    broker_warmup_enabled: bool = True
    broker_monitor_enabled: bool = True
    broker_connect_backoff_initial_seconds: float = 5.0
    broker_connect_backoff_max_seconds: float = 300.0
    broker_heartbeat_interval_seconds: float = 30.0
    broker_heartbeat_timeout_seconds: int = 5
    broker_snapshot_refresh_interval_seconds: float = 60.0
    broker_snapshot_refresh_timeout_seconds: int = 10
    broker_execution_recovery_interval_seconds: float = 900.0
    broker_execution_recovery_failure_cooldown_seconds: float = 900.0
    broker_status_refresh_min_interval_seconds: float = 30.0
    broker_api_startup_failure_slow_probe_seconds: float = 900.0
    ibkr_api_max_requests_per_second: float = 45.0
    ibkr_api_pacing_timeout_seconds: float = 2.0
    ibkr_market_data_line_limit: int = 80
    ibkr_historical_requests_per_10_minutes: int = 50
    rl_observed_bar_min_coverage_ratio: float = 0.8
    market_data_backfill_worker_enabled: bool = True
    market_data_backfill_interval_seconds: float = 60.0
    market_data_backfill_batch_size: int = 3
    market_data_backfill_timeout_seconds: int = 45
    market_stream_auto_reconnect_enabled: bool = True
    market_stream_reconnect_interval_seconds: float = 15.0
    market_stream_max_subscriptions: int = 120
    market_stream_stale_after_seconds: float = 180.0
    market_stream_stale_reconnect_enabled: bool = True
    execution_runtime_enabled: bool = False
    execution_runtime_interval_seconds: float = 5.0
    execution_runtime_timeout_seconds: int = 10
    execution_runtime_submission_lead_seconds: float = 60.0
    execution_runtime_allow_startup_issues: bool = False
    execution_runtime_lease_seconds: float = 30.0
    execution_runtime_restart_backoff_initial_seconds: float = 30.0
    execution_runtime_restart_backoff_max_seconds: float = 300.0

    @property
    def effective_market_stream_max_subscriptions(self) -> int:
        configured_stream_cap = int(self.market_stream_max_subscriptions)
        configured_line_limit = int(self.ibkr_market_data_line_limit)
        if configured_line_limit <= 0:
            return configured_stream_cap
        return min(configured_stream_cap, configured_line_limit)

    @classmethod
    def from_env(cls) -> "AppConfig":
        # Resolves the protected bootstrap file in production and the
        # checkout-local .env otherwise. Raises rather than starting on
        # incomplete production configuration.
        #
        # The environment is taken from the load result rather than re-read
        # from os.environ: a second read after the .env load is what previously
        # let a checkout-local file declare production *after* the gate had
        # already decided not to engage.
        bootstrap = load_runtime_environment()
        environment = bootstrap.environment

        if is_production_environment(environment):
            # No default: the development default points at a local throwaway
            # database, and silently using it in production would write the
            # ledger somewhere nobody is reading.
            database_url = require_production_value(
                "DATABASE_URL", getenv("DATABASE_URL")
            )
        else:
            database_url = getenv(
                "DATABASE_URL",
                "postgresql://postgres:postgres@localhost:5432/ibkr_trader",
            )

        return cls(
            environment=environment,
            timezone=getenv("APP_TIMEZONE", "Europe/Stockholm"),
            database_url=database_url,
            session_calendar_path=_q_data_path("xsto.world.calendar"),
            stockholm_instruments_path=_q_data_path("xsto.world.universe"),
            stockholm_identity_path=_q_data_path("xsto.world.instrument_identity"),
            output_root=_output_root(),
            api=ApiServerConfig.from_env(),
            ibkr=IbkrConnectionConfig.from_env(),
            broker_warmup_enabled=getenv(
                "BROKER_WARMUP_ENABLED",
                "true",
            ).lower()
            not in {"0", "false", "no"},
            broker_monitor_enabled=getenv(
                "BROKER_MONITOR_ENABLED",
                "true",
            ).lower()
            not in {"0", "false", "no"},
            broker_connect_backoff_initial_seconds=float(
                getenv("BROKER_CONNECT_BACKOFF_INITIAL_SECONDS", "5")
            ),
            broker_connect_backoff_max_seconds=float(
                getenv("BROKER_CONNECT_BACKOFF_MAX_SECONDS", "300")
            ),
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
            broker_execution_recovery_interval_seconds=float(
                getenv("BROKER_EXECUTION_RECOVERY_INTERVAL_SECONDS", "900")
            ),
            broker_execution_recovery_failure_cooldown_seconds=float(
                getenv("BROKER_EXECUTION_RECOVERY_FAILURE_COOLDOWN_SECONDS", "900")
            ),
            broker_status_refresh_min_interval_seconds=float(
                getenv("BROKER_STATUS_REFRESH_MIN_INTERVAL_SECONDS", "30")
            ),
            broker_api_startup_failure_slow_probe_seconds=float(
                getenv("IBKR_API_STARTUP_FAILURE_SLOW_PROBE_SECONDS", "900")
            ),
            ibkr_api_max_requests_per_second=float(
                getenv("IBKR_API_MAX_REQUESTS_PER_SECOND", "45")
            ),
            ibkr_api_pacing_timeout_seconds=float(
                getenv("IBKR_API_PACING_TIMEOUT_SECONDS", "2")
            ),
            ibkr_market_data_line_limit=int(
                getenv("IBKR_MARKET_DATA_LINE_LIMIT", "80")
            ),
            ibkr_historical_requests_per_10_minutes=int(
                getenv("IBKR_HISTORICAL_REQUESTS_PER_10_MINUTES", "50")
            ),
            rl_observed_bar_min_coverage_ratio=float(
                getenv("RL_OBSERVED_BAR_MIN_COVERAGE_RATIO", "0.8")
            ),
            market_data_backfill_worker_enabled=getenv(
                "MARKET_DATA_BACKFILL_WORKER_ENABLED",
                "true",
            ).lower()
            not in {"0", "false", "no"},
            market_data_backfill_interval_seconds=float(
                getenv("MARKET_DATA_BACKFILL_INTERVAL_SECONDS", "60")
            ),
            market_data_backfill_batch_size=int(
                getenv("MARKET_DATA_BACKFILL_BATCH_SIZE", "3")
            ),
            market_data_backfill_timeout_seconds=int(
                getenv("MARKET_DATA_BACKFILL_TIMEOUT_SECONDS", "45")
            ),
            market_stream_auto_reconnect_enabled=getenv(
                "MARKET_STREAM_AUTO_RECONNECT_ENABLED",
                "true",
            ).lower()
            not in {"0", "false", "no"},
            market_stream_reconnect_interval_seconds=float(
                getenv("MARKET_STREAM_RECONNECT_INTERVAL_SECONDS", "15")
            ),
            market_stream_max_subscriptions=int(
                getenv("MARKET_STREAM_MAX_SUBSCRIPTIONS", "120")
            ),
            market_stream_stale_after_seconds=float(
                getenv("MARKET_STREAM_STALE_AFTER_SECONDS", "180")
            ),
            market_stream_stale_reconnect_enabled=getenv(
                "MARKET_STREAM_STALE_RECONNECT_ENABLED",
                "true",
            ).lower()
            not in {"0", "false", "no"},
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
            execution_runtime_submission_lead_seconds=float(
                getenv("EXECUTION_RUNTIME_SUBMISSION_LEAD_SECONDS", "60")
            ),
            execution_runtime_allow_startup_issues=getenv(
                "EXECUTION_RUNTIME_ALLOW_STARTUP_ISSUES",
                "false",
            ).lower()
            not in {"0", "false", "no"},
            execution_runtime_lease_seconds=float(
                getenv("EXECUTION_RUNTIME_LEASE_SECONDS", "30")
            ),
            execution_runtime_restart_backoff_initial_seconds=float(
                getenv("EXECUTION_RUNTIME_RESTART_BACKOFF_INITIAL_SECONDS", "30")
            ),
            execution_runtime_restart_backoff_max_seconds=float(
                getenv("EXECUTION_RUNTIME_RESTART_BACKOFF_MAX_SECONDS", "300")
            ),
        )
