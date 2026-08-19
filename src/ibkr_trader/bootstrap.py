"""Source-independent, fail-closed bootstrap configuration.

Historically this service resolved its environment from ``<checkout>/.env``.
That couples production configuration to whichever source tree the process
happened to start from: a second checkout, a stale working copy, or a developer
tree could silently supply production values. It also means the defaults in
``config.py`` decide what happens when configuration is simply absent, and some
of those defaults are actively dangerous in production - ``DATABASE_URL``
defaults to a local throwaway Postgres, and ``IBKR_PORT`` defaults to ``7497``,
which is the *paper* port.

This module makes production configuration come from one protected absolute
path, ``/etc/ibkr-trader/bootstrap.env``, and makes a missing or incomplete
bootstrap a hard startup failure instead of a silent fall back to defaults.

Two rules define the behaviour:

* **Source-independent.** In production the checkout-local ``.env`` is never
  read. Configuration lives outside the source tree and does not move with it.
* **Fail-closed.** In production a missing file, an unreadable file, a
  world-readable file, or a missing required key raises
  :class:`BootstrapConfigurationError` and the process does not start. Refusing
  to start is recoverable; trading against the wrong database or the paper port
  because a default filled in silently is not.

Outside production the file is optional and the existing ``.env`` flow is kept,
so development and tests are unaffected.

Secret *values* are never logged or included in error messages or the audit
payload. Only key names appear.
"""

from __future__ import annotations

import stat
from dataclasses import dataclass, field
from os import environ, getenv
from pathlib import Path

#: Protected location of the production bootstrap environment.
DEFAULT_BOOTSTRAP_ENV_PATH = Path("/etc/ibkr-trader/bootstrap.env")

#: Overrides the bootstrap path. Intended for tests and for staging hosts that
#: keep their protected environment somewhere other than the default.
BOOTSTRAP_ENV_PATH_VAR = "IBKR_TRADER_BOOTSTRAP_ENV"

#: ``APP_ENV`` values that select fail-closed production behaviour.
#:
#: Deliberately excludes ``live``. That word already means something else in
#: this codebase - an RL *deployment mode* (``virtual``/``paper``/``live``) - and
#: overloading it to also flip startup into fail-closed mode would make the
#: trigger ambiguous. The trigger for refusing to start must be unambiguous, so
#: it is spelled out explicitly.
PRODUCTION_ENVIRONMENTS = frozenset({"production", "prod"})

#: Keys that must be present and non-empty in production.
#:
#: Each of these has a default in ``config.py`` that is wrong-but-plausible in
#: production, which is exactly the failure mode this list exists to prevent:
#: ``DATABASE_URL`` would point at a local throwaway database, and ``IBKR_PORT``
#: would point at the paper-trading port rather than the live Gateway.
REQUIRED_PRODUCTION_KEYS = (
    "DATABASE_URL",
    "IBKR_HOST",
    "IBKR_PORT",
)

#: At least one of these must be present and non-empty in production, so the
#: runtime always knows which IBKR account it is acting for.
REQUIRED_PRODUCTION_ACCOUNT_KEYS = ("IBKR_ACCOUNT_ID", "IBKR_ACCOUNT_IDS")


class BootstrapConfigurationError(RuntimeError):
    """Raised when production bootstrap configuration cannot be trusted.

    Always describes what failed, which path it failed on, and what the operator
    must do. Never contains a secret value.
    """


@dataclass(frozen=True, slots=True)
class BootstrapLoadResult:
    """Audit record of how the runtime environment was resolved.

    Contains key *names* only, never values, so it is safe to log and to expose
    through the operator API.
    """

    environment: str
    is_production: bool
    #: "bootstrap" (protected file), "dotenv" (checkout-local), or "none".
    source: str
    path: Path | None
    applied_keys: tuple[str, ...] = ()
    #: Keys where the protected file overrode a different ambient value.
    overridden_keys: tuple[str, ...] = ()
    warnings: tuple[str, ...] = field(default=())

    def to_payload(self) -> dict[str, object]:
        return {
            "environment": self.environment,
            "is_production": self.is_production,
            "source": self.source,
            "path": str(self.path) if self.path is not None else None,
            "applied_keys": list(self.applied_keys),
            "overridden_keys": list(self.overridden_keys),
            "warnings": list(self.warnings),
        }


def is_production_environment(environment: str | None) -> bool:
    """Whether ``environment`` selects fail-closed production behaviour."""
    return (environment or "").strip().lower() in PRODUCTION_ENVIRONMENTS


def bootstrap_env_path() -> Path:
    """Resolve the bootstrap file path, honouring the override variable."""
    configured = getenv(BOOTSTRAP_ENV_PATH_VAR, "").strip()
    if configured:
        return Path(configured).expanduser()
    return DEFAULT_BOOTSTRAP_ENV_PATH


def _parse_env_text(text: str) -> dict[str, str]:
    """Parse ``KEY=value`` lines, ignoring blanks, comments and ``export``."""
    # Imported lazily to keep config.py as the single owner of the line format.
    from ibkr_trader.config import _parse_env_line

    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        parsed = _parse_env_line(raw_line)
        if parsed is None:
            continue
        key, value = parsed
        values[key] = value
    return values


def _assert_not_world_accessible(path: Path) -> None:
    """Refuse to use a secrets file that any local user can read.

    Group access is permitted so a service group can share the file; world
    access is not, because that defeats the point of a protected environment.
    """
    try:
        mode = path.stat().st_mode
    except OSError as exc:
        raise BootstrapConfigurationError(
            f"Bootstrap environment file {path} could not be inspected: {exc}. "
            "Production startup requires a readable protected environment file."
        ) from exc

    if mode & (stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH):
        raise BootstrapConfigurationError(
            f"Bootstrap environment file {path} is world-accessible "
            f"(mode {stat.filemode(mode)}). It holds secrets and must not be "
            "readable by all local users. Fix with: "
            f"chmod o= {path}"
        )


def _read_bootstrap_file(path: Path) -> dict[str, str]:
    if not path.exists():
        raise BootstrapConfigurationError(
            f"Bootstrap environment file {path} does not exist. Production "
            "startup is fail-closed and will not fall back to checkout-local "
            "configuration or to built-in defaults. Create the file (see "
            "docs/bootstrap-configuration.md) or set "
            f"{BOOTSTRAP_ENV_PATH_VAR} to its location."
        )
    if not path.is_file():
        raise BootstrapConfigurationError(
            f"Bootstrap environment path {path} is not a regular file."
        )

    _assert_not_world_accessible(path)

    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise BootstrapConfigurationError(
            f"Bootstrap environment file {path} could not be read: {exc}. "
            "Check that the service user has read access."
        ) from exc

    return _parse_env_text(text)


def _assert_required_keys_present(values: dict[str, str], path: Path) -> None:
    """Verify the production contract, reporting *all* problems at once.

    Reporting every missing key together matters operationally: fixing a
    production outage one restart-and-discover-the-next-missing-key at a time is
    exactly the loop this check exists to avoid.
    """
    missing = [
        key
        for key in REQUIRED_PRODUCTION_KEYS
        if not (values.get(key) or "").strip()
    ]
    has_account = any(
        (values.get(key) or "").strip() for key in REQUIRED_PRODUCTION_ACCOUNT_KEYS
    )

    problems: list[str] = []
    if missing:
        problems.append(f"missing or empty required keys: {', '.join(missing)}")
    if not has_account:
        problems.append(
            "no IBKR account configured; set one of "
            f"{' or '.join(REQUIRED_PRODUCTION_ACCOUNT_KEYS)}"
        )

    if problems:
        raise BootstrapConfigurationError(
            f"Bootstrap environment file {path} is incomplete for production: "
            + "; ".join(problems)
            + ". Production startup is fail-closed: these have unsafe defaults "
            "(a local throwaway database, and the paper-trading IBKR port) and "
            "will not be guessed."
        )


def load_runtime_environment(
    *,
    environment: str | None = None,
    bootstrap_path: Path | None = None,
    dotenv_path: Path | None = None,
) -> BootstrapLoadResult:
    """Populate ``os.environ`` for this process and report how it was resolved.

    In production the protected file is authoritative and overrides ambient
    values. A protected configuration that any inherited environment variable
    can silently replace is not protected; overridden key names are recorded in
    the result so the change is auditable.

    Outside production nothing is authoritative: the bootstrap file, if present,
    and then the checkout-local ``.env`` are applied with ``setdefault``, so an
    explicitly exported variable still wins for local work.

    Args:
        environment: ``APP_ENV`` value. Read from the environment when omitted.
        bootstrap_path: Override the protected file location.
        dotenv_path: Override the checkout-local ``.env`` location.

    Raises:
        BootstrapConfigurationError: In production, when the protected file is
            missing, unreadable, world-accessible, or incomplete.
    """
    from ibkr_trader.config import DEFAULT_ENV_FILE, load_dotenv_file

    resolved_environment = (
        environment if environment is not None else getenv("APP_ENV", "dev")
    )
    production = is_production_environment(resolved_environment)
    path = bootstrap_path or bootstrap_env_path()

    if production:
        values = _read_bootstrap_file(path)
        _assert_required_keys_present(values, path)

        overridden = tuple(
            sorted(
                key
                for key, value in values.items()
                if key in environ and environ[key] != value
            )
        )
        for key, value in values.items():
            environ[key] = value

        # APP_ENV is the input to this decision; the file must not redefine it
        # into a non-production value and thereby disable its own enforcement.
        environ["APP_ENV"] = resolved_environment

        return BootstrapLoadResult(
            environment=resolved_environment,
            is_production=True,
            source="bootstrap",
            path=path,
            applied_keys=tuple(sorted(values)),
            overridden_keys=overridden,
            warnings=(),
        )

    warnings: list[str] = []
    applied: list[str] = []
    source = "none"

    if path.exists():
        try:
            values = _read_bootstrap_file(path)
        except BootstrapConfigurationError as exc:
            # Outside production this is informational, not fatal.
            warnings.append(str(exc))
        else:
            for key, value in values.items():
                environ.setdefault(key, value)
            applied.extend(values)
            source = "bootstrap"

    resolved_dotenv = dotenv_path or DEFAULT_ENV_FILE
    if resolved_dotenv.exists():
        load_dotenv_file(resolved_dotenv)
        source = "bootstrap+dotenv" if source == "bootstrap" else "dotenv"

    return BootstrapLoadResult(
        environment=resolved_environment,
        is_production=False,
        source=source,
        path=path if path.exists() else None,
        applied_keys=tuple(sorted(set(applied))),
        overridden_keys=(),
        warnings=tuple(warnings),
    )


def require_production_value(key: str, value: str | None) -> str:
    """Return a required production value or fail closed.

    Used by ``config.py`` for settings whose built-in default would be unsafe in
    production.
    """
    resolved = (value or "").strip()
    if not resolved:
        raise BootstrapConfigurationError(
            f"{key} is required in production but was not set. It is sourced "
            f"from {bootstrap_env_path()}; production startup will not fall "
            "back to a built-in default for this setting."
        )
    return resolved
