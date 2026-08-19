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

import logging
import os
import stat
from dataclasses import dataclass, field
from os import environ, getenv
from pathlib import Path

LOGGER = logging.getLogger(__name__)

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

#: IBKR paper-trading ports. Pointing the live runtime at one of these is the
#: specific accident this module exists to prevent, so it is refused rather
#: than merely defaulted away from.
PAPER_TRADING_PORTS = frozenset({7497, 7496})

#: Records a deliberate decision to run production against a paper port.
PAPER_PORT_ACKNOWLEDGEMENT_KEY = "IBKR_ALLOW_PAPER_PORT_IN_PRODUCTION"


def _paper_port_acknowledged(values: dict[str, str]) -> bool:
    return (values.get(PAPER_PORT_ACKNOWLEDGEMENT_KEY) or "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


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


def bootstrap_env_path(*, production: bool = False) -> Path:
    """Resolve the bootstrap file path, honouring the override variable.

    In production the override must already be absolute. ``expanduser()`` would
    otherwise turn ``~/bootstrap.env`` into an absolute path *before* the
    absolute-path check, so the home-relative form that check exists to reject
    would sail through and point production configuration at a user-writable
    file.
    """
    configured = getenv(BOOTSTRAP_ENV_PATH_VAR, "").strip()
    if not configured:
        return DEFAULT_BOOTSTRAP_ENV_PATH
    if production and not Path(configured).is_absolute():
        raise BootstrapConfigurationError(
            f"{BOOTSTRAP_ENV_PATH_VAR}={configured!r} is not an absolute path. "
            "In production the protected environment must be identified by "
            "absolute path so it cannot be relocated by the process's working "
            "directory or home directory."
        )
    return Path(configured).expanduser()


def _parse_env_text(text: str) -> dict[str, str]:
    """Parse ``KEY=value`` lines, ignoring blanks, comments and ``export``.

    On a duplicated key the **first** occurrence wins, matching
    ``load_dotenv_file``'s ``environ.setdefault``. This is not cosmetic: when
    this parse resolved duplicates last-wins while the apply step resolved them
    first-wins, a file could be inspected as ``APP_ENV=dev`` and applied as
    ``APP_ENV=production``, so the pre-check passed on a value that was never
    the one used.
    """
    # Imported lazily to keep config.py as the single owner of the line format.
    from ibkr_trader.config import _parse_env_line

    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        parsed = _parse_env_line(raw_line)
        if parsed is None:
            continue
        key, value = parsed
        values.setdefault(key, value)
    return values


def _assert_cannot_declare_production(
    values: dict[str, str], path: Path, kind: str
) -> None:
    """Refuse a file that escalates ``APP_ENV`` into production.

    Applies to both the checkout ``.env`` and the bootstrap file when the gate
    has *not* engaged. Selecting production is what decides whether the
    protected file is read and validated at all, so a file that is applied
    before that decision must not be able to make it. Otherwise the file's own
    keys land in the environment unvalidated - no required-key check, no
    paper-port check - and later readers of ``APP_ENV`` see production.
    """
    declared = values.get("APP_ENV")
    if declared is None:
        return
    if not _looks_like_production(declared):
        return
    raise BootstrapConfigurationError(
        f"{path} sets APP_ENV={declared!r}, but a {kind} must not declare "
        "production. Production is selected by the service environment "
        "(systemd Environment=), so that it cannot be redefined by a file that "
        "is loaded before the decision to enforce production has been made. "
        "Remove APP_ENV from that file."
    )


def _strip_value_noise(value: str) -> str:
    """Remove trailing comments, quotes and punctuation from an ``APP_ENV`` value."""
    cleaned = (value or "").strip()
    # `APP_ENV=production # live`: '#' does not start a comment mid-value (that
    # would corrupt passwords containing '#'), so the comment is part of the value.
    if "#" in cleaned:
        cleaned = cleaned.split("#", 1)[0].strip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {'"', "'"}:
        cleaned = cleaned[1:-1].strip()
    return cleaned.strip(";,.").strip().lower()


def _looks_like_production(value: str) -> bool:
    """Whether a value is a *malformed attempt* to select production.

    ``APP_ENV=production # live`` equals no production alias, so plain equality
    silently degrades it to development: the operator believes production is on
    while the gate quietly disengages.

    The test is deliberately narrow - it asks whether the value becomes an exact
    alias once comment, quote and punctuation noise is removed. An earlier
    substring test was too blunt: ``nonprod``, ``preprod``, ``non-production``
    and ``prod-eu`` all contain a production token while being legitimately
    non-production, and refusing them would have turned a safety check into a
    self-inflicted startup outage.

    Consequence worth knowing: a name like ``prod-eu`` is *not* treated as
    production. Production is exactly the values in
    :data:`PRODUCTION_ENVIRONMENTS`; anything else is development, by design.
    """
    raw = (value or "").strip().lower()
    if raw in PRODUCTION_ENVIRONMENTS:
        return True
    return _strip_value_noise(value) in PRODUCTION_ENVIRONMENTS


def _bootstrap_path_exists(path: Path) -> bool | None:
    """Whether the bootstrap file exists, or ``None`` if that is unknowable.

    ``Path.exists()`` does not swallow ``EACCES``: when the service user cannot
    traverse the parent directory it raises rather than returning ``False``.
    That is a third answer - "cannot tell" - and it must be distinguished from
    "absent", because the two need different handling in each branch.
    """
    try:
        return path.exists()
    except OSError:
        return None


def _describe_access_failure(path: Path, exc: OSError) -> str:
    return (
        f"Bootstrap environment file {path} could not be accessed: {exc}. The "
        f"service runs as uid {os.geteuid()}, which must be able to traverse "
        f"{path.parent} and read the file. Check ownership and mode on both "
        "the directory and the file."
    )


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

    # Group *read* is allowed so a service group can share the file. Group
    # *write* is not: anyone in that group could redirect DATABASE_URL to
    # another database, or change IBKR_PORT, without touching this code.
    if mode & stat.S_IWGRP:
        raise BootstrapConfigurationError(
            f"Bootstrap environment file {path} is group-writable "
            f"(mode {stat.filemode(mode)}). Anyone in that group could "
            "redirect the ledger database or the Gateway port. Fix with: "
            f"chmod g-w {path}"
        )


def _read_bootstrap_file(path: Path) -> dict[str, str]:
    try:
        exists = path.exists()
    except OSError as exc:
        raise BootstrapConfigurationError(
            _describe_access_failure(path, exc)
        ) from exc

    if not exists:
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

    # Shape, not just presence. Without this an unparseable port survives to a
    # bare ValueError from int() much later, defeating report-everything-at-once.
    raw_port = (values.get("IBKR_PORT") or "").strip()
    port: int | None = None
    if raw_port:
        try:
            port = int(raw_port)
        except ValueError:
            problems.append(
                f"IBKR_PORT is not an integer ({raw_port!r}); note that a "
                "trailing '# comment' is part of the value, not a comment"
            )

    # Presence of the key was never the point: 7497/7496 are the paper ports,
    # and copying values across from .env is the most likely way this file gets
    # populated. Refusing here is narrow and explicitly overridable.
    if port in PAPER_TRADING_PORTS and not _paper_port_acknowledged(values):
        problems.append(
            f"IBKR_PORT={port} is an IBKR paper-trading port. If that is "
            f"genuinely intended in production, set "
            f"{PAPER_PORT_ACKNOWLEDGEMENT_KEY}=1 in the same file to record "
            "the decision"
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
    from ibkr_trader.config import DEFAULT_ENV_FILE

    resolved_environment = (
        environment if environment is not None else getenv("APP_ENV", "dev")
    )
    production = is_production_environment(resolved_environment)
    # The override is read from the same ambient environment this design treats
    # as untrustworthy, so in production it is constrained to absolute paths.
    path = bootstrap_path or bootstrap_env_path(production=production)

    if production:
        if not path.is_absolute():
            raise BootstrapConfigurationError(
                f"Bootstrap path {str(path)!r} is not absolute. In production "
                "the protected environment must be identified by absolute path."
            )

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

        # Key names only; the payload is value-free by construction. Without
        # this a production start leaves no trace of which file was read or
        # which ambient values it replaced.
        LOGGER.info(
            "Production bootstrap loaded from %s: %d key(s) applied", path, len(values)
        )
        if overridden:
            LOGGER.warning(
                "Bootstrap file overrode ambient environment values for: %s",
                ", ".join(overridden),
            )

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

    # Both candidate files are parsed and vetted BEFORE anything is applied.
    # The gate has already declined to engage, so neither file may now declare
    # production: doing so would put its keys into the environment without the
    # required-key, paper-port or permission checks ever running, while later
    # readers of APP_ENV saw production. Vetting first also means a rejected
    # file leaves os.environ untouched.
    #
    # Each file is parsed exactly once and that same dict is what gets applied,
    # so the value inspected is always the value used.
    resolved_dotenv = dotenv_path or DEFAULT_ENV_FILE

    bootstrap_values: dict[str, str] = {}
    exists = _bootstrap_path_exists(path)
    if exists is None:
        # Unreadable /etc is not fatal here: the file is documented as optional
        # outside production, and a dev box or CI runner must still start.
        warnings.append(
            f"Bootstrap environment file {path} could not be accessed; "
            "continuing without it because it is optional outside production."
        )
    elif exists:
        try:
            bootstrap_values = _read_bootstrap_file(path)
        except BootstrapConfigurationError as exc:
            # Outside production a malformed/ill-permissioned file is
            # informational, not fatal.
            warnings.append(str(exc))
        else:
            _assert_cannot_declare_production(
                bootstrap_values, path, "bootstrap file loaded outside production"
            )

    dotenv_values: dict[str, str] = {}
    if _bootstrap_path_exists(resolved_dotenv):
        dotenv_values = _parse_env_text(
            resolved_dotenv.read_text(encoding="utf-8")
        )
        _assert_cannot_declare_production(
            dotenv_values, resolved_dotenv, "checkout-local .env"
        )

    # An ambient APP_ENV that looks production must not reach here at all: the
    # caller resolved a non-production environment, so a production-looking
    # ambient value means it was malformed (e.g. "production # live") and
    # silently degraded. Refusing is the only safe reading.
    if _looks_like_production(resolved_environment):
        raise BootstrapConfigurationError(
            f"APP_ENV={resolved_environment!r} is not a recognised environment "
            "but looks like an attempt to select production. Production "
            f"requires APP_ENV to be exactly one of {sorted(PRODUCTION_ENVIRONMENTS)}; "
            "a trailing comment or stray character is part of the value."
        )

    if bootstrap_values:
        for key, value in bootstrap_values.items():
            environ.setdefault(key, value)
        applied.extend(bootstrap_values)
        source = "bootstrap"

    if dotenv_values:
        for key, value in dotenv_values.items():
            environ.setdefault(key, value)
        source = "bootstrap+dotenv" if source == "bootstrap" else "dotenv"

    effective_environment = getenv("APP_ENV", resolved_environment)

    # Invariant: after this call os.environ["APP_ENV"] always agrees with the
    # returned environment. Every other read of APP_ENV in the codebase depends
    # on it, and the split-brain state it prevents (result says dev, environment
    # says production) is how the paper port was reached.
    environ["APP_ENV"] = effective_environment

    if _bootstrap_path_exists(path) is not True:
        path = None

    LOGGER.info(
        "Runtime environment resolved: environment=%s source=%s path=%s",
        effective_environment,
        source,
        path,
    )

    return BootstrapLoadResult(
        environment=effective_environment,
        is_production=False,
        source=source,
        path=path,
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
