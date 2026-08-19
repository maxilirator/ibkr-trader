"""Typed, non-secret runtime settings registry.

Operational knobs currently live only as environment variables read through
``config.py``. That makes them invisible at runtime: an operator cannot see what
the running process actually resolved without reading the unit file, the
protected bootstrap file and the checkout ``.env``, and reconciling three
sources by hand.

This registry declares those knobs as typed definitions, resolves them against a
PostgreSQL table, and reports the effective value together with **where it came
from**. It is deliberately read-only: no application code path writes a setting,
so the registry cannot itself become a way to change trading behaviour.

Two hard rules:

* **Non-secret only.** Secrets stay in ``/etc/ibkr-trader/bootstrap.env``. A
  definition whose key looks secret is rejected at import time, so the split
  cannot erode as settings are added later. See :func:`assert_not_secret_key`.
* **No silent defaults over bad data.** A stored value that does not parse as its
  declared type is reported as an error against that setting, not quietly
  replaced by the default. An operator reading the dashboard must not be shown a
  value the runtime is not actually using.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from ibkr_trader.db.base import session_scope
from ibkr_trader.db.models import RuntimeSettingRecord


class SettingType(StrEnum):
    """Declared type of a setting value."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"


#: Substrings that mark a key as secret-bearing. Matching is on the key name, so
#: this is a structural guard rather than a content check: it prevents a secret
#: from ever being *declared* as a database-backed setting.
SECRET_KEY_MARKERS = (
    "PASSWORD",
    "SECRET",
    "TOKEN",
    "CREDENTIAL",
    "PRIVATE_KEY",
    "API_KEY",
    "DATABASE_URL",
    "DSN",
)


class SettingsRegistryError(RuntimeError):
    """Raised when the settings registry is misdeclared or unreadable."""


def assert_not_secret_key(key: str) -> None:
    """Reject a key that looks like it carries a secret.

    Enforced at import time for every declared setting. The database is for
    non-secret, audited operational settings; secrets live in the protected
    bootstrap file. Documenting that split is not enough - without a check it
    erodes the first time someone adds a convenient "just one" credential.
    """
    upper = key.upper()
    for marker in SECRET_KEY_MARKERS:
        if marker in upper:
            raise SettingsRegistryError(
                f"Setting {key!r} looks secret-bearing (matched {marker!r}). "
                "Secrets belong in /etc/ibkr-trader/bootstrap.env, not in the "
                "PostgreSQL settings registry, which is non-secret and audited."
            )


@dataclass(frozen=True, slots=True)
class SettingDefinition:
    """A declared, typed, non-secret runtime setting."""

    key: str
    value_type: SettingType
    default: Any
    description: str
    category: str

    def parse(self, raw: str) -> Any:
        """Parse a stored string into the declared type.

        Raises:
            ValueError: If ``raw`` is not a valid value for this type. Callers
                surface the error rather than substituting the default.
        """
        text = raw.strip()
        if self.value_type is SettingType.STRING:
            return text
        if self.value_type is SettingType.INTEGER:
            return int(text)
        if self.value_type is SettingType.FLOAT:
            return float(text)
        if self.value_type is SettingType.BOOLEAN:
            lowered = text.lower()
            if lowered in {"1", "true", "yes", "on"}:
                return True
            if lowered in {"0", "false", "no", "off"}:
                return False
            raise ValueError(f"{text!r} is not a boolean")
        raise ValueError(f"unsupported setting type {self.value_type}")


def _define(
    key: str,
    value_type: SettingType,
    default: Any,
    description: str,
    category: str,
) -> SettingDefinition:
    assert_not_secret_key(key)
    return SettingDefinition(
        key=key,
        value_type=value_type,
        default=default,
        description=description,
        category=category,
    )


#: The declared registry. Keys mirror the environment variables ``config.py``
#: reads, so an operator can map a dashboard row to the variable that sets it.
SETTING_DEFINITIONS: tuple[SettingDefinition, ...] = (
    _define(
        "MARKET_STREAM_AUTO_RECONNECT_ENABLED",
        SettingType.BOOLEAN,
        True,
        "Whether the market stream supervisor runs. With this off a dropped "
        "stream never recovers without operator action.",
        "market-stream",
    ),
    _define(
        "MARKET_STREAM_STALE_AFTER_SECONDS",
        SettingType.FLOAT,
        180.0,
        "Age at which stream data counts as stale. Non-positive disables "
        "staleness detection entirely, which makes freshness unverifiable.",
        "market-stream",
    ),
    _define(
        "MARKET_STREAM_STALE_RECONNECT_ENABLED",
        SettingType.BOOLEAN,
        True,
        "Whether a connected-but-stale stream is recycled.",
        "market-stream",
    ),
    _define(
        "MARKET_STREAM_RECONNECT_INTERVAL_SECONDS",
        SettingType.FLOAT,
        15.0,
        "Supervisor poll interval for stream reconnection.",
        "market-stream",
    ),
    _define(
        "MARKET_STREAM_MAX_SUBSCRIPTIONS",
        SettingType.INTEGER,
        120,
        "Cap on concurrent stream subscriptions, further limited by the IBKR "
        "market-data line limit.",
        "market-stream",
    ),
    _define(
        "IBKR_MARKET_DATA_LINE_LIMIT",
        SettingType.INTEGER,
        80,
        "IBKR account market-data line entitlement.",
        "market-stream",
    ),
    _define(
        "BROKER_MONITOR_ENABLED",
        SettingType.BOOLEAN,
        True,
        "Whether the background broker monitor cycles.",
        "broker",
    ),
    _define(
        "BROKER_HEARTBEAT_INTERVAL_SECONDS",
        SettingType.FLOAT,
        30.0,
        "Diagnostic heartbeat interval against the Gateway.",
        "broker",
    ),
    _define(
        "BROKER_SNAPSHOT_REFRESH_INTERVAL_SECONDS",
        SettingType.FLOAT,
        60.0,
        "Account/position snapshot refresh interval.",
        "broker",
    ),
    _define(
        "BROKER_CONNECT_BACKOFF_INITIAL_SECONDS",
        SettingType.FLOAT,
        5.0,
        "Initial backoff after a failed broker connect.",
        "broker",
    ),
    _define(
        "BROKER_CONNECT_BACKOFF_MAX_SECONDS",
        SettingType.FLOAT,
        300.0,
        "Maximum backoff between broker connect attempts.",
        "broker",
    ),
    _define(
        "IBKR_API_MAX_REQUESTS_PER_SECOND",
        SettingType.FLOAT,
        45.0,
        "Client-side IBKR API pacing ceiling.",
        "broker",
    ),
    _define(
        "MARKET_DATA_BACKFILL_WORKER_ENABLED",
        SettingType.BOOLEAN,
        True,
        "Whether the historical backfill worker runs.",
        "market-data",
    ),
    _define(
        "MARKET_DATA_BACKFILL_INTERVAL_SECONDS",
        SettingType.FLOAT,
        60.0,
        "Backfill worker poll interval.",
        "market-data",
    ),
    _define(
        "MARKET_DATA_BACKFILL_BATCH_SIZE",
        SettingType.INTEGER,
        3,
        "Backfill requests processed per cycle.",
        "market-data",
    ),
    _define(
        "EXECUTION_RUNTIME_ENABLED",
        SettingType.BOOLEAN,
        False,
        "Whether the execution runtime submits instructions. Off by default; "
        "turning it on is a trading-behaviour change.",
        "execution",
    ),
    _define(
        "EXECUTION_RUNTIME_INTERVAL_SECONDS",
        SettingType.FLOAT,
        5.0,
        "Execution runtime cycle interval.",
        "execution",
    ),
    _define(
        "RL_OBSERVED_BAR_MIN_COVERAGE_RATIO",
        SettingType.FLOAT,
        0.8,
        "Minimum observed-bar coverage before an RL decision is accepted.",
        "rl",
    ),
)

SETTING_DEFINITIONS_BY_KEY: dict[str, SettingDefinition] = {
    definition.key: definition for definition in SETTING_DEFINITIONS
}


@dataclass(frozen=True, slots=True)
class ResolvedSetting:
    """A declared setting resolved against the database."""

    key: str
    value_type: SettingType
    description: str
    category: str
    default_value: Any
    effective_value: Any
    #: "default" when no database row exists, "database" when one does.
    source: str
    updated_by: str | None
    updated_at_iso: str | None
    note: str | None
    #: Set when a stored value could not be parsed. The effective value then
    #: falls back to the default, and this says so explicitly rather than
    #: presenting a value the runtime is not using.
    error: str | None

    def to_payload(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "value_type": str(self.value_type),
            "description": self.description,
            "category": self.category,
            "default_value": self.default_value,
            "effective_value": self.effective_value,
            "source": self.source,
            "updated_by": self.updated_by,
            "updated_at": self.updated_at_iso,
            "note": self.note,
            "error": self.error,
        }


def _resolve(
    definition: SettingDefinition, record: RuntimeSettingRecord | None
) -> ResolvedSetting:
    if record is None:
        return ResolvedSetting(
            key=definition.key,
            value_type=definition.value_type,
            description=definition.description,
            category=definition.category,
            default_value=definition.default,
            effective_value=definition.default,
            source="default",
            updated_by=None,
            updated_at_iso=None,
            note=None,
            error=None,
        )

    error: str | None = None
    try:
        effective = definition.parse(record.value)
    except ValueError as exc:
        effective = definition.default
        error = (
            f"Stored value {record.value!r} is not a valid "
            f"{definition.value_type} ({exc}); the runtime uses the default."
        )

    return ResolvedSetting(
        key=definition.key,
        value_type=definition.value_type,
        description=definition.description,
        category=definition.category,
        default_value=definition.default,
        effective_value=effective,
        source="database",
        updated_by=record.updated_by,
        updated_at_iso=(
            record.updated_at.isoformat() if record.updated_at is not None else None
        ),
        note=record.note,
        error=error,
    )


@dataclass(frozen=True, slots=True)
class SettingsRegistrySnapshot:
    """Every declared setting resolved, plus stored rows nobody declares."""

    settings: tuple[ResolvedSetting, ...]
    #: Rows present in the database with no matching definition. Surfaced rather
    #: than ignored: such a row is either a typo or a setting removed from the
    #: code, and in both cases an operator is looking at a value that affects
    #: nothing. Silently dropping it would make the dashboard misleading.
    undeclared_keys: tuple[str, ...]


def read_settings_registry(
    session_factory: sessionmaker[Session],
) -> SettingsRegistrySnapshot:
    """Resolve every declared setting against the database.

    Read-only. Settings with no stored row resolve to their declared default and
    report ``source="default"``, which is a real answer rather than a guess: the
    runtime genuinely uses the default in that case.
    """
    with session_scope(session_factory) as session:
        records = {
            record.setting_key: record
            for record in session.execute(select(RuntimeSettingRecord)).scalars()
        }

    settings = tuple(
        _resolve(definition, records.get(definition.key))
        for definition in SETTING_DEFINITIONS
    )
    undeclared = tuple(
        sorted(key for key in records if key not in SETTING_DEFINITIONS_BY_KEY)
    )
    return SettingsRegistrySnapshot(settings=settings, undeclared_keys=undeclared)


def serialize_settings_registry(
    snapshot: SettingsRegistrySnapshot,
) -> dict[str, Any]:
    """Build the operator API payload for the settings registry."""
    return {
        "settings": [setting.to_payload() for setting in snapshot.settings],
        "categories": sorted({setting.category for setting in snapshot.settings}),
        "error_count": sum(1 for setting in snapshot.settings if setting.error),
        "undeclared_keys": list(snapshot.undeclared_keys),
        "read_only": True,
    }
