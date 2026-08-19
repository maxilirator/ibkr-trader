"""Named recovery and stream states for the IBKR broker and market stream.

Before this module, broker and stream health lived as loose booleans and counters
spread across ``session_manager``, ``broker_circuit`` and ``market_stream``:
``connected``, ``consecutive_failures``, ``cooldown_until``, ``is_stale``,
``open``. Operators and callers each re-derived "is it actually usable right now"
from those primitives, and each did it slightly differently.

This module defines the single authoritative classification. Two things matter:

1. The states are *named*, so logs, the dashboard and alerts agree on vocabulary.
2. Each state carries the operational authority it implies, so callers ask
   ``policy.allows_new_entries`` rather than re-deriving intent from counters.

Every classifier here is a pure function of a snapshot. There is no I/O and no
clock access, so state transitions are directly testable and a given snapshot
always classifies identically.

Unresolvable input is classified ``UNKNOWN``, never "healthy". Per the repository
rule against silent fallbacks, an unknown broker state withholds every authority:
we would rather stop trading on a classification gap than trade on a guess.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class BrokerRecoveryState(StrEnum):
    """Named broker-connection recovery states.

    Ordered loosely from most to least usable. The string values are stable
    contract surface: they appear in the operator dashboard, structured logs and
    alert payloads, so renaming one is a breaking change.
    """

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    RECOVERING = "recovering"
    BLOCKED = "blocked"
    DOWN = "down"
    MAINTENANCE = "maintenance"
    UNKNOWN = "unknown"


class StreamState(StrEnum):
    """Named market-data stream states."""

    STREAMING = "streaming"
    IDLE = "idle"
    STALE = "stale"
    RECONNECTING = "reconnecting"
    BLOCKED = "blocked"
    STOPPED = "stopped"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class RecoveryAuthority:
    """What the runtime is permitted to do in a given broker state.

    Kept separate from the state enum so the authority table is data, not
    branching scattered through call sites.
    """

    allows_new_entries: bool
    allows_order_management: bool
    allows_market_data: bool
    requires_operator_attention: bool


#: Authority implied by each broker state.
#:
#: ``allows_order_management`` stays true in DEGRADED because cancelling or
#: flattening an existing position is exactly what an operator needs during a
#: partial outage; only *new* risk is withheld. In BLOCKED/DOWN/UNKNOWN the
#: broker is not reachable in a trustworthy way, so nothing is authorised.
_BROKER_AUTHORITY: Mapping[BrokerRecoveryState, RecoveryAuthority] = {
    BrokerRecoveryState.HEALTHY: RecoveryAuthority(
        allows_new_entries=True,
        allows_order_management=True,
        allows_market_data=True,
        requires_operator_attention=False,
    ),
    BrokerRecoveryState.DEGRADED: RecoveryAuthority(
        allows_new_entries=False,
        allows_order_management=True,
        allows_market_data=True,
        requires_operator_attention=True,
    ),
    BrokerRecoveryState.RECOVERING: RecoveryAuthority(
        allows_new_entries=False,
        allows_order_management=False,
        allows_market_data=False,
        requires_operator_attention=False,
    ),
    BrokerRecoveryState.BLOCKED: RecoveryAuthority(
        allows_new_entries=False,
        allows_order_management=False,
        allows_market_data=False,
        requires_operator_attention=True,
    ),
    BrokerRecoveryState.DOWN: RecoveryAuthority(
        allows_new_entries=False,
        allows_order_management=False,
        allows_market_data=False,
        requires_operator_attention=True,
    ),
    # Maintenance is an intentional operator state, not a fault: no attention flag.
    BrokerRecoveryState.MAINTENANCE: RecoveryAuthority(
        allows_new_entries=False,
        allows_order_management=False,
        allows_market_data=False,
        requires_operator_attention=False,
    ),
    BrokerRecoveryState.UNKNOWN: RecoveryAuthority(
        allows_new_entries=False,
        allows_order_management=False,
        allows_market_data=False,
        requires_operator_attention=True,
    ),
}


def broker_authority(state: BrokerRecoveryState) -> RecoveryAuthority:
    """Return the operational authority implied by ``state``."""
    return _BROKER_AUTHORITY[state]


@dataclass(frozen=True, slots=True)
class BrokerRecoveryAssessment:
    """A classified broker state plus the evidence that produced it."""

    state: BrokerRecoveryState
    reason: str
    authority: RecoveryAuthority

    def to_payload(self) -> dict[str, Any]:
        """Serialise for the operator API and structured logs."""
        return {
            "state": str(self.state),
            "reason": self.reason,
            "allows_new_entries": self.authority.allows_new_entries,
            "allows_order_management": self.authority.allows_order_management,
            "allows_market_data": self.authority.allows_market_data,
            "requires_operator_attention": self.authority.requires_operator_attention,
        }


@dataclass(frozen=True, slots=True)
class StreamAssessment:
    """A classified market-stream state plus the evidence that produced it."""

    state: StreamState
    reason: str

    @property
    def is_usable(self) -> bool:
        """Whether stream data may be treated as current enough to act on."""
        return self.state is StreamState.STREAMING

    def to_payload(self) -> dict[str, Any]:
        return {
            "state": str(self.state),
            "reason": self.reason,
            "is_usable": self.is_usable,
        }


def _assessment(
    state: BrokerRecoveryState, reason: str
) -> BrokerRecoveryAssessment:
    return BrokerRecoveryAssessment(
        state=state, reason=reason, authority=broker_authority(state)
    )


def classify_broker_recovery_state(
    *,
    connected: bool | None,
    consecutive_failures: int | None,
    cooldown_seconds_remaining: int | None = None,
    circuit_open: bool = False,
    circuit_reason: str | None = None,
    maintenance_mode: bool = False,
    degraded_failure_threshold: int = 1,
    down_failure_threshold: int = 3,
) -> BrokerRecoveryAssessment:
    """Classify broker connection health into a named recovery state.

    Precedence is deliberate and is the core of the policy:

    ``MAINTENANCE`` outranks everything because an operator declared it, and a
    maintenance window must not page anyone. ``BLOCKED`` (circuit breaker) then
    outranks connection state, because an open circuit means we have decided not
    to trust the broker even if the socket happens to be up.

    ``down_failure_threshold`` separates "retrying" from "given up": below it a
    disconnected session is ``RECOVERING`` (expected, self-healing, no page);
    at or above it the session is ``DOWN`` and needs a human.

    Args:
        connected: Whether the session reports an live connection. ``None`` means
            the caller could not establish it, which yields ``UNKNOWN``.
        consecutive_failures: Failure streak length. ``None`` yields ``UNKNOWN``.
        cooldown_seconds_remaining: Seconds until the next connect attempt is
            permitted, when a backoff cooldown is active.
        circuit_open: Whether the shared broker circuit breaker is open.
        circuit_reason: Operator-facing reason the circuit tripped.
        maintenance_mode: Whether the operator declared broker maintenance.
        degraded_failure_threshold: Failure streak at which a *connected* session
            is reported ``DEGRADED`` rather than ``HEALTHY``.
        down_failure_threshold: Failure streak at which a *disconnected* session
            is reported ``DOWN`` rather than ``RECOVERING``.

    Returns:
        The classified state, a concrete operator-facing reason, and the
        authority that state grants.
    """
    if maintenance_mode:
        return _assessment(
            BrokerRecoveryState.MAINTENANCE,
            "Operator enabled broker maintenance mode; broker use is suspended.",
        )

    if circuit_open:
        suffix = f" Reason: {circuit_reason}" if circuit_reason else ""
        return _assessment(
            BrokerRecoveryState.BLOCKED,
            f"Broker circuit breaker is open; broker calls are suppressed.{suffix}",
        )

    # Unresolvable input must never read as healthy. A negative failure count is
    # nonsense rather than zero, so it is unknown too: silently clamping it to 0
    # would turn a corrupt payload into HEALTHY.
    if connected is None or consecutive_failures is None or consecutive_failures < 0:
        return _assessment(
            BrokerRecoveryState.UNKNOWN,
            "Broker session state could not be established "
            f"(connected={connected!r}, consecutive_failures={consecutive_failures!r}).",
        )

    failures = int(consecutive_failures)

    if connected:
        if failures >= max(1, degraded_failure_threshold):
            return _assessment(
                BrokerRecoveryState.DEGRADED,
                f"Session is connected but has {failures} consecutive failure(s); "
                "new entries are withheld while it settles.",
            )
        return _assessment(
            BrokerRecoveryState.HEALTHY,
            "Session is connected with no recent failures.",
        )

    if failures >= max(1, down_failure_threshold):
        return _assessment(
            BrokerRecoveryState.DOWN,
            f"Session is disconnected after {failures} consecutive failure(s), "
            f"at or above the down threshold of {down_failure_threshold}.",
        )

    cooldown = cooldown_seconds_remaining or 0
    if cooldown > 0:
        return _assessment(
            BrokerRecoveryState.RECOVERING,
            f"Session is disconnected and waiting {cooldown}s of connect backoff "
            f"after {failures} failure(s).",
        )

    return _assessment(
        BrokerRecoveryState.RECOVERING,
        f"Session is disconnected and eligible to reconnect after {failures} failure(s).",
    )


def classify_stream_state(
    *,
    running: bool | None,
    subscription_count: int | None,
    latest_market_data_age_seconds: int | None,
    stale_after_seconds: int | None,
    is_stale: bool | None = None,
    cooldown_seconds_remaining: int | None = None,
    auto_reconnect_active: bool | None = None,
    staleness_detectable: bool | None = None,
) -> StreamAssessment:
    """Classify market-stream health into a named stream state.

    A stopped stream is only ``RECONNECTING`` when a supervisor is actually
    alive to reconnect it; otherwise it is ``STOPPED`` and will not recover on
    its own, which is a materially different operator situation and so gets its
    own name. Whether a supervisor is running is not knowable from config alone,
    so ``auto_reconnect_active`` must be observed; ``None`` yields ``UNKNOWN``
    rather than an optimistic assumption that something will fix it.

    A running stream with no subscriptions is ``IDLE``, not ``STALE``: no data is
    expected, so data age carries no signal and must not raise a false alarm.

    ``is_stale`` is treated as corroborating, not authoritative. The stream
    service returns ``False`` both for "data is fresh" and for "staleness
    detection is switched off", so trusting it alone would report a long-dead
    stream as ``STREAMING``. When staleness cannot be established the state is
    ``UNKNOWN``.

    Args:
        running: Whether the stream client is connected. ``None`` yields ``UNKNOWN``.
        subscription_count: Active subscription count. ``None`` yields ``UNKNOWN``.
        latest_market_data_age_seconds: Age of the newest tick, or ``None`` when
            no tick has ever arrived.
        stale_after_seconds: Age at which data counts as stale.
        is_stale: The stream service's own staleness verdict, if any.
        cooldown_seconds_remaining: Seconds until a reconnect is permitted.
        auto_reconnect_active: Whether a reconnect supervisor is alive.
        staleness_detectable: Whether the service has staleness detection on.
            ``None`` falls back to inferring it from ``stale_after_seconds``.

    Returns:
        The classified stream state and a concrete operator-facing reason.
    """
    if running is None or subscription_count is None:
        return StreamAssessment(
            StreamState.UNKNOWN,
            "Market stream state could not be established "
            f"(running={running!r}, subscription_count={subscription_count!r}).",
        )

    if not running:
        if auto_reconnect_active is None:
            return StreamAssessment(
                StreamState.UNKNOWN,
                "Market stream is not running and it could not be established "
                "whether a reconnect supervisor is active.",
            )
        if not auto_reconnect_active:
            return StreamAssessment(
                StreamState.STOPPED,
                "Market stream is not running and no reconnect supervisor is "
                "active; it will not recover without operator action.",
            )
        cooldown = cooldown_seconds_remaining or 0
        if cooldown > 0:
            return StreamAssessment(
                StreamState.BLOCKED,
                f"Market stream is not running and is held off for {cooldown}s "
                "of reconnect cooldown.",
            )
        return StreamAssessment(
            StreamState.RECONNECTING,
            "Market stream is not running and is eligible to reconnect.",
        )

    subscriptions = max(0, int(subscription_count))
    if subscriptions == 0:
        return StreamAssessment(
            StreamState.IDLE,
            "Market stream is running with no active subscriptions; "
            "no market data is expected.",
        )

    if is_stale:
        age = latest_market_data_age_seconds
        age_text = f"{age}s old" if age is not None else "never received"
        return StreamAssessment(
            StreamState.STALE,
            f"Market stream reports stale data across {subscriptions} "
            f"subscription(s); newest tick is {age_text}.",
        )

    if latest_market_data_age_seconds is None:
        # Subscribed but nothing has ever arrived: not yet trustworthy.
        return StreamAssessment(
            StreamState.STALE,
            f"Market stream is running with {subscriptions} subscription(s) but "
            "has never received a tick.",
        )

    has_threshold = stale_after_seconds is not None and stale_after_seconds > 0
    detectable = has_threshold if staleness_detectable is None else staleness_detectable

    if not detectable:
        # `is_stale=False` here means "not checked", not "fresh". Reporting
        # STREAMING would be a synthetic success state over a possibly dead feed.
        return StreamAssessment(
            StreamState.UNKNOWN,
            f"Market stream has {subscriptions} subscription(s) and its newest "
            f"tick is {latest_market_data_age_seconds}s old, but staleness "
            "detection is disabled, so freshness could not be established.",
        )

    # Applied whenever a threshold exists, independent of the service verdict.
    if has_threshold and latest_market_data_age_seconds >= stale_after_seconds:
        return StreamAssessment(
            StreamState.STALE,
            f"Newest tick is {latest_market_data_age_seconds}s old, at or beyond "
            f"the {stale_after_seconds}s staleness threshold.",
        )

    return StreamAssessment(
        StreamState.STREAMING,
        f"Market stream is running with {subscriptions} subscription(s); "
        f"newest tick is {latest_market_data_age_seconds}s old.",
    )


def _optional_int(payload: Mapping[str, Any], key: str) -> int | None:
    """Read an int field, treating absent/unparseable values as unresolved.

    Returning ``None`` rather than a default is what lets a malformed snapshot
    classify as ``UNKNOWN`` instead of silently reading as healthy.
    """
    value = payload.get(key)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_bool(payload: Mapping[str, Any], key: str) -> bool | None:
    value = payload.get(key)
    return value if isinstance(value, bool) else None


def assess_broker_session_payload(
    session_payload: Mapping[str, Any] | None,
    *,
    circuit_payload: Mapping[str, Any] | None = None,
    maintenance_mode: bool = False,
    degraded_failure_threshold: int = 1,
    down_failure_threshold: int = 3,
) -> BrokerRecoveryAssessment:
    """Classify a serialised managed-session snapshot.

    Adapts the dict shape produced by
    :func:`ibkr_trader.ibkr.session_manager.serialize_managed_session_status` and
    :meth:`ibkr_trader.ibkr.broker_circuit.BrokerHealthCircuit.snapshot` so
    callers do not have to unpack them by hand.
    """
    if session_payload is None:
        return _assessment(
            BrokerRecoveryState.UNKNOWN,
            "No broker session snapshot was available to classify.",
        )

    if session_payload.get("status_available") is False:
        # A non-blocking status() call that lost the lock race. Its `connected`
        # field is a last-known value, and the session is most likely busy
        # serving a broker call - reading it as "disconnected" would report a
        # working session as RECOVERING or DOWN.
        return _assessment(
            BrokerRecoveryState.UNKNOWN,
            "Broker session state could not be observed because the session "
            "lock was busy; the session is most likely serving a broker call.",
        )

    if maintenance_mode:
        return _assessment(
            BrokerRecoveryState.MAINTENANCE,
            "Operator enabled broker maintenance mode; broker use is suspended.",
        )

    # A per-session circuit breaker blocks that session even when the shared
    # circuit is closed. The shared circuit can be cleared by an unrelated
    # subsystem (a successful market-stream connect clears it), so relying on it
    # alone would report a circuit-broken primary session as merely RECOVERING.
    session_circuit_until = session_payload.get("circuit_breaker_until")
    session_circuit_reason = session_payload.get("circuit_breaker_reason")
    session_circuit_open = bool(session_circuit_until or session_circuit_reason)

    circuit = circuit_payload or {}
    return classify_broker_recovery_state(
        connected=_optional_bool(session_payload, "connected"),
        consecutive_failures=_optional_int(session_payload, "consecutive_failures"),
        cooldown_seconds_remaining=_optional_int(
            session_payload, "cooldown_seconds_remaining"
        ),
        circuit_open=bool(circuit.get("open", False)) or session_circuit_open,
        circuit_reason=session_circuit_reason or circuit.get("reason"),
        maintenance_mode=maintenance_mode,
        degraded_failure_threshold=degraded_failure_threshold,
        down_failure_threshold=down_failure_threshold,
    )


def assess_market_stream_payload(
    stream_payload: Mapping[str, Any] | None,
) -> StreamAssessment:
    """Classify a market-stream snapshot dict.

    Uses ``subscribed_count`` rather than the length of ``subscriptions``,
    because that list is filtered by the caller's requested symbols and would
    under-report an unfiltered view of the stream.
    """
    if stream_payload is None:
        return StreamAssessment(
            StreamState.UNKNOWN,
            "No market stream snapshot was available to classify.",
        )

    if stream_payload.get("status_available") is False:
        reason = stream_payload.get("unavailable_reason")
        return StreamAssessment(
            StreamState.UNKNOWN,
            str(reason)
            if reason
            else "Market stream state could not be observed.",
        )

    return classify_stream_state(
        running=_optional_bool(stream_payload, "running"),
        subscription_count=_optional_int(stream_payload, "subscribed_count"),
        latest_market_data_age_seconds=_optional_int(
            stream_payload, "latest_market_data_age_seconds"
        ),
        stale_after_seconds=_optional_int(stream_payload, "stale_after_seconds"),
        is_stale=_optional_bool(stream_payload, "is_stale"),
        cooldown_seconds_remaining=_optional_int(
            stream_payload, "cooldown_seconds_remaining"
        ),
        # Whether a supervisor is alive, not whether one was configured. Absent
        # means unresolved, never "something will fix it".
        auto_reconnect_active=_optional_bool(stream_payload, "auto_reconnect_active"),
        staleness_detectable=_optional_bool(stream_payload, "staleness_detectable"),
    )
