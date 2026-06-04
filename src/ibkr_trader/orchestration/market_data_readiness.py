from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from time import sleep as _sleep
from typing import Any
from typing import Callable
from typing import Mapping

from ibkr_trader.domain.execution_payloads import parse_execution_instruction_payload
from ibkr_trader.ibkr.market_stream import MarketStreamContract


@dataclass(slots=True)
class MarketDataReadiness:
    """Decision data gate result used before broker entry submission."""

    ready: bool
    symbol: str | None
    reason: str
    evidence: dict[str, Any]

    def as_payload(self) -> dict[str, Any]:
        return {
            "ready": self.ready,
            "symbol": self.symbol,
            "reason": self.reason,
            "evidence": self.evidence,
        }


MarketDataReadinessChecker = Callable[
    [str, Mapping[str, Any], datetime],
    MarketDataReadiness | Mapping[str, Any] | bool,
]


def build_market_stream_readiness_checker(
    market_stream_service: Any,
    *,
    max_age_seconds: float,
    market_data_type: str | None = None,
    first_data_wait_seconds: float = 8.0,
    first_data_poll_seconds: float = 0.5,
    sleep_fn: Callable[[float], None] = _sleep,
) -> MarketDataReadinessChecker:
    """Build a policy-entry gate backed by the live market-stream service."""

    max_age_seconds = max(0.0, float(max_age_seconds))
    first_data_wait_seconds = max(0.0, float(first_data_wait_seconds))
    first_data_poll_seconds = max(0.1, float(first_data_poll_seconds))

    def check(
        instruction_id: str,
        instruction_payload: Mapping[str, Any],
        cycle_started_at: datetime,
    ) -> MarketDataReadiness:
        try:
            raw_instruction = instruction_payload.get("instruction")
            if not isinstance(raw_instruction, Mapping):
                return _not_ready(
                    symbol=None,
                    reason="instruction_payload_missing_instruction",
                    evidence={"instruction_id": instruction_id},
                )
            instruction = parse_execution_instruction_payload(raw_instruction)
            contract = _contract_from_instruction(instruction)
            stream_snapshot = _subscribe_and_snapshot(
                market_stream_service,
                contract,
                market_data_type=market_data_type,
            )
            readiness = evaluate_market_stream_snapshot(
                stream_snapshot,
                symbol=contract.key,
                reference_at=cycle_started_at,
                max_age_seconds=max_age_seconds,
            )
            if readiness.reason != "market_stream_has_no_quote_or_bar":
                return readiness

            # A fresh subscription often needs a few seconds before IBKR sends
            # the first quote or trade. Wait briefly so the first due runtime
            # cycle can submit after the first tick instead of logging a false
            # active failure and waiting for the next cycle.
            attempts = int(first_data_wait_seconds / first_data_poll_seconds)
            for _ in range(attempts):
                sleep_fn(first_data_poll_seconds)
                stream_snapshot = market_stream_service.snapshot(
                    symbols=[contract.key],
                    bar_limit=1,
                )
                readiness = evaluate_market_stream_snapshot(
                    stream_snapshot,
                    symbol=contract.key,
                    reference_at=cycle_started_at,
                    max_age_seconds=max_age_seconds,
                )
                if readiness.reason != "market_stream_has_no_quote_or_bar":
                    return readiness
            return readiness
        except Exception as exc:
            return _not_ready(
                symbol=_payload_symbol(instruction_payload),
                reason="market_stream_readiness_check_failed",
                evidence={
                    "instruction_id": instruction_id,
                    "error": str(exc),
                },
            )

    return check


def normalize_market_data_readiness(
    result: MarketDataReadiness | Mapping[str, Any] | bool,
) -> dict[str, Any]:
    """Normalize checker output into the persisted runtime event payload."""

    if isinstance(result, MarketDataReadiness):
        payload = result.as_payload()
    elif isinstance(result, Mapping):
        payload = dict(result)
    else:
        payload = {
            "ready": bool(result),
            "reason": "checker_returned_boolean",
            "evidence": {},
        }
    payload["ready"] = bool(payload.get("ready"))
    if payload.get("reason") in (None, ""):
        payload["reason"] = "market_stream_ready" if payload["ready"] else "unknown"
    evidence = payload.get("evidence")
    if not isinstance(evidence, Mapping):
        payload["evidence"] = {}
    return payload


def evaluate_market_stream_snapshot(
    stream_snapshot: Mapping[str, Any],
    *,
    symbol: str,
    reference_at: datetime,
    max_age_seconds: float,
) -> MarketDataReadiness:
    """Evaluate whether a symbol has a live stream subscription and fresh data."""

    normalized_symbol = symbol.strip().upper()
    reference_at = _ensure_utc(reference_at)
    subscription = _find_subscription(stream_snapshot, normalized_symbol)
    quote = _find_quote(stream_snapshot, normalized_symbol)
    latest_bar = _latest_bar(stream_snapshot, normalized_symbol)
    latest_times = [
        item
        for item in (
            _parse_timestamp((quote or {}).get("last_trade_at")),
            _parse_timestamp((quote or {}).get("updated_at")),
            _parse_timestamp((latest_bar or {}).get("timestamp")),
        )
        if item is not None
    ]
    latest_market_data_at = max(latest_times, default=None)
    age_seconds = (
        max(0, int((reference_at - latest_market_data_at).total_seconds()))
        if latest_market_data_at is not None
        else None
    )

    evidence = {
        "reference_at": reference_at.isoformat(),
        "required_max_age_seconds": int(max_age_seconds),
        "latest_market_data_at": (
            latest_market_data_at.isoformat()
            if latest_market_data_at is not None
            else None
        ),
        "latest_market_data_age_seconds": age_seconds,
        "stream_running": bool(stream_snapshot.get("running")),
        "stream_last_error": stream_snapshot.get("last_error"),
        "desired_symbols": list(stream_snapshot.get("desired_symbols") or []),
        "subscription": subscription,
        "quote": quote,
        "latest_bar": latest_bar,
    }

    if not stream_snapshot.get("running"):
        return _not_ready(
            symbol=normalized_symbol,
            reason="market_stream_not_running",
            evidence=evidence,
        )
    if subscription is None:
        return _not_ready(
            symbol=normalized_symbol,
            reason="market_stream_not_subscribed",
            evidence=evidence,
        )
    if str(subscription.get("status") or "").lower() == "error":
        return _not_ready(
            symbol=normalized_symbol,
            reason="market_stream_subscription_error",
            evidence=evidence,
        )
    if latest_market_data_at is None:
        return _not_ready(
            symbol=normalized_symbol,
            reason="market_stream_has_no_quote_or_bar",
            evidence=evidence,
        )
    if age_seconds is not None and age_seconds > max_age_seconds:
        return _not_ready(
            symbol=normalized_symbol,
            reason="market_stream_data_stale",
            evidence=evidence,
        )
    return MarketDataReadiness(
        ready=True,
        symbol=normalized_symbol,
        reason="market_stream_ready",
        evidence=evidence,
    )


def _contract_from_instruction(instruction: Any) -> MarketStreamContract:
    instrument = instruction.instrument
    primary_exchange = instrument.primary_exchange
    security_type = str(instrument.security_type.value)
    return MarketStreamContract(
        symbol=str(instrument.symbol).strip().upper(),
        exchange=str(instrument.exchange or "SMART").strip().upper(),
        currency=str(instrument.currency or "SEK").strip().upper(),
        security_type=security_type,
        primary_exchange=(
            str(primary_exchange).strip().upper()
            if primary_exchange not in (None, "")
            else ("SFB" if security_type == "STK" else None)
        ),
        isin=instrument.isin,
    )


def _subscribe_and_snapshot(
    market_stream_service: Any,
    contract: MarketStreamContract,
    *,
    market_data_type: str | None,
) -> Mapping[str, Any]:
    if hasattr(market_stream_service, "subscribe_many"):
        market_stream_service.subscribe_many(
            [contract],
            replace=False,
            market_data_type=market_data_type,
        )
    else:
        market_stream_service.set_desired_many(
            [contract],
            replace=False,
            market_data_type=market_data_type,
        )
    return market_stream_service.snapshot(symbols=[contract.key], bar_limit=1)


def _not_ready(
    *,
    symbol: str | None,
    reason: str,
    evidence: Mapping[str, Any],
) -> MarketDataReadiness:
    return MarketDataReadiness(
        ready=False,
        symbol=symbol.strip().upper() if symbol else None,
        reason=reason,
        evidence=dict(evidence),
    )


def _payload_symbol(payload: Mapping[str, Any]) -> str | None:
    raw_instruction = payload.get("instruction")
    if not isinstance(raw_instruction, Mapping):
        return None
    raw_instrument = raw_instruction.get("instrument")
    if not isinstance(raw_instrument, Mapping):
        return None
    symbol = raw_instrument.get("symbol")
    return str(symbol).strip().upper() if symbol not in (None, "") else None


def _find_subscription(
    stream_snapshot: Mapping[str, Any],
    symbol: str,
) -> dict[str, Any] | None:
    for subscription in stream_snapshot.get("subscriptions") or []:
        if not isinstance(subscription, Mapping):
            continue
        contract = subscription.get("contract")
        if isinstance(contract, Mapping) and _matches_symbol(
            contract.get("symbol"),
            symbol,
        ):
            return dict(subscription)
    return None


def _find_quote(
    stream_snapshot: Mapping[str, Any],
    symbol: str,
) -> dict[str, Any] | None:
    for quote in stream_snapshot.get("quotes") or []:
        if not isinstance(quote, Mapping):
            continue
        if _matches_symbol(quote.get("symbol"), symbol):
            return dict(quote)
    return None


def _latest_bar(
    stream_snapshot: Mapping[str, Any],
    symbol: str,
) -> dict[str, Any] | None:
    bars_by_symbol = stream_snapshot.get("bars_by_symbol")
    if not isinstance(bars_by_symbol, Mapping):
        return None
    bars = bars_by_symbol.get(symbol)
    if bars is None and " " in symbol:
        bars = bars_by_symbol.get(symbol.replace(" ", "-"))
    if bars is None and "-" in symbol:
        bars = bars_by_symbol.get(symbol.replace("-", " "))
    if not isinstance(bars, list) or not bars:
        return None
    latest = bars[-1]
    return dict(latest) if isinstance(latest, Mapping) else None


def _matches_symbol(value: Any, symbol: str) -> bool:
    if value in (None, ""):
        return False
    normalized = str(value).strip().upper()
    return normalized in {
        symbol,
        symbol.replace(" ", "-"),
        symbol.replace("-", " "),
    }


def _parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
