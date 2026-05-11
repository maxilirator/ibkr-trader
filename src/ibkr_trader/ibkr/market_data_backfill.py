from __future__ import annotations

import hashlib
import json
import logging
import threading
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any, Callable, Mapping, Sequence

from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import MarketDataBackfillRequestRecord
from ibkr_trader.ibkr.historical_bars import HistoricalBarsQuery
from ibkr_trader.ibkr.historical_bars import read_historical_bars
from ibkr_trader.ibkr.market_stream_store import persist_market_stream_bars

LOGGER = logging.getLogger(__name__)

BACKFILL_STATUS_PENDING = "PENDING"
BACKFILL_STATUS_RUNNING = "RUNNING"
BACKFILL_STATUS_SUCCEEDED = "SUCCEEDED"
BACKFILL_STATUS_FAILED_RETRYABLE = "FAILED_RETRYABLE"
BACKFILL_STATUS_FAILED_FINAL = "FAILED_FINAL"

RETRYABLE_BACKFILL_STATUSES = {
    BACKFILL_STATUS_PENDING,
    BACKFILL_STATUS_FAILED_RETRYABLE,
}

DEFAULT_BACKFILL_SOURCE = "ibkr_historical_backfill_1m"

HistoricalExecutor = Callable[[str, Callable[[Any], dict[str, Any]]], dict[str, Any]]


def enqueue_market_data_backfill_request(
    session_factory: sessionmaker[Session],
    *,
    symbol: str,
    trade_date: str,
    requested_until: datetime,
    instrument: Mapping[str, Any] | None = None,
    reason: str,
    duration: str = "1 D",
    bar_size: str = "1 min",
    what_to_show: str = "TRADES",
    use_rth: bool = True,
) -> dict[str, Any]:
    """Create or extend one coalesced historical request for a symbol/day."""

    if requested_until.tzinfo is None:
        raise ValueError("requested_until must include timezone information")
    normalized = _normalized_request_fields(
        symbol=symbol,
        trade_date=trade_date,
        instrument=instrument,
        duration=duration,
        bar_size=bar_size,
        what_to_show=what_to_show,
        use_rth=use_rth,
    )
    request_key = build_backfill_request_key(normalized)
    now = utc_now()
    with session_scope(session_factory) as session:
        row = session.execute(
            select(MarketDataBackfillRequestRecord).where(
                MarketDataBackfillRequestRecord.request_key == request_key
            )
        ).scalar_one_or_none()
        if row is None:
            row = MarketDataBackfillRequestRecord(
                request_key=request_key,
                requested_until=requested_until,
                covered_until=None,
                status=BACKFILL_STATUS_PENDING,
                reason=_truncate(reason, 128),
                requested_at=now,
                leased_at=None,
                completed_at=None,
                next_retry_at=None,
                attempt_count=0,
                last_error=None,
                request_payload={
                    "policy": "coalesced_symbol_day_observed_bar_backfill",
                    **normalized,
                    "requested_until": requested_until.isoformat(),
                },
                result_payload={},
                **normalized,
            )
            session.add(row)
            session.flush()
            serialized = serialize_market_data_backfill_request(row)
            serialized["enqueue_action"] = "created"
            return serialized

        already_covered = (
            row.status == BACKFILL_STATUS_SUCCEEDED
            and row.covered_until is not None
            and _is_at_least(row.covered_until, requested_until)
        )
        already_pending = (
            row.status in {BACKFILL_STATUS_PENDING, BACKFILL_STATUS_RUNNING, BACKFILL_STATUS_FAILED_RETRYABLE}
            and _is_at_least(row.requested_until, requested_until)
        )
        if already_covered:
            serialized = serialize_market_data_backfill_request(row)
            serialized["enqueue_action"] = "already_covered"
            return serialized
        if already_pending:
            serialized = serialize_market_data_backfill_request(row)
            serialized["enqueue_action"] = "already_pending"
            return serialized

        row.requested_until = _max_requested_until(row.requested_until, requested_until)
        row.reason = _truncate(reason, 128)
        row.requested_at = now
        row.completed_at = None
        row.next_retry_at = None
        row.last_error = None
        if row.status != BACKFILL_STATUS_RUNNING:
            row.status = BACKFILL_STATUS_PENDING
            row.leased_at = None
        row.request_payload = {
            "policy": "coalesced_symbol_day_observed_bar_backfill",
            **normalized,
            "requested_until": row.requested_until.isoformat(),
        }
        session.flush()
        serialized = serialize_market_data_backfill_request(row)
        serialized["enqueue_action"] = "extended"
        return serialized


def build_backfill_request_key(fields: Mapping[str, Any]) -> str:
    payload = json.dumps(
        {
            "symbol": fields["symbol"],
            "exchange": fields["exchange"],
            "currency": fields["currency"],
            "security_type": fields["security_type"],
            "primary_exchange": fields.get("primary_exchange"),
            "local_symbol": fields.get("local_symbol"),
            "isin": fields.get("isin"),
            "trade_date": fields["trade_date"],
            "duration": fields["duration"],
            "bar_size": fields["bar_size"],
            "what_to_show": fields["what_to_show"],
            "use_rth": bool(fields["use_rth"]),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
    return f"ibkr-hist-bars:{fields['trade_date']}:{fields['symbol']}:{digest}"


def claim_due_market_data_backfill_requests(
    session_factory: sessionmaker[Session],
    *,
    limit: int,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    resolved_now = now or utc_now()
    with session_scope(session_factory) as session:
        rows = (
            session.execute(
                select(MarketDataBackfillRequestRecord)
                .where(
                    MarketDataBackfillRequestRecord.status.in_(
                        RETRYABLE_BACKFILL_STATUSES
                    ),
                    or_(
                        MarketDataBackfillRequestRecord.next_retry_at.is_(None),
                        MarketDataBackfillRequestRecord.next_retry_at <= resolved_now,
                    ),
                )
                .order_by(
                    MarketDataBackfillRequestRecord.requested_at.asc(),
                    MarketDataBackfillRequestRecord.id.asc(),
                )
                .limit(limit)
            )
            .scalars()
            .all()
        )
        claimed: list[dict[str, Any]] = []
        for row in rows:
            row.status = BACKFILL_STATUS_RUNNING
            row.leased_at = resolved_now
            row.completed_at = None
            row.next_retry_at = None
            row.attempt_count = int(row.attempt_count or 0) + 1
            claimed.append(serialize_market_data_backfill_request(row))
        return claimed


def mark_market_data_backfill_succeeded(
    session_factory: sessionmaker[Session],
    *,
    request_id: int,
    covered_until: datetime | None = None,
    result_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    now = utc_now()
    with session_scope(session_factory) as session:
        row = _get_request_row(session, request_id)
        resolved_covered_until = covered_until or _aware(row.requested_until)
        row.status = BACKFILL_STATUS_SUCCEEDED
        row.covered_until = resolved_covered_until
        row.completed_at = now
        row.next_retry_at = None
        row.last_error = None
        row.result_payload = dict(result_payload or {})
        session.flush()
        return serialize_market_data_backfill_request(row)


def mark_market_data_backfill_failed(
    session_factory: sessionmaker[Session],
    *,
    request_id: int,
    error: str,
    retryable: bool = True,
    retry_after_seconds: float = 120.0,
    result_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    now = utc_now()
    with session_scope(session_factory) as session:
        row = _get_request_row(session, request_id)
        row.status = (
            BACKFILL_STATUS_FAILED_RETRYABLE
            if retryable
            else BACKFILL_STATUS_FAILED_FINAL
        )
        row.completed_at = now
        row.next_retry_at = (
            now + timedelta(seconds=max(float(retry_after_seconds), 0.0))
            if retryable
            else None
        )
        row.last_error = _truncate(error, 4096)
        row.result_payload = dict(result_payload or {})
        session.flush()
        return serialize_market_data_backfill_request(row)


def list_market_data_backfill_requests(
    session_factory: sessionmaker[Session],
    *,
    symbols: Sequence[str] | None = None,
    statuses: Sequence[str] | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    normalized_symbols = sorted(
        {str(symbol).strip().upper() for symbol in symbols or [] if str(symbol).strip()}
    )
    normalized_statuses = [
        str(status).strip().upper()
        for status in statuses or []
        if str(status).strip()
    ]
    with session_scope(session_factory) as session:
        statement = select(MarketDataBackfillRequestRecord)
        if normalized_symbols:
            statement = statement.where(
                MarketDataBackfillRequestRecord.symbol.in_(normalized_symbols)
            )
        if normalized_statuses:
            statement = statement.where(
                MarketDataBackfillRequestRecord.status.in_(normalized_statuses)
            )
        rows = (
            session.execute(
                statement.order_by(
                    MarketDataBackfillRequestRecord.requested_at.desc(),
                    MarketDataBackfillRequestRecord.id.desc(),
                ).limit(max(int(limit), 1))
            )
            .scalars()
            .all()
        )
    return [serialize_market_data_backfill_request(row) for row in rows]


def run_due_market_data_backfills(
    session_factory: sessionmaker[Session],
    *,
    broker_config: IbkrConnectionConfig,
    execute_historical: HistoricalExecutor,
    limit: int,
    timeout: int,
    source: str = DEFAULT_BACKFILL_SOURCE,
) -> dict[str, Any]:
    claimed = claim_due_market_data_backfill_requests(
        session_factory,
        limit=limit,
    )
    completed: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    for request in claimed:
        request_id = int(request["id"])
        query = HistoricalBarsQuery(
            symbol=request["symbol"],
            security_type=request["security_type"],
            exchange=request["exchange"],
            currency=request["currency"],
            primary_exchange=request.get("primary_exchange"),
            local_symbol=request.get("local_symbol"),
            isin=request.get("isin"),
            duration=request["duration"],
            bar_size=request["bar_size"],
            what_to_show=request["what_to_show"],
            use_rth=bool(request["use_rth"]),
            end_at=_parse_iso_datetime(request["requested_until"]),
        )
        try:
            result = execute_historical(
                "market_data_backfill_today",
                lambda broker_app, query=query: read_historical_bars(
                    broker_config,
                    query,
                    timeout=timeout,
                    app=broker_app,
                ),
            )
            bars = _normalize_historical_bars_for_persistence(
                result.get("bars", []),
                timezone_info=query.end_at.tzinfo if query.end_at is not None else None,
            )
            persist_result = persist_market_stream_bars(
                session_factory,
                bars_by_symbol={request["symbol"]: bars},
                instruments_by_symbol={request["symbol"]: request},
                source=source,
            )
            completed.append(
                mark_market_data_backfill_succeeded(
                    session_factory,
                    request_id=request_id,
                    covered_until=query.end_at,
                    result_payload={
                        "bar_count": result.get("bar_count", len(bars)),
                        "persist_result": persist_result,
                        "source": source,
                        "resolved_contract": result.get("resolved_contract"),
                    },
                )
            )
        except Exception as exc:  # pragma: no cover - exercised through tests with fakes.
            LOGGER.warning(
                "Failed to backfill market data for %s.",
                request.get("symbol"),
                exc_info=True,
            )
            failed.append(
                mark_market_data_backfill_failed(
                    session_factory,
                    request_id=request_id,
                    error=str(exc),
                    retryable=True,
                    retry_after_seconds=120.0,
                    result_payload={"error_type": type(exc).__name__},
                )
            )
    return {
        "claimed_count": len(claimed),
        "completed_count": len(completed),
        "failed_count": len(failed),
        "completed": completed,
        "failed": failed,
    }


class BackgroundMarketDataBackfillService:
    """Slow, paced worker for coalesced RL observed-bar backfills."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        *,
        broker_config: IbkrConnectionConfig,
        execute_historical: HistoricalExecutor,
        interval_seconds: float,
        batch_size: int,
        timeout_seconds: int,
    ) -> None:
        self._session_factory = session_factory
        self._broker_config = broker_config
        self._execute_historical = execute_historical
        self._interval_seconds = max(float(interval_seconds), 1.0)
        self._batch_size = max(int(batch_size), 1)
        self._timeout_seconds = max(int(timeout_seconds), 1)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_result: dict[str, Any] | None = None
        self._last_error: str | None = None
        self._last_run_at: datetime | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="market-data-backfill-worker",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=5)

    def run_once(self) -> dict[str, Any]:
        self._last_run_at = utc_now()
        try:
            result = run_due_market_data_backfills(
                self._session_factory,
                broker_config=self._broker_config,
                execute_historical=self._execute_historical,
                limit=self._batch_size,
                timeout=self._timeout_seconds,
            )
            self._last_result = result
            self._last_error = None
            return result
        except Exception as exc:
            self._last_error = str(exc)
            raise

    def status(self) -> dict[str, Any]:
        return {
            "running": self._thread is not None and self._thread.is_alive(),
            "interval_seconds": self._interval_seconds,
            "batch_size": self._batch_size,
            "timeout_seconds": self._timeout_seconds,
            "last_run_at": (
                self._last_run_at.isoformat()
                if self._last_run_at is not None
                else None
            ),
            "last_error": self._last_error,
            "last_result": self._last_result,
        }

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception:
                LOGGER.warning("Market data backfill worker cycle failed.", exc_info=True)
            self._stop_event.wait(self._interval_seconds)


def serialize_market_data_backfill_request(
    row: MarketDataBackfillRequestRecord,
) -> dict[str, Any]:
    request_payload = dict(row.request_payload or {})
    return {
        "id": row.id,
        "request_key": row.request_key,
        "symbol": row.symbol,
        "exchange": row.exchange,
        "currency": row.currency,
        "security_type": row.security_type,
        "primary_exchange": row.primary_exchange,
        "local_symbol": row.local_symbol,
        "isin": row.isin,
        "trade_date": row.trade_date,
        "requested_until": (
            str(request_payload["requested_until"])
            if request_payload.get("requested_until") is not None
            else _serialize_datetime(row.requested_until)
        ),
        "covered_until": _serialize_datetime(row.covered_until),
        "duration": row.duration,
        "bar_size": row.bar_size,
        "what_to_show": row.what_to_show,
        "use_rth": row.use_rth,
        "status": row.status,
        "reason": row.reason,
        "requested_at": _serialize_datetime(row.requested_at),
        "leased_at": _serialize_datetime(row.leased_at),
        "completed_at": _serialize_datetime(row.completed_at),
        "next_retry_at": _serialize_datetime(row.next_retry_at),
        "attempt_count": row.attempt_count,
        "last_error": row.last_error,
        "request_payload": request_payload,
        "result_payload": dict(row.result_payload or {}),
    }


def _normalized_request_fields(
    *,
    symbol: str,
    trade_date: str,
    instrument: Mapping[str, Any] | None,
    duration: str,
    bar_size: str,
    what_to_show: str,
    use_rth: bool,
) -> dict[str, Any]:
    raw_instrument = dict(instrument or {})
    normalized_symbol = str(symbol or raw_instrument.get("symbol") or "").strip().upper()
    if not normalized_symbol:
        raise ValueError("symbol is required")
    normalized_trade_date = str(trade_date).strip()
    if not normalized_trade_date:
        raise ValueError("trade_date is required")
    return {
        "symbol": normalized_symbol,
        "exchange": _normalize_text(raw_instrument.get("exchange"), default="SMART"),
        "currency": _normalize_text(raw_instrument.get("currency"), default="SEK"),
        "security_type": _normalize_text(raw_instrument.get("security_type"), default="STK"),
        "primary_exchange": _normalize_optional_text(
            raw_instrument.get("primary_exchange")
        ),
        "local_symbol": _normalize_optional_text(raw_instrument.get("local_symbol")),
        "isin": _normalize_optional_text(raw_instrument.get("isin")),
        "trade_date": normalized_trade_date,
        "duration": str(duration).strip() or "1 D",
        "bar_size": str(bar_size).strip() or "1 min",
        "what_to_show": str(what_to_show).strip().upper() or "TRADES",
        "use_rth": bool(use_rth),
    }


def _get_request_row(
    session: Session,
    request_id: int,
) -> MarketDataBackfillRequestRecord:
    row = session.get(MarketDataBackfillRequestRecord, int(request_id))
    if row is None:
        raise LookupError(f"market data backfill request {request_id} was not found")
    return row


def _parse_iso_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _is_at_least(existing: datetime, requested: datetime) -> bool:
    if existing.tzinfo is None:
        return existing >= requested.replace(tzinfo=None)
    return existing >= requested


def _max_requested_until(existing: datetime, requested: datetime) -> datetime:
    if _is_at_least(existing, requested):
        return existing
    return requested


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat() if value.tzinfo is None else _aware(value).isoformat()


def _normalize_text(value: Any, *, default: str) -> str:
    raw = str(value or default).strip().upper()
    return raw or default


def _normalize_optional_text(value: Any) -> str | None:
    raw = str(value or "").strip().upper()
    return raw or None


def _truncate(value: str, max_length: int) -> str:
    raw = str(value or "").strip()
    return raw[:max_length]


def _normalize_historical_bars_for_persistence(
    bars: Any,
    *,
    timezone_info: timezone | None,
) -> list[dict[str, Any]]:
    if not isinstance(bars, Sequence) or isinstance(bars, (str, bytes)):
        return []
    normalized: list[dict[str, Any]] = []
    for raw_bar in bars:
        if not isinstance(raw_bar, Mapping):
            continue
        bar = dict(raw_bar)
        timestamp = bar.get("timestamp") or bar.get("date")
        parsed = _parse_historical_bar_timestamp(
            timestamp,
            timezone_info=timezone_info,
        )
        if parsed is not None:
            bar["timestamp"] = parsed.isoformat()
        normalized.append(bar)
    return normalized


def _parse_historical_bar_timestamp(
    value: Any,
    *,
    timezone_info: timezone | None,
) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            parts = raw.split()
            if len(parts) < 2:
                return None
            parsed = None
            for fmt in ("%Y%m%d %H:%M:%S", "%Y%m%d %H:%M"):
                try:
                    parsed = datetime.strptime(f"{parts[0]} {parts[1]}", fmt)
                    break
                except ValueError:
                    continue
            if parsed is None:
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone_info or timezone.utc)
    return parsed
