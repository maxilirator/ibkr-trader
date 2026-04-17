from __future__ import annotations

import argparse
import ipaddress
import json
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping

from ibkr_trader.config import AppConfig
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.domain.contract_resolution import ContractResolveQuery
from ibkr_trader.domain.execution_contract import (
    ExecutionInstructionBatch,
)
from ibkr_trader.domain.execution_payloads import parse_datetime
from ibkr_trader.domain.execution_payloads import parse_decimal
from ibkr_trader.domain.execution_payloads import parse_date
from ibkr_trader.domain.execution_payloads import parse_execution_batch_payload
from ibkr_trader.ibkr.account_summary import (
    DEFAULT_ACCOUNT_SUMMARY_TAGS,
    read_account_summary,
)
from ibkr_trader.ibkr.contracts import (
    resolve_contracts,
    serialize_contract_resolve_result,
)
from ibkr_trader.ibkr.errors import IbkrDependencyError
from ibkr_trader.ibkr.historical_bars import HistoricalBarsQuery, read_historical_bars
from ibkr_trader.ibkr.order_execution import cancel_broker_order
from ibkr_trader.ibkr.order_execution import submit_order_from_batch
from ibkr_trader.ibkr.order_execution import submit_order_from_instruction
from ibkr_trader.ibkr.order_execution import submit_exit_order_from_instruction
from ibkr_trader.ibkr.order_preview import preview_execution_batch
from ibkr_trader.ibkr.probe import probe_gateway
from ibkr_trader.ibkr.runtime_snapshot import (
    fetch_broker_runtime_snapshot,
    serialize_broker_runtime_snapshot,
)
from ibkr_trader.ibkr.shortability import (
    ShortabilityMarketDataType,
    ShortabilitySource,
    ShortabilitySnapshotQuery,
    collect_shortability_snapshot,
    persist_shortability_snapshot,
)
from ibkr_trader.ibkr.tick_stream import TickStreamQuery
from ibkr_trader.ibkr.tick_stream import _normalize_tick_type
from ibkr_trader.ibkr.tick_stream import collect_tick_stream_sample
from ibkr_trader.ibkr.session_manager import CanonicalSyncSessions
from ibkr_trader.orchestration.entry_submission import PersistedInstructionNotFoundError
from ibkr_trader.orchestration.entry_submission import PersistedInstructionStateError
from ibkr_trader.orchestration.entry_submission import cancel_persisted_instruction_entry
from ibkr_trader.orchestration.entry_submission import serialize_persisted_broker_cancellation
from ibkr_trader.orchestration.entry_submission import serialize_persisted_broker_submission
from ibkr_trader.orchestration.entry_submission import submit_persisted_instruction_entry
from ibkr_trader.orchestration.instruction_status import InstructionStatusNotFoundError
from ibkr_trader.orchestration.instruction_status import read_instruction_status
from ibkr_trader.orchestration.instruction_status import serialize_instruction_status
from ibkr_trader.orchestration.runtime_worker import run_runtime_cycle
from ibkr_trader.orchestration.runtime_worker import serialize_runtime_cycle_result
from ibkr_trader.orchestration.scheduling import build_batch_runtime_schedule
from ibkr_trader.orchestration.submission import SubmissionConflictError
from ibkr_trader.orchestration.submission import submit_execution_batch


class ApiDependencyError(RuntimeError):
    """Raised when optional API server dependencies are unavailable."""


def is_loopback_host(host: str | None) -> bool:
    if not host:
        return False
    if host == "localhost":
        return True

    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def enforce_loopback_binding(host: str, *, require_loopback_only: bool) -> None:
    if require_loopback_only and not is_loopback_host(host):
        raise ValueError(
            "API host must be loopback when API_REQUIRE_LOOPBACK_ONLY is enabled."
        )


def parse_contract_resolve_payload(payload: Mapping[str, Any]) -> ContractResolveQuery:
    query = ContractResolveQuery(
        symbol=str(payload["symbol"]).upper(),
        security_type=str(payload.get("security_type", "STK")).upper(),
        exchange=str(payload["exchange"]).upper(),
        currency=str(payload["currency"]).upper(),
        primary_exchange=(
            str(payload["primary_exchange"]).upper()
            if payload.get("primary_exchange") is not None
            else None
        ),
        local_symbol=(
            str(payload["local_symbol"])
            if payload.get("local_symbol") is not None
            else None
        ),
        include_expired=bool(payload.get("include_expired", False)),
        isin=str(payload["isin"]) if payload.get("isin") is not None else None,
    )
    query.validate()
    return query


def parse_account_summary_payload(payload: Mapping[str, Any]) -> tuple[tuple[str, ...], str, str | None]:
    raw_tags = payload.get("tags")
    if raw_tags is None:
        tags = DEFAULT_ACCOUNT_SUMMARY_TAGS
    else:
        if not isinstance(raw_tags, list) or not raw_tags:
            raise ValueError("tags must be a non-empty array of strings")
        tags = tuple(str(tag) for tag in raw_tags)
        if not all(tag for tag in tags):
            raise ValueError("tags must contain only non-empty strings")

    group = str(payload.get("group", "All"))
    account_id = (
        str(payload["account_id"])
        if payload.get("account_id") is not None
        else None
    )
    return tags, group, account_id


def parse_historical_bars_payload(payload: Mapping[str, Any]) -> HistoricalBarsQuery:
    end_at = (
        parse_datetime(payload["end_at"], "end_at")
        if payload.get("end_at") is not None
        else None
    )
    query = HistoricalBarsQuery(
        symbol=str(payload["symbol"]).upper(),
        security_type=str(payload.get("security_type", "STK")).upper(),
        exchange=str(payload["exchange"]).upper(),
        currency=str(payload["currency"]).upper(),
        primary_exchange=(
            str(payload["primary_exchange"]).upper()
            if payload.get("primary_exchange") is not None
            else None
        ),
        local_symbol=(
            str(payload["local_symbol"])
            if payload.get("local_symbol") is not None
            else None
        ),
        isin=str(payload["isin"]) if payload.get("isin") is not None else None,
        duration=str(payload["duration"]),
        bar_size=str(payload["bar_size"]),
        what_to_show=str(payload.get("what_to_show", "TRADES")).upper(),
        use_rth=bool(payload.get("use_rth", True)),
        end_at=end_at,
    )
    query.validate()
    return query


def parse_runtime_cycle_payload(
    payload: Mapping[str, Any],
) -> tuple[datetime | None, int, tuple[str, ...] | None]:
    now_at = (
        parse_datetime(payload["now_at"], "now_at")
        if payload.get("now_at") is not None
        else None
    )
    timeout = int(payload.get("timeout", 10))
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    raw_instruction_ids = payload.get("instruction_ids")
    instruction_ids: tuple[str, ...] | None = None
    if raw_instruction_ids is not None:
        if not isinstance(raw_instruction_ids, list) or not raw_instruction_ids:
            raise ValueError("instruction_ids must be a non-empty array of strings")
        parsed_instruction_ids = tuple(str(item).strip() for item in raw_instruction_ids)
        if not all(parsed_instruction_ids):
            raise ValueError("instruction_ids must contain only non-empty strings")
        if len(set(parsed_instruction_ids)) != len(parsed_instruction_ids):
            raise ValueError("instruction_ids must not contain duplicates")
        instruction_ids = parsed_instruction_ids
    return now_at, timeout, instruction_ids


def parse_tick_stream_payload(payload: Mapping[str, Any]) -> TickStreamQuery:
    raw_tick_types = payload.get("tick_types", ["Last", "BidAsk"])
    if not isinstance(raw_tick_types, list) or not raw_tick_types:
        raise ValueError("tick_types must be a non-empty array of strings")

    query = TickStreamQuery(
        symbol=str(payload["symbol"]).upper(),
        security_type=str(payload.get("security_type", "STK")).upper(),
        exchange=str(payload["exchange"]).upper(),
        currency=str(payload["currency"]).upper(),
        primary_exchange=(
            str(payload["primary_exchange"]).upper()
            if payload.get("primary_exchange") is not None
            else None
        ),
        local_symbol=(
            str(payload["local_symbol"])
            if payload.get("local_symbol") is not None
            else None
        ),
        isin=str(payload["isin"]) if payload.get("isin") is not None else None,
        tick_types=tuple(_normalize_tick_type(item) for item in raw_tick_types),
        duration_seconds=float(payload.get("duration_seconds", 5.0)),
        max_events=int(payload.get("max_events", 500)),
        ignore_size=bool(payload.get("ignore_size", False)),
    )
    query.validate()
    return query


def parse_shortability_snapshot_payload(
    payload: Mapping[str, Any],
) -> ShortabilitySnapshotQuery:
    raw_symbols = payload.get("symbols")
    symbols: tuple[str, ...] | None = None
    if raw_symbols is not None:
        if not isinstance(raw_symbols, list) or not raw_symbols:
            raise ValueError("symbols must be a non-empty array of strings")
        symbols = tuple(str(symbol).strip().upper() for symbol in raw_symbols)
        if not all(symbols):
            raise ValueError("symbols must contain only non-empty strings")
        if len(set(symbols)) != len(symbols):
            raise ValueError("symbols must not contain duplicates")

    raw_market_data_type = str(payload.get("market_data_type", "LIVE")).strip().upper()
    normalized_market_data_type = raw_market_data_type.replace("-", "_").replace(" ", "_")
    try:
        market_data_type = ShortabilityMarketDataType(normalized_market_data_type)
    except ValueError as exc:
        raise ValueError(
            "market_data_type must be one of LIVE, FROZEN, DELAYED, DELAYED_FROZEN"
        ) from exc

    raw_source = str(
        payload.get("source", ShortabilitySource.OFFICIAL_IBKR_PAGE.value)
    ).strip()
    normalized_source = raw_source.upper().replace("-", "_").replace(" ", "_")
    source_aliases = {
        "OFFICIAL": ShortabilitySource.OFFICIAL_IBKR_PAGE,
        "OFFICIAL_PAGE": ShortabilitySource.OFFICIAL_IBKR_PAGE,
        "OFFICIAL_IBKR_PAGE": ShortabilitySource.OFFICIAL_IBKR_PAGE,
        "BROKER": ShortabilitySource.BROKER_TICKS,
        "BROKER_TICK": ShortabilitySource.BROKER_TICKS,
        "BROKER_TICKS": ShortabilitySource.BROKER_TICKS,
    }
    source = source_aliases.get(normalized_source)
    if source is None:
        raise ValueError("source must be OFFICIAL_IBKR_PAGE or BROKER_TICKS")

    query = ShortabilitySnapshotQuery(
        symbols=symbols,
        as_of_date=(
            parse_date(payload["as_of_date"], "as_of_date")
            if payload.get("as_of_date") is not None
            else None
        ),
        exchange=str(payload.get("exchange", "SMART")).upper(),
        primary_exchange=str(payload.get("primary_exchange", "SFB")).upper(),
        currency=str(payload.get("currency", "SEK")).upper(),
        security_type=str(payload.get("security_type", "STK")).upper(),
        source=source,
        only_shortable=bool(payload.get("only_shortable", True)),
        market_data_type=market_data_type,
        per_symbol_timeout_seconds=float(payload.get("per_symbol_timeout_seconds", 2.0)),
        max_concurrent=int(payload.get("max_concurrent", 25)),
        max_symbols=(
            int(payload["max_symbols"])
            if payload.get("max_symbols") is not None
            else None
        ),
    )
    query.validate()
    return query


def _serialize_for_json(payload: Any) -> Any:
    if isinstance(payload, Enum):
        return payload.value
    if isinstance(payload, Decimal):
        return str(payload)
    if isinstance(payload, datetime):
        return payload.isoformat()
    if isinstance(payload, date):
        return payload.isoformat()
    if isinstance(payload, list):
        return [_serialize_for_json(item) for item in payload]
    if isinstance(payload, tuple):
        return [_serialize_for_json(item) for item in payload]
    if isinstance(payload, dict):
        return {key: _serialize_for_json(value) for key, value in payload.items()}
    return payload


def serialize_execution_batch(batch: ExecutionInstructionBatch) -> dict[str, Any]:
    payload = asdict(batch)
    payload = _serialize_for_json(payload)
    return payload


def serialize_runtime_schedule_preview(payload: Any) -> dict[str, Any]:
    serialized = asdict(payload)
    return _serialize_for_json(serialized)


def serialize_submitted_batch(payload: Any) -> dict[str, Any]:
    serialized = asdict(payload)
    return _serialize_for_json(serialized)


def _load_fastapi_runtime() -> tuple[Any, Any, Any, Any]:
    try:
        from fastapi import FastAPI, HTTPException, Request
        from fastapi.responses import JSONResponse
    except ModuleNotFoundError as exc:
        raise ApiDependencyError(
            "FastAPI server dependencies are not installed. "
            "Install the optional `server` dependencies for this project."
        ) from exc

    return FastAPI, HTTPException, Request, JSONResponse


def create_app(config: AppConfig | None = None) -> Any:
    app_config = config or AppConfig.from_env()
    engine = build_engine(app_config.database_url)
    session_factory = create_session_factory(engine)
    enforce_loopback_binding(
        app_config.api.host,
        require_loopback_only=app_config.api.require_loopback_only,
    )
    FastAPI, HTTPException, Request, JSONResponse = _load_fastapi_runtime()
    broker_sessions = CanonicalSyncSessions(app_config.ibkr)

    @asynccontextmanager
    async def lifespan(_: Any) -> Any:
        broker_sessions.warmup()
        try:
            yield
        finally:
            broker_sessions.shutdown()

    app = FastAPI(
        title="IBKR Trader Local API",
        version="0.1.0",
        summary="Local-only control plane for the IBKR Trader runtime.",
        lifespan=lifespan,
    )
    app.state.broker_sessions = broker_sessions

    def with_primary_session(operation_name: str, operation: Any) -> Any:
        return broker_sessions.primary.execute(operation_name, operation)

    def with_diagnostic_session(operation_name: str, operation: Any) -> Any:
        return broker_sessions.diagnostic.execute(operation_name, operation)

    def submit_order_with_primary(
        broker_config: Any,
        instruction: Any,
        *,
        timeout: int = 10,
    ) -> dict[str, Any]:
        return with_primary_session(
            "persisted_entry_submit",
            lambda broker_app: submit_order_from_instruction(
                broker_config,
                instruction,
                timeout=timeout,
                app=broker_app,
            )
        )

    def submit_exit_with_primary(
        broker_config: Any,
        instruction: Any,
        *,
        quantity: Decimal,
        order_type: Any,
        order_ref: str,
        timeout: int = 10,
        limit_price: Decimal | None = None,
        stop_price: Decimal | None = None,
        oca_group: str | None = None,
        oca_type: int | None = None,
    ) -> dict[str, Any]:
        return with_primary_session(
            "runtime_exit_submit",
            lambda broker_app: submit_exit_order_from_instruction(
                broker_config,
                instruction,
                quantity=quantity,
                order_type=order_type,
                order_ref=order_ref,
                timeout=timeout,
                limit_price=limit_price,
                stop_price=stop_price,
                oca_group=oca_group,
                oca_type=oca_type,
                app=broker_app,
            )
        )

    def cancel_order_with_primary(
        broker_config: Any,
        order_id: int,
        *,
        timeout: int = 10,
    ) -> dict[str, Any]:
        return with_primary_session(
            "broker_cancel",
            lambda broker_app: cancel_broker_order(
                broker_config,
                order_id,
                timeout=timeout,
                app=broker_app,
            )
        )

    def fetch_runtime_snapshot_with_primary(
        broker_config: Any,
        *,
        timeout: int = 10,
    ) -> Any:
        return with_primary_session(
            "broker_runtime_snapshot",
            lambda broker_app: fetch_broker_runtime_snapshot(
                broker_config,
                timeout=timeout,
                app=broker_app,
            )
        )

    @app.middleware("http")
    async def require_local_client(request: Request, call_next: Any) -> Any:
        client_host = request.client.host if request.client else None
        if (
            app_config.api.require_loopback_only
            and not is_loopback_host(client_host)
        ):
            return JSONResponse(
                status_code=403,
                content={"detail": "This API accepts loopback clients only."},
            )
        return await call_next(request)

    @app.get("/healthz")
    def healthz() -> dict[str, Any]:
        return {
            "status": "ok",
            "local_only": app_config.api.require_loopback_only,
            "api_host": app_config.api.host,
            "api_port": app_config.api.port,
            "runtime_timezone": app_config.timezone,
            "session_calendar_path": str(app_config.session_calendar_path),
            "broker_sessions": broker_sessions.status_snapshot(),
            "broker_operations": broker_sessions.activity_tracker.snapshot(recent_limit=10),
        }

    @app.get("/v1/ibkr/telemetry")
    def get_ibkr_telemetry(recent_limit: int = 50) -> dict[str, Any]:
        if recent_limit <= 0:
            raise HTTPException(status_code=400, detail="recent_limit must be positive")
        if recent_limit > 200:
            raise HTTPException(status_code=400, detail="recent_limit must be at most 200")
        return {
            "accepted": True,
            "telemetry": broker_sessions.telemetry_snapshot(recent_limit=recent_limit),
        }

    @app.post("/v1/ibkr/probe")
    def run_ibkr_probe(timeout: int = 5) -> dict[str, Any]:
        try:
            result = with_diagnostic_session(
                "probe",
                lambda broker_app: probe_gateway(
                    app_config.ibkr.diagnostic_session(),
                    timeout=timeout,
                    app=broker_app,
                )
            )
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

        return json.loads(result.to_json())

    @app.post("/v1/contracts/resolve")
    def resolve_ibkr_contract(payload: dict[str, Any], timeout: int = 10) -> dict[str, Any]:
        try:
            query = parse_contract_resolve_payload(payload)
            result = with_diagnostic_session(
                "contract_resolve",
                lambda broker_app: resolve_contracts(
                    app_config.ibkr.diagnostic_session(),
                    query,
                    timeout=timeout,
                    app=broker_app,
                )
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

        return serialize_contract_resolve_result(result)

    @app.post("/v1/accounts/summary")
    def get_account_summary(payload: dict[str, Any] | None = None, timeout: int = 10) -> dict[str, Any]:
        request_payload = payload or {}
        try:
            tags, group, account_id = parse_account_summary_payload(request_payload)
            return with_diagnostic_session(
                "account_summary",
                lambda broker_app: read_account_summary(
                    app_config.ibkr.diagnostic_session(),
                    tags=tags,
                    group=group,
                    account_id=account_id,
                    timeout=timeout,
                    app=broker_app,
                )
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

    @app.get("/v1/broker/runtime-snapshot")
    def get_broker_runtime_snapshot(timeout: int = 10) -> dict[str, Any]:
        try:
            snapshot = with_primary_session(
                "broker_runtime_snapshot",
                lambda broker_app: fetch_broker_runtime_snapshot(
                    app_config.ibkr.primary_session(),
                    timeout=timeout,
                    app=broker_app,
                )
            )
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

        return {
            "accepted": True,
            "session_client_id": app_config.ibkr.client_id,
            "visibility_limits": {
                "live_broker_open_orders_only": True,
                "untransmitted_tws_orders_visible_via_api": False,
                "note": (
                    "IBKR does not expose untransmitted TWS-local orders through the "
                    "normal open-order API path while they remain untransmitted."
                ),
            },
            "broker_runtime": serialize_broker_runtime_snapshot(snapshot),
        }

    @app.post("/v1/market-data/historical-bars")
    def get_historical_bars(payload: dict[str, Any], timeout: int = 20) -> dict[str, Any]:
        try:
            query = parse_historical_bars_payload(payload)
            return with_diagnostic_session(
                "historical_bars",
                lambda broker_app: read_historical_bars(
                    app_config.ibkr.diagnostic_session(),
                    query,
                    timeout=timeout,
                    app=broker_app,
                )
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

    @app.post("/v1/market-data/tick-stream-sample")
    def get_tick_stream_sample(payload: dict[str, Any], timeout: int = 15) -> dict[str, Any]:
        try:
            query = parse_tick_stream_payload(payload)
            return collect_tick_stream_sample(
                app_config.ibkr.streaming_session(),
                query,
                timeout=timeout,
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

    @app.post("/v1/market-data/shortability-snapshot")
    def get_shortability_snapshot(
        payload: dict[str, Any] | None = None,
        timeout: int = 120,
    ) -> dict[str, Any]:
        request_payload = payload or {}
        try:
            query = parse_shortability_snapshot_payload(request_payload)
            snapshot = collect_shortability_snapshot(
                app_config.ibkr.streaming_session(),
                query,
                instruments_path=app_config.stockholm_instruments_path,
                identity_path=app_config.stockholm_identity_path,
                timeout=timeout,
            )
            persist_requested = bool(
                request_payload.get(
                    "persist",
                    query.symbols is None and query.max_symbols is None,
                )
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

        persisted_artifacts = None
        if persist_requested:
            persisted_artifacts = persist_shortability_snapshot(
                snapshot,
                instruments_dir=app_config.stockholm_instruments_path.parent,
                meta_dir=app_config.stockholm_identity_path.parent / "shortability",
            )

        return {
            "accepted": True,
            "session_client_id": (
                app_config.ibkr.streaming_client_id
                if query.source == ShortabilitySource.BROKER_TICKS
                else None
            ),
            "stockholm_instruments_path": str(app_config.stockholm_instruments_path),
            "persisted_artifacts": persisted_artifacts,
            "shortability_snapshot": snapshot,
        }

    @app.post("/v1/orders/preview")
    def preview_orders(payload: dict[str, Any], timeout: int = 10) -> dict[str, Any]:
        try:
            batch = parse_execution_batch_payload(payload)
            return with_diagnostic_session(
                "order_preview",
                lambda broker_app: preview_execution_batch(
                    app_config.ibkr.diagnostic_session(),
                    batch,
                    timeout=timeout,
                    app=broker_app,
                )
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

    @app.post("/v1/orders/submit")
    def submit_order(payload: dict[str, Any], timeout: int = 10) -> dict[str, Any]:
        try:
            batch = parse_execution_batch_payload(payload)
            result = with_primary_session(
                "order_submit",
                lambda broker_app: submit_order_from_batch(
                    app_config.ibkr.primary_session(),
                    batch,
                    timeout=timeout,
                    app=broker_app,
                )
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

        return {
            "accepted": True,
            "mode": "manual_broker_submit",
            "runtime_timezone": app_config.timezone,
            "session_client_id": app_config.ibkr.client_id,
            "submitted_order": result,
        }

    @app.post("/v1/orders/{order_id}/cancel")
    def cancel_order(order_id: int, timeout: int = 10) -> dict[str, Any]:
        try:
            result = with_primary_session(
                "order_cancel",
                lambda broker_app: cancel_broker_order(
                    app_config.ibkr.primary_session(),
                    order_id,
                    timeout=timeout,
                    app=broker_app,
                )
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

        return {
            "accepted": True,
            "mode": "manual_broker_cancel",
            "session_client_id": app_config.ibkr.client_id,
            "order_id": order_id,
            "cancelled_order": result,
        }

    @app.post("/v1/instructions/validate")
    def validate_instruction(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            batch = parse_execution_batch_payload(payload)
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "instruction_count": len(batch.instructions),
            "batch": serialize_execution_batch(batch),
        }

    @app.get("/v1/instructions/{instruction_id}")
    def get_instruction_status(
        instruction_id: str,
        include_events: bool = True,
    ) -> dict[str, Any]:
        try:
            result = read_instruction_status(
                session_factory,
                instruction_id,
                include_events=include_events,
            )
        except InstructionStatusNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        return {
            "accepted": True,
            "instruction": serialize_instruction_status(result),
        }

    @app.post("/v1/instructions/submit")
    def submit_instruction(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            batch = parse_execution_batch_payload(payload)
            result = submit_execution_batch(
                session_factory,
                batch,
                runtime_timezone=app_config.timezone,
                session_calendar_path=app_config.session_calendar_path,
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except SubmissionConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        return {
            "accepted": True,
            "instruction_count": result.instruction_count,
            "runtime_timezone": app_config.timezone,
            "session_calendar_path": str(app_config.session_calendar_path),
            "submitted": serialize_submitted_batch(result),
        }

    @app.post("/v1/instructions/{instruction_id}/submit-entry")
    def submit_instruction_entry(instruction_id: str, timeout: int = 10) -> dict[str, Any]:
        try:
            result = submit_persisted_instruction_entry(
                session_factory,
                app_config.ibkr.primary_session(),
                instruction_id,
                timeout=timeout,
                submitter=submit_order_with_primary,
            )
        except PersistedInstructionNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except PersistedInstructionStateError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

        return {
            "accepted": True,
            "mode": "persisted_entry_submit",
            "runtime_timezone": app_config.timezone,
            "session_client_id": app_config.ibkr.client_id,
            "submitted_entry": serialize_persisted_broker_submission(result),
        }

    @app.post("/v1/instructions/{instruction_id}/cancel-entry")
    def cancel_instruction_entry(instruction_id: str, timeout: int = 10) -> dict[str, Any]:
        try:
            result = cancel_persisted_instruction_entry(
                session_factory,
                app_config.ibkr.primary_session(),
                instruction_id,
                timeout=timeout,
                canceler=cancel_order_with_primary,
            )
        except PersistedInstructionNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except PersistedInstructionStateError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

        return {
            "accepted": True,
            "mode": "persisted_entry_cancel",
            "runtime_timezone": app_config.timezone,
            "session_client_id": app_config.ibkr.client_id,
            "cancelled_entry": serialize_persisted_broker_cancellation(result),
        }

    @app.post("/v1/instructions/schedule-preview")
    def preview_instruction_schedule(payload: dict[str, Any]) -> dict[str, Any]:
        try:
            batch = parse_execution_batch_payload(payload)
            schedule = build_batch_runtime_schedule(
                batch,
                runtime_timezone=app_config.timezone,
                session_calendar_path=app_config.session_calendar_path,
            )
        except (KeyError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        return {
            "accepted": True,
            "runtime_timezone": app_config.timezone,
            "session_calendar_path": str(app_config.session_calendar_path),
            "schedule": serialize_runtime_schedule_preview(schedule),
        }

    @app.post("/v1/runtime/run-once")
    def run_runtime_cycle_once(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        request_payload = payload or {}
        try:
            now_at, timeout, instruction_ids = parse_runtime_cycle_payload(request_payload)
            result = run_runtime_cycle(
                session_factory,
                app_config.ibkr.primary_session(),
                runtime_timezone=app_config.timezone,
                session_calendar_path=app_config.session_calendar_path,
                now=now_at,
                timeout=timeout,
                instruction_ids=instruction_ids,
                entry_submitter=submit_order_with_primary,
                exit_submitter=submit_exit_with_primary,
                broker_snapshot_fetcher=fetch_runtime_snapshot_with_primary,
                broker_order_canceler=cancel_order_with_primary,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=504, detail=str(exc)) from exc

        return {
            "accepted": True,
            "runtime_timezone": app_config.timezone,
            "session_calendar_path": str(app_config.session_calendar_path),
            "runtime_cycle": serialize_runtime_cycle_result(result),
        }

    return app


def run_server(config: AppConfig | None = None, *, reload: bool = False) -> None:
    app_config = config or AppConfig.from_env()
    enforce_loopback_binding(
        app_config.api.host,
        require_loopback_only=app_config.api.require_loopback_only,
    )

    try:
        import uvicorn
    except ModuleNotFoundError as exc:
        raise ApiDependencyError(
            "Uvicorn is not installed. Install the optional `server` dependencies "
            "for this project."
        ) from exc

    uvicorn.run(
        create_app(app_config),
        host=app_config.api.host,
        port=app_config.api.port,
        reload=reload,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the local-only FastAPI server for IBKR Trader."
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable development reload mode.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    run_server(reload=args.reload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
