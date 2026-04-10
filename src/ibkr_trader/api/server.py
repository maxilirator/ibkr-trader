from __future__ import annotations

import argparse
import ipaddress
import json
from dataclasses import asdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any, Mapping

from ibkr_trader.config import AppConfig
from ibkr_trader.domain.contract_resolution import ContractResolveQuery
from ibkr_trader.domain.execution_contract import (
    AccountRef,
    EntrySpec,
    ExecutionInstruction,
    ExecutionInstructionBatch,
    ExitSpec,
    InstrumentRef,
    IntentSpec,
    OrderType,
    PositionSide,
    SecurityType,
    SizingMode,
    SizingSpec,
    SourceContext,
    TimeInForce,
    TraceSpec,
)
from ibkr_trader.ibkr.account_summary import (
    DEFAULT_ACCOUNT_SUMMARY_TAGS,
    read_account_summary,
)
from ibkr_trader.ibkr.contracts import (
    resolve_contracts,
    serialize_contract_resolve_result,
)
from ibkr_trader.ibkr.historical_bars import HistoricalBarsQuery, read_historical_bars
from ibkr_trader.ibkr.order_preview import preview_execution_batch
from ibkr_trader.ibkr.probe import IbkrDependencyError, probe_gateway
from ibkr_trader.orchestration.scheduling import build_batch_runtime_schedule


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


def _parse_decimal(value: Any, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be a valid decimal-compatible value") from exc


def _parse_datetime(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO-8601 string with timezone")

    normalized_value = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized_value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid ISO-8601 datetime") from exc

    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include timezone information")
    return parsed


def _parse_date(value: Any, field_name: str) -> date:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO-8601 date string")

    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid ISO-8601 date") from exc


def _parse_string_map(value: Any, field_name: str) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a string-to-string object")

    parsed: dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(key, str) or not isinstance(item, str):
            raise ValueError(f"{field_name} must be a string-to-string object")
        parsed[key] = item
    return parsed


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
        _parse_datetime(payload["end_at"], "end_at")
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


def parse_execution_instruction_payload(payload: Mapping[str, Any]) -> ExecutionInstruction:
    account_payload = payload.get("account")
    if not isinstance(account_payload, Mapping):
        raise ValueError("account must be an object")

    instrument_payload = payload.get("instrument")
    if not isinstance(instrument_payload, Mapping):
        raise ValueError("instrument must be an object")

    intent_payload = payload.get("intent")
    if not isinstance(intent_payload, Mapping):
        raise ValueError("intent must be an object")

    sizing_payload = payload.get("sizing")
    if not isinstance(sizing_payload, Mapping):
        raise ValueError("sizing must be an object")

    entry_payload = payload.get("entry")
    if not isinstance(entry_payload, Mapping):
        raise ValueError("entry must be an object")

    exit_payload = payload.get("exit")
    if not isinstance(exit_payload, Mapping):
        raise ValueError("exit must be an object")

    trace_payload = payload.get("trace", {})
    if not isinstance(trace_payload, Mapping):
        raise ValueError("trace must be an object")

    instruction = ExecutionInstruction(
        instruction_id=str(payload["instruction_id"]),
        account=AccountRef(
            account_key=str(account_payload["account_key"]),
            book_key=str(account_payload["book_key"]),
            book_role=(
                str(account_payload["book_role"])
                if account_payload.get("book_role") is not None
                else None
            ),
            book_side=(
                PositionSide(str(account_payload["book_side"]).upper())
                if account_payload.get("book_side") is not None
                else None
            ),
        ),
        instrument=InstrumentRef(
            symbol=str(instrument_payload["symbol"]).upper(),
            exchange=str(instrument_payload["exchange"]).upper(),
            currency=str(instrument_payload["currency"]).upper(),
            security_type=SecurityType(
                str(instrument_payload.get("security_type", "STK")).upper()
            ),
            isin=(
                str(instrument_payload["isin"])
                if instrument_payload.get("isin") is not None
                else None
            ),
            primary_exchange=(
                str(instrument_payload["primary_exchange"]).upper()
                if instrument_payload.get("primary_exchange") is not None
                else None
            ),
            aliases=tuple(
                str(alias) for alias in instrument_payload.get("aliases", ())
            ),
        ),
        intent=IntentSpec(
            side=str(intent_payload["side"]).upper(),
            position_side=PositionSide(str(intent_payload["position_side"]).upper()),
        ),
        sizing=SizingSpec(
            mode=SizingMode(str(sizing_payload["mode"])),
            target_fraction_of_account=(
                _parse_decimal(
                    sizing_payload["target_fraction_of_account"],
                    "sizing.target_fraction_of_account",
                )
                if sizing_payload.get("target_fraction_of_account") is not None
                else None
            ),
            target_notional=(
                _parse_decimal(sizing_payload["target_notional"], "sizing.target_notional")
                if sizing_payload.get("target_notional") is not None
                else None
            ),
            target_quantity=(
                _parse_decimal(sizing_payload["target_quantity"], "sizing.target_quantity")
                if sizing_payload.get("target_quantity") is not None
                else None
            ),
        ),
        entry=EntrySpec(
            order_type=OrderType(str(entry_payload["order_type"]).upper()),
            submit_at=_parse_datetime(entry_payload["submit_at"], "entry.submit_at"),
            expire_at=_parse_datetime(entry_payload["expire_at"], "entry.expire_at"),
            limit_price=(
                _parse_decimal(entry_payload["limit_price"], "entry.limit_price")
                if entry_payload.get("limit_price") is not None
                else None
            ),
            time_in_force=TimeInForce(
                str(entry_payload.get("time_in_force", "DAY")).upper()
            ),
            max_submit_count=int(entry_payload.get("max_submit_count", 1)),
            cancel_unfilled_at_expiry=bool(
                entry_payload.get("cancel_unfilled_at_expiry", True)
            ),
        ),
        exit=ExitSpec(
            take_profit_pct=(
                _parse_decimal(exit_payload["take_profit_pct"], "exit.take_profit_pct")
                if exit_payload.get("take_profit_pct") is not None
                else None
            ),
            stop_loss_pct=(
                _parse_decimal(exit_payload["stop_loss_pct"], "exit.stop_loss_pct")
                if exit_payload.get("stop_loss_pct") is not None
                else None
            ),
            catastrophic_stop_loss_pct=(
                _parse_decimal(
                    exit_payload["catastrophic_stop_loss_pct"],
                    "exit.catastrophic_stop_loss_pct",
                )
                if exit_payload.get("catastrophic_stop_loss_pct") is not None
                else None
            ),
            force_exit_next_session_open=bool(
                exit_payload.get("force_exit_next_session_open", False)
            ),
        ),
        trace=TraceSpec(
            reason_code=str(trace_payload["reason_code"]),
            execution_policy=(
                str(trace_payload["execution_policy"])
                if trace_payload.get("execution_policy") is not None
                else None
            ),
            trade_date=(
                _parse_date(trace_payload["trade_date"], "trace.trade_date")
                if trace_payload.get("trade_date") is not None
                else None
            ),
            data_cutoff_date=(
                _parse_date(trace_payload["data_cutoff_date"], "trace.data_cutoff_date")
                if trace_payload.get("data_cutoff_date") is not None
                else None
            ),
            company_name=(
                str(trace_payload["company_name"])
                if trace_payload.get("company_name") is not None
                else None
            ),
            metadata=_parse_string_map(trace_payload.get("metadata"), "trace.metadata"),
        ),
    )
    instruction.validate()
    return instruction


def parse_execution_batch_payload(payload: Mapping[str, Any]) -> ExecutionInstructionBatch:
    source_payload = payload.get("source")
    if not isinstance(source_payload, Mapping):
        raise ValueError("source must be an object")

    instructions_payload = payload.get("instructions")
    if not isinstance(instructions_payload, list):
        raise ValueError("instructions must be an array")

    batch = ExecutionInstructionBatch(
        schema_version=str(payload["schema_version"]),
        source=SourceContext(
            system=str(source_payload["system"]),
            batch_id=str(source_payload["batch_id"]),
            generated_at=_parse_datetime(source_payload["generated_at"], "source.generated_at"),
            release_id=(
                str(source_payload["release_id"])
                if source_payload.get("release_id") is not None
                else None
            ),
            strategy_id=(
                str(source_payload["strategy_id"])
                if source_payload.get("strategy_id") is not None
                else None
            ),
            policy_id=(
                str(source_payload["policy_id"])
                if source_payload.get("policy_id") is not None
                else None
            ),
        ),
        instructions=tuple(
            parse_execution_instruction_payload(item) for item in instructions_payload
        ),
    )
    batch.validate()
    return batch


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
    enforce_loopback_binding(
        app_config.api.host,
        require_loopback_only=app_config.api.require_loopback_only,
    )
    FastAPI, HTTPException, Request, JSONResponse = _load_fastapi_runtime()

    app = FastAPI(
        title="IBKR Trader Local API",
        version="0.1.0",
        summary="Local-only control plane for the IBKR Trader runtime.",
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
        }

    @app.post("/v1/ibkr/probe")
    def run_ibkr_probe(timeout: int = 5) -> dict[str, Any]:
        try:
            result = probe_gateway(app_config.ibkr.diagnostic_session(), timeout=timeout)
        except IbkrDependencyError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ConnectionError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        return json.loads(result.to_json())

    @app.post("/v1/contracts/resolve")
    def resolve_ibkr_contract(payload: dict[str, Any], timeout: int = 10) -> dict[str, Any]:
        try:
            query = parse_contract_resolve_payload(payload)
            result = resolve_contracts(
                app_config.ibkr.diagnostic_session(),
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

        return serialize_contract_resolve_result(result)

    @app.post("/v1/accounts/summary")
    def get_account_summary(payload: dict[str, Any] | None = None, timeout: int = 10) -> dict[str, Any]:
        request_payload = payload or {}
        try:
            tags, group, account_id = parse_account_summary_payload(request_payload)
            return read_account_summary(
                app_config.ibkr.diagnostic_session(),
                tags=tags,
                group=group,
                account_id=account_id,
                timeout=timeout,
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

    @app.post("/v1/market-data/historical-bars")
    def get_historical_bars(payload: dict[str, Any], timeout: int = 20) -> dict[str, Any]:
        try:
            query = parse_historical_bars_payload(payload)
            return read_historical_bars(
                app_config.ibkr.diagnostic_session(),
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

    @app.post("/v1/orders/preview")
    def preview_orders(payload: dict[str, Any], timeout: int = 10) -> dict[str, Any]:
        try:
            batch = parse_execution_batch_payload(payload)
            return preview_execution_batch(
                app_config.ibkr.diagnostic_session(),
                batch,
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
