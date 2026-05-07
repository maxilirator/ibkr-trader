from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Callable, Protocol, TypeVar, runtime_checkable

from ibkr_trader.config import AppConfig, IbkrConnectionConfig
from ibkr_trader.ibkr.sync_wrapper import (
    load_response_timeout_class as _load_response_timeout_class,
)
from ibkr_trader.ibkr.sync_wrapper import load_sync_wrapper_class as _load_sync_wrapper_class

LOGGER = logging.getLogger(__name__)
_T = TypeVar("_T")


@runtime_checkable
class SyncWrapperProtocol(Protocol):
    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool: ...

    def disconnect_and_stop(self) -> None: ...

    def get_current_time(self, *, timeout: int | None = None) -> int: ...

    def get_next_valid_id(self, *, timeout: int | None = None) -> int: ...


@dataclass(slots=True)
class GatewayProbeResult:
    host: str
    port: int
    client_id: int
    broker_current_time: datetime
    next_valid_order_id: int

    def to_json(self) -> str:
        payload = asdict(self)
        payload["broker_current_time"] = self.broker_current_time.isoformat()
        return json.dumps(payload, indent=2, sort_keys=True)


def _elapsed_ms(started_at: float) -> int:
    return max(0, int((time.monotonic() - started_at) * 1000))


def _run_probe_step(
    step_name: str,
    call: Callable[[], _T],
    *,
    config: IbkrConnectionConfig,
    timeout: int,
    timeout_cls: type[Exception],
) -> _T:
    started_at = time.monotonic()
    try:
        return call()
    except timeout_cls as exc:
        elapsed_ms = _elapsed_ms(started_at)
        LOGGER.warning(
            "IBKR Gateway probe timed out during %s: host=%s port=%s "
            "client_id=%s timeout_seconds=%s elapsed_ms=%s error=%s",
            step_name,
            config.host,
            config.port,
            config.client_id,
            timeout,
            elapsed_ms,
            exc,
        )
        raise TimeoutError(
            "Connected to IBKR, but the Gateway did not answer the probe requests "
            f"during {step_name} within {timeout}s."
        ) from exc


def probe_gateway(
    config: IbkrConnectionConfig,
    *,
    timeout: int = 5,
    sync_wrapper_cls: type[SyncWrapperProtocol] | None = None,
    response_timeout_cls: type[Exception] | None = None,
    app: SyncWrapperProtocol | None = None,
) -> GatewayProbeResult:
    timeout_cls = response_timeout_cls
    if timeout_cls is None:
        timeout_cls = TimeoutError if sync_wrapper_cls is not None else _load_response_timeout_class()
    runtime_app = app
    owns_connection = runtime_app is None
    if runtime_app is None:
        wrapper_cls = sync_wrapper_cls or _load_sync_wrapper_class()
        runtime_app = wrapper_cls(timeout=timeout)
        if not runtime_app.connect_and_start(
            host=config.host,
            port=config.port,
            client_id=config.client_id,
        ):
            LOGGER.warning(
                "IBKR Gateway probe connection failed: host=%s port=%s client_id=%s",
                config.host,
                config.port,
                config.client_id,
            )
            raise ConnectionError(
                f"Failed to connect to IBKR at {config.host}:{config.port} "
                f"with client_id={config.client_id}."
            )

    try:
        raw_broker_current_time = _run_probe_step(
            "current_time",
            lambda: runtime_app.get_current_time(timeout=timeout),
            config=config,
            timeout=timeout,
            timeout_cls=timeout_cls,
        )
        broker_current_time = datetime.fromtimestamp(
            raw_broker_current_time,
            tz=UTC,
        )
        next_valid_order_id = _run_probe_step(
            "next_valid_id",
            lambda: runtime_app.get_next_valid_id(timeout=timeout),
            config=config,
            timeout=timeout,
            timeout_cls=timeout_cls,
        )
    finally:
        if owns_connection:
            runtime_app.disconnect_and_stop()

    return GatewayProbeResult(
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        broker_current_time=broker_current_time,
        next_valid_order_id=next_valid_order_id,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Probe an IB Gateway / TWS API session using the official IBKR client."
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=5,
        help="Request timeout in seconds for the probe calls.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = AppConfig.from_env().ibkr.diagnostic_session()
    result = probe_gateway(config, timeout=args.timeout)
    print(result.to_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
