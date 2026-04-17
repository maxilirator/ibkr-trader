from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

from ibkr_trader.config import AppConfig, IbkrConnectionConfig
from ibkr_trader.ibkr.errors import IbkrDependencyError
from ibkr_trader.ibkr.sync_wrapper import (
    load_response_timeout_class as _load_response_timeout_class,
)
from ibkr_trader.ibkr.sync_wrapper import load_sync_wrapper_class as _load_sync_wrapper_class


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


def probe_gateway(
    config: IbkrConnectionConfig,
    *,
    timeout: int = 5,
    sync_wrapper_cls: type[SyncWrapperProtocol] | None = None,
    response_timeout_cls: type[Exception] | None = None,
    app: SyncWrapperProtocol | None = None,
) -> GatewayProbeResult:
    timeout_cls = response_timeout_cls or _load_response_timeout_class()
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
            raise ConnectionError(
                f"Failed to connect to IBKR at {config.host}:{config.port} "
                f"with client_id={config.client_id}."
            )

    try:
        try:
            broker_current_time = datetime.fromtimestamp(
                runtime_app.get_current_time(timeout=timeout),
                tz=UTC,
            )
            next_valid_order_id = runtime_app.get_next_valid_id(timeout=timeout)
        except timeout_cls as exc:
            raise TimeoutError(
                "Connected to IBKR, but the Gateway did not answer the probe requests."
            ) from exc
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
