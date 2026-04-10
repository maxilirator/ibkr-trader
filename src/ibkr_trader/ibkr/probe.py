from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

from ibkr_trader.config import AppConfig, IbkrConnectionConfig


class IbkrDependencyError(RuntimeError):
    """Raised when the official IBKR Python client is unavailable."""


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


def _load_sync_wrapper_class() -> type[SyncWrapperProtocol]:
    try:
        from ibapi.sync_wrapper import TWSSyncWrapper
    except ModuleNotFoundError as exc:
        raise IbkrDependencyError(
            "The official IBKR Python client is not installed. "
            "Install the current TWS API package from IBKR and make sure "
            "the `ibapi` module is available in this environment."
        ) from exc

    return TWSSyncWrapper


def probe_gateway(
    config: IbkrConnectionConfig,
    *,
    timeout: int = 5,
    sync_wrapper_cls: type[SyncWrapperProtocol] | None = None,
) -> GatewayProbeResult:
    wrapper_cls = sync_wrapper_cls or _load_sync_wrapper_class()
    app = wrapper_cls(timeout=timeout)

    if not app.connect_and_start(
        host=config.host,
        port=config.port,
        client_id=config.client_id,
    ):
        raise ConnectionError(
            f"Failed to connect to IBKR at {config.host}:{config.port} "
            f"with client_id={config.client_id}."
        )

    try:
        broker_current_time = datetime.fromtimestamp(
            app.get_current_time(timeout=timeout),
            tz=UTC,
        )
        next_valid_order_id = app.get_next_valid_id(timeout=timeout)
    finally:
        app.disconnect_and_stop()

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
