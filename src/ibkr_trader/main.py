from __future__ import annotations

from pprint import pprint

from ibkr_trader.config import AppConfig


def main() -> None:
    config = AppConfig.from_env()
    pprint(
        {
            "environment": config.environment,
            "timezone": config.timezone,
            "session_calendar_path": str(config.session_calendar_path),
            "ibkr_host": config.ibkr.host,
            "ibkr_port": config.ibkr.port,
            "account_id_configured": bool(config.ibkr.account_id),
        }
    )


if __name__ == "__main__":
    main()
