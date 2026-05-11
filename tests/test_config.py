from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from ibkr_trader.config import ApiServerConfig, AppConfig, IbkrConnectionConfig, load_dotenv_file
from ibkr_trader.ibkr.client_ids import DIAGNOSTIC_CLIENT_ID
from ibkr_trader.ibkr.client_ids import HISTORICAL_CLIENT_ID
from ibkr_trader.ibkr.client_ids import PRIMARY_RUNTIME_CLIENT_ID
from ibkr_trader.ibkr.client_ids import STREAMING_CLIENT_ID


class ConfigTests(TestCase):
    def test_ibkr_defaults_match_canonical_client_id_policy(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            config = IbkrConnectionConfig.from_env()

        self.assertEqual(config.host, "127.0.0.1")
        self.assertEqual(config.port, 7497)
        self.assertEqual(config.client_id, PRIMARY_RUNTIME_CLIENT_ID)
        self.assertEqual(config.diagnostic_client_id, DIAGNOSTIC_CLIENT_ID)
        self.assertEqual(config.historical_client_id, HISTORICAL_CLIENT_ID)
        self.assertEqual(config.streaming_client_id, STREAMING_CLIENT_ID)

    def test_ibkr_connection_can_create_diagnostic_session(self) -> None:
        config = IbkrConnectionConfig(
            host="127.0.0.1",
            port=7497,
            client_id=PRIMARY_RUNTIME_CLIENT_ID,
            diagnostic_client_id=DIAGNOSTIC_CLIENT_ID,
            historical_client_id=HISTORICAL_CLIENT_ID,
            streaming_client_id=STREAMING_CLIENT_ID,
            account_id="DU1234567",
        )

        diagnostic = config.diagnostic_session()

        self.assertEqual(diagnostic.client_id, DIAGNOSTIC_CLIENT_ID)
        self.assertEqual(diagnostic.host, "127.0.0.1")
        self.assertEqual(diagnostic.port, 7497)

    def test_ibkr_connection_can_create_historical_session(self) -> None:
        config = IbkrConnectionConfig(
            host="127.0.0.1",
            port=7497,
            client_id=PRIMARY_RUNTIME_CLIENT_ID,
            diagnostic_client_id=DIAGNOSTIC_CLIENT_ID,
            historical_client_id=HISTORICAL_CLIENT_ID,
            streaming_client_id=STREAMING_CLIENT_ID,
            account_id="DU1234567",
        )

        historical = config.historical_session()

        self.assertEqual(historical.client_id, HISTORICAL_CLIENT_ID)
        self.assertEqual(historical.host, "127.0.0.1")
        self.assertEqual(historical.port, 7497)

    def test_ibkr_connection_can_create_streaming_session(self) -> None:
        config = IbkrConnectionConfig(
            host="127.0.0.1",
            port=7497,
            client_id=PRIMARY_RUNTIME_CLIENT_ID,
            diagnostic_client_id=DIAGNOSTIC_CLIENT_ID,
            historical_client_id=HISTORICAL_CLIENT_ID,
            streaming_client_id=STREAMING_CLIENT_ID,
            account_id="DU1234567",
        )

        streaming = config.streaming_session()

        self.assertEqual(streaming.client_id, STREAMING_CLIENT_ID)
        self.assertEqual(streaming.host, "127.0.0.1")
        self.assertEqual(streaming.port, 7497)

    def test_api_defaults_match_local_only_expectation(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            config = ApiServerConfig.from_env()

        self.assertEqual(config.host, "127.0.0.1")
        self.assertEqual(config.port, 8000)
        self.assertTrue(config.require_loopback_only)

    def test_app_defaults_use_stockholm_timezone(self) -> None:
        with patch("ibkr_trader.config.load_dotenv_file"), patch.dict("os.environ", {}, clear=True):
            config = AppConfig.from_env()

        self.assertEqual(config.timezone, "Europe/Stockholm")
        self.assertTrue(str(config.session_calendar_path).endswith("/q-data/xsto/calendars/day_sessions.parquet"))
        self.assertTrue(str(config.stockholm_instruments_path).endswith("/q-data/xsto/instruments/all.txt"))
        self.assertTrue(str(config.stockholm_identity_path).endswith("/q-data/xsto/meta/instrument_identity.parquet"))
        self.assertEqual(config.ibkr_api_max_requests_per_second, 45)
        self.assertEqual(config.broker_api_startup_failure_slow_probe_seconds, 900)
        self.assertEqual(config.ibkr_market_data_line_limit, 80)
        self.assertEqual(config.ibkr_historical_requests_per_10_minutes, 50)
        self.assertEqual(config.effective_market_stream_max_subscriptions, 80)

    def test_market_data_line_limit_caps_stream_subscription_limit(self) -> None:
        with patch("ibkr_trader.config.load_dotenv_file"), patch.dict(
            "os.environ",
            {
                "MARKET_STREAM_MAX_SUBSCRIPTIONS": "120",
                "IBKR_MARKET_DATA_LINE_LIMIT": "70",
            },
            clear=True,
        ):
            config = AppConfig.from_env()

        self.assertEqual(config.market_stream_max_subscriptions, 120)
        self.assertEqual(config.ibkr_market_data_line_limit, 70)
        self.assertEqual(config.effective_market_stream_max_subscriptions, 70)

    def test_dotenv_file_populates_missing_values(self) -> None:
        with TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "APP_ENV=paper",
                        "API_PORT=8100",
                        "IBKR_ACCOUNT_ID=DU1234567",
                    ]
                ),
                encoding="utf-8",
            )

            with patch.dict("os.environ", {}, clear=True):
                load_dotenv_file(env_path)
                config = AppConfig.from_env()

        self.assertEqual(config.environment, "paper")
        self.assertEqual(config.api.port, 8100)
        self.assertEqual(config.ibkr.account_id, "DU1234567")

    def test_real_environment_overrides_dotenv(self) -> None:
        with TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text("APP_ENV=paper\n", encoding="utf-8")

            with patch.dict("os.environ", {"APP_ENV": "live"}, clear=True):
                load_dotenv_file(env_path)
                config = AppConfig.from_env()

        self.assertEqual(config.environment, "live")
