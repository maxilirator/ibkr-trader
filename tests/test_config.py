from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from ibkr_trader.config import ApiServerConfig, AppConfig, IbkrConnectionConfig, load_dotenv_file


class ConfigTests(TestCase):
    def test_ibkr_defaults_match_paper_gateway_recommendation(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            config = IbkrConnectionConfig.from_env()

        self.assertEqual(config.host, "127.0.0.1")
        self.assertEqual(config.port, 4002)
        self.assertEqual(config.client_id, 0)

    def test_api_defaults_match_local_only_expectation(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            config = ApiServerConfig.from_env()

        self.assertEqual(config.host, "127.0.0.1")
        self.assertEqual(config.port, 8000)
        self.assertTrue(config.require_loopback_only)

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
