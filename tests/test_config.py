from __future__ import annotations

from unittest import TestCase
from unittest.mock import patch

from ibkr_trader.config import IbkrConnectionConfig


class ConfigTests(TestCase):
    def test_ibkr_defaults_match_paper_gateway_recommendation(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            config = IbkrConnectionConfig.from_env()

        self.assertEqual(config.host, "127.0.0.1")
        self.assertEqual(config.port, 4002)
        self.assertEqual(config.client_id, 0)
