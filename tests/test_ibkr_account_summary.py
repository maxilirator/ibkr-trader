from __future__ import annotations

from unittest import TestCase

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.ibkr.account_summary import (
    DEFAULT_ACCOUNT_SUMMARY_TAGS,
    normalize_account_summary_payload,
    read_account_summary,
)


class _FakeAccountSummarySyncWrapper:
    def __init__(self, timeout: int) -> None:
        self.timeout = timeout
        self.connected = False
        self.disconnected = False
        self.requested_tags: str | None = None
        self.requested_group: str | None = None
        self.connection_args: tuple[str, int, int] | None = None

    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool:
        self.connected = True
        self.connection_args = (host, port, client_id)
        return True

    def disconnect_and_stop(self) -> None:
        self.disconnected = True

    def get_account_summary(
        self,
        tags: str,
        group: str = "All",
        timeout: int = 5,
    ) -> dict[str, dict[str, dict[str, str]]]:
        self.requested_tags = tags
        self.requested_group = group
        return {
            "DU1234567": {
                "NetLiquidation": {"value": "125000.45", "currency": "USD"},
                "BuyingPower": {"value": "250000.90", "currency": "USD"},
                "AccountType": {"value": "INDIVIDUAL", "currency": ""},
            },
            "U9999999": {
                "NetLiquidation": {"value": "5.00", "currency": "USD"},
            },
        }


class AccountSummaryTests(TestCase):
    def test_normalize_account_summary_payload_filters_by_account(self) -> None:
        payload = normalize_account_summary_payload(
            {
                "DU1234567": {
                    "NetLiquidation": {"value": "125000.45", "currency": "USD"},
                    "BuyingPower": {"value": "250000.90", "currency": "USD"},
                },
                "U9999999": {
                    "NetLiquidation": {"value": "5.00", "currency": "USD"},
                },
            },
            requested_tags=("NetLiquidation", "BuyingPower"),
            account_id="DU1234567",
            group="All",
        )

        self.assertEqual(payload["group"], "All")
        self.assertEqual(payload["account_filter"], "DU1234567")
        self.assertEqual(sorted(payload["accounts"].keys()), ["DU1234567"])
        self.assertEqual(
            payload["accounts"]["DU1234567"]["NetLiquidation"]["value"],
            "125000.45",
        )

    def test_read_account_summary_uses_requested_tags(self) -> None:
        config = IbkrConnectionConfig(
            host="127.0.0.1",
            port=7497,
            client_id=7,
            diagnostic_client_id=7,
            account_id="DU1234567",
        )

        payload = read_account_summary(
            config,
            tags=("NetLiquidation", "BuyingPower"),
            account_id="DU1234567",
            sync_wrapper_cls=_FakeAccountSummarySyncWrapper,
            response_timeout_cls=TimeoutError,
        )

        self.assertEqual(payload["requested_tags"], ["NetLiquidation", "BuyingPower"])
        self.assertEqual(list(payload["accounts"].keys()), ["DU1234567"])
        self.assertEqual(
            payload["accounts"]["DU1234567"]["BuyingPower"]["currency"],
            "USD",
        )

    def test_default_tags_include_nav_fields(self) -> None:
        self.assertIn("NetLiquidation", DEFAULT_ACCOUNT_SUMMARY_TAGS)
        self.assertIn("BuyingPower", DEFAULT_ACCOUNT_SUMMARY_TAGS)
        self.assertIn("AvailableFunds", DEFAULT_ACCOUNT_SUMMARY_TAGS)
