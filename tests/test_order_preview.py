from __future__ import annotations

from types import SimpleNamespace
from unittest import TestCase

from ibkr_trader.api.server import parse_execution_batch_payload
from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.ibkr.order_preview import preview_execution_batch


class _FakeContract:
    def __init__(self) -> None:
        self.symbol = ""
        self.secType = ""
        self.exchange = ""
        self.currency = ""
        self.includeExpired = False
        self.primaryExchange = ""
        self.localSymbol = ""
        self.secIdType = ""
        self.secId = ""


class _FakePreviewSyncWrapper:
    def __init__(self, timeout: int) -> None:
        self.timeout = timeout
        self.connected = False
        self.disconnected = False

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
        return {
            "DU1234567": {
                "NetLiquidation": {"value": "100000.00", "currency": "USD"},
                "BuyingPower": {"value": "200000.00", "currency": "USD"},
                "AvailableFunds": {"value": "90000.00", "currency": "USD"},
                "ExcessLiquidity": {"value": "85000.00", "currency": "USD"},
                "AccountType": {"value": "INDIVIDUAL", "currency": ""},
            }
        }

    def get_contract_details(self, contract: _FakeContract, timeout: int | None = None) -> list[object]:
        return [
            SimpleNamespace(
                contract=SimpleNamespace(
                    conId=265598,
                    symbol=contract.symbol,
                    localSymbol=contract.symbol,
                    secType=contract.secType,
                    exchange="SMART",
                    primaryExchange=contract.primaryExchange or "NASDAQ",
                    currency=contract.currency,
                    tradingClass=contract.symbol,
                ),
                marketName="NMS",
                minTick=0.01,
                validExchanges="SMART,NASDAQ",
                orderTypes="ACTIVETIM,ADJUST,ALERT,LMT,MKT",
                timeZoneId="US/Eastern",
                tradingHours="20260410:093000-160000",
                liquidHours="20260410:093000-160000",
                stockType="COMMON",
                industry="Technology",
                category="Computers",
                subcategory="Computers",
                longName="APPLE INC",
                secIdList=[SimpleNamespace(tag="ISIN", value="US0378331005")],
            )
        ]


class OrderPreviewTests(TestCase):
    def test_preview_ready_for_target_quantity(self) -> None:
        batch = parse_execution_batch_payload(
            {
                "schema_version": "2026-04-10",
                "source": {
                    "system": "q-training",
                    "batch_id": "batch-1",
                    "generated_at": "2026-04-10T02:15:44Z",
                },
                "instructions": [
                    {
                        "instruction_id": "demo-1",
                        "account": {
                            "account_key": "GTW05",
                            "book_key": "long_risk_book",
                        },
                        "instrument": {
                            "symbol": "AAPL",
                            "security_type": "STK",
                            "exchange": "SMART",
                            "currency": "USD",
                            "primary_exchange": "NASDAQ",
                        },
                        "intent": {
                            "side": "BUY",
                            "position_side": "LONG",
                        },
                        "sizing": {
                            "mode": "target_quantity",
                            "target_quantity": "10",
                        },
                        "entry": {
                            "order_type": "LIMIT",
                            "submit_at": "2026-04-10T09:25:00-04:00",
                            "expire_at": "2026-04-10T16:00:00-04:00",
                            "limit_price": "180.50",
                        },
                        "exit": {
                            "take_profit_pct": "0.02",
                        },
                        "trace": {
                            "reason_code": "preview-test",
                        },
                    }
                ],
            }
        )

        payload = preview_execution_batch(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=7497,
                client_id=7,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            batch,
            sync_wrapper_cls=_FakePreviewSyncWrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
        )

        preview = payload["previews"][0]
        self.assertEqual(preview["status"], "ready")
        self.assertEqual(preview["order"]["total_quantity"], "10")
        self.assertEqual(preview["order"]["order_type"], "LMT")
        self.assertEqual(preview["instrument"]["resolved"]["con_id"], 265598)

    def test_preview_flags_fx_requirement_for_fraction_sizing(self) -> None:
        batch = parse_execution_batch_payload(
            {
                "schema_version": "2026-04-10",
                "source": {
                    "system": "q-training",
                    "batch_id": "batch-2",
                    "generated_at": "2026-04-10T02:15:44Z",
                },
                "instructions": [
                    {
                        "instruction_id": "demo-2",
                        "account": {
                            "account_key": "GTW05",
                            "book_key": "long_risk_book",
                        },
                        "instrument": {
                            "symbol": "SIVE",
                            "security_type": "STK",
                            "exchange": "SMART",
                            "currency": "SEK",
                            "primary_exchange": "SFB",
                        },
                        "intent": {
                            "side": "BUY",
                            "position_side": "LONG",
                        },
                        "sizing": {
                            "mode": "fraction_of_account_nav",
                            "target_fraction_of_account": "0.10",
                        },
                        "entry": {
                            "order_type": "LIMIT",
                            "submit_at": "2026-04-10T09:25:00+02:00",
                            "expire_at": "2026-04-10T17:30:00+02:00",
                            "limit_price": "11.3131",
                        },
                        "exit": {
                            "take_profit_pct": "0.02",
                        },
                        "trace": {
                            "reason_code": "preview-test",
                        },
                    }
                ],
            }
        )

        payload = preview_execution_batch(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=7497,
                client_id=7,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            batch,
            sync_wrapper_cls=_FakePreviewSyncWrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
        )

        preview = payload["previews"][0]
        self.assertEqual(preview["status"], "unresolved")
        self.assertIn("FX-aware conversion", " ".join(preview["issues"]))
        self.assertIsNone(preview["order"]["total_quantity"])
