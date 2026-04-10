from __future__ import annotations

from types import SimpleNamespace
from unittest import TestCase

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.ibkr.historical_bars import HistoricalBarsQuery, read_historical_bars


class _FakeContract:
    def __init__(self) -> None:
        self.conId = 0
        self.symbol = ""
        self.localSymbol = ""
        self.secType = ""
        self.exchange = ""
        self.primaryExchange = ""
        self.currency = ""
        self.tradingClass = ""
        self.includeExpired = False
        self.secIdType = ""
        self.secId = ""


class _FakeHistoricalBarsSyncWrapper:
    def __init__(self, timeout: int) -> None:
        self.timeout = timeout
        self.connected = False
        self.disconnected = False
        self.contract_details: list[object] = []
        self.errors: dict[int, list[dict[str, object]]] = {}

    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool:
        self.connected = True
        self.connection_args = (host, port, client_id)
        return True

    def disconnect_and_stop(self) -> None:
        self.disconnected = True

    def get_contract_details(self, contract: _FakeContract, timeout: int | None = None) -> list[object]:
        return [
            SimpleNamespace(
                contract=SimpleNamespace(
                    conId=489,
                    symbol=contract.symbol,
                    localSymbol=contract.symbol,
                    secType=contract.secType,
                    exchange="SMART",
                    primaryExchange=contract.primaryExchange or "SFB",
                    currency=contract.currency,
                    tradingClass=contract.symbol,
                ),
                marketName="SFB",
                minTick=0.0001,
                validExchanges="SMART,SFB",
                orderTypes="LMT,MKT",
                timeZoneId="Europe/Stockholm",
                tradingHours="20260410:090000-173000",
                liquidHours="20260410:090000-173000",
                stockType="COMMON",
                industry="Semiconductors",
                category="Technology",
                subcategory="Components",
                longName="SIVERS SEMICONDUCTORS",
                secIdList=[SimpleNamespace(tag="ISIN", value="SE0003917798")],
            )
        ]

    def get_historical_data(
        self,
        contract: _FakeContract,
        end_date_time: str,
        duration_str: str,
        bar_size_setting: str,
        what_to_show: str,
        use_rth: bool = True,
        format_date: int = 1,
        timeout: int | None = None,
    ) -> list[object]:
        self.historical_args = (
            contract.symbol,
            end_date_time,
            duration_str,
            bar_size_setting,
            what_to_show,
            use_rth,
            format_date,
            timeout,
        )
        return [
            SimpleNamespace(
                date="20260410 09:00:00",
                open="11.10",
                high="11.30",
                low="11.05",
                close="11.20",
                volume="12000",
                wap="11.18",
                barCount="47",
            ),
            SimpleNamespace(
                date="20260410 09:05:00",
                open="11.20",
                high="11.35",
                low="11.18",
                close="11.31",
                volume="9400",
                wap="11.27",
                barCount="39",
            ),
        ]


class HistoricalBarsTests(TestCase):
    def test_read_historical_bars_returns_native_currency_bars(self) -> None:
        payload = read_historical_bars(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=7497,
                client_id=7,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            HistoricalBarsQuery(
                symbol="SIVE",
                security_type="STK",
                exchange="SMART",
                currency="SEK",
                primary_exchange="SFB",
                isin="SE0003917798",
                duration="2 D",
                bar_size="5 mins",
                what_to_show="TRADES",
                use_rth=True,
            ),
            sync_wrapper_cls=_FakeHistoricalBarsSyncWrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
        )

        self.assertEqual(payload["currency"], "SEK")
        self.assertEqual(payload["bar_count"], 2)
        self.assertEqual(payload["resolved_contract"]["primary_exchange"], "SFB")
        self.assertEqual(payload["bars"][0]["currency"], "SEK")
        self.assertEqual(payload["bars"][1]["close"], "11.31")

    def test_read_historical_bars_formats_end_at_for_ibkr(self) -> None:
        wrapper = _FakeHistoricalBarsSyncWrapper(timeout=20)

        class _WrapperFactory:
            def __init__(self, instance: _FakeHistoricalBarsSyncWrapper) -> None:
                self.instance = instance

            def __call__(self, timeout: int) -> _FakeHistoricalBarsSyncWrapper:
                self.instance.timeout = timeout
                return self.instance

        from datetime import datetime

        read_historical_bars(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=7497,
                client_id=7,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            HistoricalBarsQuery(
                symbol="SIVE",
                security_type="STK",
                exchange="SMART",
                currency="SEK",
                duration="2 D",
                bar_size="1 day",
                what_to_show="TRADES",
                use_rth=True,
                end_at=datetime.fromisoformat("2026-04-10T17:30:00+02:00"),
            ),
            sync_wrapper_cls=_WrapperFactory(wrapper),
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
        )

        self.assertEqual(wrapper.historical_args[1], "20260410-15:30:00 UTC")
