from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest import TestCase

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.domain.contract_resolution import ContractResolveQuery
from ibkr_trader.ibkr.contracts import (
    build_ibkr_contract,
    resolve_contracts,
    serialize_contract_resolve_result,
)


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


class _FakeSyncWrapper:
    last_contract: _FakeContract | None = None

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

    def get_contract_details(self, contract: _FakeContract, timeout: int | None = None) -> list[object]:
        type(self).last_contract = contract
        return [
            SimpleNamespace(
                contract=SimpleNamespace(
                    conId=38708077,
                    symbol="SIVE",
                    localSymbol="SIVE",
                    secType="STK",
                    exchange="SMART",
                    primaryExchange="XSTO",
                    currency="SEK",
                    tradingClass="SIVE",
                ),
                marketName="NORDIC",
                minTick=0.0001,
                validExchanges="SMART,XSTO",
                orderTypes="ACTIVETIM,ADJUST,ALERT",
                timeZoneId="Europe/Stockholm",
                tradingHours="20260410:090000-173000",
                liquidHours="20260410:090000-173000",
                stockType="COMMON",
                industry="Technology",
                category="Semiconductors",
                subcategory="Fabless",
                longName="SIVERS SEMICONDUCTORS AB",
                secIdList=[
                    SimpleNamespace(tag="ISIN", value="SE0003917798"),
                    SimpleNamespace(tag="FIGI", value="BBG000000001"),
                ],
            )
        ]


class ContractResolverTests(TestCase):
    def test_build_ibkr_contract_sets_expected_fields(self) -> None:
        query = ContractResolveQuery(
            symbol="SIVE",
            security_type="STK",
            exchange="XSTO",
            currency="SEK",
            primary_exchange="XSTO",
            local_symbol="SIVE",
            include_expired=False,
            isin="SE0003917798",
        )

        contract = build_ibkr_contract(query, contract_cls=_FakeContract)

        self.assertEqual(contract.symbol, "SIVE")
        self.assertEqual(contract.secType, "STK")
        self.assertEqual(contract.exchange, "XSTO")
        self.assertEqual(contract.currency, "SEK")
        self.assertEqual(contract.primaryExchange, "XSTO")
        self.assertEqual(contract.localSymbol, "SIVE")
        self.assertEqual(contract.secIdType, "ISIN")
        self.assertEqual(contract.secId, "SE0003917798")

    def test_resolve_contracts_returns_broker_matches(self) -> None:
        query = ContractResolveQuery(
            symbol="SIVE",
            security_type="STK",
            exchange="XSTO",
            currency="SEK",
            isin="SE0003917798",
        )
        config = IbkrConnectionConfig(
            host="127.0.0.1",
            port=7497,
            client_id=0,
            diagnostic_client_id=7,
            account_id="DU1234567",
        )

        result = resolve_contracts(
            config,
            query,
            timeout=5,
            sync_wrapper_cls=_FakeSyncWrapper,
            contract_cls=_FakeContract,
            response_timeout_cls=TimeoutError,
        )

        self.assertEqual(result.match_count, 1)
        self.assertTrue(result.is_unique)
        self.assertEqual(result.matches[0].symbol, "SIVE")
        self.assertEqual(result.matches[0].primary_exchange, "XSTO")
        self.assertEqual(result.matches[0].sec_ids["ISIN"], "SE0003917798")
        self.assertEqual(result.matches[0].min_tick, Decimal("0.0001"))
        self.assertEqual(_FakeSyncWrapper.last_contract.secIdType, "ISIN")

    def test_serialize_contract_resolve_result_flattens_json_shape(self) -> None:
        query = ContractResolveQuery(
            symbol="AAPL",
            security_type="STK",
            exchange="SMART",
            currency="USD",
        )
        result = resolve_contracts(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=7497,
                client_id=0,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            query,
            timeout=5,
            sync_wrapper_cls=_FakeSyncWrapper,
            contract_cls=_FakeContract,
            response_timeout_cls=TimeoutError,
        )

        payload = serialize_contract_resolve_result(result)

        self.assertEqual(payload["match_count"], 1)
        self.assertTrue(payload["is_unique"])
        self.assertEqual(payload["matches"][0]["min_tick"], "0.0001")
