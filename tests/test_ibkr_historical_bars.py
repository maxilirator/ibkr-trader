from __future__ import annotations

from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.ibkr.historical_bars import (
    CachedContractLookupError,
    HistoricalBarsQuery,
    NegativeContractCache,
    read_historical_bars,
)


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
        self.contract_detail_call_count = 0
        self.errors: dict[int, list[dict[str, object]]] = {}

    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool:
        self.connected = True
        self.connection_args = (host, port, client_id)
        return True

    def disconnect_and_stop(self) -> None:
        self.disconnected = True

    def get_contract_details(self, contract: _FakeContract, timeout: int | None = None) -> list[object]:
        self.contract_detail_call_count += 1
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


class _FakeContractLookupFailureWrapper:
    """Simulates a contract lookup that always fails via a broker timeout.

    `errors` is populated the way `_extract_latest_broker_error` expects:
    a request-id-keyed dict of error dicts with `errorCode`/`errorString`.
    Setting `next_error_code = None` simulates a timeout with no broker error
    recorded at all (a plain connectivity/timeout failure).
    """

    def __init__(self, timeout: int) -> None:
        self.timeout = timeout
        self.connected = False
        self.disconnected = False
        self.contract_detail_call_count = 0
        self.errors: dict[int, list[dict[str, object]]] = {}
        self.next_error_code: int | None = 200
        self.next_error_string: str = "No security definition has been found for the request"

    def connect_and_start(self, *, host: str, port: int, client_id: int) -> bool:
        self.connected = True
        return True

    def disconnect_and_stop(self) -> None:
        self.disconnected = True

    def get_contract_details(self, contract: _FakeContract, timeout: int | None = None) -> list[object]:
        self.contract_detail_call_count += 1
        if self.next_error_code is not None:
            self.errors = {
                0: [{"errorCode": self.next_error_code, "errorString": self.next_error_string}]
            }
        else:
            self.errors = {}
        raise TimeoutError("simulated contract lookup timeout")

    def get_historical_data(self, *args: object, **kwargs: object) -> list[object]:
        raise AssertionError("historical data should not be requested after a lookup failure")


def _abera_query() -> HistoricalBarsQuery:
    return HistoricalBarsQuery(
        symbol="ABERA",
        security_type="STK",
        exchange="SMART",
        currency="SEK",
        primary_exchange="SFB",
        duration="1 D",
        bar_size="1 min",
        what_to_show="TRADES",
        use_rth=True,
    )


def _connection_config() -> IbkrConnectionConfig:
    return IbkrConnectionConfig(
        host="127.0.0.1",
        port=7497,
        client_id=7,
        diagnostic_client_id=7,
        account_id="DU1234567",
    )


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

        self.assertEqual(wrapper.historical_args[1], "20260410-15:30:00")

    def test_read_historical_bars_can_reuse_contract_details_cache(self) -> None:
        wrapper = _FakeHistoricalBarsSyncWrapper(timeout=20)
        cache = {}
        query = HistoricalBarsQuery(
            symbol="SIVE",
            security_type="STK",
            exchange="SMART",
            currency="SEK",
            primary_exchange="SFB",
            isin="SE0003917798",
            duration="1 D",
            bar_size="1 min",
            what_to_show="TRADES",
            use_rth=True,
        )

        read_historical_bars(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=7497,
                client_id=7,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            query,
            sync_wrapper_cls=lambda timeout: wrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
            contract_details_cache=cache,
        )
        read_historical_bars(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=7497,
                client_id=7,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            HistoricalBarsQuery(
                symbol=query.symbol,
                security_type=query.security_type,
                exchange=query.exchange,
                currency=query.currency,
                primary_exchange=query.primary_exchange,
                isin=query.isin,
                duration=query.duration,
                bar_size=query.bar_size,
                what_to_show="MIDPOINT",
                use_rth=query.use_rth,
            ),
            sync_wrapper_cls=lambda timeout: wrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
            contract_details_cache=cache,
        )

        self.assertEqual(wrapper.contract_detail_call_count, 1)

    def test_terminal_error_200_is_cached_and_second_lookup_makes_no_broker_call(self) -> None:
        wrapper = _FakeContractLookupFailureWrapper(timeout=5)
        negative_cache = NegativeContractCache()
        query = _abera_query()

        with self.assertRaises(LookupError):
            read_historical_bars(
                _connection_config(),
                query,
                sync_wrapper_cls=lambda timeout: wrapper,
                response_timeout_cls=TimeoutError,
                contract_cls=_FakeContract,
                negative_contract_cache=negative_cache,
            )
        self.assertEqual(wrapper.contract_detail_call_count, 1)

        with self.assertRaises(CachedContractLookupError):
            read_historical_bars(
                _connection_config(),
                query,
                sync_wrapper_cls=lambda timeout: wrapper,
                response_timeout_cls=TimeoutError,
                contract_cls=_FakeContract,
                negative_contract_cache=negative_cache,
            )
        # No second broker call: the negative cache short-circuited it.
        self.assertEqual(wrapper.contract_detail_call_count, 1)

    def test_plain_timeout_without_broker_error_is_not_cached_and_is_retried(self) -> None:
        wrapper = _FakeContractLookupFailureWrapper(timeout=5)
        wrapper.next_error_code = None
        negative_cache = NegativeContractCache()
        query = _abera_query()

        for expected_call_count in (1, 2):
            with self.assertRaises(TimeoutError):
                read_historical_bars(
                    _connection_config(),
                    query,
                    sync_wrapper_cls=lambda timeout: wrapper,
                    response_timeout_cls=TimeoutError,
                    contract_cls=_FakeContract,
                    negative_contract_cache=negative_cache,
                )
            self.assertEqual(wrapper.contract_detail_call_count, expected_call_count)

    def test_non_terminal_broker_error_code_is_not_cached_and_is_retried(self) -> None:
        # 1100/1101/1102 connectivity errors arrive on reqId -1 and are already
        # filtered out by `_extract_latest_broker_error`. This covers the other
        # retryable case: a request-scoped error that is not "no security
        # definition" - a pacing rejection - which must stay retryable too.
        wrapper = _FakeContractLookupFailureWrapper(timeout=5)
        wrapper.next_error_code = 100
        wrapper.next_error_string = "Max rate of messages per second exceeded"
        negative_cache = NegativeContractCache()
        query = _abera_query()

        for expected_call_count in (1, 2):
            with self.assertRaises(LookupError):
                read_historical_bars(
                    _connection_config(),
                    query,
                    sync_wrapper_cls=lambda timeout: wrapper,
                    response_timeout_cls=TimeoutError,
                    contract_cls=_FakeContract,
                    negative_contract_cache=negative_cache,
                )
            self.assertEqual(wrapper.contract_detail_call_count, expected_call_count)

    def test_negative_cache_entry_expires_after_ttl(self) -> None:
        negative_cache = NegativeContractCache()
        cache_key = ("ABERA", "STK", "SMART", "SEK", "SFB", None, None)
        fake_time = [1_000.0]

        with patch(
            "ibkr_trader.ibkr.historical_bars.time.monotonic",
            side_effect=lambda: fake_time[0],
        ):
            negative_cache.set(
                cache_key,
                "[200] No security definition has been found for the request",
                ttl_seconds=10.0,
            )
            self.assertIsNotNone(negative_cache.get(cache_key))

            fake_time[0] += 11.0
            self.assertIsNone(negative_cache.get(cache_key))

    def test_ttl_expiry_triggers_a_fresh_broker_lookup(self) -> None:
        wrapper = _FakeContractLookupFailureWrapper(timeout=5)
        negative_cache = NegativeContractCache()
        query = _abera_query()
        fake_time = [1_000.0]

        with patch(
            "ibkr_trader.ibkr.historical_bars.time.monotonic",
            side_effect=lambda: fake_time[0],
        ):
            with self.assertRaises(LookupError):
                read_historical_bars(
                    _connection_config(),
                    query,
                    sync_wrapper_cls=lambda timeout: wrapper,
                    response_timeout_cls=TimeoutError,
                    contract_cls=_FakeContract,
                    negative_contract_cache=negative_cache,
                    negative_contract_cache_ttl_seconds=10.0,
                )
            self.assertEqual(wrapper.contract_detail_call_count, 1)

            with self.assertRaises(CachedContractLookupError):
                read_historical_bars(
                    _connection_config(),
                    query,
                    sync_wrapper_cls=lambda timeout: wrapper,
                    response_timeout_cls=TimeoutError,
                    contract_cls=_FakeContract,
                    negative_contract_cache=negative_cache,
                    negative_contract_cache_ttl_seconds=10.0,
                )
            self.assertEqual(wrapper.contract_detail_call_count, 1)

            fake_time[0] += 11.0

            with self.assertRaises(LookupError):
                read_historical_bars(
                    _connection_config(),
                    query,
                    sync_wrapper_cls=lambda timeout: wrapper,
                    response_timeout_cls=TimeoutError,
                    contract_cls=_FakeContract,
                    negative_contract_cache=negative_cache,
                    negative_contract_cache_ttl_seconds=10.0,
                )
            # TTL expired: the cache no longer short-circuits, so a fresh
            # broker call happens.
            self.assertEqual(wrapper.contract_detail_call_count, 2)

    def test_negative_caching_disabled_when_ttl_is_non_positive(self) -> None:
        wrapper = _FakeContractLookupFailureWrapper(timeout=5)
        negative_cache = NegativeContractCache()
        query = _abera_query()

        for _ in range(2):
            with self.assertRaises(LookupError):
                read_historical_bars(
                    _connection_config(),
                    query,
                    sync_wrapper_cls=lambda timeout: wrapper,
                    response_timeout_cls=TimeoutError,
                    contract_cls=_FakeContract,
                    negative_contract_cache=negative_cache,
                    negative_contract_cache_ttl_seconds=0.0,
                )

        self.assertEqual(wrapper.contract_detail_call_count, 2)
        self.assertIsNone(negative_cache.get(("ABERA", "STK", "SMART", "SEK", "SFB", None, None)))
