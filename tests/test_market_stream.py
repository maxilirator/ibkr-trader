from __future__ import annotations

from datetime import UTC
from datetime import datetime
from unittest import TestCase
from unittest.mock import patch

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.ibkr.broker_circuit import BrokerHealthCircuit
from ibkr_trader.ibkr.market_stream import LiveMarketDataStreamService
from ibkr_trader.ibkr.market_stream import MarketStreamContract
from ibkr_trader.ibkr.market_stream import MarketStreamQuote
from ibkr_trader.ibkr.market_stream import MarketStreamSubscription
from ibkr_trader.ibkr.market_stream import _normalize_ib_error_args
from ibkr_trader.ibkr.pacing import BrokerApiPacingGovernor
from ibkr_trader.ibkr.pacing import BrokerPacingConfig
from ibkr_trader.ibkr.pacing import BrokerPacingLimitExceeded


class MarketStreamTests(TestCase):
    def test_normalize_ib_error_args_accepts_common_ibapi_shapes(self) -> None:
        self.assertEqual(
            _normalize_ib_error_args((2104, "Market data farm connection is OK")),
            (None, 2104, "Market data farm connection is OK", ""),
        )
        self.assertEqual(
            _normalize_ib_error_args((1714314495, 2104, "Market data farm connection is OK")),
            (1714314495, 2104, "Market data farm connection is OK", ""),
        )
        self.assertEqual(
            _normalize_ib_error_args((2104, "Market data farm connection is OK", "{}")),
            (None, 2104, "Market data farm connection is OK", "{}"),
        )

    def test_stream_ticks_build_one_minute_bars(self) -> None:
        service = LiveMarketDataStreamService(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4002,
                client_id=9,
                diagnostic_client_id=7,
                account_id="DU1234567",
            )
        )
        contract = MarketStreamContract(symbol="AXFO")
        service._subscriptions_by_key["AXFO"] = MarketStreamSubscription(
            request_id=100,
            contract=contract,
            subscribed_at=datetime(2026, 4, 28, 7, 0, tzinfo=UTC),
        )
        service._subscription_keys_by_req_id[100] = "AXFO"
        service._quotes_by_key["AXFO"] = MarketStreamQuote(
            symbol="AXFO",
            exchange="SMART",
            currency="SEK",
            security_type="STK",
            primary_exchange="SFB",
        )

        with patch(
            "ibkr_trader.ibkr.market_stream._utc_now",
            side_effect=[
                datetime(2026, 4, 28, 7, 1, 10, tzinfo=UTC),
                datetime(2026, 4, 28, 7, 1, 40, tzinfo=UTC),
                datetime(2026, 4, 28, 7, 2, 5, tzinfo=UTC),
            ],
        ):
            service._on_tick_price(req_id=100, tick_type=4, price=100)
            service._on_tick_price(req_id=100, tick_type=4, price=101)
            service._on_tick_price(req_id=100, tick_type=4, price=99)

        snapshot = service.snapshot(symbols=["AXFO"], bar_limit=10)

        self.assertEqual(snapshot["quote_count"], 1)
        self.assertEqual(snapshot["quotes"][0]["last_price"], "99")
        bars = snapshot["bars_by_symbol"]["AXFO"]
        self.assertEqual(len(bars), 2)
        self.assertEqual(bars[0]["timestamp"], "2026-04-28T07:01:00+00:00")
        self.assertEqual(bars[0]["open"], "100")
        self.assertEqual(bars[0]["high"], "101")
        self.assertEqual(bars[0]["low"], "100")
        self.assertEqual(bars[0]["close"], "101")
        self.assertEqual(bars[0]["bar_count"], "2")
        self.assertEqual(bars[1]["open"], "99")

    def test_failed_connect_enters_cooldown_without_repeated_socket_attempts(self) -> None:
        connect_calls = 0

        class FakeWrapper:
            pass

        class FakeContract:
            pass

        class FakeClient:
            def __init__(self, wrapper: object) -> None:
                self.wrapper = wrapper
                self.connected = False

            def connect(self, host: str, port: int, client_id: int) -> None:
                nonlocal connect_calls
                connect_calls += 1

            def run(self) -> None:
                return

            def isConnected(self) -> bool:  # noqa: N802
                return self.connected

            def disconnect(self) -> None:
                self.connected = False

        service = LiveMarketDataStreamService(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4002,
                client_id=9,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            timeout=0,
            initial_connect_backoff_seconds=60,
            max_connect_backoff_seconds=60,
        )

        with patch(
            "ibkr_trader.ibkr.market_stream._load_market_data_runtime",
            return_value=(FakeClient, FakeWrapper, FakeContract),
        ):
            with self.assertRaisesRegex(ConnectionError, "Failed to connect"):
                service.connect_and_start()
            with self.assertRaisesRegex(ConnectionError, "cooling down"):
                service.connect_and_start()

        snapshot = service.snapshot()
        self.assertFalse(snapshot["running"])
        self.assertEqual(snapshot["connect_attempt_count"], 1)
        self.assertEqual(snapshot["consecutive_failures"], 1)
        self.assertEqual(connect_calls, 1)

    def test_subscribe_failure_keeps_desired_symbols_for_reconnect(self) -> None:
        connect_calls = 0

        class FakeWrapper:
            pass

        class FakeContract:
            pass

        class FakeClient:
            def __init__(self, wrapper: object) -> None:
                self.wrapper = wrapper
                self.connected = False

            def connect(self, host: str, port: int, client_id: int) -> None:
                nonlocal connect_calls
                connect_calls += 1

            def run(self) -> None:
                return

            def isConnected(self) -> bool:  # noqa: N802
                return self.connected

            def disconnect(self) -> None:
                self.connected = False

        service = LiveMarketDataStreamService(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4002,
                client_id=9,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            timeout=0,
            initial_connect_backoff_seconds=60,
            max_connect_backoff_seconds=60,
        )

        with patch(
            "ibkr_trader.ibkr.market_stream._load_market_data_runtime",
            return_value=(FakeClient, FakeWrapper, FakeContract),
        ):
            with self.assertRaisesRegex(ConnectionError, "Failed to connect"):
                service.subscribe_many([MarketStreamContract(symbol="AXFO")])
            with self.assertRaisesRegex(ConnectionError, "cooling down"):
                service.subscribe_many([MarketStreamContract(symbol="AXFO")])

        snapshot = service.snapshot()
        self.assertEqual(snapshot["desired_subscription_count"], 1)
        self.assertEqual(snapshot["desired_symbols"], ["AXFO"])
        self.assertEqual(snapshot["connect_attempt_count"], 1)
        self.assertEqual(connect_calls, 1)

    def test_unexpected_disconnect_enters_cooldown_before_reconnect(self) -> None:
        service = LiveMarketDataStreamService(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4002,
                client_id=9,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            initial_connect_backoff_seconds=60,
            max_connect_backoff_seconds=60,
        )
        service._desired_contracts_by_key["AXFO"] = MarketStreamContract(symbol="AXFO")
        with patch(
            "ibkr_trader.ibkr.market_stream._utc_now",
            side_effect=[
                datetime(2026, 4, 28, 7, 0, tzinfo=UTC),
                datetime(2026, 4, 28, 7, 1, tzinfo=UTC),
                datetime(2026, 4, 28, 7, 1, tzinfo=UTC),
            ],
        ):
            with service._lock:
                service._record_connect_success_locked()
                service._record_unexpected_disconnect_locked()
                service._record_unexpected_disconnect_locked()

        snapshot = service.snapshot()
        self.assertFalse(snapshot["running"])
        self.assertEqual(snapshot["consecutive_failures"], 1)
        self.assertEqual(
            snapshot["last_error"],
            "market stream disconnected after a successful broker connection",
        )
        self.assertEqual(
            snapshot["last_disconnect_observed_at"],
            "2026-04-28T07:01:00+00:00",
        )

    def test_identical_subscribe_request_is_noop_when_stream_is_active(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.market_data_type_requests = 0
                self.market_data_requests = 0
                self.cancel_requests = 0

            def isConnected(self) -> bool:  # noqa: N802
                return True

            def reqMarketDataType(self, market_data_type: int) -> None:  # noqa: N802
                _ = market_data_type
                self.market_data_type_requests += 1

            def reqMktData(self, *args: object) -> None:  # noqa: N802
                _ = args
                self.market_data_requests += 1

            def cancelMktData(self, request_id: int) -> None:  # noqa: N802
                _ = request_id
                self.cancel_requests += 1

        class FakeContract:
            pass

        service = LiveMarketDataStreamService(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4002,
                client_id=9,
                diagnostic_client_id=7,
                account_id="DU1234567",
            )
        )
        client = FakeClient()
        contract = MarketStreamContract(symbol="AXFO")
        service._client = client
        service._contract_cls = FakeContract
        service._desired_contracts_by_key["AXFO"] = contract
        service._desired_market_data_type = "LIVE"
        service._subscriptions_by_key["AXFO"] = MarketStreamSubscription(
            request_id=100,
            contract=contract,
            subscribed_at=datetime(2026, 5, 8, 7, 0, tzinfo=UTC),
            status="subscribed",
        )

        snapshot = service.subscribe_many(
            [contract],
            replace=True,
            market_data_type="LIVE",
        )

        self.assertEqual(snapshot["subscribe_request_count"], 1)
        self.assertEqual(snapshot["subscribe_noop_count"], 1)
        self.assertEqual(snapshot["actual_subscription_count"], 0)
        self.assertEqual(snapshot["market_data_type_request_count"], 0)
        self.assertEqual(client.market_data_type_requests, 0)
        self.assertEqual(client.market_data_requests, 0)
        self.assertEqual(client.cancel_requests, 0)

    def test_subscribe_respects_configured_market_data_line_limit(self) -> None:
        service = LiveMarketDataStreamService(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4002,
                client_id=9,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            pacing_governor=BrokerApiPacingGovernor(
                BrokerPacingConfig(max_market_data_lines=1)
            ),
        )

        with self.assertRaisesRegex(
            BrokerPacingLimitExceeded,
            "market data line limit",
        ):
            service.subscribe_many(
                [
                    MarketStreamContract(symbol="AXFO"),
                    MarketStreamContract(symbol="VOLV B"),
                ],
                replace=True,
            )

        snapshot = service.snapshot()
        self.assertEqual(snapshot["desired_subscription_count"], 0)
        self.assertEqual(snapshot["market_data_line_limit"], 1)

    def test_set_desired_many_does_not_touch_broker_socket(self) -> None:
        service = LiveMarketDataStreamService(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4002,
                client_id=9,
                diagnostic_client_id=7,
                account_id="DU1234567",
            )
        )

        snapshot = service.set_desired_many(
            [MarketStreamContract(symbol="AXFO")],
            replace=True,
            market_data_type="LIVE",
        )
        repeated_snapshot = service.set_desired_many(
            [MarketStreamContract(symbol="AXFO")],
            replace=True,
            market_data_type="LIVE",
        )

        self.assertFalse(snapshot["running"])
        self.assertEqual(snapshot["desired_subscription_count"], 1)
        self.assertEqual(snapshot["desired_symbols"], ["AXFO"])
        self.assertEqual(snapshot["desired_update_count"], 1)
        self.assertEqual(snapshot["actual_subscription_count"], 0)
        self.assertEqual(repeated_snapshot["desired_noop_count"], 1)

    def test_restore_desired_subscriptions_cancels_stale_extras(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.cancel_requests: list[int] = []

            def isConnected(self) -> bool:  # noqa: N802
                return True

            def cancelMktData(self, request_id: int) -> None:  # noqa: N802
                self.cancel_requests.append(request_id)

        service = LiveMarketDataStreamService(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4002,
                client_id=9,
                diagnostic_client_id=7,
                account_id="DU1234567",
            )
        )
        client = FakeClient()
        keep_contract = MarketStreamContract(symbol="AXFO")
        stale_contract = MarketStreamContract(symbol="VOLV B")
        service._client = client
        service._desired_contracts_by_key["AXFO"] = keep_contract
        service._subscriptions_by_key["AXFO"] = MarketStreamSubscription(
            request_id=100,
            contract=keep_contract,
            subscribed_at=datetime(2026, 5, 8, 7, 0, tzinfo=UTC),
        )
        service._subscriptions_by_key["VOLV B"] = MarketStreamSubscription(
            request_id=101,
            contract=stale_contract,
            subscribed_at=datetime(2026, 5, 8, 7, 0, tzinfo=UTC),
        )
        service._subscription_keys_by_req_id[100] = "AXFO"
        service._subscription_keys_by_req_id[101] = "VOLV B"

        snapshot = service._restore_desired_subscriptions()

        self.assertEqual(client.cancel_requests, [101])
        self.assertEqual(snapshot["subscribed_count"], 1)
        self.assertEqual(snapshot["subscriptions"][0]["contract"]["symbol"], "AXFO")

    def test_connectivity_1101_marks_subscriptions_for_resubscribe(self) -> None:
        service = LiveMarketDataStreamService(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4002,
                client_id=9,
                diagnostic_client_id=7,
                account_id="DU1234567",
            )
        )
        contract = MarketStreamContract(symbol="AXFO")
        service._desired_contracts_by_key["AXFO"] = contract
        service._subscriptions_by_key["AXFO"] = MarketStreamSubscription(
            request_id=100,
            contract=contract,
            subscribed_at=datetime(2026, 5, 8, 7, 0, tzinfo=UTC),
        )
        service._subscription_keys_by_req_id[100] = "AXFO"

        service._on_error(
            req_id=-1,
            error_time=None,
            error_code=1101,
            error_string="Connectivity between IB and server has been restored - data lost",
            advanced_order_reject_json="",
        )

        snapshot = service.snapshot()
        self.assertEqual(snapshot["connectivity_resubscribe_count"], 1)
        self.assertEqual(snapshot["last_connectivity_event_code"], 1101)
        self.assertEqual(snapshot["subscriptions"][0]["status"], "error")
        self.assertTrue(service._desired_changed_event.is_set())

    def test_connectivity_1102_records_maintained_without_churn(self) -> None:
        service = LiveMarketDataStreamService(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4002,
                client_id=9,
                diagnostic_client_id=7,
                account_id="DU1234567",
            )
        )
        contract = MarketStreamContract(symbol="AXFO")
        service._desired_contracts_by_key["AXFO"] = contract
        service._subscriptions_by_key["AXFO"] = MarketStreamSubscription(
            request_id=100,
            contract=contract,
            subscribed_at=datetime(2026, 5, 8, 7, 0, tzinfo=UTC),
        )
        service._subscription_keys_by_req_id[100] = "AXFO"

        service._on_error(
            req_id=-1,
            error_time=None,
            error_code=1102,
            error_string="Connectivity between IB and server has been restored - data maintained",
            advanced_order_reject_json="",
        )

        snapshot = service.snapshot()
        self.assertEqual(snapshot["connectivity_maintained_count"], 1)
        self.assertEqual(snapshot["last_connectivity_event_code"], 1102)
        self.assertEqual(snapshot["subscriptions"][0]["status"], "subscribed")
        self.assertFalse(service._desired_changed_event.is_set())

    def test_global_broker_circuit_blocks_stream_restore(self) -> None:
        circuit = BrokerHealthCircuit(default_open_seconds=900)
        circuit.trip(
            reason="api_startup_no_next_valid_id",
            source="diagnostic",
            error="socket connected but API startup did not return nextValidId",
        )
        service = LiveMarketDataStreamService(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4002,
                client_id=9,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            broker_circuit=circuit,
        )
        service._desired_contracts_by_key["AXFO"] = MarketStreamContract(symbol="AXFO")

        with self.assertRaisesRegex(ConnectionError, "Global IBKR broker circuit"):
            service._restore_desired_subscriptions()

    def test_snapshot_marks_connected_stream_stale_when_ticks_stop(self) -> None:
        class FakeClient:
            def isConnected(self) -> bool:  # noqa: N802
                return True

        service = LiveMarketDataStreamService(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4002,
                client_id=9,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            stale_data_after_seconds=120,
            stale_reconnect_timezone="Europe/Stockholm",
        )
        service._client = FakeClient()
        contract = MarketStreamContract(symbol="AXFO")
        service._desired_contracts_by_key["AXFO"] = contract
        service._subscriptions_by_key["AXFO"] = MarketStreamSubscription(
            request_id=100,
            contract=contract,
            subscribed_at=datetime(2026, 5, 6, 7, 0, tzinfo=UTC),
        )
        service._quotes_by_key["AXFO"] = MarketStreamQuote(
            symbol="AXFO",
            exchange="SMART",
            currency="SEK",
            security_type="STK",
            primary_exchange="SFB",
            updated_at=datetime(2026, 5, 6, 7, 0, tzinfo=UTC),
            last_trade_at=datetime(2026, 5, 6, 7, 0, tzinfo=UTC),
        )

        with patch(
            "ibkr_trader.ibkr.market_stream._utc_now",
            return_value=datetime(2026, 5, 6, 7, 3, 1, tzinfo=UTC),
        ):
            snapshot = service.snapshot()

        self.assertTrue(snapshot["running"])
        self.assertTrue(snapshot["is_stale"])
        self.assertEqual(snapshot["latest_market_data_age_seconds"], 181)
        self.assertEqual(snapshot["stale_after_seconds"], 120)
        self.assertTrue(snapshot["stale_reconnect_allowed"])

    def test_snapshot_disallows_stale_reconnect_outside_trading_window(self) -> None:
        class FakeClient:
            def isConnected(self) -> bool:  # noqa: N802
                return True

        service = LiveMarketDataStreamService(
            IbkrConnectionConfig(
                host="127.0.0.1",
                port=4002,
                client_id=9,
                diagnostic_client_id=7,
                account_id="DU1234567",
            ),
            stale_data_after_seconds=120,
            stale_reconnect_timezone="Europe/Stockholm",
        )
        service._client = FakeClient()
        contract = MarketStreamContract(symbol="AXFO")
        service._subscriptions_by_key["AXFO"] = MarketStreamSubscription(
            request_id=100,
            contract=contract,
            subscribed_at=datetime(2026, 5, 6, 7, 0, tzinfo=UTC),
        )
        service._quotes_by_key["AXFO"] = MarketStreamQuote(
            symbol="AXFO",
            exchange="SMART",
            currency="SEK",
            security_type="STK",
            primary_exchange="SFB",
            updated_at=datetime(2026, 5, 6, 7, 0, tzinfo=UTC),
        )

        with patch(
            "ibkr_trader.ibkr.market_stream._utc_now",
            return_value=datetime(2026, 5, 6, 18, 0, tzinfo=UTC),
        ):
            snapshot = service.snapshot()

        self.assertTrue(snapshot["is_stale"])
        self.assertFalse(snapshot["stale_reconnect_allowed"])
