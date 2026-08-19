from __future__ import annotations

from tests._api_server_shared import *  # noqa: F401,F403
from ibkr_trader.ibkr.runtime_snapshot import BrokerRuntimeSnapshot


class ApiServerTests03(ApiServerTestCase):
    def test_rl_observation_endpoint_reads_market_stream_by_default(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        class FakeMarketStreamService:
            def snapshot(self, *, symbols=None, bar_limit=390):
                _ = bar_limit
                return {
                    "bars_by_symbol": {
                        symbol: [
                            {
                                "timestamp": "2026-04-28T09:00:00+02:00",
                                "open": "100",
                                "high": "101",
                                "low": "99",
                                "close": "100",
                                "currency": "SEK",
                            },
                            {
                                "timestamp": "2026-04-28T09:01:00+02:00",
                                "open": "100",
                                "high": "102",
                                "low": "100",
                                "close": "101",
                                "currency": "SEK",
                            },
                            {
                                "timestamp": "2026-04-28T09:05:00+02:00",
                                "open": "101",
                                "high": "103",
                                "low": "101",
                                "close": "102",
                                "currency": "SEK",
                            },
                        ]
                        for symbol in symbols or []
                    }
                }

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "stream_observation.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            engine.dispose()

            app = create_app(
                AppConfig(
                    environment="test",
                    timezone="Europe/Stockholm",
                    database_url=database_url,
                    session_calendar_path=Path(temp_dir) / "day_sessions.csv",
                    stockholm_instruments_path=Path("/tmp/all.txt"),
                    stockholm_identity_path=Path("/tmp/identity.parquet"),
                    api=ApiServerConfig(
                        host="127.0.0.1",
                        port=8000,
                        require_loopback_only=False,
                    ),
                    ibkr=IbkrConnectionConfig(
                        host="127.0.0.1",
                        port=4001,
                        client_id=0,
                        diagnostic_client_id=7,
                        streaming_client_id=9,
                        account_id="DU1234567",
                    ),
                )
            )

            with (
                patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
                patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
                TestClient(app) as client,
            ):
                client.app.state.market_stream_service = FakeMarketStreamService()
                self.assertEqual(
                    client.post(
                        "/v1/rl/models/register",
                        json={
                            "model_key": "long_trial_106_v1",
                            "display_name": "Long Trial 106 V1",
                            "strategy_family": "canonical_long_live_execution_policy",
                            "side": "LONG",
                            "action_space": ["skip", "wait", "market_entry"],
                            "observation_contract": {
                                "bar_family": "phase1_intraday_ohlc_v1",
                                "bar_interval": "5m",
                                "session_timezone": "Europe/Stockholm",
                                "session_open_local": "09:00",
                                "session_close_local": "17:30",
                                "include_market_context": False,
                            },
                        },
                    ).status_code,
                    200,
                )
                self.assertEqual(
                    client.post(
                        "/v1/rl/deployments",
                        json={
                            "deployment_key": "long_trial_106_virtual_shared_01",
                            "model_key": "long_trial_106_v1",
                            "account_key": "VIRTUALRL01",
                            "book_key": "rl_shared_long_trial_106_virtual_01",
                            "mode": "virtual",
                            "status": "running",
                            "allowed_symbols": ["AXFO"],
                        },
                    ).status_code,
                    200,
                )
                response = client.post(
                    "/v1/rl/observations/build",
                    json={
                        "deployment_key": "long_trial_106_virtual_shared_01",
                        "symbols": ["AXFO"],
                        "as_of": "2026-04-28T09:07:30+02:00",
                        "history_overrides": {
                            "AXFO": {
                                "prev_close": "100",
                                "history_features": {
                                    "prev_open_rel_close": 0.0,
                                    "prev_high_rel_close": 0.02,
                                    "prev_low_rel_close": -0.02,
                                    "prev_close_rel_open": 0.0,
                                    "prev_high_rel_low": 0.04,
                                    "trailing_intraday_realized_vol": 0.01,
                                    "trailing_session_count_norm": 1.0,
                                },
                            }
                        },
                    },
                )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["source_mode"], "market_stream")
        self.assertEqual(body["streamed_symbols"], ["AXFO"])
        self.assertEqual(
            body["rl_observation"]["observations"]["AXFO"]["model_decision"]["usable_bar_count"],
            1,
        )

    def test_rl_observation_endpoint_pauses_missing_stream_symbol_and_enqueues_backfill(
        self,
    ) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        class FakeMarketStreamService:
            def snapshot(self, *, symbols=None, bar_limit=390):
                _ = bar_limit
                return {"bars_by_symbol": {symbol: [] for symbol in symbols or []}}

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "stream_missing_observation.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            engine.dispose()

            app = create_app(
                AppConfig(
                    environment="test",
                    timezone="Europe/Stockholm",
                    database_url=database_url,
                    session_calendar_path=Path(temp_dir) / "day_sessions.csv",
                    stockholm_instruments_path=Path("/tmp/all.txt"),
                    stockholm_identity_path=Path("/tmp/identity.parquet"),
                    api=ApiServerConfig(
                        host="127.0.0.1",
                        port=8000,
                        require_loopback_only=False,
                    ),
                    ibkr=IbkrConnectionConfig(
                        host="127.0.0.1",
                        port=4001,
                        client_id=0,
                        diagnostic_client_id=7,
                        streaming_client_id=9,
                        account_id="DU1234567",
                    ),
                )
            )

            with (
                patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
                patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
                TestClient(app) as client,
            ):
                client.app.state.market_stream_service = FakeMarketStreamService()
                self.assertEqual(
                    client.post(
                        "/v1/rl/models/register",
                        json={
                            "model_key": "long_trial_106_v1",
                            "display_name": "Long Trial 106 V1",
                            "strategy_family": "canonical_long_live_execution_policy",
                            "side": "LONG",
                            "action_space": ["skip", "wait", "market_entry"],
                            "observation_contract": {
                                "bar_family": "phase1_intraday_ohlc_v1",
                                "bar_interval": "5m",
                                "session_timezone": "Europe/Stockholm",
                                "session_open_local": "09:00",
                                "session_close_local": "17:30",
                                "include_market_context": False,
                            },
                        },
                    ).status_code,
                    200,
                )
                self.assertEqual(
                    client.post(
                        "/v1/rl/deployments",
                        json={
                            "deployment_key": "long_trial_106_virtual_shared_01",
                            "model_key": "long_trial_106_v1",
                            "account_key": "VIRTUALRL01",
                            "book_key": "rl_shared_long_trial_106_virtual_01",
                            "mode": "virtual",
                            "status": "running",
                            "allowed_symbols": ["AXFO"],
                        },
                    ).status_code,
                    200,
                )
                response = client.post(
                    "/v1/rl/observations/build",
                    json={
                        "deployment_key": "long_trial_106_virtual_shared_01",
                        "symbols": ["AXFO"],
                        "as_of": "2026-04-28T09:07:30+02:00",
                        "fetch": {
                            "mode": "market_stream",
                            "backfill_missing": True,
                            "instruments": {
                                "AXFO": {
                                    "exchange": "XSTO",
                                    "currency": "SEK",
                                    "primary_exchange": "SFB",
                                }
                            },
                        },
                    },
                )

            self.assertEqual(response.status_code, 200)
            body = response.json()
            decision = body["rl_observation"]["observations"]["AXFO"]["model_decision"]
            self.assertFalse(decision["ready"])
            self.assertEqual(
                decision["reason"],
                "paused_market_stream_bars_missing_backfill_pending",
            )
            self.assertEqual(body["backfill_request_count"], 1)
            check_engine = build_engine(database_url)
            try:
                requests = list_market_data_backfill_requests(
                    create_session_factory(check_engine)
                )
                self.assertEqual(len(requests), 1)
                self.assertEqual(requests[0]["symbol"], "AXFO")
                self.assertEqual(requests[0]["status"], "PENDING")
                self.assertEqual(
                    requests[0]["requested_until"],
                    "2026-04-28T09:05:00+02:00",
                )
                self.assertEqual(requests[0]["duration"], "2 D")
            finally:
                check_engine.dispose()

    def test_market_stream_endpoints_use_persistent_service(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        class FakeMarketStreamService:
            def __init__(self) -> None:
                self.calls = []
                self.desired_calls = []

            def subscribe_many(self, contracts, *, replace, market_data_type):
                self.calls.append((contracts, replace, market_data_type))
                return {
                    "running": True,
                    "subscribed_count": len(contracts),
                    "subscriptions": [],
                    "quote_count": 0,
                    "quotes": [],
                    "bars_by_symbol": {contract.symbol: [] for contract in contracts},
                    "errors": [],
                }

            def set_desired_many(self, contracts, *, replace, market_data_type):
                self.desired_calls.append((contracts, replace, market_data_type))
                return {
                    "running": False,
                    "desired_subscription_count": len(contracts),
                    "desired_symbols": sorted(contract.symbol for contract in contracts),
                    "subscribed_count": 0,
                    "subscriptions": [],
                    "quote_count": 0,
                    "quotes": [],
                    "bars_by_symbol": {contract.symbol: [] for contract in contracts},
                    "errors": [],
                }

            def snapshot(self, *, symbols=None, bar_limit=390):
                return {
                    "running": True,
                    "subscribed_count": 2,
                    "subscriptions": [],
                    "quote_count": 0,
                    "quotes": [],
                    "bars_by_symbol": {symbol: [] for symbol in symbols or []},
                    "errors": [],
                    "bar_limit": bar_limit,
                }

            def stop(self):
                self.calls.append(("stop",))

        app = create_app(
            AppConfig(
                environment="test",
                timezone="Europe/Stockholm",
                database_url="sqlite+pysqlite:///:memory:",
                session_calendar_path=Path("/tmp/day_sessions.csv"),
                stockholm_instruments_path=Path("/tmp/all.txt"),
                stockholm_identity_path=Path("/tmp/identity.parquet"),
                api=ApiServerConfig(
                    host="127.0.0.1",
                    port=8000,
                    require_loopback_only=False,
                ),
                ibkr=IbkrConnectionConfig(
                    host="127.0.0.1",
                    port=4001,
                    client_id=0,
                    diagnostic_client_id=7,
                    streaming_client_id=9,
                    account_id="DU1234567",
                ),
            )
        )
        fake_service = FakeMarketStreamService()

        with (
            patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
            patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
            TestClient(app) as client,
        ):
            client.app.state.market_stream_service = fake_service
            desired_response = client.post(
                "/v1/market-data/stream/desired",
                json={"symbols": ["axfo", "azn"], "market_data_type": "delayed"},
            )
            subscribe_response = client.post(
                "/v1/market-data/stream/subscribe",
                json={"symbols": ["axfo", "azn"], "market_data_type": "delayed"},
            )
            snapshot_response = client.get(
                "/v1/market-data/stream/snapshot?symbols=AXFO,AZN&bar_limit=10"
            )

        self.assertEqual(desired_response.status_code, 200)
        self.assertEqual(subscribe_response.status_code, 200)
        self.assertEqual(snapshot_response.status_code, 200)
        self.assertEqual(
            desired_response.json()["mode"],
            "streaming_market_data_desired",
        )
        desired_contracts, desired_replace, desired_market_data_type = (
            fake_service.desired_calls[0]
        )
        self.assertEqual(
            [contract.symbol for contract in desired_contracts],
            ["AXFO", "AZN"],
        )
        self.assertTrue(desired_replace)
        self.assertEqual(desired_market_data_type, "DELAYED")
        self.assertEqual(subscribe_response.json()["stream"]["subscribed_count"], 2)
        contracts, replace, market_data_type = fake_service.calls[0]
        self.assertEqual([contract.symbol for contract in contracts], ["AXFO", "AZN"])
        self.assertTrue(replace)
        self.assertEqual(market_data_type, "DELAYED")
        self.assertEqual(
            sorted(snapshot_response.json()["stream"]["bars_by_symbol"]),
            ["AXFO", "AZN"],
        )

    def test_parse_shortability_snapshot_payload_uses_stockholm_defaults(self) -> None:
        query = parse_shortability_snapshot_payload({})

        self.assertEqual(query.exchange, "SMART")
        self.assertEqual(query.primary_exchange, "SFB")
        self.assertEqual(query.currency, "SEK")
        self.assertEqual(query.security_type, "STK")
        self.assertEqual(query.source, ShortabilitySource.OFFICIAL_IBKR_PAGE)
        self.assertEqual(query.market_data_type, ShortabilityMarketDataType.LIVE)
        self.assertTrue(query.only_shortable)
        self.assertIsNone(query.as_of_date)

    def test_parse_shortability_snapshot_payload_accepts_symbols_date_source_and_delayed_type(self) -> None:
        query = parse_shortability_snapshot_payload(
            {
                "symbols": ["sive", "abb"],
                "as_of_date": "2026-04-14",
                "source": "broker_ticks",
                "market_data_type": "delayed_frozen",
                "max_symbols": 25,
                "max_concurrent": 10,
                "per_symbol_timeout_seconds": 1.5,
            }
        )

        self.assertEqual(query.symbols, ("SIVE", "ABB"))
        self.assertEqual(query.source, ShortabilitySource.BROKER_TICKS)
        self.assertEqual(
            query.market_data_type,
            ShortabilityMarketDataType.DELAYED_FROZEN,
        )
        self.assertEqual(query.as_of_date, date(2026, 4, 14))
        self.assertEqual(query.max_symbols, 25)
        self.assertEqual(query.max_concurrent, 10)
        self.assertEqual(query.per_symbol_timeout_seconds, 1.5)

    def test_background_execution_recovery_runs_when_instruction_is_active(self) -> None:
        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "recovery_active.db"
            engine = build_engine(f"sqlite+pysqlite:///{database_path}")
            create_schema(engine)
            session_factory = create_session_factory(engine)

            session = session_factory()
            try:
                session.add(
                    InstructionRecord(
                        instruction_id="runtime-sive-1",
                        schema_version="2026-04-10",
                        source_system="q-training",
                        batch_id="batch-1",
                        account_key="GTW05",
                        book_key="long_risk_book",
                        symbol="SIVE",
                        exchange="SMART",
                        currency="SEK",
                        state="EXIT_PENDING",
                        submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
                        expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
                        order_type="LIMIT",
                        side="BUY",
                        payload={"instruction": {"instruction_id": "runtime-sive-1"}},
                    )
                )
                session.commit()
            finally:
                session.close()

            self.assertTrue(should_include_background_execution_recovery(session_factory))

    def test_background_execution_recovery_runs_when_broker_order_is_unsettled(self) -> None:
        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "recovery_order.db"
            engine = build_engine(f"sqlite+pysqlite:///{database_path}")
            create_schema(engine)
            session_factory = create_session_factory(engine)
            broker_order_id: int

            session = session_factory()
            try:
                broker_account = BrokerAccountRecord(
                    broker_kind="IBKR",
                    account_key="GTW05",
                    base_currency="SEK",
                )
                session.add(broker_account)
                session.flush()
                session.add(
                    BrokerOrderRecord(
                        instruction_id=None,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="GTW05",
                        order_role="EXIT",
                        external_order_id="3953",
                        external_perm_id="449407988",
                        external_client_id="0",
                        order_ref="runtime-sive-1:exit:forced",
                        symbol="SIVE",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        side="SELL",
                        order_type="MKT",
                        status="PendingCancel",
                        total_quantity="100",
                        submitted_at=datetime(2026, 4, 10, 7, 30, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 10, 7, 31, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    )
                )
                session.commit()
            finally:
                session.close()

            self.assertTrue(should_include_background_execution_recovery(session_factory))

    def test_background_execution_recovery_runs_for_filled_order_without_fill(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "recovery_filled_missing_fill.db"
            engine = build_engine(f"sqlite+pysqlite:///{database_path}")
            create_schema(engine)
            session_factory = create_session_factory(engine)

            session = session_factory()
            try:
                broker_account = BrokerAccountRecord(
                    broker_kind="IBKR",
                    account_key="U25245596",
                    base_currency="SEK",
                )
                session.add(broker_account)
                session.flush()
                broker_order = BrokerOrderRecord(
                    instruction_id=None,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="U25245596",
                    order_role="EXIT",
                    external_order_id="4956",
                    external_perm_id="1456474004",
                    external_client_id="0",
                    order_ref="operator-flatten-20260615-U25245596-INTRUMTR-rights-01:exit:operator_flatten_market",
                    symbol="INTRUMTR",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    primary_exchange="SFB",
                    local_symbol="INTRUMTR",
                    side="SELL",
                    order_type="MKT",
                    status="Filled",
                    total_quantity="1100",
                    submitted_at=datetime(2026, 6, 15, 13, 9, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 6, 15, 13, 9, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
                session.add(broker_order)
                session.flush()
                broker_order_id = broker_order.id
                session.commit()
            finally:
                session.close()

            self.assertTrue(should_include_background_execution_recovery(session_factory))

            session = session_factory()
            try:
                broker_order = session.get(BrokerOrderRecord, broker_order_id)
                assert broker_order is not None
                session.add(
                    ExecutionFillRecord(
                        broker_order_id=broker_order.id,
                        broker_account_id=broker_order.broker_account_id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        external_execution_id="00014800.6a2f9cf7.01.01",
                        external_order_id="4956",
                        external_perm_id="1456474004",
                        order_ref=broker_order.order_ref,
                        symbol="INTRUMTR",
                        exchange="SFB",
                        currency="SEK",
                        security_type="STK",
                        side="SLD",
                        quantity="287",
                        price="12.60",
                        commission="0.084981",
                        commission_currency="SEK",
                        executed_at=datetime(
                            2026,
                            6,
                            15,
                            13,
                            9,
                            57,
                            tzinfo=timezone.utc,
                        ),
                        raw_payload={},
                    )
                )
                session.commit()
            finally:
                session.close()

            self.assertFalse(should_include_background_execution_recovery(session_factory))

    def test_background_execution_recovery_falls_back_to_light_snapshot_on_timeout(
        self,
    ) -> None:
        class _FakeBrokerSession:
            def execute(
                self,
                operation_name: str,
                operation: object,
                *,
                ignore_cooldown: bool = False,
            ) -> object:
                del operation_name, ignore_cooldown
                return operation(object())

            def drain_broker_callback_events(
                self,
                *,
                connect_if_needed: bool = False,
            ) -> list[dict[str, object]]:
                del connect_if_needed
                return []

        class _FakeBrokerSessions:
            def __init__(self, *args: object, **kwargs: object) -> None:
                del args, kwargs
                self.primary = _FakeBrokerSession()
                self.diagnostic = _FakeBrokerSession()
                self.historical = _FakeBrokerSession()

            def warmup(self) -> None:
                return None

            def shutdown(self) -> None:
                return None

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "recovery_fallback.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            session_factory = create_session_factory(engine)
            session = session_factory()
            try:
                session.add(
                    InstructionRecord(
                        instruction_id="runtime-sive-1",
                        schema_version="2026-04-10",
                        source_system="q-training",
                        batch_id="batch-1",
                        account_key="GTW05",
                        book_key="long_risk_book",
                        symbol="SIVE",
                        exchange="SMART",
                        currency="SEK",
                        state="ENTRY_SUBMITTED",
                        submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
                        expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
                        order_type="LIMIT",
                        side="BUY",
                        payload={"instruction": {"instruction_id": "runtime-sive-1"}},
                    )
                )
                session.commit()
            finally:
                session.close()
                engine.dispose()

            calls: list[bool] = []
            light_snapshot = BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            )

            def fake_snapshot_fetch(*args: object, **kwargs: object) -> BrokerRuntimeSnapshot:
                del args
                include_executions = bool(kwargs["include_executions"])
                calls.append(include_executions)
                if include_executions:
                    raise TimeoutError(
                        "Timed out while requesting executions for the IBKR runtime snapshot."
                    )
                return light_snapshot

            with (
                patch("ibkr_trader.api.server.CanonicalSyncSessions", _FakeBrokerSessions),
                patch("ibkr_trader.api.server.fetch_broker_runtime_snapshot", fake_snapshot_fetch),
            ):
                app = create_app(
                    AppConfig(
                        environment="test",
                        timezone="Europe/Stockholm",
                        database_url=database_url,
                        session_calendar_path=Path("/tmp/day_sessions.csv"),
                        stockholm_instruments_path=Path("/tmp/all.txt"),
                        stockholm_identity_path=Path("/tmp/identity.parquet"),
                        api=ApiServerConfig(
                            host="127.0.0.1",
                            port=8000,
                            require_loopback_only=False,
                        ),
                        ibkr=IbkrConnectionConfig(
                            host="127.0.0.1",
                            port=4001,
                            client_id=0,
                            diagnostic_client_id=7,
                            streaming_client_id=9,
                            account_id="U25245596",
                        ),
                    )
                )

                snapshot = app.state.broker_monitor._snapshot_fetcher()
                second_snapshot = app.state.broker_monitor._snapshot_fetcher()

        self.assertIs(snapshot, light_snapshot)
        self.assertIs(second_snapshot, light_snapshot)
        self.assertEqual(calls, [True, False, False])

    def test_operator_snapshot_endpoint_returns_durable_ledger_state(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "operator_snapshot.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            session_factory = create_session_factory(engine)
            session = session_factory()
            try:
                broker_account = BrokerAccountRecord(
                    broker_kind="IBKR",
                    account_key="U25245596",
                    account_label="Live Sweden",
                    base_currency="SEK",
                )
                session.add(broker_account)
                session.flush()
                session.add(
                    AccountSnapshotRecord(
                        broker_account_id=broker_account.id,
                        snapshot_at=datetime(2026, 4, 19, 8, 15, tzinfo=timezone.utc),
                        source="runtime_snapshot",
                        net_liquidation="100500.00",
                        total_cash_value="55000.00",
                        buying_power="200000.00",
                        available_funds="120000.00",
                        excess_liquidity="119000.00",
                        cushion="0.91",
                        currency="SEK",
                    )
                )
                session.add(
                    InstructionRecord(
                        instruction_id="instr-001",
                        schema_version="2026-04-10",
                        source_system="q-training",
                        batch_id="batch-001",
                        account_key="U25245596",
                        book_key="long_risk_book",
                        symbol="SAAB",
                        exchange="SMART",
                        currency="SEK",
                        state="ENTRY_PENDING",
                        submit_at=datetime(2026, 4, 19, 8, 20, tzinfo=timezone.utc),
                        expire_at=datetime(2026, 4, 19, 15, 30, tzinfo=timezone.utc),
                        order_type="LIMIT",
                        side="BUY",
                        payload={"instruction": {"instruction_id": "instr-001"}},
                    )
                )
                session.add(
                    InstructionRecord(
                        instruction_id="instr-terminal",
                        schema_version="2026-04-10",
                        source_system="q-training",
                        batch_id="batch-001",
                        account_key="U25245596",
                        book_key="long_risk_book",
                        symbol="SINCH",
                        exchange="SMART",
                        currency="SEK",
                        state="ENTRY_CANCELLED",
                        submit_at=datetime(2026, 4, 19, 8, 20, tzinfo=timezone.utc),
                        expire_at=datetime(2026, 4, 19, 15, 30, tzinfo=timezone.utc),
                        order_type="LIMIT",
                        side="BUY",
                        payload={"instruction": {"instruction_id": "instr-terminal"}},
                    )
                )
                session.commit()
            finally:
                session.close()
                engine.dispose()

            app = create_app(
                AppConfig(
                    environment="test",
                    timezone="Europe/Stockholm",
                    database_url=database_url,
                    session_calendar_path=Path("/tmp/day_sessions.csv"),
                    stockholm_instruments_path=Path("/tmp/all.txt"),
                    stockholm_identity_path=Path("/tmp/identity.parquet"),
                    api=ApiServerConfig(
                        host="127.0.0.1",
                        port=8000,
                        require_loopback_only=False,
                    ),
                    ibkr=IbkrConnectionConfig(
                        host="127.0.0.1",
                        port=4001,
                        client_id=0,
                        diagnostic_client_id=7,
                        streaming_client_id=9,
                        account_id="U25245596",
                    ),
                )
            )

            with (
                patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
                patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
                TestClient(app) as client,
            ):
                response = client.get("/v1/read/operator-snapshot")
                response_with_terminal = client.get(
                    "/v1/read/operator-snapshot?include_terminal_instructions=true"
                )

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertTrue(body["accepted"])
            self.assertEqual(body["operator_snapshot"]["accounts"][0]["account_key"], "U25245596")
            self.assertEqual(
                body["operator_snapshot"]["instructions"][0]["instruction_id"],
                "instr-001",
            )
            default_instruction_ids = {
                instruction["instruction_id"]
                for instruction in body["operator_snapshot"]["instructions"]
            }
            self.assertNotIn("instr-terminal", default_instruction_ids)
            body_with_terminal = response_with_terminal.json()
            with_terminal_instruction_ids = {
                instruction["instruction_id"]
                for instruction in body_with_terminal["operator_snapshot"]["instructions"]
            }
            self.assertIn("instr-terminal", with_terminal_instruction_ids)

    def test_operator_snapshot_default_keeps_more_than_fifty_instruction_owners(
        self,
    ) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "operator_snapshot_many.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            session_factory = create_session_factory(engine)
            session = session_factory()
            try:
                for index in range(60):
                    session.add(
                        InstructionRecord(
                            instruction_id=f"virtual-owner-{index:03d}",
                            schema_version="2026-04-10",
                            source_system="q-training",
                            batch_id="batch-001",
                            account_key="VIRTUALRL01",
                            book_key="rl_virtual",
                            is_virtual=True,
                            symbol=f"SYM{index:03d}",
                            exchange="XSTO",
                            currency="SEK",
                            state="POSITION_OPEN",
                            submit_at=datetime(
                                2026, 4, 19, 8, 20, tzinfo=timezone.utc
                            ),
                            expire_at=datetime(
                                2026, 4, 19, 15, 30, tzinfo=timezone.utc
                            ),
                            order_type="MARKET",
                            side="BUY",
                            payload={
                                "instruction": {
                                    "instruction_id": f"virtual-owner-{index:03d}"
                                }
                            },
                        )
                    )
                session.commit()
            finally:
                session.close()
                engine.dispose()

            app = create_app(
                AppConfig(
                    environment="test",
                    timezone="Europe/Stockholm",
                    database_url=database_url,
                    session_calendar_path=Path("/tmp/day_sessions.csv"),
                    stockholm_instruments_path=Path("/tmp/all.txt"),
                    stockholm_identity_path=Path("/tmp/identity.parquet"),
                    api=ApiServerConfig(
                        host="127.0.0.1",
                        port=8000,
                        require_loopback_only=False,
                    ),
                    ibkr=IbkrConnectionConfig(
                        host="127.0.0.1",
                        port=4001,
                        client_id=0,
                        diagnostic_client_id=7,
                        streaming_client_id=9,
                        account_id="U25245596",
                    ),
                )
            )

            with (
                patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
                patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
                TestClient(app) as client,
            ):
                response = client.get("/v1/read/operator-snapshot")

            self.assertEqual(response.status_code, 200)
            body = response.json()
            instruction_ids = {
                instruction["instruction_id"]
                for instruction in body["operator_snapshot"]["instructions"]
            }
            self.assertEqual(len(instruction_ids), 60)
            self.assertIn("virtual-owner-000", instruction_ids)
            self.assertIn("virtual-owner-059", instruction_ids)
