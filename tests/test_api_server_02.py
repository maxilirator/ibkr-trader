from __future__ import annotations

from tests._api_server_shared import *  # noqa: F401,F403


class ApiServerTests02(ApiServerTestCase):
    def test_rl_registry_endpoints_round_trip(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "rl_registry.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            engine.dispose()

            app = create_app(
                AppConfig(
                    environment="test",
                    timezone="Europe/Stockholm",
                    database_url=database_url,
                    session_calendar_path=Path(temp_dir) / "day_sessions.parquet",
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
                register_response = client.post(
                    "/v1/rl/models/register",
                    json={
                        "model_key": "short_trial36_v1",
                        "display_name": "Short Trial 36 V1",
                        "strategy_family": "canonical_short_live_execution_policy",
                        "side": "SHORT",
                        "action_space": ["skip", "market_entry", "exit_market"],
                        "observation_contract": {
                            "bar_family": "stockholm_intraday_1m_v1"
                        },
                    },
                )
                self.assertEqual(register_response.status_code, 200)

                upsert_response = client.post(
                    "/v1/rl/models/upsert",
                    json={
                        "model_key": "short_trial36_v1",
                        "display_name": "Short Trial 36 V1",
                        "strategy_family": "canonical_short_live_execution_policy",
                        "side": "SHORT",
                        "action_space": [
                            "skip",
                            "market_entry",
                            "exit_market",
                            "exit_tp_180bp",
                        ],
                        "observation_contract": {
                            "bar_family": "phase1_intraday_ohlc_v1",
                            "bar_interval": "5m",
                        },
                        "execution_mapping_version": "short_actions_v1",
                    },
                )
                self.assertEqual(upsert_response.status_code, 200)
                self.assertEqual(
                    upsert_response.json()["trader_model"]["observation_contract"][
                        "bar_family"
                    ],
                    "phase1_intraday_ohlc_v1",
                )

                deployment_response = client.post(
                    "/v1/rl/deployments",
                    json={
                        "deployment_key": "short_trial36_live_01",
                        "model_key": "short_trial36_v1",
                        "account_key": "U25245596",
                        "book_key": "rl_short_trial36_live_01",
                        "mode": "live",
                        "status": "running",
                        "allowed_symbols": ["SIVE", "VOLV-B"],
                    },
                )
                self.assertEqual(deployment_response.status_code, 200)

                update_deployment_response = client.patch(
                    "/v1/rl/deployments/short_trial36_live_01",
                    json={
                        "allowed_symbols": ["SIVE", "VOLV-B", "ERIC-B"],
                        "risk_limits": {"max_open_positions": 3},
                        "metadata": {"edited_by": "test"},
                    },
                )
                self.assertEqual(update_deployment_response.status_code, 200)
                updated_deployment = update_deployment_response.json()[
                    "trader_deployment"
                ]
                self.assertEqual(
                    updated_deployment["allowed_symbols"],
                    ["SIVE", "VOLV-B", "ERIC-B"],
                )
                self.assertEqual(
                    updated_deployment["risk_limits"]["max_open_positions"],
                    3,
                )

                action_response = client.post(
                    "/v1/rl/actions/log",
                    json={
                        "deployment_key": "short_trial36_live_01",
                        "symbol": "SIVE",
                        "action_name": "market_entry",
                        "observed_at": "2026-04-25T09:25:00+02:00",
                        "state_before": "FLAT",
                        "state_after": "ENTRY_PENDING",
                        "action_status": "translated",
                    },
                )
                self.assertEqual(action_response.status_code, 200)

                heartbeat_response = client.post(
                    "/v1/rl/deployments/short_trial36_live_01/heartbeat",
                    json={
                        "status": "running",
                        "last_seen_at": "2026-04-25T09:30:00+02:00",
                        "last_bar_at": "2026-04-25T09:29:00+02:00",
                        "metrics": {"bar_lag_seconds": 4},
                    },
                )
                self.assertEqual(heartbeat_response.status_code, 200)

                dashboard_response = client.get("/v1/read/rl-dashboard")
                self.assertEqual(dashboard_response.status_code, 200)
                body = dashboard_response.json()
                self.assertTrue(body["accepted"])
                self.assertEqual(body["rl_dashboard"]["summary"]["model_count"], 1)
                self.assertEqual(
                    body["rl_dashboard"]["summary"]["deployment_count"],
                    1,
                )
                self.assertEqual(
                    body["rl_dashboard"]["summary"]["recent_action_count"],
                    1,
                )
                self.assertEqual(
                    body["rl_dashboard"]["deployments"][0]["account_key"],
                    "U25245596",
                )
                self.assertEqual(
                    body["rl_dashboard"]["recent_actions"][0]["action_name"],
                    "market_entry",
                )

    def test_rl_observation_endpoint_builds_model_facing_prefix(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        def bars_for_live_day() -> list[dict[str, str]]:
            bars: list[dict[str, str]] = []
            for minute in range(8):
                bars.append(
                    {
                        "timestamp": f"20260428 09:{minute:02d}:00",
                        "open": f"{110 + minute:.2f}",
                        "high": f"{111 + minute:.2f}",
                        "low": f"{109 + minute:.2f}",
                        "close": f"{110.5 + minute:.2f}",
                    }
                )
            return bars

        history_features = {
            "prev_open_rel_close": 0.01,
            "prev_high_rel_close": 0.02,
            "prev_low_rel_close": -0.01,
            "prev_close_rel_open": 0.03,
            "prev_high_rel_low": 0.04,
            "trailing_intraday_realized_vol": 0.02,
            "trailing_session_count_norm": 0.5,
        }

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "rl_observations.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            engine.dispose()

            app = create_app(
                AppConfig(
                    environment="test",
                    timezone="Europe/Stockholm",
                    database_url=database_url,
                    session_calendar_path=Path(temp_dir) / "day_sessions.parquet",
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
                register_response = client.post(
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
                            "include_vol_normalized_intraday_state": True,
                        },
                    },
                )
                self.assertEqual(register_response.status_code, 200)
                deployment_response = client.post(
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
                )
                self.assertEqual(deployment_response.status_code, 200)
                observation_response = client.post(
                    "/v1/rl/observations/build",
                    json={
                        "deployment_key": "long_trial_106_virtual_shared_01",
                        "symbols": ["AXFO"],
                        "as_of": "2026-04-28T09:07:30+02:00",
                        "source_bars": {"AXFO": bars_for_live_day()},
                        "history_overrides": {
                            "AXFO": {
                                "prev_close": "100",
                                "history_features": history_features,
                            }
                        },
                    },
                )

        self.assertEqual(observation_response.status_code, 200)
        body = observation_response.json()
        self.assertTrue(body["accepted"])
        observation = body["rl_observation"]
        self.assertEqual(observation["input_contract"]["bar_interval"], "5m")
        self.assertEqual(observation["input_contract"]["decision_cadence"], "5m")
        self.assertEqual(
            observation["observations"]["AXFO"]["latest_bar_complete"],
            False,
        )
        self.assertEqual(
            observation["observations"]["AXFO"]["model_decision"]["usable_bar_count"],
            1,
        )
        self.assertAlmostEqual(
            observation["observations"]["AXFO"]["features"]["base_dynamic"][1][0],
            1.0 / 101.0,
        )

    def test_parse_tick_stream_payload_normalizes_tick_types(self) -> None:
        query = parse_tick_stream_payload(
            {
                "symbol": "aapl",
                "security_type": "stk",
                "exchange": "smart",
                "currency": "usd",
                "primary_exchange": "nasdaq",
                "tick_types": ["last", "bid_ask", "mid-point"],
                "duration_seconds": 3,
                "max_events": 100,
            }
        )

        self.assertEqual(query.symbol, "AAPL")
        self.assertEqual(query.exchange, "SMART")
        self.assertEqual(query.currency, "USD")
        self.assertEqual(query.primary_exchange, "NASDAQ")
        self.assertEqual(query.tick_types, ("Last", "BidAsk", "MidPoint"))
        self.assertEqual(query.duration_seconds, 3)
        self.assertEqual(query.max_events, 100)

    def test_parse_tick_stream_payload_rejects_empty_tick_types(self) -> None:
        with self.assertRaisesRegex(ValueError, "tick_types"):
            parse_tick_stream_payload(
                {
                    "symbol": "AAPL",
                    "exchange": "SMART",
                    "currency": "USD",
                    "tick_types": [],
                }
            )

    def test_parse_market_stream_subscribe_payload_normalizes_symbols(self) -> None:
        payload = parse_market_stream_subscribe_payload(
            {
                "symbols": ["axfo", "azn"],
                "market_data_type": "delayed",
                "replace": True,
            }
        )

        self.assertEqual([item.symbol for item in payload["contracts"]], ["AXFO", "AZN"])
        self.assertEqual(payload["contracts"][0].exchange, "SMART")
        self.assertEqual(payload["contracts"][0].primary_exchange, "SFB")
        self.assertEqual(payload["market_data_type"], "DELAYED")
        self.assertTrue(payload["replace"])

    def test_parse_market_stream_subscribe_payload_uses_configurable_cap(self) -> None:
        with self.assertRaisesRegex(ValueError, "limited to 2 symbols"):
            parse_market_stream_subscribe_payload(
                {
                    "symbols": ["aaa", "bbb", "ccc"],
                    "market_data_type": "live",
                },
                max_contracts=2,
            )

    def test_market_stream_contracts_for_open_orders_uses_stockholm_stream_defaults(
        self,
    ) -> None:
        contracts = market_stream_contracts_for_open_orders(
            {
                18: BrokerOpenOrder(
                    order_id=18,
                    perm_id=10018,
                    client_id=0,
                    status="PreSubmitted",
                    order_ref="2026-04-29-U25245596-live_top1_31_seedpicker-HTRO-long-01",
                    action="SELL",
                    total_quantity=None,
                    symbol="htro",
                    security_type="STK",
                    exchange="SFB",
                    primary_exchange=None,
                    currency="SEK",
                    local_symbol="HTRO",
                ),
                19: BrokerOpenOrder(
                    order_id=19,
                    perm_id=10019,
                    client_id=0,
                    status="Cancelled",
                    order_ref=None,
                    action="SELL",
                    total_quantity=None,
                    symbol="OLD",
                    security_type="STK",
                    exchange="SMART",
                    primary_exchange="SFB",
                    currency="SEK",
                ),
            }
        )

        self.assertEqual(len(contracts), 1)
        self.assertEqual(contracts[0].symbol, "HTRO")
        self.assertEqual(contracts[0].security_type, "STK")
        self.assertEqual(contracts[0].exchange, "SMART")
        self.assertEqual(contracts[0].primary_exchange, "SFB")
        self.assertEqual(contracts[0].currency, "SEK")
        self.assertEqual(contracts[0].local_symbol, "HTRO")

    def test_market_stream_contracts_for_runtime_holdings_subscribes_positions(
        self,
    ) -> None:
        contracts = market_stream_contracts_for_runtime_holdings(
            SimpleNamespace(
                portfolio=(),
                positions=(
                    SimpleNamespace(
                        account="U25245596",
                        symbol="hm b",
                        security_type="STK",
                        exchange="SFB",
                        primary_exchange=None,
                        currency="SEK",
                        local_symbol="HM B",
                        position="2",
                    ),
                    SimpleNamespace(
                        account="U25245596",
                        symbol="OLD",
                        security_type="STK",
                        exchange="SFB",
                        primary_exchange=None,
                        currency="SEK",
                        local_symbol="OLD",
                        position="0",
                    ),
                ),
            )
        )

        self.assertEqual([contract.symbol for contract in contracts], ["HM B"])
        self.assertEqual(contracts[0].exchange, "SMART")
        self.assertEqual(contracts[0].primary_exchange, "SFB")

    def test_market_stream_contracts_for_open_virtual_positions_subscribes_holdings(
        self,
    ) -> None:
        engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(engine)
        session_factory = create_session_factory(engine)
        session = session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind="VIRTUAL",
                account_key="VIRTUALRL01",
                account_label="Virtual RL",
                base_currency="SEK",
                is_virtual=True,
            )
            session.add(broker_account)
            session.flush()
            session.add_all(
                [
                    PositionSnapshotRecord(
                        broker_account_id=broker_account.id,
                        is_virtual=True,
                        snapshot_at=datetime(2026, 4, 29, 14, 0, tzinfo=timezone.utc),
                        source="virtual_execution",
                        symbol="AZN",
                        exchange="SFB",
                        currency="SEK",
                        security_type="STK",
                        primary_exchange=None,
                        local_symbol="AZN",
                        quantity="1",
                        average_cost="1700",
                        market_price="1701",
                        market_value="1701",
                        unrealized_pnl="1",
                        realized_pnl="0",
                    ),
                    PositionSnapshotRecord(
                        broker_account_id=broker_account.id,
                        is_virtual=True,
                        snapshot_at=datetime(2026, 4, 29, 14, 1, tzinfo=timezone.utc),
                        source="virtual_execution",
                        symbol="OLD",
                        exchange="SFB",
                        currency="SEK",
                        security_type="STK",
                        primary_exchange=None,
                        local_symbol="OLD",
                        quantity="0",
                        average_cost=None,
                        market_price="10",
                        market_value="0",
                        unrealized_pnl="0",
                        realized_pnl="0",
                    ),
                ]
            )
            session.commit()
        finally:
            session.close()

        try:
            contracts = market_stream_contracts_for_current_holdings(
                session_factory,
            )
            self.assertEqual([contract.symbol for contract in contracts], ["AZN"])
            self.assertEqual(contracts[0].exchange, "SMART")
            self.assertEqual(contracts[0].primary_exchange, "SFB")
            self.assertEqual(contracts[0].local_symbol, "AZN")

            virtual_contracts = market_stream_contracts_for_open_virtual_positions(
                session_factory,
            )
            self.assertEqual(
                [contract.symbol for contract in virtual_contracts],
                ["AZN"],
            )
        finally:
            engine.dispose()

    def test_market_stream_contracts_for_pending_entries_prewarms_entry_symbols(
        self,
    ) -> None:
        from ibkr_trader.api.market_stream_payloads import (
            market_stream_contracts_for_pending_entries,
        )

        engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(engine)
        session_factory = create_session_factory(engine)
        session = session_factory()
        try:
            session.add_all(
                [
                    InstructionRecord(
                        instruction_id="model-routed-hem",
                        schema_version="2026-04-10",
                        source_system="q-training",
                        batch_id="batch-1",
                        account_key="VIRTUALSEEDRL01",
                        book_key="seedpicker_rl_long_01",
                        symbol="HEM",
                        exchange="SFB",
                        currency="SEK",
                        state="MODEL_ROUTED_PENDING",
                        submit_at=datetime(2026, 4, 10, 7, 0, tzinfo=timezone.utc),
                        expire_at=datetime(2030, 4, 10, 15, 30, tzinfo=timezone.utc),
                        order_type="MODEL_ROUTED",
                        side="BUY",
                        payload={},
                    ),
                    InstructionRecord(
                        instruction_id="pending-norion",
                        schema_version="2026-04-10",
                        source_system="q-training",
                        batch_id="batch-1",
                        account_key="U25245596",
                        book_key="long",
                        symbol="NORION",
                        exchange="SFB",
                        currency="SEK",
                        state="ENTRY_PENDING",
                        submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
                        expire_at=datetime(2030, 4, 10, 15, 30, tzinfo=timezone.utc),
                        order_type="LIMIT",
                        side="BUY",
                        payload={},
                    ),
                    InstructionRecord(
                        instruction_id="archived-axfo",
                        schema_version="2026-04-10",
                        source_system="q-training",
                        batch_id="batch-1",
                        account_key="U25245596",
                        book_key="long",
                        symbol="AXFO",
                        exchange="SFB",
                        currency="SEK",
                        state="ENTRY_PENDING",
                        submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
                        expire_at=datetime(2030, 4, 10, 15, 30, tzinfo=timezone.utc),
                        order_type="LIMIT",
                        side="BUY",
                        archived_at=datetime(2026, 4, 10, 8, 0, tzinfo=timezone.utc),
                        payload={},
                    ),
                ]
            )
            session.commit()
        finally:
            session.close()

        try:
            contracts = market_stream_contracts_for_pending_entries(session_factory)
            self.assertEqual(
                [contract.symbol for contract in contracts],
                ["HEM", "NORION"],
            )
            for contract in contracts:
                self.assertEqual(contract.exchange, "SMART")
                self.assertEqual(contract.primary_exchange, "SFB")
        finally:
            engine.dispose()

    def test_subscribe_open_order_market_streams_replaces_with_active_intents(
        self,
    ) -> None:
        from ibkr_trader.api.market_stream_payloads import (
            subscribe_open_order_market_streams,
        )

        class FakeMarketStreamService:
            def __init__(self) -> None:
                self.calls = []

            def subscribe_many(self, contracts, *, replace, market_data_type):
                self.calls.append((contracts, replace, market_data_type))
                return {"desired_symbols": [contract.symbol for contract in contracts]}

        engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(engine)
        session_factory = create_session_factory(engine)
        session = session_factory()
        try:
            session.add(
                InstructionRecord(
                    instruction_id="model-routed-norion",
                    schema_version="2026-04-10",
                    source_system="q-training",
                    batch_id="batch-1",
                    account_key="VIRTUALSEEDRL01",
                    book_key="seedpicker_rl_long_01",
                    symbol="NORION",
                    exchange="SFB",
                    currency="SEK",
                    state="MODEL_ROUTED_PENDING",
                    submit_at=datetime(2026, 4, 10, 7, 0, tzinfo=timezone.utc),
                    expire_at=datetime(2030, 4, 10, 15, 30, tzinfo=timezone.utc),
                    order_type="MODEL_ROUTED",
                    side="BUY",
                    payload={},
                )
            )
            session.commit()
        finally:
            session.close()

        try:
            service = FakeMarketStreamService()
            snapshot = type("Snapshot", (), {"open_orders": {}})()

            symbols = subscribe_open_order_market_streams(
                service,
                snapshot,
                session_factory,
            )

            self.assertEqual(symbols, ["NORION", "OMXS30"])
            contracts, replace, market_data_type = service.calls[0]
            self.assertEqual(
                [contract.symbol for contract in contracts],
                ["NORION", "OMXS30"],
            )
            benchmark_contract = contracts[1]
            self.assertEqual(benchmark_contract.security_type, "IND")
            self.assertEqual(benchmark_contract.exchange, "OMS")
            self.assertEqual(benchmark_contract.currency, "SEK")
            self.assertTrue(replace)
            self.assertEqual(market_data_type, "LIVE")
        finally:
            engine.dispose()

    def test_subscribe_open_order_market_streams_keeps_benchmark_without_targets(
        self,
    ) -> None:
        from ibkr_trader.api.market_stream_payloads import (
            subscribe_open_order_market_streams,
        )

        class FakeMarketStreamService:
            def __init__(self) -> None:
                self.calls = []

            def subscribe_many(self, contracts, *, replace, market_data_type):
                self.calls.append((contracts, replace, market_data_type))
                return {"desired_symbols": [contract.symbol for contract in contracts]}

        engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(engine)
        session_factory = create_session_factory(engine)

        try:
            service = FakeMarketStreamService()
            snapshot = type("Snapshot", (), {"open_orders": {}})()

            symbols = subscribe_open_order_market_streams(
                service,
                snapshot,
                session_factory,
            )

            self.assertEqual(symbols, ["OMXS30"])
            contracts, replace, market_data_type = service.calls[0]
            self.assertEqual([contract.symbol for contract in contracts], ["OMXS30"])
            self.assertEqual(contracts[0].security_type, "IND")
            self.assertEqual(contracts[0].exchange, "OMS")
            self.assertEqual(contracts[0].currency, "SEK")
            self.assertTrue(replace)
            self.assertEqual(market_data_type, "LIVE")
        finally:
            engine.dispose()

    def test_subscribe_open_order_market_streams_can_suppress_benchmark_after_hours(
        self,
    ) -> None:
        from ibkr_trader.api.market_stream_payloads import (
            subscribe_open_order_market_streams,
        )

        class FakeMarketStreamService:
            def __init__(self) -> None:
                self.calls = []

            def subscribe_many(self, contracts, *, replace, market_data_type):
                self.calls.append((contracts, replace, market_data_type))
                return {"desired_symbols": [contract.symbol for contract in contracts]}

        engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(engine)
        session_factory = create_session_factory(engine)

        try:
            service = FakeMarketStreamService()
            snapshot = type("Snapshot", (), {"open_orders": {}})()

            symbols = subscribe_open_order_market_streams(
                service,
                snapshot,
                session_factory,
                include_operator_benchmarks=False,
            )

            self.assertEqual(symbols, [])
            contracts, replace, market_data_type = service.calls[0]
            self.assertEqual(contracts, [])
            self.assertTrue(replace)
            self.assertIsNone(market_data_type)
        finally:
            engine.dispose()

    def test_operator_benchmark_stream_window_uses_session_calendar(self) -> None:
        from ibkr_trader.api.server import _should_include_operator_benchmark_streams

        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "sessions.csv"
            schedule_path.write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind",
                        "2026-06-04,Europe/Stockholm,09:00,17:30,regular",
                    ]
                ),
                encoding="utf-8",
            )

            self.assertTrue(
                _should_include_operator_benchmark_streams(
                    reference_at=datetime.fromisoformat("2026-06-04T09:00:00+02:00"),
                    runtime_timezone="Europe/Stockholm",
                    session_calendar_path=schedule_path,
                )
            )
            self.assertTrue(
                _should_include_operator_benchmark_streams(
                    reference_at=datetime.fromisoformat("2026-06-04T15:00:00+02:00"),
                    runtime_timezone="Europe/Stockholm",
                    session_calendar_path=schedule_path,
                )
            )
            self.assertFalse(
                _should_include_operator_benchmark_streams(
                    reference_at=datetime.fromisoformat("2026-06-04T17:30:01+02:00"),
                    runtime_timezone="Europe/Stockholm",
                    session_calendar_path=schedule_path,
                )
            )
            self.assertFalse(
                _should_include_operator_benchmark_streams(
                    reference_at=datetime.fromisoformat("2026-06-05T12:00:00+02:00"),
                    runtime_timezone="Europe/Stockholm",
                    session_calendar_path=schedule_path,
                )
            )

    def test_operator_snapshot_stream_overlay_marks_positions_orders_and_accounts(
        self,
    ) -> None:
        snapshot = {
            "accounts": [
                {
                    "account_key": "U25245596",
                    "net_liquidation": "100000",
                    "day_performance": {
                        "start_net_liquidation": "100000",
                        "latest_return_pct": "0.00",
                        "points": [
                            {
                                "snapshot_at": "2026-04-29T07:00:00+00:00",
                                "net_liquidation": "100000",
                                "return_pct": "0.00",
                            }
                        ],
                    },
                }
            ],
            "positions": [
                {
                    "account_key": "U25245596",
                    "symbol": "AZN",
                    "quantity": "2",
                    "average_cost": "100",
                    "market_price": "101",
                    "market_value": "202",
                    "unrealized_pnl": "2",
                }
            ],
            "open_orders": [
                {
                    "account_key": "U25245596",
                    "symbol": "AZN",
                    "working_price": "104",
                    "working_price_reference": "LIMIT",
                    "limit_price": "104",
                }
            ],
        }
        stream_snapshot = {
            "running": True,
            "desired_subscription_count": 1,
            "quote_count": 0,
            "bars_by_symbol": {
                "AZN": [
                    {
                        "timestamp": "2026-04-29T07:01:00+00:00",
                        "close": "102",
                    },
                    {
                        "timestamp": "2026-04-29T07:02:00+00:00",
                        "close": "103",
                    },
                ]
            },
        }

        enriched = enrich_operator_snapshot_with_market_stream(
            snapshot,
            stream_snapshot,
        )

        self.assertEqual(enriched["positions"][0]["market_price"], "103")
        self.assertEqual(enriched["positions"][0]["market_value"], "206")
        self.assertEqual(enriched["positions"][0]["unrealized_pnl"], "6")
        self.assertEqual(enriched["open_orders"][0]["reference_market_price"], "103")
        self.assertEqual(enriched["open_orders"][0]["last_market_price_direction"], "UP")
        self.assertEqual(enriched["open_orders"][0]["price_spread"], "+1.00")
        self.assertEqual(enriched["accounts"][0]["net_liquidation"], "100004")
        self.assertEqual(
            enriched["accounts"][0]["day_performance"]["latest_return_pct"],
            "0.00",
        )
        self.assertTrue(enriched["market_stream_overlay"]["applied"])

    def test_operator_snapshot_stream_overlay_does_not_double_count_live_position_value(
        self,
    ) -> None:
        snapshot = {
            "accounts": [
                {
                    "account_key": "U25245596",
                    "is_virtual": False,
                    "net_liquidation": "18716.12",
                    "day_performance": {"points": []},
                }
            ],
            "positions": [
                {
                    "account_key": "U25245596",
                    "symbol": "HTRO",
                    "quantity": "518",
                    "average_cost": "34.01559531",
                    "market_price": None,
                    "market_value": "0",
                    "unrealized_pnl": None,
                }
            ],
            "open_orders": [],
        }

        enriched = enrich_operator_snapshot_with_market_stream(
            snapshot,
            {
                "running": True,
                "bars_by_symbol": {
                    "HTRO": [
                        {
                            "timestamp": "2026-04-29T14:44:00+00:00",
                            "close": "32.21",
                        }
                    ]
                },
            },
        )

        self.assertEqual(enriched["positions"][0]["market_value"], "16684.78")
        self.assertEqual(enriched["accounts"][0]["net_liquidation"], "18716.12")
        self.assertEqual(enriched["market_stream_overlay"]["marked_account_count"], 0)

    def test_parse_market_stream_subscribe_payload_enriches_stockholm_identity(self) -> None:
        payload = parse_market_stream_subscribe_payload(
            {
                "symbols": ["eric-b"],
                "market_data_type": "live",
            },
            stockholm_identity_map={
                "ERIC-B": SimpleNamespace(
                    ticker_alias="ERIC B",
                    isin="SE0000108656",
                )
            },
        )

        contract = payload["contracts"][0]
        self.assertEqual(contract.symbol, "ERIC-B")
        self.assertEqual(contract.local_symbol, "ERIC B")
        self.assertEqual(contract.isin, "SE0000108656")

    def test_parse_market_stream_subscribe_payload_enriches_share_class_alias(
        self,
    ) -> None:
        payload = parse_market_stream_subscribe_payload(
            {
                "symbols": ["eric b"],
                "market_data_type": "live",
            },
            stockholm_identity_map={
                "ERIC-B": SimpleNamespace(
                    ticker_alias="ERIC B",
                    isin="SE0000108656",
                )
            },
        )

        contract = payload["contracts"][0]
        self.assertEqual(contract.symbol, "ERIC B")
        self.assertEqual(contract.local_symbol, "ERIC B")
        self.assertEqual(contract.isin, "SE0000108656")

    def test_tick_stream_sample_endpoint_returns_stream_events(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        app = create_app(
            AppConfig(
                environment="test",
                timezone="Europe/Stockholm",
                database_url="sqlite+pysqlite:///:memory:",
                session_calendar_path=Path("/tmp/day_sessions.parquet"),
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

        expected_payload = {
            "query": {
                "symbol": "AAPL",
                "exchange": "SMART",
                "currency": "USD",
                "tick_types": ["Last"],
            },
            "event_count": 1,
            "events": [
                {
                    "stream": "Last",
                    "timestamp": "2026-04-27T13:31:00Z",
                    "price": "180.25",
                    "size": "100",
                }
            ],
        }

        with (
            patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
            patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
            patch(
                "ibkr_trader.api.server.collect_tick_stream_sample",
                return_value=expected_payload,
            ) as collect_mock,
            TestClient(app) as client,
        ):
            response = client.post(
                "/v1/market-data/tick-stream-sample",
                json={
                    "symbol": "aapl",
                    "exchange": "smart",
                    "currency": "usd",
                    "primary_exchange": "nasdaq",
                    "tick_types": ["last"],
                    "duration_seconds": 1,
                    "max_events": 1,
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["event_count"], 1)
        collect_mock.assert_called_once()
        query = collect_mock.call_args.args[1]
        self.assertEqual(query.symbol, "AAPL")
        self.assertEqual(query.primary_exchange, "NASDAQ")
        self.assertEqual(query.tick_types, ("Last",))
