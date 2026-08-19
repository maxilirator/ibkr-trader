from __future__ import annotations

from tests._api_server_shared import *  # noqa: F401,F403


class ApiServerTests01(ApiServerTestCase):
    def test_is_loopback_host_accepts_loopback_names_and_ips(self) -> None:
        self.assertTrue(is_loopback_host("127.0.0.1"))
        self.assertTrue(is_loopback_host("::1"))
        self.assertTrue(is_loopback_host("localhost"))
        self.assertFalse(is_loopback_host("0.0.0.0"))
        self.assertFalse(is_loopback_host("192.168.1.15"))

    def test_enforce_loopback_binding_rejects_nonlocal_host(self) -> None:
        with self.assertRaisesRegex(ValueError, "loopback"):
            enforce_loopback_binding("0.0.0.0", require_loopback_only=True)

    def test_parse_positive_limit_validates_bounds(self) -> None:
        self.assertEqual(parse_positive_limit(5, field_name="limit", maximum=10), 5)
        with self.assertRaisesRegex(ValueError, "positive"):
            parse_positive_limit(0, field_name="limit", maximum=10)
        with self.assertRaisesRegex(ValueError, "at most 10"):
            parse_positive_limit(11, field_name="limit", maximum=10)

    def test_build_rl_runtime_state_uses_current_holding_snapshot(self) -> None:
        engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(engine)
        session_factory = create_session_factory(engine)
        session = session_factory()
        try:
            model = TraderModelRecord(
                model_key="long_trial_106_v1",
                display_name="Long Trial 106",
                strategy_family="canonical_long",
                side="LONG",
                action_space_json=[],
                observation_contract_json={},
                metadata_json={},
            )
            session.add(model)
            session.flush()
            session.add(
                TraderDeploymentRecord(
                    trader_model_id=model.id,
                    deployment_key="long_trial_106_virtual_shared_01",
                    account_key="VIRTUALRL01",
                    book_key="rl_shared_long_trial_106_virtual_01",
                    mode="virtual",
                    status="running",
                    is_virtual=True,
                    allowed_symbols_json=["SHB A"],
                    risk_limits_json={},
                    action_constraints_json={},
                    metadata_json={},
                )
            )
            account = BrokerAccountRecord(
                broker_kind="virtual",
                account_key="VIRTUALRL01",
                base_currency="SEK",
                is_virtual=True,
                metadata_json={},
            )
            session.add(account)
            session.flush()
            session.add(
                InstructionRecord(
                    instruction_id="owned-shb",
                    schema_version="2026-04-10",
                    source_system="rl-runner",
                    batch_id="batch-1",
                    account_key="VIRTUALRL01",
                    book_key="rl_shared_long_trial_106_virtual_01",
                    is_virtual=True,
                    symbol="SHB A",
                    exchange="XSTO",
                    currency="SEK",
                    state="POSITION_OPEN",
                    submit_at=datetime(2026, 5, 7, 7, 5, tzinfo=timezone.utc),
                    expire_at=datetime(2026, 5, 7, 15, 30, tzinfo=timezone.utc),
                    order_type="MARKET",
                    side="BUY",
                    entry_filled_quantity="88",
                    entry_avg_fill_price="130.45",
                    entry_filled_at=datetime(2026, 5, 7, 7, 5, tzinfo=timezone.utc),
                    payload={
                        "instruction": {
                            "trace": {
                                "metadata": {
                                    "rl_deployment_key": "long_trial_106_virtual_shared_01",
                                    "rl_action_name": "market_entry",
                                    "rl_decision_id": "decision-001",
                                    "static_features": {"values": [1, 2, 3]},
                                }
                            }
                        }
                    },
                )
            )
            session.add(
                PositionSnapshotRecord(
                    broker_account_id=account.id,
                    is_virtual=True,
                    snapshot_at=datetime(2026, 5, 7, 7, 10, tzinfo=timezone.utc),
                    source="virtual_execution",
                    symbol="SHB A",
                    exchange="XSTO",
                    currency="SEK",
                    security_type="STK",
                    quantity="88",
                    average_cost="130.50",
                    market_price="131.00",
                    owner_instruction_id="owned-shb",
                    owner_source_instruction_id="candidate-shb",
                    owner_deployment_key="long_trial_106_virtual_shared_01",
                    owner_book_key="rl_shared_long_trial_106_virtual_01",
                    raw_payload={},
                )
            )
            session.commit()
        finally:
            session.close()

        try:
            snapshot = build_rl_runtime_state_snapshot(
                session_factory,
                deployment_key="long_trial_106_virtual_shared_01",
                symbols=["SHB A"],
            )
        finally:
            engine.dispose()

        row = snapshot["symbols"][0]
        self.assertEqual(row["status"], "ready")
        self.assertEqual(row["state_before"], "LONG_OPEN")
        self.assertTrue(row["runner_state"]["in_position"])
        self.assertEqual(row["runner_state"]["entry_price"], "130.50")
        self.assertEqual(row["runner_state"]["entry_bar_idx"], 0)
        self.assertEqual(row["position_snapshot"]["quantity"], "88")
        self.assertEqual(
            row["position_snapshot"]["owner_deployment_key"],
            "long_trial_106_virtual_shared_01",
        )
        self.assertEqual(
            row["active_instructions"][0]["metadata"],
            {
                "rl_deployment_key": "long_trial_106_virtual_shared_01",
                "rl_action_name": "market_entry",
                "rl_decision_id": "decision-001",
            },
        )

    def test_build_rl_runtime_state_blocks_virtual_holding_without_owner(self) -> None:
        engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(engine)
        session_factory = create_session_factory(engine)
        session = session_factory()
        try:
            model = TraderModelRecord(
                model_key="long_trial_106_v1",
                display_name="Long Trial 106",
                strategy_family="canonical_long",
                side="LONG",
                action_space_json=[],
                observation_contract_json={},
                metadata_json={},
            )
            session.add(model)
            session.flush()
            session.add(
                TraderDeploymentRecord(
                    trader_model_id=model.id,
                    deployment_key="long_trial_106_virtual_shared_01",
                    account_key="VIRTUALRL01",
                    book_key="rl_shared_long_trial_106_virtual_01",
                    mode="virtual",
                    status="running",
                    is_virtual=True,
                    allowed_symbols_json=["SHB A"],
                    risk_limits_json={},
                    action_constraints_json={},
                    metadata_json={},
                )
            )
            account = BrokerAccountRecord(
                broker_kind="virtual",
                account_key="VIRTUALRL01",
                base_currency="SEK",
                is_virtual=True,
                metadata_json={},
            )
            session.add(account)
            session.flush()
            session.add(
                InstructionRecord(
                    instruction_id="owned-shb",
                    schema_version="2026-04-10",
                    source_system="rl-runner",
                    batch_id="batch-1",
                    account_key="VIRTUALRL01",
                    book_key="rl_shared_long_trial_106_virtual_01",
                    is_virtual=True,
                    symbol="SHB A",
                    exchange="XSTO",
                    currency="SEK",
                    state="POSITION_OPEN",
                    submit_at=datetime(2026, 5, 7, 13, 35, tzinfo=timezone.utc),
                    expire_at=datetime(2026, 5, 7, 15, 30, tzinfo=timezone.utc),
                    order_type="MARKET",
                    side="BUY",
                    entry_filled_quantity="88",
                    entry_avg_fill_price="130.45",
                    payload={
                        "instruction": {
                            "trace": {
                                "metadata": {
                                    "rl_deployment_key": "long_trial_106_virtual_shared_01",
                                    "rl_action_name": "market_entry",
                                }
                            }
                        }
                    },
                )
            )
            session.add(
                PositionSnapshotRecord(
                    broker_account_id=account.id,
                    is_virtual=True,
                    snapshot_at=datetime(2026, 5, 7, 13, 40, tzinfo=timezone.utc),
                    source="virtual_execution",
                    symbol="SHB A",
                    exchange="XSTO",
                    currency="SEK",
                    security_type="STK",
                    quantity="88",
                    average_cost="130.50",
                    market_price="131.00",
                    raw_payload={},
                )
            )
            session.commit()
        finally:
            session.close()

        try:
            snapshot = build_rl_runtime_state_snapshot(
                session_factory,
                deployment_key="long_trial_106_virtual_shared_01",
                symbols=["SHB A"],
            )
        finally:
            engine.dispose()

        row = snapshot["symbols"][0]
        self.assertEqual(row["status"], "blocked")
        self.assertEqual(row["state_before"], "INCONSISTENT")
        self.assertEqual(row["blockers"][0]["reason"], "virtual_position_missing_owner")

    def test_build_rl_runtime_state_blocks_duplicate_active_positions(self) -> None:
        engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(engine)
        session_factory = create_session_factory(engine)
        session = session_factory()
        try:
            model = TraderModelRecord(
                model_key="long_trial_106_v1",
                display_name="Long Trial 106",
                strategy_family="canonical_long",
                side="LONG",
                action_space_json=[],
                observation_contract_json={},
                metadata_json={},
            )
            session.add(model)
            session.flush()
            session.add(
                TraderDeploymentRecord(
                    trader_model_id=model.id,
                    deployment_key="long_trial_106_virtual_shared_01",
                    account_key="VIRTUALRL01",
                    book_key="rl_shared_long_trial_106_virtual_01",
                    mode="virtual",
                    status="running",
                    is_virtual=True,
                    allowed_symbols_json=["BALD B"],
                    risk_limits_json={},
                    action_constraints_json={},
                    metadata_json={},
                )
            )
            account = BrokerAccountRecord(
                broker_kind="virtual",
                account_key="VIRTUALRL01",
                base_currency="SEK",
                is_virtual=True,
                metadata_json={},
            )
            session.add(account)
            session.flush()
            for idx in range(2):
                session.add(
                    InstructionRecord(
                        instruction_id=f"owned-bald-{idx}",
                        schema_version="2026-04-10",
                        source_system="rl-runner",
                        batch_id=f"batch-{idx}",
                        account_key="VIRTUALRL01",
                        book_key="rl_shared_long_trial_106_virtual_01",
                        is_virtual=True,
                        symbol="BALD B",
                        exchange="XSTO",
                        currency="SEK",
                        state="POSITION_OPEN",
                        submit_at=datetime(2026, 5, 7, 14, 30, tzinfo=timezone.utc),
                        expire_at=datetime(2026, 5, 7, 15, 30, tzinfo=timezone.utc),
                        order_type="MARKET",
                        side="BUY",
                        entry_filled_quantity="205",
                        entry_avg_fill_price="56.00",
                        payload={
                            "instruction": {
                                "trace": {
                                    "metadata": {
                                        "rl_deployment_key": "long_trial_106_virtual_shared_01",
                                        "rl_action_name": "market_entry",
                                    }
                                }
                            }
                        },
                    )
                )
            session.add(
                PositionSnapshotRecord(
                    broker_account_id=account.id,
                    is_virtual=True,
                    snapshot_at=datetime(2026, 5, 7, 14, 40, tzinfo=timezone.utc),
                    source="virtual_execution",
                    symbol="BALD B",
                    exchange="XSTO",
                    currency="SEK",
                    security_type="STK",
                    quantity="410",
                    average_cost="56.00",
                    raw_payload={},
                )
            )
            session.commit()
        finally:
            session.close()

        try:
            snapshot = build_rl_runtime_state_snapshot(
                session_factory,
                deployment_key="long_trial_106_virtual_shared_01",
                symbols=["BALD B"],
            )
        finally:
            engine.dispose()

        row = snapshot["symbols"][0]
        self.assertEqual(row["status"], "blocked")
        self.assertEqual(row["state_before"], "INCONSISTENT")
        self.assertEqual(row["blockers"][0]["reason"], "duplicate_active_positions")

    def test_parse_contract_resolve_payload_normalizes_values(self) -> None:
        query = parse_contract_resolve_payload(
            {
                "symbol": "sive",
                "security_type": "stk",
                "exchange": "xsto",
                "currency": "sek",
                "primary_exchange": "xsto",
                "isin": "SE0003917798",
            }
        )

        self.assertEqual(query.symbol, "SIVE")
        self.assertEqual(query.security_type, "STK")
        self.assertEqual(query.exchange, "XSTO")
        self.assertEqual(query.currency, "SEK")
        self.assertEqual(query.primary_exchange, "XSTO")
        self.assertEqual(query.isin, "SE0003917798")

    def test_parse_account_summary_payload_accepts_defaults(self) -> None:
        tags, group, account_id = parse_account_summary_payload({})

        self.assertIn("NetLiquidation", tags)
        self.assertEqual(group, "All")
        self.assertIsNone(account_id)

    def test_parse_historical_bars_payload_normalizes_values(self) -> None:
        query = parse_historical_bars_payload(
            {
                "symbol": "sive",
                "security_type": "stk",
                "exchange": "smart",
                "currency": "sek",
                "primary_exchange": "sfb",
                "duration": "2 D",
                "bar_size": "5 mins",
                "what_to_show": "trades",
                "use_rth": True,
                "end_at": "2026-04-10T17:30:00+02:00",
            }
        )

        self.assertEqual(query.symbol, "SIVE")
        self.assertEqual(query.security_type, "STK")
        self.assertEqual(query.exchange, "SMART")
        self.assertEqual(query.currency, "SEK")
        self.assertEqual(query.primary_exchange, "SFB")
        self.assertEqual(query.duration, "2 D")
        self.assertEqual(query.bar_size, "5 mins")
        self.assertEqual(query.what_to_show, "TRADES")
        self.assertTrue(query.use_rth)
        self.assertEqual(query.end_at.isoformat(), "2026-04-10T17:30:00+02:00")

    def test_parse_stockholm_intraday_backfill_payload_normalizes_values(self) -> None:
        query = parse_stockholm_intraday_backfill_payload(
            {
                "as_of_date": "2026-04-24",
                "bar_size": "1 min",
                "what_to_show": ["trades", "midpoint", "ask"],
                "use_rth": True,
                "max_symbols": 10,
                "start_after": "sive",
                "symbols": ["volcar-b", "sive"],
                "include_remapped": True,
                "sleep_seconds": 0.0,
                "max_runtime_seconds": 12.5,
            }
        )

        self.assertEqual(query.as_of_date.isoformat(), "2026-04-24")
        self.assertEqual(query.bar_size, "1 min")
        self.assertEqual(query.what_to_show, ("TRADES", "MIDPOINT", "ASK"))
        self.assertTrue(query.use_rth)
        self.assertEqual(query.max_symbols, 10)
        self.assertEqual(query.start_after, "sive")
        self.assertEqual(query.symbols, ("volcar-b", "sive"))
        self.assertTrue(query.include_remapped)
        self.assertEqual(query.sleep_seconds, 0.0)
        self.assertEqual(query.max_runtime_seconds, 12.5)

    def test_parse_trader_payloads_normalize_values(self) -> None:
        model_payload = parse_trader_model_payload(
            {
                "model_key": "Short_Trial36_V1",
                "display_name": "Short Trial 36 V1",
                "strategy_family": "canonical_short_live_execution_policy",
                "side": "short",
                "action_space": ["skip", "market_entry", "exit_market"],
                "observation_contract": {"bar_family": "stockholm_intraday_1m_v1"},
                "metadata": {"canonical_seed": 140},
            }
        )
        deployment_payload = parse_trader_deployment_payload(
            {
                "deployment_key": "Short_Trial36_Live_01",
                "model_key": "Short_Trial36_V1",
                "account_key": "u25245596",
                "book_key": "RL_SHORT_TRIAL36_LIVE_01",
                "mode": "live",
                "status": "running",
                "allowed_symbols": ["sive", "volv-b"],
            }
        )
        action_payload = parse_trader_action_payload(
            {
                "deployment_key": "Short_Trial36_Live_01",
                "symbol": "sive",
                "action_name": "market_entry",
                "observed_at": "2026-04-25T09:25:00+02:00",
                "state_before": "flat",
                "state_after": "entry_pending",
                "action_status": "translated",
                "payload": {"confidence": 0.73},
            }
        )
        heartbeat_payload = parse_trader_heartbeat_payload(
            {
                "status": "running",
                "last_seen_at": "2026-04-25T09:30:00+02:00",
                "last_bar_at": "2026-04-25T09:29:00+02:00",
                "metrics": {"bar_lag_seconds": 4},
            }
        )

        self.assertEqual(model_payload["model_key"], "short_trial36_v1")
        self.assertEqual(model_payload["side"], "SHORT")
        self.assertEqual(deployment_payload["deployment_key"], "short_trial36_live_01")
        self.assertEqual(deployment_payload["account_key"], "U25245596")
        self.assertEqual(deployment_payload["allowed_symbols"], ("SIVE", "VOLV-B"))
        deployment_update_payload = parse_trader_deployment_update_payload(
            {
                "status": "running",
                "allowed_symbols": ["volv-b", "sive", "volv-b"],
                "metadata": {"edited_by": "operator"},
            }
        )
        self.assertEqual(deployment_update_payload["status"], "running")
        self.assertEqual(
            deployment_update_payload["allowed_symbols"],
            ("VOLV-B", "SIVE"),
        )
        self.assertEqual(
            deployment_update_payload["metadata"]["edited_by"],
            "operator",
        )
        self.assertEqual(action_payload["symbol"], "SIVE")
        self.assertEqual(action_payload["state_before"], "FLAT")
        self.assertEqual(heartbeat_payload["status"], "running")
        self.assertEqual(
            heartbeat_payload["last_bar_at"].isoformat(),
            "2026-04-25T09:29:00+02:00",
        )

    def test_parse_trader_payloads_require_explicit_runtime_fields(self) -> None:
        with self.assertRaisesRegex(ValueError, "side is required"):
            parse_trader_model_payload(
                {
                    "model_key": "long_trial_v1",
                    "display_name": "Long Trial V1",
                    "strategy_family": "canonical_long_live_execution_policy",
                    "action_space": ["skip", "market_entry"],
                }
            )

        with self.assertRaisesRegex(ValueError, "mode is required"):
            parse_trader_deployment_payload(
                {
                    "deployment_key": "long_trial_virtual_01",
                    "model_key": "long_trial_v1",
                    "account_key": "virtual0001",
                    "book_key": "rl_long_trial_virtual_01",
                    "status": "running",
                }
            )

        with self.assertRaisesRegex(ValueError, "observed_at is required"):
            parse_trader_action_payload(
                {
                    "deployment_key": "long_trial_virtual_01",
                    "symbol": "SIVE",
                    "action_name": "market_entry",
                    "action_status": "translated",
                }
            )

        with self.assertRaisesRegex(ValueError, "last_seen_at is required"):
            parse_trader_heartbeat_payload({"status": "running"})

    def test_parse_rl_observation_build_payload_accepts_source_bars(self) -> None:
        payload = parse_rl_observation_build_payload(
            {
                "deployment_key": "Long_Trial_106_Virtual_Shared_01",
                "symbols": ["axfo", "azn"],
                "as_of": "2026-04-28T09:07:30+02:00",
                "source_bars": {"AXFO": []},
                "history_overrides": {"AXFO": {"prev_close": "100"}},
                "static_features": {
                    "AXFO": {
                        "feature_names": ["rank_score_z"],
                        "values": ["0.25"],
                    }
                },
                "include_source_bars": True,
            }
        )

        self.assertEqual(payload["deployment_key"], "long_trial_106_virtual_shared_01")
        self.assertEqual(payload["symbols"], ("AXFO", "AZN"))
        self.assertEqual(payload["as_of"].isoformat(), "2026-04-28T09:07:30+02:00")
        self.assertEqual(
            payload["static_features"]["AXFO"]["feature_names"],
            ["rank_score_z"],
        )
        self.assertTrue(payload["include_source_bars"])

    def test_completed_rl_bar_as_of_snaps_to_latest_completed_5m_bar(self) -> None:
        snapped = _completed_rl_bar_as_of(
            as_of=datetime.fromisoformat("2026-04-28T09:07:30+02:00"),
            observation_contract={
                "bar_interval": "5m",
                "session_timezone": "Europe/Stockholm",
                "session_open_local": "09:00",
                "session_close_local": "17:30",
            },
            config_overrides={},
            fallback_timezone="Europe/Stockholm",
        )

        self.assertEqual(snapped.isoformat(), "2026-04-28T09:05:00+02:00")

    def test_healthz_includes_broker_runtime_status(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "healthz.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
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
                        account_id="DU1234567",
                    ),
                    broker_warmup_enabled=False,
                )
            )

            with TestClient(app) as client:
                response = client.get("/healthz")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["status"], "ok")
        self.assertIn("broker_sessions", body)
        self.assertIn("broker_circuit", body)
        self.assertIn("broker_pacing", body)

    def test_stockholm_intraday_backfill_endpoint_returns_paged_batch(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

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

        expected_payload = {
            "query": {
                "as_of_date": "2026-04-24",
                "bar_size": "1 min",
                "what_to_show": ["TRADES", "MIDPOINT"],
                "use_rth": True,
                "max_symbols": 2,
                "start_after": None,
                "symbols": None,
                "include_remapped": False,
                "sleep_seconds": 0.0,
                "max_runtime_seconds": 55.0,
            },
            "universe": {
                "stockholm_instruments_path": "/tmp/all.txt",
                "stockholm_identity_path": "/tmp/identity.parquet",
                "current_universe_size": 705,
                "page_size": 2,
                "next_cursor": "sive",
                "requested_page_next_cursor": "sive",
            },
            "summary": {
                "requested_symbol_count": 2,
                "processed_symbol_count": 2,
                "ok_count": 2,
                "lookup_error_count": 0,
                "timeout_count": 0,
                "error_count": 0,
                "partial_count": 0,
                "skipped_remapped_count": 0,
                "unsupported_series_count": 0,
                "not_requested_series_count": 0,
                "resolves_cleanly_count": 2,
                "resolves_suspiciously_remapped_count": 0,
                "budget_exhausted": False,
                "elapsed_seconds": 0.0,
            },
            "entries": [],
        }

        with (
            patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
            patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
            patch(
                "ibkr_trader.ibkr.session_manager.ManagedSyncSession.execute",
                side_effect=lambda _operation_name, callback, **_kwargs: callback(None),
            ),
            patch(
                "ibkr_trader.api.server.collect_stockholm_intraday_backfill",
                return_value=expected_payload,
            ) as collect_mock,
            TestClient(app) as client,
        ):
            response = client.post(
                "/v1/market-data/stockholm-intraday-backfill",
                json={
                    "as_of_date": "2026-04-24",
                    "what_to_show": ["trades", "midpoint"],
                    "max_symbols": 2,
                    "sleep_seconds": 0.0,
                },
            )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["accepted"])
        self.assertEqual(body["market"], "stockholm")
        self.assertEqual(body["series_mode"], "paged_batch")
        self.assertEqual(body["summary"]["requested_symbol_count"], 2)
        self.assertEqual(body["universe"]["next_cursor"], "sive")
        collect_mock.assert_called_once()
        self.assertEqual(collect_mock.call_args.args[0].client_id, 8)
