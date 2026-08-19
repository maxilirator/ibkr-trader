from __future__ import annotations

from tests._api_server_shared import *  # noqa: F401,F403


class ApiServerTests04(ApiServerTestCase):
    def test_rl_candidates_endpoint_returns_model_routed_names(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "rl_candidates.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            session_factory = create_session_factory(engine)
            session = session_factory()
            try:
                trader_model = TraderModelRecord(
                    model_key="long_trial_106_v1",
                    display_name="Long Trial 106 V1",
                    strategy_family="canonical_long_live_execution_policy",
                    side="LONG",
                    action_space_json=["wait", "entry_prevclose_-50bp"],
                    observation_contract_json={"bar_family": "phase1_intraday_ohlc_v1"},
                    execution_mapping_version="long_actions_v1",
                    metadata_json={},
                )
                session.add(trader_model)
                session.flush()
                session.add(
                    TraderDeploymentRecord(
                        trader_model_id=trader_model.id,
                        deployment_key="long_trial_106_virtual_shared_01",
                        account_key="VIRTUALRL01",
                        book_key="rl_shared_long_trial_106_virtual_01",
                        mode="virtual",
                        status="running",
                        is_virtual=True,
                        allowed_symbols_json=["AXFO"],
                        risk_limits_json={},
                        action_constraints_json={},
                        metadata_json={},
                    )
                )
                session.add(
                    InstructionRecord(
                        instruction_id="candidate-AXFO",
                        schema_version="2026-04-25",
                        source_system="upstream-agent",
                        batch_id="candidate-batch-001",
                        account_key="VIRTUALRL01",
                        book_key="rl_shared_long_trial_106_virtual_01",
                        symbol="AXFO",
                        exchange="XSTO",
                        currency="SEK",
                        state="MODEL_ROUTED_PENDING",
                        submit_at=datetime(2099, 4, 28, 7, 0, tzinfo=timezone.utc),
                        expire_at=datetime(2099, 4, 28, 15, 30, tzinfo=timezone.utc),
                        order_type="MODEL_ROUTED",
                        side="BUY",
                        payload={
                            "schema_version": "2026-04-25",
                            "source": {
                                "system": "upstream-agent",
                                "batch_id": "candidate-batch-001",
                                "generated_at": "2099-04-28T06:30:00Z",
                            },
                            "instruction": {
                                "instruction_id": "candidate-AXFO",
                                "account": {
                                    "account_key": "VIRTUALRL01",
                                    "book_key": "rl_shared_long_trial_106_virtual_01",
                                },
                                "instrument": {
                                    "symbol": "AXFO",
                                    "security_type": "STK",
                                    "exchange": "XSTO",
                                    "currency": "SEK",
                                },
                                "intent": {
                                    "side": "BUY",
                                    "position_side": "LONG",
                                },
                                "sizing": {
                                    "mode": "target_notional",
                                    "target_notional": "1000",
                                },
                                "execution": {
                                    "mode": "model_routed",
                                    "model_id": "long_trial_106_v1",
                                    "model_family": (
                                        "canonical_long_live_execution_policy"
                                    ),
                                    "window": {
                                        "start_at": "2099-04-28T09:00:00+02:00",
                                        "end_at": "2099-04-28T17:30:00+02:00",
                                    },
                                },
                                "trace": {
                                    "reason_code": "rl_model_routed_candidate",
                                    "trade_date": "2099-04-28",
                                },
                            },
                        },
                    )
                )
                session.add(
                    InstructionRecord(
                        instruction_id="entry-instruction-001",
                        schema_version="2026-04-10",
                        source_system="q-training",
                        batch_id="entry-batch-001",
                        account_key="VIRTUALRL01",
                        book_key="rl_shared_long_trial_106_virtual_01",
                        symbol="AXFO",
                        exchange="XSTO",
                        currency="SEK",
                        state="ENTRY_PENDING",
                        submit_at=datetime(2026, 4, 28, 7, 0, tzinfo=timezone.utc),
                        expire_at=datetime(2026, 4, 28, 15, 30, tzinfo=timezone.utc),
                        order_type="LIMIT",
                        side="BUY",
                        payload={"instruction": {"instruction_id": "entry-instruction-001"}},
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
                response = client.get(
                    "/v1/rl/candidates",
                    params={"deployment_key": "long_trial_106_virtual_shared_01"},
                )

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertTrue(body["accepted"])
            self.assertEqual(body["candidate_count"], 1)
            candidate = body["candidates"][0]
            self.assertEqual(candidate["candidate_id"], "candidate-AXFO")
            self.assertEqual(candidate["state"], "MODEL_ROUTED_PENDING")
            self.assertEqual(candidate["model_id"], "long_trial_106_v1")
            self.assertEqual(candidate["symbol"], "AXFO")
            self.assertEqual(candidate["execution_window"]["start_at"], "2099-04-28T09:00:00+02:00")
            self.assertEqual(
                candidate["candidate"]["instruction_id"],
                "candidate-AXFO",
            )

    def test_rl_candidates_endpoint_retires_completed_lifecycle_candidates(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "rl_candidate_lifecycle.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            session_factory = create_session_factory(engine)
            session = session_factory()
            try:
                model = TraderModelRecord(
                    model_key="long_trial_106_v1",
                    display_name="Long Trial 106",
                    strategy_family="bucket_booster",
                    side="LONG",
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
                        allowed_symbols_json=["AXFO"],
                        risk_limits_json={},
                        action_constraints_json={},
                        metadata_json={},
                    )
                )
                session.add(
                    InstructionRecord(
                        instruction_id="candidate-AXFO",
                        schema_version="2026-04-25",
                        source_system="upstream-agent",
                        batch_id="candidate-batch-001",
                        account_key="VIRTUALRL01",
                        book_key="rl_shared_long_trial_106_virtual_01",
                        is_virtual=True,
                        symbol="AXFO",
                        exchange="XSTO",
                        currency="SEK",
                        state="MODEL_ROUTED_PENDING",
                        submit_at=datetime(2099, 4, 28, 7, 0, tzinfo=timezone.utc),
                        expire_at=datetime(2099, 4, 28, 15, 30, tzinfo=timezone.utc),
                        order_type="MODEL_ROUTED",
                        side="BUY",
                        payload={
                            "schema_version": "2026-04-25",
                            "source": {
                                "system": "upstream-agent",
                                "batch_id": "candidate-batch-001",
                                "generated_at": "2099-04-28T06:30:00Z",
                            },
                            "instruction": {
                                "instruction_id": "candidate-AXFO",
                                "account": {
                                    "account_key": "VIRTUALRL01",
                                    "book_key": "rl_shared_long_trial_106_virtual_01",
                                },
                                "instrument": {
                                    "symbol": "AXFO",
                                    "security_type": "STK",
                                    "exchange": "XSTO",
                                    "currency": "SEK",
                                },
                                "intent": {
                                    "side": "BUY",
                                    "position_side": "LONG",
                                },
                                "sizing": {
                                    "mode": "target_notional",
                                    "target_notional": "1000",
                                },
                                "execution": {
                                    "mode": "model_routed",
                                    "model_id": "long_trial_106_v1",
                                    "window": {
                                        "start_at": "2099-04-28T09:00:00+02:00",
                                        "end_at": "2099-04-28T17:30:00+02:00",
                                    },
                                },
                                "lifecycle": {
                                    "trade_date": "2099-04-28",
                                    "scope": "account_book_side_symbol_trade_date",
                                    "max_entry_orders": 1,
                                    "max_exit_orders": 1,
                                    "allow_reentry_after_exit": False,
                                    "allow_reentry_after_cancel": False,
                                    "retire_from_active_universe_when_flat": True,
                                },
                                "trace": {
                                    "reason_code": "rl_model_routed_candidate",
                                    "trade_date": "2099-04-28",
                                },
                            },
                        },
                    )
                )
                session.add(
                    InstructionRecord(
                        instruction_id="generated-roundtrip-001",
                        schema_version="2026-04-10",
                        source_system="rl-runner",
                        batch_id="generated-batch-001",
                        account_key="VIRTUALRL01",
                        book_key="rl_shared_long_trial_106_virtual_01",
                        is_virtual=True,
                        symbol="AXFO",
                        exchange="XSTO",
                        currency="SEK",
                        state="COMPLETED",
                        submit_at=datetime(2099, 4, 28, 7, 5, tzinfo=timezone.utc),
                        expire_at=datetime(2099, 4, 28, 15, 30, tzinfo=timezone.utc),
                        order_type="MARKET",
                        side="BUY",
                        entry_filled_quantity="10",
                        exit_filled_quantity="10",
                        payload={
                            "instruction": {
                                "instruction_id": "generated-roundtrip-001",
                                "trace": {
                                    "metadata": {
                                        "rl_source_instruction_id": "candidate-AXFO"
                                    }
                                },
                            }
                        },
                    )
                )
                session.commit()
            finally:
                session.close()

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
                response = client.get(
                    "/v1/rl/candidates",
                    params={"deployment_key": "long_trial_106_virtual_shared_01"},
                )

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertTrue(body["accepted"])
            self.assertEqual(body["candidate_count"], 0)

            session = session_factory()
            try:
                candidate = session.query(InstructionRecord).filter_by(
                    instruction_id="candidate-AXFO"
                ).one()
                generated = session.query(InstructionRecord).filter_by(
                    instruction_id="generated-roundtrip-001"
                ).one()
                events = session.query(InstructionEventRecord).filter_by(
                    instruction_id=candidate.id,
                    event_type="rl_candidate_lifecycle_retired",
                ).all()
                self.assertIsNotNone(candidate.archived_at)
                self.assertIsNone(generated.archived_at)
                self.assertEqual(len(events), 1)
                self.assertEqual(
                    events[0].payload["retirement_detail"]["retirement_trigger"],
                    "completed_entry_and_exit",
                )
            finally:
                session.close()
                engine.dispose()

    def test_rl_dashboard_archives_expired_candidate_sources(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "rl_dashboard_rollover.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            session_factory = create_session_factory(engine)
            session = session_factory()
            try:
                for instruction_id, expire_at in (
                    (
                        "expired-candidate",
                        datetime(2000, 1, 1, 15, 30, tzinfo=timezone.utc),
                    ),
                    (
                        "future-candidate",
                        datetime(2099, 1, 1, 15, 30, tzinfo=timezone.utc),
                    ),
                ):
                    session.add(
                        InstructionRecord(
                            instruction_id=instruction_id,
                            schema_version="2026-04-25",
                            source_system="upstream-agent",
                            batch_id="candidate-batch-001",
                            account_key="VIRTUALRL01",
                            book_key="rl_shared_long_trial_106_virtual_01",
                            is_virtual=True,
                            symbol="AXFO",
                            exchange="XSTO",
                            currency="SEK",
                            state="MODEL_ROUTED_PENDING",
                            submit_at=datetime(
                                2000,
                                1,
                                1,
                                7,
                                0,
                                tzinfo=timezone.utc,
                            ),
                            expire_at=expire_at,
                            order_type="MODEL_ROUTED",
                            side="BUY",
                            payload={
                                "instruction": {
                                    "instruction_id": instruction_id,
                                    "execution": {
                                        "mode": "model_routed",
                                        "model_id": "long_trial_106_v1",
                                    },
                                }
                            },
                        )
                    )
                session.add(
                    InstructionRecord(
                        instruction_id="generated-position",
                        schema_version="2026-04-10",
                        source_system="rl-runner",
                        batch_id="generated-batch",
                        account_key="VIRTUALRL01",
                        book_key="rl_shared_long_trial_106_virtual_01",
                        is_virtual=True,
                        symbol="AXFO",
                        exchange="XSTO",
                        currency="SEK",
                        state="POSITION_OPEN",
                        submit_at=datetime(2000, 1, 1, 7, 0, tzinfo=timezone.utc),
                        expire_at=datetime(2099, 1, 1, 7, 0, tzinfo=timezone.utc),
                        order_type="LIMIT",
                        side="BUY",
                        payload={
                            "instruction": {
                                "instruction_id": "generated-position",
                                "trace": {
                                    "metadata": {
                                        "rl_source_instruction_id": (
                                            "expired-candidate"
                                        )
                                    }
                                },
                            }
                        },
                    )
                )
                session.commit()
            finally:
                session.close()

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
                response = client.get("/v1/read/rl-dashboard")

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertEqual(body["rl_dashboard"]["summary"]["candidate_count"], 1)
            self.assertEqual(
                body["rl_dashboard"]["candidates"][0]["candidate_id"],
                "future-candidate",
            )

            session = session_factory()
            try:
                rows = {
                    row.instruction_id: row
                    for row in session.query(InstructionRecord).all()
                }
                self.assertIsNotNone(rows["expired-candidate"].archived_at)
                self.assertIsNone(rows["future-candidate"].archived_at)
                self.assertIsNone(rows["generated-position"].archived_at)
            finally:
                session.close()
                engine.dispose()

    def test_ledger_snapshot_endpoint_returns_append_only_history(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "ledger_snapshot.db"
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

                instruction = InstructionRecord(
                    instruction_id="instr-001",
                    schema_version="2026-04-10",
                    source_system="q-training",
                    batch_id="batch-001",
                    account_key="U25245596",
                    book_key="long_risk_book",
                    symbol="SAAB",
                    exchange="SMART",
                    currency="SEK",
                    state="ENTRY_SUBMITTED",
                    submit_at=datetime(2026, 4, 19, 7, 20, tzinfo=timezone.utc),
                    expire_at=datetime(2026, 4, 19, 15, 30, tzinfo=timezone.utc),
                    order_type="LIMIT",
                    side="BUY",
                    broker_order_id=11,
                    broker_order_status="Submitted",
                    payload={},
                )
                session.add(instruction)
                session.flush()

                broker_order = BrokerOrderRecord(
                    instruction_id=instruction.id,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="U25245596",
                    order_role="ENTRY",
                    external_order_id="11",
                    external_perm_id="9001",
                    external_client_id="0",
                    order_ref="instr-001",
                    symbol="SAAB",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    primary_exchange="SFB",
                    local_symbol="SAAB-B",
                    side="BUY",
                    order_type="LMT",
                    time_in_force="DAY",
                    status="Submitted",
                    total_quantity="2",
                    limit_price="100.00",
                    stop_price=None,
                    submitted_at=datetime(2026, 4, 19, 7, 21, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 19, 7, 22, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
                session.add(broker_order)
                session.flush()

                session.add(
                    InstructionEventRecord(
                        instruction_id=instruction.id,
                        event_type="entry_submitted",
                        source="runtime",
                        event_at=datetime(2026, 4, 19, 7, 21, tzinfo=timezone.utc),
                        state_before="ENTRY_PENDING",
                        state_after="ENTRY_SUBMITTED",
                        payload={},
                        note="Runtime submitted the entry order.",
                    )
                )
                session.add(
                    BrokerOrderEventRecord(
                        broker_order_id=broker_order.id,
                        event_type="order_error_callback",
                        event_at=datetime(2026, 4, 19, 7, 22, tzinfo=timezone.utc),
                        status_before="PreSubmitted",
                        status_after="Submitted",
                        payload={"errorCode": 201, "errorMsg": "Order held for review"},
                        note="Broker callback arrived.",
                    )
                )
                session.add(
                    ExecutionFillRecord(
                        broker_order_id=broker_order.id,
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        external_execution_id="exec-001",
                        external_order_id="11",
                        external_perm_id="9001",
                        order_ref="instr-001",
                        symbol="SAAB",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        side="BOT",
                        quantity="1",
                        price="100.50",
                        commission="1.00",
                        commission_currency="SEK",
                        executed_at=datetime(2026, 4, 19, 7, 23, tzinfo=timezone.utc),
                        raw_payload={},
                    )
                )

                reconciliation_run = ReconciliationRunRecord(
                    run_kind="runtime_cycle",
                    broker_kind="IBKR",
                    account_key="U25245596",
                    runtime_timezone="Europe/Stockholm",
                    started_at=datetime(2026, 4, 19, 7, 25, tzinfo=timezone.utc),
                    completed_at=datetime(2026, 4, 19, 7, 25, 3, tzinfo=timezone.utc),
                    status="WARNINGS",
                    issue_count=1,
                    action_count=1,
                    metadata_json={},
                )
                session.add(reconciliation_run)
                session.flush()
                session.add(
                    ReconciliationIssueRecord(
                        reconciliation_run_id=reconciliation_run.id,
                        instruction_id="instr-001",
                        stage="reconcile_instruction",
                        severity="ERROR",
                        message="Order state drift detected.",
                        observed_at=datetime(2026, 4, 19, 7, 25, 3, tzinfo=timezone.utc),
                        payload={"broker_order_id": 11},
                    )
                )
                session.add(
                    InstructionSetCancellationRecord(
                        requested_at=datetime(2026, 4, 19, 7, 26, tzinfo=timezone.utc),
                        requested_by="dashboard",
                        reason="Cancel stale row.",
                        selectors={"instruction_ids": ["instr-001"]},
                        status="COMPLETED",
                        matched_instruction_count=1,
                        cancelled_pending_count=0,
                        cancelled_submitted_count=1,
                        skipped_count=0,
                        failed_count=0,
                        result_payload={
                            "results": [
                                {
                                    "instruction_id": "instr-001",
                                    "action": "cancelled_submitted_entry",
                                }
                            ]
                        },
                    )
                )
                session.commit()
            finally:
                session.close()
                engine.dispose()

            set_kill_switch_state(
                session_factory,
                enabled=True,
                reason="Freeze new entries.",
                updated_by="test-suite",
            )

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
                response = client.get("/v1/read/ledger-snapshot?focus_instruction_id=instr-001")

            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertTrue(body["accepted"])
            self.assertEqual(
                body["ledger_snapshot"]["focus_instruction"]["instruction_id"],
                "instr-001",
            )
            self.assertEqual(body["ledger_snapshot"]["summary"]["instruction_count"], 1)
            self.assertEqual(
                body["ledger_snapshot"]["broker_order_events"][0]["message"],
                "[201] Order held for review",
            )

    def test_kill_switch_endpoints_round_trip(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "controls.db"
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
                initial = client.get("/v1/controls/kill-switch")
                updated = client.post(
                    "/v1/controls/kill-switch",
                    json={
                        "enabled": True,
                        "reason": "Freeze new entries.",
                        "updated_by": "test-suite",
                    },
                )
                after = client.get("/v1/controls/kill-switch")

            self.assertEqual(initial.status_code, 200)
            self.assertFalse(initial.json()["kill_switch"]["enabled"])
            self.assertEqual(updated.status_code, 200)
            self.assertTrue(updated.json()["kill_switch"]["enabled"])
            self.assertEqual(after.status_code, 200)
            self.assertEqual(after.json()["kill_switch"]["reason"], "Freeze new entries.")
