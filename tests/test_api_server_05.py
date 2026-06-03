from __future__ import annotations

from tests._api_server_shared import *  # noqa: F401,F403


class ApiServerTests05(ApiServerTestCase):
    def test_operator_review_endpoints_round_trip(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "operator_review.db"
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

                broker_order = BrokerOrderRecord(
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

                broker_event = BrokerOrderEventRecord(
                    broker_order_id=broker_order.id,
                    event_type="order_error_callback",
                    event_at=datetime(2026, 4, 19, 7, 22, tzinfo=timezone.utc),
                    status_before="PreSubmitted",
                    status_after="Submitted",
                    payload={"errorCode": 201, "errorMsg": "Order held for review"},
                    note="Broker callback arrived.",
                )
                session.add(broker_event)

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
                issue = ReconciliationIssueRecord(
                    reconciliation_run_id=reconciliation_run.id,
                    instruction_id="instr-001",
                    stage="reconcile_instruction",
                    severity="ERROR",
                    message="Order state drift detected.",
                    observed_at=datetime(2026, 4, 19, 7, 25, 3, tzinfo=timezone.utc),
                    payload={"broker_order_id": broker_order.id},
                )
                session.add(issue)
                session.commit()
            finally:
                session.close()
                engine.dispose()

            app = create_app(
                AppConfig(
                    environment="test",
                    timezone="Europe/Stockholm",
                    database_url=database_url,
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
                        account_id="U25245596",
                    ),
                )
            )

            with (
                patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
                patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
                TestClient(app) as client,
            ):
                attention_response = client.post(
                    "/v1/broker-attention/1/review",
                    json={"action": "ARCHIVE", "updated_by": "test-suite"},
                )
                issue_response = client.post(
                    "/v1/reconciliation-issues/1/review",
                    json={"action": "RESOLVE", "updated_by": "test-suite"},
                )
                archive_response = client.post(
                    "/v1/reconciliation-issues/archive-open",
                    json={"updated_by": "test-suite"},
                )

            self.assertEqual(attention_response.status_code, 200)
            self.assertEqual(
                attention_response.json()["operator_review"]["status"],
                "ARCHIVED",
            )
            self.assertEqual(issue_response.status_code, 200)
            self.assertEqual(
                issue_response.json()["operator_review"]["status"],
                "RESOLVED",
            )
            self.assertEqual(archive_response.status_code, 200)
            self.assertEqual(
                archive_response.json()["reconciliation_issue_archive"][
                    "archived_issue_count"
                ],
                1,
            )

    def test_submit_endpoint_rejects_when_kill_switch_is_enabled(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "submit_kill_switch.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            engine.dispose()
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)

            app = create_app(
                AppConfig(
                    environment="test",
                    timezone="Europe/Stockholm",
                    database_url=database_url,
                    session_calendar_path=schedule_path,
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
                client.post(
                    "/v1/controls/kill-switch",
                    json={
                        "enabled": True,
                        "reason": "Freeze new entries.",
                        "updated_by": "test-suite",
                    },
                )
                response = client.post("/v1/instructions/submit", json=_sample_submit_payload())

            self.assertEqual(response.status_code, 409)
            self.assertIn("kill switch", response.text)

    def test_submit_endpoint_accepts_exact_replay_and_rejects_changed_duplicate(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "submit_idempotency.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            engine.dispose()
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)

            app = create_app(
                AppConfig(
                    environment="test",
                    timezone="Europe/Stockholm",
                    database_url=database_url,
                    session_calendar_path=schedule_path,
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

            changed_payload = deepcopy(_sample_submit_payload())
            changed_instruction = changed_payload["instructions"][0]
            assert isinstance(changed_instruction, dict)
            changed_entry = changed_instruction["entry"]
            assert isinstance(changed_entry, dict)
            changed_entry["limit_price"] = "11.9999"

            with (
                patch("ibkr_trader.api.server.CanonicalSyncSessions.warmup", return_value=None),
                patch("ibkr_trader.api.server.CanonicalSyncSessions.shutdown", return_value=None),
                TestClient(app) as client,
            ):
                first = client.post("/v1/instructions/submit", json=_sample_submit_payload())
                replay = client.post("/v1/instructions/submit", json=_sample_submit_payload())
                changed = client.post("/v1/instructions/submit", json=changed_payload)

            self.assertEqual(first.status_code, 200)
            self.assertEqual(replay.status_code, 200)
            self.assertEqual(
                replay.json()["submitted"]["instructions"][0]["record_id"],
                first.json()["submitted"]["instructions"][0]["record_id"],
            )
            self.assertEqual(changed.status_code, 409)
            self.assertIn("different payload", changed.text)

    def test_virtual_account_and_market_watch_endpoints_persist_virtual_rows(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "virtual_api.db"
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
                account_response = client.post(
                    "/v1/virtual/accounts",
                    json={
                        "account_key": "virtual0001",
                        "base_currency": "SEK",
                        "account_label": "RL virtual sandbox",
                        "cash_balance": "200000",
                    },
                )
                quote_response = client.post(
                    "/v1/virtual/market-watch",
                    json={
                        "account_key": "virtual0001",
                        "observed_at": "2026-04-27T09:01:00Z",
                        "symbol": "sive",
                        "security_type": "stk",
                        "exchange": "xsto",
                        "currency": "sek",
                        "bid_price": "10.00",
                        "ask_price": "10.00",
                        "last_price": "10.00",
                        "source": "test-suite",
                    },
                )
                list_response = client.get(
                    "/v1/virtual/market-watch?account_key=virtual0001&limit=5"
                )

            self.assertEqual(account_response.status_code, 200)
            self.assertEqual(quote_response.status_code, 200)
            self.assertEqual(list_response.status_code, 200)
            account_body = account_response.json()["virtual_account"]
            self.assertEqual(account_body["account_key"], "VIRTUAL0001")
            self.assertEqual(account_body["broker_kind"], "VIRTUAL")
            self.assertTrue(account_body["is_virtual"])
            self.assertEqual(account_body["cash_balance"], "200000")

            quote_body = quote_response.json()["virtual_market_watch"]
            self.assertEqual(quote_body["quote"]["account_key"], "VIRTUAL0001")
            self.assertEqual(quote_body["quote"]["symbol"], "SIVE")
            self.assertEqual(quote_body["filled_order_count"], 0)
            self.assertEqual(list_response.json()["quote_count"], 1)

            engine = build_engine(database_url)
            session_factory = create_session_factory(engine)
            session = session_factory()
            try:
                account = (
                    session.query(BrokerAccountRecord)
                    .filter_by(broker_kind="VIRTUAL", account_key="VIRTUAL0001")
                    .one()
                )
                quote = (
                    session.query(VirtualMarketQuoteRecord)
                    .filter_by(account_key="VIRTUAL0001", symbol="SIVE")
                    .one()
                )
                snapshot_count = (
                    session.query(AccountSnapshotRecord)
                    .filter_by(broker_account_id=account.id, is_virtual=True)
                    .count()
                )
                self.assertTrue(account.is_virtual)
                self.assertEqual(quote.currency, "SEK")
                self.assertEqual(quote.ask_price, "10.00")
                self.assertGreaterEqual(snapshot_count, 1)
                latest_snapshot = (
                    session.query(AccountSnapshotRecord)
                    .filter_by(broker_account_id=account.id, is_virtual=True)
                    .order_by(AccountSnapshotRecord.snapshot_at.desc())
                    .first()
                )
                self.assertEqual(latest_snapshot.total_cash_value, "200000")
                self.assertEqual(latest_snapshot.buying_power, "200000")
                self.assertEqual(latest_snapshot.available_funds, "200000")
            finally:
                session.close()
                engine.dispose()

    def test_cancel_set_endpoint_cancels_pending_instructions(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "cancel_set.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            session_factory = create_session_factory(engine)
            session = session_factory()
            try:
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
                session.commit()
            finally:
                session.close()
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
                response = client.post(
                    "/v1/instructions/cancel-set",
                    json={
                        "batch_id": "batch-001",
                        "requested_by": "test-suite",
                    },
                )

            self.assertEqual(response.status_code, 200)
            body = response.json()["cancelled_instruction_set"]
            self.assertEqual(body["status"], "COMPLETED")
            self.assertEqual(body["cancelled_pending_count"], 1)
            self.assertEqual(body["matched_instruction_count"], 1)

    def test_ibkr_telemetry_limit_must_be_positive(self) -> None:
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

        client = TestClient(app)
        response = client.get("/v1/ibkr/telemetry?recent_limit=0")
        self.assertEqual(response.status_code, 400)
        self.assertIn("recent_limit", response.text)

    def test_parse_runtime_cycle_payload_accepts_optional_timestamp(self) -> None:
        now_at, timeout, instruction_ids = parse_runtime_cycle_payload(
            {
                "now_at": "2026-04-13T09:00:00+02:00",
                "timeout": 15,
                "instruction_ids": ["instruction-1", "instruction-2"],
            }
        )

        self.assertEqual(now_at.isoformat(), "2026-04-13T09:00:00+02:00")
        self.assertEqual(timeout, 15)
        self.assertEqual(instruction_ids, ("instruction-1", "instruction-2"))

    def test_parse_kill_switch_payload_requires_boolean_enabled(self) -> None:
        enabled, reason, updated_by = parse_kill_switch_payload(
            {
                "enabled": True,
                "reason": "Freeze new entries.",
                "updated_by": "dashboard",
            }
        )

        self.assertTrue(enabled)
        self.assertEqual(reason, "Freeze new entries.")
        self.assertEqual(updated_by, "dashboard")

        with self.assertRaisesRegex(ValueError, "boolean"):
            parse_kill_switch_payload({"enabled": "yes"})

    def test_parse_operator_review_payload_requires_valid_action_and_updated_by(self) -> None:
        action, updated_by, note = parse_operator_review_payload(
            {
                "action": "ACKNOWLEDGE",
                "updated_by": "dashboard",
                "note": "Looks good.",
            }
        )

        self.assertEqual(action, "ACKNOWLEDGE")
        self.assertEqual(updated_by, "dashboard")
        self.assertEqual(note, "Looks good.")

        with self.assertRaisesRegex(ValueError, "required"):
            parse_operator_review_payload({})

        with self.assertRaisesRegex(ValueError, "updated_by"):
            parse_operator_review_payload({"action": "ACKNOWLEDGE", "updated_by": "   "})

    def test_parse_execution_batch_payload_validates_contract(self) -> None:
        batch = parse_execution_batch_payload(
            {
                "schema_version": "2026-04-10",
                "source": {
                    "system": "q-training",
                    "batch_id": "trial_27-2026-04-10-prod-long-01",
                    "generated_at": "2026-04-10T02:15:44Z",
                    "release_id": "release-1",
                    "strategy_id": "trial_27",
                    "policy_id": "policy-1",
                },
                "instructions": [
                    {
                        "instruction_id": "2026-04-10-GTW05-long_risk_book-SIVE-long-01",
                        "account": {
                            "account_key": "GTW05",
                            "book_key": "long_risk_book",
                            "book_role": "prod",
                            "book_side": "long",
                        },
                        "instrument": {
                            "symbol": "sive",
                            "security_type": "stk",
                            "exchange": "xsto",
                            "currency": "sek",
                            "isin": "SE0003917798",
                            "aliases": ["SIVE.ST", "sivers-ima"],
                        },
                        "intent": {
                            "side": "buy",
                            "position_side": "long",
                        },
                        "sizing": {
                            "mode": "fraction_of_account_nav",
                            "target_fraction_of_account": "1.0",
                        },
                        "entry": {
                            "order_type": "limit",
                            "submit_at": "2026-04-10T09:25:00+02:00",
                            "expire_at": "2026-04-10T17:30:00+02:00",
                            "limit_price": "11.3131",
                            "time_in_force": "day",
                            "max_submit_count": 1,
                            "cancel_unfilled_at_expiry": True,
                        },
                        "exit": {
                            "take_profit_pct": "0.02",
                            "catastrophic_stop_loss_pct": "0.15",
                            "force_exit_next_session_open": True,
                        },
                        "trace": {
                            "reason_code": "risk_policy_orderbook",
                            "execution_policy": "policy-x",
                            "trade_date": "2026-04-10",
                            "data_cutoff_date": "2026-04-09",
                            "company_name": "Sivers Semiconductors",
                            "metadata": {
                                "entry_reference_type": "prev_close",
                                "entry_reference_price": "11.37",
                            },
                        },
                    }
                ],
            }
        )

        serialized = serialize_execution_batch(batch)

        self.assertEqual(serialized["schema_version"], "2026-04-10")
        self.assertEqual(serialized["instructions"][0]["instrument"]["symbol"], "SIVE")
        self.assertEqual(serialized["instructions"][0]["instrument"]["exchange"], "XSTO")
        self.assertEqual(serialized["instructions"][0]["entry"]["limit_price"], "11.3131")
        self.assertEqual(
            serialized["instructions"][0]["sizing"]["target_fraction_of_account"],
            "1.0",
        )

    def test_parse_execution_batch_payload_requires_absolute_timestamps(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone"):
            parse_execution_batch_payload(
                {
                    "schema_version": "2026-04-10",
                    "source": {
                        "system": "q-training",
                        "batch_id": "trial_27-2026-04-10-prod-long-01",
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
                                "symbol": "SIVE",
                                "security_type": "STK",
                                "exchange": "XSTO",
                                "currency": "SEK",
                            },
                            "intent": {
                                "side": "BUY",
                                "position_side": "LONG",
                            },
                            "sizing": {
                                "mode": "fraction_of_account_nav",
                                "target_fraction_of_account": "1.0",
                            },
                            "entry": {
                                "order_type": "LIMIT",
                                "submit_at": "2026-04-10T09:25:00",
                                "expire_at": "2026-04-10T17:30:00+02:00",
                                "limit_price": "11.3131",
                            },
                            "exit": {
                                "take_profit_pct": "0.02",
                            },
                            "trace": {
                                "reason_code": "risk_policy_orderbook",
                            },
                        }
                    ],
                }
            )

    def test_parse_execution_batch_payload_requires_single_sizing_target(self) -> None:
        with self.assertRaisesRegex(ValueError, "exactly one"):
            parse_execution_batch_payload(
                {
                    "schema_version": "2026-04-10",
                    "source": {
                        "system": "q-training",
                        "batch_id": "trial_27-2026-04-10-prod-long-01",
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
                                "symbol": "SIVE",
                                "security_type": "STK",
                                "exchange": "XSTO",
                                "currency": "SEK",
                            },
                            "intent": {
                                "side": "BUY",
                                "position_side": "LONG",
                            },
                            "sizing": {
                                "mode": "fraction_of_account_nav",
                                "target_fraction_of_account": "1.0",
                                "target_notional": "100000",
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
                                "reason_code": "risk_policy_orderbook",
                            },
                        }
                    ],
                }
            )

    def test_serialize_runtime_schedule_preview_projects_stockholm_times(self) -> None:
        batch = parse_execution_batch_payload(
            {
                "schema_version": "2026-04-10",
                "source": {
                    "system": "q-training",
                    "batch_id": "trial_27-2026-04-10-prod-long-01",
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
                            "symbol": "SIVE",
                            "security_type": "STK",
                            "exchange": "XSTO",
                            "currency": "SEK",
                        },
                        "intent": {
                            "side": "BUY",
                            "position_side": "LONG",
                        },
                        "sizing": {
                            "mode": "fraction_of_account_nav",
                            "target_fraction_of_account": "1.0",
                        },
                        "entry": {
                            "order_type": "LIMIT",
                            "submit_at": "2026-04-10T07:25:00Z",
                            "expire_at": "2026-04-10T15:30:00Z",
                            "limit_price": "11.3131",
                        },
                        "exit": {
                            "force_exit_next_session_open": True,
                        },
                        "trace": {
                            "reason_code": "risk_policy_orderbook",
                        },
                    }
                ],
            }
        )

        preview = serialize_runtime_schedule_preview(
            build_batch_runtime_schedule(batch, runtime_timezone="Europe/Stockholm")
        )

        self.assertEqual(preview["runtime_timezone"], "Europe/Stockholm")
        self.assertEqual(
            preview["instructions"][0]["submit_at_runtime"],
            "2026-04-10T09:25:00+02:00",
        )
        self.assertEqual(
            preview["instructions"][0]["next_session_exit"]["status"],
            "calendar_required",
        )
