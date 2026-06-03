from __future__ import annotations

from tests._rl_action_translation_shared import *  # noqa: F401,F403


class RLActionTranslationApiTests01(RLActionTranslationApiTestsBase):
    def test_translate_endpoint_submits_and_logs_deterministic_instruction(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            schedule_path = temp_path / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            database_url = f"sqlite+pysqlite:///{temp_path / 'rl_translate.db'}"
            engine = build_engine(database_url)
            create_schema(engine)
            engine.dispose()

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
                model_response = client.post(
                    "/v1/rl/models/register",
                    json={
                        "model_key": "long_trial_106_v1",
                        "display_name": "Long Trial 106 V1",
                        "strategy_family": "canonical_long",
                        "side": "LONG",
                        "action_space": [
                            "skip",
                            "wait",
                            "market_entry",
                            "entry_prevclose_-50bp",
                            "exit_tp_200bp",
                        ],
                        "observation_contract": {
                            "bar_family": "phase1_intraday_ohlc_v1",
                            "bar_interval": "5m",
                        },
                        "execution_mapping_version": "long_actions_v1",
                    },
                )
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
                        "risk_limits": {},
                        "action_constraints": {
                            "position_side": "LONG",
                            "execution_mapping_version": "long_actions_v1",
                        },
                    },
                )
                source_response = client.post(
                    "/v1/instructions/submit",
                    json=_model_routed_payload(
                        instruction_id="api-long-axfo-1",
                        model_id="long_trial_106_v1",
                        symbol="AXFO",
                        side="LONG",
                        book_key="rl_shared_long_trial_106_virtual_01",
                    ),
                )
                translate_response = client.post(
                    "/v1/rl/actions/translate",
                    json={
                        "deployment_key": "long_trial_106_virtual_shared_01",
                        "source_instruction_id": "api-long-axfo-1",
                        "action_name": "entry_prevclose_-50bp",
                        "state_before": "FLAT",
                        "observed_at": "2026-04-27T07:05:00Z",
                        "previous_close": "100",
                        "decision_id": "2026-04-27T07:05:00Z",
                        "submit": True,
                        "log_action": True,
                        "model_diagnostics": {
                            "chosen_action": "entry_prevclose_-50bp",
                            "action_margin": "0.42",
                            "q_values": ["0.1", "0.5"],
                        },
                    },
                )

        self.assertEqual(model_response.status_code, 200)
        self.assertEqual(deployment_response.status_code, 200)
        self.assertEqual(source_response.status_code, 200)
        self.assertEqual(translate_response.status_code, 200)
        body = translate_response.json()
        self.assertTrue(body["accepted"])
        self.assertTrue(body["submitted"])
        self.assertEqual(body["translation"]["action_status"], "translated")
        instruction = body["translation"]["instruction_payload"]["instructions"][0]
        self.assertEqual(instruction["intent"], {"side": "BUY", "position_side": "LONG"})
        self.assertEqual(instruction["entry"]["limit_price"], "99.5000")
        self.assertEqual(body["submitted_batch"]["instruction_count"], 1)
        self.assertEqual(body["trader_action"]["action_status"], "translated")
        self.assertEqual(
            body["trader_action"]["payload"]["model_diagnostics"]["chosen_action"],
            "entry_prevclose_-50bp",
        )

    def test_translate_endpoint_executes_owned_long_take_profit_exit(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            schedule_path = temp_path / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            database_url = f"sqlite+pysqlite:///{temp_path / 'rl_exit_translate.db'}"
            engine = build_engine(database_url)
            create_schema(engine)
            engine.dispose()

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
                    "/v1/rl/models/register",
                    json={
                        "model_key": "long_trial_106_v1",
                        "display_name": "Long Trial 106 V1",
                        "strategy_family": "canonical_long",
                        "side": "LONG",
                        "action_space": [
                            "skip",
                            "wait",
                            "market_entry",
                            "entry_prevclose_-50bp",
                            "exit_tp_200bp",
                            "clear_exit",
                        ],
                        "observation_contract": {
                            "bar_family": "phase1_intraday_ohlc_v1",
                            "bar_interval": "5m",
                        },
                        "execution_mapping_version": "long_actions_v1",
                    },
                )
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
                        "risk_limits": {},
                        "action_constraints": {
                            "position_side": "LONG",
                            "execution_mapping_version": "long_actions_v1",
                        },
                    },
                )
                client.post(
                    "/v1/instructions/submit",
                    json=_model_routed_payload(
                        instruction_id="api-long-exit-axfo-1",
                        model_id="long_trial_106_v1",
                        symbol="AXFO",
                        side="LONG",
                        book_key="rl_shared_long_trial_106_virtual_01",
                    ),
                )
                entry_response = client.post(
                    "/v1/rl/actions/translate",
                    json={
                        "deployment_key": "long_trial_106_virtual_shared_01",
                        "source_instruction_id": "api-long-exit-axfo-1",
                        "action_name": "entry_prevclose_-50bp",
                        "state_before": "FLAT",
                        "observed_at": "2026-04-27T07:05:00Z",
                        "previous_close": "100",
                        "decision_id": "entry-decision",
                        "submit": True,
                        "log_action": True,
                    },
                )
                generated_instruction_id = entry_response.json()["translation"][
                    "instruction_payload"
                ]["instructions"][0]["instruction_id"]
                inspection_engine = build_engine(database_url)
                inspection_session_factory = create_session_factory(inspection_engine)
                session = inspection_session_factory()
                try:
                    instruction = session.execute(
                        select(InstructionRecord).where(
                            InstructionRecord.instruction_id == generated_instruction_id
                        )
                    ).scalar_one()
                    instruction.state = ExecutionState.POSITION_OPEN.value
                    instruction.entry_filled_quantity = "1"
                    instruction.entry_avg_fill_price = "99.50"
                    session.commit()
                finally:
                    session.close()
                    inspection_engine.dispose()
                exit_response = client.post(
                    "/v1/rl/actions/translate",
                    json={
                        "deployment_key": "long_trial_106_virtual_shared_01",
                        "source_instruction_id": "api-long-exit-axfo-1",
                        "action_name": "exit_tp_200bp",
                        "state_before": "LONG_OPEN",
                        "observed_at": "2026-04-27T07:10:00Z",
                        "previous_close": "100",
                        "decision_id": "exit-decision",
                        "submit": True,
                        "log_action": True,
                    },
                )

        self.assertEqual(exit_response.status_code, 200)
        body = exit_response.json()
        self.assertTrue(body["submitted"])
        self.assertEqual(body["translation"]["action_status"], "translated")
        self.assertEqual(body["action_execution"]["state_after"], "EXIT_PENDING")
        self.assertEqual(body["action_execution"]["limit_price"], "101.4900")
        self.assertEqual(body["trader_action"]["action_status"], "executed")

    def test_short_limit_entry_fills_only_when_stream_crosses_up_to_limit(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            result = _translate(
                _model_routed_payload(
                    instruction_id="short-cross-1",
                    model_id="short_trial36_v1",
                    symbol="AZA",
                    side="SHORT",
                    book_key="rl_shared_short_trial_36_virtual_01",
                ),
                deployment_key="short_trial_36_virtual_shared_01",
                action_name="entry_prevclose_88bp",
            )
            self._submit_translated(result.instruction_payload, schedule_path)

            self._record_quote(symbol="AZA", price=Decimal("100.00"), minute=5)
            self._run_cycle(schedule_path, 6)
            self._run_cycle(schedule_path, 7)
            session = self.session_factory()
            try:
                order = session.execute(select(BrokerOrderRecord)).scalar_one()
                instruction = session.execute(select(InstructionRecord)).scalar_one()
                self.assertEqual(order.status, "Submitted")
                self.assertEqual(instruction.state, ExecutionState.ENTRY_SUBMITTED.value)
            finally:
                session.close()

            self._record_quote(symbol="AZA", price=Decimal("100.88"), minute=8)
            self._run_cycle(schedule_path, 9)
            session = self.session_factory()
            try:
                order = session.execute(select(BrokerOrderRecord)).scalar_one()
                instruction = session.execute(select(InstructionRecord)).scalar_one()
                self.assertEqual(order.status, "FILLED")
                self.assertEqual(instruction.state, ExecutionState.POSITION_OPEN.value)
            finally:
                session.close()

    def test_market_entry_fills_on_next_virtual_runtime_cycle(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            _write_schedule_fixture(schedule_path)
            result = _translate(
                _model_routed_payload(
                    instruction_id="long-market-fill-1",
                    model_id="long_trial_106_v1",
                    symbol="AZN",
                    side="LONG",
                    book_key="rl_shared_long_trial_106_virtual_01",
                ),
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="market_entry",
            )
            self._submit_translated(result.instruction_payload, schedule_path)

            self._record_quote(symbol="AZN", price=Decimal("101.25"), minute=5)
            self._run_cycle(schedule_path, 6)
            self._run_cycle(schedule_path, 7)
            session = self.session_factory()
            try:
                order = session.execute(select(BrokerOrderRecord)).scalar_one()
                instruction = session.execute(select(InstructionRecord)).scalar_one()
                self.assertEqual(order.order_type, "MKT")
                self.assertEqual(order.side, "BUY")
                self.assertEqual(order.status, "FILLED")
                self.assertEqual(instruction.state, ExecutionState.POSITION_OPEN.value)
            finally:
                session.close()
