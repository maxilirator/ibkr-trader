from __future__ import annotations

from tests._rl_action_translation_shared import *  # noqa: F401,F403


class RLActionVirtualExecutionTests01(RLActionVirtualExecutionTestsBase):
    def test_long_limit_entry_fills_only_when_stream_crosses_down_to_limit(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            _write_schedule_fixture(schedule_path)
            result = _translate(
                _model_routed_payload(
                    instruction_id="long-cross-1",
                    model_id="long_trial_106_v1",
                    symbol="AXFO",
                    side="LONG",
                    book_key="rl_shared_long_trial_106_virtual_01",
                ),
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="entry_prevclose_-50bp",
            )
            self._submit_translated(result.instruction_payload, schedule_path)

            self._record_quote(symbol="AXFO", price=Decimal("100.00"), minute=5)
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

            self._record_quote(symbol="AXFO", price=Decimal("99.50"), minute=8)
            self._run_cycle(schedule_path, 9)
            session = self.session_factory()
            try:
                order = session.execute(select(BrokerOrderRecord)).scalar_one()
                instruction = session.execute(select(InstructionRecord)).scalar_one()
                self.assertEqual(order.status, "FILLED")
                self.assertEqual(instruction.state, ExecutionState.POSITION_OPEN.value)
            finally:
                session.close()

    def test_virtual_decision_bar_matches_training_limit_fill_semantics(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            _write_schedule_fixture(schedule_path)
            result = _translate(
                _model_routed_payload(
                    instruction_id="long-parity-1",
                    model_id="long_trial_106_v1",
                    symbol="AXFO",
                    side="LONG",
                    book_key="rl_shared_long_trial_106_virtual_01",
                ),
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="entry_prevclose_-50bp",
            )
            self._submit_translated(result.instruction_payload, schedule_path)
            cycle = self._run_cycle(schedule_path, 5)
            self.assertEqual(len(cycle.submitted_entries), 1)

            # q-training fills a long prev-close -50bp limit when the same
            # decision bar's low crosses 99.50; the virtual tape must do the same.
            record_virtual_market_quote(
                self.session_factory,
                account_key="VIRTUALRL01",
                symbol="AXFO",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                last_price=Decimal("100.20"),
                bid_price=Decimal("100.20"),
                ask_price=Decimal("100.20"),
                observed_at=datetime(2026, 4, 27, 7, 5, tzinfo=timezone.utc),
                source="rl_decision_bar",
                raw_payload={
                    "latest_stream_bar": {
                        "timestamp": "2026-04-27T07:00:00+00:00",
                        "open": "100.00",
                        "high": "100.80",
                        "low": "99.40",
                        "close": "100.20",
                    }
                },
                metadata={"fill_price_policy": "training_limit_price"},
            )

        session = self.session_factory()
        try:
            order = session.execute(select(BrokerOrderRecord)).scalar_one()
            fill = session.execute(select(ExecutionFillRecord)).scalar_one()
            self.assertEqual(order.status, "FILLED")
            self.assertEqual(fill.price, "99.5000")
            self.assertEqual(
                fill.raw_payload["condition_code"],
                "BUY_LIMIT_MET:STREAM_BAR_LOW",
            )
        finally:
            session.close()

    def test_virtual_entry_parity_with_q_training_prevclose_limit_replay(self) -> None:
        IntradayReplaySpec, simulate_component_session = _load_q_training_intraday_simulator()
        if IntradayReplaySpec is None or simulate_component_session is None:
            self.skipTest("q-training intraday simulator is not available")

        import pandas as pd

        cases = [
            {
                "symbol": "AXFO",
                "side": "LONG",
                "model_id": "long_trial_106_v1",
                "book_key": "rl_shared_long_trial_106_virtual_01",
                "deployment_key": "long_trial_106_virtual_shared_01",
                "action_name": "entry_prevclose_-50bp",
                "entry_prev_close_rel": -0.005,
                "bar": {
                    "open": "100.00",
                    "high": "100.80",
                    "low": "99.40",
                    "close": "100.20",
                },
                "condition_code": "BUY_LIMIT_MET:STREAM_BAR_LOW",
            },
            {
                "symbol": "AZA",
                "side": "SHORT",
                "model_id": "short_trial36_v1",
                "book_key": "rl_shared_short_trial_36_virtual_01",
                "deployment_key": "short_trial_36_virtual_shared_01",
                "action_name": "entry_prevclose_88bp",
                "entry_prev_close_rel": 0.0088,
                "bar": {
                    "open": "100.70",
                    "high": "101.20",
                    "low": "100.20",
                    "close": "100.60",
                },
                "condition_code": "SELL_LIMIT_MET:STREAM_BAR_HIGH",
            },
        ]

        for case in cases:
            with self.subTest(side=case["side"]):
                engine = build_engine("sqlite+pysqlite:///:memory:")
                create_schema(engine)
                session_factory = create_session_factory(engine)
                self.session_factory = session_factory
                self.engine = engine
                try:
                    with TemporaryDirectory() as temp_dir:
                        schedule_path = Path(temp_dir) / "day_sessions.csv"
                        _write_schedule_fixture(schedule_path)
                        bar = case["bar"]
                        replay = simulate_component_session(
                            session_df=pd.DataFrame(
                                [
                                    {
                                        "instrument": case["symbol"].lower(),
                                        "session_date": pd.Timestamp("2026-04-27"),
                                        "ts_local": pd.Timestamp(
                                            "2026-04-27 09:05:00+02:00"
                                        ),
                                        "open": float(bar["open"]),
                                        "high": float(bar["high"]),
                                        "low": float(bar["low"]),
                                        "close": float(bar["close"]),
                                    }
                                ]
                            ),
                            side=str(case["side"]).lower(),
                            weight=1.0,
                            spec=IntradayReplaySpec(
                                entry_mode="prev_close_limit",
                                entry_bar_offset=0,
                                entry_prev_close_rel=case["entry_prev_close_rel"],
                            ),
                            prev_close_price=100.0,
                        )
                        self.assertTrue(replay.entry_filled)

                        result = _translate(
                            _model_routed_payload(
                                instruction_id=f"{case['side'].lower()}-parity-1",
                                model_id=case["model_id"],
                                symbol=case["symbol"],
                                side=case["side"],
                                book_key=case["book_key"],
                            ),
                            deployment_key=case["deployment_key"],
                            action_name=case["action_name"],
                        )
                        self._submit_translated(result.instruction_payload, schedule_path)
                        cycle = self._run_cycle(schedule_path, 5)
                        self.assertEqual(len(cycle.submitted_entries), 1)

                        record_virtual_market_quote(
                            session_factory,
                            account_key="VIRTUALRL01",
                            symbol=case["symbol"],
                            exchange="SMART",
                            currency="SEK",
                            security_type="STK",
                            primary_exchange="SFB",
                            last_price=Decimal(bar["close"]),
                            bid_price=Decimal(bar["close"]),
                            ask_price=Decimal(bar["close"]),
                            observed_at=datetime(2026, 4, 27, 7, 5, tzinfo=timezone.utc),
                            source="rl_decision_bar",
                            raw_payload={
                                "latest_stream_bar": {
                                    "timestamp": "2026-04-27T07:05:00+00:00",
                                    **bar,
                                }
                            },
                            metadata={"fill_price_policy": "training_limit_price"},
                        )

                        session = session_factory()
                        try:
                            fill = session.execute(select(ExecutionFillRecord)).scalar_one()
                            self.assertEqual(
                                Decimal(fill.price),
                                Decimal(str(replay.entry_price)).quantize(Decimal("0.0001")),
                            )
                            self.assertEqual(
                                fill.raw_payload["condition_code"],
                                case["condition_code"],
                            )
                        finally:
                            session.close()
                finally:
                    engine.dispose()

    def test_cancel_entry_marks_owned_pending_instruction_cancelled(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            _write_schedule_fixture(schedule_path)
            source_payload = _model_routed_payload(
                instruction_id="long-cancel-entry-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
                book_key="rl_shared_long_trial_106_virtual_01",
            )
            result = _translate(
                source_payload,
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="entry_prevclose_-50bp",
            )
            self._submit_translated(result.instruction_payload, schedule_path)
            source_batch = parse_execution_batch_payload(source_payload)

            execution = execute_owned_rl_action(
                self.session_factory,
                self.config,
                source_batch.instructions[0],
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="cancel_entry",
            )

            self.assertEqual(execution.state_before, ExecutionState.ENTRY_PENDING.value)
            self.assertEqual(execution.state_after, ExecutionState.ENTRY_CANCELLED.value)
            session = self.session_factory()
            try:
                instruction = session.execute(select(InstructionRecord)).scalar_one()
                self.assertEqual(instruction.state, ExecutionState.ENTRY_CANCELLED.value)
            finally:
                session.close()

    def test_long_take_profit_exit_submits_sell_limit_above_entry_fill(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            _write_schedule_fixture(schedule_path)
            source_payload = _model_routed_payload(
                instruction_id="long-owned-exit-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
                book_key="rl_shared_long_trial_106_virtual_01",
            )
            result = _translate(
                source_payload,
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="entry_prevclose_-50bp",
            )
            self._submit_translated(result.instruction_payload, schedule_path)
            self._record_quote(symbol="AXFO", price=Decimal("99.50"), minute=5)
            self._run_cycle(schedule_path, 6)
            self._run_cycle(schedule_path, 7)
            source_batch = parse_execution_batch_payload(source_payload)

            execution = execute_owned_rl_action(
                self.session_factory,
                self.config,
                source_batch.instructions[0],
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="exit_tp_200bp",
            )

            self.assertEqual(execution.state_before, ExecutionState.POSITION_OPEN.value)
            self.assertEqual(execution.state_after, ExecutionState.EXIT_PENDING.value)
            self.assertEqual(execution.limit_price, "101.4900")
            session = self.session_factory()
            try:
                orders = session.execute(
                    select(BrokerOrderRecord).order_by(BrokerOrderRecord.id.asc())
                ).scalars().all()
                self.assertEqual(len(orders), 2)
                self.assertEqual(orders[1].order_role, "EXIT")
                self.assertEqual(orders[1].side, "SELL")
                self.assertEqual(orders[1].order_type, "LMT")
                self.assertEqual(orders[1].limit_price, "101.4900")
                instruction = session.execute(select(InstructionRecord)).scalar_one()
                self.assertEqual(instruction.state, ExecutionState.EXIT_PENDING.value)
            finally:
                session.close()

    def test_short_take_profit_exit_submits_buy_limit_below_entry_fill(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            _write_schedule_fixture(schedule_path)
            source_payload = _model_routed_payload(
                instruction_id="short-owned-exit-1",
                model_id="short_trial36_v1",
                symbol="AZA",
                side="SHORT",
                book_key="rl_shared_short_trial_36_virtual_01",
            )
            result = _translate(
                source_payload,
                deployment_key="short_trial_36_virtual_shared_01",
                action_name="entry_prevclose_88bp",
            )
            self._submit_translated(result.instruction_payload, schedule_path)
            self._record_quote(symbol="AZA", price=Decimal("100.88"), minute=5)
            self._run_cycle(schedule_path, 6)
            self._run_cycle(schedule_path, 7)
            source_batch = parse_execution_batch_payload(source_payload)

            execution = execute_owned_rl_action(
                self.session_factory,
                self.config,
                source_batch.instructions[0],
                deployment_key="short_trial_36_virtual_shared_01",
                action_name="exit_tp_180bp",
            )

            self.assertEqual(execution.state_after, ExecutionState.EXIT_PENDING.value)
            self.assertEqual(execution.limit_price, "99.0642")
            session = self.session_factory()
            try:
                orders = session.execute(
                    select(BrokerOrderRecord).order_by(BrokerOrderRecord.id.asc())
                ).scalars().all()
                self.assertEqual(len(orders), 2)
                self.assertEqual(orders[0].side, "SELL")
                self.assertEqual(orders[1].order_role, "EXIT")
                self.assertEqual(orders[1].side, "BUY")
                self.assertEqual(orders[1].order_type, "LMT")
                self.assertEqual(orders[1].limit_price, "99.0642")
            finally:
                session.close()

    def test_clear_exit_cancels_owned_exit_and_keeps_position_open(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            _write_schedule_fixture(schedule_path)
            source_payload = _model_routed_payload(
                instruction_id="long-clear-exit-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
                book_key="rl_shared_long_trial_106_virtual_01",
            )
            result = _translate(
                source_payload,
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="entry_prevclose_-50bp",
            )
            self._submit_translated(result.instruction_payload, schedule_path)
            self._record_quote(symbol="AXFO", price=Decimal("99.50"), minute=5)
            self._run_cycle(schedule_path, 6)
            self._run_cycle(schedule_path, 7)
            source_batch = parse_execution_batch_payload(source_payload)
            execute_owned_rl_action(
                self.session_factory,
                self.config,
                source_batch.instructions[0],
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="exit_tp_200bp",
            )

            execution = execute_owned_rl_action(
                self.session_factory,
                self.config,
                source_batch.instructions[0],
                deployment_key="long_trial_106_virtual_shared_01",
                action_name="clear_exit",
            )

            self.assertEqual(execution.state_before, ExecutionState.EXIT_PENDING.value)
            self.assertEqual(execution.state_after, ExecutionState.POSITION_OPEN.value)
            session = self.session_factory()
            try:
                orders = session.execute(
                    select(BrokerOrderRecord).order_by(BrokerOrderRecord.id.asc())
                ).scalars().all()
                self.assertEqual(orders[1].status, "Cancelled")
                instruction = session.execute(select(InstructionRecord)).scalar_one()
                self.assertEqual(instruction.state, ExecutionState.POSITION_OPEN.value)
            finally:
                session.close()
