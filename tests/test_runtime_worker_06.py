from __future__ import annotations

from tests._runtime_worker_shared import *  # noqa: F401,F403


class RuntimeWorkerTests06(RuntimeWorkerTestCase):
    def test_run_runtime_cycle_submits_forced_exit_when_next_session_is_due(self) -> None:
        payload = _sive_payload()
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.POSITION_OPEN.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            entry_filled_quantity="100",
        )

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: object,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: object = None,
            stop_price: object = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            self.assertEqual(order_ref, "runtime-sive-1:exit:forced")
            self.assertEqual(str(quantity), "100")
            self.assertIsNone(limit_price)
            self.assertIsNone(stop_price)
            return {
                "instruction_id": "runtime-sive-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 489000, "symbol": "SIVE"},
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": "MKT",
                    "time_in_force": "DAY",
                    "limit_price": None,
                    "total_quantity": "100",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 31,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": "100",
                    "avgFillPrice": 0.0,
                    "permId": 9101,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                },
            }

        snapshot_kwargs: dict[str, object] = {}

        def fake_snapshot_fetcher(*args: object, **kwargs: object) -> BrokerRuntimeSnapshot:
            del args
            snapshot_kwargs.update(kwargs)
            return BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(_sive_broker_position(),),
                account_values={},
            )

        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            schedule_path.with_suffix(".csv").write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
                        "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
                    ]
                ),
                encoding="utf-8",
            )

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
                exit_submitter=fake_exit_submitter,
                submission_lead_time=timedelta(minutes=1),
                broker_snapshot_fetcher=fake_snapshot_fetcher,
            )

        self.assertEqual(len(result.submitted_exits), 1)
        self.assertTrue(snapshot_kwargs["include_open_orders"])
        self.assertFalse(snapshot_kwargs["include_executions"])
        self.assertTrue(snapshot_kwargs["include_positions"])
        record = self._read_record("runtime-sive-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.exit_order_id, 31)
        self.assertEqual(record.exit_submitted_quantity, "100")

    def test_run_runtime_cycle_blocks_forced_exit_without_broker_position(self) -> None:
        payload = _sive_payload()
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.POSITION_OPEN.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            entry_filled_quantity="100",
        )

        def fake_exit_submitter(*args: object, **kwargs: object) -> dict[str, object]:
            raise AssertionError("forced exit must wait for a matching broker position")

        def fake_canceler(*args: object, **kwargs: object) -> dict[str, object]:
            raise AssertionError("open exits must not be cancelled before position check")

        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            schedule_path.with_suffix(".csv").write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
                        "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
                    ]
                ),
                encoding="utf-8",
            )

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
                exit_submitter=fake_exit_submitter,
                broker_order_canceler=fake_canceler,
                submission_lead_time=timedelta(minutes=1),
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={
                        21: BrokerOpenOrder(
                            order_id=21,
                            perm_id=9001,
                            client_id=0,
                            status="Submitted",
                            order_ref="runtime-sive-1:exit:take_profit",
                            action="SELL",
                            total_quantity="100",
                            symbol="SIVE",
                            account="DU1234567",
                            security_type="STK",
                            exchange="SMART",
                            primary_exchange="SFB",
                            currency="SEK",
                            local_symbol="SIVE",
                            order_type="LMT",
                        ),
                    },
                    executions=(),
                    portfolio=(),
                    positions=(),
                    account_values={},
                ),
            )

        self.assertEqual(result.submitted_exits, ())
        self.assertEqual(len(result.issues), 1)
        self.assertEqual(result.issues[0].stage, "forced_exit_position_check")
        self.assertIn("observed none", result.issues[0].message)
        session = self.session_factory()
        try:
            event = session.execute(
                select(InstructionEventRecord).where(
                    InstructionEventRecord.event_type
                    == "forced_exit_blocked_broker_position_mismatch"
                )
            ).scalar_one()
            self.assertEqual(event.payload["reason"], "missing_broker_position")
            self.assertEqual(event.payload["required_quantity"], "100")
            self.assertIsNone(event.payload["observed_quantity"])
        finally:
            session.close()

    def test_run_runtime_cycle_prioritizes_due_forced_exit_over_protective_repair(self) -> None:
        payload = _sive_payload()
        payload["instruction"]["exit"]["take_profit_pct"] = "0.02"
        payload["instruction"]["exit"]["catastrophic_stop_loss_pct"] = "0.15"
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.POSITION_OPEN.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            entry_filled_quantity="100",
        )

        calls: list[str] = []

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: object,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: object = None,
            stop_price: object = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            del broker_config, instruction, timeout, limit_price, stop_price
            del oca_group, oca_type
            calls.append(order_ref)
            return {
                "instruction_id": "runtime-sive-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 489000, "symbol": "SIVE"},
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": "MKT",
                    "time_in_force": "DAY",
                    "limit_price": None,
                    "total_quantity": str(quantity),
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 31,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": str(quantity),
                    "avgFillPrice": 0.0,
                    "permId": 9101,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                },
            }

        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            schedule_path.with_suffix(".csv").write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
                        "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
                    ]
                ),
                encoding="utf-8",
            )

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
                exit_submitter=fake_exit_submitter,
                submission_lead_time=timedelta(minutes=1),
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={},
                    executions=(),
                    portfolio=(),
                    positions=(_sive_broker_position(),),
                    account_values={},
                ),
            )

        self.assertEqual(calls, ["runtime-sive-1:exit:forced"])
        self.assertEqual(len(result.submitted_exits), 1)

    def test_run_runtime_cycle_skips_forced_exit_when_matching_live_exit_order_exists(self) -> None:
        payload = _sive_payload()
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            entry_filled_quantity="100",
        )

        def fake_exit_submitter(*args: object, **kwargs: object) -> dict[str, object]:
            raise AssertionError("matching live exit order should suppress duplicate forced exit")

        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            schedule_path.with_suffix(".csv").write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
                        "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
                    ]
                ),
                encoding="utf-8",
            )

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
                exit_submitter=fake_exit_submitter,
                submission_lead_time=timedelta(minutes=1),
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={
                        77: BrokerOpenOrder(
                            order_id=77,
                            perm_id=9077,
                            client_id=13,
                            status="PreSubmitted",
                            order_ref="manual-sive-close-open",
                            action="SELL",
                            total_quantity=Decimal("100"),
                            symbol="SIVE",
                            account="DU1234567",
                            security_type="STK",
                            exchange="SMART",
                            primary_exchange="SFB",
                            currency="SEK",
                            local_symbol="SIVE",
                            order_type="MKT",
                        ),
                    },
                    executions=(),
                    portfolio=(),
                    positions=(_sive_broker_position(),),
                    account_values={},
                ),
            )

        self.assertEqual(result.submitted_exits, ())
        self.assertEqual(result.issues, ())

    def test_run_runtime_cycle_suppresses_recent_terminal_forced_exit_retry(self) -> None:
        payload = _sive_payload()
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            entry_filled_quantity="100",
            exit_order_id=31,
        )

        session = self.session_factory()
        try:
            instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-sive-1"
                )
            ).scalar_one()
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="DU1234567",
                base_currency="SEK",
                metadata_json={},
            )
            session.add(broker_account)
            session.flush()
            session.add(
                BrokerOrderRecord(
                    instruction_id=instruction.id,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="DU1234567",
                    order_role="EXIT",
                    external_order_id="31",
                    external_perm_id="9031",
                    external_client_id="0",
                    order_ref="runtime-sive-1:exit:forced",
                    symbol="SIVE",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    primary_exchange="SFB",
                    local_symbol="SIVE",
                    side="SELL",
                    order_type="MKT",
                    time_in_force="DAY",
                    status="Inactive",
                    total_quantity="100",
                    submitted_at=datetime(2026, 4, 13, 6, 58, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 13, 6, 58, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
            )
            session.commit()
        finally:
            session.close()

        def fake_exit_submitter(*args: object, **kwargs: object) -> dict[str, object]:
            raise AssertionError("recent terminal forced exit must suppress retries")

        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            schedule_path.with_suffix(".csv").write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
                        "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
                    ]
                ),
                encoding="utf-8",
            )

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
                exit_submitter=fake_exit_submitter,
                submission_lead_time=timedelta(minutes=1),
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={},
                    executions=(),
                    portfolio=(),
                    positions=(_sive_broker_position(),),
                    account_values={},
                ),
            )

        self.assertEqual(result.submitted_exits, ())
        self.assertEqual(
            [(issue.instruction_id, issue.stage) for issue in result.issues],
            [("runtime-sive-1", "forced_exit_retry")],
        )
        session = self.session_factory()
        try:
            event_type = session.execute(
                select(InstructionEventRecord.event_type)
                .join(InstructionRecord)
                .where(
                    InstructionRecord.instruction_id == "runtime-sive-1",
                    InstructionEventRecord.event_type
                    == "forced_exit_retry_blocked_terminal_failure",
                )
            ).scalar_one()
        finally:
            session.close()
        self.assertEqual(event_type, "forced_exit_retry_blocked_terminal_failure")

    def test_run_runtime_cycle_blocks_due_entries_while_next_session_exit_is_active(self) -> None:
        exit_payload = _sive_payload()
        entry_payload = _aapl_payload()
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.POSITION_OPEN.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=exit_payload,
            entry_filled_quantity="100",
        )
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 13, 13, 0, tzinfo=timezone.utc),
            payload=entry_payload,
        )

        entry_submit_calls: list[str] = []

        def fake_entry_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            entry_submit_calls.append(instruction.instruction_id)
            raise AssertionError("due entries should be blocked while urgent exits remain active")

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: object,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: object = None,
            stop_price: object = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            self.assertEqual(order_ref, "runtime-sive-1:exit:forced")
            return {
                "instruction_id": "runtime-sive-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 489000, "symbol": "SIVE"},
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": "MKT",
                    "time_in_force": "DAY",
                    "limit_price": None,
                    "total_quantity": "100",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 31,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": "100",
                    "avgFillPrice": 0.0,
                    "permId": 9101,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                },
            }

        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            schedule_path.with_suffix(".csv").write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
                        "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
                    ]
                ),
                encoding="utf-8",
            )

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
                entry_submitter=fake_entry_submitter,
                exit_submitter=fake_exit_submitter,
                submission_lead_time=timedelta(minutes=1),
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={},
                    executions=(),
                    portfolio=(),
                    positions=(_sive_broker_position(),),
                    account_values={},
                ),
            )

        self.assertEqual(entry_submit_calls, [])
        self.assertEqual(len(result.submitted_exits), 1)
        self.assertEqual(result.issues, ())
        self.assertEqual(
            self._read_record("runtime-aapl-1").state,
            ExecutionState.ENTRY_PENDING.value,
        )

    def test_run_runtime_cycle_blocks_due_entries_only_for_same_account(self) -> None:
        exit_payload = _sive_payload()
        entry_payload = _aapl_payload()
        entry_payload["instruction"]["account"]["account_key"] = "OTHER_ACCOUNT"
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.POSITION_OPEN.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=exit_payload,
            entry_filled_quantity="100",
            account_key="GTW05",
        )
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 13, 13, 0, tzinfo=timezone.utc),
            payload=entry_payload,
            account_key="OTHER_ACCOUNT",
        )

        entry_submit_calls: list[str] = []

        def fake_entry_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            del broker_config, timeout
            entry_submit_calls.append(instruction.instruction_id)
            return {
                "instruction_id": "runtime-aapl-1",
                "account": "OTHER_ACCOUNT",
                "warnings": [],
                "resolved_contract": {"con_id": 265598, "symbol": "AAPL"},
                "order": {
                    "order_ref": "runtime-aapl-1",
                    "action": "BUY",
                    "order_type": "LMT",
                    "time_in_force": "DAY",
                    "limit_price": "200.00",
                    "total_quantity": "1",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 42,
                    "status": "PreSubmitted",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 9042,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                },
            }

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: object,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: object = None,
            stop_price: object = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            del broker_config, instruction, quantity, order_type, timeout
            del limit_price, stop_price, oca_group, oca_type
            self.assertEqual(order_ref, "runtime-sive-1:exit:forced")
            return {
                "instruction_id": "runtime-sive-1",
                "account": "GTW05",
                "warnings": [],
                "resolved_contract": {"con_id": 489000, "symbol": "SIVE"},
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": "MKT",
                    "time_in_force": "DAY",
                    "limit_price": None,
                    "total_quantity": "100",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 31,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": "100",
                    "avgFillPrice": 0.0,
                    "permId": 9101,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                },
            }

        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.parquet"
            schedule_path.with_suffix(".csv").write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-10,Europe/Stockholm,09:00,17:30,regular,base,override",
                        "2026-04-13,Europe/Stockholm,09:00,17:30,regular,base,override",
                    ]
                ),
                encoding="utf-8",
            )

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
                entry_submitter=fake_entry_submitter,
                exit_submitter=fake_exit_submitter,
                submission_lead_time=timedelta(minutes=1),
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={},
                    executions=(),
                    portfolio=(),
                    positions=(_sive_broker_position(),),
                    account_values={},
                ),
            )

        self.assertEqual(entry_submit_calls, ["runtime-aapl-1"])
        self.assertEqual(len(result.submitted_exits), 1)
        self.assertEqual(len(result.submitted_entries), 1)
        self.assertEqual(
            self._read_record("runtime-sive-1").state,
            ExecutionState.EXIT_PENDING.value,
        )
        self.assertEqual(
            self._read_record("runtime-aapl-1").state,
            ExecutionState.ENTRY_SUBMITTED.value,
        )
