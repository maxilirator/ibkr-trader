from __future__ import annotations

from tests._runtime_worker_shared import *  # noqa: F401,F403


class RuntimeWorkerTests07(RuntimeWorkerTestCase):
    def test_run_runtime_cycle_cancels_all_open_exit_orders_before_forced_exit(self) -> None:
        payload = _sive_payload()
        payload["instruction"]["exit"]["take_profit_pct"] = "0.02"
        payload["instruction"]["exit"]["catastrophic_stop_loss_pct"] = "0.15"
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
            exit_order_id=21,
        )

        cancelled_ids: list[int] = []

        def fake_canceler(
            broker_config: IbkrConnectionConfig,
            order_id: int,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            cancelled_ids.append(order_id)
            return {"broker_order_status": {"orderId": order_id, "status": "Cancelled"}}

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
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            schedule_path.write_text(
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
                        22: BrokerOpenOrder(
                            order_id=22,
                            perm_id=9002,
                            client_id=0,
                            status="Submitted",
                            order_ref="runtime-sive-1:exit:catastrophic_stop",
                            action="SELL",
                            total_quantity="100",
                            symbol="SIVE",
                            account="DU1234567",
                            security_type="STK",
                            exchange="SMART",
                            primary_exchange="SFB",
                            currency="SEK",
                            local_symbol="SIVE",
                            order_type="STP",
                        ),
                    },
                    executions=(),
                    portfolio=(),
                    positions=(_sive_broker_position(),),
                    account_values={},
                ),
            )

        self.assertEqual(cancelled_ids, [21, 22])
        self.assertEqual(len(result.submitted_exits), 1)

    def test_run_runtime_cycle_cleans_same_symbol_exit_orders_before_forced_exit(self) -> None:
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

        session = self.session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="DU1234567",
                base_currency="SEK",
            )
            session.add(broker_account)
            session.flush()
            session.add(
                BrokerOrderRecord(
                    instruction_id=None,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="DU1234567",
                    order_role="EXIT",
                    external_order_id="41",
                    external_perm_id="9141",
                    external_client_id="0",
                    order_ref="old-sive:exit:take_profit",
                    symbol="SIVE",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    primary_exchange="SFB",
                    local_symbol="SIVE",
                    side="SELL",
                    order_type="LMT",
                    time_in_force="DAY",
                    status="Submitted",
                    total_quantity="100",
                    limit_price="21.00",
                    stop_price=None,
                    submitted_at=datetime(2026, 4, 10, 8, 30, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 10, 8, 30, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
            )
            session.commit()
        finally:
            session.close()

        cancelled_ids: list[int] = []

        def fake_canceler(
            broker_config: IbkrConnectionConfig,
            order_id: int,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            cancelled_ids.append(order_id)
            return {"broker_order_status": {"orderId": order_id, "status": "Cancelled"}}

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
                    "orderId": 51,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": "100",
                    "avgFillPrice": 0.0,
                    "permId": 9151,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                },
            }

        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            schedule_path.write_text(
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
                        41: BrokerOpenOrder(
                            order_id=41,
                            perm_id=9141,
                            client_id=0,
                            status="Submitted",
                            order_ref=None,
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
                        42: BrokerOpenOrder(
                            order_id=42,
                            perm_id=9142,
                            client_id=0,
                            status="Submitted",
                            order_ref="old-sive:exit:catastrophic_stop",
                            action="SELL",
                            total_quantity="100",
                            symbol="SIVE",
                            account="DU1234567",
                            security_type="STK",
                            exchange="SMART",
                            primary_exchange="SFB",
                            currency="SEK",
                            local_symbol="SIVE",
                            order_type="STP",
                        ),
                    },
                    executions=(),
                    portfolio=(),
                    positions=(_sive_broker_position(),),
                    account_values={},
                ),
            )

        self.assertEqual(cancelled_ids, [41, 42])
        self.assertEqual(len(result.submitted_exits), 1)
        session = self.session_factory()
        try:
            cleanup_event = session.execute(
                select(InstructionEventRecord).where(
                    InstructionEventRecord.event_type
                    == "forced_exit_conflicting_orders_cleanup_started"
                )
            ).scalar_one()
            self.assertEqual(
                [
                    order["broker_order_id"]
                    for order in cleanup_event.payload["conflicting_orders"]
                ],
                [41, 42],
            )
        finally:
            session.close()

    def test_run_runtime_cycle_blocks_forced_exit_when_conflict_cancel_fails(self) -> None:
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

        submitted_refs: list[str] = []

        def fake_canceler(
            broker_config: IbkrConnectionConfig,
            order_id: int,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            raise RuntimeError(f"cancel failed for {order_id}")

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
            submitted_refs.append(order_ref)
            return {}

        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            schedule_path.write_text(
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
                        42: BrokerOpenOrder(
                            order_id=42,
                            perm_id=9142,
                            client_id=0,
                            status="Submitted",
                            order_ref="old-sive:exit:catastrophic_stop",
                            action="SELL",
                            total_quantity="100",
                            symbol="SIVE",
                            account="DU1234567",
                            security_type="STK",
                            exchange="SMART",
                            primary_exchange="SFB",
                            currency="SEK",
                            local_symbol="SIVE",
                            order_type="STP",
                        ),
                    },
                    executions=(),
                    portfolio=(),
                    positions=(_sive_broker_position(),),
                    account_values={},
                ),
            )

        self.assertEqual(submitted_refs, [])
        self.assertEqual(len(result.submitted_exits), 0)
        self.assertEqual(len(result.issues), 1)
        self.assertIn("cancel failed for 42", result.issues[0].message)

    def test_run_runtime_cycle_blocks_forced_exit_when_conflict_cancel_is_unconfirmed(self) -> None:
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

        submitted_refs: list[str] = []

        def fake_canceler(
            broker_config: IbkrConnectionConfig,
            order_id: int,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            return {"broker_order_status": {"orderId": order_id, "status": "PendingCancel"}}

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
            submitted_refs.append(order_ref)
            return {}

        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            schedule_path.write_text(
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
                        42: BrokerOpenOrder(
                            order_id=42,
                            perm_id=9142,
                            client_id=0,
                            status="Submitted",
                            order_ref="old-sive:exit:catastrophic_stop",
                            action="SELL",
                            total_quantity="100",
                            symbol="SIVE",
                            account="DU1234567",
                            security_type="STK",
                            exchange="SMART",
                            primary_exchange="SFB",
                            currency="SEK",
                            local_symbol="SIVE",
                            order_type="STP",
                        ),
                    },
                    executions=(),
                    portfolio=(),
                    positions=(_sive_broker_position(),),
                    account_values={},
                ),
            )

        self.assertEqual(submitted_refs, [])
        self.assertEqual(len(result.submitted_exits), 0)
        self.assertEqual(len(result.issues), 1)
        self.assertIn("broker status was PENDINGCANCEL", result.issues[0].message)

    def test_run_runtime_cycle_keeps_existing_forced_exit_order(self) -> None:
        payload = _sive_payload()
        payload["instruction"]["exit"]["take_profit_pct"] = "0.02"
        payload["instruction"]["exit"]["catastrophic_stop_loss_pct"] = "0.15"
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
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="DU1234567",
                base_currency="SEK",
            )
            session.add(broker_account)
            session.flush()
            instruction_record = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-sive-1"
                )
            ).scalar_one()
            session.add(
                BrokerOrderRecord(
                    instruction_id=instruction_record.id,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="DU1234567",
                    order_role="EXIT",
                    external_order_id="31",
                    external_perm_id="9101",
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
                    status="PreSubmitted",
                    total_quantity="100",
                    limit_price=None,
                    stop_price=None,
                    submitted_at=datetime(2026, 4, 13, 6, 58, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 13, 6, 58, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
            )
            session.commit()
        finally:
            session.close()

        cancelled_ids: list[int] = []
        submitted_refs: list[str] = []

        def fake_canceler(
            broker_config: IbkrConnectionConfig,
            order_id: int,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            cancelled_ids.append(order_id)
            return {"broker_order_status": {"orderId": order_id, "status": "Cancelled"}}

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
            submitted_refs.append(order_ref)
            return {}

        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            schedule_path.write_text(
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
                        31: BrokerOpenOrder(
                            order_id=31,
                            perm_id=9101,
                            client_id=0,
                            status="PreSubmitted",
                            order_ref="runtime-sive-1:exit:forced",
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
                    positions=(),
                    account_values={},
                ),
            )

        self.assertEqual(cancelled_ids, [])
        self.assertEqual(submitted_refs, [])
        self.assertEqual(len(result.submitted_exits), 0)

    def test_run_runtime_cycle_completes_instruction_after_exit_fill(self) -> None:
        payload = _aapl_payload()
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
            entry_filled_quantity="1",
            exit_order_id=21,
        )

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            now=datetime(2026, 4, 10, 20, 10, tzinfo=timezone.utc),
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(
                    BrokerExecution(
                        exec_id="E-2",
                        order_id=21,
                        perm_id=9001,
                        client_id=0,
                        order_ref="runtime-aapl-1:exit:take_profit",
                        side="SLD",
                        shares="1",
                        price="204.00",
                        exchange="NASDAQ",
                        executed_at=datetime(2026, 4, 10, 20, 9, tzinfo=timezone.utc),
                        symbol="AAPL",
                        account="DU1234567",
                        security_type="STK",
                        primary_exchange="NASDAQ",
                        currency="USD",
                        local_symbol="AAPL",
                    ),
                ),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.completed_instructions), 1)
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.COMPLETED.value)
        self.assertEqual(record.exit_filled_quantity, "1")
        self.assertEqual(record.exit_avg_fill_price, "204.00")

    def test_run_runtime_cycle_marks_expired_unfilled_entry_cancelled(self) -> None:
        payload = _aapl_payload()
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_SUBMITTED.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
            broker_order_id=11,
        )
        session = self.session_factory()
        try:
            instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-aapl-1"
                )
            ).scalar_one()
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="DU1234567",
                base_currency="USD",
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
                    order_role="ENTRY",
                    external_order_id="11",
                    external_perm_id="8001",
                    external_client_id="0",
                    order_ref="runtime-aapl-1",
                    symbol="AAPL",
                    exchange="SMART",
                    currency="USD",
                    security_type="STK",
                    primary_exchange="NASDAQ",
                    local_symbol="AAPL",
                    side="BUY",
                    order_type="LMT",
                    time_in_force="DAY",
                    status="Submitted",
                    total_quantity="1",
                    limit_price="200.00",
                    submitted_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
            )
            session.commit()
        finally:
            session.close()

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            now=datetime(2026, 4, 10, 20, 10, tzinfo=timezone.utc),
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.cancelled_entries), 1)
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.ENTRY_CANCELLED.value)
        self.assertEqual(record.broker_order_status, "NOT_FOUND_AT_BROKER")
