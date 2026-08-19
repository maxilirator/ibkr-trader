from __future__ import annotations

from tests._runtime_worker_shared import *  # noqa: F401,F403


class RuntimeWorkerTests02(RuntimeWorkerTestCase):
    def test_run_runtime_cycle_submits_stockholm_open_entry_one_minute_early(self) -> None:
        payload = _sive_payload()
        payload["instruction"]["entry"]["submit_at"] = "2026-04-10T09:00:00+02:00"
        payload["instruction"]["entry"]["expire_at"] = "2026-04-10T10:00:00+02:00"
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 7, 0, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 8, 0, tzinfo=timezone.utc),
            payload=payload,
        )

        submit_calls: list[str] = []

        def fake_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            submit_calls.append(instruction.instruction_id)
            return {
                "instruction_id": "runtime-sive-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 489000, "symbol": "SIVE"},
                "order": {
                    "order_ref": "runtime-sive-1",
                    "action": "BUY",
                    "order_type": "LMT",
                    "time_in_force": "DAY",
                    "limit_price": "11.3131",
                    "total_quantity": "100",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 11,
                    "status": "PreSubmitted",
                    "filled": "0",
                    "remaining": "100",
                    "avgFillPrice": 0.0,
                    "permId": 8001,
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
                    ]
                ),
                encoding="utf-8",
            )

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 10, 6, 59, tzinfo=timezone.utc),
                entry_submitter=fake_submitter,
                submission_lead_time=timedelta(minutes=1),
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={},
                    executions=(),
                    portfolio=(),
                    positions=(_sive_broker_position(),),
                    account_values={},
                ),
            )

        self.assertEqual(submit_calls, ["runtime-sive-1"])
        self.assertEqual(len(result.submitted_entries), 1)

    def test_run_runtime_cycle_submits_stockholm_close_entry_one_minute_early(self) -> None:
        payload = _sive_payload()
        payload["instruction"]["entry"]["submit_at"] = "2026-04-10T17:30:00+02:00"
        payload["instruction"]["entry"]["expire_at"] = "2026-04-10T17:31:00+02:00"
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 31, tzinfo=timezone.utc),
            payload=payload,
        )

        submit_calls: list[str] = []

        def fake_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            submit_calls.append(instruction.instruction_id)
            return {
                "instruction_id": "runtime-sive-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 489000, "symbol": "SIVE"},
                "order": {
                    "order_ref": "runtime-sive-1",
                    "action": "BUY",
                    "order_type": "LMT",
                    "time_in_force": "DAY",
                    "limit_price": "11.3131",
                    "total_quantity": "100",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 11,
                    "status": "PreSubmitted",
                    "filled": "0",
                    "remaining": "100",
                    "avgFillPrice": 0.0,
                    "permId": 8001,
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
                    ]
                ),
                encoding="utf-8",
            )

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 10, 15, 29, tzinfo=timezone.utc),
                entry_submitter=fake_submitter,
                submission_lead_time=timedelta(minutes=1),
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={},
                    executions=(),
                    portfolio=(),
                    positions=(_sive_broker_position(),),
                    account_values={},
                ),
            )

        self.assertEqual(submit_calls, ["runtime-sive-1"])
        self.assertEqual(len(result.submitted_entries), 1)

    def test_run_runtime_cycle_skips_due_entry_when_kill_switch_is_enabled(self) -> None:
        payload = _aapl_payload()
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
        )
        set_kill_switch_state(
            self.session_factory,
            enabled=True,
            reason="Freeze new entries.",
            updated_by="test",
        )

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            entry_submitter=lambda *args, **kwargs: self.fail(
                "entry submitter should not be called while kill switch is enabled"
            ),
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(result.submitted_entries, ())
        self.assertEqual(len(result.issues), 1)
        self.assertEqual(result.issues[0].stage, "kill_switch")
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.ENTRY_PENDING.value)

    def test_run_runtime_cycle_cancels_open_entry_when_kill_switch_is_enabled(self) -> None:
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
        set_kill_switch_state(
            self.session_factory,
            enabled=True,
            reason="Freeze new entries.",
            updated_by="test",
        )

        def fake_canceler(
            broker_config: IbkrConnectionConfig,
            order_id: int,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            self.assertEqual(order_id, 11)
            return {
                "broker_order_status": {
                    "orderId": 11,
                    "status": "Cancelled",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 8001,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                }
            }

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            entry_canceler=fake_canceler,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={
                    11: BrokerOpenOrder(
                        order_id=11,
                        perm_id=8001,
                        client_id=0,
                        status="Submitted",
                        order_ref="runtime-aapl-1",
                        action="BUY",
                        total_quantity=Decimal("1"),
                        symbol="AAPL",
                        account="DU1234567",
                        security_type="STK",
                        exchange="SMART",
                        primary_exchange="NASDAQ",
                        currency="USD",
                        local_symbol="AAPL",
                        order_type="LMT",
                        limit_price=Decimal("200.00"),
                        aux_price=None,
                        outside_rth=False,
                        oca_group=None,
                        oca_type=None,
                        transmit=True,
                        warning_text=None,
                        reject_reason=None,
                        completed_status=None,
                        completed_time=None,
                    )
                },
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.cancelled_entries), 1)
        self.assertEqual(
            result.cancelled_entries[0].action,
            "entry_cancelled_by_kill_switch",
        )
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.ENTRY_CANCELLED.value)
        self.assertEqual(record.broker_order_status, "Cancelled")

    def test_run_runtime_cycle_persists_runtime_snapshot_to_ledger(self) -> None:
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

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={
                    11: BrokerOpenOrder(
                        order_id=11,
                        perm_id=8001,
                        client_id=0,
                        status="Submitted",
                        order_ref="runtime-aapl-1",
                        action="BUY",
                        total_quantity=Decimal("1"),
                        symbol="AAPL",
                        account="DU1234567",
                        security_type="STK",
                        exchange="SMART",
                        primary_exchange="NASDAQ",
                        currency="USD",
                        local_symbol="AAPL",
                        order_type="LMT",
                        limit_price=Decimal("200.00"),
                        aux_price=None,
                        outside_rth=False,
                        oca_group=None,
                        oca_type=None,
                        transmit=True,
                        warning_text=None,
                        reject_reason=None,
                        completed_status=None,
                        completed_time=None,
                    )
                },
                executions=(),
                portfolio=(
                    BrokerPortfolioItem(
                        account="DU1234567",
                        symbol="AAPL",
                        local_symbol="AAPL",
                        security_type="STK",
                        exchange="SMART",
                        primary_exchange="NASDAQ",
                        currency="USD",
                        position=Decimal("1"),
                        market_price=Decimal("201.00"),
                        market_value=Decimal("201.00"),
                        average_cost=Decimal("200.00"),
                        unrealized_pnl=Decimal("1.00"),
                        realized_pnl=Decimal("0"),
                    ),
                ),
                positions=(
                    BrokerPosition(
                        account="DU1234567",
                        symbol="AAPL",
                        local_symbol="AAPL",
                        security_type="STK",
                        exchange="SMART",
                        primary_exchange="NASDAQ",
                        currency="USD",
                        position=Decimal("1"),
                        average_cost=Decimal("200.00"),
                    ),
                ),
                account_values={
                    "DU1234567": {
                        "NetLiquidation": {"value": "100000.00", "currency": "USD"},
                        "BuyingPower": {"value": "200000.00", "currency": "USD"},
                    }
                },
            ),
        )

        self.assertEqual(result.issues, ())
        session = self.session_factory()
        try:
            self.assertEqual(
                len(session.execute(select(AccountSnapshotRecord)).scalars().all()),
                1,
            )
            self.assertEqual(
                len(session.execute(select(PositionSnapshotRecord)).scalars().all()),
                1,
            )
            broker_order = session.execute(select(BrokerOrderRecord)).scalar_one()
            self.assertEqual(broker_order.external_order_id, "11")
            self.assertEqual(broker_order.status, "Submitted")
        finally:
            session.close()

    def test_run_runtime_cycle_persists_callback_events_before_reconciliation(self) -> None:
        self._insert_broker_order(external_order_id="11", status="PreSubmitted")

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            broker_callback_fetcher=lambda: [
                {
                    "event_type": "order_status",
                    "event_at": datetime(2026, 4, 10, 19, 55, 30, tzinfo=timezone.utc),
                    "order_status": {
                        "orderId": 11,
                        "status": "Submitted",
                        "filled": "0",
                        "remaining": "1",
                        "avgFillPrice": "0.0",
                        "permId": 8001,
                        "parentId": 0,
                        "lastFillPrice": "0.0",
                        "clientId": 0,
                        "whyHeld": "",
                        "mktCapPrice": "0.0",
                    },
                }
            ],
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(result.issues, ())
        session = self.session_factory()
        try:
            broker_order = session.execute(select(BrokerOrderRecord)).scalar_one()
            self.assertEqual(broker_order.status, "Submitted")
        finally:
            session.close()

    def test_run_runtime_cycle_persists_reconciliation_run_summary(self) -> None:
        payload = _aapl_payload()
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
        )

        def fake_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            return {
                "instruction_id": "runtime-aapl-1",
                "account": "DU1234567",
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
                    "orderId": 11,
                    "status": "PreSubmitted",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 8001,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                },
            }

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            entry_submitter=fake_submitter,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.submitted_entries), 1)
        runs = self._read_reconciliation_runs()
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0].run_kind, "runtime_cycle")
        self.assertEqual(runs[0].status, "CLEAN")
        self.assertEqual(runs[0].issue_count, 0)
        self.assertEqual(runs[0].action_count, 1)
        self.assertEqual(runs[0].metadata_json["due_instruction_count"], 1)
        self.assertEqual(runs[0].metadata_json["active_instruction_count"], 0)

    def test_run_runtime_cycle_persists_reconciliation_issues_on_early_return(self) -> None:
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
        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            broker_snapshot_fetcher=lambda *args, **kwargs: (_ for _ in ()).throw(
                RuntimeError("snapshot down")
            ),
        )

        self.assertEqual(len(result.issues), 1)
        self.assertEqual(result.issues[0].stage, "broker_snapshot")

        session = self.session_factory()
        try:
            run = session.execute(select(ReconciliationRunRecord)).scalar_one()
            issue = session.execute(select(ReconciliationIssueRecord)).scalar_one()
            self.assertEqual(run.status, "WARNINGS")
            self.assertEqual(run.issue_count, 1)
            self.assertEqual(issue.stage, "broker_snapshot")
            self.assertEqual(issue.message, "snapshot down")
        finally:
            session.close()

    def test_run_runtime_cycle_skips_routine_broker_polling_after_session_close(self) -> None:
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
            broker_order_id=11,
            entry_filled_quantity="80",
            entry_avg_fill_price="146.90",
        )

        with TemporaryDirectory() as tmpdir:
            calendar_path = Path(tmpdir) / "sessions.csv"
            calendar_path.write_text(
                "\n".join(
                    (
                        "session_date,timezone,open_time,close_time,session_kind",
                        "2026-04-10,Europe/Stockholm,09:00:00,17:30:00,regular",
                    )
                ),
                encoding="utf-8",
            )

            def fail_if_called(*args: object, **kwargs: object) -> BrokerRuntimeSnapshot:
                raise AssertionError("broker snapshot should not be fetched after close")

            def callbacks_fail_if_called() -> list[dict[str, object]]:
                raise AssertionError("broker callbacks should not be drained after close")

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=calendar_path,
                now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
                broker_snapshot_fetcher=fail_if_called,
                broker_callback_fetcher=callbacks_fail_if_called,
                virtual_market_sync=lambda at: (_ for _ in ()).throw(
                    AssertionError("virtual market sync should not run after close")
                ),
                exit_submitter=lambda *args, **kwargs: (_ for _ in ()).throw(
                    AssertionError("active exits should not be reconciled after close")
                ),
            )

        self.assertEqual(result.issues, ())
        self.assertEqual(self._read_reconciliation_runs()[0].status, "CLEAN")

    def test_run_runtime_cycle_cancels_expired_submitted_entry_after_session_close(self) -> None:
        payload = _sive_payload()
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.ENTRY_SUBMITTED.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            broker_order_id=11,
        )

        with TemporaryDirectory() as tmpdir:
            calendar_path = Path(tmpdir) / "sessions.csv"
            calendar_path.write_text(
                "\n".join(
                    (
                        "session_date,timezone,open_time,close_time,session_kind",
                        "2026-04-10,Europe/Stockholm,09:00:00,17:30:00,regular",
                    )
                ),
                encoding="utf-8",
            )
            cancelled_order_ids: list[int] = []

            def fake_canceler(
                broker_config: IbkrConnectionConfig,
                order_id: int,
                *,
                timeout: int = 10,
            ) -> dict[str, object]:
                del broker_config, timeout
                cancelled_order_ids.append(order_id)
                return {
                    "broker_order_status": {
                        "orderId": order_id,
                        "status": "Cancelled",
                        "filled": "0",
                        "remaining": "100",
                        "avgFillPrice": 0.0,
                        "permId": 8001,
                        "parentId": 0,
                        "lastFillPrice": 0.0,
                        "clientId": 0,
                        "whyHeld": "",
                        "mktCapPrice": 0.0,
                    }
                }

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=calendar_path,
                now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
                entry_canceler=fake_canceler,
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={
                        11: BrokerOpenOrder(
                            order_id=11,
                            perm_id=8001,
                            client_id=0,
                            status="Submitted",
                            order_ref="runtime-sive-1",
                            action="BUY",
                            total_quantity=Decimal("100"),
                            symbol="SIVE",
                            account="DU1234567",
                            security_type="STK",
                            exchange="SMART",
                            primary_exchange="SFB",
                            currency="SEK",
                            local_symbol="SIVE",
                            order_type="LMT",
                            limit_price=Decimal("11.31"),
                            aux_price=None,
                            outside_rth=False,
                            oca_group=None,
                            oca_type=None,
                            transmit=True,
                            warning_text=None,
                            reject_reason=None,
                            completed_status=None,
                            completed_time=None,
                        )
                    },
                    executions=(),
                    portfolio=(),
                    positions=(),
                    account_values={},
                ),
            )

        self.assertEqual(cancelled_order_ids, [11])
        self.assertEqual(len(result.cancelled_entries), 1)
        self.assertEqual(result.cancelled_entries[0].action, "entry_cancelled_at_expiry")
        record = self._read_record("runtime-sive-1")
        self.assertEqual(record.state, ExecutionState.ENTRY_CANCELLED.value)
        self.assertEqual(record.broker_order_status, "Cancelled")

    def test_run_startup_reconciliation_skips_active_scan_after_session_close(self) -> None:
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
            broker_order_id=11,
            entry_filled_quantity="80",
            entry_avg_fill_price="146.90",
        )

        with TemporaryDirectory() as tmpdir:
            calendar_path = Path(tmpdir) / "sessions.csv"
            calendar_path.write_text(
                "\n".join(
                    (
                        "session_date,timezone,open_time,close_time,session_kind",
                        "2026-04-10,Europe/Stockholm,09:00:00,17:30:00,regular",
                    )
                ),
                encoding="utf-8",
            )

            result = run_startup_reconciliation(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=calendar_path,
                now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
                broker_snapshot_fetcher=lambda *args, **kwargs: (_ for _ in ()).throw(
                    AssertionError("startup should not fetch broker snapshot after close")
                ),
                broker_callback_fetcher=lambda: (_ for _ in ()).throw(
                    AssertionError("startup should not drain callbacks after close")
                ),
                virtual_market_sync=lambda at: (_ for _ in ()).throw(
                    AssertionError("startup should not sync virtual market after close")
                ),
                exit_submitter=lambda *args, **kwargs: (_ for _ in ()).throw(
                    AssertionError("startup should not reconcile active exits after close")
                ),
            )

        self.assertEqual(result.issues, ())
        self.assertEqual(self._read_reconciliation_runs()[0].status, "CLEAN")
