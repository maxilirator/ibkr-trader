from __future__ import annotations

from tests._runtime_worker_shared import *  # noqa: F401,F403


class RuntimeWorkerTests01(RuntimeWorkerTestCase):
    def test_run_runtime_cycle_uses_default_utc_clock_when_now_is_omitted(self) -> None:
        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            submit_due_entries=False,
        )

        self.assertEqual(result.cycle_started_at.tzinfo, timezone.utc)
        self.assertEqual(result.issues, ())

    def test_runtime_broker_operations_keep_normal_cycle_snapshot_light(self) -> None:
        recorded_operations: list[str] = []

        class _FakePrimary:
            def execute(self, operation_name: str, fn: object) -> object:
                recorded_operations.append(operation_name)
                return fn(object())

        class _FakeSessions:
            primary = _FakePrimary()

        with patch(
            "ibkr_trader.orchestration.runtime_worker.fetch_broker_runtime_snapshot",
            return_value=BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        ) as snapshot_fetch:
            broker_ops = _build_runtime_broker_operations(_FakeSessions())
            broker_ops.fetch_snapshot(self.config, timeout=17)

        self.assertEqual(recorded_operations, ["runtime_snapshot"])
        self.assertEqual(snapshot_fetch.call_args.kwargs["timeout"], 17)
        self.assertFalse(snapshot_fetch.call_args.kwargs["include_open_orders"])
        self.assertFalse(snapshot_fetch.call_args.kwargs["include_executions"])
        self.assertFalse(snapshot_fetch.call_args.kwargs["include_account_updates"])
        self.assertFalse(snapshot_fetch.call_args.kwargs["include_positions"])

    def test_runtime_broker_operations_use_rich_snapshot_for_reconciliation(self) -> None:
        recorded_operations: list[str] = []

        class _FakePrimary:
            def execute(self, operation_name: str, fn: object) -> object:
                recorded_operations.append(operation_name)
                return fn(object())

        class _FakeSessions:
            primary = _FakePrimary()

        with patch(
            "ibkr_trader.orchestration.runtime_worker.fetch_broker_runtime_snapshot",
            return_value=BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        ) as snapshot_fetch:
            broker_ops = _build_runtime_broker_operations(_FakeSessions())
            broker_ops.fetch_reconciliation_snapshot(
                self.config,
                timeout=23,
                include_open_orders=False,
                include_executions=False,
                include_positions=False,
            )

        self.assertEqual(recorded_operations, ["runtime_reconciliation_snapshot"])
        self.assertEqual(snapshot_fetch.call_args.kwargs["timeout"], 23)
        self.assertTrue(snapshot_fetch.call_args.kwargs["include_open_orders"])
        self.assertTrue(snapshot_fetch.call_args.kwargs["include_executions"])
        self.assertFalse(snapshot_fetch.call_args.kwargs["include_account_updates"])
        self.assertTrue(snapshot_fetch.call_args.kwargs["include_positions"])

    def test_run_runtime_cycle_submits_due_entry(self) -> None:
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
            self.assertEqual(broker_config.client_id, 0)
            self.assertEqual(instruction.instruction_id, "runtime-aapl-1")
            self.assertEqual(timeout, 10)
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
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.ENTRY_SUBMITTED.value)
        self.assertEqual(record.broker_order_id, 11)
        self.assertEqual(record.entry_submitted_quantity, "1")

    def test_run_runtime_cycle_keeps_active_real_work_snapshot_light(self) -> None:
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_SUBMITTED.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=_aapl_payload(),
            broker_order_id=11,
            account_key="GTW05",
        )
        snapshot_kwargs: dict[str, object] = {}

        def fake_snapshot_fetcher(*args: object, **kwargs: object) -> BrokerRuntimeSnapshot:
            del args
            snapshot_kwargs.update(kwargs)
            return BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            )

        run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            broker_snapshot_fetcher=fake_snapshot_fetcher,
        )

        self.assertTrue(snapshot_kwargs["include_open_orders"])
        self.assertFalse(snapshot_kwargs["include_executions"])
        self.assertFalse(snapshot_kwargs["include_positions"])

    def test_submit_due_pending_entries_skips_stale_already_submitted_entry(self) -> None:
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_SUBMITTED.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=_aapl_payload(),
            broker_order_id=11,
        )
        submitted_entries = []
        cancelled_entries = []
        issues = []

        _submit_due_pending_entries(
            self.session_factory,
            self.config,
            due_instruction_ids=["runtime-aapl-1"],
            cycle_started_at=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            timeout=10,
            kill_switch_enabled=False,
            entry_submitter=lambda *args, **kwargs: self.fail(
                "stale already-submitted entries must not be submitted again"
            ),
            broker_retry_delays=(),
            sleep_fn=lambda seconds: None,
            submitted_entries=submitted_entries,
            cancelled_entries=cancelled_entries,
            issues=issues,
        )

        self.assertEqual(submitted_entries, [])
        self.assertEqual(cancelled_entries, [])
        self.assertEqual(issues, [])
        session = self.session_factory()
        try:
            event = session.execute(
                select(InstructionEventRecord).where(
                    InstructionEventRecord.event_type
                    == "runtime_entry_submit_skipped"
                )
            ).scalar_one()
            self.assertEqual(event.state_before, ExecutionState.ENTRY_SUBMITTED.value)
        finally:
            session.close()

    def test_submit_due_pending_entries_persists_wire_audit_on_terminal_submit_failure(self) -> None:
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 20, 30, tzinfo=timezone.utc),
            payload=_aapl_payload(),
        )
        submitted_entries = []
        cancelled_entries = []
        issues = []

        def fake_submitter(*args: object, **kwargs: object) -> dict[str, object]:
            del args, kwargs
            exc = LookupError("IBKR rejected the WhatIf preflight: [202] rejected")
            exc.ibkr_wire_audit = [  # type: ignore[attr-defined]
                {
                    "event_type": "outbound_order_request",
                    "request": {
                        "api_method": "placeOrder",
                        "stage": "what_if_preflight",
                        "order": {
                            "order_ref": "runtime-aapl-1",
                            "order_type": "LMT",
                            "transmit": True,
                            "what_if": True,
                        },
                    },
                }
            ]
            raise exc

        _submit_due_pending_entries(
            self.session_factory,
            self.config,
            due_instruction_ids=["runtime-aapl-1"],
            cycle_started_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            timeout=10,
            kill_switch_enabled=False,
            entry_submitter=fake_submitter,
            broker_retry_delays=(),
            sleep_fn=lambda seconds: None,
            submitted_entries=submitted_entries,
            cancelled_entries=cancelled_entries,
            issues=issues,
        )

        session = self.session_factory()
        try:
            event = session.execute(
                select(InstructionEventRecord).where(
                    InstructionEventRecord.event_type == "entry_submit_failed"
                )
            ).scalar_one()
            self.assertEqual(
                event.payload["ibkr_wire_audit"][0]["request"]["stage"],
                "what_if_preflight",
            )
            self.assertTrue(
                event.payload["ibkr_wire_audit"][0]["request"]["order"]["transmit"]
            )
        finally:
            session.close()

    def test_submit_due_pending_entries_blocks_when_market_stream_is_not_ready(self) -> None:
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 20, 30, tzinfo=timezone.utc),
            payload=_aapl_payload(),
        )
        submitted_entries = []
        cancelled_entries = []
        issues = []

        _submit_due_pending_entries(
            self.session_factory,
            self.config,
            due_instruction_ids=["runtime-aapl-1"],
            cycle_started_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            timeout=10,
            kill_switch_enabled=False,
            entry_submitter=lambda *args, **kwargs: self.fail(
                "entry submission must wait for fresh market-stream data"
            ),
            broker_retry_delays=(),
            sleep_fn=lambda seconds: None,
            submitted_entries=submitted_entries,
            cancelled_entries=cancelled_entries,
            issues=issues,
            market_data_readiness_checker=lambda *args: {
                "ready": False,
                "symbol": "AAPL",
                "reason": "market_stream_data_stale",
                "evidence": {"latest_market_data_age_seconds": 600},
            },
        )

        self.assertEqual(submitted_entries, [])
        self.assertEqual(cancelled_entries, [])
        self.assertEqual(len(issues), 1)
        self.assertEqual(issues[0].stage, "market_data_readiness")
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.ENTRY_PENDING.value)

        session = self.session_factory()
        try:
            event = session.execute(
                select(InstructionEventRecord).where(
                    InstructionEventRecord.event_type
                    == "entry_submit_blocked_market_data_not_ready"
                )
            ).scalar_one()
            self.assertFalse(event.payload["ready"])
            self.assertEqual(event.payload["reason"], "market_stream_data_stale")
        finally:
            session.close()

    def test_run_runtime_cycle_records_ready_market_stream_evidence_before_submit(self) -> None:
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 20, 30, tzinfo=timezone.utc),
            payload=_aapl_payload(),
        )
        checks: list[tuple[str, str]] = []

        def readiness_checker(
            instruction_id: str,
            payload: dict[str, object],
            cycle_started_at: datetime,
        ) -> dict[str, object]:
            checks.append((instruction_id, payload["instruction"]["instrument"]["symbol"]))
            self.assertEqual(cycle_started_at, datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc))
            return {
                "ready": True,
                "symbol": "AAPL",
                "reason": "market_stream_ready",
                "evidence": {
                    "latest_market_data_at": "2026-04-10T19:59:59+00:00",
                    "latest_market_data_age_seconds": 1,
                },
            }

        def fake_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            del broker_config, timeout
            return {
                "instruction_id": instruction.instruction_id,
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 265598, "symbol": "AAPL"},
                "order": {
                    "order_ref": instruction.instruction_id,
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
            now=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
            entry_submitter=fake_submitter,
            market_data_readiness_checker=readiness_checker,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(checks, [("runtime-aapl-1", "AAPL")])
        self.assertEqual(len(result.submitted_entries), 1)
        session = self.session_factory()
        try:
            event_types = [
                event.event_type
                for event in session.execute(
                    select(InstructionEventRecord).order_by(InstructionEventRecord.id)
                ).scalars()
            ]
            self.assertLess(
                event_types.index("entry_market_data_ready"),
                event_types.index("entry_order_submitted"),
            )
        finally:
            session.close()

    def test_run_runtime_cycle_archives_resolved_market_data_readiness_issue(self) -> None:
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 20, 30, tzinfo=timezone.utc),
            payload=_aapl_payload(),
        )
        session = self.session_factory()
        try:
            reconciliation_run = ReconciliationRunRecord(
                run_kind="runtime_cycle",
                broker_kind="IBKR",
                account_key="DU1234567",
                runtime_timezone="Europe/Stockholm",
                started_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
                completed_at=datetime(2026, 4, 10, 19, 55, 5, tzinfo=timezone.utc),
                status="WARNINGS",
                issue_count=1,
                action_count=0,
                metadata_json={},
            )
            reconciliation_run.issues.append(
                ReconciliationIssueRecord(
                    instruction_id="runtime-aapl-1",
                    stage="market_data_readiness",
                    severity="ERROR",
                    message=(
                        "Skipped due entry submission because live market-stream "
                        "data is not ready: market_stream_has_no_quote_or_bar."
                    ),
                    observed_at=datetime(2026, 4, 10, 19, 55, 5, tzinfo=timezone.utc),
                    payload={},
                )
            )
            session.add(reconciliation_run)
            session.commit()
        finally:
            session.close()

        def fake_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            del broker_config, timeout
            return {
                "instruction_id": instruction.instruction_id,
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 265598, "symbol": "AAPL"},
                "order": {
                    "order_ref": instruction.instruction_id,
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
            now=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
            entry_submitter=fake_submitter,
            market_data_readiness_checker=lambda *args: {
                "ready": True,
                "symbol": "AAPL",
                "reason": "market_stream_ready",
                "evidence": {"latest_market_data_age_seconds": 1},
            },
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.submitted_entries), 1)
        session = self.session_factory()
        try:
            issue = session.execute(select(ReconciliationIssueRecord)).scalar_one()
            self.assertIsNotNone(issue.archived_at)
            self.assertEqual(issue.archived_by, "runtime_cycle")
            self.assertIn("Market stream evidence became ready", issue.archive_reason)
            self.assertEqual(
                issue.payload["auto_resolved_by"],
                "entry_market_data_ready",
            )
        finally:
            session.close()

    def test_run_runtime_cycle_resubmits_cancelled_entry_before_expiry(self) -> None:
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_SUBMITTED.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 20, 30, tzinfo=timezone.utc),
            payload=_aapl_payload(),
            broker_order_id=11,
        )
        session = self.session_factory()
        try:
            instruction_record = session.execute(
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
                    instruction_id=instruction_record.id,
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
                    status="Cancelled",
                    total_quantity="1",
                    limit_price="200.00",
                    submitted_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
                    raw_payload={
                        "last_order_error_callback": {
                            "errorCode": 202,
                            "errorString": "Order Canceled - reason:",
                        }
                    },
                    metadata_json={},
                )
            )
            session.commit()
        finally:
            session.close()

        submit_calls: list[str] = []

        def fake_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            del broker_config, timeout
            submit_calls.append(instruction.instruction_id)
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
                    "orderId": 22,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 8022,
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
            now=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
            entry_submitter=fake_submitter,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(submit_calls, ["runtime-aapl-1"])
        self.assertEqual(len(result.submitted_entries), 1)
        self.assertEqual(
            result.submitted_entries[0].action,
            "entry_resubmitted_after_broker_cancel",
        )
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.ENTRY_SUBMITTED.value)
        self.assertEqual(record.broker_order_id, 22)
        self.assertEqual(record.broker_order_status, "Submitted")

        session = self.session_factory()
        try:
            event_types = [
                event.event_type
                for event in session.execute(
                    select(InstructionEventRecord)
                    .join(InstructionRecord)
                    .where(InstructionRecord.instruction_id == "runtime-aapl-1")
                    .order_by(InstructionEventRecord.id)
                ).scalars()
            ]
        finally:
            session.close()
        self.assertIn("entry_order_requeued_for_resubmit", event_types)
        self.assertIn("entry_order_submitted", event_types)

    def test_run_runtime_cycle_submits_due_virtual_entry_with_active_real_work(self) -> None:
        real_payload = _sive_payload()
        virtual_payload = _aapl_payload()
        virtual_payload["instruction"]["instruction_id"] = "runtime-virtual-aapl-1"
        virtual_payload["instruction"]["account"]["account_key"] = "virtualrl01"
        virtual_payload["instruction"]["account"]["book_key"] = "rl_virtual_long"
        self._insert_instruction(
            instruction_id="runtime-sive-real-open-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.POSITION_OPEN.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=real_payload,
            broker_order_id=1001,
            entry_filled_quantity="100",
            entry_avg_fill_price="10.00",
            account_key="GTW05",
        )
        self._insert_instruction(
            instruction_id="runtime-virtual-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=virtual_payload,
            account_key="virtualrl01",
            book_key="rl_virtual_long",
            is_virtual=True,
        )

        submitted: list[str] = []

        def fake_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            del broker_config, timeout
            submitted.append(instruction.instruction_id)
            return {
                "instruction_id": instruction.instruction_id,
                "account": "virtualrl01",
                "is_virtual": True,
                "warnings": [],
                "resolved_contract": {"con_id": 265598, "symbol": "AAPL"},
                "order": {
                    "order_ref": instruction.instruction_id,
                    "action": "BUY",
                    "order_type": "LMT",
                    "time_in_force": "DAY",
                    "limit_price": "200.00",
                    "total_quantity": "1",
                    "outside_rth": False,
                    "transmit": False,
                    "is_virtual": True,
                },
                "broker_order_status": {
                    "orderId": 90001,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 990001,
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
            broker_snapshot_fetcher=lambda *args, **kwargs: (_ for _ in ()).throw(
                ConnectionError("gateway down")
            ),
        )

        self.assertEqual(submitted, ["runtime-virtual-aapl-1"])
        self.assertEqual(len(result.submitted_entries), 1)
        self.assertTrue(any(issue.stage == "broker_snapshot" for issue in result.issues))
        self.assertEqual(
            self._read_record("runtime-virtual-aapl-1").state,
            ExecutionState.ENTRY_SUBMITTED.value,
        )
        self.assertEqual(
            self._read_record("runtime-sive-real-open-1").state,
            ExecutionState.POSITION_OPEN.value,
        )

    def test_run_runtime_cycle_submits_due_real_entry_when_snapshot_unavailable(self) -> None:
        payload = _aapl_payload()
        active_payload = _sive_payload()
        self._insert_instruction(
            instruction_id="runtime-sive-active-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.ENTRY_SUBMITTED.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=active_payload,
            broker_order_id=1001,
            account_key="GTW05",
        )
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
            account_key="GTW05",
        )

        submitted: list[str] = []

        def fake_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            del broker_config, timeout
            submitted.append(instruction.instruction_id)
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
            broker_snapshot_fetcher=lambda *args, **kwargs: (_ for _ in ()).throw(
                TimeoutError("executions timed out")
            ),
        )

        self.assertEqual(submitted, ["runtime-aapl-1"])
        self.assertEqual(len(result.submitted_entries), 1)
        self.assertTrue(any(issue.stage == "broker_snapshot" for issue in result.issues))
        self.assertTrue(any(issue.stage == "entry_submit" for issue in result.issues))
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.ENTRY_SUBMITTED.value)
        self.assertEqual(record.broker_order_id, 11)

    def test_run_runtime_cycle_cancels_expired_pending_entry_before_submit(self) -> None:
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

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            now=datetime(2026, 4, 10, 20, 5, tzinfo=timezone.utc),
            entry_submitter=lambda *args, **kwargs: self.fail(
                "expired pending entries must not be submitted"
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
        self.assertEqual(len(result.cancelled_entries), 1)
        self.assertEqual(
            self._read_record("runtime-aapl-1").state,
            ExecutionState.ENTRY_CANCELLED.value,
        )

    def test_run_runtime_cycle_uses_session_close_as_effective_pending_expiry(self) -> None:
        payload = _sive_payload()
        payload["instruction"]["entry"]["submit_at"] = "2026-04-30T09:25:00+02:00"
        payload["instruction"]["entry"]["expire_at"] = "2026-04-30T17:30:00+02:00"
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 30, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 30, 15, 30, tzinfo=timezone.utc),
            payload=payload,
        )

        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            schedule_path.write_text(
                "\n".join(
                    [
                        "session_date,timezone,open_time,close_time,session_kind,base_calendar,overrides_source",
                        "2026-04-30,Europe/Stockholm,09:00,13:00,override,base,override",
                    ]
                ),
                encoding="utf-8",
            )

            result = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 30, 11, 11, tzinfo=timezone.utc),
                entry_submitter=lambda *args, **kwargs: self.fail(
                    "entries past the exchange session close must be cancelled"
                ),
                broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                    open_orders={},
                    executions=(),
                    portfolio=(),
                    positions=(_sive_broker_position(),),
                    account_values={},
                ),
            )

        self.assertEqual(len(result.cancelled_entries), 1)
        record = self._read_record("runtime-sive-1")
        self.assertEqual(record.state, ExecutionState.ENTRY_CANCELLED.value)
        self.assertIsNone(record.broker_order_id)

    def test_run_runtime_cycle_marks_terminal_due_entry_submit_failure(self) -> None:
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

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            entry_submitter=lambda *args, **kwargs: (_ for _ in ()).throw(
                ValueError("insufficient funds")
            ),
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.issues), 1)
        self.assertEqual(result.issues[0].stage, "entry_submit")
        self.assertEqual(
            self._read_record("runtime-aapl-1").state,
            ExecutionState.FAILED.value,
        )
