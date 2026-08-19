from __future__ import annotations

from tests._runtime_worker_shared import *  # noqa: F401,F403


class RuntimeWorkerTests08(RuntimeWorkerTestCase):
    def test_runtime_cycle_archives_resolved_forced_exit_cleanup_warning(self) -> None:
        payload = _sive_payload()
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.COMPLETED.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            entry_filled_quantity="100",
            entry_avg_fill_price="57.7",
        )

        session = self.session_factory()
        try:
            instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-sive-1"
                )
            ).scalar_one()
            instruction.exit_filled_quantity = "100"
            instruction.exit_avg_fill_price = "58.5"
            instruction.exit_filled_at = datetime(2026, 4, 13, 7, 0, tzinfo=timezone.utc)
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
                    external_order_id="4925",
                    external_perm_id="1236369441",
                    external_client_id="0",
                    order_ref="runtime-sive-1:exit:catastrophic_stop",
                    symbol="SIVE",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    side="SELL",
                    order_type="STP",
                    status="Cancelled",
                    total_quantity="100",
                    stop_price="49.05",
                    last_status_at=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
            )
            run = ReconciliationRunRecord(
                run_kind="runtime_cycle",
                broker_kind="IBKR",
                account_key="DU1234567",
                runtime_timezone="Europe/Stockholm",
                started_at=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
                completed_at=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
                status="WARNINGS",
                issue_count=1,
                action_count=0,
                metadata_json={},
            )
            session.add(run)
            session.flush()
            issue = ReconciliationIssueRecord(
                reconciliation_run_id=run.id,
                instruction_id="runtime-sive-1",
                stage="reconcile_instruction",
                severity="ERROR",
                message=(
                    "Forced exit cleanup could not confirm cancellation of broker "
                    "order 4925; broker status was PRESUBMITTED."
                ),
                observed_at=datetime(2026, 4, 13, 6, 59, tzinfo=timezone.utc),
                payload={},
            )
            session.add(issue)
            session.commit()
            issue_id = issue.id
        finally:
            session.close()

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.csv"),
            now=datetime(2026, 4, 13, 7, 1, tzinfo=timezone.utc),
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
            issue = session.get(ReconciliationIssueRecord, issue_id)
            self.assertIsNotNone(issue.archived_at)
            self.assertEqual(issue.archived_by, "runtime_cycle")
            self.assertIn("Forced-exit cleanup warning", issue.archive_reason)
            self.assertEqual(
                issue.payload["auto_resolved_by"],
                "forced_exit_cleanup_resolved",
            )
            self.assertEqual(
                issue.payload["resolution"]["reason"],
                "instruction_completed",
            )
        finally:
            session.close()

    def test_persisted_open_exit_orders_dedupes_replaced_order_lineage(self) -> None:
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
            exit_order_id=3953,
            entry_filled_quantity="100",
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
                account_key="GTW05",
                base_currency="USD",
                metadata_json={},
            )
            session.add(broker_account)
            session.flush()
            session.add_all(
                [
                    BrokerOrderRecord(
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="GTW05",
                        order_role="EXIT",
                        external_order_id="3952",
                        external_perm_id="449407988",
                        order_ref="runtime-sive-1:exit:forced",
                        symbol="SIVE",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        side="SELL",
                        order_type="MKT",
                        status="PreSubmitted",
                        total_quantity="100",
                        last_status_at=datetime(2026, 4, 10, 7, 30, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    ),
                    BrokerOrderRecord(
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="GTW05",
                        order_role="EXIT",
                        external_order_id="3953",
                        external_perm_id="449407988",
                        order_ref="runtime-sive-1:exit:forced",
                        symbol="SIVE",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        side="SELL",
                        order_type="MKT",
                        status="PreSubmitted",
                        total_quantity="100",
                        last_status_at=datetime(2026, 4, 10, 7, 31, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    ),
                ]
            )
            session.commit()
            records = [instruction]
        finally:
            session.close()

        result = _persisted_open_order_ids_by_instruction(
            self.session_factory,
            records=records,
            order_role="EXIT",
        )

        self.assertEqual(result["runtime-sive-1"], (3953,))

    def test_persisted_open_order_ids_ignore_orders_with_matching_execution_fill(self) -> None:
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
            exit_order_id=3953,
            entry_filled_quantity="100",
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
                account_key="GTW05",
                base_currency="USD",
                metadata_json={},
            )
            session.add(broker_account)
            session.flush()
            broker_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="GTW05",
                order_role="EXIT",
                external_order_id="3953",
                external_perm_id="449407988",
                order_ref="runtime-sive-1:exit:forced",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                side="SELL",
                order_type="MKT",
                status="PendingCancel",
                total_quantity="100",
                last_status_at=datetime(2026, 4, 10, 7, 31, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add(broker_order)
            session.flush()
            session.add(
                ExecutionFillRecord(
                    broker_order_id=broker_order.id,
                    instruction_id=instruction.id,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="GTW05",
                    external_execution_id="0001",
                    external_order_id="3953",
                    external_perm_id="449407988",
                    order_ref="runtime-sive-1:exit:forced",
                    symbol="SIVE",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    side="SLD",
                    quantity="100",
                    price="22.61",
                    executed_at=datetime(2026, 4, 10, 7, 32, tzinfo=timezone.utc),
                    raw_payload={},
                )
            )
            session.commit()
            records = [instruction]
        finally:
            session.close()

        result = _persisted_open_order_ids_by_instruction(
            self.session_factory,
            records=records,
            order_role="EXIT",
        )

        self.assertEqual(result["runtime-sive-1"], ())

    def test_persisted_open_exit_orders_require_exact_fill_lineage(self) -> None:
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

        session = self.session_factory()
        try:
            instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-sive-1"
                )
            ).scalar_one()
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="GTW05",
                base_currency="USD",
                metadata_json={},
            )
            session.add(broker_account)
            session.flush()
            stop_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="GTW05",
                order_role="EXIT",
                external_order_id="3952",
                external_perm_id="449407988",
                order_ref="runtime-sive-1:exit:catastrophic_stop",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                side="SELL",
                order_type="STP",
                status="PendingCancel",
                total_quantity="100",
                last_status_at=datetime(2026, 4, 10, 7, 30, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            take_profit_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="GTW05",
                order_role="EXIT",
                external_order_id="3953",
                external_perm_id="449407989",
                order_ref="runtime-sive-1:exit:take_profit",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                side="SELL",
                order_type="LMT",
                status="PreSubmitted",
                total_quantity="100",
                last_status_at=datetime(2026, 4, 10, 7, 31, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add_all([stop_order, take_profit_order])
            session.flush()
            session.add(
                ExecutionFillRecord(
                    broker_order_id=stop_order.id,
                    instruction_id=instruction.id,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="GTW05",
                    external_execution_id="fill-stop-1",
                    external_order_id="3952",
                    external_perm_id="449407988",
                    order_ref="runtime-sive-1:exit:catastrophic_stop",
                    symbol="SIVE",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    side="SLD",
                    quantity="100",
                    price="95.00",
                    executed_at=datetime(2026, 4, 10, 7, 32, tzinfo=timezone.utc),
                    raw_payload={},
                )
            )
            session.commit()
            records = [instruction]
        finally:
            session.close()

        result = _persisted_open_order_ids_by_instruction(
            self.session_factory,
            records=records,
            order_role="EXIT",
        )

        self.assertEqual(result["runtime-sive-1"], (3953,))

    def test_run_runtime_cycle_completes_instruction_from_persisted_exit_fill(self) -> None:
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
            exit_order_id=3953,
            entry_filled_quantity="100",
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
                account_key="GTW05",
                base_currency="USD",
                metadata_json={},
            )
            session.add(broker_account)
            session.flush()
            broker_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="GTW05",
                order_role="EXIT",
                external_order_id="3953",
                external_perm_id="449407988",
                order_ref="runtime-sive-1:exit:forced",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                side="SELL",
                order_type="MKT",
                status="PendingCancel",
                total_quantity="100",
                last_status_at=datetime(2026, 4, 10, 7, 31, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add(broker_order)
            session.flush()
            session.add(
                ExecutionFillRecord(
                    broker_order_id=broker_order.id,
                    instruction_id=instruction.id,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="GTW05",
                    external_execution_id="0001",
                    external_order_id="3953",
                    external_perm_id="449407988",
                    order_ref="runtime-sive-1:exit:forced",
                    symbol="SIVE",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    side="SLD",
                    quantity="100",
                    price="22.61",
                    executed_at=datetime(2026, 4, 10, 7, 32, tzinfo=timezone.utc),
                    raw_payload={},
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
            now=datetime(2026, 4, 10, 8, 30, tzinfo=timezone.utc),
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.completed_instructions), 1)
        record = self._read_record("runtime-sive-1")
        self.assertEqual(record.state, ExecutionState.COMPLETED.value)
        self.assertEqual(record.exit_order_status, "Filled")
        self.assertEqual(record.exit_filled_quantity, "100")
