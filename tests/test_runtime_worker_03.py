from __future__ import annotations

from tests._runtime_worker_shared import *  # noqa: F401,F403


class RuntimeWorkerTests03(RuntimeWorkerTestCase):
    def test_run_runtime_cycle_suppresses_repeated_broker_outage_audits(self) -> None:
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

        def failing_snapshot(attempt: int):
            def _raise(*args: object, **kwargs: object) -> BrokerRuntimeSnapshot:
                raise ConnectionError(
                    "Broker session 'primary' is cooling down after "
                    f"{attempt} failed broker attempt(s); next retry at "
                    f"2026-04-10T19:{56 + attempt:02d}:00Z. Last error: "
                    "Failed to connect to IBKR at 127.0.0.1:4002 with client_id=0"
                )

            return _raise

        first = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            broker_snapshot_fetcher=failing_snapshot(1),
        )
        second = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 19, 57, tzinfo=timezone.utc),
            broker_snapshot_fetcher=failing_snapshot(2),
        )

        self.assertEqual(len(first.issues), 1)
        self.assertEqual(len(second.issues), 1)

        session = self.session_factory()
        try:
            runs = list(session.execute(select(ReconciliationRunRecord)).scalars())
            issues = list(session.execute(select(ReconciliationIssueRecord)).scalars())
            self.assertEqual(len(runs), 1)
            self.assertEqual(len(issues), 1)
            self.assertEqual(runs[0].issue_count, 1)
            self.assertEqual(
                runs[0].metadata_json["suppressed_reconciliation_repeats"],
                1,
            )
            self.assertEqual(
                runs[0].metadata_json["last_suppressed_active_instruction_count"],
                1,
            )
        finally:
            session.close()

    def test_run_runtime_cycle_records_new_broker_outage_after_cooldown(self) -> None:
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

        def fail_snapshot(*args: object, **kwargs: object) -> BrokerRuntimeSnapshot:
            raise ConnectionError(
                "No response received for current_time request 0 within 5 seconds"
            )

        run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2030, 4, 10, 19, 56, tzinfo=timezone.utc),
            broker_snapshot_fetcher=fail_snapshot,
        )
        run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2030, 4, 10, 20, 7, tzinfo=timezone.utc),
            broker_snapshot_fetcher=fail_snapshot,
        )

        self.assertEqual(len(self._read_reconciliation_runs()), 2)

    def test_run_startup_reconciliation_skips_due_entry_submission(self) -> None:
        pending_payload = _aapl_payload()
        active_payload = _aapl_payload()
        active_payload["instruction"]["instruction_id"] = "runtime-aapl-2"
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=pending_payload,
        )
        self._insert_instruction(
            instruction_id="runtime-aapl-2",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_SUBMITTED.value,
            submit_at=datetime(2026, 4, 10, 19, 50, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=active_payload,
            broker_order_id=22,
        )

        result = run_startup_reconciliation(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(result.submitted_entries, ())
        self.assertEqual(
            self._read_record("runtime-aapl-1").state,
            ExecutionState.ENTRY_PENDING.value,
        )

        runs = self._read_reconciliation_runs()
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0].run_kind, "startup_reconciliation")
        self.assertIs(runs[0].metadata_json["submit_due_entries"], False)
        self.assertEqual(runs[0].metadata_json["due_instruction_count"], 1)
        self.assertEqual(runs[0].metadata_json["active_instruction_count"], 1)

    def test_run_runtime_cycle_can_target_selected_instruction_ids(self) -> None:
        payload = _aapl_payload()
        other_payload = _aapl_payload()
        other_payload["instruction"]["instruction_id"] = "runtime-aapl-2"
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
        self._insert_instruction(
            instruction_id="runtime-aapl-2",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=other_payload,
        )

        submitted_ids: list[str] = []

        def fake_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            submitted_ids.append(instruction.instruction_id)
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
                    "orderId": 11 if instruction.instruction_id == "runtime-aapl-1" else 12,
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
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            instruction_ids=("runtime-aapl-2",),
            entry_submitter=fake_submitter,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual([action.instruction_id for action in result.submitted_entries], ["runtime-aapl-2"])
        self.assertEqual(submitted_ids, ["runtime-aapl-2"])
        self.assertEqual(
            self._read_record("runtime-aapl-1").state,
            ExecutionState.ENTRY_PENDING.value,
        )
        self.assertEqual(
            self._read_record("runtime-aapl-2").state,
            ExecutionState.ENTRY_SUBMITTED.value,
        )

    def test_run_runtime_cycle_retries_transient_entry_submit_connection_error(self) -> None:
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

        attempts = {"count": 0}
        sleep_calls: list[float] = []

        def flaky_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise ConnectionError(
                    "Failed to connect to IBKR at 127.0.0.1:7497 with client_id=0."
                )
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
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 19, 56, tzinfo=timezone.utc),
            entry_submitter=flaky_submitter,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
            broker_retry_delays=(0.25,),
            sleep_fn=sleep_calls.append,
        )

        self.assertEqual(attempts["count"], 2)
        self.assertEqual(sleep_calls, [0.25])
        self.assertEqual(len(result.submitted_entries), 1)
        self.assertEqual(
            self._read_record("runtime-aapl-1").state,
            ExecutionState.ENTRY_SUBMITTED.value,
        )

    def test_run_runtime_cycle_reconciles_entry_fill_and_submits_take_profit(self) -> None:
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
            self.assertEqual(order_ref, "runtime-aapl-1:exit:take_profit")
            self.assertEqual(str(quantity), "1")
            self.assertEqual(str(limit_price), "204.00")
            self.assertIsNone(stop_price)
            self.assertIsNone(oca_group)
            self.assertIsNone(oca_type)
            return {
                "instruction_id": "runtime-aapl-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 265598, "symbol": "AAPL"},
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": "LMT",
                    "time_in_force": "DAY",
                    "limit_price": "204.00",
                    "total_quantity": "1",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 21,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 9001,
                    "parentId": 0,
                    "lastFillPrice": 0.0,
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": 0.0,
                },
                "ibkr_wire_audit": [
                    {
                        "event_type": "outbound_order_request",
                        "event_at": datetime(
                            2026, 4, 10, 20, 5, tzinfo=timezone.utc
                        ),
                        "request": {
                            "api_method": "placeOrder",
                            "stage": "live_order_submit",
                            "order": {
                                "order_ref": "runtime-aapl-1:exit:take_profit"
                            },
                        },
                    }
                ],
            }

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 20, 5, tzinfo=timezone.utc),
            exit_submitter=fake_exit_submitter,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(
                    BrokerExecution(
                        exec_id="E-1",
                        order_id=11,
                        perm_id=8001,
                        client_id=0,
                        order_ref="runtime-aapl-1",
                        side="BOT",
                        shares="1",
                        price="200.00",
                        exchange="NASDAQ",
                        executed_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
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

        self.assertEqual(len(result.filled_entries), 1)
        self.assertEqual(len(result.submitted_exits), 1)
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.entry_filled_quantity, "1")
        self.assertEqual(record.entry_avg_fill_price, "200.00")
        self.assertEqual(record.exit_order_id, 21)
        self.assertEqual(record.exit_submitted_quantity, "1")
        session = self.session_factory()
        try:
            broker_orders = session.execute(
                select(BrokerOrderRecord).order_by(BrokerOrderRecord.id)
            ).scalars().all()
            self.assertEqual(len(broker_orders), 2)
            self.assertEqual(
                [(item.order_role, item.status) for item in broker_orders],
                [("ENTRY", "FILLED"), ("EXIT", "Submitted")],
            )
            exit_event = session.execute(
                select(InstructionEventRecord).where(
                    InstructionEventRecord.event_type == "take_profit_exit_submitted"
                )
            ).scalar_one()
            self.assertEqual(
                exit_event.payload["broker_submission"]["ibkr_wire_audit"][0][
                    "event_at"
                ],
                "2026-04-10T20:05:00+00:00",
            )
        finally:
            session.close()

    def test_run_runtime_cycle_submits_take_profit_and_catastrophic_stop(self) -> None:
        payload = _aapl_payload()
        payload["instruction"]["exit"]["catastrophic_stop_loss_pct"] = "0.15"
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

        calls: list[dict[str, object]] = []

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
            calls.append(
                {
                    "order_ref": order_ref,
                    "order_type": order_type,
                    "limit_price": limit_price,
                    "stop_price": stop_price,
                    "oca_group": oca_group,
                    "oca_type": oca_type,
                }
            )
            order_id = 21 if order_ref.endswith("take_profit") else 22
            order_type_code = "LMT" if order_ref.endswith("take_profit") else "STP"
            return {
                "instruction_id": "runtime-aapl-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 265598, "symbol": "AAPL"},
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": order_type_code,
                    "time_in_force": "DAY",
                    "limit_price": (
                        str(limit_price) if limit_price is not None else None
                    ),
                    "stop_price": (
                        str(stop_price) if stop_price is not None else None
                    ),
                    "total_quantity": "1",
                    "outside_rth": False,
                    "oca_group": oca_group,
                    "oca_type": oca_type,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": order_id,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 9000 + order_id,
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
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 20, 5, tzinfo=timezone.utc),
            exit_submitter=fake_exit_submitter,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(
                    BrokerExecution(
                        exec_id="E-1",
                        order_id=11,
                        perm_id=8001,
                        client_id=0,
                        order_ref="runtime-aapl-1",
                        side="BOT",
                        shares="1",
                        price="200.00",
                        exchange="NASDAQ",
                        executed_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
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

        self.assertEqual(len(result.filled_entries), 1)
        self.assertEqual(len(result.submitted_exits), 2)
        self.assertEqual(
            [call["order_ref"] for call in calls],
            [
                "runtime-aapl-1:exit:catastrophic_stop",
                "runtime-aapl-1:exit:take_profit",
            ],
        )
        self.assertEqual(calls[0]["stop_price"], Decimal("170.00"))
        self.assertEqual(calls[1]["limit_price"], Decimal("204.00"))
        self.assertTrue(str(calls[0]["oca_group"]).startswith("OCA"))
        self.assertLessEqual(len(str(calls[0]["oca_group"])), 32)
        self.assertNotIn(":", str(calls[0]["oca_group"]))
        self.assertEqual(calls[0]["oca_group"], calls[1]["oca_group"])
        self.assertEqual(calls[0]["oca_type"], 1)
        self.assertEqual(calls[1]["oca_type"], 1)
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.exit_order_id, 22)
        self.assertEqual(record.exit_submitted_quantity, "1")

    def test_run_runtime_cycle_keeps_entry_fill_when_protective_exit_fails(
        self,
    ) -> None:
        payload = _aapl_payload()
        payload["instruction"]["exit"]["catastrophic_stop_loss_pct"] = "0.15"
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

        exit_submit_calls: list[str] = []

        def rejecting_exit_submitter(
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
            del (
                broker_config,
                instruction,
                quantity,
                order_type,
                timeout,
                limit_price,
                stop_price,
                oca_group,
                oca_type,
            )
            exit_submit_calls.append(order_ref)
            raise RuntimeError("IBKR rejected the order submission: [401] OCA Group")

        snapshot = BrokerRuntimeSnapshot(
            open_orders={},
            executions=(
                BrokerExecution(
                    exec_id="E-1",
                    order_id=11,
                    perm_id=8001,
                    client_id=0,
                    order_ref="runtime-aapl-1",
                    side="BOT",
                    shares="1",
                    price="200.00",
                    exchange="NASDAQ",
                    executed_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
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
        )

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 20, 5, tzinfo=timezone.utc),
            exit_submitter=rejecting_exit_submitter,
            broker_snapshot_fetcher=lambda *args, **kwargs: snapshot,
        )

        self.assertEqual(len(result.issues), 0)
        self.assertEqual(len(result.filled_entries), 1)
        self.assertEqual(len(result.submitted_exits), 0)
        self.assertEqual(
            exit_submit_calls,
            [
                "runtime-aapl-1:exit:catastrophic_stop",
                "runtime-aapl-1:exit:catastrophic_stop",
                "runtime-aapl-1:exit:take_profit",
            ],
        )
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.POSITION_OPEN.value)
        self.assertEqual(record.entry_filled_quantity, "1")
        self.assertEqual(record.entry_avg_fill_price, "200.00")
        self.assertIsNone(record.exit_order_id)

        run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 20, 6, tzinfo=timezone.utc),
            exit_submitter=rejecting_exit_submitter,
            broker_snapshot_fetcher=lambda *args, **kwargs: snapshot,
        )
        self.assertEqual(len(exit_submit_calls), 3)

        session = self.session_factory()
        try:
            event_types = [
                event.event_type
                for event in session.execute(
                    select(InstructionEventRecord).order_by(
                        InstructionEventRecord.id
                    )
                ).scalars()
            ]
            self.assertEqual(
                event_types,
                [
                    "entry_order_filled",
                    "protective_exit_submission_claimed",
                    "protective_exit_submit_failed",
                    "protective_exit_submit_failed",
                    "protective_exit_submit_failed",
                ],
            )
        finally:
            session.close()
