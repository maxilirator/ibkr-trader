from __future__ import annotations

from tests._runtime_worker_shared import *  # noqa: F401,F403


class RuntimeWorkerTests04(RuntimeWorkerTestCase):
    def test_run_runtime_cycle_falls_back_to_single_stop_when_oca_is_rejected(
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

        calls: list[dict[str, object]] = []

        def oca_rejecting_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: Decimal,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: Decimal | None = None,
            stop_price: Decimal | None = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            del broker_config, instruction, timeout
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
            if oca_group is not None:
                raise RuntimeError("IBKR rejected the order submission: [401] OCA Group")
            self.assertEqual(order_ref, "runtime-aapl-1:exit:catastrophic_stop")
            return {
                "instruction_id": "runtime-aapl-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 265598, "symbol": "AAPL"},
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": "STP",
                    "time_in_force": "DAY",
                    "limit_price": None,
                    "stop_price": str(stop_price),
                    "total_quantity": str(quantity),
                    "outside_rth": False,
                    "oca_group": None,
                    "oca_type": None,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 31,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": str(quantity),
                    "avgFillPrice": 0.0,
                    "permId": 9031,
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
            exit_submitter=oca_rejecting_exit_submitter,
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
        self.assertEqual(
            [call["order_ref"] for call in calls],
            [
                "runtime-aapl-1:exit:catastrophic_stop",
                "runtime-aapl-1:exit:catastrophic_stop",
            ],
        )
        self.assertIsNotNone(calls[0]["oca_group"])
        self.assertIsNone(calls[1]["oca_group"])
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.exit_order_id, 31)
        self.assertTrue(result.submitted_exits[0].detail["fallback_without_oca"])

    def test_run_runtime_cycle_repairs_missing_protective_exits_for_open_position(
        self,
    ) -> None:
        payload = _aapl_payload()
        payload["instruction"]["exit"]["catastrophic_stop_loss_pct"] = "0.15"
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.POSITION_OPEN.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
            broker_order_id=11,
            entry_filled_quantity="1",
            entry_avg_fill_price="200.00",
        )

        calls: list[dict[str, object]] = []

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: Decimal,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: Decimal | None = None,
            stop_price: Decimal | None = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            del broker_config, instruction, timeout
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
            order_id = 41 if order_ref.endswith("catastrophic_stop") else 42
            order_type_code = "STP" if order_ref.endswith("catastrophic_stop") else "LMT"
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
                    "limit_price": str(limit_price) if limit_price is not None else None,
                    "stop_price": str(stop_price) if stop_price is not None else None,
                    "total_quantity": str(quantity),
                    "outside_rth": False,
                    "oca_group": oca_group,
                    "oca_type": oca_type,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": order_id,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": str(quantity),
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
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.submitted_exits), 2)
        self.assertEqual(
            [call["order_ref"] for call in calls],
            [
                "runtime-aapl-1:exit:catastrophic_stop",
                "runtime-aapl-1:exit:take_profit",
            ],
        )
        self.assertEqual(calls[0]["oca_group"], calls[1]["oca_group"])
        self.assertTrue(str(calls[0]["oca_group"]).startswith("OCA"))
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.exit_order_id, 41)

    def test_run_runtime_cycle_repairs_missing_take_profit_when_stop_is_open(
        self,
    ) -> None:
        payload = _aapl_payload()
        payload["instruction"]["exit"]["catastrophic_stop_loss_pct"] = "0.15"
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
            broker_order_id=11,
            exit_order_id=41,
            entry_filled_quantity="1",
            entry_avg_fill_price="200.00",
        )

        calls: list[dict[str, object]] = []

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: Decimal,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: Decimal | None = None,
            stop_price: Decimal | None = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            del broker_config, instruction, timeout
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
            return {
                "instruction_id": "runtime-aapl-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {
                    "con_id": 265598,
                    "symbol": "AAPL",
                    "security_type": "STK",
                    "exchange": "SMART",
                    "currency": "USD",
                },
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": "LMT",
                    "time_in_force": "DAY",
                    "limit_price": str(limit_price) if limit_price is not None else None,
                    "stop_price": None,
                    "total_quantity": str(quantity),
                    "outside_rth": False,
                    "oca_group": oca_group,
                    "oca_type": oca_type,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 42,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": str(quantity),
                    "avgFillPrice": 0.0,
                    "permId": 9042,
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
                open_orders={
                    41: BrokerOpenOrder(
                        order_id=41,
                        perm_id=9041,
                        client_id=0,
                        status="PreSubmitted",
                        order_ref="runtime-aapl-1:exit:catastrophic_stop",
                        action="SELL",
                        total_quantity=Decimal("1"),
                        symbol="AAPL",
                        account="DU1234567",
                        security_type="STK",
                        exchange="SMART",
                        primary_exchange="NASDAQ",
                        currency="USD",
                        local_symbol="AAPL",
                        order_type="STP",
                        aux_price=Decimal("170.00"),
                        oca_group="OCA-test",
                        oca_type=1,
                    )
                },
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(result.submitted_exits), 1)
        self.assertEqual(calls[0]["order_ref"], "runtime-aapl-1:exit:take_profit")
        self.assertEqual(str(calls[0]["limit_price"]), "204.00")
        self.assertIsNone(calls[0]["stop_price"])
        self.assertIsNotNone(calls[0]["oca_group"])
        self.assertEqual(calls[0]["oca_type"], 1)
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.exit_order_id, 41)

    def test_run_runtime_cycle_cancels_obsolete_exit_before_protective_repair(
        self,
    ) -> None:
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
            broker_order_id=11,
            exit_order_id=41,
            entry_filled_quantity="1",
            entry_avg_fill_price="200.00",
        )
        self._insert_exit_broker_order(
            instruction_id="runtime-aapl-1",
            external_order_id="41",
            order_ref="runtime-aapl-1:exit:delayed_limit",
            symbol="AAPL",
            currency="USD",
            limit_price="199.00",
        )

        operations: list[str] = []

        def fake_canceler(
            broker_config: IbkrConnectionConfig,
            order_id: int,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            del broker_config, timeout
            operations.append(f"cancel:{order_id}")
            return {"broker_order_status": {"orderId": order_id, "status": "Cancelled"}}

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: Decimal,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: Decimal | None = None,
            stop_price: Decimal | None = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            del broker_config, instruction, timeout, order_type, stop_price, oca_group, oca_type
            operations.append(f"submit:{order_ref}")
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
                    "limit_price": str(limit_price),
                    "total_quantity": str(quantity),
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 42,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": str(quantity),
                    "avgFillPrice": 0.0,
                    "permId": 9042,
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
            broker_order_canceler=fake_canceler,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={
                    41: BrokerOpenOrder(
                        order_id=41,
                        perm_id=9041,
                        client_id=0,
                        status="Submitted",
                        order_ref="runtime-aapl-1:exit:delayed_limit",
                        action="SELL",
                        total_quantity=Decimal("1"),
                        symbol="AAPL",
                        account="DU1234567",
                        security_type="STK",
                        exchange="SMART",
                        primary_exchange="NASDAQ",
                        currency="USD",
                        local_symbol="AAPL",
                        order_type="LMT",
                        limit_price=Decimal("199.00"),
                    )
                },
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(
            operations,
            ["cancel:41", "submit:runtime-aapl-1:exit:take_profit"],
        )
        self.assertEqual(len(result.submitted_exits), 1)
        session = self.session_factory()
        try:
            cleanup_event = session.execute(
                select(InstructionEventRecord).where(
                    InstructionEventRecord.event_type
                    == "exit_intent_obsolete_orders_cleanup_started"
                )
            ).scalar_one()
            self.assertEqual(
                cleanup_event.payload["desired_order_refs"],
                ["runtime-aapl-1:exit:take_profit"],
            )
            self.assertEqual(
                cleanup_event.payload["obsolete_orders"][0]["broker_order_id"],
                41,
            )
        finally:
            session.close()

    def test_run_runtime_cycle_blocks_protective_repair_when_obsolete_cancel_unconfirmed(
        self,
    ) -> None:
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
            broker_order_id=11,
            exit_order_id=41,
            entry_filled_quantity="1",
            entry_avg_fill_price="200.00",
        )

        submitted_refs: list[str] = []

        def fake_canceler(
            broker_config: IbkrConnectionConfig,
            order_id: int,
            *,
            timeout: int = 10,
        ) -> dict[str, object]:
            del broker_config, timeout
            return {"broker_order_status": {"orderId": order_id, "status": "PendingCancel"}}

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            **kwargs: object,
        ) -> dict[str, object]:
            del broker_config, instruction
            submitted_refs.append(str(kwargs["order_ref"]))
            raise AssertionError("replacement exit must wait for confirmed cancellation")

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 20, 5, tzinfo=timezone.utc),
            exit_submitter=fake_exit_submitter,
            broker_order_canceler=fake_canceler,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={
                    41: BrokerOpenOrder(
                        order_id=41,
                        perm_id=9041,
                        client_id=0,
                        status="Submitted",
                        order_ref="runtime-aapl-1:exit:delayed_limit",
                        action="SELL",
                        total_quantity=Decimal("1"),
                        symbol="AAPL",
                        account="DU1234567",
                        security_type="STK",
                        exchange="SMART",
                        primary_exchange="NASDAQ",
                        currency="USD",
                        local_symbol="AAPL",
                        order_type="LMT",
                        limit_price=Decimal("199.00"),
                    )
                },
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(submitted_refs, [])
        self.assertEqual(result.submitted_exits, ())
        self.assertEqual(len(result.issues), 1)
        self.assertIn("broker status was PENDINGCANCEL", result.issues[0].message)

    def test_run_runtime_cycle_marks_missing_protective_exits_stale_and_repairs(
        self,
    ) -> None:
        payload = _aapl_payload()
        payload["instruction"]["exit"]["catastrophic_stop_loss_pct"] = "0.15"
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 19, 59, tzinfo=timezone.utc),
            payload=payload,
            broker_order_id=11,
            exit_order_id=41,
            entry_filled_quantity="1",
            entry_avg_fill_price="200.00",
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
            session.add_all(
                [
                    BrokerOrderRecord(
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="DU1234567",
                        order_role="EXIT",
                        external_order_id="41",
                        external_perm_id="9041",
                        external_client_id="0",
                        order_ref="runtime-aapl-1:exit:catastrophic_stop",
                        symbol="AAPL",
                        exchange="SMART",
                        currency="USD",
                        security_type="STK",
                        primary_exchange="NASDAQ",
                        local_symbol="AAPL",
                        side="SELL",
                        order_type="STP",
                        time_in_force="DAY",
                        status="PreSubmitted",
                        total_quantity="1",
                        limit_price=None,
                        stop_price="170.00",
                        submitted_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    ),
                    BrokerOrderRecord(
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="DU1234567",
                        order_role="EXIT",
                        external_order_id="42",
                        external_perm_id="9042",
                        external_client_id="0",
                        order_ref="runtime-aapl-1:exit:take_profit",
                        symbol="AAPL",
                        exchange="SMART",
                        currency="USD",
                        security_type="STK",
                        primary_exchange="NASDAQ",
                        local_symbol="AAPL",
                        side="SELL",
                        order_type="LMT",
                        time_in_force="DAY",
                        status="Submitted",
                        total_quantity="1",
                        limit_price="204.00",
                        stop_price=None,
                        submitted_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    ),
                ]
            )
            session.commit()
        finally:
            session.close()

        calls: list[str] = []

        def fake_exit_submitter(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            quantity: Decimal,
            order_type: object,
            order_ref: str,
            timeout: int = 10,
            limit_price: Decimal | None = None,
            stop_price: Decimal | None = None,
            oca_group: str | None = None,
            oca_type: int | None = None,
        ) -> dict[str, object]:
            del broker_config, instruction, timeout
            calls.append(order_ref)
            order_id = 51 if order_ref.endswith("catastrophic_stop") else 52
            order_type_code = "STP" if order_ref.endswith("catastrophic_stop") else "LMT"
            return {
                "instruction_id": "runtime-aapl-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {
                    "con_id": 265598,
                    "symbol": "AAPL",
                    "security_type": "STK",
                    "exchange": "SMART",
                    "currency": "USD",
                },
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": order_type_code,
                    "time_in_force": "DAY",
                    "limit_price": str(limit_price) if limit_price is not None else None,
                    "stop_price": str(stop_price) if stop_price is not None else None,
                    "total_quantity": str(quantity),
                    "outside_rth": False,
                    "oca_group": oca_group,
                    "oca_type": oca_type,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": order_id,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": str(quantity),
                    "avgFillPrice": 0.0,
                    "permId": 9050 + order_id,
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
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(
            calls,
            [
                "runtime-aapl-1:exit:catastrophic_stop",
                "runtime-aapl-1:exit:take_profit",
            ],
        )
        self.assertEqual(len(result.submitted_exits), 2)
        session = self.session_factory()
        try:
            statuses_by_order_id = {
                row.external_order_id: row.status
                for row in session.execute(select(BrokerOrderRecord)).scalars()
            }
            self.assertEqual(statuses_by_order_id["41"], "NOT_FOUND_AT_BROKER")
            self.assertEqual(statuses_by_order_id["42"], "NOT_FOUND_AT_BROKER")
            self.assertEqual(statuses_by_order_id["51"], "Submitted")
            self.assertEqual(statuses_by_order_id["52"], "Submitted")
        finally:
            session.close()
