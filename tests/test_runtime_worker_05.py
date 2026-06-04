from __future__ import annotations

from tests._runtime_worker_shared import *  # noqa: F401,F403


class RuntimeWorkerTests05(RuntimeWorkerTestCase):
    def test_run_runtime_cycle_blocks_repair_when_duplicate_exit_refs_are_active(
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
                        external_order_id="43",
                        external_perm_id="9043",
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
                        status="PreSubmitted",
                        total_quantity="1",
                        limit_price="204.00",
                        submitted_at=datetime(2026, 4, 10, 20, 1, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 10, 20, 1, tzinfo=timezone.utc),
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
            **kwargs: object,
        ) -> dict[str, object]:
            del broker_config, instruction
            calls.append(str(kwargs["order_ref"]))
            raise AssertionError("duplicate active exits must block repair submits")

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 20, 5, tzinfo=timezone.utc),
            exit_submitter=fake_exit_submitter,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders=_duplicate_take_profit_open_orders(),
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(calls, [])
        self.assertEqual(result.submitted_exits, ())
        session = self.session_factory()
        try:
            event_type = session.execute(
                select(InstructionEventRecord.event_type).where(
                    InstructionEventRecord.event_type
                    == "protective_exit_duplicate_blocked"
                )
            ).scalar_one()
            self.assertEqual(event_type, "protective_exit_duplicate_blocked")
        finally:
            session.close()

    def test_run_runtime_cycle_can_use_persisted_order_status_fill_without_executions(self) -> None:
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
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="DU1234567",
                base_currency="USD",
            )
            session.add(broker_account)
            session.flush()
            instruction_record = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-aapl-1"
                )
            ).scalar_one()
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
                    status="Filled",
                    total_quantity="1",
                    limit_price="200.00",
                    stop_price=None,
                    submitted_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={
                        "last_order_status_callback": {
                            "orderId": 11,
                            "status": "Filled",
                            "filled": "1",
                            "remaining": "0",
                            "avgFillPrice": "200.00",
                            "permId": 8001,
                            "parentId": 0,
                            "lastFillPrice": "200.00",
                            "clientId": 0,
                            "whyHeld": "",
                            "mktCapPrice": "0.0",
                        }
                    },
                )
            )
            session.commit()
        finally:
            session.close()

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
            return {
                "contract": {"symbol": "AAPL", "exchange": "SMART", "currency": "USD"},
                "order": {
                    "order_id": 21,
                    "order_ref": order_ref,
                    "order_type": "LMT",
                    "action": "SELL",
                    "time_in_force": "DAY",
                    "limit_price": "204.00",
                    "total_quantity": str(quantity),
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 21,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": str(quantity),
                    "avgFillPrice": 0.0,
                    "permId": 9001,
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

        self.assertEqual(len(result.filled_entries), 1)
        self.assertEqual(len(result.submitted_exits), 1)
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.entry_filled_quantity, "1")
        self.assertEqual(record.entry_avg_fill_price, "200.00")

    def test_run_runtime_cycle_uses_order_status_fill_when_snapshot_times_out(self) -> None:
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
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="DU1234567",
                base_currency="USD",
            )
            session.add(broker_account)
            session.flush()
            instruction_record = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-aapl-1"
                )
            ).scalar_one()
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
                    status="Filled",
                    total_quantity="1",
                    limit_price="200.00",
                    stop_price=None,
                    submitted_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 10, 20, 0, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={
                        "last_order_status_callback": {
                            "orderId": 11,
                            "status": "Filled",
                            "filled": "1",
                            "remaining": "0",
                            "avgFillPrice": "200.00",
                            "permId": 8001,
                            "parentId": 0,
                            "lastFillPrice": "200.00",
                            "clientId": 0,
                            "whyHeld": "",
                            "mktCapPrice": "0.0",
                        }
                    },
                )
            )
            session.commit()
        finally:
            session.close()

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
            del broker_config, instruction, order_type, timeout
            del limit_price, stop_price, oca_group, oca_type
            return {
                "contract": {"symbol": "AAPL", "exchange": "SMART", "currency": "USD"},
                "order": {
                    "order_id": 21,
                    "order_ref": order_ref,
                    "order_type": "LMT",
                    "action": "SELL",
                    "time_in_force": "DAY",
                    "limit_price": "204.00",
                    "total_quantity": str(quantity),
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 21,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": str(quantity),
                    "avgFillPrice": 0.0,
                    "permId": 9001,
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
            broker_snapshot_fetcher=lambda *args, **kwargs: (_ for _ in ()).throw(
                TimeoutError("executions timed out")
            ),
        )

        self.assertTrue(any(issue.stage == "broker_snapshot" for issue in result.issues))
        self.assertEqual(len(result.filled_entries), 1)
        self.assertEqual(len(result.submitted_exits), 1)
        record = self._read_record("runtime-aapl-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.entry_filled_quantity, "1")
        self.assertEqual(record.entry_avg_fill_price, "200.00")

    def test_run_runtime_cycle_uses_order_status_time_for_execution_missing_time(self) -> None:
        payload = _aapl_payload()
        self._insert_instruction(
            instruction_id="runtime-aapl-1",
            symbol="AAPL",
            exchange="SMART",
            currency="USD",
            state=ExecutionState.ENTRY_SUBMITTED.value,
            submit_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 20, 10, tzinfo=timezone.utc),
            payload=payload,
            broker_order_id=11,
        )
        status_fill_at = datetime(2026, 4, 10, 20, 0, 44, tzinfo=timezone.utc)

        session = self.session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="DU1234567",
                base_currency="USD",
            )
            session.add(broker_account)
            session.flush()
            instruction_record = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-aapl-1"
                )
            ).scalar_one()
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
                    status="Filled",
                    total_quantity="1",
                    limit_price="200.00",
                    stop_price=None,
                    submitted_at=datetime(2026, 4, 10, 19, 55, tzinfo=timezone.utc),
                    last_status_at=status_fill_at,
                    raw_payload={},
                    metadata_json={
                        "last_order_status_callback": {
                            "orderId": 11,
                            "status": "Filled",
                            "filled": "1",
                            "remaining": "0",
                            "avgFillPrice": "200.00",
                            "permId": 8001,
                            "parentId": 0,
                            "lastFillPrice": "200.00",
                            "clientId": 0,
                            "whyHeld": "",
                            "mktCapPrice": "0.0",
                        }
                    },
                )
            )
            session.commit()
        finally:
            session.close()

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
            del broker_config, instruction, order_type, timeout
            del limit_price, stop_price, oca_group, oca_type
            return {
                "contract": {"symbol": "AAPL", "exchange": "SMART", "currency": "USD"},
                "order": {
                    "order_id": 21,
                    "order_ref": order_ref,
                    "order_type": "LMT",
                    "action": "SELL",
                    "time_in_force": "DAY",
                    "limit_price": "204.00",
                    "total_quantity": str(quantity),
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 21,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": str(quantity),
                    "avgFillPrice": 0.0,
                    "permId": 9001,
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
                        exec_id="missing-time-exec",
                        order_id=11,
                        perm_id=8001,
                        client_id=0,
                        order_ref="runtime-aapl-1",
                        side="BOT",
                        shares=Decimal("1"),
                        price=Decimal("200.00"),
                        exchange="NASDAQ",
                        executed_at=None,
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
        self.assertEqual(
            record.entry_filled_at.replace(tzinfo=timezone.utc),
            status_fill_at,
        )
        session = self.session_factory()
        try:
            execution_fill = session.execute(select(ExecutionFillRecord)).scalar_one()
            self.assertEqual(
                execution_fill.executed_at.replace(tzinfo=timezone.utc),
                status_fill_at,
            )
            self.assertEqual(
                execution_fill.raw_payload[
                    "executed_at_inferred_from_order_status_callback"
                ],
                True,
            )
        finally:
            session.close()

    def test_run_runtime_cycle_submits_delayed_market_anchored_limit_exit(self) -> None:
        payload = _sive_payload()
        payload["instruction"]["exit"] = {
            "delayed_limit": {
                "submit_at": "2026-04-10T10:30:00+02:00",
                "limit_offset_pct": "0.05",
            }
        }
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.POSITION_OPEN.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            entry_filled_quantity="1",
        )

        market_price_calls: list[dict[str, object]] = []

        def fake_market_price_reader(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            at: datetime,
            timeout: int = 10,
        ) -> dict[str, object]:
            market_price_calls.append({"at": at, "timeout": timeout})
            return {
                "price": "20.00",
                "observed_at": "20260410 10:29:00",
                "currency": "SEK",
                "source": "test_latest_trade_price",
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
            self.assertEqual(order_ref, "runtime-sive-1:exit:delayed_limit")
            self.assertEqual(order_type, OrderType.LIMIT)
            self.assertEqual(str(quantity), "1")
            self.assertEqual(limit_price, Decimal("21.00"))
            self.assertIsNone(stop_price)
            return {
                "instruction_id": "runtime-sive-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 489000, "symbol": "SIVE"},
                "order": {
                    "order_ref": order_ref,
                    "action": "SELL",
                    "order_type": "LMT",
                    "time_in_force": "DAY",
                    "limit_price": str(limit_price),
                    "total_quantity": "1",
                    "outside_rth": False,
                    "transmit": True,
                },
                "broker_order_status": {
                    "orderId": 41,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": "1",
                    "avgFillPrice": 0.0,
                    "permId": 9141,
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
            now=datetime(2026, 4, 10, 8, 30, tzinfo=timezone.utc),
            exit_submitter=fake_exit_submitter,
            market_price_reader=fake_market_price_reader,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(len(market_price_calls), 1)
        self.assertEqual(len(result.submitted_exits), 1)
        record = self._read_record("runtime-sive-1")
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
        self.assertEqual(record.exit_order_id, 41)
        self.assertEqual(record.exit_submitted_quantity, "1")

    def test_run_runtime_cycle_does_not_resubmit_delayed_exit_when_ledger_has_open_exit(self) -> None:
        payload = _sive_payload()
        payload["instruction"]["exit"] = {
            "delayed_limit": {
                "submit_at": "2026-04-10T10:30:00+02:00",
                "limit_offset_pct": "0.05",
            }
        }
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            entry_filled_quantity="1",
            exit_order_id=41,
        )

        session = self.session_factory()
        try:
            instruction_record = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "runtime-sive-1"
                )
            ).scalar_one()
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="DU1234567",
                base_currency="SEK",
            )
            session.add(broker_account)
            session.flush()
            session.add(
                BrokerOrderRecord(
                    instruction_id=instruction_record.id,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="DU1234567",
                    order_role="EXIT",
                    external_order_id="41",
                    external_perm_id="9141",
                    external_client_id="0",
                    order_ref="runtime-sive-1:exit:delayed_limit",
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
                    total_quantity="1",
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

        market_price_calls: list[dict[str, object]] = []
        exit_submit_calls: list[dict[str, object]] = []

        def fake_market_price_reader(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            at: datetime,
            timeout: int = 10,
        ) -> dict[str, object]:
            market_price_calls.append({"at": at, "timeout": timeout})
            return {
                "price": "20.00",
                "observed_at": "20260410 10:29:00",
                "currency": "SEK",
                "source": "test_latest_trade_price",
            }

        def fake_exit_submitter(**kwargs: object) -> dict[str, object]:
            exit_submit_calls.append(kwargs)
            raise AssertionError("Delayed exit should not be resubmitted when a persisted open exit exists.")

        result = run_runtime_cycle(
            self.session_factory,
            self.config,
            runtime_timezone="Europe/Stockholm",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            now=datetime(2026, 4, 10, 8, 30, tzinfo=timezone.utc),
            exit_submitter=fake_exit_submitter,
            market_price_reader=fake_market_price_reader,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders=_delayed_limit_open_orders(),
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
        )

        self.assertEqual(market_price_calls, [])
        self.assertEqual(exit_submit_calls, [])
        self.assertEqual(len(result.submitted_exits), 0)
        record = self._read_record("runtime-sive-1")
        self.assertEqual(record.exit_order_id, 41)
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)

    def test_run_runtime_cycle_cancels_obsolete_protective_before_delayed_exit(self) -> None:
        payload = _sive_payload()
        payload["instruction"]["exit"] = {
            "delayed_limit": {
                "submit_at": "2026-04-10T10:30:00+02:00",
                "limit_offset_pct": "0.05",
            }
        }
        self._insert_instruction(
            instruction_id="runtime-sive-1",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            state=ExecutionState.EXIT_PENDING.value,
            submit_at=datetime(2026, 4, 10, 7, 25, tzinfo=timezone.utc),
            expire_at=datetime(2026, 4, 10, 15, 30, tzinfo=timezone.utc),
            payload=payload,
            entry_filled_quantity="1",
            exit_order_id=21,
        )
        self._insert_exit_broker_order(
            instruction_id="runtime-sive-1",
            external_order_id="21",
            order_ref="runtime-sive-1:exit:take_profit",
            symbol="SIVE",
            currency="SEK",
            limit_price="21.00",
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

        def fake_market_price_reader(
            broker_config: IbkrConnectionConfig,
            instruction: object,
            *,
            at: datetime,
            timeout: int = 10,
        ) -> dict[str, object]:
            del broker_config, instruction, at, timeout
            operations.append("market")
            return {
                "price": "20.00",
                "observed_at": "20260410 10:29:00",
                "currency": "SEK",
                "source": "test_latest_trade_price",
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
            del broker_config, instruction, timeout, order_type, stop_price, oca_group, oca_type
            operations.append(f"submit:{order_ref}")
            return {
                "instruction_id": "runtime-sive-1",
                "account": "DU1234567",
                "warnings": [],
                "resolved_contract": {"con_id": 489000, "symbol": "SIVE"},
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
                    "orderId": 41,
                    "status": "Submitted",
                    "filled": "0",
                    "remaining": str(quantity),
                    "avgFillPrice": 0.0,
                    "permId": 9141,
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
            now=datetime(2026, 4, 10, 8, 30, tzinfo=timezone.utc),
            exit_submitter=fake_exit_submitter,
            market_price_reader=fake_market_price_reader,
            broker_order_canceler=fake_canceler,
            broker_snapshot_fetcher=lambda *args, **kwargs: BrokerRuntimeSnapshot(
                open_orders={
                    21: BrokerOpenOrder(
                        order_id=21,
                        perm_id=9021,
                        client_id=0,
                        status="Submitted",
                        order_ref="runtime-sive-1:exit:take_profit",
                        action="SELL",
                        total_quantity=Decimal("1"),
                        symbol="SIVE",
                        account="DU1234567",
                        security_type="STK",
                        exchange="SMART",
                        primary_exchange="SFB",
                        currency="SEK",
                        local_symbol="SIVE",
                        order_type="LMT",
                        limit_price=Decimal("21.00"),
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
            ["cancel:21", "market", "submit:runtime-sive-1:exit:delayed_limit"],
        )
        self.assertEqual(len(result.submitted_exits), 1)
        record = self._read_record("runtime-sive-1")
        self.assertEqual(record.exit_order_id, 41)
        self.assertEqual(record.state, ExecutionState.EXIT_PENDING.value)
