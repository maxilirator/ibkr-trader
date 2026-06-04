from __future__ import annotations

from tests._ledger_persistence_shared import *  # noqa: F401,F403


class BrokerLedgerPersistenceTests02(BrokerLedgerPersistenceTestCase):
    def test_persist_broker_runtime_snapshot_relinks_order_ref_to_current_instruction(
        self,
    ) -> None:
        self._insert_instruction()
        session = self.session_factory()
        try:
            old_instruction = InstructionRecord(
                instruction_id="old-aapl-1",
                schema_version="2026-04-10",
                source_system="q-training",
                batch_id="old-batch",
                account_key="DU1234567",
                book_key="long_risk_book",
                symbol="AAPL",
                exchange="SMART",
                currency="USD",
                state=ExecutionState.COMPLETED.value,
                submit_at=datetime(2026, 4, 18, 19, 55, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 18, 19, 59, tzinfo=timezone.utc),
                order_type="LIMIT",
                side="BUY",
                payload={"instruction": {"instruction_id": "old-aapl-1"}},
            )
            session.add(old_instruction)
            broker_account = BrokerAccountRecord(
                broker_kind=BROKER_KIND_IBKR,
                account_key="DU1234567",
                base_currency="USD",
            )
            session.add(broker_account)
            session.flush()
            session.add(
                BrokerOrderRecord(
                    instruction_id=old_instruction.id,
                    broker_account_id=broker_account.id,
                    broker_kind=BROKER_KIND_IBKR,
                    account_key="DU1234567",
                    order_role="ENTRY",
                    external_order_id="18",
                    external_perm_id="9002",
                    external_client_id="0",
                    order_ref="persisted-aapl-1",
                    symbol="AAPL",
                    exchange="SMART",
                    currency="USD",
                    security_type="STK",
                    side="BUY",
                    order_type="LMT",
                    status="PreSubmitted",
                    raw_payload={},
                    metadata_json={},
                )
            )
            session.commit()
        finally:
            session.close()

        persist_broker_runtime_snapshot(
            self.session_factory,
            BrokerRuntimeSnapshot(
                open_orders={
                    18: BrokerOpenOrder(
                        order_id=18,
                        perm_id=9002,
                        client_id=0,
                        status="Submitted",
                        order_ref="persisted-aapl-1",
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
                    )
                },
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
            broker_kind=BROKER_KIND_IBKR,
            captured_at=datetime(2026, 4, 19, 8, 30, tzinfo=timezone.utc),
            default_account_key="DU1234567",
        )

        session = self.session_factory()
        try:
            broker_order = session.execute(
                select(BrokerOrderRecord).where(
                    BrokerOrderRecord.external_order_id == "18"
                )
            ).scalar_one()
            current_instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "persisted-aapl-1"
                )
            ).scalar_one()

            self.assertEqual(broker_order.status, "Submitted")
            self.assertEqual(broker_order.instruction_id, current_instruction.id)
        finally:
            session.close()

    def test_persist_broker_runtime_snapshot_reclassifies_manual_sell_as_active_exit(
        self,
    ) -> None:
        session = self.session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind=BROKER_KIND_IBKR,
                account_key="U25245596",
                base_currency="SEK",
            )
            session.add(broker_account)
            session.flush()
            instruction = InstructionRecord(
                instruction_id="nibe-long-1",
                schema_version="2026-04-10",
                source_system="q-training",
                batch_id="batch-1",
                account_key="U25245596",
                book_key="long_risk_book",
                symbol="NIBE B",
                exchange="SMART",
                currency="SEK",
                state=ExecutionState.EXIT_PENDING.value,
                submit_at=datetime(2026, 5, 7, 7, 25, tzinfo=timezone.utc),
                expire_at=datetime(2026, 5, 7, 15, 30, tzinfo=timezone.utc),
                order_type="LIMIT",
                side="BUY",
                entry_filled_quantity="430",
                payload={
                    "instruction": {
                        "instruction_id": "nibe-long-1",
                        "instrument": {
                            "symbol": "NIBE B",
                            "local_symbol": "NIBE B",
                            "security_type": "STK",
                            "currency": "SEK",
                        },
                    }
                },
            )
            session.add(instruction)
            session.flush()
            session.add(
                BrokerOrderRecord(
                    instruction_id=None,
                    broker_account_id=broker_account.id,
                    broker_kind=BROKER_KIND_IBKR,
                    account_key="U25245596",
                    order_role="ENTRY",
                    external_order_id="1",
                    external_perm_id="1931699017",
                    external_client_id="13",
                    order_ref="manual-nibe-close-open",
                    symbol="NIBE.B",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    primary_exchange="SFB",
                    local_symbol="NIBE B",
                    side="SELL",
                    order_type="MKT",
                    status="PreSubmitted",
                    total_quantity="430",
                    raw_payload={},
                    metadata_json={},
                )
            )
            session.commit()
        finally:
            session.close()

        persist_broker_runtime_snapshot(
            self.session_factory,
            BrokerRuntimeSnapshot(
                open_orders={},
                executions=(
                    BrokerExecution(
                        exec_id="manual-nibe-exec-1",
                        order_id=1,
                        perm_id=1931699017,
                        client_id=13,
                        order_ref="manual-nibe-close-open",
                        side="SLD",
                        shares=Decimal("430"),
                        price=Decimal("43.61"),
                        exchange="SFB",
                        executed_at=datetime(2026, 5, 8, 7, 0, tzinfo=timezone.utc),
                        symbol="NIBE.B",
                        account="U25245596",
                        security_type="STK",
                        primary_exchange="SFB",
                        currency="SEK",
                        local_symbol="NIBE B",
                    ),
                ),
                portfolio=(),
                positions=(),
                account_values={},
            ),
            broker_kind=BROKER_KIND_IBKR,
            captured_at=datetime(2026, 5, 8, 7, 1, tzinfo=timezone.utc),
            default_account_key="U25245596",
        )

        session = self.session_factory()
        try:
            instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "nibe-long-1"
                )
            ).scalar_one()
            broker_order = session.execute(
                select(BrokerOrderRecord).where(
                    BrokerOrderRecord.order_ref == "manual-nibe-close-open"
                )
            ).scalar_one()
            execution_fill = session.execute(select(ExecutionFillRecord)).scalar_one()

            self.assertEqual(broker_order.instruction_id, instruction.id)
            self.assertEqual(broker_order.order_role, "EXIT")
            self.assertEqual(broker_order.status, "FILLED")
            self.assertEqual(execution_fill.instruction_id, instruction.id)
            self.assertEqual(execution_fill.broker_order_id, broker_order.id)
        finally:
            session.close()

    def test_persist_broker_runtime_snapshot_raises_for_missing_execution_account(self) -> None:
        snapshot = BrokerRuntimeSnapshot(
            open_orders={},
            executions=(
                BrokerExecution(
                    exec_id="missing-account-exec",
                    order_id=17,
                    perm_id=9001,
                    client_id=0,
                    order_ref="persisted-aapl-1",
                    side="BOT",
                    shares=Decimal("1"),
                    price=Decimal("200.00"),
                    exchange="NASDAQ",
                    executed_at=datetime(2026, 4, 19, 8, 31, tzinfo=timezone.utc),
                    symbol="AAPL",
                    account=None,
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

        with self.assertRaisesRegex(ValueError, "did not include a broker account"):
            persist_broker_runtime_snapshot(
                self.session_factory,
                snapshot,
                broker_kind=BROKER_KIND_IBKR,
                captured_at=datetime(2026, 4, 19, 8, 30, tzinfo=timezone.utc),
            )

    def test_missing_execution_time_uses_matching_order_status_fill_time(self) -> None:
        self._insert_instruction()
        self._insert_broker_order()
        status_fill_at = datetime(2026, 4, 19, 8, 26, 44, tzinfo=timezone.utc)
        captured_at = datetime(2026, 4, 19, 13, 28, 56, tzinfo=timezone.utc)
        session = self.session_factory()
        try:
            broker_order = session.execute(
                select(BrokerOrderRecord).where(
                    BrokerOrderRecord.external_order_id == "11"
                )
            ).scalar_one()
            broker_order.status = "Filled"
            broker_order.last_status_at = status_fill_at
            broker_order.metadata_json = {
                "last_order_status_callback": {
                    "orderId": 11,
                    "status": "Filled",
                    "filled": "1",
                    "remaining": "0",
                    "avgFillPrice": "200.00",
                    "permId": 9001,
                    "parentId": 0,
                    "lastFillPrice": "200.00",
                    "clientId": 0,
                    "whyHeld": "",
                    "mktCapPrice": "0.0",
                }
            }
            session.commit()
        finally:
            session.close()

        persist_broker_runtime_snapshot(
            self.session_factory,
            BrokerRuntimeSnapshot(
                open_orders={},
                executions=(
                    BrokerExecution(
                        exec_id="missing-time-exec",
                        order_id=11,
                        perm_id=9001,
                        client_id=0,
                        order_ref="persisted-aapl-1",
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
            broker_kind=BROKER_KIND_IBKR,
            captured_at=captured_at,
            default_account_key="DU1234567",
        )

        session = self.session_factory()
        try:
            broker_order = session.execute(
                select(BrokerOrderRecord).where(
                    BrokerOrderRecord.external_order_id == "11"
                )
            ).scalar_one()
            execution_fill = session.execute(select(ExecutionFillRecord)).scalar_one()
            broker_order_event = session.execute(
                select(BrokerOrderEventRecord).where(
                    BrokerOrderEventRecord.event_type == "execution_fill_observed"
                )
            ).scalar_one()
            self.assertEqual(
                execution_fill.executed_at.replace(tzinfo=timezone.utc),
                status_fill_at,
            )
            self.assertEqual(
                broker_order.last_status_at.replace(tzinfo=timezone.utc),
                status_fill_at,
            )
            self.assertEqual(
                broker_order_event.event_at.replace(tzinfo=timezone.utc),
                status_fill_at,
            )
            self.assertEqual(
                execution_fill.raw_payload[
                    "executed_at_inferred_from_order_status_callback"
                ],
                True,
            )
            self.assertEqual(
                execution_fill.raw_payload["order_status_callback_at"],
                status_fill_at.isoformat(),
            )
            self.assertNotIn(
                "executed_at_inferred_from_snapshot_capture",
                execution_fill.raw_payload,
            )
        finally:
            session.close()

    def test_persist_broker_runtime_snapshot_uses_capture_time_when_execution_time_is_missing(self) -> None:
        self._insert_instruction()
        captured_at = datetime(2026, 4, 19, 8, 30, tzinfo=timezone.utc)
        snapshot = BrokerRuntimeSnapshot(
            open_orders={},
            executions=(
                BrokerExecution(
                    exec_id="missing-time-exec",
                    order_id=17,
                    perm_id=9001,
                    client_id=0,
                    order_ref="persisted-aapl-1",
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
        )

        persist_broker_runtime_snapshot(
            self.session_factory,
            snapshot,
            broker_kind=BROKER_KIND_IBKR,
            captured_at=captured_at,
            default_account_key="DU1234567",
        )

        session = self.session_factory()
        try:
            execution_fill = session.execute(select(ExecutionFillRecord)).scalar_one()
            self.assertEqual(
                execution_fill.executed_at.replace(tzinfo=timezone.utc),
                captured_at,
            )
            self.assertEqual(
                execution_fill.raw_payload["executed_at_inferred_from_snapshot_capture"],
                True,
            )
        finally:
            session.close()

    def test_persist_broker_runtime_snapshot_updates_existing_fill_when_commission_arrives_later(self) -> None:
        self._insert_instruction()
        captured_at = datetime(2026, 4, 19, 8, 30, tzinfo=timezone.utc)
        snapshot_without_commission = BrokerRuntimeSnapshot(
            open_orders={},
            executions=(
                BrokerExecution(
                    exec_id="late-commission-exec",
                    order_id=17,
                    perm_id=9001,
                    client_id=0,
                    order_ref="persisted-aapl-1",
                    side="BOT",
                    shares=Decimal("1"),
                    price=Decimal("200.00"),
                    exchange="NASDAQ",
                    executed_at=datetime(2026, 4, 19, 8, 31, tzinfo=timezone.utc),
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

        persist_broker_runtime_snapshot(
            self.session_factory,
            snapshot_without_commission,
            broker_kind=BROKER_KIND_IBKR,
            captured_at=captured_at,
            default_account_key="DU1234567",
        )

        snapshot_with_commission = BrokerRuntimeSnapshot(
            open_orders={},
            executions=(
                BrokerExecution(
                    exec_id="late-commission-exec",
                    order_id=17,
                    perm_id=9001,
                    client_id=0,
                    order_ref="persisted-aapl-1",
                    side="BOT",
                    shares=Decimal("1"),
                    price=Decimal("200.00"),
                    exchange="NASDAQ",
                    executed_at=datetime(2026, 4, 19, 8, 31, tzinfo=timezone.utc),
                    symbol="AAPL",
                    account="DU1234567",
                    security_type="STK",
                    primary_exchange="NASDAQ",
                    currency="USD",
                    local_symbol="AAPL",
                    commission=Decimal("1.10"),
                    commission_currency="USD",
                ),
            ),
            portfolio=(),
            positions=(),
            account_values={},
        )

        persist_broker_runtime_snapshot(
            self.session_factory,
            snapshot_with_commission,
            broker_kind=BROKER_KIND_IBKR,
            captured_at=captured_at,
            default_account_key="DU1234567",
        )

        session = self.session_factory()
        try:
            execution_fill = session.execute(select(ExecutionFillRecord)).scalar_one()
            self.assertEqual(execution_fill.commission, "1.10")
            self.assertEqual(execution_fill.commission_currency, "USD")
            self.assertEqual(execution_fill.raw_payload["commission"], "1.10")
        finally:
            session.close()

    def test_persist_broker_runtime_snapshot_merges_duplicate_portfolio_rows(self) -> None:
        captured_at = datetime(2026, 4, 21, 14, 31, tzinfo=timezone.utc)
        snapshot = BrokerRuntimeSnapshot(
            open_orders={},
            executions=(),
            portfolio=(
                BrokerPortfolioItem(
                    account="DU1234567",
                    symbol="SIVE",
                    local_symbol="SIVE",
                    security_type="STK",
                    exchange="SFB",
                    primary_exchange="SFB",
                    currency="SEK",
                    position=Decimal("1"),
                    market_price=Decimal("29.70"),
                    market_value=Decimal("29.70"),
                    average_cost=Decimal("29.72"),
                    unrealized_pnl=Decimal("-0.02"),
                    realized_pnl=Decimal("0"),
                ),
                BrokerPortfolioItem(
                    account="DU1234567",
                    symbol="SIVE",
                    local_symbol="SIVE",
                    security_type="STK",
                    exchange="SFB",
                    primary_exchange="SFB",
                    currency="SEK",
                    position=Decimal("1"),
                    market_price=Decimal("29.71"),
                    market_value=Decimal("29.71"),
                    average_cost=Decimal("29.72"),
                    unrealized_pnl=Decimal("-0.01"),
                    realized_pnl=Decimal("0"),
                ),
            ),
            positions=(
                BrokerPosition(
                    account="DU1234567",
                    symbol="SIVE",
                    local_symbol="SIVE",
                    security_type="STK",
                    exchange="SFB",
                    primary_exchange="SFB",
                    currency="SEK",
                    position=Decimal("1"),
                    average_cost=Decimal("29.72"),
                ),
            ),
            account_values={
                "DU1234567": {
                    "NetLiquidation": {"value": "19324.51", "currency": "SEK"},
                }
            },
        )

        persist_broker_runtime_snapshot(
            self.session_factory,
            snapshot,
            broker_kind=BROKER_KIND_IBKR,
            captured_at=captured_at,
            default_account_key="DU1234567",
        )

        session = self.session_factory()
        try:
            position_snapshots = session.execute(select(PositionSnapshotRecord)).scalars().all()
            self.assertEqual(len(position_snapshots), 1)
            self.assertEqual(position_snapshots[0].symbol, "SIVE")
            self.assertEqual(position_snapshots[0].market_price, "29.71")
        finally:
            session.close()

    def test_persist_broker_callback_events_updates_order_status_and_rejects(self) -> None:
        self._insert_broker_order()

        persist_broker_callback_events(
            self.session_factory,
            [
                {
                    "event_type": "order_status",
                    "event_at": datetime(2026, 4, 19, 8, 31, tzinfo=timezone.utc),
                    "order_status": {
                        "orderId": 11,
                        "status": "Submitted",
                        "filled": "0",
                        "remaining": "1",
                        "avgFillPrice": "0.0",
                        "permId": 9001,
                        "parentId": 0,
                        "lastFillPrice": "0.0",
                        "clientId": 0,
                        "whyHeld": "",
                        "mktCapPrice": "0.0",
                    },
                },
                {
                    "event_type": "order_error",
                    "event_at": datetime(2026, 4, 19, 8, 32, tzinfo=timezone.utc),
                    "error": {
                        "orderId": 11,
                        "errorTime": 0,
                        "errorCode": 202,
                        "errorString": "Rejected by exchange",
                        "advancedOrderRejectJson": '{"reason":"test"}',
                    },
                },
            ],
            broker_kind=BROKER_KIND_IBKR,
            default_account_key="DU1234567",
        )

        session = self.session_factory()
        try:
            broker_order = session.execute(select(BrokerOrderRecord)).scalar_one()
            broker_order_events = session.execute(
                select(BrokerOrderEventRecord).order_by(BrokerOrderEventRecord.id)
            ).scalars().all()

            self.assertEqual(broker_order.status, "Submitted")
            self.assertEqual(
                [event.event_type for event in broker_order_events],
                ["order_status_callback", "order_error_callback"],
            )
            self.assertEqual(
                broker_order.metadata_json["last_order_error_callback"]["errorCode"],
                202,
            )
        finally:
            session.close()

    def test_persist_broker_callback_events_keeps_unmatched_order_error(
        self,
    ) -> None:
        persist_broker_callback_events(
            self.session_factory,
            [
                {
                    "event_type": "order_error",
                    "event_at": datetime(2026, 4, 19, 8, 32, tzinfo=timezone.utc),
                    "error": {
                        "orderId": 4833,
                        "errorTime": 0,
                        "errorCode": 401,
                        "errorString": "OCA Group",
                        "advancedOrderRejectJson": None,
                    },
                },
            ],
            broker_kind=BROKER_KIND_IBKR,
            default_account_key="U25245596",
        )

        session = self.session_factory()
        try:
            broker_order = session.execute(select(BrokerOrderRecord)).scalar_one()
            broker_order_events = session.execute(
                select(BrokerOrderEventRecord).order_by(BrokerOrderEventRecord.id)
            ).scalars().all()

            self.assertEqual(broker_order.account_key, "U25245596")
            self.assertEqual(broker_order.order_role, "BROKER_NATIVE")
            self.assertEqual(broker_order.external_order_id, "4833")
            self.assertEqual(broker_order.status, "ERROR")
            self.assertTrue(broker_order.metadata_json["unmatched_callback"])
            self.assertEqual(
                [event.event_type for event in broker_order_events],
                ["order_error_callback"],
            )
            self.assertEqual(broker_order_events[0].payload["errorCode"], 401)
        finally:
            session.close()

    def test_order_status_cancelled_callback_closes_unfilled_entry_instruction(
        self,
    ) -> None:
        self._insert_instruction()
        session = self.session_factory()
        try:
            instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "persisted-aapl-1"
                )
            ).scalar_one()
            broker_account = BrokerAccountRecord(
                broker_kind=BROKER_KIND_IBKR,
                account_key="DU1234567",
                base_currency="USD",
            )
            session.add(broker_account)
            session.flush()
            session.add(
                BrokerOrderRecord(
                    instruction_id=instruction.id,
                    broker_account_id=broker_account.id,
                    broker_kind=BROKER_KIND_IBKR,
                    account_key="DU1234567",
                    order_role="ENTRY",
                    external_order_id="11",
                    external_perm_id="9001",
                    external_client_id="0",
                    order_ref="persisted-aapl-1",
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
                    stop_price=None,
                    submitted_at=datetime(2026, 4, 19, 8, 30, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 19, 8, 30, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
            )
            session.commit()
        finally:
            session.close()

        persist_broker_callback_events(
            self.session_factory,
            [
                {
                    "event_type": "order_status",
                    "event_at": datetime(2026, 4, 19, 8, 32, tzinfo=timezone.utc),
                    "order_status": {
                        "orderId": 11,
                        "status": "Cancelled",
                        "filled": "0",
                        "remaining": "1",
                        "avgFillPrice": "0.0",
                        "permId": 9001,
                        "parentId": 0,
                        "lastFillPrice": "0.0",
                        "clientId": 0,
                        "whyHeld": "",
                        "mktCapPrice": "0.0",
                    },
                }
            ],
            broker_kind=BROKER_KIND_IBKR,
            default_account_key="DU1234567",
        )

        session = self.session_factory()
        try:
            instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "persisted-aapl-1"
                )
            ).scalar_one()
            instruction_events = session.execute(
                select(InstructionEventRecord)
                .where(InstructionEventRecord.instruction_id == instruction.id)
                .order_by(InstructionEventRecord.id)
            ).scalars().all()

            self.assertEqual(instruction.state, ExecutionState.ENTRY_CANCELLED.value)
            self.assertEqual(instruction.broker_order_status, "Cancelled")
            self.assertEqual(
                [event.event_type for event in instruction_events],
                ["entry_order_cancelled"],
            )
            self.assertEqual(
                instruction_events[0].state_before,
                ExecutionState.ENTRY_SUBMITTED.value,
            )
            self.assertEqual(
                instruction_events[0].state_after,
                ExecutionState.ENTRY_CANCELLED.value,
            )
            self.assertEqual(
                instruction_events[0].payload["broker_order_status"]["status"],
                "Cancelled",
            )
        finally:
            session.close()

    def test_order_status_callback_marks_entry_filled_from_broker_status(
        self,
    ) -> None:
        self._insert_instruction()
        session = self.session_factory()
        try:
            instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "persisted-aapl-1"
                )
            ).scalar_one()
            broker_account = BrokerAccountRecord(
                broker_kind=BROKER_KIND_IBKR,
                account_key="DU1234567",
                base_currency="USD",
            )
            session.add(broker_account)
            session.flush()
            session.add(
                BrokerOrderRecord(
                    instruction_id=instruction.id,
                    broker_account_id=broker_account.id,
                    broker_kind=BROKER_KIND_IBKR,
                    account_key="DU1234567",
                    order_role="ENTRY",
                    external_order_id="11",
                    external_perm_id="9001",
                    external_client_id="0",
                    order_ref="persisted-aapl-1",
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
                    stop_price=None,
                    submitted_at=datetime(2026, 4, 19, 8, 30, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 19, 8, 30, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
            )
            session.commit()
        finally:
            session.close()

        persist_broker_callback_events(
            self.session_factory,
            [
                {
                    "event_type": "order_status",
                    "event_at": datetime(2026, 4, 19, 8, 32, tzinfo=timezone.utc),
                    "order_status": {
                        "orderId": 11,
                        "status": "Filled",
                        "filled": "1",
                        "remaining": "0",
                        "avgFillPrice": "200.00",
                        "permId": 9001,
                        "parentId": 0,
                        "lastFillPrice": "200.00",
                        "clientId": 0,
                        "whyHeld": "",
                        "mktCapPrice": "0.0",
                    },
                }
            ],
            broker_kind=BROKER_KIND_IBKR,
            default_account_key="DU1234567",
        )

        session = self.session_factory()
        try:
            instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "persisted-aapl-1"
                )
            ).scalar_one()
            instruction_events = session.execute(
                select(InstructionEventRecord)
                .where(InstructionEventRecord.instruction_id == instruction.id)
                .order_by(InstructionEventRecord.id)
            ).scalars().all()
            fills = session.execute(select(ExecutionFillRecord)).scalars().all()

            self.assertEqual(instruction.state, ExecutionState.POSITION_OPEN.value)
            self.assertEqual(instruction.broker_order_status, "Filled")
            self.assertEqual(instruction.entry_filled_quantity, "1")
            self.assertEqual(instruction.entry_avg_fill_price, "200.00")
            self.assertEqual(
                instruction.entry_filled_at,
                datetime(2026, 4, 19, 8, 32),
            )
            self.assertEqual(
                [event.event_type for event in instruction_events],
                ["entry_order_filled"],
            )
            self.assertEqual(
                instruction_events[0].payload["evidence_source"],
                "broker_order_status",
            )
            self.assertEqual(fills, [])
        finally:
            session.close()
