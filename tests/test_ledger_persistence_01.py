from __future__ import annotations

from tests._ledger_persistence_shared import *  # noqa: F401,F403


class BrokerLedgerPersistenceTests01(BrokerLedgerPersistenceTestCase):
    def test_runtime_snapshot_does_not_mark_missing_open_orders_from_empty_sample(
        self,
    ) -> None:
        self._insert_broker_order()

        persist_broker_runtime_snapshot(
            self.session_factory,
            BrokerRuntimeSnapshot(
                open_orders={},
                executions=(),
                portfolio=(),
                positions=(),
                account_values={},
            ),
            broker_kind=BROKER_KIND_IBKR,
            captured_at=datetime(2026, 4, 19, 8, 32, tzinfo=timezone.utc),
            default_account_key="DU1234567",
            close_missing_open_orders=True,
        )

        session = self.session_factory()
        try:
            broker_order = session.execute(select(BrokerOrderRecord)).scalar_one()
            events = session.execute(
                select(BrokerOrderEventRecord).order_by(BrokerOrderEventRecord.id)
            ).scalars().all()

            self.assertEqual(broker_order.status, "PreSubmitted")
            self.assertEqual(broker_order.metadata_json, {})
            self.assertEqual(events, [])
        finally:
            session.close()

    def test_runtime_snapshot_marks_missing_open_orders_not_found_when_authoritative(
        self,
    ) -> None:
        self._insert_broker_order()

        persist_broker_runtime_snapshot(
            self.session_factory,
            BrokerRuntimeSnapshot(
                open_orders={
                    12: BrokerOpenOrder(
                        order_id=12,
                        perm_id=9002,
                        client_id=0,
                        status="PreSubmitted",
                        order_ref="other-aapl-2",
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
                        limit_price=Decimal("201.00"),
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
            broker_kind=BROKER_KIND_IBKR,
            captured_at=datetime(2026, 4, 19, 8, 32, tzinfo=timezone.utc),
            default_account_key="DU1234567",
            close_missing_open_orders=True,
        )

        session = self.session_factory()
        try:
            broker_order = session.execute(
                select(BrokerOrderRecord).where(BrokerOrderRecord.external_order_id == "11")
            ).scalar_one()
            events = session.execute(
                select(BrokerOrderEventRecord)
                .where(BrokerOrderEventRecord.broker_order_id == broker_order.id)
                .order_by(BrokerOrderEventRecord.id)
            ).scalars().all()

            self.assertEqual(broker_order.status, "NOT_FOUND_AT_BROKER")
            self.assertTrue(
                broker_order.metadata_json["missing_from_runtime_snapshot"]
            )
            self.assertEqual(len(events), 1)
            self.assertEqual(
                events[0].event_type,
                "open_order_missing_from_runtime_snapshot",
            )
            self.assertEqual(events[0].status_before, "PreSubmitted")
            self.assertEqual(events[0].status_after, "NOT_FOUND_AT_BROKER")
        finally:
            session.close()

    def test_runtime_snapshot_clears_missing_metadata_when_open_order_reappears(
        self,
    ) -> None:
        self._insert_broker_order()
        session = self.session_factory()
        try:
            broker_order = session.execute(select(BrokerOrderRecord)).scalar_one()
            broker_order.status = "NOT_FOUND_AT_BROKER"
            broker_order.metadata_json = {
                "missing_from_runtime_snapshot": True,
                "missing_from_runtime_snapshot_at": "2026-04-19T08:31:00+00:00",
                "missing_from_runtime_snapshot_account_scope": ["DU1234567"],
                "missing_from_runtime_snapshot_open_order_count": 0,
                "last_order_error_callback": {"errorCode": 201},
            }
            session.commit()
        finally:
            session.close()

        persist_broker_runtime_snapshot(
            self.session_factory,
            BrokerRuntimeSnapshot(
                open_orders={
                    11: BrokerOpenOrder(
                        order_id=11,
                        perm_id=9001,
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
            broker_kind=BROKER_KIND_IBKR,
            captured_at=datetime(2026, 4, 19, 8, 32, tzinfo=timezone.utc),
            default_account_key="DU1234567",
            close_missing_open_orders=True,
        )

        session = self.session_factory()
        try:
            broker_order = session.execute(select(BrokerOrderRecord)).scalar_one()

            self.assertEqual(broker_order.status, "Submitted")
            self.assertEqual(broker_order.external_perm_id, "9001")
            self.assertNotIn("missing_from_runtime_snapshot", broker_order.metadata_json)
            self.assertNotIn(
                "missing_from_runtime_snapshot_at",
                broker_order.metadata_json,
            )
            self.assertNotIn("last_order_error_callback", broker_order.metadata_json)
        finally:
            session.close()

    def test_runtime_snapshot_resets_submitted_at_when_order_id_is_reused(
        self,
    ) -> None:
        self._insert_broker_order()
        session = self.session_factory()
        try:
            broker_order = session.execute(select(BrokerOrderRecord)).scalar_one()
            broker_order.metadata_json = {
                "last_order_error_callback": {"errorCode": 201},
                "broker_submission": {"instruction_id": "old-instruction"},
            }
            session.commit()
        finally:
            session.close()

        captured_at = datetime(2026, 4, 19, 9, 15, tzinfo=timezone.utc)
        persist_broker_runtime_snapshot(
            self.session_factory,
            BrokerRuntimeSnapshot(
                open_orders={
                    11: BrokerOpenOrder(
                        order_id=11,
                        perm_id=9002,
                        client_id=0,
                        status="Submitted",
                        order_ref="new-aapl-2",
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
                        limit_price=Decimal("201.00"),
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
            broker_kind=BROKER_KIND_IBKR,
            captured_at=captured_at,
            default_account_key="DU1234567",
            close_missing_open_orders=True,
        )

        session = self.session_factory()
        try:
            broker_orders = session.execute(
                select(BrokerOrderRecord).order_by(BrokerOrderRecord.id.asc())
            ).scalars().all()

            self.assertEqual(len(broker_orders), 2)
            self.assertIsNone(broker_orders[0].external_order_id)
            self.assertIn("last_order_error_callback", broker_orders[0].metadata_json)
            self.assertIn("broker_submission", broker_orders[0].metadata_json)

            broker_order = broker_orders[1]
            self.assertEqual(broker_order.external_order_id, "11")
            self.assertEqual(broker_order.external_perm_id, "9002")
            self.assertEqual(broker_order.order_ref, "new-aapl-2")
            observed_submitted_at = broker_order.submitted_at
            if observed_submitted_at.tzinfo is None:
                observed_submitted_at = observed_submitted_at.replace(tzinfo=timezone.utc)
            self.assertEqual(observed_submitted_at, captured_at)
            self.assertNotIn("last_order_error_callback", broker_order.metadata_json)
            self.assertNotIn("broker_submission", broker_order.metadata_json)
        finally:
            session.close()

    def test_persist_broker_runtime_snapshot_writes_real_ledger_rows(self) -> None:
        self._insert_instruction()

        captured_at = datetime(2026, 4, 19, 8, 30, tzinfo=timezone.utc)
        initial_snapshot = BrokerRuntimeSnapshot(
            open_orders={
                17: BrokerOpenOrder(
                    order_id=17,
                    perm_id=9001,
                    client_id=0,
                    status="PreSubmitted",
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
            executions=(
                BrokerExecution(
                    exec_id="00014800.69ddd749.01.01",
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
                    commission=Decimal("1.25"),
                    commission_currency="USD",
                ),
            ),
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
                    market_price=Decimal("201.50"),
                    market_value=Decimal("201.50"),
                    average_cost=Decimal("200.00"),
                    unrealized_pnl=Decimal("1.50"),
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
                    "TotalCashValue": {"value": "99800.00", "currency": "USD"},
                }
            },
        )

        persist_broker_runtime_snapshot(
            self.session_factory,
            initial_snapshot,
            broker_kind=BROKER_KIND_IBKR,
            captured_at=captured_at,
            default_account_key="DU1234567",
        )

        updated_snapshot = BrokerRuntimeSnapshot(
            open_orders={
                17: BrokerOpenOrder(
                    order_id=17,
                    perm_id=9001,
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
            executions=initial_snapshot.executions,
            portfolio=initial_snapshot.portfolio,
            positions=initial_snapshot.positions,
            account_values=initial_snapshot.account_values,
        )

        persist_broker_runtime_snapshot(
            self.session_factory,
            updated_snapshot,
            broker_kind=BROKER_KIND_IBKR,
            captured_at=datetime(2026, 4, 19, 8, 32, tzinfo=timezone.utc),
            default_account_key="DU1234567",
        )

        session = self.session_factory()
        try:
            broker_accounts = session.execute(select(BrokerAccountRecord)).scalars().all()
            account_snapshots = session.execute(select(AccountSnapshotRecord)).scalars().all()
            position_snapshots = session.execute(select(PositionSnapshotRecord)).scalars().all()
            broker_orders = session.execute(select(BrokerOrderRecord)).scalars().all()
            broker_order_events = session.execute(
                select(BrokerOrderEventRecord).order_by(BrokerOrderEventRecord.id)
            ).scalars().all()
            execution_fills = session.execute(select(ExecutionFillRecord)).scalars().all()

            self.assertEqual(len(broker_accounts), 1)
            self.assertEqual(broker_accounts[0].account_key, "DU1234567")
            self.assertEqual(len(account_snapshots), 2)
            self.assertEqual(account_snapshots[0].net_liquidation, "100000.00")
            self.assertEqual(len(position_snapshots), 2)
            self.assertEqual(position_snapshots[0].quantity, "1")
            self.assertEqual(len(broker_orders), 1)
            self.assertEqual(broker_orders[0].status, "Submitted")
            self.assertEqual(broker_orders[0].instruction_id, 1)
            self.assertEqual(len(broker_order_events), 3)
            self.assertEqual(
                [event.event_type for event in broker_order_events],
                ["open_order_observed", "execution_fill_observed", "open_order_updated"],
            )
            self.assertEqual(
                [event.status_after for event in broker_order_events],
                ["PreSubmitted", "FILLED", "Submitted"],
            )
            self.assertEqual(len(execution_fills), 1)
            self.assertEqual(execution_fills[0].external_execution_id, "00014800.69ddd749.01.01")
            self.assertEqual(execution_fills[0].instruction_id, 1)
            self.assertEqual(execution_fills[0].commission, "1.25")
            self.assertEqual(execution_fills[0].commission_currency, "USD")
        finally:
            session.close()

    def test_persist_broker_runtime_snapshot_preserves_prior_lineage_for_reused_order_id(
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
            current_instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "persisted-aapl-1"
                )
            ).scalar_one()
            old_instruction.broker_order_id = 17
            current_instruction.broker_order_id = 17
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
                    external_order_id="17",
                    external_perm_id="8001",
                    external_client_id="0",
                    order_ref=None,
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
                    17: BrokerOpenOrder(
                        order_id=17,
                        perm_id=9001,
                        client_id=0,
                        status="Submitted",
                        order_ref=None,
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
            broker_orders = session.execute(
                select(BrokerOrderRecord).order_by(BrokerOrderRecord.id.asc())
            ).scalars().all()
            broker_order_events = session.execute(
                select(BrokerOrderEventRecord).order_by(BrokerOrderEventRecord.id.asc())
            ).scalars().all()

            self.assertEqual(len(broker_orders), 2)
            self.assertIsNone(broker_orders[0].external_order_id)
            self.assertEqual(broker_orders[0].external_perm_id, "8001")
            self.assertEqual(
                broker_orders[0].metadata_json["retired_reused_external_order_ids"][0][
                    "external_order_id"
                ],
                "17",
            )
            self.assertEqual(broker_orders[1].external_order_id, "17")
            self.assertEqual(broker_orders[1].external_perm_id, "9001")
            self.assertEqual(broker_orders[1].status, "Submitted")
            self.assertIsNone(broker_orders[1].order_ref)
            self.assertIsNone(broker_orders[1].instruction_id)
            self.assertEqual(
                [event.event_type for event in broker_order_events],
                ["external_order_id_reused", "open_order_observed"],
            )
        finally:
            session.close()

    def test_persist_broker_runtime_snapshot_links_execution_fill_after_reused_order_id(
        self,
    ) -> None:
        session = self.session_factory()
        try:
            instruction = InstructionRecord(
                instruction_id="nibe-long-1",
                schema_version="2026-04-10",
                source_system="q-training",
                batch_id="batch-1",
                account_key="U25245596",
                book_key="long_book",
                symbol="NIBE B",
                exchange="SFB",
                currency="SEK",
                state=ExecutionState.EXIT_PENDING.value,
                submit_at=datetime(2026, 5, 8, 6, 50, tzinfo=timezone.utc),
                expire_at=datetime(2026, 5, 8, 15, 30, tzinfo=timezone.utc),
                order_type="LIMIT",
                side="BUY",
                payload={
                    "instruction": {
                        "instruction_id": "nibe-long-1",
                        "account": {
                            "account_key": "U25245596",
                            "book_key": "long_book",
                        },
                        "instrument": {
                            "symbol": "NIBE B",
                            "security_type": "STK",
                            "exchange": "SFB",
                            "primary_exchange": "SFB",
                            "currency": "SEK",
                            "local_symbol": "NIBE B",
                        },
                        "intent": {
                            "side": "BUY",
                            "position_side": "LONG",
                        },
                    }
                },
            )
            session.add(instruction)
            session.flush()
            broker_account = BrokerAccountRecord(
                broker_kind=BROKER_KIND_IBKR,
                account_key="U25245596",
                base_currency="SEK",
            )
            session.add(broker_account)
            session.flush()
            old_broker_order = BrokerOrderRecord(
                instruction_id=None,
                broker_account_id=broker_account.id,
                broker_kind=BROKER_KIND_IBKR,
                account_key="U25245596",
                order_role="BROKER_NATIVE",
                external_order_id="95",
                external_perm_id="8001",
                external_client_id="0",
                order_ref="manual-nibe-close-open",
                symbol="NIBE B",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="NIBE B",
                side="BUY",
                order_type="LMT",
                status="FILLED",
                total_quantity="10",
                limit_price="200.00",
                stop_price=None,
                submitted_at=datetime(2026, 5, 7, 9, 0, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 5, 7, 9, 5, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add(old_broker_order)
            session.flush()
            session.add(
                ExecutionFillRecord(
                    broker_order_id=old_broker_order.id,
                    instruction_id=None,
                    broker_account_id=broker_account.id,
                    broker_kind=BROKER_KIND_IBKR,
                    account_key="U25245596",
                    is_virtual=False,
                    external_execution_id="old-exec-95",
                    external_order_id="95",
                    external_perm_id="8001",
                    order_ref="manual-nibe-close-open",
                    symbol="NIBE B",
                    exchange="SFB",
                    currency="SEK",
                    security_type="STK",
                    side="BUY",
                    quantity="10",
                    price="200.00",
                    commission=None,
                    commission_currency=None,
                    executed_at=datetime(2026, 5, 7, 9, 5, tzinfo=timezone.utc),
                    raw_payload={},
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
                        exec_id="new-exec-95",
                        order_id=95,
                        perm_id=9001,
                        client_id=0,
                        order_ref="nibe-long-1:exit:take_profit",
                        side="SLD",
                        shares=Decimal("36"),
                        price=Decimal("517.80"),
                        exchange="SFB",
                        executed_at=datetime(2026, 5, 8, 7, 1, tzinfo=timezone.utc),
                        symbol="NIBE B",
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
            broker_orders = session.execute(
                select(BrokerOrderRecord).order_by(BrokerOrderRecord.id.asc())
            ).scalars().all()
            execution_fills = session.execute(
                select(ExecutionFillRecord).order_by(ExecutionFillRecord.id.asc())
            ).scalars().all()
            instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "nibe-long-1"
                )
            ).scalar_one()

            self.assertEqual(len(broker_orders), 2)
            self.assertIsNone(broker_orders[0].external_order_id)
            self.assertEqual(broker_orders[1].external_order_id, "95")
            self.assertEqual(broker_orders[1].external_perm_id, "9001")
            self.assertEqual(broker_orders[1].instruction_id, instruction.id)
            self.assertEqual(broker_orders[1].order_role, "EXIT")
            self.assertEqual(broker_orders[1].status, "FILLED")

            self.assertEqual(len(execution_fills), 2)
            self.assertEqual(execution_fills[0].broker_order_id, broker_orders[0].id)
            self.assertEqual(execution_fills[1].broker_order_id, broker_orders[1].id)
            self.assertEqual(execution_fills[1].instruction_id, instruction.id)
            self.assertEqual(execution_fills[1].external_order_id, "95")
            self.assertEqual(execution_fills[1].external_perm_id, "9001")
        finally:
            session.close()
