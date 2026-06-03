from __future__ import annotations

from tests._ledger_persistence_shared import *  # noqa: F401,F403


class BrokerLedgerPersistenceTests03(BrokerLedgerPersistenceTestCase):
    def test_runtime_snapshot_marks_missing_entry_instruction_cancelled(
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
            self.assertEqual(instruction.broker_order_status, "NOT_FOUND_AT_BROKER")
            self.assertEqual(
                [event.event_type for event in instruction_events],
                ["entry_order_cancelled"],
            )
            self.assertEqual(instruction_events[0].source, "runtime_snapshot")
        finally:
            session.close()

    def test_order_error_risk_mitigation_marks_entry_instruction_needs_review(
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
                    status="Cancelled",
                    total_quantity="1",
                    limit_price="200.00",
                    stop_price=None,
                    submitted_at=datetime(2026, 4, 19, 8, 30, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 19, 8, 32, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
            )
            session.commit()
        finally:
            session.close()

        with patch(
            "ibkr_trader.ledger.instruction_projection.send_operator_alert"
        ) as send_alert:
            persist_broker_callback_events(
                self.session_factory,
                [
                    {
                        "event_type": "order_error",
                        "event_at": datetime(2026, 4, 19, 8, 32, tzinfo=timezone.utc),
                        "error": {
                            "orderId": 11,
                            "errorTime": 0,
                            "errorCode": 202,
                            "errorString": "Order cancelled by risk mitigation - TRDV.",
                            "advancedOrderRejectJson": None,
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

            self.assertEqual(instruction.state, ExecutionState.NEEDS_REVIEW.value)
            self.assertEqual(instruction.broker_order_status, "Cancelled")
            self.assertEqual(
                [event.event_type for event in instruction_events],
                ["entry_order_needs_review"],
            )
            self.assertEqual(
                instruction_events[0].state_before,
                ExecutionState.ENTRY_SUBMITTED.value,
            )
            self.assertEqual(
                instruction_events[0].state_after,
                ExecutionState.NEEDS_REVIEW.value,
            )
            self.assertEqual(
                instruction_events[0].payload["order_error_callback"]["errorCode"],
                202,
            )
        finally:
            session.close()

        send_alert.assert_called_once()

    def test_open_order_snapshot_upgrades_unmatched_exit_order_role(self) -> None:
        self._insert_instruction()
        session = self.session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind=BROKER_KIND_IBKR,
                account_key="DU1234567",
                base_currency="USD",
            )
            session.add(broker_account)
            session.flush()
            session.add(
                BrokerOrderRecord(
                    instruction_id=None,
                    broker_account_id=broker_account.id,
                    broker_kind=BROKER_KIND_IBKR,
                    account_key="DU1234567",
                    order_role="BROKER_NATIVE",
                    external_order_id="4845",
                    external_perm_id=None,
                    external_client_id=None,
                    order_ref=None,
                    symbol="UNKNOWN",
                    exchange="UNKNOWN",
                    currency="UNKNOWN",
                    security_type="UNKNOWN",
                    side="UNKNOWN",
                    order_type="UNKNOWN",
                    status="ERROR",
                    submitted_at=None,
                    last_status_at=datetime(2026, 4, 19, 8, 31, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={
                        "unmatched_callback": True,
                        "reconstructed_from_broker_error": True,
                    },
                )
            )
            session.commit()
        finally:
            session.close()

        persist_broker_runtime_snapshot(
            self.session_factory,
            BrokerRuntimeSnapshot(
                open_orders={
                    4845: BrokerOpenOrder(
                        order_id=4845,
                        perm_id=9045,
                        client_id=0,
                        status="PreSubmitted",
                        order_ref="persisted-aapl-1:exit:catastrophic_stop",
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
        )

        session = self.session_factory()
        try:
            broker_order = session.execute(
                select(BrokerOrderRecord).where(
                    BrokerOrderRecord.external_order_id == "4845"
                )
            ).scalar_one()
            instruction = session.execute(
                select(InstructionRecord).where(
                    InstructionRecord.instruction_id == "persisted-aapl-1"
                )
            ).scalar_one()

            self.assertEqual(broker_order.order_role, "EXIT")
            self.assertEqual(broker_order.instruction_id, instruction.id)
            self.assertEqual(
                broker_order.order_ref,
                "persisted-aapl-1:exit:catastrophic_stop",
            )
            self.assertEqual(broker_order.side, "SELL")
            self.assertEqual(broker_order.order_type, "STP")
        finally:
            session.close()

    def test_open_order_snapshot_does_not_merge_replacement_order_by_ref(self) -> None:
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
                    order_role="EXIT",
                    external_order_id="4845",
                    external_perm_id="9045",
                    external_client_id="0",
                    order_ref="persisted-aapl-1:exit:catastrophic_stop",
                    symbol="AAPL",
                    exchange="SMART",
                    currency="USD",
                    security_type="STK",
                    primary_exchange="NASDAQ",
                    local_symbol="AAPL",
                    side="SELL",
                    order_type="STP",
                    status="Inactive",
                    total_quantity="1",
                    stop_price="170.00",
                    submitted_at=datetime(2026, 4, 19, 8, 30, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 19, 8, 31, tzinfo=timezone.utc),
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
                    4864: BrokerOpenOrder(
                        order_id=4864,
                        perm_id=9064,
                        client_id=0,
                        status="PreSubmitted",
                        order_ref="persisted-aapl-1:exit:catastrophic_stop",
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
        )

        session = self.session_factory()
        try:
            broker_orders = list(
                session.execute(
                    select(BrokerOrderRecord).order_by(BrokerOrderRecord.external_order_id)
                ).scalars()
            )

            self.assertEqual([row.external_order_id for row in broker_orders], ["4845", "4864"])
            self.assertEqual([row.status for row in broker_orders], ["Inactive", "PreSubmitted"])
            self.assertEqual(
                [row.order_ref for row in broker_orders],
                [
                    "persisted-aapl-1:exit:catastrophic_stop",
                    "persisted-aapl-1:exit:catastrophic_stop",
                ],
            )
        finally:
            session.close()

    def test_persist_broker_callback_events_reconstructs_exit_order_from_instruction_event(self) -> None:
        session = self.session_factory()
        try:
            instruction = InstructionRecord(
                instruction_id="persisted-sive-exit-1",
                schema_version="2026-04-10",
                source_system="q-training",
                batch_id="batch-1",
                account_key="U25245596",
                book_key="short_book",
                symbol="SIVE",
                exchange="SFB",
                currency="SEK",
                state=ExecutionState.EXIT_PENDING.value,
                submit_at=datetime(2026, 4, 21, 7, 20, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 21, 15, 30, tzinfo=timezone.utc),
                order_type="MKT",
                side="BUY",
                entry_submitted_quantity="1",
                entry_filled_quantity="1",
                entry_avg_fill_price="29.72",
                entry_filled_at=datetime(2026, 4, 21, 8, 20, tzinfo=timezone.utc),
                exit_order_id=95,
                exit_order_status="Submitted",
                exit_submitted_quantity="1",
                payload={
                    "instruction": {
                        "instruction_id": "persisted-sive-exit-1",
                        "account": {
                            "account_key": "U25245596",
                            "book_key": "short_book",
                        },
                        "instrument": {
                            "symbol": "SIVE",
                            "security_type": "STK",
                            "exchange": "SFB",
                            "primary_exchange": "SFB",
                            "currency": "SEK",
                            "local_symbol": "SIVE",
                        },
                        "entry": {
                            "order_type": "MARKET",
                            "submit_at": "2026-04-21T10:20:00+02:00",
                            "expire_at": "2026-04-21T10:21:00+02:00",
                            "time_in_force": "DAY",
                        },
                        "exit": {
                            "delayed_limit": {
                                "submit_at": "2026-04-21T10:21:00+02:00",
                                "limit_offset_pct": "0.05",
                                "reference": "MARKET_AT_TRIGGER",
                            }
                        },
                    }
                },
            )
            session.add(instruction)
            session.flush()
            session.add(
                InstructionEventRecord(
                    instruction_id=instruction.id,
                    event_type="delayed_limit_exit_submitted",
                    source="runtime_cycle",
                    event_at=datetime(2026, 4, 21, 8, 21, tzinfo=timezone.utc),
                    state_before=ExecutionState.POSITION_OPEN.value,
                    state_after=ExecutionState.EXIT_PENDING.value,
                    payload={
                        "broker_submission": {
                            "account": "U25245596",
                            "order": {
                                "action": "SELL",
                                "order_type": "LIMIT",
                                "total_quantity": "1",
                                "limit_price": "31.20",
                                "order_ref": "persisted-sive-exit-1:exit:delayed_limit",
                            },
                            "broker_order_status": {
                                "orderId": 95,
                                "permId": 91095,
                                "clientId": 0,
                                "status": "Submitted",
                            },
                            "resolved_contract": {
                                "symbol": "SIVE",
                                "exchange": "SFB",
                                "primary_exchange": "SFB",
                                "currency": "SEK",
                                "security_type": "STK",
                                "local_symbol": "SIVE",
                            },
                        }
                    },
                    note="Submitted delayed limit exit anchored to live market at trigger time.",
                )
            )
            session.commit()
        finally:
            session.close()

        persist_broker_callback_events(
            self.session_factory,
            [
                {
                    "event_type": "order_error",
                    "event_at": datetime(2026, 4, 21, 8, 22, tzinfo=timezone.utc),
                    "error": {
                        "orderId": 95,
                        "errorTime": 0,
                        "errorCode": 201,
                        "errorString": "Order rejected",
                        "advancedOrderRejectJson": None,
                    },
                }
            ],
            broker_kind=BROKER_KIND_IBKR,
            default_account_key="U25245596",
        )

        session = self.session_factory()
        try:
            broker_order = session.execute(
                select(BrokerOrderRecord).where(BrokerOrderRecord.external_order_id == "95")
            ).scalar_one()
            broker_order_events = session.execute(
                select(BrokerOrderEventRecord)
                .where(BrokerOrderEventRecord.broker_order_id == broker_order.id)
                .order_by(BrokerOrderEventRecord.id)
            ).scalars().all()

            self.assertEqual(broker_order.order_role, "EXIT")
            self.assertEqual(broker_order.side, "SELL")
            self.assertEqual(broker_order.order_type, "LIMIT")
            self.assertEqual(broker_order.limit_price, "31.20")
            self.assertEqual(broker_order.status, "Submitted")
            self.assertTrue(
                broker_order.raw_payload["reconstructed_from_instruction_event"]
            )
            self.assertEqual(
                [event.event_type for event in broker_order_events],
                ["order_error_callback"],
            )
            self.assertEqual(
                broker_order.metadata_json["last_order_error_callback"]["errorCode"],
                201,
            )
        finally:
            session.close()

    def test_persist_broker_callback_events_marks_not_found_order_as_closed(self) -> None:
        session = self.session_factory()
        try:
            instruction = InstructionRecord(
                instruction_id="persisted-volcar-exit-1",
                schema_version="2026-04-10",
                source_system="q-training",
                batch_id="batch-1",
                account_key="U25245596",
                book_key="long_book",
                symbol="VOLCAR.B",
                exchange="SFB",
                currency="SEK",
                state=ExecutionState.EXIT_PENDING.value,
                submit_at=datetime(2026, 4, 23, 6, 59, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 23, 15, 30, tzinfo=timezone.utc),
                order_type="MKT",
                side="BUY",
                entry_submitted_quantity="827",
                entry_filled_quantity="827",
                entry_avg_fill_price="23.26",
                entry_filled_at=datetime(2026, 4, 21, 8, 2, tzinfo=timezone.utc),
                exit_order_id=3952,
                exit_perm_id=449407988,
                exit_order_status="PreSubmitted",
                exit_submitted_quantity="827",
                payload={
                    "instruction": {
                        "instruction_id": "persisted-volcar-exit-1",
                        "account": {
                            "account_key": "U25245596",
                            "book_key": "long_book",
                        },
                        "instrument": {
                            "symbol": "VOLCAR.B",
                            "security_type": "STK",
                            "exchange": "SFB",
                            "currency": "SEK",
                        },
                        "intent": {
                            "side": "BUY",
                            "position_side": "LONG",
                        },
                        "sizing": {
                            "mode": "target_quantity",
                            "target_quantity": "827",
                        },
                        "entry": {
                            "order_type": "MKT",
                            "submit_at": "2026-04-23T08:59:00+02:00",
                            "expire_at": "2026-04-23T17:30:00+02:00",
                        },
                        "exit": {
                            "force_exit_next_session_open": True,
                        },
                        "trace": {
                            "reason_code": "ledger-test",
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
                metadata_json={},
            )
            session.add(broker_account)
            session.flush()
            broker_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind=BROKER_KIND_IBKR,
                account_key="U25245596",
                order_role="EXIT",
                external_order_id="3952",
                external_perm_id="449407988",
                external_client_id="0",
                order_ref="persisted-volcar-exit-1:exit:forced",
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="VOLCAR B",
                side="SELL",
                order_type="MKT",
                time_in_force="DAY",
                status="PreSubmitted",
                total_quantity="827",
                submitted_at=datetime(2026, 4, 23, 6, 30, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 23, 6, 30, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add(broker_order)
            session.commit()
        finally:
            session.close()

        persist_broker_callback_events(
            self.session_factory,
            [
                {
                    "event_type": "order_error",
                    "event_at": datetime(2026, 4, 23, 6, 31, tzinfo=timezone.utc),
                    "error": {
                        "orderId": 3952,
                        "errorTime": 0,
                        "errorCode": 10147,
                        "errorString": "OrderId 3952 that needs to be cancelled is not found.",
                        "advancedOrderRejectJson": None,
                    },
                }
            ],
            broker_kind=BROKER_KIND_IBKR,
            default_account_key="U25245596",
        )

        session = self.session_factory()
        try:
            broker_order = session.execute(
                select(BrokerOrderRecord).where(BrokerOrderRecord.external_order_id == "3952")
            ).scalar_one()
            broker_order_events = session.execute(
                select(BrokerOrderEventRecord)
                .where(BrokerOrderEventRecord.broker_order_id == broker_order.id)
                .order_by(BrokerOrderEventRecord.id)
            ).scalars().all()

            self.assertEqual(broker_order.status, "NOT_FOUND_AT_BROKER")
            self.assertEqual(
                broker_order.metadata_json["last_order_error_callback"]["errorCode"],
                10147,
            )
            self.assertEqual(
                [event.event_type for event in broker_order_events],
                ["order_error_callback"],
            )
            self.assertEqual(broker_order_events[0].status_before, "PreSubmitted")
            self.assertEqual(broker_order_events[0].status_after, "NOT_FOUND_AT_BROKER")
        finally:
            session.close()
