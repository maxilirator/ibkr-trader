from __future__ import annotations

from tests._operator_dashboard_shared import *  # noqa: F401,F403


class OperatorDashboardReadModelTests01(OperatorDashboardReadModelTestCase):
    def test_build_operator_dashboard_snapshot_returns_latest_durable_views(self) -> None:
        self._seed_operator_data()
        record_broker_attention_review_action(
            self.session_factory,
            event_id=1,
            action_type="ACKNOWLEDGE",
            updated_by="dashboard",
        )
        record_reconciliation_issue_review_action(
            self.session_factory,
            issue_id=1,
            action_type="RESOLVE",
            updated_by="dashboard",
        )

        snapshot = build_operator_dashboard_snapshot(
            self.session_factory,
            order_limit=10,
            fill_limit=10,
            attention_limit=10,
            reconciliation_run_limit=10,
        )

        self.assertTrue(snapshot.kill_switch.enabled)
        self.assertEqual(snapshot.kill_switch.reason, "Operator halt for review.")
        self.assertEqual(len(snapshot.accounts), 1)
        self.assertEqual(snapshot.accounts[0].net_liquidation, "101500.00")

        self.assertEqual(len(snapshot.positions), 1)
        self.assertEqual(snapshot.positions[0].symbol, "SAAB")
        self.assertEqual(snapshot.positions[0].quantity, "2")

        self.assertEqual(len(snapshot.open_orders), 1)
        self.assertEqual(snapshot.open_orders[0].external_order_id, "11")
        self.assertEqual(snapshot.open_orders[0].warning_text, "Held in TWS for review.")
        self.assertEqual(snapshot.open_orders[0].order_purpose, "Entry")
        self.assertEqual(snapshot.open_orders[0].working_price, "100")
        self.assertEqual(snapshot.open_orders[0].working_price_reference, "LIMIT")
        self.assertEqual(snapshot.open_orders[0].reference_market_price, "102.00")
        self.assertEqual(snapshot.open_orders[0].last_market_price_direction, "UP")
        self.assertEqual(snapshot.open_orders[0].price_spread, "-2.00")
        self.assertEqual(snapshot.open_orders[0].price_spread_pct, "-1.96")
        self.assertEqual(snapshot.open_orders[0].spread_reference, "LIMIT")

        self.assertEqual(len(snapshot.recent_fills), 1)
        self.assertEqual(snapshot.recent_fills[0].external_execution_id, "exec-001")

        self.assertEqual(len(snapshot.recent_broker_attention), 1)
        self.assertEqual(
            snapshot.recent_broker_attention[0].message,
            "[201] Order held for review",
        )
        self.assertEqual(
            snapshot.recent_broker_attention[0].operator_review.status,
            "ACKNOWLEDGED",
        )

        self.assertEqual(len(snapshot.recent_reconciliation_runs), 1)
        self.assertEqual(snapshot.recent_reconciliation_runs[0].status, "WARNINGS")
        self.assertEqual(len(snapshot.recent_reconciliation_runs[0].issues), 1)
        self.assertEqual(
            snapshot.recent_reconciliation_runs[0].issues[0].operator_review.status,
            "RESOLVED",
        )

    def test_recent_exit_fill_includes_per_fill_realized_pnl(self) -> None:
        session: Session = self.session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="U25245596",
                account_label="Live Sweden",
                base_currency="SEK",
            )
            session.add(broker_account)
            session.flush()

            instruction = InstructionRecord(
                instruction_id="instr-saab-long-1",
                schema_version="2026-04-10",
                source_system="test",
                batch_id="batch-1",
                account_key="U25245596",
                book_key="long_risk_book",
                symbol="SAAB",
                exchange="SMART",
                currency="SEK",
                state="COMPLETED",
                submit_at=datetime(2026, 4, 19, 7, 0, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 19, 15, 30, tzinfo=timezone.utc),
                order_type="LIMIT",
                side="BUY",
                payload={
                    "instruction": {
                        "intent": {"side": "BUY", "position_side": "LONG"}
                    }
                },
            )
            session.add(instruction)
            session.flush()

            entry_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="ENTRY",
                external_order_id="entry-1",
                external_perm_id="entry-perm-1",
                external_client_id="0",
                order_ref=instruction.instruction_id,
                symbol="SAAB",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="SAAB B",
                side="BUY",
                order_type="LMT",
                time_in_force="DAY",
                status="Filled",
                total_quantity="10",
                limit_price="100.00",
                stop_price=None,
                submitted_at=datetime(2026, 4, 19, 7, 0, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 19, 7, 1, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            exit_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="EXIT",
                external_order_id="exit-1",
                external_perm_id="exit-perm-1",
                external_client_id="0",
                order_ref="manual-saab-close",
                symbol="SAAB",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="SAAB B",
                side="SELL",
                order_type="LMT",
                time_in_force="DAY",
                status="Filled",
                total_quantity="4",
                limit_price="105.00",
                stop_price=None,
                submitted_at=datetime(2026, 4, 19, 8, 0, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 19, 8, 1, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add_all([entry_order, exit_order])
            session.flush()

            session.add_all(
                [
                    ExecutionFillRecord(
                        broker_order_id=entry_order.id,
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        external_execution_id="entry-fill-1",
                        external_order_id="entry-1",
                        external_perm_id="entry-perm-1",
                        order_ref=instruction.instruction_id,
                        symbol="SAAB",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        side="BOT",
                        quantity="10",
                        price="100.00",
                        commission="2.00",
                        commission_currency="SEK",
                        executed_at=datetime(2026, 4, 19, 7, 1, tzinfo=timezone.utc),
                        raw_payload={},
                    ),
                    ExecutionFillRecord(
                        broker_order_id=exit_order.id,
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        external_execution_id="exit-fill-1",
                        external_order_id="exit-1",
                        external_perm_id="exit-perm-1",
                        order_ref="manual-saab-close",
                        symbol="SAAB",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        side="SLD",
                        quantity="4",
                        price="105.00",
                        commission="1.00",
                        commission_currency="SEK",
                        executed_at=datetime(2026, 4, 19, 8, 1, tzinfo=timezone.utc),
                        raw_payload={},
                    ),
                ]
            )
            session.commit()
        finally:
            session.close()

        snapshot = build_operator_dashboard_snapshot(
            self.session_factory,
            fill_limit=10,
        )

        exit_fill = snapshot.recent_fills[0]
        self.assertEqual(exit_fill.external_execution_id, "exit-fill-1")
        self.assertEqual(exit_fill.order_role, "EXIT")
        self.assertEqual(exit_fill.position_side, "LONG")
        self.assertEqual(exit_fill.realized_pnl, "+18.20")
        self.assertEqual(exit_fill.realized_pnl_gross, "+20.00")
        self.assertEqual(exit_fill.realized_pnl_currency, "SEK")
        self.assertEqual(exit_fill.realized_pnl_basis_price, "100")

    def test_build_operator_dashboard_snapshot_includes_account_day_performance(self) -> None:
        self._seed_operator_data()

        with patch(
            "ibkr_trader.read_models.operator_dashboard.utc_now",
            return_value=datetime(2026, 4, 19, 8, 0, tzinfo=timezone.utc),
        ):
            snapshot = build_operator_dashboard_snapshot(self.session_factory)

        performance = snapshot.accounts[0].day_performance
        self.assertEqual(performance.start_net_liquidation, "100000")
        self.assertEqual(performance.latest_net_liquidation, "101500")
        self.assertEqual(performance.latest_return_pct, "+1.50")
        self.assertEqual(len(performance.points), 2)
        self.assertEqual(performance.points[0].return_pct, "0.00")
        self.assertEqual(performance.points[1].return_pct, "+1.50")

    def test_account_day_performance_uses_trading_session_window(self) -> None:
        session: Session = self.session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="U25245596",
                base_currency="SEK",
            )
            session.add(broker_account)
            session.flush()
            for snapshot_at, net_liquidation in [
                (datetime(2026, 5, 6, 6, 45, tzinfo=timezone.utc), "99000.00"),
                (datetime(2026, 5, 6, 7, 0, tzinfo=timezone.utc), "100000.00"),
                (datetime(2026, 5, 6, 14, 55, tzinfo=timezone.utc), "101000.00"),
                (datetime(2026, 5, 6, 15, 20, tzinfo=timezone.utc), "102000.00"),
                (datetime(2026, 5, 6, 16, 5, tzinfo=timezone.utc), "103000.00"),
            ]:
                session.add(
                    AccountSnapshotRecord(
                        broker_account_id=broker_account.id,
                        snapshot_at=snapshot_at,
                        source="runtime_snapshot",
                        net_liquidation=net_liquidation,
                        currency="SEK",
                    )
                )
            session.commit()
        finally:
            session.close()

        with patch(
            "ibkr_trader.read_models.operator_dashboard.utc_now",
            return_value=datetime(2026, 5, 6, 20, 0, tzinfo=timezone.utc),
        ):
            snapshot = build_operator_dashboard_snapshot(self.session_factory)

        performance = snapshot.accounts[0].day_performance
        self.assertEqual(performance.start_net_liquidation, "100000")
        self.assertEqual(performance.latest_net_liquidation, "102000")
        self.assertEqual(performance.latest_return_pct, "+2.00")
        self.assertEqual(
            [
                point.snapshot_at.replace(tzinfo=timezone.utc)
                for point in performance.points
            ],
            [
                datetime(2026, 5, 6, 7, 0, tzinfo=timezone.utc),
                datetime(2026, 5, 6, 14, 55, tzinfo=timezone.utc),
                datetime(2026, 5, 6, 15, 20, tzinfo=timezone.utc),
            ],
        )

    def test_build_operator_dashboard_snapshot_hides_archived_attention_and_warnings(self) -> None:
        self._seed_operator_data()
        record_broker_attention_review_action(
            self.session_factory,
            event_id=1,
            action_type="ARCHIVE",
            updated_by="dashboard",
        )
        record_reconciliation_issue_review_action(
            self.session_factory,
            issue_id=1,
            action_type="ARCHIVE",
            updated_by="dashboard",
        )

        snapshot = build_operator_dashboard_snapshot(
            self.session_factory,
            order_limit=10,
            fill_limit=10,
            attention_limit=10,
            reconciliation_run_limit=10,
        )

        self.assertEqual(snapshot.recent_broker_attention, ())
        self.assertEqual(snapshot.recent_reconciliation_runs, ())

    def test_build_operator_dashboard_snapshot_reports_exit_orders_against_fill_basis(self) -> None:
        session: Session = self.session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="U25245596",
                account_label="Live Sweden",
                base_currency="SEK",
            )
            session.add(broker_account)
            session.flush()

            instruction = InstructionRecord(
                instruction_id="2026-04-21-U25245596-long_risk_book-VOLCAR B-long-01",
                schema_version="2026-04-10",
                source_system="test",
                batch_id="batch-1",
                account_key="U25245596",
                book_key="long_risk_book",
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                state="EXIT_PENDING",
                submit_at=datetime(2026, 4, 21, 8, 0, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 21, 15, 30, tzinfo=timezone.utc),
                order_type="LMT",
                side="BUY",
                payload={},
            )
            session.add(instruction)
            session.flush()

            session.add_all(
                [
                    PositionSnapshotRecord(
                        broker_account_id=broker_account.id,
                        snapshot_at=datetime(2026, 4, 21, 12, 30, tzinfo=timezone.utc),
                        source="runtime_snapshot",
                        symbol="VOLCAR.B",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        primary_exchange="SFB",
                        local_symbol="VOLCAR B",
                        quantity="827",
                        average_cost="23.3192503",
                        market_price="23.30",
                        market_value="19269.10",
                        unrealized_pnl="-15.91",
                        realized_pnl="0.00",
                    ),
                    PositionSnapshotRecord(
                        broker_account_id=broker_account.id,
                        snapshot_at=datetime(2026, 4, 21, 12, 31, tzinfo=timezone.utc),
                        source="runtime_snapshot",
                        symbol="VOLCAR.B",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        primary_exchange="SFB",
                        local_symbol="VOLCAR B",
                        quantity="827",
                        average_cost="23.3192503",
                        market_price="23.30674555",
                        market_value="19274.68",
                        unrealized_pnl="-10.34",
                        realized_pnl="0.00",
                    ),
                ]
            )

            entry_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="ENTRY",
                external_order_id="85",
                external_perm_id="1030141445",
                external_client_id="0",
                order_ref=instruction.instruction_id,
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="VOLCAR B",
                side="BUY",
                order_type="MKT",
                time_in_force="DAY",
                status="Filled",
                total_quantity="827",
                limit_price=None,
                stop_price=None,
                submitted_at=datetime(2026, 4, 21, 8, 2, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 21, 8, 2, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            take_profit_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="EXIT",
                external_order_id="87",
                external_perm_id="1030141447",
                external_client_id="0",
                order_ref=f"{instruction.instruction_id}:exit:take_profit",
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="VOLCAR B",
                side="SELL",
                order_type="LMT",
                time_in_force="DAY",
                status="Submitted",
                total_quantity="827",
                limit_price="23.73",
                stop_price="0.0",
                submitted_at=datetime(2026, 4, 21, 8, 2, 1, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 21, 12, 31, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            catastrophic_stop_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="EXIT",
                external_order_id="88",
                external_perm_id="1030141448",
                external_client_id="0",
                order_ref=f"{instruction.instruction_id}:exit:catastrophic_stop",
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="VOLCAR B",
                side="SELL",
                order_type="STP",
                time_in_force="DAY",
                status="PreSubmitted",
                total_quantity="827",
                limit_price="0.0",
                stop_price="19.775",
                submitted_at=datetime(2026, 4, 21, 8, 2, 2, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 21, 12, 31, 1, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add_all([entry_order, take_profit_order, catastrophic_stop_order])
            session.flush()

            session.add(
                ExecutionFillRecord(
                    broker_order_id=entry_order.id,
                    instruction_id=instruction.id,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="U25245596",
                    external_execution_id="exec-volcar-entry",
                    external_order_id="85",
                    external_perm_id="1030141445",
                    order_ref=instruction.instruction_id,
                    symbol="VOLCAR.B",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    side="BOT",
                    quantity="827",
                    price="23.26",
                    commission="49.00",
                    commission_currency="SEK",
                    executed_at=datetime(2026, 4, 21, 8, 2, 0, tzinfo=timezone.utc),
                    raw_payload={},
                )
            )
            session.commit()
        finally:
            session.close()

        snapshot = build_operator_dashboard_snapshot(
            self.session_factory,
            order_limit=10,
            fill_limit=10,
            attention_limit=10,
            reconciliation_run_limit=10,
        )

        open_orders_by_ref = {row.order_ref: row for row in snapshot.open_orders}
        take_profit = open_orders_by_ref[
            "2026-04-21-U25245596-long_risk_book-VOLCAR B-long-01:exit:take_profit"
        ]
        catastrophic_stop = open_orders_by_ref[
            "2026-04-21-U25245596-long_risk_book-VOLCAR B-long-01:exit:catastrophic_stop"
        ]

        self.assertEqual(take_profit.order_purpose, "Take Profit")
        self.assertEqual(take_profit.working_price, "23.73")
        self.assertEqual(take_profit.working_price_reference, "LIMIT")
        self.assertEqual(take_profit.fill_basis_price, "23.26")
        self.assertEqual(take_profit.fill_price_spread, "+0.47")
        self.assertEqual(take_profit.fill_price_spread_pct, "+2.02")
        self.assertEqual(take_profit.spread_reference, "LIMIT")
        self.assertEqual(take_profit.price_spread, "+0.42")
        self.assertEqual(take_profit.price_spread_pct, "+1.82")

        self.assertEqual(catastrophic_stop.order_purpose, "Catastrophic Stop")
        self.assertEqual(catastrophic_stop.working_price, "19.775")
        self.assertEqual(catastrophic_stop.working_price_reference, "STOP")
        self.assertEqual(catastrophic_stop.fill_basis_price, "23.26")
        self.assertEqual(catastrophic_stop.fill_price_spread, "-3.48")
        self.assertEqual(catastrophic_stop.fill_price_spread_pct, "-14.98")
        self.assertEqual(catastrophic_stop.spread_reference, "STOP")

    def test_build_operator_dashboard_snapshot_hides_auto_recovered_insufficient_funds_rejects(self) -> None:
        session: Session = self.session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="U25245596",
                account_label="Live Sweden",
                base_currency="SEK",
            )
            session.add(broker_account)
            session.flush()

            instruction = InstructionRecord(
                instruction_id="2026-04-21-U25245596-long_risk_book-VOLCAR B-long-01",
                schema_version="2026-04-10",
                source_system="test",
                batch_id="batch-1",
                account_key="U25245596",
                book_key="long_risk_book",
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                state="POSITION_OPEN",
                submit_at=datetime(2026, 4, 21, 7, 20, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 21, 15, 30, tzinfo=timezone.utc),
                order_type="LMT",
                side="BUY",
                payload={},
            )
            session.add(instruction)
            session.flush()

            rejected_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="ENTRY",
                external_order_id="84",
                external_perm_id="10084",
                external_client_id="0",
                order_ref=instruction.instruction_id,
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="VOLCAR B",
                side="BUY",
                order_type="LMT",
                time_in_force="DAY",
                status="Inactive",
                total_quantity="830",
                limit_price="23.26",
                submitted_at=datetime(2026, 4, 21, 7, 25, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 21, 7, 25, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            replacement_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="ENTRY",
                external_order_id="85",
                external_perm_id="10085",
                external_client_id="0",
                order_ref=instruction.instruction_id,
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="VOLCAR B",
                side="BUY",
                order_type="LMT",
                time_in_force="DAY",
                status="Filled",
                total_quantity="827",
                limit_price="23.26",
                submitted_at=datetime(2026, 4, 21, 7, 25, 2, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 21, 7, 26, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add_all([rejected_order, replacement_order])
            session.flush()

            session.add(
                BrokerOrderEventRecord(
                    broker_order_id=rejected_order.id,
                    event_type="order_error_callback",
                    event_at=datetime(2026, 4, 21, 7, 25, 1, tzinfo=timezone.utc),
                    status_before="Submitted",
                    status_after="Inactive",
                    payload={
                        "errorCode": 201,
                        "errorString": (
                            "Order rejected - reason:We are unable to accept your order. "
                            "Your Available Funds are insufficient to cover the change in the "
                            "account's margin requirements."
                        ),
                    },
                    note="Persisted broker order error callback directly from the live session.",
                )
            )
            session.commit()
        finally:
            session.close()

        snapshot = build_operator_dashboard_snapshot(
            self.session_factory,
            order_limit=10,
            fill_limit=10,
            attention_limit=10,
            reconciliation_run_limit=10,
        )

        self.assertEqual(tuple(snapshot.recent_broker_attention), ())

    def test_build_operator_dashboard_snapshot_hides_expected_unfilled_entry_expiry_cancel(self) -> None:
        session: Session = self.session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind="IBKR",
                account_key="U25245596",
                account_label="Live Sweden",
                base_currency="SEK",
            )
            session.add(broker_account)
            session.flush()

            instruction = InstructionRecord(
                instruction_id="2026-05-28-U25245596-live_top1_31_seedpicker-HEXA B-long-01",
                schema_version="2026-04-10",
                source_system="test",
                batch_id="batch-1",
                account_key="U25245596",
                book_key="live_top1_31_seedpicker",
                symbol="HEXA B",
                exchange="SMART",
                currency="SEK",
                state="ENTRY_CANCELLED",
                submit_at=datetime(2026, 5, 28, 7, 25, tzinfo=timezone.utc),
                expire_at=datetime(2026, 5, 28, 15, 30, tzinfo=timezone.utc),
                order_type="LMT",
                side="BUY",
                entry_filled_quantity="0",
                payload={},
            )
            session.add(instruction)
            session.flush()

            broker_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="ENTRY",
                external_order_id="4904",
                external_perm_id="1468602653",
                external_client_id="0",
                order_ref=instruction.instruction_id,
                symbol="HEXA.B",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="HEXA B",
                side="BUY",
                order_type="LMT",
                time_in_force="DAY",
                status="Cancelled",
                total_quantity="227",
                limit_price="83.96",
                submitted_at=datetime(2026, 5, 28, 7, 25, 31, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 5, 28, 15, 30, 5, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add(broker_order)
            session.flush()

            session.add(
                BrokerOrderEventRecord(
                    broker_order_id=broker_order.id,
                    event_type="order_error_callback",
                    event_at=datetime(2026, 5, 28, 15, 30, 5, tzinfo=timezone.utc),
                    status_before="PendingCancel",
                    status_after="Cancelled",
                    payload={
                        "orderId": 4904,
                        "errorCode": 202,
                        "errorString": "Order Canceled - reason:",
                    },
                    note="Persisted broker order error callback directly from the live session.",
                )
            )
            session.commit()
        finally:
            session.close()

        snapshot = build_operator_dashboard_snapshot(
            self.session_factory,
            order_limit=10,
            fill_limit=10,
            attention_limit=10,
            reconciliation_run_limit=10,
        )

        self.assertEqual(tuple(snapshot.recent_broker_attention), ())
