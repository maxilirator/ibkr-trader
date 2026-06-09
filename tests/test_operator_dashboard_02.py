from __future__ import annotations

from tests._operator_dashboard_shared import *  # noqa: F401,F403


class OperatorDashboardReadModelTests02(OperatorDashboardReadModelTestCase):
    def test_build_operator_dashboard_snapshot_keeps_pre_expiry_entry_cancel_attention(self) -> None:
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
                instruction_id="2026-05-22-U25245596-live_top1_31_seedpicker-HEXA B-long-01",
                schema_version="2026-04-10",
                source_system="test",
                batch_id="batch-1",
                account_key="U25245596",
                book_key="live_top1_31_seedpicker",
                symbol="HEXA B",
                exchange="SMART",
                currency="SEK",
                state="ENTRY_CANCELLED",
                submit_at=datetime(2026, 5, 22, 7, 25, tzinfo=timezone.utc),
                expire_at=datetime(2026, 5, 22, 15, 30, tzinfo=timezone.utc),
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
                external_order_id="4869",
                external_perm_id="1468600000",
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
                submitted_at=datetime(2026, 5, 22, 7, 25, 31, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 5, 22, 7, 25, 45, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add(broker_order)
            session.flush()

            session.add(
                BrokerOrderEventRecord(
                    broker_order_id=broker_order.id,
                    event_type="order_error_callback",
                    event_at=datetime(2026, 5, 22, 7, 25, 45, tzinfo=timezone.utc),
                    status_before="Submitted",
                    status_after="Cancelled",
                    payload={
                        "orderId": 4869,
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

        self.assertEqual(len(snapshot.recent_broker_attention), 1)
        self.assertEqual(
            snapshot.recent_broker_attention[0].message,
            "[202] Order Canceled - reason:",
        )

    def test_build_operator_dashboard_snapshot_hides_expected_oca_exit_sibling_cancel(self) -> None:
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
                instruction_id="2026-05-04-U25245596-live_top1_31_seedpicker-HACK-long-01",
                schema_version="2026-04-10",
                source_system="test",
                batch_id="batch-1",
                account_key="U25245596",
                book_key="live_top1_31_seedpicker",
                symbol="HACK",
                exchange="SMART",
                currency="SEK",
                state="COMPLETED",
                submit_at=datetime(2026, 5, 4, 7, 25, tzinfo=timezone.utc),
                expire_at=datetime(2026, 5, 4, 15, 30, tzinfo=timezone.utc),
                order_type="LMT",
                side="BUY",
                payload={},
            )
            session.add(instruction)
            session.flush()

            oca_group = "OCAB5AB0E78DC34DB63"
            stop_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="EXIT",
                external_order_id="4840",
                external_perm_id="1010318184",
                external_client_id="0",
                order_ref=f"{instruction.instruction_id}:exit:catastrophic_stop",
                symbol="HACK",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="HACK",
                side="SELL",
                order_type="STP",
                time_in_force="DAY",
                status="Cancelled",
                total_quantity="229",
                stop_price="66.10",
                submitted_at=datetime(2026, 5, 4, 7, 25, 13, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 5, 4, 12, 22, 50, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={"oca_group": oca_group},
            )
            take_profit_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="EXIT",
                external_order_id="4841",
                external_perm_id="1010318185",
                external_client_id="0",
                order_ref=f"{instruction.instruction_id}:exit:take_profit",
                symbol="HACK",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="HACK",
                side="SELL",
                order_type="LMT",
                time_in_force="DAY",
                status="FILLED",
                total_quantity="229",
                limit_price="79.30",
                submitted_at=datetime(2026, 5, 4, 7, 25, 14, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 5, 4, 12, 22, 50, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={"oca_group": oca_group},
            )
            session.add_all([stop_order, take_profit_order])
            session.flush()

            session.add(
                BrokerOrderEventRecord(
                    broker_order_id=stop_order.id,
                    event_type="order_error_callback",
                    event_at=datetime(2026, 5, 4, 12, 22, 50, tzinfo=timezone.utc),
                    status_before="Cancelled",
                    status_after="Cancelled",
                    payload={
                        "orderId": 4840,
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

    def test_build_operator_dashboard_snapshot_hides_forced_exit_protective_cleanup_cancel(self) -> None:
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
                instruction_id="2026-06-05-U25245596-live_top1_31_seedpicker-SINCH-long-01",
                schema_version="2026-04-10",
                source_system="test",
                batch_id="batch-1",
                account_key="U25245596",
                book_key="live_top1_31_seedpicker",
                symbol="SINCH",
                exchange="SMART",
                currency="SEK",
                state="EXIT_PENDING",
                submit_at=datetime(2026, 6, 5, 7, 25, tzinfo=timezone.utc),
                expire_at=datetime(2026, 6, 5, 15, 30, tzinfo=timezone.utc),
                order_type="LMT",
                side="BUY",
                payload={},
            )
            session.add(instruction)
            session.flush()

            take_profit_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="EXIT",
                external_order_id="4950",
                external_perm_id="1468603000",
                external_client_id="0",
                order_ref=f"{instruction.instruction_id}:exit:take_profit",
                symbol="SINCH",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="SINCH",
                side="SELL",
                order_type="LMT",
                time_in_force="DAY",
                status="Cancelled",
                total_quantity="455",
                limit_price="43.20",
                submitted_at=datetime(2026, 6, 5, 7, 25, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 6, 8, 6, 59, 33, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            forced_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="EXIT",
                external_order_id="4960",
                external_perm_id="1468603001",
                external_client_id="0",
                order_ref=f"{instruction.instruction_id}:exit:forced",
                symbol="SINCH",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="SINCH",
                side="SELL",
                order_type="LMT",
                time_in_force="DAY",
                status="Submitted",
                total_quantity="455",
                limit_price="39.50",
                submitted_at=datetime(2026, 6, 8, 7, 0, 8, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 6, 8, 7, 0, 8, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add_all([take_profit_order, forced_order])
            session.flush()

            session.add(
                BrokerOrderEventRecord(
                    broker_order_id=take_profit_order.id,
                    event_type="order_error_callback",
                    event_at=datetime(2026, 6, 8, 6, 59, 33, tzinfo=timezone.utc),
                    status_before="PendingCancel",
                    status_after="Cancelled",
                    payload={
                        "orderId": 4950,
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

    def test_build_operator_dashboard_snapshot_labels_price_collar_callback_as_warning(self) -> None:
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

            broker_order = BrokerOrderRecord(
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="EXIT",
                external_order_id="4960",
                external_perm_id="1468603001",
                external_client_id="0",
                order_ref="2026-06-05-U25245596-live_top1_31_seedpicker-SINCH-long-01:exit:forced",
                symbol="SINCH",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="SINCH",
                side="SELL",
                order_type="LMT",
                time_in_force="DAY",
                status="Submitted",
                total_quantity="455",
                limit_price="39.50",
                submitted_at=datetime(2026, 6, 8, 7, 0, 8, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 6, 8, 7, 0, 8, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add(broker_order)
            session.flush()

            session.add(
                BrokerOrderEventRecord(
                    broker_order_id=broker_order.id,
                    event_type="order_error_callback",
                    event_at=datetime(2026, 6, 8, 7, 0, 8, tzinfo=timezone.utc),
                    status_before="Submitted",
                    status_after="Submitted",
                    payload={
                        "orderId": 4960,
                        "errorCode": 2161,
                        "errorString": (
                            "SELL 455 SINCH SFB for U25245596 In accordance with "
                            "our regulatory obligations as a broker, we will initially "
                            "cap (or limit) the price of your Limit Order to 39.50."
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

        self.assertEqual(len(snapshot.recent_broker_attention), 1)
        self.assertEqual(snapshot.recent_broker_attention[0].event_type, "broker_warning")
        self.assertTrue(
            snapshot.recent_broker_attention[0].message.startswith("[2161] SELL 455 SINCH")
        )

    def test_build_operator_dashboard_snapshot_keeps_unmatched_exit_cancel_attention(self) -> None:
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
                instruction_id="2026-05-04-U25245596-live_top1_31_seedpicker-HACK-long-01",
                schema_version="2026-04-10",
                source_system="test",
                batch_id="batch-1",
                account_key="U25245596",
                book_key="live_top1_31_seedpicker",
                symbol="HACK",
                exchange="SMART",
                currency="SEK",
                state="POSITION_OPEN",
                submit_at=datetime(2026, 5, 4, 7, 25, tzinfo=timezone.utc),
                expire_at=datetime(2026, 5, 4, 15, 30, tzinfo=timezone.utc),
                order_type="LMT",
                side="BUY",
                payload={},
            )
            session.add(instruction)
            session.flush()

            stop_order = BrokerOrderRecord(
                instruction_id=instruction.id,
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="EXIT",
                external_order_id="4840",
                external_perm_id="1010318184",
                external_client_id="0",
                order_ref=f"{instruction.instruction_id}:exit:catastrophic_stop",
                symbol="HACK",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                local_symbol="HACK",
                side="SELL",
                order_type="STP",
                time_in_force="DAY",
                status="Cancelled",
                total_quantity="229",
                stop_price="66.10",
                submitted_at=datetime(2026, 5, 4, 7, 25, 13, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 5, 4, 12, 22, 50, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={"oca_group": "OCAB5AB0E78DC34DB63"},
            )
            session.add(stop_order)
            session.flush()

            session.add(
                BrokerOrderEventRecord(
                    broker_order_id=stop_order.id,
                    event_type="order_error_callback",
                    event_at=datetime(2026, 5, 4, 12, 22, 50, tzinfo=timezone.utc),
                    status_before="Cancelled",
                    status_after="Cancelled",
                    payload={
                        "orderId": 4840,
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

        self.assertEqual(len(snapshot.recent_broker_attention), 1)
        self.assertEqual(
            snapshot.recent_broker_attention[0].message,
            "[202] Order Canceled - reason:",
        )

    def test_build_operator_dashboard_snapshot_dedupes_replaced_open_order_lineage(self) -> None:
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
                submit_at=datetime(2026, 4, 21, 7, 20, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 21, 15, 30, tzinfo=timezone.utc),
                order_type="LMT",
                side="BUY",
                payload={},
            )
            session.add(instruction)
            session.flush()

            session.add_all(
                [
                    BrokerOrderRecord(
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        order_role="EXIT",
                        external_order_id="3952",
                        external_perm_id="449407988",
                        external_client_id="0",
                        order_ref=f"{instruction.instruction_id}:exit:forced",
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
                    ),
                    BrokerOrderRecord(
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        order_role="EXIT",
                        external_order_id="3953",
                        external_perm_id="449407988",
                        external_client_id="0",
                        order_ref=f"{instruction.instruction_id}:exit:forced",
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
                        submitted_at=datetime(2026, 4, 23, 6, 31, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 23, 6, 31, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    ),
                ]
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

        volcar_orders = [row for row in snapshot.open_orders if row.symbol == "VOLCAR.B"]
        self.assertEqual(len(volcar_orders), 1)
        self.assertEqual(volcar_orders[0].external_order_id, "3953")

    def test_build_operator_dashboard_snapshot_hides_effectively_closed_exit_orders(self) -> None:
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

            volcar_instruction = InstructionRecord(
                instruction_id="2026-04-21-U25245596-long_risk_book-VOLCAR B-long-01",
                schema_version="2026-04-10",
                source_system="test",
                batch_id="batch-volcar",
                account_key="U25245596",
                book_key="long_risk_book",
                symbol="VOLCAR.B",
                exchange="SMART",
                currency="SEK",
                state="EXIT_PENDING",
                submit_at=datetime(2026, 4, 21, 7, 20, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 21, 15, 30, tzinfo=timezone.utc),
                order_type="LMT",
                side="BUY",
                payload={},
            )
            sive_instruction = InstructionRecord(
                instruction_id="2026-04-20-U25245596-manual_delayed_sive-buy-01",
                schema_version="2026-04-10",
                source_system="manual-test",
                batch_id="batch-sive",
                account_key="U25245596",
                book_key="manual_delayed_sive",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                state="EXIT_PENDING",
                submit_at=datetime(2026, 4, 20, 13, 55, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 20, 13, 58, tzinfo=timezone.utc),
                order_type="MKT",
                side="BUY",
                payload={},
            )
            session.add_all([volcar_instruction, sive_instruction])
            session.flush()

            session.add_all(
                [
                    BrokerOrderRecord(
                        instruction_id=volcar_instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        order_role="EXIT",
                        external_order_id="3953",
                        external_perm_id="449407988",
                        external_client_id="0",
                        order_ref=f"{volcar_instruction.instruction_id}:exit:forced",
                        symbol="VOLCAR.B",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        primary_exchange="SFB",
                        local_symbol="VOLCAR B",
                        side="SELL",
                        order_type="MKT",
                        time_in_force="DAY",
                        status="PendingCancel",
                        total_quantity="827",
                        submitted_at=datetime(2026, 4, 23, 6, 31, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 23, 7, 44, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    ),
                    BrokerOrderRecord(
                        instruction_id=sive_instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        order_role="EXIT",
                        external_order_id="38",
                        external_perm_id="156906838",
                        external_client_id="0",
                        order_ref=f"{sive_instruction.instruction_id}:exit:delayed_limit",
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
                        limit_price="32.7",
                        submitted_at=datetime(2026, 4, 20, 13, 58, tzinfo=timezone.utc),
                        last_status_at=datetime(2026, 4, 21, 15, 15, 47, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    ),
                    PositionSnapshotRecord(
                        broker_account_id=broker_account.id,
                        snapshot_at=datetime(2026, 4, 23, 19, 42, tzinfo=timezone.utc),
                        source="runtime_snapshot",
                        symbol="VOLCAR.B",
                        exchange="SFB",
                        currency="SEK",
                        security_type="STK",
                        primary_exchange=None,
                        local_symbol="VOLCAR B",
                        quantity="0",
                        average_cost="0.0",
                        market_price="22.55",
                        market_value="0.0",
                        unrealized_pnl="0",
                        realized_pnl="-635.55",
                    ),
                    ExecutionFillRecord(
                        broker_order_id=None,
                        instruction_id=sive_instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        external_execution_id="00014800.69e72208.01.01",
                        external_order_id="67",
                        external_perm_id="156906838",
                        order_ref=f"{sive_instruction.instruction_id}:exit:delayed_limit",
                        symbol="SIVE",
                        exchange="SFB",
                        currency="SEK",
                        security_type="STK",
                        side="SLD",
                        quantity="1",
                        price="32.7",
                        commission="49.0",
                        commission_currency="SEK",
                        executed_at=datetime(2026, 4, 21, 15, 15, 58, tzinfo=timezone.utc),
                        raw_payload={},
                    ),
                ]
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

        symbols = {row.symbol for row in snapshot.open_orders}
        self.assertNotIn("VOLCAR.B", symbols)
        self.assertNotIn("SIVE", symbols)

    def test_open_orders_are_not_starved_by_recent_closed_rows(self) -> None:
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
            session.add(
                BrokerOrderRecord(
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="U25245596",
                    order_role="ENTRY",
                    external_order_id="open-1",
                    external_perm_id="open-perm-1",
                    external_client_id="0",
                    order_ref="old-open-order",
                    symbol="SAAB",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    side="BUY",
                    order_type="LMT",
                    time_in_force="DAY",
                    status="Submitted",
                    total_quantity="2",
                    limit_price="100.00",
                    submitted_at=datetime(2026, 4, 19, 7, 0, tzinfo=timezone.utc),
                    last_status_at=datetime(2026, 4, 19, 7, 0, tzinfo=timezone.utc),
                    raw_payload={},
                    metadata_json={},
                )
            )
            for index in range(20):
                session.add(
                    BrokerOrderRecord(
                        broker_account_id=broker_account.id,
                        broker_kind="IBKR",
                        account_key="U25245596",
                        order_role="ENTRY",
                        external_order_id=f"closed-{index}",
                        external_perm_id=f"closed-perm-{index}",
                        external_client_id="0",
                        order_ref=f"closed-order-{index}",
                        symbol="ERIC B",
                        exchange="SMART",
                        currency="SEK",
                        security_type="STK",
                        side="BUY",
                        order_type="LMT",
                        time_in_force="DAY",
                        status="Filled",
                        total_quantity="1",
                        limit_price="80.00",
                        submitted_at=datetime(
                            2026, 4, 19, 7, index + 1, tzinfo=timezone.utc
                        ),
                        last_status_at=datetime(
                            2026, 4, 19, 7, index + 1, tzinfo=timezone.utc
                        ),
                        raw_payload={},
                        metadata_json={},
                    )
                )
            session.commit()
        finally:
            session.close()

        snapshot = build_operator_dashboard_snapshot(
            self.session_factory,
            order_limit=1,
            fill_limit=10,
            attention_limit=10,
            reconciliation_run_limit=10,
        )

        self.assertEqual(len(snapshot.open_orders), 1)
        self.assertEqual(snapshot.open_orders[0].order_ref, "old-open-order")

    def test_partially_filled_exit_order_remains_open(self) -> None:
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
            broker_order = BrokerOrderRecord(
                broker_account_id=broker_account.id,
                broker_kind="IBKR",
                account_key="U25245596",
                order_role="EXIT",
                external_order_id="exit-1",
                external_perm_id="exit-perm-1",
                external_client_id="0",
                order_ref="position-1:exit:forced",
                symbol="SAAB",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                side="SELL",
                order_type="LMT",
                time_in_force="DAY",
                status="Submitted",
                total_quantity="10",
                limit_price="105.00",
                submitted_at=datetime(2026, 4, 19, 7, 0, tzinfo=timezone.utc),
                last_status_at=datetime(2026, 4, 19, 7, 0, tzinfo=timezone.utc),
                raw_payload={},
                metadata_json={},
            )
            session.add(broker_order)
            session.flush()
            session.add(
                ExecutionFillRecord(
                    broker_order_id=broker_order.id,
                    instruction_id=None,
                    broker_account_id=broker_account.id,
                    broker_kind="IBKR",
                    account_key="U25245596",
                    external_execution_id="partial-exit-fill",
                    external_order_id="exit-1",
                    external_perm_id="exit-perm-1",
                    order_ref="position-1:exit:forced",
                    symbol="SAAB",
                    exchange="SMART",
                    currency="SEK",
                    security_type="STK",
                    side="SLD",
                    quantity="4",
                    price="105.00",
                    commission="1.00",
                    commission_currency="SEK",
                    executed_at=datetime(2026, 4, 19, 7, 1, tzinfo=timezone.utc),
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

        self.assertEqual(len(snapshot.open_orders), 1)
        self.assertEqual(snapshot.open_orders[0].external_order_id, "exit-1")
