from __future__ import annotations

from tests._virtual_trading_shared import *  # noqa: F401,F403


class VirtualTradingTests01(VirtualTradingTestsBase):
    def test_virtual_broker_ids_fit_instruction_columns(self) -> None:
        for _ in range(100):
            order_id = _new_virtual_order_id()
            perm_id = _new_virtual_perm_id(order_id)

            self.assertLessEqual(order_id, 2_147_483_647)
            self.assertLessEqual(perm_id, 2_147_483_647)
            self.assertGreater(order_id, 0)
            self.assertGreater(perm_id, 0)

    def test_rl_decision_bar_quote_only_fills_matching_virtual_owner(self) -> None:
        session = self.session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind=BROKER_KIND_VIRTUAL,
                account_key="VIRTUALRL02",
                base_currency="SEK",
                is_virtual=True,
                metadata_json={"virtual_account": True},
            )
            session.add(broker_account)
            session.flush()

            def add_order(
                *,
                instruction_id: str,
                deployment_key: str,
                source_instruction_id: str,
                side: str,
                external_order_id: str,
            ) -> None:
                instruction = InstructionRecord(
                    instruction_id=instruction_id,
                    schema_version="2026-04-10",
                    source_system="rl-runner",
                    batch_id=f"{instruction_id}-batch",
                    account_key="VIRTUALRL02",
                    book_key=f"{deployment_key}-book",
                    is_virtual=True,
                    symbol="SIVE",
                    exchange="XSTO",
                    currency="SEK",
                    state=ExecutionState.ENTRY_SUBMITTED.value,
                    submit_at=datetime(2026, 4, 27, 7, 0, tzinfo=timezone.utc),
                    expire_at=datetime(2026, 4, 27, 15, 30, tzinfo=timezone.utc),
                    order_type="MARKET",
                    side=side,
                    payload={
                        "instruction": {
                            "trace": {
                                "metadata": {
                                    "rl_deployment_key": deployment_key,
                                    "rl_source_instruction_id": source_instruction_id,
                                }
                            }
                        }
                    },
                )
                session.add(instruction)
                session.flush()
                session.add(
                    BrokerOrderRecord(
                        instruction_id=instruction.id,
                        broker_account_id=broker_account.id,
                        broker_kind=BROKER_KIND_VIRTUAL,
                        account_key="VIRTUALRL02",
                        is_virtual=True,
                        order_role="ENTRY",
                        external_order_id=external_order_id,
                        external_perm_id=str(int(external_order_id) + 1000),
                        order_ref=instruction_id,
                        symbol="SIVE",
                        exchange="XSTO",
                        currency="SEK",
                        security_type="STK",
                        side=side,
                        order_type="MKT",
                        time_in_force="DAY",
                        status="Submitted",
                        total_quantity="10",
                        submitted_at=datetime(2026, 4, 27, 7, 0, tzinfo=timezone.utc),
                        raw_payload={},
                        metadata_json={},
                    )
                )

            add_order(
                instruction_id="long-order",
                deployment_key="long_dep",
                source_instruction_id="long-source",
                side="BUY",
                external_order_id="1001",
            )
            add_order(
                instruction_id="short-order",
                deployment_key="short_dep",
                source_instruction_id="short-source",
                side="SELL",
                external_order_id="1002",
            )
            session.commit()
        finally:
            session.close()

        result = record_virtual_market_quote(
            self.session_factory,
            account_key="VIRTUALRL02",
            symbol="SIVE",
            exchange="XSTO",
            currency="SEK",
            security_type="STK",
            bid_price=Decimal("10.00"),
            ask_price=Decimal("10.00"),
            last_price=Decimal("10.00"),
            observed_at=datetime(2026, 4, 27, 7, 1, tzinfo=timezone.utc),
            source="rl_decision_bar",
            metadata={
                "deployment_key": "long_dep",
                "source_instruction_id": "long-source",
                "purpose": "virtual_same_bar_fill_parity",
            },
        )

        self.assertEqual(result["filled_order_count"], 1)
        self.assertEqual(result["filled_orders"][0]["order_ref"], "long-order")

        session = self.session_factory()
        try:
            orders = {
                order.order_ref: order.status
                for order in session.execute(
                    select(BrokerOrderRecord).order_by(BrokerOrderRecord.order_ref)
                ).scalars()
            }
            self.assertEqual(orders["long-order"], "FILLED")
            self.assertEqual(orders["short-order"], "Submitted")
        finally:
            session.close()

    def test_scoped_rl_decision_bar_quote_does_not_price_other_virtual_exit(self) -> None:
        payload = deepcopy(_virtual_payload())
        payload["source"]["system"] = "rl-runner"
        instruction_payload = payload["instructions"][0]
        instruction_payload["instruction_id"] = "long-generated-order"
        instruction_payload["account"]["account_key"] = "VIRTUALRL02"
        instruction_payload["trace"]["metadata"] = {
            "rl_deployment_key": "long_dep",
            "rl_source_instruction_id": "long-source",
        }
        instruction = parse_execution_batch_payload(payload).instructions[0]

        ensure_virtual_account_record(
            self.session_factory,
            account_key="VIRTUALRL02",
            cash_balance=Decimal("500000"),
        )
        record_virtual_market_quote(
            self.session_factory,
            account_key="VIRTUALRL02",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            security_type="STK",
            last_price=Decimal("10.00"),
            bid_price=Decimal("10.00"),
            ask_price=Decimal("10.05"),
            observed_at=datetime(2026, 4, 27, 7, 0, tzinfo=timezone.utc),
            source="ibkr_live_market_stream_virtual_bridge",
        )
        record_virtual_market_quote(
            self.session_factory,
            account_key="VIRTUALRL02",
            symbol="SIVE",
            exchange="SMART",
            currency="SEK",
            security_type="STK",
            last_price=Decimal("99.00"),
            bid_price=Decimal("99.00"),
            ask_price=Decimal("99.05"),
            observed_at=datetime(2026, 4, 27, 7, 1, tzinfo=timezone.utc),
            source="rl_decision_bar",
            metadata={
                "deployment_key": "short_dep",
                "source_instruction_id": "short-source",
                "purpose": "virtual_same_bar_fill_parity",
            },
        )

        submission = submit_virtual_exit_order(
            self.session_factory,
            self.config,
            instruction,
            quantity=Decimal("10"),
            order_type="MARKET",
            order_ref="long-generated-order:exit:rl_market",
        )

        quote = submission["virtual_execution"]["market_quote"]
        self.assertEqual(quote["last_price"], "10.00")
        self.assertNotEqual(quote["metadata"].get("deployment_key"), "short_dep")

    def test_virtual_trading_round_trip_uses_market_watch_and_fixed_fee(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            _write_schedule_fixture(schedule_path)
            batch = parse_execution_batch_payload(_virtual_payload())

            record_virtual_market_quote(
                self.session_factory,
                account_key="virtual0001",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                last_price=Decimal("10.00"),
                ask_price=Decimal("10.00"),
                bid_price=Decimal("9.95"),
                observed_at=datetime(2026, 4, 27, 7, 0, tzinfo=timezone.utc),
                source="test",
            )
            submit_execution_batch(
                self.session_factory,
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )

            first_cycle = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 1, tzinfo=timezone.utc),
            )
            self.assertEqual(len(first_cycle.submitted_entries), 1)

            second_cycle = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 2, tzinfo=timezone.utc),
            )
            self.assertEqual(len(second_cycle.filled_entries), 1)
            self.assertEqual(len(second_cycle.submitted_exits), 1)

            quote_result = record_virtual_market_quote(
                self.session_factory,
                account_key="VIRTUAL0001",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                last_price=Decimal("11.50"),
                bid_price=Decimal("11.50"),
                ask_price=Decimal("11.55"),
                observed_at=datetime(2026, 4, 27, 7, 3, tzinfo=timezone.utc),
                source="test",
            )
            self.assertEqual(quote_result["filled_order_count"], 1)

            third_cycle = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 4, tzinfo=timezone.utc),
            )
            self.assertEqual(len(third_cycle.completed_instructions), 1)

        session = self.session_factory()
        try:
            instruction = session.execute(select(InstructionRecord)).scalar_one()
            broker_account = session.execute(select(BrokerAccountRecord)).scalar_one()
            orders = session.execute(
                select(BrokerOrderRecord).order_by(BrokerOrderRecord.id)
            ).scalars().all()
            fills = session.execute(
                select(ExecutionFillRecord).order_by(ExecutionFillRecord.id)
            ).scalars().all()
            positions = session.execute(
                select(PositionSnapshotRecord).order_by(PositionSnapshotRecord.id)
            ).scalars().all()
            account_snapshots = session.execute(
                select(AccountSnapshotRecord).order_by(AccountSnapshotRecord.id)
            ).scalars().all()

            self.assertEqual(instruction.state, ExecutionState.COMPLETED.value)
            self.assertTrue(instruction.is_virtual)
            self.assertEqual(broker_account.broker_kind, BROKER_KIND_VIRTUAL)
            self.assertTrue(broker_account.is_virtual)
            self.assertEqual(len(orders), 2)
            self.assertTrue(all(order.is_virtual for order in orders))
            self.assertEqual([order.order_role for order in orders], ["ENTRY", "EXIT"])
            self.assertEqual([order.status for order in orders], ["FILLED", "FILLED"])
            self.assertEqual(len(fills), 2)
            self.assertTrue(all(fill.is_virtual for fill in fills))
            self.assertEqual([fill.quantity for fill in fills], ["100", "100"])
            self.assertEqual([fill.commission for fill in fills], ["15", "15"])
            self.assertEqual([fill.commission_currency for fill in fills], ["SEK", "SEK"])
            self.assertEqual(positions[-1].quantity, "0")
            self.assertTrue(positions[-1].is_virtual)
            self.assertTrue(account_snapshots[-1].is_virtual)
            self.assertEqual(account_snapshots[-1].total_cash_value, "120.00")
            self.assertEqual(account_snapshots[-1].net_liquidation, "120.00")
        finally:
            session.close()

    def test_virtual_exit_fill_cancels_open_exit_sibling(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            _write_schedule_fixture(schedule_path)
            payload = deepcopy(_virtual_payload())
            payload["instructions"][0]["exit"]["catastrophic_stop_loss_pct"] = "0.15"
            batch = parse_execution_batch_payload(payload)

            record_virtual_market_quote(
                self.session_factory,
                account_key="virtual0001",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                last_price=Decimal("10.00"),
                ask_price=Decimal("10.00"),
                bid_price=Decimal("9.95"),
                observed_at=datetime(2026, 4, 27, 7, 0, tzinfo=timezone.utc),
                source="test",
            )
            submit_execution_batch(
                self.session_factory,
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )
            run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 1, tzinfo=timezone.utc),
            )
            second_cycle = run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 2, tzinfo=timezone.utc),
            )
            self.assertEqual(len(second_cycle.submitted_exits), 2)

            quote_result = record_virtual_market_quote(
                self.session_factory,
                account_key="VIRTUAL0001",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                last_price=Decimal("11.50"),
                bid_price=Decimal("11.50"),
                ask_price=Decimal("11.55"),
                observed_at=datetime(2026, 4, 27, 7, 3, tzinfo=timezone.utc),
                source="test",
            )
            self.assertEqual(quote_result["filled_order_count"], 1)

        session = self.session_factory()
        try:
            exit_orders = session.execute(
                select(BrokerOrderRecord)
                .where(BrokerOrderRecord.order_role == "EXIT")
                .order_by(BrokerOrderRecord.order_ref)
            ).scalars().all()
            events = session.execute(
                select(BrokerOrderEventRecord)
                .where(BrokerOrderEventRecord.event_type == "virtual_exit_sibling_cancelled")
            ).scalars().all()

            self.assertEqual(len(exit_orders), 2)
            self.assertEqual(
                sorted(order.status for order in exit_orders),
                ["Cancelled", "FILLED"],
            )
            self.assertEqual(len(events), 1)
        finally:
            session.close()

    def test_virtual_entry_target_notional_converts_to_whole_share_quantity(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            _write_schedule_fixture(schedule_path)
            payload = deepcopy(_virtual_payload())
            instruction_payload = payload["instructions"][0]
            instruction_payload["sizing"] = {
                "mode": "target_notional",
                "target_notional": "1000",
            }
            batch = parse_execution_batch_payload(payload)

            record_virtual_market_quote(
                self.session_factory,
                account_key="virtual0001",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                last_price=Decimal("10.40"),
                ask_price=Decimal("10.40"),
                bid_price=Decimal("10.35"),
                observed_at=datetime(2026, 4, 27, 7, 0, tzinfo=timezone.utc),
                source="test",
            )
            submit_execution_batch(
                self.session_factory,
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )

            run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 1, tzinfo=timezone.utc),
            )
            run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 2, tzinfo=timezone.utc),
            )

        session = self.session_factory()
        try:
            order = session.execute(
                select(BrokerOrderRecord).where(BrokerOrderRecord.order_role == "ENTRY")
            ).scalar_one()
            fill = session.execute(select(ExecutionFillRecord)).scalar_one()
            self.assertEqual(order.total_quantity, "95")
            self.assertEqual(fill.quantity, "95")
            self.assertEqual(
                order.metadata_json["broker_submission"]["virtual_execution"]["sizing"][
                    "target_notional"
                ],
                "1000",
            )
        finally:
            session.close()

    def test_virtual_short_entry_does_not_credit_short_sale_proceeds_to_account_cash(
        self,
    ) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            _write_schedule_fixture(schedule_path)
            ensure_virtual_account_record(
                self.session_factory,
                account_key="virtual0001",
                cash_balance=Decimal("200000"),
                snapshot_at=datetime(2026, 4, 27, 6, 55, tzinfo=timezone.utc),
            )
            payload = deepcopy(_virtual_payload())
            instruction_payload = payload["instructions"][0]
            instruction_payload["instruction_id"] = "virtual-sive-short-1"
            instruction_payload["intent"] = {
                "side": "SELL",
                "position_side": "SHORT",
            }
            instruction_payload["account"]["book_side"] = "SHORT"
            instruction_payload["sizing"] = {
                "mode": "target_notional",
                "target_notional": "1000",
            }
            batch = parse_execution_batch_payload(payload)

            record_virtual_market_quote(
                self.session_factory,
                account_key="virtual0001",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                last_price=Decimal("10.60"),
                ask_price=Decimal("10.65"),
                bid_price=Decimal("10.60"),
                observed_at=datetime(2026, 4, 27, 7, 0, tzinfo=timezone.utc),
                source="test",
            )
            submit_execution_batch(
                self.session_factory,
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )
            run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 1, tzinfo=timezone.utc),
            )
            run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 2, tzinfo=timezone.utc),
            )

        session = self.session_factory()
        try:
            fill = session.execute(select(ExecutionFillRecord)).scalar_one()
            snapshots = session.execute(
                select(AccountSnapshotRecord).order_by(AccountSnapshotRecord.id)
            ).scalars().all()
            self.assertEqual(fill.quantity, "95")
            self.assertEqual(snapshots[-1].total_cash_value, "199985")
            self.assertEqual(snapshots[-1].buying_power, "199985.00")
        finally:
            session.close()

    def test_virtual_quote_marks_open_positions_and_account_nav(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            _write_schedule_fixture(schedule_path)
            ensure_virtual_account_record(
                self.session_factory,
                account_key="virtual0001",
                cash_balance=Decimal("200000"),
                snapshot_at=datetime(2026, 4, 27, 6, 55, tzinfo=timezone.utc),
            )
            batch = parse_execution_batch_payload(_virtual_payload())

            record_virtual_market_quote(
                self.session_factory,
                account_key="virtual0001",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                last_price=Decimal("10.00"),
                ask_price=Decimal("10.00"),
                bid_price=Decimal("9.95"),
                observed_at=datetime(2026, 4, 27, 7, 0, tzinfo=timezone.utc),
                source="test",
            )
            submit_execution_batch(
                self.session_factory,
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )
            run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 1, tzinfo=timezone.utc),
            )
            run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 2, tzinfo=timezone.utc),
            )

            mark_result = record_virtual_market_quote(
                self.session_factory,
                account_key="virtual0001",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                last_price=Decimal("10.50"),
                bid_price=Decimal("10.50"),
                ask_price=Decimal("10.55"),
                observed_at=datetime(2026, 4, 27, 7, 3, tzinfo=timezone.utc),
                source="test",
            )
            self.assertEqual(mark_result["filled_order_count"], 0)

        session = self.session_factory()
        try:
            position = session.execute(
                select(PositionSnapshotRecord)
                .where(PositionSnapshotRecord.symbol == "SIVE")
                .order_by(PositionSnapshotRecord.id.desc())
                .limit(1)
            ).scalar_one()
            account_snapshot = session.execute(
                select(AccountSnapshotRecord).order_by(AccountSnapshotRecord.id.desc()).limit(1)
            ).scalar_one()
            self.assertEqual(position.source, "virtual_market_mark")
            self.assertEqual(position.market_price, "10.50")
            self.assertEqual(position.unrealized_pnl, "50.00")
            self.assertEqual(account_snapshot.total_cash_value, "199985")
            self.assertEqual(account_snapshot.net_liquidation, "200035.00")
        finally:
            session.close()

    def test_virtual_position_snapshots_keep_rl_owner_metadata(self) -> None:
        with TemporaryDirectory() as temp_dir:
            schedule_path = Path(temp_dir) / "day_sessions.csv"
            _write_schedule_fixture(schedule_path)
            ensure_virtual_account_record(
                self.session_factory,
                account_key="virtual0001",
                cash_balance=Decimal("200000"),
                snapshot_at=datetime(2026, 4, 27, 6, 55, tzinfo=timezone.utc),
            )
            payload = _virtual_payload()
            instruction = payload["instructions"][0]
            instruction["source"] = "rl-runner"
            instruction["trace"]["metadata"] = {
                "rl_deployment_key": "long_trial_106_virtual_shared_01",
                "rl_source_instruction_id": "candidate-sive-1",
            }
            batch = parse_execution_batch_payload(payload)

            record_virtual_market_quote(
                self.session_factory,
                account_key="virtual0001",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                last_price=Decimal("10.00"),
                ask_price=Decimal("10.00"),
                bid_price=Decimal("9.95"),
                observed_at=datetime(2026, 4, 27, 7, 0, tzinfo=timezone.utc),
                source="test",
            )
            submit_execution_batch(
                self.session_factory,
                batch,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
            )
            run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 1, tzinfo=timezone.utc),
            )
            run_runtime_cycle(
                self.session_factory,
                self.config,
                runtime_timezone="Europe/Stockholm",
                session_calendar_path=schedule_path,
                now=datetime(2026, 4, 27, 7, 2, tzinfo=timezone.utc),
            )

        session = self.session_factory()
        try:
            position = session.execute(
                select(PositionSnapshotRecord)
                .where(PositionSnapshotRecord.symbol == "SIVE")
                .order_by(PositionSnapshotRecord.id.desc())
                .limit(1)
            ).scalar_one()
            self.assertEqual(position.owner_instruction_id, "virtual-sive-roundtrip-1")
            self.assertEqual(
                position.owner_deployment_key,
                "long_trial_106_virtual_shared_01",
            )
            self.assertEqual(position.owner_source_instruction_id, "candidate-sive-1")
            self.assertEqual(position.owner_book_key, "rl_virtual_book")
            self.assertEqual(
                position.raw_payload["owner"]["owner_deployment_key"],
                "long_trial_106_virtual_shared_01",
            )
        finally:
            session.close()

    def test_rl_virtual_fill_without_owner_metadata_is_rejected(self) -> None:
        session = self.session_factory()
        try:
            broker_account = BrokerAccountRecord(
                broker_kind=BROKER_KIND_VIRTUAL,
                account_key="VIRTUAL0001",
                base_currency="SEK",
                is_virtual=True,
            )
            instruction = InstructionRecord(
                instruction_id="rl-unowned-virtual-1",
                schema_version="2026-04-10",
                source_system="rl-runner",
                batch_id="batch-1",
                account_key="VIRTUAL0001",
                book_key="rl_virtual_book",
                is_virtual=True,
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                state=ExecutionState.ENTRY_SUBMITTED.value,
                submit_at=datetime(2026, 4, 27, 7, 0, tzinfo=timezone.utc),
                expire_at=datetime(2026, 4, 27, 15, 30, tzinfo=timezone.utc),
                order_type="LIMIT",
                side="BUY",
                payload={"instruction": {"trace": {"metadata": {}}}},
            )
            order = BrokerOrderRecord(
                instruction=instruction,
                broker_account=broker_account,
                broker_kind=BROKER_KIND_VIRTUAL,
                account_key="VIRTUAL0001",
                is_virtual=True,
                order_role="ENTRY",
                external_order_id="800000001",
                external_perm_id="900000001",
                order_ref="rl-unowned-virtual-1",
                symbol="SIVE",
                exchange="SMART",
                currency="SEK",
                security_type="STK",
                primary_exchange="SFB",
                side="BUY",
                order_type="LMT",
                status="Submitted",
                total_quantity="100",
            )
            session.add_all([broker_account, instruction, order])
            session.flush()
            with self.assertRaisesRegex(ValueError, "owner_deployment_key"):
                persist_virtual_execution_fill(
                    session,
                    broker_order=order,
                    instruction_record=instruction,
                    fill_payload={
                        "external_execution_id": "virtual-unowned-fill-1",
                        "external_order_id": "800000001",
                        "external_perm_id": "900000001",
                        "order_ref": "rl-unowned-virtual-1",
                        "side": "BOT",
                        "quantity": "100",
                        "price": "10.00",
                        "commission": "15",
                        "commission_currency": "SEK",
                        "executed_at": datetime(2026, 4, 27, 7, 2, tzinfo=timezone.utc),
                    },
                    observed_at=datetime(2026, 4, 27, 7, 2, tzinfo=timezone.utc),
                )
        finally:
            session.rollback()
            session.close()
