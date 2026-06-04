from __future__ import annotations

from tests._order_execution_shared import *  # noqa: F401,F403
from ibkr_trader.ibkr.order_contract_queries import build_order_contract_query


class OrderExecutionTests01(OrderExecutionTestsBase):
    def test_order_contract_query_normalizes_stockholm_share_class_symbols(self) -> None:
        payload = _base_payload()
        instruction_payload = payload["instructions"][0]
        instruction_payload["instrument"] = {
            "symbol": "HEXA B",
            "security_type": "STK",
            "exchange": "SMART",
            "currency": "SEK",
            "primary_exchange": "SFB",
        }
        instruction = parse_execution_batch_payload(payload).instructions[0]

        query = build_order_contract_query(instruction)

        self.assertEqual(query.symbol, "HEXA.B")
        self.assertEqual(query.local_symbol, "HEXA B")
        self.assertEqual(query.primary_exchange, "SFB")

    def test_order_contract_query_leaves_non_share_class_stockholm_symbols_alone(self) -> None:
        payload = _base_payload()
        instruction_payload = payload["instructions"][0]
        instruction_payload["instrument"] = {
            "symbol": "NOKIA SEK",
            "security_type": "STK",
            "exchange": "SMART",
            "currency": "SEK",
            "primary_exchange": "SFB",
        }
        instruction = parse_execution_batch_payload(payload).instructions[0]

        query = build_order_contract_query(instruction)

        self.assertEqual(query.symbol, "NOKIA SEK")
        self.assertIsNone(query.local_symbol)

    def test_submit_order_from_batch_resolves_stockholm_display_share_class_symbol(self) -> None:
        class _StockholmShareClassWrapper(_FakeOrderExecutionSyncWrapper):
            def get_account_updates(
                self,
                account_code: str = "",
                timeout: int = 10,
            ) -> dict[str, object]:
                return {
                    "portfolio": [],
                    "account_values": {
                        "DU1234567": {
                            "NetLiquidation": {"value": "100000.00", "currency": "SEK"},
                            "TotalCashValue": {"value": "100000.00", "currency": "SEK"},
                            "BuyingPower": {"value": "200000.00", "currency": "SEK"},
                            "AvailableFunds": {"value": "100000.00", "currency": "SEK"},
                            "ExcessLiquidity": {"value": "100000.00", "currency": "SEK"},
                            "AccountType": {"value": "INDIVIDUAL", "currency": ""},
                        }
                    },
                }

            def get_contract_details(
                self,
                contract: _FakeContract,
                timeout: int | None = None,
            ) -> list[object]:
                if contract.symbol != "HEXA.B" or contract.localSymbol != "HEXA B":
                    return []
                return [
                    SimpleNamespace(
                        contract=SimpleNamespace(
                            conId=490414358,
                            symbol=contract.symbol,
                            localSymbol=contract.localSymbol,
                            secType=contract.secType,
                            exchange="SMART",
                            primaryExchange=contract.primaryExchange,
                            currency=contract.currency,
                            tradingClass="HEXA.B",
                        ),
                        marketName="HEXA.B",
                        minTick=0.0001,
                        validExchanges="SMART,SFB,EUIBSI",
                        marketRuleIds="26,1875,1876",
                        orderTypes="LMT,MKT,WHATIF,GTC",
                        timeZoneId="MET",
                        tradingHours="20260603:0900-20260603:1730",
                        liquidHours="20260603:0900-20260603:1730",
                        stockType="COMMON",
                        industry="Industrial",
                        category="Machinery-Diversified",
                        subcategory="Machinery-General Indust",
                        longName="HEXAGON AB-B SHS",
                        secIdList=[SimpleNamespace(tag="ISIN", value="SE0015961909")],
                    )
                ]

            def get_market_rule(self, market_rule_id: int, timeout: int = 5) -> list[object]:
                return [SimpleNamespace(lowEdge=0, increment=0.02)]

        payload = _base_payload()
        instruction_payload = payload["instructions"][0]
        instruction_payload["instrument"] = {
            "symbol": "HEXA B",
            "security_type": "STK",
            "exchange": "SMART",
            "currency": "SEK",
            "primary_exchange": "SFB",
        }
        instruction_payload["sizing"] = {
            "mode": "target_quantity",
            "target_quantity": "1",
            "funding_basis": "cash",
        }
        instruction_payload["entry"]["limit_price"] = "50.00"
        instruction_payload["entry"]["time_in_force"] = "GTC"
        batch = parse_execution_batch_payload(payload)

        result = submit_order_from_batch(
            self.config,
            batch,
            sync_wrapper_cls=_StockholmShareClassWrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
            order_cls=_FakeOrder,
        )

        self.assertEqual(result["resolved_contract"]["symbol"], "HEXA.B")
        self.assertEqual(result["resolved_contract"]["local_symbol"], "HEXA B")
        self.assertEqual(result["order"]["time_in_force"], "GTC")
        self.assertEqual(result["order"]["limit_price"], "50.00")
        wire_contracts = [
            event["request"]["contract"]
            for event in result["ibkr_wire_audit"]
            if event["event_type"] == "outbound_order_request"
        ]
        self.assertEqual(wire_contracts[0]["symbol"], "HEXA.B")
        self.assertEqual(wire_contracts[-1]["symbol"], "HEXA.B")

    def test_submit_order_from_batch_builds_limit_order(self) -> None:
        batch = parse_execution_batch_payload(_base_payload())

        result = submit_order_from_batch(
            self.config,
            batch,
            sync_wrapper_cls=_FakeOrderExecutionSyncWrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
            order_cls=_FakeOrder,
        )

        self.assertEqual(result["instruction_id"], "ny-paper-1")
        self.assertEqual(result["account"], "DU1234567")
        self.assertEqual(result["resolved_contract"]["con_id"], 265598)
        self.assertEqual(result["order"]["action"], "BUY")
        self.assertEqual(result["order"]["order_type"], "LMT")
        self.assertEqual(result["order"]["time_in_force"], "DAY")
        self.assertEqual(result["order"]["limit_price"], "120.00")
        self.assertEqual(result["order"]["total_quantity"], "10")
        self.assertEqual(result["broker_order_status"]["status"], "Submitted")
        self.assertEqual(result["tws_submission"]["source"], "openOrder")
        self.assertEqual(result["tws_submission"]["order_state"]["status"], "Inactive")
        self.assertEqual(
            result["tws_submission"]["order_state"]["warning_text"],
            "Order held in TWS pending manual transmit.",
        )
        self.assertEqual(
            [event["request"]["stage"] for event in result["ibkr_wire_audit"]],
            ["what_if_preflight", "live_order_submit"],
        )
        self.assertEqual(
            result["ibkr_wire_audit"][1]["request"]["order"]["order_ref"],
            "ny-paper-1",
        )
        self.assertEqual(
            result["ibkr_wire_audit"][1]["request"]["order"]["limit_price"],
            "120.0",
        )

    def test_submit_order_from_batch_normalizes_limit_price_to_market_rule(self) -> None:
        payload = _base_payload()
        payload["instructions"][0]["entry"]["limit_price"] = "23.5417"
        batch = parse_execution_batch_payload(payload)

        result = submit_order_from_batch(
            self.config,
            batch,
            sync_wrapper_cls=_FakeOrderExecutionSyncWrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
            order_cls=_FakeOrder,
        )

        self.assertEqual(result["order"]["limit_price"], "23.54")
        self.assertEqual(result["order"]["price_increment"], "0.01")
        self.assertIn(
            "Entry limit price was normalized to the nearest valid IBKR tick increment.",
            result["warnings"],
        )

    def test_submit_exit_order_uses_stockholm_tick_fallback_when_market_rule_times_out(self) -> None:
        class _StockholmMarketRuleTimeoutWrapper(_FakeOrderExecutionSyncWrapper):
            def get_contract_details(
                self,
                contract: _FakeContract,
                timeout: int | None = None,
            ) -> list[object]:
                return [
                    SimpleNamespace(
                        contract=SimpleNamespace(
                            conId=492734118,
                            symbol="NIBE.B",
                            localSymbol="NIBE B",
                            secType="STK",
                            exchange="SMART",
                            primaryExchange="SFB",
                            currency="SEK",
                            tradingClass="NIBE.B",
                        ),
                        marketName="NIBE.B",
                        minTick=0.0001,
                        validExchanges="SMART,SFB,EUIBSI",
                        marketRuleIds="26,1875,1876",
                        orderTypes="LMT,MKT,STP",
                        timeZoneId="MET",
                        tradingHours="20260506:0900-20260506:1730",
                        liquidHours="20260506:0900-20260506:1730",
                        stockType="COMMON",
                        industry="Industrial",
                        category="Building Materials",
                        subcategory="Bldg Prod-Air&Heating",
                        longName="NIBE INDUSTRIER AB-B SHS",
                        secIdList=[SimpleNamespace(tag="ISIN", value="SE0015988019")],
                    )
                ]

            def get_market_rule(self, market_rule_id: int, timeout: int = 5) -> list[object]:
                raise TimeoutError("market rule lookup timed out")

        payload = _base_payload()
        instruction_payload = payload["instructions"][0]
        instruction_payload["instruction_id"] = "nibe-live-1"
        instruction_payload["instrument"] = {
            "symbol": "NIBE B",
            "security_type": "STK",
            "exchange": "SMART",
            "currency": "SEK",
            "primary_exchange": "SFB",
        }
        instruction_payload["entry"]["limit_price"] = "45.5113"
        batch = parse_execution_batch_payload(payload)

        result = submit_exit_order_from_instruction(
            self.config,
            batch.instructions[0],
            quantity=Decimal("430"),
            order_type=OrderType.LIMIT,
            limit_price=Decimal("46.4202"),
            order_ref="nibe-live-1:exit:take_profit",
            sync_wrapper_cls=_StockholmMarketRuleTimeoutWrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
            order_cls=_FakeOrder,
        )

        self.assertEqual(result["order"]["action"], "SELL")
        self.assertEqual(result["order"]["order_type"], "LMT")
        self.assertEqual(result["order"]["limit_price"], "46.43")
        self.assertEqual(result["order"]["limit_price_increment"], "0.01")
        self.assertIn(
            "Exit limit price was normalized to the nearest valid IBKR tick increment.",
            result["warnings"],
        )

    def test_submit_order_from_batch_builds_market_order(self) -> None:
        payload = _base_payload()
        payload["instructions"][0]["entry"]["order_type"] = "MARKET"
        del payload["instructions"][0]["entry"]["limit_price"]
        batch = parse_execution_batch_payload(payload)

        result = submit_order_from_batch(
            self.config,
            batch,
            sync_wrapper_cls=_FakeOrderExecutionSyncWrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
            order_cls=_FakeOrder,
        )

        self.assertEqual(result["order"]["order_type"], "MKT")
        self.assertIsNone(result["order"]["limit_price"])
        self.assertIsNone(result["order"]["price_increment"])
        self.assertEqual(result["order"]["total_quantity"], "10")

    def test_submit_order_from_batch_reduces_quantity_after_insufficient_funds_reject(self) -> None:
        class _InsufficientFundsWrapper(_FakeOrderExecutionSyncWrapper):
            def place_order_sync(
                self,
                contract: object,
                order: object,
                timeout: int | None = None,
            ) -> dict[str, object]:
                result = super().place_order_sync(contract, order, timeout)
                if int(order.totalQuantity) > 791:
                    self.errors[int(result["orderId"])] = [
                        {
                            "errorCode": 201,
                            "errorString": (
                                "Order rejected - reason:We are unable to accept your order. "
                                "Your Available Funds are in sufficient to cover the change in the "
                                "account's margin requirements if this order executes. In order to "
                                "obtain the desired position your Equity with Loan Value [19468.45 SEK] "
                                "must exceed the new total Initial Margin of  [20319.14 SEK]."
                            ),
                        }
                    ]
                return result

        payload = _base_payload()
        payload["instructions"][0]["sizing"] = {
            "mode": "fraction_of_account_nav",
            "target_fraction_of_account": "1.0",
        }
        batch = parse_execution_batch_payload(payload)

        result = submit_order_from_batch(
            self.config,
            batch,
            sync_wrapper_cls=_InsufficientFundsWrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
            order_cls=_FakeOrder,
        )

        self.assertEqual(result["order"]["limit_price"], "120.00")
        self.assertLessEqual(Decimal(result["order"]["total_quantity"]), Decimal("791"))
        self.assertIn(
            "Entry quantity was reduced after an IBKR insufficient-funds reject.",
            result["warnings"],
        )

    def test_submit_order_from_batch_keeps_non_fatal_ibkr_warning(self) -> None:
        class _WarningWrapper(_FakeOrderExecutionSyncWrapper):
            def place_order_sync(
                self,
                contract: object,
                order: object,
                timeout: int | None = None,
            ) -> dict[str, object]:
                result = super().place_order_sync(contract, order, timeout)
                self.errors[int(result["orderId"])] = [
                    {
                        "errorCode": 399,
                        "errorString": (
                            "Order Message: BUY 10 AAPL Warning: Price is outside "
                            "the current NBBO."
                        ),
                    }
                ]
                return result

        batch = parse_execution_batch_payload(_base_payload())

        result = submit_order_from_batch(
            self.config,
            batch,
            sync_wrapper_cls=_WarningWrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
            order_cls=_FakeOrder,
        )

        self.assertEqual(result["broker_order_status"]["status"], "Submitted")
        self.assertIn("IBKR order warning [399]", " ".join(result["warnings"]))

    def test_submit_order_from_batch_cancels_deferred_next_session_warning(self) -> None:
        class _DeferredWarningWrapper(_FakeOrderExecutionSyncWrapper):
            cancelled_order_ids: list[int] = []

            def place_order_sync(
                self,
                contract: object,
                order: object,
                timeout: int | None = None,
            ) -> dict[str, object]:
                result = super().place_order_sync(contract, order, timeout)
                self.errors[int(result["orderId"])] = [
                    {
                        "errorCode": 399,
                        "errorString": (
                            "Order Message: BUY 10 AAPL Warning: Your order will not be placed "
                            "at the exchange until 2026-04-13 09:30:00 US/Eastern."
                        ),
                    }
                ]
                return result

            def cancel_order_sync(
                self,
                order_id: int,
                orderCancel: object | None = None,
                timeout: int = 3,
            ) -> dict[str, object]:
                self.cancelled_order_ids.append(order_id)
                return super().cancel_order_sync(order_id, orderCancel, timeout)

        batch = parse_execution_batch_payload(_base_payload())

        with self.assertRaisesRegex(
            LookupError,
            "deferred exchange activation beyond the entry trade date",
        ):
            submit_order_from_batch(
                self.config,
                batch,
                sync_wrapper_cls=_DeferredWarningWrapper,
                response_timeout_cls=TimeoutError,
                contract_cls=_FakeContract,
                order_cls=_FakeOrder,
            )

        self.assertEqual(_DeferredWarningWrapper.cancelled_order_ids, [18])

    def test_submit_order_from_batch_blocks_long_fraction_sizing_without_cash(self) -> None:
        class _NoCashWrapper(_FakeOrderExecutionSyncWrapper):
            total_cash_value = "0.00"

            def place_order_sync(
                self,
                contract: object,
                order: object,
                timeout: int | None = None,
            ) -> dict[str, object]:
                raise AssertionError("cash-backed long sizing without cash must not place an order")

        payload = _base_payload()
        payload["instructions"][0]["sizing"] = {
            "mode": "fraction_of_account_nav",
            "target_fraction_of_account": "1.0",
        }
        batch = parse_execution_batch_payload(payload)

        with self.assertRaisesRegex(ValueError, "no positive cash balance available"):
            submit_order_from_batch(
                self.config,
                batch,
                sync_wrapper_cls=_NoCashWrapper,
                response_timeout_cls=TimeoutError,
                contract_cls=_FakeContract,
                order_cls=_FakeOrder,
            )

    def test_submit_order_from_batch_rejects_fractional_stock_quantity(self) -> None:
        payload = _base_payload()
        payload["instructions"][0]["sizing"]["target_quantity"] = "10.5"
        batch = parse_execution_batch_payload(payload)

        with self.assertRaisesRegex(ValueError, "whole-share amount"):
            submit_order_from_batch(
                self.config,
                batch,
                sync_wrapper_cls=_FakeOrderExecutionSyncWrapper,
                response_timeout_cls=TimeoutError,
                contract_cls=_FakeContract,
                order_cls=_FakeOrder,
            )

    def test_submit_order_from_batch_rejects_explicit_short_on_non_shortable_stockholm_account(self) -> None:
        class _SwedenShortWrapper(_FakeOrderExecutionSyncWrapper):
            def get_account_updates(
                self,
                account_code: str = "",
                timeout: int = 10,
            ) -> dict[str, object]:
                return {
                    "portfolio": [],
                    "account_values": {
                        "DU1234567": {
                            "NetLiquidation": {"value": "25000.00", "currency": "SEK"},
                            "BuyingPower": {"value": "200000.00", "currency": "SEK"},
                            "AvailableFunds": {"value": "100000.00", "currency": "SEK"},
                            "ExcessLiquidity": {"value": "100000.00", "currency": "SEK"},
                            "AccountType": {"value": "INDIVIDUAL", "currency": ""},
                            "Leverage-S": {"value": "1.00", "currency": ""},
                            "Currency": {"value": "SEK", "currency": "SEK"},
                        }
                    },
                }

            def get_historical_data(
                self,
                contract: _FakeContract,
                end_date_time: str,
                duration_str: str,
                bar_size_setting: str,
                what_to_show: str,
                use_rth: bool = True,
                format_date: int = 1,
                timeout: int | None = None,
            ) -> list[object]:
                if (contract.symbol, contract.currency) == ("EUR", "SEK"):
                    return [SimpleNamespace(date="20260421", close="10.00")]
                if (contract.symbol, contract.currency) == ("SEK", "EUR"):
                    return [SimpleNamespace(date="20260421", close="0.10")]
                raise AssertionError(f"Unexpected FX request for {contract.symbol}.{contract.currency}")

        payload = _base_payload()
        payload["instructions"][0]["instrument"] = {
            "symbol": "ACUVI",
            "security_type": "STK",
            "exchange": "XSTO",
            "currency": "SEK",
            "primary_exchange": "XSTO",
        }
        payload["instructions"][0]["intent"] = {
            "side": "SELL",
            "position_side": "SHORT",
        }
        batch = parse_execution_batch_payload(payload)

        with self.assertRaisesRegex(ValueError, "not present on the persisted official IBKR Sweden shortable list"):
            submit_order_from_batch(
                self.config,
                batch,
                sync_wrapper_cls=_SwedenShortWrapper,
                response_timeout_cls=TimeoutError,
                contract_cls=_FakeContract,
                order_cls=_FakeOrder,
            )

    def test_submit_order_from_batch_rejects_short_when_position_lookup_times_out(self) -> None:
        class _TimedOutShortWrapper(_FakeOrderExecutionSyncWrapper):
            def get_positions(self, timeout: int = 10) -> dict[str, list[object]]:
                raise TimeoutError("positions timed out")

            def get_historical_data(
                self,
                contract: _FakeContract,
                end_date_time: str,
                duration_str: str,
                bar_size_setting: str,
                what_to_show: str,
                use_rth: bool = True,
                format_date: int = 1,
                timeout: int | None = None,
            ) -> list[object]:
                if (contract.symbol, contract.currency) == ("USD", "EUR"):
                    raise TimeoutError("direct USD.EUR unavailable")
                if (contract.symbol, contract.currency) == ("EUR", "USD"):
                    return [SimpleNamespace(date="20260421", close="1.10")]
                raise AssertionError(
                    f"Unexpected FX request for {contract.symbol}.{contract.currency}"
                )

        payload = _base_payload()
        payload["instructions"][0]["intent"] = {
            "side": "SELL",
            "position_side": "SHORT",
        }
        payload["instructions"][0]["entry"]["order_type"] = "MARKET"
        del payload["instructions"][0]["entry"]["limit_price"]
        batch = parse_execution_batch_payload(payload)

        with self.assertRaisesRegex(ValueError, "positions lookup timed out"):
            submit_order_from_batch(
                self.config,
                batch,
                sync_wrapper_cls=_TimedOutShortWrapper,
                response_timeout_cls=TimeoutError,
                contract_cls=_FakeContract,
                order_cls=_FakeOrder,
            )

    def test_parse_execution_batch_payload_rejects_long_entry_with_sell_side(self) -> None:
        payload = _base_payload()
        payload["instructions"][0]["intent"] = {
            "side": "SELL",
            "position_side": "LONG",
        }

        with self.assertRaisesRegex(ValueError, "LONG entries must use intent.side=BUY"):
            parse_execution_batch_payload(payload)

    def test_submit_order_from_batch_rounds_down_fraction_of_nav_quantity(self) -> None:
        payload = _base_payload()
        payload["instructions"][0]["sizing"] = {
            "mode": "fraction_of_account_nav",
            "target_fraction_of_account": "0.10",
        }
        payload["instructions"][0]["entry"]["limit_price"] = "123.00"
        batch = parse_execution_batch_payload(payload)

        result = submit_order_from_batch(
            self.config,
            batch,
            sync_wrapper_cls=_FakeOrderExecutionSyncWrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
            order_cls=_FakeOrder,
        )

        self.assertEqual(result["order"]["total_quantity"], "81")
        self.assertIn("rounded down to a whole share", " ".join(result["warnings"]))

    def test_submit_order_from_batch_reduces_full_cash_quantity_before_live_submit(self) -> None:
        class _TightWhatIfWrapper(_FakeOrderExecutionSyncWrapper):
            total_cash_value = "10000.00"

            def preview_order_sync(
                self,
                contract: object,
                order: object,
                timeout: int | None = None,
            ) -> dict[str, object]:
                preview = super().preview_order_sync(contract, order, timeout)
                order_state = preview["orderState"]
                total_notional = Decimal(str(order.totalQuantity)) * Decimal(str(order.lmtPrice or 0))
                post_trade_cash = Decimal("10000.00") - total_notional
                order_state.equityWithLoanBefore = "10000.00"
                order_state.equityWithLoanChange = str(-total_notional)
                order_state.equityWithLoanAfter = str(post_trade_cash)
                order_state.initMarginBefore = "0.00"
                order_state.initMarginChange = "0.00"
                order_state.initMarginAfter = "0.00"
                order_state.commission = "10.00"
                order_state.minCommission = "10.00"
                order_state.maxCommission = "10.00"
                return preview

        payload = _base_payload()
        payload["instructions"][0]["sizing"] = {
            "mode": "fraction_of_account_nav",
            "target_fraction_of_account": "1.0",
        }
        payload["instructions"][0]["entry"]["limit_price"] = "100.00"
        batch = parse_execution_batch_payload(payload)

        result = submit_order_from_batch(
            self.config,
            batch,
            sync_wrapper_cls=_TightWhatIfWrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
            order_cls=_FakeOrder,
        )

        self.assertEqual(result["order"]["total_quantity"], "99")
        self.assertIn(
            "Cash-backed long sizing reserved 25.000000 USD before computing a full-account entry size.",
            result["warnings"],
        )
        self.assertEqual(result["whatif_preview"]["what_if"], True)
        self.assertEqual(result["whatif_preview"]["transmit"], True)

    def test_submit_order_from_batch_continues_after_clean_whatif_timeout(self) -> None:
        class _TimeoutWhatIfWrapper(_FakeOrderExecutionSyncWrapper):
            def preview_order_sync(
                self,
                contract: object,
                order: object,
                timeout: int | None = None,
            ) -> dict[str, object]:
                self.previewed_orders.append((contract, order, timeout))
                raise TimeoutError("preview timed out")

        batch = parse_execution_batch_payload(_base_payload())

        result = submit_order_from_batch(
            self.config,
            batch,
            sync_wrapper_cls=_TimeoutWhatIfWrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
            order_cls=_FakeOrder,
        )

        self.assertEqual(result["order"]["total_quantity"], "10")
        self.assertEqual(result["broker_order_status"]["status"], "Submitted")
        self.assertIsNone(result["whatif_preview"])
        self.assertIn(
            "Timed out while requesting the IBKR WhatIf order preview; continuing with cash-reserve sizing.",
            result["warnings"],
        )

    def test_submit_order_from_batch_continues_after_blank_whatif_202_notice(self) -> None:
        class _BlankCancelWhatIfWrapper(_FakeOrderExecutionSyncWrapper):
            def preview_order_sync(
                self,
                contract: object,
                order: object,
                timeout: int | None = None,
            ) -> dict[str, object]:
                self.previewed_orders.append((contract, order, timeout))
                order_id = self._next_order_id
                self._next_order_id += 1
                order.orderId = order_id
                self._append_wire_audit("what_if_preflight", contract, order)
                self.errors[order_id] = [
                    {
                        "errorCode": 202,
                        "errorString": "Order Canceled - reason:",
                        "advancedOrderRejectJson": "",
                    }
                ]
                raise TimeoutError("preview timed out")

        batch = parse_execution_batch_payload(_base_payload())

        result = submit_order_from_batch(
            self.config,
            batch,
            sync_wrapper_cls=_BlankCancelWhatIfWrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
            order_cls=_FakeOrder,
        )

        self.assertEqual(result["order"]["total_quantity"], "10")
        self.assertEqual(result["broker_order_status"]["status"], "Submitted")
        self.assertIsNone(result["whatif_preview"])
        self.assertIn(
            "IBKR emitted a blank 202 cancellation notice during the WhatIf preflight; continuing with cash-reserve sizing.",
            result["warnings"],
        )

    def test_submit_order_from_batch_continues_after_descriptive_whatif_202_reject(self) -> None:
        class _RejectedWhatIfWrapper(_FakeOrderExecutionSyncWrapper):
            def preview_order_sync(
                self,
                contract: object,
                order: object,
                timeout: int | None = None,
            ) -> dict[str, object]:
                self.previewed_orders.append((contract, order, timeout))
                order_id = self._next_order_id
                self._next_order_id += 1
                order.orderId = order_id
                self._append_wire_audit("what_if_preflight", contract, order)
                self.errors[order_id] = [
                    {
                        "errorCode": 202,
                        "errorString": "Order Canceled - reason: limit price too far through the market",
                        "advancedOrderRejectJson": "",
                    }
                ]
                raise TimeoutError("preview timed out")

        batch = parse_execution_batch_payload(_base_payload())

        result = submit_order_from_batch(
            self.config,
            batch,
            sync_wrapper_cls=_RejectedWhatIfWrapper,
            response_timeout_cls=TimeoutError,
            contract_cls=_FakeContract,
            order_cls=_FakeOrder,
        )

        self.assertEqual(result["order"]["total_quantity"], "10")
        self.assertEqual(result["broker_order_status"]["status"], "Submitted")
        self.assertIsNone(result["whatif_preview"])
        self.assertIn(
            "IBKR rejected the WhatIf preflight ([202] Order Canceled - reason: limit price too far through the market); continuing with cash-reserve sizing.",
            result["warnings"],
        )
        stages = [
            event["request"]["stage"]
            for event in result["ibkr_wire_audit"]
            if event["event_type"] == "outbound_order_request"
        ]
        self.assertEqual(stages, ["what_if_preflight", "live_order_submit"])

    def test_cancel_broker_order_returns_cancel_status(self) -> None:
        result = cancel_broker_order(
            self.config,
            17,
            sync_wrapper_cls=_FakeOrderExecutionSyncWrapper,
            response_timeout_cls=TimeoutError,
        )

        self.assertEqual(result["broker_order_status"]["orderId"], 17)
        self.assertEqual(result["broker_order_status"]["status"], "Cancelled")

    def test_cancel_broker_order_treats_missing_broker_order_as_already_gone(self) -> None:
        result = cancel_broker_order(
            self.config,
            33,
            sync_wrapper_cls=_FakeMissingCancelOrderExecutionSyncWrapper,
            response_timeout_cls=TimeoutError,
        )

        self.assertEqual(result["broker_order_status"]["orderId"], 33)
        self.assertEqual(result["broker_order_status"]["status"], "NOT_FOUND_AT_BROKER")
        self.assertIn("already absent", result["warning"])

    def test_cancel_broker_order_treats_already_cancelled_order_as_done(self) -> None:
        result = cancel_broker_order(
            self.config,
            44,
            sync_wrapper_cls=_FakeAlreadyCancelledOrderExecutionSyncWrapper,
            response_timeout_cls=TimeoutError,
        )

        self.assertEqual(result["broker_order_status"]["orderId"], 44)
        self.assertEqual(result["broker_order_status"]["status"], "Cancelled")
        self.assertIn("already cancelled", result["warning"])
