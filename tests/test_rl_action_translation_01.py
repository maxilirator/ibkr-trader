from __future__ import annotations

from tests._rl_action_translation_shared import *  # noqa: F401,F403


class RLActionTranslationTests01(RLActionTranslationTestsBase):
    def test_long_prevclose_action_maps_to_buy_limit_below_previous_close(self) -> None:
        result = _translate(
            _model_routed_payload(
                instruction_id="long-axfo-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
                book_key="rl_shared_long_trial_106_virtual_01",
            ),
            deployment_key="long_trial_106_virtual_shared_01",
            action_name="entry_prevclose_-50bp",
        )

        self.assertEqual(result.action_status, ACTION_STATUS_TRANSLATED)
        instruction = result.instruction_payload["instructions"][0]
        self.assertEqual(instruction["intent"], {"side": "BUY", "position_side": "LONG"})
        self.assertEqual(instruction["entry"]["order_type"], "LIMIT")
        self.assertEqual(instruction["entry"]["limit_price"], "99.5000")

    def test_short_prevclose_action_maps_to_sell_limit_above_previous_close(self) -> None:
        result = _translate(
            _model_routed_payload(
                instruction_id="short-aza-1",
                model_id="short_trial36_v1",
                symbol="AZA",
                side="SHORT",
                book_key="rl_shared_short_trial_36_virtual_01",
            ),
            deployment_key="short_trial_36_virtual_shared_01",
            action_name="entry_prevclose_88bp",
        )

        self.assertEqual(result.action_status, ACTION_STATUS_TRANSLATED)
        instruction = result.instruction_payload["instructions"][0]
        self.assertEqual(instruction["intent"], {"side": "SELL", "position_side": "SHORT"})
        self.assertEqual(instruction["entry"]["order_type"], "LIMIT")
        self.assertEqual(instruction["entry"]["limit_price"], "100.8800")

    def test_long_market_entry_maps_to_buy_and_has_no_limit_price(self) -> None:
        result = _translate(
            _model_routed_payload(
                instruction_id="long-market-1",
                model_id="long_trial_106_v1",
                symbol="AZN",
                side="LONG",
            ),
            deployment_key="long_trial_106_virtual_shared_01",
            action_name="market_entry",
        )

        self.assertEqual(result.action_status, ACTION_STATUS_TRANSLATED)
        instruction = result.instruction_payload["instructions"][0]
        self.assertEqual(instruction["intent"], {"side": "BUY", "position_side": "LONG"})
        self.assertEqual(instruction["entry"]["order_type"], "MARKET")
        self.assertNotIn("limit_price", instruction["entry"])

    def test_entry_translation_preserves_source_lifecycle_policy(self) -> None:
        payload = _model_routed_payload(
            instruction_id="long-lifecycle-1",
            model_id="long_trial_106_v1",
            symbol="AZN",
            side="LONG",
        )
        payload["instructions"][0]["lifecycle"] = {
            "trade_date": "2026-04-27",
            "scope": "account_book_side_symbol_trade_date",
            "max_entry_orders": 1,
            "max_exit_orders": 1,
            "allow_reentry_after_exit": False,
            "allow_reentry_after_cancel": False,
            "retire_from_active_universe_when_flat": True,
        }

        result = _translate(
            payload,
            deployment_key="long_trial_106_virtual_shared_01",
            action_name="market_entry",
        )

        instruction = result.instruction_payload["instructions"][0]
        self.assertEqual(
            instruction["lifecycle"],
            {
                "trade_date": "2026-04-27",
                "scope": "account_book_side_symbol_trade_date",
                "max_entry_orders": 1,
                "max_exit_orders": 1,
                "allow_reentry_after_exit": False,
                "allow_reentry_after_cancel": False,
                "retire_from_active_universe_when_flat": True,
            },
        )

    def test_short_market_entry_maps_to_sell_and_has_no_limit_price(self) -> None:
        result = _translate(
            _model_routed_payload(
                instruction_id="short-market-1",
                model_id="short_trial36_v1",
                symbol="AZA",
                side="SHORT",
                book_key="rl_shared_short_trial_36_virtual_01",
            ),
            deployment_key="short_trial_36_virtual_shared_01",
            action_name="market_entry",
        )

        self.assertEqual(result.action_status, ACTION_STATUS_TRANSLATED)
        instruction = result.instruction_payload["instructions"][0]
        self.assertEqual(instruction["intent"], {"side": "SELL", "position_side": "SHORT"})
        self.assertEqual(instruction["entry"]["order_type"], "MARKET")
        self.assertNotIn("limit_price", instruction["entry"])

    def test_wait_and_skip_do_not_generate_instructions(self) -> None:
        for action_name in ("skip", "wait"):
            result = _translate(
                _model_routed_payload(
                    instruction_id=f"long-{action_name}-1",
                    model_id="long_trial_106_v1",
                    symbol="AXFO",
                    side="LONG",
                ),
                deployment_key="long_trial_106_virtual_shared_01",
                action_name=action_name,
            )
            self.assertEqual(result.action_status, ACTION_STATUS_LOGGED)
            self.assertIsNone(result.instruction_payload)

    def test_pending_entry_action_maintains_existing_pending_order(self) -> None:
        result = _translate(
            _model_routed_payload(
                instruction_id="long-pending-entry-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
            ),
            deployment_key="long_trial_106_virtual_shared_01",
            action_name="entry_prevclose_-50bp",
            state_before="ENTRY_PENDING",
        )

        self.assertEqual(result.action_status, ACTION_STATUS_LOGGED)
        self.assertEqual(result.state_after, "ENTRY_PENDING")
        self.assertIsNone(result.instruction_payload)
        self.assertIn("already pending", result.note)

    def test_long_rejects_short_prevclose_direction(self) -> None:
        result = _translate(
            _model_routed_payload(
                instruction_id="long-wrong-entry-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
            ),
            deployment_key="long_trial_106_virtual_shared_01",
            action_name="entry_prevclose_88bp",
        )

        self.assertEqual(result.action_status, ACTION_STATUS_INVALID)
        self.assertIsNone(result.instruction_payload)

    def test_short_rejects_long_prevclose_direction(self) -> None:
        result = _translate(
            _model_routed_payload(
                instruction_id="short-wrong-entry-1",
                model_id="short_trial36_v1",
                symbol="AZA",
                side="SHORT",
            ),
            deployment_key="short_trial_36_virtual_shared_01",
            action_name="entry_prevclose_-50bp",
        )

        self.assertEqual(result.action_status, ACTION_STATUS_INVALID)
        self.assertIsNone(result.instruction_payload)

    def test_exit_actions_translate_to_owned_mutations_without_instruction_payload(self) -> None:
        long_result = _translate(
            _model_routed_payload(
                instruction_id="long-exit-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
            ),
            deployment_key="long_trial_106_virtual_shared_01",
            action_name="exit_tp_200bp",
            state_before=LONG_OPEN,
        )
        short_result = _translate(
            _model_routed_payload(
                instruction_id="short-exit-1",
                model_id="short_trial36_v1",
                symbol="AZA",
                side="SHORT",
            ),
            deployment_key="short_trial_36_virtual_shared_01",
            action_name="exit_tp_180bp",
            state_before=SHORT_OPEN,
        )

        self.assertEqual(long_result.action_status, ACTION_STATUS_TRANSLATED)
        self.assertEqual(long_result.state_after, "EXIT_PENDING")
        self.assertIsNone(long_result.instruction_payload)
        self.assertEqual(short_result.action_status, ACTION_STATUS_TRANSLATED)
        self.assertEqual(short_result.state_after, "EXIT_PENDING")
        self.assertIsNone(short_result.instruction_payload)

    def test_wrong_side_take_profit_actions_fail_closed(self) -> None:
        long_result = _translate(
            _model_routed_payload(
                instruction_id="long-wrong-exit-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
            ),
            deployment_key="long_trial_106_virtual_shared_01",
            action_name="exit_tp_180bp",
            state_before=LONG_OPEN,
        )
        short_result = _translate(
            _model_routed_payload(
                instruction_id="short-wrong-exit-1",
                model_id="short_trial36_v1",
                symbol="AZA",
                side="SHORT",
            ),
            deployment_key="short_trial_36_virtual_shared_01",
            action_name="exit_tp_200bp",
            state_before=SHORT_OPEN,
        )

        self.assertEqual(long_result.action_status, ACTION_STATUS_INVALID)
        self.assertIsNone(long_result.instruction_payload)
        self.assertIn("long take-profit", long_result.note)
        self.assertEqual(short_result.action_status, ACTION_STATUS_INVALID)
        self.assertIsNone(short_result.instruction_payload)
        self.assertIn("short take-profit", short_result.note)

    def test_exit_market_is_allowed_only_from_matching_open_state(self) -> None:
        long_result = _translate(
            _model_routed_payload(
                instruction_id="long-exit-market-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
            ),
            deployment_key="long_trial_106_virtual_shared_01",
            action_name="exit_market",
            state_before=LONG_OPEN,
        )
        wrong_state_result = _translate(
            _model_routed_payload(
                instruction_id="long-exit-market-wrong-state-1",
                model_id="long_trial_106_v1",
                symbol="AXFO",
                side="LONG",
            ),
            deployment_key="long_trial_106_virtual_shared_01",
            action_name="exit_market",
            state_before=SHORT_OPEN,
        )

        self.assertEqual(long_result.action_status, ACTION_STATUS_TRANSLATED)
        self.assertEqual(long_result.state_after, "EXIT_PENDING")
        self.assertIsNone(long_result.instruction_payload)
        self.assertEqual(wrong_state_result.action_status, ACTION_STATUS_INVALID)
        self.assertIsNone(wrong_state_result.instruction_payload)
