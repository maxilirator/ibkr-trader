from __future__ import annotations

from unittest import TestCase

from ibkr_trader.api.server import (
    enforce_loopback_binding,
    is_loopback_host,
    parse_account_summary_payload,
    parse_contract_resolve_payload,
    parse_execution_batch_payload,
    parse_historical_bars_payload,
    serialize_execution_batch,
    serialize_runtime_schedule_preview,
)
from ibkr_trader.orchestration.scheduling import build_batch_runtime_schedule


class ApiServerTests(TestCase):
    def test_is_loopback_host_accepts_loopback_names_and_ips(self) -> None:
        self.assertTrue(is_loopback_host("127.0.0.1"))
        self.assertTrue(is_loopback_host("::1"))
        self.assertTrue(is_loopback_host("localhost"))
        self.assertFalse(is_loopback_host("0.0.0.0"))
        self.assertFalse(is_loopback_host("192.168.1.15"))

    def test_enforce_loopback_binding_rejects_nonlocal_host(self) -> None:
        with self.assertRaisesRegex(ValueError, "loopback"):
            enforce_loopback_binding("0.0.0.0", require_loopback_only=True)

    def test_parse_contract_resolve_payload_normalizes_values(self) -> None:
        query = parse_contract_resolve_payload(
            {
                "symbol": "sive",
                "security_type": "stk",
                "exchange": "xsto",
                "currency": "sek",
                "primary_exchange": "xsto",
                "isin": "SE0003917798",
            }
        )

        self.assertEqual(query.symbol, "SIVE")
        self.assertEqual(query.security_type, "STK")
        self.assertEqual(query.exchange, "XSTO")
        self.assertEqual(query.currency, "SEK")
        self.assertEqual(query.primary_exchange, "XSTO")
        self.assertEqual(query.isin, "SE0003917798")

    def test_parse_account_summary_payload_accepts_defaults(self) -> None:
        tags, group, account_id = parse_account_summary_payload({})

        self.assertIn("NetLiquidation", tags)
        self.assertEqual(group, "All")
        self.assertIsNone(account_id)

    def test_parse_historical_bars_payload_normalizes_values(self) -> None:
        query = parse_historical_bars_payload(
            {
                "symbol": "sive",
                "security_type": "stk",
                "exchange": "smart",
                "currency": "sek",
                "primary_exchange": "sfb",
                "duration": "2 D",
                "bar_size": "5 mins",
                "what_to_show": "trades",
                "use_rth": True,
                "end_at": "2026-04-10T17:30:00+02:00",
            }
        )

        self.assertEqual(query.symbol, "SIVE")
        self.assertEqual(query.security_type, "STK")
        self.assertEqual(query.exchange, "SMART")
        self.assertEqual(query.currency, "SEK")
        self.assertEqual(query.primary_exchange, "SFB")
        self.assertEqual(query.duration, "2 D")
        self.assertEqual(query.bar_size, "5 mins")
        self.assertEqual(query.what_to_show, "TRADES")
        self.assertTrue(query.use_rth)
        self.assertEqual(query.end_at.isoformat(), "2026-04-10T17:30:00+02:00")

    def test_parse_execution_batch_payload_validates_contract(self) -> None:
        batch = parse_execution_batch_payload(
            {
                "schema_version": "2026-04-10",
                "source": {
                    "system": "q-training",
                    "batch_id": "trial_27-2026-04-10-prod-long-01",
                    "generated_at": "2026-04-10T02:15:44Z",
                    "release_id": "release-1",
                    "strategy_id": "trial_27",
                    "policy_id": "policy-1",
                },
                "instructions": [
                    {
                        "instruction_id": "2026-04-10-GTW05-long_risk_book-SIVE-long-01",
                        "account": {
                            "account_key": "GTW05",
                            "book_key": "long_risk_book",
                            "book_role": "prod",
                            "book_side": "long",
                        },
                        "instrument": {
                            "symbol": "sive",
                            "security_type": "stk",
                            "exchange": "xsto",
                            "currency": "sek",
                            "isin": "SE0003917798",
                            "aliases": ["SIVE.ST", "sivers-ima"],
                        },
                        "intent": {
                            "side": "buy",
                            "position_side": "long",
                        },
                        "sizing": {
                            "mode": "fraction_of_account_nav",
                            "target_fraction_of_account": "1.0",
                        },
                        "entry": {
                            "order_type": "limit",
                            "submit_at": "2026-04-10T09:25:00+02:00",
                            "expire_at": "2026-04-10T17:30:00+02:00",
                            "limit_price": "11.3131",
                            "time_in_force": "day",
                            "max_submit_count": 1,
                            "cancel_unfilled_at_expiry": True,
                        },
                        "exit": {
                            "take_profit_pct": "0.02",
                            "catastrophic_stop_loss_pct": "0.15",
                            "force_exit_next_session_open": True,
                        },
                        "trace": {
                            "reason_code": "risk_policy_orderbook",
                            "execution_policy": "policy-x",
                            "trade_date": "2026-04-10",
                            "data_cutoff_date": "2026-04-09",
                            "company_name": "Sivers Semiconductors",
                            "metadata": {
                                "entry_reference_type": "prev_close",
                                "entry_reference_price": "11.37",
                            },
                        },
                    }
                ],
            }
        )

        serialized = serialize_execution_batch(batch)

        self.assertEqual(serialized["schema_version"], "2026-04-10")
        self.assertEqual(serialized["instructions"][0]["instrument"]["symbol"], "SIVE")
        self.assertEqual(serialized["instructions"][0]["instrument"]["exchange"], "XSTO")
        self.assertEqual(serialized["instructions"][0]["entry"]["limit_price"], "11.3131")
        self.assertEqual(
            serialized["instructions"][0]["sizing"]["target_fraction_of_account"],
            "1.0",
        )

    def test_parse_execution_batch_payload_requires_absolute_timestamps(self) -> None:
        with self.assertRaisesRegex(ValueError, "timezone"):
            parse_execution_batch_payload(
                {
                    "schema_version": "2026-04-10",
                    "source": {
                        "system": "q-training",
                        "batch_id": "trial_27-2026-04-10-prod-long-01",
                        "generated_at": "2026-04-10T02:15:44Z",
                    },
                    "instructions": [
                        {
                            "instruction_id": "demo-1",
                            "account": {
                                "account_key": "GTW05",
                                "book_key": "long_risk_book",
                            },
                            "instrument": {
                                "symbol": "SIVE",
                                "security_type": "STK",
                                "exchange": "XSTO",
                                "currency": "SEK",
                            },
                            "intent": {
                                "side": "BUY",
                                "position_side": "LONG",
                            },
                            "sizing": {
                                "mode": "fraction_of_account_nav",
                                "target_fraction_of_account": "1.0",
                            },
                            "entry": {
                                "order_type": "LIMIT",
                                "submit_at": "2026-04-10T09:25:00",
                                "expire_at": "2026-04-10T17:30:00+02:00",
                                "limit_price": "11.3131",
                            },
                            "exit": {
                                "take_profit_pct": "0.02",
                            },
                            "trace": {
                                "reason_code": "risk_policy_orderbook",
                            },
                        }
                    ],
                }
            )

    def test_parse_execution_batch_payload_requires_single_sizing_target(self) -> None:
        with self.assertRaisesRegex(ValueError, "exactly one"):
            parse_execution_batch_payload(
                {
                    "schema_version": "2026-04-10",
                    "source": {
                        "system": "q-training",
                        "batch_id": "trial_27-2026-04-10-prod-long-01",
                        "generated_at": "2026-04-10T02:15:44Z",
                    },
                    "instructions": [
                        {
                            "instruction_id": "demo-1",
                            "account": {
                                "account_key": "GTW05",
                                "book_key": "long_risk_book",
                            },
                            "instrument": {
                                "symbol": "SIVE",
                                "security_type": "STK",
                                "exchange": "XSTO",
                                "currency": "SEK",
                            },
                            "intent": {
                                "side": "BUY",
                                "position_side": "LONG",
                            },
                            "sizing": {
                                "mode": "fraction_of_account_nav",
                                "target_fraction_of_account": "1.0",
                                "target_notional": "100000",
                            },
                            "entry": {
                                "order_type": "LIMIT",
                                "submit_at": "2026-04-10T09:25:00+02:00",
                                "expire_at": "2026-04-10T17:30:00+02:00",
                                "limit_price": "11.3131",
                            },
                            "exit": {
                                "take_profit_pct": "0.02",
                            },
                            "trace": {
                                "reason_code": "risk_policy_orderbook",
                            },
                        }
                    ],
                }
            )

    def test_serialize_runtime_schedule_preview_projects_stockholm_times(self) -> None:
        batch = parse_execution_batch_payload(
            {
                "schema_version": "2026-04-10",
                "source": {
                    "system": "q-training",
                    "batch_id": "trial_27-2026-04-10-prod-long-01",
                    "generated_at": "2026-04-10T02:15:44Z",
                },
                "instructions": [
                    {
                        "instruction_id": "demo-1",
                        "account": {
                            "account_key": "GTW05",
                            "book_key": "long_risk_book",
                        },
                        "instrument": {
                            "symbol": "SIVE",
                            "security_type": "STK",
                            "exchange": "XSTO",
                            "currency": "SEK",
                        },
                        "intent": {
                            "side": "BUY",
                            "position_side": "LONG",
                        },
                        "sizing": {
                            "mode": "fraction_of_account_nav",
                            "target_fraction_of_account": "1.0",
                        },
                        "entry": {
                            "order_type": "LIMIT",
                            "submit_at": "2026-04-10T07:25:00Z",
                            "expire_at": "2026-04-10T15:30:00Z",
                            "limit_price": "11.3131",
                        },
                        "exit": {
                            "force_exit_next_session_open": True,
                        },
                        "trace": {
                            "reason_code": "risk_policy_orderbook",
                        },
                    }
                ],
            }
        )

        preview = serialize_runtime_schedule_preview(
            build_batch_runtime_schedule(batch, runtime_timezone="Europe/Stockholm")
        )

        self.assertEqual(preview["runtime_timezone"], "Europe/Stockholm")
        self.assertEqual(
            preview["instructions"][0]["submit_at_runtime"],
            "2026-04-10T09:25:00+02:00",
        )
        self.assertEqual(
            preview["instructions"][0]["next_session_exit"]["status"],
            "calendar_required",
        )
