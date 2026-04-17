from __future__ import annotations

from datetime import date
from unittest import TestCase

from ibkr_trader.api.server import (
    enforce_loopback_binding,
    is_loopback_host,
    parse_account_summary_payload,
    parse_contract_resolve_payload,
    parse_execution_batch_payload,
    parse_historical_bars_payload,
    parse_runtime_cycle_payload,
    parse_shortability_snapshot_payload,
    parse_tick_stream_payload,
    serialize_execution_batch,
    serialize_runtime_schedule_preview,
)
from ibkr_trader.ibkr.shortability import ShortabilityMarketDataType
from ibkr_trader.ibkr.shortability import ShortabilitySource
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

    def test_parse_tick_stream_payload_normalizes_tick_types(self) -> None:
        query = parse_tick_stream_payload(
            {
                "symbol": "aapl",
                "security_type": "stk",
                "exchange": "smart",
                "currency": "usd",
                "primary_exchange": "nasdaq",
                "tick_types": ["last", "bid_ask", "mid-point"],
                "duration_seconds": 3,
                "max_events": 100,
            }
        )

        self.assertEqual(query.symbol, "AAPL")
        self.assertEqual(query.exchange, "SMART")
        self.assertEqual(query.currency, "USD")
        self.assertEqual(query.primary_exchange, "NASDAQ")
        self.assertEqual(query.tick_types, ("Last", "BidAsk", "MidPoint"))
        self.assertEqual(query.duration_seconds, 3)
        self.assertEqual(query.max_events, 100)

    def test_parse_tick_stream_payload_rejects_empty_tick_types(self) -> None:
        with self.assertRaisesRegex(ValueError, "tick_types"):
            parse_tick_stream_payload(
                {
                    "symbol": "AAPL",
                    "exchange": "SMART",
                    "currency": "USD",
                    "tick_types": [],
                }
            )

    def test_parse_shortability_snapshot_payload_uses_stockholm_defaults(self) -> None:
        query = parse_shortability_snapshot_payload({})

        self.assertEqual(query.exchange, "SMART")
        self.assertEqual(query.primary_exchange, "SFB")
        self.assertEqual(query.currency, "SEK")
        self.assertEqual(query.security_type, "STK")
        self.assertEqual(query.source, ShortabilitySource.OFFICIAL_IBKR_PAGE)
        self.assertEqual(query.market_data_type, ShortabilityMarketDataType.LIVE)
        self.assertTrue(query.only_shortable)
        self.assertIsNone(query.as_of_date)

    def test_parse_shortability_snapshot_payload_accepts_symbols_date_source_and_delayed_type(self) -> None:
        query = parse_shortability_snapshot_payload(
            {
                "symbols": ["sive", "abb"],
                "as_of_date": "2026-04-14",
                "source": "broker_ticks",
                "market_data_type": "delayed_frozen",
                "max_symbols": 25,
                "max_concurrent": 10,
                "per_symbol_timeout_seconds": 1.5,
            }
        )

        self.assertEqual(query.symbols, ("SIVE", "ABB"))
        self.assertEqual(query.source, ShortabilitySource.BROKER_TICKS)
        self.assertEqual(
            query.market_data_type,
            ShortabilityMarketDataType.DELAYED_FROZEN,
        )
        self.assertEqual(query.as_of_date, date(2026, 4, 14))
        self.assertEqual(query.max_symbols, 25)
        self.assertEqual(query.max_concurrent, 10)
        self.assertEqual(query.per_symbol_timeout_seconds, 1.5)

    def test_ibkr_telemetry_limit_must_be_positive(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        from ibkr_trader.api.server import create_app
        from ibkr_trader.config import AppConfig
        from ibkr_trader.config import ApiServerConfig
        from ibkr_trader.config import IbkrConnectionConfig
        from pathlib import Path

        app = create_app(
            AppConfig(
                environment="test",
                timezone="Europe/Stockholm",
                database_url="sqlite+pysqlite:///:memory:",
                session_calendar_path=Path("/tmp/day_sessions.parquet"),
                stockholm_instruments_path=Path("/tmp/all.txt"),
                stockholm_identity_path=Path("/tmp/identity.parquet"),
                api=ApiServerConfig(
                    host="127.0.0.1",
                    port=8000,
                    require_loopback_only=False,
                ),
                ibkr=IbkrConnectionConfig(
                    host="127.0.0.1",
                    port=4001,
                    client_id=0,
                    diagnostic_client_id=7,
                    streaming_client_id=9,
                    account_id="DU1234567",
                ),
            )
        )

        client = TestClient(app)
        response = client.get("/v1/ibkr/telemetry?recent_limit=0")
        self.assertEqual(response.status_code, 400)
        self.assertIn("recent_limit", response.text)

    def test_parse_runtime_cycle_payload_accepts_optional_timestamp(self) -> None:
        now_at, timeout, instruction_ids = parse_runtime_cycle_payload(
            {
                "now_at": "2026-04-13T09:00:00+02:00",
                "timeout": 15,
                "instruction_ids": ["instruction-1", "instruction-2"],
            }
        )

        self.assertEqual(now_at.isoformat(), "2026-04-13T09:00:00+02:00")
        self.assertEqual(timeout, 15)
        self.assertEqual(instruction_ids, ("instruction-1", "instruction-2"))

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
