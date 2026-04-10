from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase

from ibkr_trader.domain.instructions import (
    EntryOrderType,
    ExitPolicy,
    Side,
    TimedEntry,
    TradeInstruction,
)
from ibkr_trader.orchestration.state_machine import ExecutionState, InstructionRuntime


class InstructionModelTests(TestCase):
    def test_limit_order_requires_price(self) -> None:
        entry = TimedEntry(
            symbol="AAPL",
            side=Side.BUY,
            quantity=Decimal("10"),
            order_type=EntryOrderType.LIMIT,
            activate_at=datetime.now(timezone.utc),
        )

        with self.assertRaisesRegex(ValueError, "limit orders require limit_price"):
            entry.validate()

    def test_instruction_runtime_advances_states(self) -> None:
        instruction = TradeInstruction(
            instruction_id="demo-1",
            created_at=datetime.now(timezone.utc),
            entry=TimedEntry(
                symbol="AAPL",
                side=Side.BUY,
                quantity=Decimal("10"),
                order_type=EntryOrderType.LIMIT,
                activate_at=datetime.now(timezone.utc),
                limit_price=Decimal("180.50"),
            ),
            exit_policy=ExitPolicy(
                take_profit_pct=Decimal("0.02"),
                stop_loss_pct=Decimal("0.15"),
                force_exit_next_session_open=True,
            ),
        )

        runtime = InstructionRuntime(instruction=instruction)
        runtime.schedule_entry()
        runtime.on_entry_submitted()
        runtime.on_entry_filled()

        self.assertEqual(runtime.state, ExecutionState.POSITION_OPEN)

    def test_instruction_runtime_can_mark_entry_cancelled(self) -> None:
        instruction = TradeInstruction(
            instruction_id="demo-2",
            created_at=datetime.now(timezone.utc),
            entry=TimedEntry(
                symbol="AAPL",
                side=Side.BUY,
                quantity=Decimal("10"),
                order_type=EntryOrderType.LIMIT,
                activate_at=datetime.now(timezone.utc),
                limit_price=Decimal("180.50"),
            ),
            exit_policy=ExitPolicy(),
        )

        runtime = InstructionRuntime(instruction=instruction)
        runtime.schedule_entry()
        runtime.on_entry_submitted()
        runtime.on_entry_cancelled()

        self.assertEqual(runtime.state, ExecutionState.ENTRY_CANCELLED)
