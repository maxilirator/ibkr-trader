from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from ibkr_trader.domain.instructions import TradeInstruction


class ExecutionState(StrEnum):
    RECEIVED = "RECEIVED"
    ENTRY_PENDING = "ENTRY_PENDING"
    ENTRY_SUBMITTED = "ENTRY_SUBMITTED"
    POSITION_OPEN = "POSITION_OPEN"
    EXIT_PENDING = "EXIT_PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass(slots=True)
class InstructionRuntime:
    instruction: TradeInstruction
    state: ExecutionState = ExecutionState.RECEIVED

    def schedule_entry(self) -> None:
        self.instruction.validate()
        self.state = ExecutionState.ENTRY_PENDING

    def on_entry_submitted(self) -> None:
        self.state = ExecutionState.ENTRY_SUBMITTED

    def on_entry_filled(self) -> None:
        self.state = ExecutionState.POSITION_OPEN

    def on_exit_workflow_started(self) -> None:
        self.state = ExecutionState.EXIT_PENDING

    def on_completed(self) -> None:
        self.state = ExecutionState.COMPLETED

