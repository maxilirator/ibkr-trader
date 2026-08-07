from __future__ import annotations

from datetime import datetime
from datetime import timezone
from decimal import Decimal
from decimal import InvalidOperation
from typing import Any, Mapping

from sqlalchemy import func
from sqlalchemy import select

from ibkr_trader.api.serialization import serialize_for_json
from ibkr_trader.db.base import session_scope
from ibkr_trader.db.base import utc_now
from ibkr_trader.db.models import BrokerAccountRecord
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import PositionSnapshotRecord
from ibkr_trader.db.models import TraderDeploymentRecord
from ibkr_trader.db.models import TraderModelRecord
from ibkr_trader.orchestration.state_machine import ExecutionState
from ibkr_trader.orchestration.trader_registry import TraderDeploymentNotFoundError
from ibkr_trader.rl.observations_common import ObservationConfig
from ibkr_trader.rl.observations_common import observation_config_from_contract


_RL_RUNTIME_ACTIVE_STATES = {
    ExecutionState.ENTRY_PENDING.value,
    ExecutionState.ENTRY_SUBMITTED.value,
    ExecutionState.POSITION_OPEN.value,
    ExecutionState.EXIT_PENDING.value,
}


def build_rl_runtime_state_snapshot(
    session_factory: Any,
    *,
    deployment_key: str,
    symbols: list[str] | None = None,
) -> dict[str, Any]:
    """Build the runner's authoritative per-symbol portfolio state."""

    normalized_deployment_key = deployment_key.strip()
    if not normalized_deployment_key:
        raise ValueError("deployment_key is required")
    symbol_set = {symbol.strip().upper() for symbol in symbols or [] if symbol.strip()}

    generated_at = utc_now()
    with session_scope(session_factory) as session:
        deployment = session.execute(
            select(TraderDeploymentRecord)
            .join(TraderModelRecord)
            .where(TraderDeploymentRecord.deployment_key == normalized_deployment_key)
        ).scalar_one_or_none()
        if deployment is None:
            raise TraderDeploymentNotFoundError(
                f"Trader deployment '{normalized_deployment_key}' was not found."
            )

        side = str(deployment.trader_model.side or "").upper()
        if side not in {"LONG", "SHORT"}:
            raise ValueError(
                f"Trader deployment '{normalized_deployment_key}' has unsupported side '{side}'."
            )
        observation_config = observation_config_from_contract(
            deployment.trader_model.observation_contract_json,
        )

        active_statement = select(InstructionRecord).where(
            InstructionRecord.account_key == deployment.account_key,
            InstructionRecord.book_key == deployment.book_key,
            InstructionRecord.source_system == "rl-runner",
            InstructionRecord.state.in_(_RL_RUNTIME_ACTIVE_STATES),
            InstructionRecord.archived_at.is_(None),
        )
        if symbol_set:
            active_statement = active_statement.where(
                func.upper(InstructionRecord.symbol).in_(symbol_set)
            )
        active_records = tuple(
            record
            for record in session.execute(active_statement).scalars()
            if _rl_runtime_instruction_deployment_key(record)
            == normalized_deployment_key
        )

        if not symbol_set:
            symbol_set = {record.symbol.upper() for record in active_records}

        latest_positions_by_symbol = _latest_position_snapshots_by_symbol(
            session,
            account_key=deployment.account_key,
            deployment_key=normalized_deployment_key,
            symbols=symbol_set,
        )
        symbol_set.update(latest_positions_by_symbol)

        records_by_symbol: dict[str, list[InstructionRecord]] = {}
        for record in active_records:
            records_by_symbol.setdefault(record.symbol.upper(), []).append(record)

        symbol_states = [
            _build_rl_runtime_symbol_state(
                symbol=symbol,
                deployment_key=normalized_deployment_key,
                side=side,
                generated_at=generated_at,
                observation_config=observation_config,
                active_records=tuple(records_by_symbol.get(symbol, ())),
                position_snapshot=latest_positions_by_symbol.get(symbol),
            )
            for symbol in sorted(symbol_set)
        ]
        snapshot = {
            "generated_at": generated_at,
            "deployment_key": normalized_deployment_key,
            "account_key": deployment.account_key,
            "book_key": deployment.book_key,
            "mode": deployment.mode,
            "is_virtual": deployment.is_virtual,
            "side": side,
            "symbols": symbol_states,
        }

    return serialize_for_json(snapshot)


def _latest_position_snapshots_by_symbol(
    session: Any,
    *,
    account_key: str,
    deployment_key: str,
    symbols: set[str],
) -> dict[str, tuple[PositionSnapshotRecord, BrokerAccountRecord]]:
    if not symbols:
        return {}
    rows = session.execute(
        select(PositionSnapshotRecord, BrokerAccountRecord)
        .join(
            BrokerAccountRecord,
            BrokerAccountRecord.id == PositionSnapshotRecord.broker_account_id,
        )
        .where(
            BrokerAccountRecord.account_key == account_key,
            func.upper(PositionSnapshotRecord.symbol).in_(symbols),
        )
        .order_by(
            PositionSnapshotRecord.snapshot_at.desc(),
            PositionSnapshotRecord.id.desc(),
        )
    ).all()
    latest_by_owner: dict[
        tuple[str, str | None, str | None],
        tuple[PositionSnapshotRecord, BrokerAccountRecord],
    ] = {}
    for position_snapshot, broker_account in rows:
        symbol = position_snapshot.symbol.upper()
        owner_key = (
            symbol,
            position_snapshot.owner_deployment_key,
            position_snapshot.owner_instruction_id,
        )
        latest_by_owner.setdefault(owner_key, (position_snapshot, broker_account))

    latest: dict[str, tuple[PositionSnapshotRecord, BrokerAccountRecord]] = {}
    unowned_virtual: dict[str, tuple[PositionSnapshotRecord, BrokerAccountRecord]] = {}
    for (
        symbol,
        owner_deployment_key,
        _owner_instruction_id,
    ), snapshot in latest_by_owner.items():
        position_snapshot, broker_account = snapshot
        quantity = _decimal_or_zero(position_snapshot.quantity)
        if (
            quantity != 0
            and (position_snapshot.is_virtual or broker_account.is_virtual)
            and not owner_deployment_key
        ):
            unowned_virtual.setdefault(symbol, snapshot)
        if owner_deployment_key == deployment_key:
            latest.setdefault(symbol, snapshot)
        elif not (position_snapshot.is_virtual or broker_account.is_virtual):
            latest.setdefault(symbol, snapshot)

    for symbol, snapshot in unowned_virtual.items():
        latest[symbol] = snapshot
    return latest


def _build_rl_runtime_symbol_state(
    *,
    symbol: str,
    deployment_key: str,
    side: str,
    generated_at: datetime,
    observation_config: ObservationConfig,
    active_records: tuple[InstructionRecord, ...],
    position_snapshot: tuple[PositionSnapshotRecord, BrokerAccountRecord] | None,
) -> dict[str, Any]:
    blockers: list[dict[str, Any]] = []
    entry_records = tuple(
        record
        for record in active_records
        if record.state
        in {
            ExecutionState.ENTRY_PENDING.value,
            ExecutionState.ENTRY_SUBMITTED.value,
        }
    )
    position_records = tuple(
        record
        for record in active_records
        if record.state
        in {
            ExecutionState.POSITION_OPEN.value,
            ExecutionState.EXIT_PENDING.value,
        }
    )
    if len(entry_records) > 1:
        blockers.append(
            {
                "reason": "duplicate_active_entries",
                "instruction_ids": [record.instruction_id for record in entry_records],
            }
        )
    if len(position_records) > 1:
        blockers.append(
            {
                "reason": "duplicate_active_positions",
                "instruction_ids": [
                    record.instruction_id for record in position_records
                ],
            }
        )

    position_payload = None
    position_quantity = Decimal("0")
    if position_snapshot is not None:
        position_record, broker_account = position_snapshot
        position_payload = _serialize_rl_runtime_position(
            position_record,
            broker_account,
        )
        position_quantity = _decimal_or_zero(position_record.quantity)
        if position_quantity != 0 and not _quantity_matches_side(
            position_quantity,
            side,
        ):
            blockers.append(
                {
                    "reason": "position_side_mismatch",
                    "quantity": str(position_quantity),
                    "expected_side": side,
                }
            )
        if position_quantity != 0 and (
            position_record.is_virtual or broker_account.is_virtual
        ):
            owner_instruction_id = str(position_record.owner_instruction_id or "")
            owner_deployment_key = str(position_record.owner_deployment_key or "")
            if not owner_instruction_id or not owner_deployment_key:
                blockers.append(
                    {
                        "reason": "virtual_position_missing_owner",
                        "quantity": str(position_quantity),
                        "position_snapshot_id": position_record.id,
                        "message": (
                            "Latest virtual holdings show a position, but the "
                            "position snapshot does not carry a durable RL owner."
                        ),
                    }
                )
            elif owner_deployment_key != deployment_key:
                blockers.append(
                    {
                        "reason": "virtual_position_owner_mismatch",
                        "quantity": str(position_quantity),
                        "position_snapshot_id": position_record.id,
                        "owner_deployment_key": owner_deployment_key,
                        "expected_deployment_key": deployment_key,
                    }
                )
            elif position_records and owner_instruction_id not in {
                record.instruction_id for record in position_records
            }:
                blockers.append(
                    {
                        "reason": "virtual_position_owner_instruction_mismatch",
                        "quantity": str(position_quantity),
                        "position_snapshot_id": position_record.id,
                        "owner_instruction_id": owner_instruction_id,
                        "active_instruction_ids": [
                            record.instruction_id for record in position_records
                        ],
                    }
                )

    if position_quantity != 0 and not position_records:
        blockers.append(
            {
                "reason": "unowned_current_holding",
                "quantity": str(position_quantity),
                "message": (
                    "Latest account holdings show a position, but no active "
                    "RL-owned instruction for this deployment owns it."
                ),
            }
        )
    if position_records and position_quantity == 0:
        blockers.append(
            {
                "reason": "owned_position_missing_current_holding",
                "instruction_ids": [
                    record.instruction_id for record in position_records
                ],
                "message": (
                    "Active RL instruction says a position is open, but the "
                    "latest account holdings are flat or missing."
                ),
            }
        )

    active_payloads = [
        _serialize_rl_runtime_instruction(record) for record in active_records
    ]
    if blockers:
        return {
            "symbol": symbol,
            "status": "blocked",
            "state_before": "INCONSISTENT",
            "runner_state": None,
            "blockers": blockers,
            "position_snapshot": position_payload,
            "active_instructions": active_payloads,
        }

    if entry_records:
        entry = entry_records[0]
        action_name = _rl_runtime_instruction_action_name(entry)
        entry_order_age = _completed_model_bars_between(
            entry.submit_at,
            generated_at,
            config=observation_config,
        )
        return {
            "symbol": symbol,
            "status": "ready",
            "state_before": "ENTRY_PENDING",
            "runner_state": {
                "in_position": False,
                "pending_entry_anchor": _rl_runtime_pending_entry_anchor(action_name),
                "pending_entry_rel_bp": _rl_runtime_entry_rel_bp(action_name),
                "pending_exit_tp_bp": None,
                "entry_price": None,
                "entry_bar_idx": None,
                "bars_since_entry_order": entry_order_age,
                "bars_since_exit_order": 0,
            },
            "blockers": [],
            "position_snapshot": position_payload,
            "active_instructions": active_payloads,
        }

    if position_records:
        position_record = position_records[0]
        entry_price = (
            _decimal_or_none(
                position_payload.get("average_cost") if position_payload else None
            )
            or _decimal_or_none(position_record.entry_avg_fill_price)
        )
        exit_pending = position_record.state == ExecutionState.EXIT_PENDING.value
        entry_bar_idx = _completed_model_bar_index_at(
            position_record.entry_filled_at or position_record.submit_at,
            config=observation_config,
        )
        exit_order_age = (
            _completed_model_bars_between(
                _rl_runtime_exit_order_started_at(position_record),
                generated_at,
                config=observation_config,
            )
            if exit_pending
            else 0
        )
        return {
            "symbol": symbol,
            "status": "ready",
            "state_before": "EXIT_PENDING"
            if exit_pending
            else ("SHORT_OPEN" if side == "SHORT" else "LONG_OPEN"),
            "runner_state": {
                "in_position": True,
                "pending_entry_anchor": None,
                "pending_entry_rel_bp": None,
                "pending_exit_tp_bp": (180 if side == "SHORT" else 200)
                if exit_pending
                else None,
                "entry_price": entry_price,
                "entry_bar_idx": entry_bar_idx,
                "bars_since_entry_order": 0,
                "bars_since_exit_order": exit_order_age,
            },
            "blockers": [],
            "position_snapshot": position_payload,
            "active_instructions": active_payloads,
        }

    return {
        "symbol": symbol,
        "status": "ready",
        "state_before": "FLAT",
        "runner_state": {
            "in_position": False,
            "pending_entry_anchor": None,
            "pending_entry_rel_bp": None,
            "pending_exit_tp_bp": None,
            "entry_price": None,
            "entry_bar_idx": None,
            "bars_since_entry_order": 0,
            "bars_since_exit_order": 0,
        },
        "blockers": [],
        "position_snapshot": position_payload,
        "active_instructions": active_payloads,
    }


def _serialize_rl_runtime_instruction(record: InstructionRecord) -> dict[str, Any]:
    return {
        "instruction_id": record.instruction_id,
        "record_id": record.id,
        "state": record.state,
        "symbol": record.symbol,
        "account_key": record.account_key,
        "book_key": record.book_key,
        "is_virtual": record.is_virtual,
        "side": record.side,
        "order_type": record.order_type,
        "broker_order_id": record.broker_order_id,
        "broker_order_status": record.broker_order_status,
        "exit_order_id": record.exit_order_id,
        "exit_order_status": record.exit_order_status,
        "entry_filled_quantity": record.entry_filled_quantity,
        "entry_avg_fill_price": record.entry_avg_fill_price,
        "entry_filled_at": record.entry_filled_at,
        "activity_at": record.updated_at,
        "metadata": _rl_runtime_instruction_runtime_metadata(record),
    }


def _serialize_rl_runtime_position(
    record: PositionSnapshotRecord,
    broker_account: BrokerAccountRecord,
) -> dict[str, Any]:
    return {
        "position_snapshot_id": record.id,
        "account_key": broker_account.account_key,
        "broker_kind": broker_account.broker_kind,
        "is_virtual": record.is_virtual or broker_account.is_virtual,
        "snapshot_at": record.snapshot_at,
        "source": record.source,
        "symbol": record.symbol,
        "exchange": record.exchange,
        "currency": record.currency,
        "security_type": record.security_type,
        "primary_exchange": record.primary_exchange,
        "local_symbol": record.local_symbol,
        "quantity": record.quantity,
        "average_cost": record.average_cost,
        "market_price": record.market_price,
        "market_value": record.market_value,
        "unrealized_pnl": record.unrealized_pnl,
        "realized_pnl": record.realized_pnl,
        "owner_instruction_id": record.owner_instruction_id,
        "owner_source_instruction_id": record.owner_source_instruction_id,
        "owner_deployment_key": record.owner_deployment_key,
        "owner_book_key": record.owner_book_key,
    }


def _rl_runtime_instruction_metadata(record: InstructionRecord) -> Mapping[str, Any]:
    payload = record.payload if isinstance(record.payload, Mapping) else {}
    instruction = payload.get("instruction", {})
    if not isinstance(instruction, Mapping):
        return {}
    trace = instruction.get("trace", {})
    if not isinstance(trace, Mapping):
        return {}
    metadata = trace.get("metadata", {})
    return metadata if isinstance(metadata, Mapping) else {}


def _rl_runtime_instruction_runtime_metadata(record: InstructionRecord) -> dict[str, Any]:
    metadata = _rl_runtime_instruction_metadata(record)
    return {
        key: metadata[key]
        for key in (
            "rl_deployment_key",
            "rl_action_name",
            "rl_decision_id",
            "rl_source_instruction_id",
        )
        if key in metadata
    }


def _rl_runtime_instruction_deployment_key(record: InstructionRecord) -> str:
    return str(
        _rl_runtime_instruction_metadata(record).get("rl_deployment_key") or ""
    )


def _rl_runtime_instruction_action_name(record: InstructionRecord) -> str:
    return str(_rl_runtime_instruction_metadata(record).get("rl_action_name") or "")


def _completed_model_bar_index_at(
    timestamp: datetime | None,
    *,
    config: ObservationConfig,
) -> int | None:
    if timestamp is None:
        return None
    observed = _ensure_aware_datetime(timestamp).astimezone(config.zoneinfo)
    session_open = datetime.combine(
        observed.date(),
        config.session_open,
        tzinfo=config.zoneinfo,
    )
    if observed < session_open:
        return None
    elapsed_minutes = int((observed - session_open).total_seconds() // 60)
    completed_count = elapsed_minutes // config.target_bar_minutes
    completed_count = min(max(completed_count, 0), config.expected_session_bars)
    if completed_count <= 0:
        return None
    return completed_count - 1


def _completed_model_bars_between(
    started_at: datetime | None,
    ended_at: datetime | None,
    *,
    config: ObservationConfig,
) -> int:
    start_idx = _completed_model_bar_index_at(started_at, config=config)
    end_idx = _completed_model_bar_index_at(ended_at, config=config)
    if start_idx is None or end_idx is None:
        return 0
    return max(0, end_idx - start_idx)


def _rl_runtime_exit_order_started_at(record: InstructionRecord) -> datetime | None:
    return record.updated_at or record.entry_filled_at or record.submit_at


def _ensure_aware_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _rl_runtime_pending_entry_anchor(action_name: str) -> str:
    if action_name == "market_entry":
        return "market"
    if action_name.startswith("entry_prevclose_"):
        return "prev_close"
    if action_name.startswith("entry_sessionopen_"):
        return "session_open"
    return "unknown"


def _rl_runtime_entry_rel_bp(action_name: str) -> int | None:
    prefix = "entry_prevclose_"
    suffix = "bp"
    if not action_name.startswith(prefix) or not action_name.endswith(suffix):
        return None
    try:
        return int(action_name.removeprefix(prefix).removesuffix(suffix))
    except ValueError:
        return None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _decimal_or_zero(value: Any) -> Decimal:
    return _decimal_or_none(value) or Decimal("0")


def _quantity_matches_side(quantity: Decimal, side: str) -> bool:
    return quantity > 0 if side == "LONG" else quantity < 0
