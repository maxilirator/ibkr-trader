#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from decimal import Decimal
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import select

from ibkr_trader.config import AppConfig
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.db.models import InstructionRecord
from ibkr_trader.db.models import MarketStreamBarRecord
from ibkr_trader.db.models import TraderDeploymentRecord
from ibkr_trader.rl.inference_vector import RunnerSymbolState
from ibkr_trader.rl.inference_vector import assemble_dqn_observation_vector
from ibkr_trader.rl.model_artifacts import promoted_rl_model_by_key
from ibkr_trader.rl.observations import build_phase1_observation_payload
from ibkr_trader.rl.observations import observation_config_from_contract
from ibkr_trader.rl.runner_decisions import action_diagnostics
from ibkr_trader.rl.runner_decisions import choose_action
from ibkr_trader.rl.runner_history import HISTORY_FEATURE_NAMES
from ibkr_trader.rl.runner_model import load_model
from ibkr_trader.rl.runner_model import static_feature_payload
from ibkr_trader.rl.runner_runtime_state import translation_state_before


STOCKHOLM_TZ = ZoneInfo("Europe/Stockholm")


@dataclass(frozen=True, slots=True)
class ReplayFill:
    entry_price: float | None = None
    entry_filled_at: datetime | None = None
    exit_price: float | None = None
    exit_filled_at: datetime | None = None


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Replay one RL deployment/symbol/day through the promoted model."
    )
    parser.add_argument("--deployment-key", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--trade-date", default=date.today().isoformat())
    parser.add_argument("--json", action="store_true", help="print machine-readable JSON")
    parser.add_argument(
        "--no-actual-fills",
        action="store_true",
        help="simulate fills only from model-bar OHLC instead of using persisted fills",
    )
    args = parser.parse_args()

    result = replay_day(
        deployment_key=args.deployment_key,
        symbol=args.symbol,
        trade_date=date.fromisoformat(args.trade_date),
        use_actual_fills=not args.no_actual_fills,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
    else:
        print_text_report(result)
    return 0


def replay_day(
    *,
    deployment_key: str,
    symbol: str,
    trade_date: date,
    use_actual_fills: bool = True,
) -> dict[str, Any]:
    settings = AppConfig.from_env()
    engine = build_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    normalized_symbol = symbol.strip().upper()

    with session_factory() as session:
        deployment = session.execute(
            select(TraderDeploymentRecord).where(
                TraderDeploymentRecord.deployment_key == deployment_key
            )
        ).scalar_one()
        model_record = deployment.trader_model
        artifact = promoted_rl_model_by_key(model_record.model_key)
        loaded = load_model(artifact)
        candidate = _load_source_candidate(
            session,
            deployment=deployment,
            symbol=normalized_symbol,
            trade_date=trade_date,
        )
        metadata = _instruction_metadata(candidate)
        static_features = static_feature_payload(
            loaded,
            candidate={
                "trace": {
                    "metadata": metadata,
                },
            },
            symbol=normalized_symbol,
            trade_date=trade_date.isoformat(),
        )
        history_override = _metadata_history_override(metadata, trade_date=trade_date)
        source_bars = _load_stream_bars(
            session,
            symbol=normalized_symbol,
            trade_date=trade_date,
        )
        actual_fill = _load_actual_fill(
            session,
            deployment=deployment,
            symbol=normalized_symbol,
            source_instruction_id=candidate.instruction_id,
            trade_date=trade_date,
        )

    config = observation_config_from_contract(model_record.observation_contract_json)
    decision_times = _decision_times_for_bars(
        source_bars,
        trade_date=trade_date,
        session_open=config.session_open,
        session_close=config.session_close,
    )
    state = RunnerSymbolState()
    events: list[dict[str, Any]] = []
    terminal = False

    for as_of in decision_times:
        if terminal:
            break
        observation_payload = build_phase1_observation_payload(
            deployment_key=deployment.deployment_key,
            model_key=model_record.model_key,
            model_side=model_record.side,
            observation_contract=model_record.observation_contract_json,
            action_space=list(model_record.action_space_json),
            as_of=as_of,
            source_bars_by_symbol={normalized_symbol: source_bars},
            symbols=[normalized_symbol],
            history_overrides={normalized_symbol: history_override},
            static_features_by_symbol={normalized_symbol: static_features},
        )
        symbol_observation = observation_payload["observations"][normalized_symbol]
        decision = symbol_observation["model_decision"]
        if not decision.get("ready"):
            events.append(
                {
                    "as_of": as_of.isoformat(),
                    "status": "not_ready",
                    "reason": decision.get("reason"),
                }
            )
            continue

        vector = assemble_dqn_observation_vector(
            symbol_observation,
            state=state,
            model_side=model_record.side,
            path_pad_length=int(observation_payload["feature_schema"]["path_pad_length"]),
            expected_obs_dim=loaded.obs_dim,
        )
        action_name, q_values = choose_action(
            loaded.model,
            loaded.action_names,
            vector,
            state,
        )
        diagnostics = action_diagnostics(
            loaded.action_names,
            q_values,
            state,
            chosen_action=action_name,
        )
        phase1_bar = _latest_usable_bar(symbol_observation)
        before = translation_state_before(state, model_record.side)
        transition = _apply_action(
            action_name,
            state=state,
            phase1_bar=phase1_bar,
            previous_close=float(history_override["prev_close"]),
            side=model_record.side,
            bar_idx=int(decision["usable_bar_count"]) - 1,
            actual_fill=actual_fill if use_actual_fills else ReplayFill(),
            as_of=as_of,
        )
        state = transition["state"]
        terminal = bool(transition["terminal"])
        after = translation_state_before(state, model_record.side)
        events.append(
            {
                "as_of": as_of.isoformat(),
                "decision_id": decision.get("decision_id"),
                "bar": {
                    key: phase1_bar.get(key)
                    for key in ("started_at", "ended_at", "open", "high", "low", "close")
                },
                "state_before": before,
                "action": action_name,
                "state_after": "TERMINAL" if terminal else after,
                "note": transition["note"],
                "top_actions": diagnostics["valid_actions_ranked"][:4],
                "runner_state": _state_payload(state),
            }
        )

    return {
        "deployment_key": deployment_key,
        "model_key": model_record.model_key,
        "symbol": normalized_symbol,
        "trade_date": trade_date.isoformat(),
        "source_candidate_id": candidate.instruction_id,
        "source_bar_count": len(source_bars),
        "use_actual_fills": use_actual_fills,
        "actual_fill": {
            "entry_price": actual_fill.entry_price,
            "entry_filled_at": actual_fill.entry_filled_at,
            "exit_price": actual_fill.exit_price,
            "exit_filled_at": actual_fill.exit_filled_at,
        },
        "history_override": {
            "prev_close": history_override["prev_close"],
            "source": history_override.get("source"),
            "warning": history_override.get("warning"),
        },
        "events": events,
    }


def print_text_report(result: Mapping[str, Any]) -> None:
    print(
        f"Replay {result['trade_date']} {result['deployment_key']} "
        f"{result['symbol']} ({result['model_key']})"
    )
    print(
        f"source bars: {result['source_bar_count']} | "
        f"candidate: {result['source_candidate_id']} | "
        f"prev_close: {result['history_override']['prev_close']} | "
        f"actual_fills: {result['use_actual_fills']}"
    )
    actual = result.get("actual_fill", {})
    if actual.get("entry_price") is not None or actual.get("exit_price") is not None:
        print(
            "actual fill: "
            f"entry={actual.get('entry_price')} at {actual.get('entry_filled_at')} | "
            f"exit={actual.get('exit_price')} at {actual.get('exit_filled_at')}"
        )
    print()
    for event in result["events"]:
        if event.get("status") == "not_ready":
            print(f"{event['as_of']} NOT_READY {event.get('reason')}")
            continue
        top = ", ".join(
            f"{item['action_name']}={float(item['q_value']):.2f}"
            for item in event["top_actions"]
        )
        print(
            f"{event['as_of']} {event['state_before']} -> "
            f"{event['action']} -> {event['state_after']}"
        )
        print(f"  bar {event['bar']['started_at']}..{event['bar']['ended_at']} "
              f"O/H/L/C={event['bar']['open']}/{event['bar']['high']}/"
              f"{event['bar']['low']}/{event['bar']['close']}")
        print(f"  {event['note']}")
        print(f"  top: {top}")


def _load_source_candidate(
    session: Any,
    *,
    deployment: TraderDeploymentRecord,
    symbol: str,
    trade_date: date,
) -> InstructionRecord:
    rows = session.execute(
        select(InstructionRecord)
        .where(
            InstructionRecord.account_key == deployment.account_key,
            InstructionRecord.book_key == deployment.book_key,
            InstructionRecord.symbol == symbol,
            InstructionRecord.source_system != "rl-runner",
        )
        .order_by(InstructionRecord.submit_at.desc(), InstructionRecord.id.desc())
    ).scalars()
    candidates = [
        row
        for row in rows
        if _as_stockholm_date(row.submit_at) == trade_date
        and _metadata_static_features(_instruction_metadata(row)) is not None
    ]
    if not candidates:
        raise LookupError(
            f"no source RL candidate found for {deployment.deployment_key} {symbol} {trade_date}"
        )
    return candidates[0]


def _load_stream_bars(
    session: Any,
    *,
    symbol: str,
    trade_date: date,
) -> list[dict[str, Any]]:
    start = datetime.combine(trade_date, time(9, 0), tzinfo=STOCKHOLM_TZ)
    end = datetime.combine(trade_date, time(17, 30), tzinfo=STOCKHOLM_TZ)
    rows = session.execute(
        select(MarketStreamBarRecord)
        .where(
            MarketStreamBarRecord.symbol == symbol,
            MarketStreamBarRecord.started_at >= start,
            MarketStreamBarRecord.started_at <= end,
        )
        .order_by(MarketStreamBarRecord.started_at.asc(), MarketStreamBarRecord.id.asc())
    ).scalars()
    bars = [
        {
            "timestamp": _aware(row.started_at).isoformat(),
            "open": row.open_price,
            "high": row.high_price,
            "low": row.low_price,
            "close": row.close_price,
            "volume": row.volume,
            "bar_count": row.bar_count,
            "currency": row.currency,
            "source": row.source,
        }
        for row in rows
    ]
    if not bars:
        raise LookupError(f"no persisted market-stream bars found for {symbol} {trade_date}")
    return bars


def _load_actual_fill(
    session: Any,
    *,
    deployment: TraderDeploymentRecord,
    symbol: str,
    source_instruction_id: str,
    trade_date: date,
) -> ReplayFill:
    rows = session.execute(
        select(InstructionRecord)
        .where(
            InstructionRecord.account_key == deployment.account_key,
            InstructionRecord.book_key == deployment.book_key,
            InstructionRecord.symbol == symbol,
            InstructionRecord.source_system == "rl-runner",
        )
        .order_by(InstructionRecord.submit_at.asc(), InstructionRecord.id.asc())
    ).scalars()
    for row in rows:
        metadata = _instruction_metadata(row)
        if metadata.get("rl_source_instruction_id") != source_instruction_id:
            continue
        if _as_stockholm_date(row.submit_at) != trade_date:
            continue
        return ReplayFill(
            entry_price=_float_or_none(row.entry_avg_fill_price),
            entry_filled_at=_aware(row.entry_filled_at) if row.entry_filled_at else None,
            exit_price=_float_or_none(row.exit_avg_fill_price),
            exit_filled_at=_aware(row.exit_filled_at) if row.exit_filled_at else None,
        )
    return ReplayFill()


def _decision_times_for_bars(
    bars: Sequence[Mapping[str, Any]],
    *,
    trade_date: date,
    session_open: time,
    session_close: time,
) -> list[datetime]:
    timestamps = [_parse_datetime(bar["timestamp"]) for bar in bars]
    max_timestamp = max(ts for ts in timestamps if ts is not None)
    session_start = datetime.combine(trade_date, session_open, tzinfo=STOCKHOLM_TZ)
    session_end = datetime.combine(trade_date, session_close, tzinfo=STOCKHOLM_TZ)
    last_decision = min(max_timestamp + timedelta(minutes=1), session_end)
    first_decision = session_start + timedelta(minutes=5)
    current = first_decision
    out: list[datetime] = []
    while current <= last_decision:
        out.append(current)
        current += timedelta(minutes=5)
    return out


def _latest_usable_bar(symbol_observation: Mapping[str, Any]) -> Mapping[str, Any]:
    decision = symbol_observation["model_decision"]
    ended_at = str(decision["latest_usable_bar_ended_at"])
    for bar in reversed(symbol_observation["phase1_bars"]):
        if str(bar.get("ended_at")) == ended_at:
            return bar
    raise LookupError(f"latest usable bar {ended_at} not found")


def _apply_action(
    action_name: str,
    *,
    state: RunnerSymbolState,
    phase1_bar: Mapping[str, Any],
    previous_close: float,
    side: str,
    bar_idx: int,
    actual_fill: ReplayFill,
    as_of: datetime,
) -> dict[str, Any]:
    trade_sign = -1.0 if side.upper() == "SHORT" else 1.0
    open_price = float(phase1_bar["open"])
    high_price = float(phase1_bar["high"])
    low_price = float(phase1_bar["low"])
    close_price = float(phase1_bar["close"])

    if action_name == "skip":
        return {"state": state, "terminal": True, "note": "model skipped the candidate"}
    if action_name == "wait" and not state.in_position:
        filled = _maybe_fill_pending_entry(
            state,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            previous_close=previous_close,
            side=side,
            bar_idx=bar_idx,
            actual_fill=actual_fill,
            as_of=as_of,
        )
        if filled is not None:
            return filled
        return {"state": state, "terminal": False, "note": "waited while flat/pending"}

    if not state.in_position and action_name.startswith("entry_prevclose_"):
        rel_bp = _action_bp(action_name, prefix="entry_prevclose_")
        state = RunnerSymbolState(
            pending_entry_anchor="prev_close",
            pending_entry_rel_bp=rel_bp,
            bars_since_entry_order=0,
        )
        filled = _maybe_fill_pending_entry(
            state,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            previous_close=previous_close,
            side=side,
            bar_idx=bar_idx,
            actual_fill=actual_fill,
            as_of=as_of,
        )
        if filled is not None:
            return filled
        return {
            "state": RunnerSymbolState(
                pending_entry_anchor="prev_close",
                pending_entry_rel_bp=rel_bp,
                bars_since_entry_order=state.bars_since_entry_order + 1,
            ),
            "terminal": False,
            "note": f"placed prev-close entry {rel_bp}bp; not filled on this bar",
        }

    if action_name == "market_entry" and not state.in_position:
        fill_price = actual_fill.entry_price if actual_fill.entry_price is not None else open_price
        fill_idx = _entry_bar_idx(actual_fill.entry_filled_at, default_idx=bar_idx)
        return {
            "state": RunnerSymbolState(
                in_position=True,
                entry_price=fill_price,
                entry_bar_idx=fill_idx,
            ),
            "terminal": False,
            "note": f"market entry filled at {fill_price}",
        }

    if action_name == "cancel_entry" and not state.in_position:
        return {
            "state": RunnerSymbolState(),
            "terminal": False,
            "note": "cancelled pending entry",
        }

    if state.in_position and action_name == "exit_market":
        exit_price = actual_fill.exit_price if actual_fill.exit_price is not None else open_price
        entry_price = float(state.entry_price or exit_price)
        pnl = trade_sign * ((exit_price / entry_price) - 1.0)
        return {
            "state": state,
            "terminal": True,
            "note": f"market exit at {exit_price}; return={pnl:.4%}",
        }

    if state.in_position and action_name.startswith("exit_tp_"):
        tp_bp = _action_bp(action_name, prefix="exit_tp_")
        tp_price = float(state.entry_price or close_price) * (1.0 + trade_sign * tp_bp / 10000.0)
        gap_fill = open_price <= tp_price if trade_sign < 0 else open_price >= tp_price
        inside_fill = low_price <= tp_price <= high_price
        if gap_fill or inside_fill:
            exit_price = open_price if gap_fill else tp_price
            entry_price = float(state.entry_price or exit_price)
            pnl = trade_sign * ((exit_price / entry_price) - 1.0)
            return {
                "state": state,
                "terminal": True,
                "note": f"take-profit {tp_bp}bp filled at {exit_price}; return={pnl:.4%}",
            }
        return {
            "state": RunnerSymbolState(
                in_position=True,
                entry_price=state.entry_price,
                entry_bar_idx=state.entry_bar_idx,
                pending_exit_tp_bp=tp_bp,
                bars_since_exit_order=state.bars_since_exit_order + 1,
            ),
            "terminal": False,
            "note": f"armed take-profit {tp_bp}bp at {tp_price:.4f}; not filled",
        }

    return {"state": state, "terminal": False, "note": "action left state unchanged"}


def _maybe_fill_pending_entry(
    state: RunnerSymbolState,
    *,
    open_price: float,
    high_price: float,
    low_price: float,
    previous_close: float,
    side: str,
    bar_idx: int,
    actual_fill: ReplayFill,
    as_of: datetime,
) -> dict[str, Any] | None:
    if state.pending_entry_anchor != "prev_close" or state.pending_entry_rel_bp is None:
        return None
    limit_price = previous_close * (1.0 + float(state.pending_entry_rel_bp) / 10000.0)
    trade_sign = -1.0 if side.upper() == "SHORT" else 1.0
    if actual_fill.entry_price is not None and (
        actual_fill.entry_filled_at is None or actual_fill.entry_filled_at <= as_of
    ):
        fill_price = actual_fill.entry_price
        fill_idx = _entry_bar_idx(actual_fill.entry_filled_at, default_idx=bar_idx)
        source = "actual persisted fill"
    else:
        open_fill = open_price >= limit_price if trade_sign < 0 else open_price <= limit_price
        inside_fill = low_price <= limit_price <= high_price
        if not (open_fill or inside_fill):
            return None
        fill_price = open_price if open_fill else limit_price
        fill_idx = bar_idx
        source = "simulated OHLC fill"
    return {
        "state": RunnerSymbolState(
            in_position=True,
            entry_price=fill_price,
            entry_bar_idx=fill_idx,
        ),
        "terminal": False,
        "note": (
            f"entry filled at {fill_price} ({source}); "
            f"limit={limit_price:.4f}; entry_bar_idx={fill_idx}"
        ),
    }


def _instruction_metadata(record: InstructionRecord) -> Mapping[str, Any]:
    payload = record.payload if isinstance(record.payload, Mapping) else {}
    instruction = payload.get("instruction")
    if not isinstance(instruction, Mapping):
        return {}
    trace = instruction.get("trace")
    if not isinstance(trace, Mapping):
        return {}
    metadata = trace.get("metadata")
    return metadata if isinstance(metadata, Mapping) else {}


def _metadata_static_features(metadata: Mapping[str, Any]) -> Any:
    for key in ("static_features", "rl_static_features", "model_static_features"):
        if metadata.get(key) is not None:
            return metadata[key]
    return None


def _metadata_history_override(
    metadata: Mapping[str, Any],
    *,
    trade_date: date,
) -> dict[str, Any]:
    prev_close = metadata.get("previous_close") or metadata.get("prev_close") or metadata.get("yesterday_close")
    if prev_close is None:
        raise ValueError("candidate metadata missing previous close/yesterday_close")
    raw_history = None
    for key in ("history_features", "rl_history_features", "source_history_features"):
        if metadata.get(key) is not None:
            raw_history = metadata[key]
            break
    if isinstance(raw_history, Mapping):
        history_features = {
            name: float(raw_history[name])
            for name in HISTORY_FEATURE_NAMES
            if raw_history.get(name) is not None
        }
    elif isinstance(raw_history, Sequence) and not isinstance(raw_history, (str, bytes)):
        history_features = {
            name: float(raw_history[idx])
            for idx, name in enumerate(HISTORY_FEATURE_NAMES)
            if idx < len(raw_history)
        }
    else:
        history_features = {name: 0.0 for name in HISTORY_FEATURE_NAMES}
    missing = [name for name in HISTORY_FEATURE_NAMES if name not in history_features]
    if missing:
        raise ValueError(f"candidate history features missing {missing}")
    return {
        "prev_close": str(prev_close),
        "history_features": history_features,
        "source": (
            "candidate_metadata_history"
            if raw_history is not None
            else "candidate_metadata_yesterday_close_neutral_history"
        ),
        "target_date": trade_date.isoformat(),
    }


def _state_payload(state: RunnerSymbolState) -> dict[str, Any]:
    return {
        "in_position": state.in_position,
        "pending_entry_anchor": state.pending_entry_anchor,
        "pending_entry_rel_bp": state.pending_entry_rel_bp,
        "pending_exit_tp_bp": state.pending_exit_tp_bp,
        "entry_price": state.entry_price,
        "entry_bar_idx": state.entry_bar_idx,
        "bars_since_entry_order": state.bars_since_entry_order,
        "bars_since_exit_order": state.bars_since_exit_order,
    }


def _action_bp(action_name: str, *, prefix: str) -> int:
    suffix = "bp"
    return int(action_name.removeprefix(prefix).removesuffix(suffix))


def _entry_bar_idx(value: datetime | None, *, default_idx: int) -> int:
    if value is None:
        return default_idx
    local = value.astimezone(STOCKHOLM_TZ)
    session_open = datetime.combine(local.date(), time(9, 0), tzinfo=STOCKHOLM_TZ)
    completed_count = int((local - session_open).total_seconds() // 60) // 5
    return max(0, completed_count - 1)


def _as_stockholm_date(value: datetime) -> date:
    return _aware(value).astimezone(STOCKHOLM_TZ).date()


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=STOCKHOLM_TZ)
    return value


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _aware(value).astimezone(STOCKHOLM_TZ)
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    parsed = datetime.fromisoformat(raw)
    return _aware(parsed).astimezone(STOCKHOLM_TZ)


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(Decimal(str(value)))


if __name__ == "__main__":
    raise SystemExit(main())
