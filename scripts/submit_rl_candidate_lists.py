#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timezone
from decimal import Decimal
from decimal import ROUND_DOWN
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pandas as pd

from ibkr_trader.rl.model_artifacts import read_static_feature_names


IDENTITY_PATH = Path("/home/mattias/dev/q-data/xsto/meta/instrument_identity.parquet")
CANDIDATE_SOURCE_ROOT = Path(
    os.environ.get("RL_CANDIDATE_SOURCE_ROOT", "/home/mattias/dev/q-training")
).expanduser()
STOCKHOLM_TZ = ZoneInfo("Europe/Stockholm")


@dataclass(frozen=True, slots=True)
class CandidateBatchConfig:
    side: str
    strategy_id: str
    model_key: str
    model_family: str
    model_artifact_id: str
    deployment_key: str
    book_key: str
    candidate_tape_path: Path
    static_feature_cols_path: Path | None = None
    strategy_key: str | None = None


CONFIGS: tuple[CandidateBatchConfig, ...] = (
    CandidateBatchConfig(
        side="LONG",
        strategy_id="long_trial_106",
        model_key="long_trial_106_v1",
        model_family="canonical_long_live_execution_policy",
        model_artifact_id="trial_106_seed240",
        deployment_key="long_trial_106_virtual_shared_01",
        book_key="rl_shared_long_trial_106_virtual_01",
        candidate_tape_path=CANDIDATE_SOURCE_ROOT
        / "artifacts/analysis/long_trial_104_ex_long_true_rl_input_materialize_ranker_v1/lockbox_candidate_tape.parquet",
        static_feature_cols_path=CANDIDATE_SOURCE_ROOT
        / "artifacts/analysis/long_trial_106_ex_long_true_rl_dqn_w128_oracle_notrade_dualseed_extension_v1/continuation/true_rl_dqn_w128_seed240/static_feature_cols.csv",
        strategy_key="bucket_booster_long",
    ),
    CandidateBatchConfig(
        side="SHORT",
        strategy_id="short_trial_36",
        model_key="short_trial36_v1",
        model_family="canonical_short_live_execution_policy",
        model_artifact_id="trial_36_seed140",
        deployment_key="short_trial_36_virtual_shared_01",
        book_key="rl_shared_short_trial_36_virtual_01",
        candidate_tape_path=CANDIDATE_SOURCE_ROOT
        / "artifacts/analysis/short_trial_14_replay_tape_ibkr_shortable_v1/lockbox_candidate_tape.parquet",
        static_feature_cols_path=CANDIDATE_SOURCE_ROOT
        / "artifacts/analysis/short_trial_36_ex_short_true_rl_dqn_w128_volnorm_market_context_triseed_v1/continuation/true_rl_dqn_w128_seed140/static_feature_cols.csv",
        strategy_key="bucket_booster_short",
    ),
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Submit selected RL names as model-routed trader candidates."
    )
    parser.add_argument("--api-base", default="http://quant.geisler.se:8000")
    parser.add_argument("--account-key", default="VIRTUALRL01")
    parser.add_argument("--trade-date", default=_today_stockholm().isoformat())
    parser.add_argument("--candidate-date", default="latest")
    parser.add_argument("--target-notional", default="1000")
    parser.add_argument(
        "--account-equity-reference",
        default=None,
        help="Account SEK net liquidation value used for allocation percentage sizing.",
    )
    parser.add_argument(
        "--long-allocation-pct",
        default="0.90",
        help="Long bucket gross exposure as a fraction of account NLV.",
    )
    parser.add_argument(
        "--short-allocation-pct",
        default="0.80",
        help="Short bucket gross exposure as a fraction of account NLV.",
    )
    parser.add_argument(
        "--long-budget",
        default=None,
        help="Optional gross SEK budget to divide across that day's long candidates.",
    )
    parser.add_argument(
        "--short-budget",
        default=None,
        help="Optional gross SEK budget to divide across that day's short candidates.",
    )
    parser.add_argument(
        "--max-notional-per-name",
        default=None,
        help="Optional SEK cap applied after dividing a side budget by candidate count.",
    )
    parser.add_argument("--min-order-notional", default="1000")
    parser.add_argument("--max-long-gross-account-pct", default="0.90")
    parser.add_argument("--max-short-gross-account-pct", default="0.80")
    parser.add_argument("--max-total-gross-account-pct", default="1.70")
    parser.add_argument("--max-abs-net-exposure-account-pct", default="0.25")
    parser.add_argument("--min-excess-liquidity-buffer-pct", default="0.20")
    parser.add_argument("--start-local", default="09:00")
    parser.add_argument("--end-local", default="17:30")
    parser.add_argument(
        "--run-suffix",
        default="",
        help="Optional suffix for batch/release/instruction ids when resubmitting a day.",
    )
    parser.add_argument(
        "--long-limit",
        type=int,
        default=None,
        help="Optional cap for long candidates. Default submits every selected row.",
    )
    parser.add_argument(
        "--short-limit",
        type=int,
        default=None,
        help="Optional cap for short candidates. Default submits every selected row.",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    trade_date = date.fromisoformat(args.trade_date)
    start_at = _combine_local(trade_date, args.start_local)
    end_at = _combine_local(trade_date, args.end_local)
    identity = load_identity_map(IDENTITY_PATH)
    generated_at = datetime.combine(
        trade_date,
        time(hour=6, minute=0),
        tzinfo=STOCKHOLM_TZ,
    ).astimezone(timezone.utc)

    api_base = args.api_base.rstrip("/")
    results: list[dict[str, Any]] = []
    for config in CONFIGS:
        limit = args.long_limit if config.side == "LONG" else args.short_limit
        rows, candidate_date = load_selected_rows(
            config.candidate_tape_path,
            candidate_date=args.candidate_date,
            limit=limit,
        )
        target_notional, capital_plan = resolve_capital_plan(
            side=config.side,
            account_key=args.account_key,
            candidate_count=len(rows),
            default_target_notional=str(args.target_notional),
            account_equity_reference=args.account_equity_reference,
            long_allocation_pct=args.long_allocation_pct,
            short_allocation_pct=args.short_allocation_pct,
            long_budget=args.long_budget,
            short_budget=args.short_budget,
            max_notional_per_name=args.max_notional_per_name,
            min_order_notional=args.min_order_notional,
            max_long_gross_account_pct=args.max_long_gross_account_pct,
            max_short_gross_account_pct=args.max_short_gross_account_pct,
            max_total_gross_account_pct=args.max_total_gross_account_pct,
            max_abs_net_exposure_account_pct=args.max_abs_net_exposure_account_pct,
            min_excess_liquidity_buffer_pct=args.min_excess_liquidity_buffer_pct,
        )
        payload = build_candidate_payload(
            config,
            rows,
            identity=identity,
            account_key=args.account_key,
            trade_date=trade_date,
            candidate_date=candidate_date,
            target_notional=target_notional,
            capital_plan=capital_plan,
            start_at=start_at,
            end_at=end_at,
            generated_at=generated_at,
            run_suffix=normalize_run_suffix(args.run_suffix),
        )
        if args.dry_run:
            results.append(
                {
                    "side": config.side,
                    "candidate_date": candidate_date,
                    "candidate_count": len(payload["instructions"]),
                    "symbols": [
                        item["instrument"]["symbol"]
                        for item in payload["instructions"]
                    ],
                    "payload": payload,
                }
            )
            continue

        validate_response = post_json(
            f"{api_base}/v1/instructions/validate",
            payload,
        )
        submit_response = post_json(
            f"{api_base}/v1/instructions/submit",
            payload,
        )
        results.append(
            {
                "side": config.side,
                "candidate_date": candidate_date,
                "candidate_count": len(payload["instructions"]),
                "symbols": [
                    item["instrument"]["symbol"]
                    for item in payload["instructions"]
                ],
                "validate_accepted": validate_response.get("accepted"),
                "submit_accepted": submit_response.get("accepted"),
                "submitted_count": submit_response.get("instruction_count"),
            }
        )

    print(json.dumps({"accepted": True, "results": results}, indent=2))
    return 0


def _today_stockholm() -> date:
    return datetime.now(STOCKHOLM_TZ).date()


def _combine_local(day: date, hhmm: str) -> datetime:
    hour_raw, minute_raw = hhmm.split(":", maxsplit=1)
    return datetime(
        day.year,
        day.month,
        day.day,
        int(hour_raw),
        int(minute_raw),
        tzinfo=STOCKHOLM_TZ,
    )


def _score_column(frame: pd.DataFrame) -> str | None:
    for candidate in (
        "meta_score",
        "panel_general__prob_mean",
        "panel_all__prob_mean",
    ):
        if candidate in frame.columns:
            return candidate
    prob_cols = [column for column in frame.columns if column.endswith("__prob")]
    return prob_cols[0] if prob_cols else None


def load_selected_rows(
    path: Path,
    *,
    candidate_date: str,
    limit: int | None,
) -> tuple[pd.DataFrame, str]:
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")
    frame = pd.read_parquet(path)
    frame = frame.copy()
    frame["_candidate_date"] = pd.to_datetime(frame["datetime"]).dt.strftime("%Y-%m-%d")
    resolved_date = (
        str(frame["_candidate_date"].max())
        if candidate_date == "latest"
        else date.fromisoformat(candidate_date).isoformat()
    )
    day = frame[frame["_candidate_date"] == resolved_date].copy()
    if day.empty:
        raise ValueError(f"no candidate rows for {resolved_date} in {path}")
    if "selected" not in day.columns:
        raise ValueError(f"{path} does not contain a selected column")
    selected = day[day["selected"].astype(bool)].copy()
    if selected.empty:
        raise ValueError(f"no selected rows for {resolved_date} in {path}")
    score_column = _score_column(selected)
    if score_column is not None:
        selected = selected.sort_values(score_column, ascending=False)
    if limit is not None:
        selected = selected.head(limit)
    return selected, resolved_date


def load_identity_map(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    frame = pd.read_parquet(path)
    identity: dict[str, dict[str, Any]] = {}
    for row in frame.to_dict(orient="records"):
        symbol = str(row.get("instrument", "")).strip().upper()
        if symbol:
            identity[symbol] = row
    return identity


def build_candidate_payload(
    config: CandidateBatchConfig,
    rows: pd.DataFrame,
    *,
    identity: Mapping[str, Mapping[str, Any]],
    account_key: str,
    trade_date: date,
    candidate_date: str,
    target_notional: str,
    start_at: datetime,
    end_at: datetime,
    generated_at: datetime,
    capital_plan: Mapping[str, Any] | None = None,
    run_suffix: str = "",
) -> dict[str, Any]:
    batch_suffix = f"-{run_suffix}" if run_suffix else ""
    source = {
        "system": "q-training",
        "batch_id": (
            f"rl-{config.side.lower()}-{trade_date.isoformat()}-"
            f"selected-{candidate_date}{batch_suffix}"
        ),
        "generated_at": generated_at.isoformat(),
        "release_id": (
            f"rl_selected_{config.side.lower()}_{trade_date.isoformat()}"
            f"{batch_suffix}"
        ),
        "strategy_id": config.strategy_id,
        "policy_id": config.model_key,
    }
    instructions = [
        build_candidate_instruction(
            config,
            row,
            identity=identity,
            account_key=account_key,
            trade_date=trade_date,
            candidate_date=candidate_date,
            target_notional=target_notional,
            capital_plan=capital_plan,
            start_at=start_at,
            end_at=end_at,
            ordinal=ordinal,
            run_suffix=run_suffix,
        )
        for ordinal, row in enumerate(rows.to_dict(orient="records"), start=1)
    ]
    return {
        "schema_version": "2026-04-25",
        "source": source,
        "instructions": instructions,
    }


def build_candidate_instruction(
    config: CandidateBatchConfig,
    row: Mapping[str, Any],
    *,
    identity: Mapping[str, Mapping[str, Any]],
    account_key: str,
    trade_date: date,
    candidate_date: str,
    target_notional: str,
    start_at: datetime,
    end_at: datetime,
    ordinal: int,
    capital_plan: Mapping[str, Any] | None = None,
    run_suffix: str = "",
) -> dict[str, Any]:
    symbol = str(row["instrument"]).strip().upper()
    identity_row = identity.get(symbol, {})
    company_name = _clean_optional(identity_row.get("company_name"))
    aliases = _parse_aliases(identity_row.get("instrument_aliases_json"))
    ticker_alias = _clean_optional(identity_row.get("ticker_alias"))
    if ticker_alias is not None and ticker_alias not in aliases:
        aliases.insert(0, ticker_alias)
    yahoo_symbol = _clean_optional(identity_row.get("yahoo_symbol"))
    if yahoo_symbol is not None and yahoo_symbol not in aliases:
        aliases.append(yahoo_symbol)

    position_side = config.side
    id_suffix = f"-{run_suffix}" if run_suffix else ""
    trace_metadata = {
        "source": "lockbox_candidate_tape",
        "candidate_date": candidate_date,
        "candidate_tape_lineage_path": str(config.candidate_tape_path),
        "selected": str(row.get("selected")),
        "score_column": _score_column(pd.DataFrame([row])),
    }
    static_features = static_feature_metadata(config, row)
    if static_features is not None:
        trace_metadata["static_features"] = static_features
    if capital_plan is not None:
        trace_metadata["capital_plan"] = dict(capital_plan)

    return {
        "instruction_id": (
            f"{trade_date.isoformat()}-{account_key.upper()}-"
            f"rl-{config.side.lower()}-{symbol}-model-routed{id_suffix}-{ordinal:02d}"
        ),
        "account": {
            "account_key": account_key.upper(),
            "book_key": config.book_key,
            "book_role": "virtual",
            "book_side": position_side,
        },
        "instrument": {
            "symbol": symbol,
            "security_type": "STK",
            "exchange": "SMART",
            "primary_exchange": "SFB",
            "currency": "SEK",
            "isin": _clean_optional(identity_row.get("isin")),
            "aliases": aliases,
        },
        "intent": {
            "side": "BUY" if position_side == "LONG" else "SELL",
            "position_side": position_side,
        },
        "sizing": {
            "mode": "target_notional",
            "target_notional": target_notional,
        },
        "execution": {
            "mode": "model_routed",
            "model_id": config.model_key,
            "model_family": config.model_family,
            "model_version": "v1",
            "model_artifact_id": config.model_artifact_id,
            "window": {
                "start_at": start_at.isoformat(),
                "end_at": end_at.isoformat(),
            },
        },
        "trace": {
            "reason_code": "rl_model_routed_selected_candidate",
            "trade_date": trade_date.isoformat(),
            "data_cutoff_date": candidate_date,
            "company_name": company_name,
            "metadata": trace_metadata,
        },
    }


def resolve_capital_plan(
    *,
    side: str,
    account_key: str,
    candidate_count: int,
    default_target_notional: str,
    account_equity_reference: str | None,
    long_allocation_pct: str | None = None,
    short_allocation_pct: str | None = None,
    long_budget: str | None,
    short_budget: str | None,
    max_notional_per_name: str | None,
    min_order_notional: str | None = "1000",
    max_long_gross_account_pct: str = "0.90",
    max_short_gross_account_pct: str = "0.80",
    max_total_gross_account_pct: str = "1.70",
    max_abs_net_exposure_account_pct: str = "0.25",
    min_excess_liquidity_buffer_pct: str = "0.20",
) -> tuple[str, dict[str, Any]]:
    normalized_side = side.upper()
    side_budget = long_budget if normalized_side == "LONG" else short_budget
    side_allocation_pct = (
        long_allocation_pct if normalized_side == "LONG" else short_allocation_pct
    )
    max_book_gross_account_pct = (
        max_long_gross_account_pct
        if normalized_side == "LONG"
        else max_short_gross_account_pct
    )
    target_notional = _clean_decimal_string(Decimal(str(default_target_notional)))
    strategy_gross_budget = None
    book_allocation_pct = None
    allocation_method = "fixed_per_name"

    if side_budget not in (None, ""):
        if candidate_count <= 0:
            raise ValueError(f"{normalized_side} capital budget requires candidates")
        allocation_method = "gross_budget_equal_weight"
        strategy_gross_budget = Decimal(str(side_budget))
        if strategy_gross_budget <= 0:
            raise ValueError(f"{normalized_side} strategy budget must be positive")
    elif (
        account_equity_reference not in (None, "")
        and side_allocation_pct not in (None, "")
    ):
        if candidate_count <= 0:
            raise ValueError(f"{normalized_side} allocation percentage requires candidates")
        allocation_method = "account_pct_gross_exposure_equal_weight"
        account_equity = Decimal(str(account_equity_reference))
        if account_equity <= 0:
            raise ValueError("account_equity_reference must be positive")
        book_allocation_pct_decimal = Decimal(str(side_allocation_pct))
        if book_allocation_pct_decimal <= 0:
            raise ValueError(f"{normalized_side} allocation percentage must be positive")
        strategy_gross_budget = account_equity * book_allocation_pct_decimal
        book_allocation_pct = _clean_decimal_string(book_allocation_pct_decimal)

    if strategy_gross_budget is not None:
        per_name = (strategy_gross_budget / Decimal(candidate_count)).quantize(
            Decimal("1"),
            rounding=ROUND_DOWN,
        )
        if max_notional_per_name not in (None, ""):
            cap = Decimal(str(max_notional_per_name))
            if cap <= 0:
                raise ValueError("max_notional_per_name must be positive")
            per_name = min(per_name, cap)
        if min_order_notional not in (None, ""):
            minimum = Decimal(str(min_order_notional))
            if minimum <= 0:
                raise ValueError("min_order_notional must be positive")
            if per_name < minimum:
                raise ValueError(
                    f"{normalized_side} per-name target notional {per_name} "
                    f"is below min_order_notional {minimum}"
                )
        if per_name <= 0:
            raise ValueError(f"{normalized_side} per-name target notional rounded to zero")
        target_notional = _clean_decimal_string(per_name)

    capital_plan = {
        "schema_version": "rl_capital_plan_v2",
        "allocation_method": allocation_method,
        "account_key": account_key.upper(),
        "account_currency": "SEK",
        "account_equity_reference": account_equity_reference,
        "capital_base": "net_liquidation_value",
        "strategy_key": (
            "bucket_booster_long" if normalized_side == "LONG" else "bucket_booster_short"
        ),
        "strategy_side": normalized_side,
        "book_allocation_pct": book_allocation_pct,
        "max_book_gross_account_pct": max_book_gross_account_pct,
        "strategy_gross_budget": (
            _clean_decimal_string(strategy_gross_budget)
            if strategy_gross_budget is not None
            else None
        ),
        "candidate_count": candidate_count,
        "per_name_target_notional": target_notional,
        "max_notional_per_name": max_notional_per_name,
        "min_order_notional": min_order_notional,
        "rounding": "whole_shares_down",
        "require_shortable": normalized_side == "SHORT",
        "require_borrow_rate_available": normalized_side == "SHORT",
        "short_sale_proceeds_reinvested": False,
        "allocation_guard": {
            "schema_version": "rl_allocation_guard_v1",
            "account_key": account_key.upper(),
            "capital_base": "net_liquidation_value",
            "max_long_gross_account_pct": max_long_gross_account_pct,
            "max_short_gross_account_pct": max_short_gross_account_pct,
            "max_total_gross_account_pct": max_total_gross_account_pct,
            "max_abs_net_exposure_account_pct": max_abs_net_exposure_account_pct,
            "min_excess_liquidity_buffer_pct": min_excess_liquidity_buffer_pct,
            "block_if_margin_preflight_fails": True,
            "block_if_projected_maintenance_margin_exceeded": True,
        },
    }
    return target_notional, capital_plan


def _clean_decimal_string(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral_value():
        return str(normalized.quantize(Decimal("1")))
    return format(normalized, "f")


def static_feature_metadata(
    config: CandidateBatchConfig,
    row: Mapping[str, Any],
) -> dict[str, Any] | None:
    if config.static_feature_cols_path is None:
        return None
    if not config.static_feature_cols_path.exists():
        return None
    feature_names = list(read_static_feature_names(config.static_feature_cols_path))
    values = [float(row[name]) for name in feature_names]
    if any(pd.isna(value) for value in values):
        raise ValueError(
            f"static features contain NaN for {config.model_key} {row.get('instrument')}"
        )
    return {
        "schema_version": "rl_static_features_v1",
        "model_key": config.model_key,
        "feature_names": feature_names,
        "values": values,
        "normalized": False,
        "source": "lockbox_candidate_row",
    }


def _clean_optional(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _parse_aliases(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if value is None or pd.isna(value):
        return []
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


def normalize_run_suffix(value: str) -> str:
    suffix = value.strip()
    if not suffix:
        return ""
    normalized = "".join(
        character.lower() if character.isalnum() else "-"
        for character in suffix
    ).strip("-")
    normalized = "-".join(part for part in normalized.split("-") if part)
    if not normalized:
        raise ValueError("run-suffix must contain at least one letter or digit")
    return normalized


def post_json(url: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"POST {url} failed with HTTP {exc.code}: {body}") from exc


if __name__ == "__main__":
    raise SystemExit(main())
