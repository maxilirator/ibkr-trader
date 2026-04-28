#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timezone
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pandas as pd

from ibkr_trader.rl.model_artifacts import promoted_rl_models
from ibkr_trader.rl.model_artifacts import read_static_feature_names


IDENTITY_PATH = Path("/home/mattias/dev/q-data/xsto/meta/instrument_identity.parquet")
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


CONFIGS: tuple[CandidateBatchConfig, ...] = tuple(
    CandidateBatchConfig(
        side=artifact.side_upper,
        strategy_id=artifact.strategy_id,
        model_key=artifact.model_key,
        model_family=artifact.model_family,
        model_artifact_id=artifact.model_artifact_id,
        deployment_key=artifact.deployment_key,
        book_key=artifact.book_key,
        candidate_tape_path=artifact.candidate_tape_path,
        static_feature_cols_path=artifact.static_feature_cols_path,
    )
    for artifact in promoted_rl_models()
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Submit selected bucket RL names as model-routed trader candidates."
    )
    parser.add_argument("--api-base", default="http://quant.geisler.se:8000")
    parser.add_argument("--account-key", default="VIRTUALRL01")
    parser.add_argument("--trade-date", default=_today_stockholm().isoformat())
    parser.add_argument("--candidate-date", default="latest")
    parser.add_argument("--target-notional", default="1000")
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
        payload = build_candidate_payload(
            config,
            rows,
            identity=identity,
            account_key=args.account_key,
            trade_date=trade_date,
            candidate_date=candidate_date,
            target_notional=str(args.target_notional),
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
    run_suffix: str = "",
) -> dict[str, Any]:
    batch_suffix = f"-{run_suffix}" if run_suffix else ""
    source = {
        "system": "q-training-bucket",
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
        "candidate_tape": str(config.candidate_tape_path),
        "selected": str(row.get("selected")),
        "score_column": _score_column(pd.DataFrame([row])),
    }
    static_features = static_feature_metadata(config, row)
    if static_features is not None:
        trace_metadata["static_features"] = static_features

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
            "funding_basis": "cash",
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
        "normalized": True,
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
