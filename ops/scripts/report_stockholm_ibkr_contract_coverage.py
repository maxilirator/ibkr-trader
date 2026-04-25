#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import replace
import json
from pathlib import Path
import re
import sys
import time
from typing import Any

from ibkr_trader.config import AppConfig
from ibkr_trader.domain.contract_resolution import ContractResolveQuery
from ibkr_trader.ibkr.contracts import resolve_contracts
from ibkr_trader.ibkr.sync_wrapper import load_response_timeout_class
from ibkr_trader.ibkr.sync_wrapper import load_sync_wrapper_class


DEFAULT_SECURITY_TYPE = "STK"
DEFAULT_EXCHANGE = "SMART"
DEFAULT_PRIMARY_EXCHANGE = "SFB"
DEFAULT_CURRENCY = "SEK"


@dataclass(slots=True)
class StockholmIdentity:
    slug: str
    company_name: str | None
    share_class: str | None
    isin: str | None
    ticker_alias: str | None
    yahoo_symbol: str | None
    instrument_aliases: tuple[str, ...]


@dataclass(slots=True)
class AttemptSpec:
    label: str
    query: ContractResolveQuery


def _normalize_token(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"[^A-Z0-9]", "", value.upper())
    return normalized or None


def _strip_yahoo_suffix(value: str | None) -> str | None:
    if not value:
        return None
    upper_value = value.upper().strip()
    if upper_value.endswith(".ST"):
        return upper_value[:-3]
    return upper_value


def _load_current_stockholm_universe(path: Path) -> list[str]:
    slugs: list[str] = []
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.reader(handle, delimiter="\t")
        for row in reader:
            if not row:
                continue
            slug = row[0].strip().lower()
            if slug:
                slugs.append(slug)
    return slugs


def _load_stockholm_identity_map(path: Path) -> dict[str, StockholmIdentity]:
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "duckdb is required for this report. Install the server extras with "
            "`pip install -e .[server]`."
        ) from exc

    connection = duckdb.connect()
    rows = connection.execute(
        """
        SELECT
            lower(instrument) AS slug,
            company_name,
            share_class,
            isin,
            ticker_alias,
            yahoo_symbol,
            instrument_aliases_json
        FROM read_parquet(?)
        """,
        [str(path)],
    ).fetchall()

    identity_map: dict[str, StockholmIdentity] = {}
    for (
        slug,
        company_name,
        share_class,
        isin,
        ticker_alias,
        yahoo_symbol,
        instrument_aliases_json,
    ) in rows:
        parsed_aliases: tuple[str, ...] = ()
        if instrument_aliases_json:
            try:
                raw_aliases = json.loads(instrument_aliases_json)
            except json.JSONDecodeError:
                raw_aliases = []
            parsed_aliases = tuple(
                str(alias)
                for alias in raw_aliases
                if isinstance(alias, str) and alias.strip()
            )
        identity_map[str(slug)] = StockholmIdentity(
            slug=str(slug),
            company_name=(str(company_name) if company_name else None),
            share_class=(str(share_class) if share_class else None),
            isin=(str(isin) if isin else None),
            ticker_alias=(str(ticker_alias).upper() if ticker_alias else None),
            yahoo_symbol=(str(yahoo_symbol).upper() if yahoo_symbol else None),
            instrument_aliases=parsed_aliases,
        )
    return identity_map


def _build_known_aliases(slug: str, identity: StockholmIdentity | None) -> tuple[str, ...]:
    aliases: list[str] = []

    def add(value: str | None) -> None:
        if not value:
            return
        stripped = value.strip().upper()
        if not stripped:
            return
        aliases.append(stripped)
        yahoo_root = _strip_yahoo_suffix(stripped)
        if yahoo_root and yahoo_root != stripped:
            aliases.append(yahoo_root)

    slug_upper = slug.upper()
    add(slug_upper)
    if "-" in slug_upper:
        root, suffix = slug_upper.split("-", 1)
        add(f"{root} {suffix}")
        add(f"{root}.{suffix}")
        add(f"{root}{suffix}")

    if identity is not None:
        add(identity.ticker_alias)
        add(identity.yahoo_symbol)

    deduped: list[str] = []
    seen: set[str] = set()
    for alias in aliases:
        if alias in seen:
            continue
        seen.add(alias)
        deduped.append(alias)
    return tuple(deduped)


def _build_primary_attempt(slug: str, identity: StockholmIdentity | None) -> AttemptSpec:
    display_symbol = (identity.ticker_alias if identity and identity.ticker_alias else slug.upper())
    query = ContractResolveQuery(
        symbol=display_symbol,
        security_type=DEFAULT_SECURITY_TYPE,
        exchange=DEFAULT_EXCHANGE,
        currency=DEFAULT_CURRENCY,
        primary_exchange=DEFAULT_PRIMARY_EXCHANGE,
        isin=(identity.isin if identity else None),
    )
    return AttemptSpec(label="primary_alias_isin_smart", query=query)


def _build_fallback_attempts(slug: str, identity: StockholmIdentity | None) -> tuple[AttemptSpec, ...]:
    attempts: list[AttemptSpec] = []

    def add(
        label: str,
        *,
        symbol: str | None,
        exchange: str = DEFAULT_EXCHANGE,
        primary_exchange: str | None = DEFAULT_PRIMARY_EXCHANGE,
        local_symbol: str | None = None,
        isin: str | None = None,
    ) -> None:
        if not symbol:
            return
        query = ContractResolveQuery(
            symbol=symbol,
            security_type=DEFAULT_SECURITY_TYPE,
            exchange=exchange,
            currency=DEFAULT_CURRENCY,
            primary_exchange=primary_exchange,
            local_symbol=local_symbol,
            isin=isin,
        )
        attempts.append(AttemptSpec(label=label, query=query))

    slug_upper = slug.upper()
    alias = identity.ticker_alias if identity else None
    isin = identity.isin if identity else None
    yahoo_root = _strip_yahoo_suffix(identity.yahoo_symbol if identity else None)

    if alias:
        add("alias_local_isin_smart", symbol=alias, local_symbol=alias, isin=isin)
        add("alias_isin_sfb", symbol=alias, exchange="SFB", primary_exchange=None, isin=isin)

    if yahoo_root:
        add("yahoo_root_isin_smart", symbol=yahoo_root, isin=isin)
        add("yahoo_root_isin_sfb", symbol=yahoo_root, exchange="SFB", primary_exchange=None, isin=isin)

    if "-" in slug_upper:
        root, suffix = slug_upper.split("-", 1)
        share_space = f"{root} {suffix}"
        share_dot = f"{root}.{suffix}"
        share_concat = f"{root}{suffix}"
        add("slug_space_isin_smart", symbol=share_space, isin=isin)
        add("slug_dot_isin_smart", symbol=share_dot, isin=isin)
        add("slug_concat_isin_smart", symbol=share_concat, isin=isin)
        add("slug_root_local_isin_smart", symbol=root, local_symbol=share_space, isin=isin)

    unique_attempts: list[AttemptSpec] = []
    seen: set[tuple[str, str, str | None, str | None, str | None]] = set()
    for attempt in attempts:
        key = (
            attempt.query.symbol,
            attempt.query.exchange,
            attempt.query.primary_exchange,
            attempt.query.local_symbol,
            attempt.query.isin,
        )
        if key in seen:
            continue
        seen.add(key)
        unique_attempts.append(attempt)
    return tuple(unique_attempts)


def _serialize_query(query: ContractResolveQuery) -> dict[str, Any]:
    return {
        "symbol": query.symbol,
        "exchange": query.exchange,
        "primary_exchange": query.primary_exchange,
        "currency": query.currency,
        "security_type": query.security_type,
        "local_symbol": query.local_symbol,
        "isin": query.isin,
    }


def _serialize_match(match: Any) -> dict[str, Any]:
    payload = asdict(match)
    payload["min_tick"] = str(payload["min_tick"]) if payload["min_tick"] is not None else None
    payload["valid_exchanges"] = list(payload["valid_exchanges"])
    payload["order_types"] = list(payload["order_types"])
    payload["sec_ids"] = dict(payload["sec_ids"])
    return payload


def _attempt_resolution(
    app: Any,
    config: Any,
    response_timeout_cls: type[Exception],
    attempt: AttemptSpec,
    *,
    timeout: int,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    try:
        result = resolve_contracts(
            config,
            attempt.query,
            timeout=timeout,
            app=app,
            response_timeout_cls=response_timeout_cls,
        )
    except LookupError as exc:
        return {
            "attempt": attempt.label,
            "query": _serialize_query(attempt.query),
            "status": "lookup_error",
            "elapsed_seconds": round(time.perf_counter() - started_at, 3),
            "detail": str(exc),
        }
    except TimeoutError as exc:
        return {
            "attempt": attempt.label,
            "query": _serialize_query(attempt.query),
            "status": "timeout",
            "elapsed_seconds": round(time.perf_counter() - started_at, 3),
            "detail": str(exc),
        }
    except Exception as exc:  # pragma: no cover - live ops guard
        return {
            "attempt": attempt.label,
            "query": _serialize_query(attempt.query),
            "status": "error",
            "elapsed_seconds": round(time.perf_counter() - started_at, 3),
            "detail": f"{type(exc).__name__}: {exc}",
        }

    if result.match_count == 0:
        return {
            "attempt": attempt.label,
            "query": _serialize_query(attempt.query),
            "status": "no_match",
            "elapsed_seconds": round(time.perf_counter() - started_at, 3),
            "match_count": 0,
        }

    if result.match_count > 1:
        return {
            "attempt": attempt.label,
            "query": _serialize_query(attempt.query),
            "status": "multiple_matches",
            "elapsed_seconds": round(time.perf_counter() - started_at, 3),
            "match_count": result.match_count,
            "matches": [_serialize_match(match) for match in result.matches[:3]],
        }

    return {
        "attempt": attempt.label,
        "query": _serialize_query(attempt.query),
        "status": "resolved",
        "elapsed_seconds": round(time.perf_counter() - started_at, 3),
        "match_count": 1,
        "match": _serialize_match(result.matches[0]),
    }


def _classify_resolution(
    slug: str,
    identity: StockholmIdentity | None,
    resolution: dict[str, Any],
) -> tuple[str, list[str]]:
    if resolution["status"] != "resolved":
        return "does_not_resolve_at_ibkr", [resolution["status"]]

    match = resolution["match"]
    known_aliases = {_normalize_token(value) for value in _build_known_aliases(slug, identity)}
    known_aliases.discard(None)

    flags: list[str] = []
    resolved_symbol = _normalize_token(match.get("symbol"))
    resolved_local_symbol = _normalize_token(match.get("local_symbol"))
    resolved_isin = (match.get("sec_ids") or {}).get("ISIN")
    expected_isin = identity.isin if identity is not None else None

    resolved_symbol_known = bool(resolved_symbol and resolved_symbol in known_aliases)
    resolved_local_symbol_known = bool(
        resolved_local_symbol and resolved_local_symbol in known_aliases
    )

    if not resolved_symbol_known and resolved_local_symbol_known:
        flags.append("resolved_symbol_remapped")
    elif not resolved_symbol_known and not resolved_local_symbol_known:
        flags.append("resolved_symbol_not_in_known_aliases")

    if expected_isin:
        if resolved_isin is None:
            flags.append("resolved_contract_missing_isin")
        elif resolved_isin != expected_isin:
            flags.append("isin_mismatch")

    if flags:
        return "resolves_suspiciously_remapped", flags
    return "resolves_cleanly", []


def _build_entry(
    slug: str,
    identity: StockholmIdentity | None,
    resolution: dict[str, Any],
    *,
    attempts: list[dict[str, Any]],
) -> dict[str, Any]:
    classification, flags = _classify_resolution(slug, identity, resolution)
    return {
        "slug": slug,
        "company_name": identity.company_name if identity is not None else None,
        "share_class": identity.share_class if identity is not None else None,
        "ticker_alias": identity.ticker_alias if identity is not None else None,
        "yahoo_symbol": identity.yahoo_symbol if identity is not None else None,
        "isin": identity.isin if identity is not None else None,
        "instrument_aliases": list(identity.instrument_aliases) if identity is not None else [],
        "classification": classification,
        "flags": flags,
        "resolved_via_attempt": resolution["attempt"],
        "resolution": resolution,
        "attempts": attempts,
    }


def _write_markdown_report(payload: dict[str, Any], path: Path) -> None:
    summary = payload["summary"]
    lines = [
        "# Stockholm IBKR Contract Coverage",
        "",
        f"- universe size: {summary['universe_size']}",
        f"- resolves cleanly: {summary['resolves_cleanly']}",
        f"- resolves suspiciously/remapped: {summary['resolves_suspiciously_remapped']}",
        f"- does not resolve at IBKR: {summary['does_not_resolve_at_ibkr']}",
        "",
        "## Suspicious Examples",
        "",
    ]

    suspicious_entries = [
        entry for entry in payload["entries"]
        if entry["classification"] == "resolves_suspiciously_remapped"
    ][:20]
    if suspicious_entries:
        for entry in suspicious_entries:
            resolution = entry["resolution"]
            match = resolution.get("match", {})
            lines.append(
                f"- `{entry['slug']}` -> `{match.get('symbol')}` / `{match.get('local_symbol')}` "
                f"flags={entry['flags']} via `{entry['resolved_via_attempt']}`"
            )
    else:
        lines.append("- none")

    lines.extend(["", "## Unresolved Examples", ""])
    unresolved_entries = [
        entry for entry in payload["entries"]
        if entry["classification"] == "does_not_resolve_at_ibkr"
    ][:20]
    if unresolved_entries:
        for entry in unresolved_entries:
            resolution = entry["resolution"]
            lines.append(
                f"- `{entry['slug']}` -> `{resolution['status']}` "
                f"via `{entry['resolved_via_attempt']}`"
            )
    else:
        lines.append("- none")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify the current Stockholm universe against IBKR contract resolution.",
    )
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--timeout-seconds", type=int, default=5)
    parser.add_argument("--sleep-seconds", type=float, default=0.1)
    parser.add_argument("--client-id", type=int, default=91)
    parser.add_argument("--skip-fallbacks", action="store_true")
    parser.add_argument(
        "--slugs-file",
        type=Path,
        default=None,
        help="Optional file with one Stockholm instrument slug per line.",
    )
    parser.add_argument("--output-json", type=Path, default=None)
    parser.add_argument("--output-markdown", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    app_config = AppConfig.from_env()
    if args.slugs_file is not None:
        universe = [
            line.strip().lower()
            for line in args.slugs_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    else:
        universe = _load_current_stockholm_universe(app_config.stockholm_instruments_path)
    if args.limit is not None:
        universe = universe[: args.limit]

    identity_map = _load_stockholm_identity_map(app_config.stockholm_identity_path)

    wrapper_cls = load_sync_wrapper_class()
    response_timeout_cls = load_response_timeout_class()
    ibkr_config = replace(app_config.ibkr, client_id=args.client_id)
    app = wrapper_cls(timeout=args.timeout_seconds)

    if not app.connect_and_start(
        host=ibkr_config.host,
        port=ibkr_config.port,
        client_id=ibkr_config.client_id,
    ):
        raise SystemExit(
            f"Failed to connect to IBKR at {ibkr_config.host}:{ibkr_config.port} "
            f"with client_id={ibkr_config.client_id}."
        )

    entries: list[dict[str, Any]] = []
    started_at = time.time()
    try:
        for index, slug in enumerate(universe, start=1):
            identity = identity_map.get(slug)
            primary_attempt = _build_primary_attempt(slug, identity)
            attempts: list[dict[str, Any]] = []

            primary_resolution = _attempt_resolution(
                app,
                ibkr_config,
                response_timeout_cls,
                primary_attempt,
                timeout=args.timeout_seconds,
            )
            attempts.append(primary_resolution)

            final_resolution = primary_resolution
            if primary_resolution["status"] != "resolved" and not args.skip_fallbacks:
                for attempt in _build_fallback_attempts(slug, identity):
                    fallback_resolution = _attempt_resolution(
                        app,
                        ibkr_config,
                        response_timeout_cls,
                        attempt,
                        timeout=args.timeout_seconds,
                    )
                    attempts.append(fallback_resolution)
                    if fallback_resolution["status"] == "resolved":
                        final_resolution = fallback_resolution
                        break
                else:
                    final_resolution = attempts[-1]

            entries.append(
                _build_entry(
                    slug,
                    identity,
                    final_resolution,
                    attempts=attempts,
                )
            )

            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)

            if index % 50 == 0:
                print(
                    f"[{index}/{len(universe)}] "
                    f"clean={sum(1 for entry in entries if entry['classification'] == 'resolves_cleanly')} "
                    f"suspicious={sum(1 for entry in entries if entry['classification'] == 'resolves_suspiciously_remapped')} "
                    f"unresolved={sum(1 for entry in entries if entry['classification'] == 'does_not_resolve_at_ibkr')}",
                    file=sys.stderr,
                )
    finally:
        app.disconnect_and_stop()

    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "config": {
            "host": ibkr_config.host,
            "port": ibkr_config.port,
            "client_id": ibkr_config.client_id,
            "timeout_seconds": args.timeout_seconds,
            "sleep_seconds": args.sleep_seconds,
            "universe_path": str(app_config.stockholm_instruments_path),
            "identity_path": str(app_config.stockholm_identity_path),
        },
        "summary": {
            "universe_size": len(universe),
            "resolves_cleanly": sum(
                1 for entry in entries if entry["classification"] == "resolves_cleanly"
            ),
            "resolves_suspiciously_remapped": sum(
                1
                for entry in entries
                if entry["classification"] == "resolves_suspiciously_remapped"
            ),
            "does_not_resolve_at_ibkr": sum(
                1
                for entry in entries
                if entry["classification"] == "does_not_resolve_at_ibkr"
            ),
            "elapsed_seconds": round(time.time() - started_at, 3),
        },
        "entries": entries,
    }

    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    if args.output_markdown is not None:
        _write_markdown_report(payload, args.output_markdown)

    print(json.dumps(payload["summary"], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
