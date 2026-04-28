from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
import math
from pathlib import Path
import re
import time as runtime_time
from typing import Any
from zoneinfo import ZoneInfo

from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.ibkr.historical_bars import HistoricalBarsQuery, read_historical_bars


DEFAULT_STOCKHOLM_INTRADAY_TYPES = (
    "TRADES",
    "MIDPOINT",
    "BID",
    "ASK",
    "ADJUSTED_LAST",
)

ContractDetailsCacheKey = tuple[
    str,
    str,
    str,
    str,
    str | None,
    str | None,
    str | None,
]


@dataclass(slots=True)
class StockholmInstrumentIdentity:
    slug: str
    company_name: str | None
    share_class: str | None
    isin: str | None
    ticker_alias: str | None
    yahoo_symbol: str | None


@dataclass(slots=True)
class StockholmIntradayBackfillQuery:
    as_of_date: date
    bar_size: str = "1 min"
    what_to_show: tuple[str, ...] = DEFAULT_STOCKHOLM_INTRADAY_TYPES
    use_rth: bool = True
    max_symbols: int = 25
    start_after: str | None = None
    symbols: tuple[str, ...] | None = None
    include_remapped: bool = False
    sleep_seconds: float = 0.05
    max_runtime_seconds: float | None = 55.0

    def validate(self) -> None:
        if not self.bar_size:
            raise ValueError("bar_size is required")
        if not self.what_to_show:
            raise ValueError("what_to_show must contain at least one value")
        if self.max_symbols <= 0:
            raise ValueError("max_symbols must be positive")
        if self.max_symbols > 100:
            raise ValueError("max_symbols must be at most 100")
        if self.sleep_seconds < 0:
            raise ValueError("sleep_seconds must be non-negative")
        if self.max_runtime_seconds is not None:
            if self.max_runtime_seconds <= 0:
                raise ValueError("max_runtime_seconds must be positive when provided")
            if self.max_runtime_seconds > 3600:
                raise ValueError("max_runtime_seconds must be at most 3600")


def _load_current_stockholm_universe(path: Path) -> list[str]:
    slugs: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip():
            continue
        slug = raw_line.split("\t", 1)[0].strip().lower()
        if slug:
            slugs.append(slug)
    return slugs


def _load_stockholm_identity_map(path: Path) -> dict[str, StockholmInstrumentIdentity]:
    try:
        import duckdb
    except ModuleNotFoundError as exc:  # pragma: no cover - runtime dependency
        raise RuntimeError(
            f"duckdb is required to read Stockholm identity parquet at {path}"
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
            yahoo_symbol
        FROM read_parquet(?)
        """,
        [str(path)],
    ).fetchall()

    identity_map: dict[str, StockholmInstrumentIdentity] = {}
    for slug, company_name, share_class, isin, ticker_alias, yahoo_symbol in rows:
        identity_map[str(slug)] = StockholmInstrumentIdentity(
            slug=str(slug),
            company_name=(str(company_name) if company_name else None),
            share_class=(str(share_class) if share_class else None),
            isin=(str(isin) if isin else None),
            ticker_alias=(str(ticker_alias).upper() if ticker_alias else None),
            yahoo_symbol=(str(yahoo_symbol).upper() if yahoo_symbol else None),
        )
    return identity_map


def _normalize_token(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = re.sub(r"[^A-Z0-9]", "", value.upper())
    return normalized or None


def _strip_yahoo_suffix(value: str | None) -> str | None:
    if not value:
        return None
    upper_value = value.strip().upper()
    if upper_value.endswith(".ST"):
        return upper_value[:-3]
    return upper_value


def _build_current_known_aliases(
    slug: str,
    identity: StockholmInstrumentIdentity | None,
) -> tuple[str, ...]:
    aliases: list[str] = []

    def add(value: str | None) -> None:
        if not value:
            return
        normalized = value.strip().upper()
        if not normalized:
            return
        aliases.append(normalized)
        yahoo_root = _strip_yahoo_suffix(normalized)
        if yahoo_root and yahoo_root != normalized:
            aliases.append(yahoo_root)

    slug_upper = slug.upper()
    add(slug_upper)
    if "-" in slug_upper:
        root, suffix = slug_upper.split("-", 1)
        add(f"{root} {suffix}")
        add(f"{root}{suffix}")
        add(f"{root}.{suffix}")

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


def _classify_resolution(
    slug: str,
    identity: StockholmInstrumentIdentity | None,
    resolved_contract: dict[str, Any],
) -> tuple[str, list[str]]:
    known_aliases = {
        _normalize_token(alias)
        for alias in _build_current_known_aliases(slug, identity)
    }
    known_aliases.discard(None)

    resolved_symbol = _normalize_token(str(resolved_contract.get("symbol", "")))
    resolved_local_symbol = _normalize_token(str(resolved_contract.get("local_symbol", "")))

    resolved_symbol_known = bool(resolved_symbol and resolved_symbol in known_aliases)
    resolved_local_symbol_known = bool(
        resolved_local_symbol and resolved_local_symbol in known_aliases
    )

    flags: list[str] = []
    if not resolved_symbol_known and resolved_local_symbol_known:
        flags.append("resolved_symbol_remapped")
    elif not resolved_symbol_known and not resolved_local_symbol_known:
        flags.append("resolved_symbol_not_in_current_aliases")

    expected_isin = identity.isin if identity is not None else None
    resolved_isin = (resolved_contract.get("sec_ids") or {}).get("ISIN")
    if expected_isin:
        if resolved_isin is None:
            flags.append("resolved_contract_missing_isin")
        elif resolved_isin != expected_isin:
            flags.append("isin_mismatch")

    if flags:
        return "resolves_suspiciously_remapped", flags
    return "resolves_cleanly", []


def _build_symbol_page(
    universe: list[str],
    *,
    max_symbols: int,
    start_after: str | None,
    explicit_symbols: tuple[str, ...] | None,
) -> tuple[list[str], str | None]:
    if explicit_symbols is not None:
        unique_symbols: list[str] = []
        seen: set[str] = set()
        for symbol in explicit_symbols:
            normalized = symbol.strip().lower()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            unique_symbols.append(normalized)
        page = unique_symbols[:max_symbols]
        next_cursor = page[-1] if len(unique_symbols) > len(page) and page else None
        return page, next_cursor

    sorted_universe = sorted(universe)
    start_index = 0
    if start_after:
        normalized_cursor = start_after.strip().lower()
        while start_index < len(sorted_universe) and sorted_universe[start_index] <= normalized_cursor:
            start_index += 1
    page = sorted_universe[start_index : start_index + max_symbols]
    next_cursor = None
    if start_index + max_symbols < len(sorted_universe) and page:
        next_cursor = page[-1]
    return page, next_cursor


def _build_historical_query(
    slug: str,
    identity: StockholmInstrumentIdentity | None,
    *,
    as_of_date: date,
    bar_size: str,
    what_to_show: str,
    use_rth: bool,
) -> HistoricalBarsQuery:
    symbol = (
        identity.ticker_alias
        if identity is not None and identity.ticker_alias
        else slug.upper()
    )
    end_at = datetime.combine(
        as_of_date,
        time(hour=17, minute=30),
        tzinfo=ZoneInfo("Europe/Stockholm"),
    )
    return HistoricalBarsQuery(
        symbol=symbol,
        security_type="STK",
        exchange="SMART",
        currency="SEK",
        primary_exchange="SFB",
        isin=(identity.isin if identity is not None else None),
        duration="1 D",
        bar_size=bar_size,
        what_to_show=what_to_show,
        use_rth=use_rth,
        end_at=end_at,
    )


def _call_timeout(
    *,
    base_timeout: int,
    deadline_at: float | None,
) -> int | None:
    if deadline_at is None:
        return base_timeout

    remaining_seconds = deadline_at - runtime_time.monotonic()
    if remaining_seconds <= 0:
        return None
    return max(1, min(base_timeout, math.ceil(remaining_seconds)))


def _series_is_unsupported_for_dated_intraday_backfill(series_name: str) -> bool:
    return series_name.upper() == "ADJUSTED_LAST"


def collect_stockholm_intraday_backfill(
    config: IbkrConnectionConfig,
    query: StockholmIntradayBackfillQuery,
    *,
    instruments_path: Path,
    identity_path: Path,
    timeout: int = 20,
    app: Any | None = None,
) -> dict[str, Any]:
    query.validate()
    started_monotonic = runtime_time.monotonic()
    deadline_at = (
        started_monotonic + query.max_runtime_seconds
        if query.max_runtime_seconds is not None
        else None
    )
    universe = _load_current_stockholm_universe(instruments_path)
    identity_map = _load_stockholm_identity_map(identity_path)
    page_slugs, next_cursor = _build_symbol_page(
        universe,
        max_symbols=query.max_symbols,
        start_after=query.start_after,
        explicit_symbols=query.symbols,
    )

    entries: list[dict[str, Any]] = []
    contract_details_cache: dict[ContractDetailsCacheKey, list[Any]] = {}
    budget_exhausted = False
    last_complete_cursor = query.start_after
    for index, slug in enumerate(page_slugs):
        first_call_timeout = _call_timeout(
            base_timeout=timeout,
            deadline_at=deadline_at,
        )
        if first_call_timeout is None:
            budget_exhausted = True
            break

        identity = identity_map.get(slug)
        entry: dict[str, Any] = {
            "slug": slug,
            "company_name": identity.company_name if identity is not None else None,
            "share_class": identity.share_class if identity is not None else None,
            "ticker_alias": identity.ticker_alias if identity is not None else None,
            "yahoo_symbol": identity.yahoo_symbol if identity is not None else None,
            "isin": identity.isin if identity is not None else None,
            "status": "pending",
            "classification": None,
            "flags": [],
            "resolved_contract": None,
            "series": {},
        }

        requested_series = list(query.what_to_show)
        unsupported_series = [
            series_name
            for series_name in requested_series
            if _series_is_unsupported_for_dated_intraday_backfill(series_name)
        ]
        fetchable_series = [
            series_name
            for series_name in requested_series
            if not _series_is_unsupported_for_dated_intraday_backfill(series_name)
        ]
        for series_name in unsupported_series:
            entry["series"][series_name] = {
                "status": "unsupported",
                "detail": (
                    "IBKR does not support explicit dated intraday requests for "
                    "ADJUSTED_LAST. Request raw intraday bars here and apply "
                    "adjustment factors downstream."
                ),
            }

        if not fetchable_series:
            entry["status"] = "error"
            entry["detail"] = "No supported intraday series were requested."
            entries.append(entry)
            last_complete_cursor = slug
            if query.sleep_seconds > 0:
                runtime_time.sleep(query.sleep_seconds)
            continue

        first_series = fetchable_series[0]
        first_query = _build_historical_query(
            slug,
            identity,
            as_of_date=query.as_of_date,
            bar_size=query.bar_size,
            what_to_show=first_series,
            use_rth=query.use_rth,
        )
        try:
            first_response = read_historical_bars(
                config,
                first_query,
                timeout=first_call_timeout,
                app=app,
                contract_details_cache=contract_details_cache,
            )
        except LookupError as exc:
            entry["status"] = "lookup_error"
            entry["detail"] = str(exc)
            entries.append(entry)
            last_complete_cursor = slug
            if query.sleep_seconds > 0:
                runtime_time.sleep(query.sleep_seconds)
            continue
        except TimeoutError as exc:
            entry["status"] = "timeout"
            entry["detail"] = str(exc)
            entries.append(entry)
            last_complete_cursor = slug
            if query.sleep_seconds > 0:
                runtime_time.sleep(query.sleep_seconds)
            continue
        except Exception as exc:  # pragma: no cover - live ops guard
            entry["status"] = "error"
            entry["detail"] = f"{type(exc).__name__}: {exc}"
            entries.append(entry)
            last_complete_cursor = slug
            if query.sleep_seconds > 0:
                runtime_time.sleep(query.sleep_seconds)
            continue

        classification, flags = _classify_resolution(
            slug,
            identity,
            first_response["resolved_contract"],
        )
        entry["classification"] = classification
        entry["flags"] = flags
        entry["resolved_contract"] = first_response["resolved_contract"]
        if classification == "resolves_suspiciously_remapped" and not query.include_remapped:
            entry["status"] = "skipped_remapped"
            entry["detail"] = "Resolved at IBKR but requires explicit remap approval."
            entries.append(entry)
            last_complete_cursor = slug
            if query.sleep_seconds > 0:
                runtime_time.sleep(query.sleep_seconds)
            continue

        entry["series"][first_series] = {
            "status": "ok",
            "bar_count": first_response["bar_count"],
            "currency": first_response["currency"],
            "bars": first_response["bars"],
        }
        entry["status"] = "ok"

        symbol_completed = True
        for series_index, series_name in enumerate(fetchable_series[1:], start=1):
            series_call_timeout = _call_timeout(
                base_timeout=timeout,
                deadline_at=deadline_at,
            )
            if series_call_timeout is None:
                budget_exhausted = True
                symbol_completed = False
                entry["status"] = "partial"
                entry["detail"] = (
                    "max_runtime_seconds budget exhausted before all requested "
                    "series were collected for this symbol."
                )
                entry["series"][series_name] = {
                    "status": "not_requested",
                    "detail": "max_runtime_seconds budget exhausted.",
                }
                for remaining_series_name in fetchable_series[series_index + 1 :]:
                    entry["series"][remaining_series_name] = {
                        "status": "not_requested",
                        "detail": "max_runtime_seconds budget exhausted.",
                    }
                break

            series_query = _build_historical_query(
                slug,
                identity,
                as_of_date=query.as_of_date,
                bar_size=query.bar_size,
                what_to_show=series_name,
                use_rth=query.use_rth,
            )
            try:
                series_response = read_historical_bars(
                    config,
                    series_query,
                    timeout=series_call_timeout,
                    app=app,
                    contract_details_cache=contract_details_cache,
                )
                entry["series"][series_name] = {
                    "status": "ok",
                    "bar_count": series_response["bar_count"],
                    "currency": series_response["currency"],
                    "bars": series_response["bars"],
                }
            except LookupError as exc:
                entry["series"][series_name] = {
                    "status": "lookup_error",
                    "detail": str(exc),
                }
            except TimeoutError as exc:
                entry["series"][series_name] = {
                    "status": "timeout",
                    "detail": str(exc),
                }
            except Exception as exc:  # pragma: no cover - live ops guard
                entry["series"][series_name] = {
                    "status": "error",
                    "detail": f"{type(exc).__name__}: {exc}",
                }

        entries.append(entry)
        if symbol_completed:
            last_complete_cursor = slug
        else:
            budget_exhausted = True
            break
        if query.sleep_seconds > 0 and index < len(page_slugs) - 1:
            runtime_time.sleep(query.sleep_seconds)

    elapsed_seconds = runtime_time.monotonic() - started_monotonic
    response_next_cursor = next_cursor
    if budget_exhausted:
        response_next_cursor = last_complete_cursor

    return {
        "query": {
            "as_of_date": query.as_of_date.isoformat(),
            "bar_size": query.bar_size,
            "what_to_show": list(query.what_to_show),
            "use_rth": query.use_rth,
            "max_symbols": query.max_symbols,
            "start_after": query.start_after,
            "symbols": list(query.symbols) if query.symbols is not None else None,
            "include_remapped": query.include_remapped,
            "sleep_seconds": query.sleep_seconds,
            "max_runtime_seconds": query.max_runtime_seconds,
        },
        "universe": {
            "stockholm_instruments_path": str(instruments_path),
            "stockholm_identity_path": str(identity_path),
            "current_universe_size": len(universe),
            "page_size": len(page_slugs),
            "next_cursor": response_next_cursor,
            "requested_page_next_cursor": next_cursor,
        },
        "summary": {
            "requested_symbol_count": len(page_slugs),
            "processed_symbol_count": len(entries),
            "ok_count": sum(1 for entry in entries if entry["status"] == "ok"),
            "lookup_error_count": sum(1 for entry in entries if entry["status"] == "lookup_error"),
            "timeout_count": sum(1 for entry in entries if entry["status"] == "timeout"),
            "error_count": sum(1 for entry in entries if entry["status"] == "error"),
            "partial_count": sum(1 for entry in entries if entry["status"] == "partial"),
            "skipped_remapped_count": sum(
                1 for entry in entries if entry["status"] == "skipped_remapped"
            ),
            "unsupported_series_count": sum(
                1
                for entry in entries
                for series in entry["series"].values()
                if series["status"] == "unsupported"
            ),
            "not_requested_series_count": sum(
                1
                for entry in entries
                for series in entry["series"].values()
                if series["status"] == "not_requested"
            ),
            "resolves_cleanly_count": sum(
                1 for entry in entries if entry["classification"] == "resolves_cleanly"
            ),
            "resolves_suspiciously_remapped_count": sum(
                1
                for entry in entries
                if entry["classification"] == "resolves_suspiciously_remapped"
            ),
            "budget_exhausted": budget_exhausted,
            "elapsed_seconds": round(elapsed_seconds, 3),
        },
        "entries": entries,
    }
