from __future__ import annotations

from ibkr_trader.ibkr.shortability_common import *

def load_stockholm_symbols_from_instruments_file(
    instruments_path: Path,
    *,
    as_of_date: date | None = None,
    max_symbols: int | None = None,
    today: date | None = None,
) -> tuple[tuple[str, ...], date]:
    if not instruments_path.exists():
        raise FileNotFoundError(
            f"Stockholm instruments path was not found: {instruments_path}"
        )
    if not instruments_path.is_file():
        raise ValueError(f"Stockholm instruments path is not a file: {instruments_path}")

    listed_instruments: list[StockholmListedInstrument] = []
    for line_number, raw_line in enumerate(
        instruments_path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        line = raw_line.strip()
        if not line:
            continue

        parts = line.split("\t")
        if len(parts) != 3:
            raise ValueError(
                f"Invalid Stockholm instruments row at line {line_number}: {raw_line!r}"
            )
        symbol, listed_from_raw, listed_to_raw = parts
        try:
            listed_from = date.fromisoformat(listed_from_raw)
            listed_to = date.fromisoformat(listed_to_raw)
        except ValueError as exc:
            raise ValueError(
                f"Invalid Stockholm instruments date at line {line_number}: {raw_line!r}"
            ) from exc

        listed_instruments.append(
            StockholmListedInstrument(
                symbol=_normalize_symbol(symbol),
                listed_from=listed_from,
                listed_to=listed_to,
            )
        )

    if not listed_instruments:
        raise ValueError(f"No listed instruments were found in {instruments_path}")

    effective_as_of_date = as_of_date
    if effective_as_of_date is None:
        reference_today = today or date.today()
        eligible_listed_to_dates = [
            item.listed_to
            for item in listed_instruments
            if item.listed_to <= reference_today
        ]
        effective_as_of_date = (
            max(eligible_listed_to_dates)
            if eligible_listed_to_dates
            else max(item.listed_to for item in listed_instruments)
        )
    symbols = sorted(
        item.symbol
        for item in listed_instruments
        if item.listed_from <= effective_as_of_date <= item.listed_to
    )

    if not symbols:
        raise ValueError(
            "No listed Stockholm instruments were found for "
            f"{effective_as_of_date.isoformat()} in {instruments_path}"
        )

    if max_symbols is not None:
        symbols = symbols[:max_symbols]

    return tuple(symbols), effective_as_of_date


def load_stockholm_identity_map(
    identity_path: Path,
    *,
    symbols: tuple[str, ...] | None = None,
) -> dict[str, StockholmInstrumentIdentity]:
    if not identity_path.exists():
        return {}
    if not identity_path.is_file():
        raise ValueError(f"Stockholm identity path is not a file: {identity_path}")

    pd = _load_stockholm_identity_runtime()
    frame = pd.read_parquet(
        identity_path,
        columns=["instrument", "isin", "ticker_alias", "yahoo_symbol"],
    )
    if symbols is not None:
        normalized_symbols = {_normalize_symbol(symbol) for symbol in symbols}
        frame = frame[frame["instrument"].str.upper().isin(normalized_symbols)]

    identity_map: dict[str, StockholmInstrumentIdentity] = {}
    for row in frame.itertuples(index=False):
        symbol = _normalize_symbol(row.instrument)
        identity_map[symbol] = StockholmInstrumentIdentity(
            symbol=symbol,
            isin=(str(row.isin).strip() or None),
            ticker_alias=(str(row.ticker_alias).strip() or None),
            yahoo_symbol=(str(row.yahoo_symbol).strip() or None),
        )
    return identity_map


def serialize_shortability_snapshot(snapshot: ShortabilitySnapshot) -> dict[str, Any]:
    payload = asdict(snapshot)
    payload["snapshot_at"] = snapshot.snapshot_at.isoformat()
    payload["universe_as_of_date"] = (
        snapshot.universe_as_of_date.isoformat()
        if snapshot.universe_as_of_date is not None
        else None
    )
    payload["entries"] = [
        {
            **entry,
            "status": entry["status"].value,
            "shortable_value": (
                str(entry["shortable_value"])
                if entry["shortable_value"] is not None
                else None
            ),
            "shortable_shares": (
                str(entry["shortable_shares"])
                if entry["shortable_shares"] is not None
                else None
            ),
        }
        for entry in payload["entries"]
    ]
    if snapshot.only_shortable:
        payload["evaluated_entries"] = [
            {
                **entry,
                "status": entry["status"].value,
                "shortable_value": (
                    str(entry["shortable_value"])
                    if entry["shortable_value"] is not None
                    else None
                ),
                "shortable_shares": (
                    str(entry["shortable_shares"])
                    if entry["shortable_shares"] is not None
                    else None
                ),
            }
            for entry in payload["evaluated_entries"]
        ]
    else:
        payload.pop("evaluated_entries", None)
    return payload


def shortability_meta_dir(output_root: Path) -> Path:
    """Where this service keeps the shortability snapshots it produces.

    Derived from ``AppConfig.output_root`` and never from a resolved q-data
    path: shared datasets are owned by q-data. Every writer and the short-sale
    reader go through this one function, because when they disagreed a refresh
    stopped updating the snapshot that blocks short orders.
    """
    return output_root / "shortability"


def latest_shortability_snapshot_path(output_root: Path) -> Path:
    """The snapshot short-sale validation reads before letting a short through."""
    return shortability_meta_dir(output_root) / "shortability_latest.json"


def _write_symbol_list(path: Path, symbols: tuple[str, ...]) -> None:
    content = "".join(f"{symbol.lower()}\n" for symbol in symbols)
    path.write_text(content, encoding="utf-8")


def persist_shortability_snapshot(
    snapshot_payload: dict[str, Any],
    *,
    instruments_dir: Path,
    meta_dir: Path,
) -> dict[str, Any]:
    instruments_dir.mkdir(parents=True, exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    as_of_date = snapshot_payload.get("universe_as_of_date") or str(
        snapshot_payload["snapshot_at"]
    )[:10]
    entries = tuple(
        snapshot_payload.get("evaluated_entries")
        or snapshot_payload.get("entries", ())
    )
    shortable_symbols = tuple(
        sorted(entry["symbol"] for entry in entries if entry["status"] == "shortable")
    )
    shortable_or_locate_symbols = tuple(
        sorted(
            entry["symbol"]
            for entry in entries
            if entry["status"] in {"shortable", "locate_required"}
        )
    )

    shortable_path = instruments_dir / "shortable.txt"
    shortable_or_locate_path = instruments_dir / "shortable_or_locate.txt"
    snapshot_path = meta_dir / f"shortability_snapshot_{as_of_date}.json"
    latest_snapshot_path = meta_dir / "shortability_latest.json"

    _write_symbol_list(shortable_path, shortable_symbols)
    _write_symbol_list(shortable_or_locate_path, shortable_or_locate_symbols)
    serialized_snapshot = json.dumps(snapshot_payload, indent=2, sort_keys=True) + "\n"
    snapshot_path.write_text(serialized_snapshot, encoding="utf-8")
    latest_snapshot_path.write_text(serialized_snapshot, encoding="utf-8")

    return asdict(
        ShortabilityPersistenceResult(
            as_of_date=as_of_date,
            shortable_count=len(shortable_symbols),
            shortable_or_locate_count=len(shortable_or_locate_symbols),
            shortable_path=str(shortable_path),
            shortable_or_locate_path=str(shortable_or_locate_path),
            snapshot_path=str(snapshot_path),
            latest_snapshot_path=str(latest_snapshot_path),
        )
    )

__all__ = [name for name in globals() if not name.startswith("__")]
