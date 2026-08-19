from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from functools import lru_cache
from pathlib import Path
from zoneinfo import ZoneInfo


@dataclass(frozen=True, slots=True)
class SessionCalendarRow:
    session_date: date
    timezone_name: str
    open_time: time
    close_time: time
    session_kind: str
    source_path: str

    def open_at(self) -> datetime:
        return datetime.combine(
            self.session_date,
            self.open_time,
            tzinfo=ZoneInfo(self.timezone_name),
        )

    def close_at(self) -> datetime:
        return datetime.combine(
            self.session_date,
            self.close_time,
            tzinfo=ZoneInfo(self.timezone_name),
        )


@dataclass(frozen=True, slots=True)
class SessionOpenResolution:
    open_at: datetime
    close_at: datetime
    session_kind: str
    timezone_name: str
    source_path: str


@dataclass(frozen=True, slots=True)
class SessionBoundaryResolution:
    boundary_at: datetime
    boundary_kind: str
    session_kind: str
    timezone_name: str
    source_path: str


def _parse_session_row(row: dict[str, str], *, source_path: Path) -> SessionCalendarRow:
    return SessionCalendarRow(
        session_date=date.fromisoformat(row["session_date"]),
        timezone_name=row["timezone"],
        open_time=time.fromisoformat(row["open_time"]),
        close_time=time.fromisoformat(row["close_time"]),
        session_kind=row["session_kind"],
        source_path=str(source_path),
    )


def _load_rows_from_csv(path: Path) -> tuple[SessionCalendarRow, ...]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return tuple(_parse_session_row(row, source_path=path) for row in reader)


def _load_rows_from_parquet(path: Path) -> tuple[SessionCalendarRow, ...]:
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise ValueError(
            f"duckdb is required to read parquet session calendars at {path}"
        ) from exc

    query = """
        SELECT session_date, timezone, open_time, close_time, session_kind
        FROM read_parquet(?)
        ORDER BY session_date
    """
    rows = duckdb.execute(query, [str(path)]).fetchall()
    return tuple(
        SessionCalendarRow(
            session_date=(
                row[0].date()
                if isinstance(row[0], datetime)
                else row[0]
                if isinstance(row[0], date)
                else date.fromisoformat(str(row[0]))
            ),
            timezone_name=str(row[1]),
            open_time=time.fromisoformat(str(row[2])),
            close_time=time.fromisoformat(str(row[3])),
            session_kind=str(row[4]),
            source_path=str(path),
        )
        for row in rows
    )


@lru_cache(maxsize=8)
def load_session_calendar(path: Path) -> tuple[SessionCalendarRow, ...]:
    """Load the calendar file that was resolved for this service.

    Only this file is read. A missing or empty calendar used to fall back to a
    sibling ``.csv`` the q-data catalog never named, and the result was cached
    for the rest of the process; a run then scheduled against a calendar nobody
    published. Stopping here is recoverable, trading on the wrong sessions is not.
    """
    resolved_path = path.resolve()
    if resolved_path.suffix == ".parquet":
        load_rows = _load_rows_from_parquet
    elif resolved_path.suffix == ".csv":
        load_rows = _load_rows_from_csv
    else:
        raise ValueError("Session calendar path must end with .parquet or .csv")

    if not resolved_path.exists():
        raise FileNotFoundError(f"Session calendar not found at {resolved_path}")

    rows = load_rows(resolved_path)
    if not rows:
        raise ValueError(f"Session calendar at {resolved_path} contains no sessions")
    return rows


def find_next_session_open(
    reference_at: datetime,
    *,
    session_calendar_path: Path,
) -> SessionOpenResolution | None:
    if reference_at.tzinfo is None:
        raise ValueError("reference_at must include timezone information")

    reference_at_utc = reference_at.astimezone(timezone.utc)
    for row in load_session_calendar(session_calendar_path):
        open_at = row.open_at()
        if open_at.astimezone(timezone.utc) <= reference_at_utc:
            continue
        return SessionOpenResolution(
            open_at=open_at,
            close_at=row.close_at(),
            session_kind=row.session_kind,
            timezone_name=row.timezone_name,
            source_path=row.source_path,
        )

    return None


def find_session_for_date(
    session_date: date,
    *,
    session_calendar_path: Path,
) -> SessionOpenResolution | None:
    for row in load_session_calendar(session_calendar_path):
        if row.session_date != session_date:
            continue
        return SessionOpenResolution(
            open_at=row.open_at(),
            close_at=row.close_at(),
            session_kind=row.session_kind,
            timezone_name=row.timezone_name,
            source_path=row.source_path,
        )
    return None


def find_matching_session_boundary(
    target_at: datetime,
    *,
    session_calendar_path: Path,
) -> SessionBoundaryResolution | None:
    if target_at.tzinfo is None:
        raise ValueError("target_at must include timezone information")

    target_at_utc = target_at.astimezone(timezone.utc)
    for row in load_session_calendar(session_calendar_path):
        open_at = row.open_at()
        if open_at.astimezone(timezone.utc) == target_at_utc:
            return SessionBoundaryResolution(
                boundary_at=open_at,
                boundary_kind="open",
                session_kind=row.session_kind,
                timezone_name=row.timezone_name,
                source_path=row.source_path,
            )

        close_at = row.close_at()
        if close_at.astimezone(timezone.utc) == target_at_utc:
            return SessionBoundaryResolution(
                boundary_at=close_at,
                boundary_kind="close",
                session_kind=row.session_kind,
                timezone_name=row.timezone_name,
                source_path=row.source_path,
            )

    return None
