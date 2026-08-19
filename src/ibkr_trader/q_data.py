"""Manifest-only resolver for shared q-data inputs.

Every shared dataset is resolved by dataset id through ``catalog.json``. There
is deliberately no fallback to a guessed directory path: an agent once built a
whole training pipeline on a stale file it found by listing a directory, and
the fix is that a consumer which cannot resolve the catalog stops rather than
reading something plausible.

Resolution also verifies the published content hash. The catalog records it,
and checking it here turns a truncated or half-written file into an error at
read time instead of a strange result much later.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

CATALOG_ENV = "Q_DATA_CATALOG_PATH"


class QDataContractError(RuntimeError):
    pass


def catalog_path() -> Path:
    """The configured catalog, or an error naming what to set."""
    configured = os.getenv(CATALOG_ENV, "").strip()
    if not configured:
        raise QDataContractError(
            f"{CATALOG_ENV} is not set. Shared q-data inputs are resolved only through "
            "the q-data catalog; there is no directory fallback, because inferring "
            "authority from a path is how a stale artefact gets read as current."
        )
    path = Path(configured)
    if not path.is_file():
        raise QDataContractError(f"{CATALOG_ENV} points to a missing catalog: {path}")
    return path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _entry(catalog: Path, dataset_id: str) -> dict[str, Any]:
    try:
        payload = json.loads(catalog.read_text(encoding="utf-8"))
        return dict(payload["datasets"][dataset_id])
    except (OSError, KeyError, json.JSONDecodeError) as exc:
        raise QDataContractError(
            f"Cannot resolve {dataset_id!r} from q-data catalog {catalog}"
        ) from exc


def resolve_dataset(catalog: Path, dataset_id: str, *, verify_hash: bool = True) -> Path:
    """Return the published file for ``dataset_id``, verifying its content hash."""
    entry = _entry(catalog, dataset_id)
    path = catalog.parent / str(entry["relative_path"])
    if not path.is_file():
        raise QDataContractError(f"q-data catalog points {dataset_id!r} to missing file {path}")
    expected = str(entry.get("content_hash", ""))
    if verify_hash and expected and _sha256(path) != expected:
        raise QDataContractError(
            f"q-data dataset {dataset_id!r} does not match its published checksum: {path}"
        )
    return path


def resolve(dataset_id: str, *, verify_hash: bool = True) -> Path:
    """Resolve ``dataset_id`` through the configured catalog."""
    return resolve_dataset(catalog_path(), dataset_id, verify_hash=verify_hash)


def dataset_metadata(dataset_id: str) -> dict[str, Any]:
    """Return the catalog entry so callers can report version and observed span."""
    return _entry(catalog_path(), dataset_id)
