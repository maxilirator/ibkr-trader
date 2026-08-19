from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from ibkr_trader.q_data import QDataContractError, resolve_dataset


def _write_catalog(root: Path, entry: dict[str, Any]) -> Path:
    catalog = root / "catalog.json"
    catalog.write_text(
        json.dumps({"datasets": {"xsto.world.calendar": entry}}),
        encoding="utf-8",
    )
    return catalog


def _publish(root: Path, relative: str, payload: bytes = b"calendar") -> str:
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


def test_resolve_dataset_uses_catalog_only(tmp_path: Path) -> None:
    relative = "markets/xsto/datasets/calendar/current/data.parquet"
    content_hash = _publish(tmp_path, relative)
    catalog = _write_catalog(
        tmp_path, {"relative_path": relative, "content_hash": content_hash}
    )

    assert resolve_dataset(catalog, "xsto.world.calendar") == tmp_path / relative


def test_resolve_dataset_rejects_missing_catalog_entry(tmp_path: Path) -> None:
    (tmp_path / "catalog.json").write_text('{"datasets": {}}', encoding="utf-8")
    with pytest.raises(QDataContractError):
        resolve_dataset(tmp_path / "catalog.json", "xsto.world.calendar")


def test_a_dataset_published_without_a_content_hash_is_rejected(tmp_path: Path) -> None:
    """An unverifiable dataset is indistinguishable from a corrupt one."""
    relative = "markets/xsto/datasets/calendar/current/data.parquet"
    _publish(tmp_path, relative)
    catalog = _write_catalog(tmp_path, {"relative_path": relative})

    with pytest.raises(QDataContractError) as caught:
        resolve_dataset(catalog, "xsto.world.calendar")

    assert "content_hash" in str(caught.value)


def test_an_empty_content_hash_is_rejected(tmp_path: Path) -> None:
    relative = "markets/xsto/datasets/calendar/current/data.parquet"
    _publish(tmp_path, relative)
    catalog = _write_catalog(tmp_path, {"relative_path": relative, "content_hash": "  "})

    with pytest.raises(QDataContractError) as caught:
        resolve_dataset(catalog, "xsto.world.calendar")

    assert "content_hash" in str(caught.value)


def test_a_tampered_dataset_is_rejected(tmp_path: Path) -> None:
    relative = "markets/xsto/datasets/calendar/current/data.parquet"
    content_hash = _publish(tmp_path, relative)
    catalog = _write_catalog(
        tmp_path, {"relative_path": relative, "content_hash": content_hash}
    )
    (tmp_path / relative).write_bytes(b"tampered")

    with pytest.raises(QDataContractError) as caught:
        resolve_dataset(catalog, "xsto.world.calendar")

    assert "checksum" in str(caught.value)


def test_an_absolute_relative_path_cannot_leave_the_catalog_directory(
    tmp_path: Path,
) -> None:
    outside = tmp_path / "outside" / "data.parquet"
    content_hash = _publish(tmp_path, "outside/data.parquet")
    catalog_root = tmp_path / "q-data"
    catalog_root.mkdir()
    catalog = _write_catalog(
        catalog_root, {"relative_path": str(outside), "content_hash": content_hash}
    )

    with pytest.raises(QDataContractError) as caught:
        resolve_dataset(catalog, "xsto.world.calendar")

    assert "relative_path" in str(caught.value)


def test_a_parent_directory_component_cannot_leave_the_catalog_directory(
    tmp_path: Path,
) -> None:
    content_hash = _publish(tmp_path, "outside/data.parquet")
    catalog_root = tmp_path / "q-data"
    catalog_root.mkdir()
    catalog = _write_catalog(
        catalog_root,
        {"relative_path": "../outside/data.parquet", "content_hash": content_hash},
    )

    with pytest.raises(QDataContractError) as caught:
        resolve_dataset(catalog, "xsto.world.calendar")

    assert "relative_path" in str(caught.value)


def test_a_catalog_entry_without_a_relative_path_is_rejected(tmp_path: Path) -> None:
    catalog = _write_catalog(tmp_path, {"content_hash": "abc"})

    with pytest.raises(QDataContractError) as caught:
        resolve_dataset(catalog, "xsto.world.calendar")

    assert "relative_path" in str(caught.value)
