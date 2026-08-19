from __future__ import annotations

import json
from pathlib import Path

import pytest

from ibkr_trader.q_data import QDataContractError, resolve_dataset


def test_resolve_dataset_uses_catalog_only(tmp_path: Path) -> None:
    data = tmp_path / "markets/xsto/datasets/calendar/current/data.parquet"
    data.parent.mkdir(parents=True)
    data.touch()
    (tmp_path / "catalog.json").write_text(
        json.dumps({"datasets": {"xsto.world.calendar": {"relative_path": str(data.relative_to(tmp_path))}}}),
        encoding="utf-8",
    )

    assert resolve_dataset(tmp_path / "catalog.json", "xsto.world.calendar") == data


def test_resolve_dataset_rejects_missing_catalog_entry(tmp_path: Path) -> None:
    (tmp_path / "catalog.json").write_text('{"datasets": {}}', encoding="utf-8")
    with pytest.raises(QDataContractError):
        resolve_dataset(tmp_path / "catalog.json", "xsto.world.calendar")
