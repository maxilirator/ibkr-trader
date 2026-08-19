"""Shared test environment.

Shared q-data inputs are resolved only through the q-data catalog, so a
catalog is now part of a working environment the same way a database URL is.
Tests that build an :class:`AppConfig` incidentally get a small, hash-correct
catalog here rather than each having to construct one.

``tests/test_config.py`` deliberately clears the environment to assert that a
missing catalog is a hard failure; this fixture does not weaken that, because
those tests set ``clear=True`` themselves.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

CATALOG_DATASETS = (
    "xsto.world.calendar",
    "xsto.world.universe",
    "xsto.world.instrument_identity",
)


def write_catalog(root: Path) -> Path:
    entries: dict[str, dict[str, str]] = {}
    for dataset_id in CATALOG_DATASETS:
        relative = f"markets/xsto/datasets/{dataset_id.replace('.', '_')}/current/data.parquet"
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = dataset_id.encode()
        target.write_bytes(payload)
        entries[dataset_id] = {
            "relative_path": relative,
            "content_hash": hashlib.sha256(payload).hexdigest(),
        }
    catalog = root / "catalog.json"
    catalog.write_text(json.dumps({"contract_version": 1, "datasets": entries}), encoding="utf-8")
    return catalog


@pytest.fixture(autouse=True, scope="session")
def q_data_catalog(tmp_path_factory: pytest.TempPathFactory) -> Path:
    catalog = write_catalog(tmp_path_factory.mktemp("q-data"))
    os.environ.setdefault("Q_DATA_CATALOG_PATH", str(catalog))
    return catalog


@pytest.fixture(autouse=True, scope="session")
def isolated_bootstrap_env(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Keep the test suite away from the real /etc/ibkr-trader/bootstrap.env.

    ``AppConfig.from_env()`` resolves the protected bootstrap path. On the live
    host that file exists and holds production secrets, so without this the
    suite would read them into the test process - and would fail outright on a
    host where /etc/ibkr-trader is not traversable by the test user.

    Tests that need a specific bootstrap file pass ``bootstrap_path`` explicitly;
    those that clear the environment with ``clear=True`` drop this override and
    get the real default path, which is correct because they also assert on
    absence rather than reading a file.
    """
    path = tmp_path_factory.mktemp("bootstrap") / "absent-bootstrap.env"
    os.environ.setdefault("IBKR_TRADER_BOOTSTRAP_ENV", str(path))
    return path
