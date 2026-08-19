"""Manifest-only resolver for shared q-data inputs."""
from __future__ import annotations

import json
from pathlib import Path


class QDataContractError(RuntimeError):
    pass


def resolve_dataset(catalog_path: Path, dataset_id: str) -> Path:
    try:
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
        relative_path = str(catalog["datasets"][dataset_id]["relative_path"])
    except (OSError, KeyError, json.JSONDecodeError) as exc:
        raise QDataContractError(
            f"Cannot resolve {dataset_id!r} from q-data catalog {catalog_path}"
        ) from exc
    path = catalog_path.parent / relative_path
    if not path.is_file():
        raise QDataContractError(
            f"q-data catalog points {dataset_id!r} to missing file {path}"
        )
    return path
