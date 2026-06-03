from __future__ import annotations

from typing import Any, Mapping


def _parse_string_list(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    required: bool = False,
    normalize: Any | None = None,
) -> tuple[str, ...]:
    raw_value = payload.get(field_name)
    if raw_value is None:
        if required:
            raise ValueError(f"{field_name} is required")
        return ()
    if not isinstance(raw_value, list) or not raw_value:
        raise ValueError(f"{field_name} must be a non-empty array of strings")
    values: list[str] = []
    seen: set[str] = set()
    for item in raw_value:
        value = str(item).strip()
        if normalize is not None:
            value = normalize(value)
        if not value:
            raise ValueError(f"{field_name} must contain only non-empty strings")
        if value in seen:
            continue
        seen.add(value)
        values.append(value)
    return tuple(values)


def _parse_json_object_field(
    payload: Mapping[str, Any],
    field_name: str,
) -> dict[str, Any]:
    raw_value = payload.get(field_name)
    if raw_value is None:
        return {}
    if not isinstance(raw_value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return dict(raw_value)


def _parse_required_string(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    normalize: Any | None = None,
) -> str:
    value = str(payload.get(field_name, "")).strip()
    if normalize is not None:
        value = normalize(value)
    if not value:
        raise ValueError(f"{field_name} is required")
    return value

def _parse_optional_string_list_update(
    payload: Mapping[str, Any],
    field_name: str,
    *,
    normalize: Any | None = None,
) -> tuple[str, ...]:
    raw_value = payload.get(field_name)
    if raw_value is None:
        return ()
    if not isinstance(raw_value, list):
        raise ValueError(f"{field_name} must be an array of strings")
    values: list[str] = []
    seen: set[str] = set()
    for item in raw_value:
        value = str(item).strip()
        if normalize is not None:
            value = normalize(value)
        if not value:
            raise ValueError(f"{field_name} must contain only non-empty strings")
        if value in seen:
            continue
        seen.add(value)
        values.append(value)
    return tuple(values)
