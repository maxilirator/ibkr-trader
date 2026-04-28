from __future__ import annotations

from decimal import Decimal


BROKER_KIND_VIRTUAL = "VIRTUAL"
VIRTUAL_ACCOUNT_PREFIX = "VIRTUAL"
VIRTUAL_FIXED_COMMISSION_SEK = Decimal("49")


def normalize_virtual_account_key(account_key: str) -> str:
    normalized = str(account_key or "").strip().upper()
    if not normalized:
        raise ValueError("virtual account_key is required")
    if not normalized.startswith(VIRTUAL_ACCOUNT_PREFIX):
        raise ValueError("virtual account_key must start with VIRTUAL")
    return normalized


def is_virtual_account_key(account_key: str | None) -> bool:
    if account_key is None:
        return False
    return str(account_key).strip().upper().startswith(VIRTUAL_ACCOUNT_PREFIX)
