from __future__ import annotations


class IbkrDependencyError(RuntimeError):
    """Raised when the official IBKR Python client is unavailable."""
