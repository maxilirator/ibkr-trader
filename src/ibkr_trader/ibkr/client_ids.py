from __future__ import annotations


# Canonical IBKR client ID policy for this repository.
#
# These IDs are fixed and role-based.
# We do not treat "pick a fresh client ID" as the normal recovery path,
# because order visibility and control are scoped by client ID and client 0
# has special order-binding behavior.
PRIMARY_RUNTIME_CLIENT_ID = 0
DIAGNOSTIC_CLIENT_ID = 7
STREAMING_CLIENT_ID = 9
