from __future__ import annotations

from unittest import TestCase

from ibkr_trader.ibkr.client_ids import DIAGNOSTIC_CLIENT_ID
from ibkr_trader.ibkr.client_ids import PRIMARY_RUNTIME_CLIENT_ID
from ibkr_trader.ibkr.client_ids import STREAMING_CLIENT_ID


class ClientIdPolicyTests(TestCase):
    def test_reserved_client_ids_match_canonical_values(self) -> None:
        self.assertEqual(PRIMARY_RUNTIME_CLIENT_ID, 0)
        self.assertEqual(DIAGNOSTIC_CLIENT_ID, 7)
        self.assertEqual(STREAMING_CLIENT_ID, 9)
