from ibkr_trader.ibkr.historical_bars import NegativeContractCache

KEY = ("ABB", "STK", "SMART", "SEK", "SFB", None, None)
OTHER = ("ZZZ", "STK", "SMART", "SEK", "SFB", None, None)
TTL = 3600.0


def test_single_error_200_is_not_cached():
    """One transient rejection must not suppress an instrument for the TTL.

    2026-07-31 saw 562 error-200s against a ~250 baseline; caching the first
    observation removed ~258 instruments that had resolved the day before.
    """
    cache = NegativeContractCache()
    assert cache.set(KEY, "[200] no security definition", ttl_seconds=TTL) is False
    assert cache.get(KEY) is None


def test_second_consecutive_error_200_is_cached():
    cache = NegativeContractCache()
    cache.set(KEY, "[200] no security definition", ttl_seconds=TTL)
    assert cache.set(KEY, "[200] no security definition", ttl_seconds=TTL) is True
    assert cache.get(KEY) == "[200] no security definition"


def test_previously_resolved_contract_is_never_cached():
    """A contract we have seen resolve cannot be talked out of existing."""
    cache = NegativeContractCache()
    cache.note_resolved(KEY)
    for _ in range(5):
        assert cache.set(KEY, "[200] no security definition", ttl_seconds=TTL) is False
    assert cache.get(KEY) is None


def test_resolution_clears_an_existing_negative_entry():
    cache = NegativeContractCache()
    cache.set(KEY, "[200] x", ttl_seconds=TTL)
    cache.set(KEY, "[200] x", ttl_seconds=TTL)
    assert cache.get(KEY) is not None
    cache.note_resolved(KEY)
    assert cache.get(KEY) is None


def test_strikes_are_per_instrument():
    cache = NegativeContractCache()
    cache.set(KEY, "[200] x", ttl_seconds=TTL)
    assert cache.set(OTHER, "[200] x", ttl_seconds=TTL) is False
    assert cache.get(OTHER) is None
