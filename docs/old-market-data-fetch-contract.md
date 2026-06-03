# Old Market Data Fetch Contract

This file documents the contract for fetching old market data, meaning
historical/backfill bars rather than the live market stream.

Use the Trader API, not IB Gateway directly:

```bash
export API=http://quant.geisler.se:8000
```

Local development defaults to `http://127.0.0.1:8000`, but local broker-connected
runs must be short diagnostics only. Do not leave a second local API, broker
probe, RL runner, or stream process connected to IB Gateway.

## Operating Rules

- Historical data uses the dedicated historical/backfill IBKR client session.
- The canonical historical client ID is `8`.
- Historical fetches count against the broker pacing governor.
- The repo default pacing budget is `50` historical requests per `10` minutes.
- Use historical fetches for operator diagnostics, controlled backfills, and
  offline data repair.
- Do not poll historical bars every minute for live RL. The normal live path is
  `/v1/market-data/stream/desired` plus `/v1/rl/observations/build` using
  `fetch.mode=market_stream`.

Common errors:

| Status | Meaning |
| --- | --- |
| `400` | Bad payload, ambiguous/no contract, or broker rejected the request. |
| `429` | Historical pacing limit exceeded. |
| `502` | Trader API could not connect to IBKR. |
| `503` | Runtime dependency missing, usually `ibapi`. |
| `504` | IBKR request timed out. |

Error bodies:

```json
{
  "detail": "Timed out while requesting historical bars for SIVE."
}
```

## Single Instrument Historical Bars

### Endpoint

```text
POST /v1/market-data/historical-bars?timeout=20
```

### Request

```json
{
  "symbol": "SIVE",
  "security_type": "STK",
  "exchange": "SMART",
  "primary_exchange": "SFB",
  "currency": "SEK",
  "local_symbol": null,
  "isin": null,
  "duration": "2 D",
  "bar_size": "1 min",
  "what_to_show": "TRADES",
  "use_rth": true,
  "end_at": "2026-04-29T17:30:00+02:00"
}
```

Fields:

| Field | Required | Notes |
| --- | --- | --- |
| `symbol` | yes | Uppercased by the API. |
| `security_type` | no | Defaults to `STK`. |
| `exchange` | yes | For Stockholm stocks use `SMART`. |
| `primary_exchange` | no | For Stockholm stocks use `SFB`. |
| `currency` | yes | For Stockholm stocks use `SEK`. |
| `local_symbol` | no | Optional IBKR local symbol override. |
| `isin` | no | Optional stricter identity match. |
| `duration` | yes | IBKR duration string, for example `1 D`, `2 D`, `30 D`. |
| `bar_size` | yes | IBKR bar size string, for example `1 min`, `5 mins`, `1 day`. |
| `what_to_show` | no | Defaults to `TRADES`. Common values are `TRADES`, `MIDPOINT`, `BID`, `ASK`. |
| `use_rth` | no | Defaults to `true`. |
| `end_at` | no | ISO timestamp with timezone. Omit for IBKR's default end time. |

### Response

```json
{
  "query": {
    "symbol": "SIVE",
    "security_type": "STK",
    "exchange": "SMART",
    "currency": "SEK",
    "primary_exchange": "SFB",
    "isin": null,
    "duration": "2 D",
    "bar_size": "1 min",
    "what_to_show": "TRADES",
    "use_rth": true,
    "end_at": "2026-04-29T17:30:00+02:00"
  },
  "resolved_contract": {
    "con_id": 123456789,
    "symbol": "SIVE",
    "local_symbol": "SIVE",
    "trading_class": "SIVE",
    "security_type": "STK",
    "exchange": "SMART",
    "primary_exchange": "SFB",
    "currency": "SEK",
    "min_tick": "0.01",
    "valid_exchanges": ["SMART", "SFB"],
    "order_types": [],
    "sec_ids": {}
  },
  "bar_count": 1,
  "currency": "SEK",
  "bars": [
    {
      "timestamp": "20260429  09:00:00",
      "open": "100",
      "high": "101",
      "low": "99",
      "close": "100.5",
      "volume": "12345",
      "wap": "100.25",
      "bar_count": "12",
      "currency": "SEK"
    }
  ]
}
```

Decimal values are strings. `timestamp` is the broker-provided historical bar
timestamp string.

### Curl

```bash
curl -fsS -X POST "$API/v1/market-data/historical-bars?timeout=20" \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "SIVE",
    "security_type": "STK",
    "exchange": "SMART",
    "primary_exchange": "SFB",
    "currency": "SEK",
    "duration": "2 D",
    "bar_size": "1 min",
    "what_to_show": "TRADES",
    "use_rth": true,
    "end_at": "2026-04-29T17:30:00+02:00"
  }'
```

### Python

```python
from __future__ import annotations

import os
from typing import Any

import requests


API = os.environ.get("API", "http://quant.geisler.se:8000").rstrip("/")


def post_json(path: str, payload: dict[str, Any], *, timeout: int = 30) -> dict[str, Any]:
    response = requests.post(f"{API}{path}", json=payload, timeout=timeout)
    try:
        body = response.json()
    except ValueError:
        body = {"detail": response.text}
    if not response.ok:
        raise RuntimeError(f"POST {path} failed with HTTP {response.status_code}: {body}")
    return body


def fetch_old_bars(symbol: str, *, end_at: str | None = None) -> list[dict[str, Any]]:
    payload = {
        "symbol": symbol,
        "security_type": "STK",
        "exchange": "SMART",
        "primary_exchange": "SFB",
        "currency": "SEK",
        "duration": "2 D",
        "bar_size": "1 min",
        "what_to_show": "TRADES",
        "use_rth": True,
    }
    if end_at is not None:
        payload["end_at"] = end_at
    result = post_json("/v1/market-data/historical-bars?timeout=20", payload)
    return result["bars"]


if __name__ == "__main__":
    bars = fetch_old_bars("SIVE", end_at="2026-04-29T17:30:00+02:00")
    print(f"received {len(bars)} bars")
```

## Stockholm Paged Intraday Backfill

Use this endpoint when fetching old 1-minute data for many Stockholm symbols.
It pages through the configured Stockholm universe and returns one page per
request.

### Endpoint

```text
POST /v1/market-data/stockholm-intraday-backfill?timeout=20
```

### Request

```json
{
  "as_of_date": "2026-04-29",
  "bar_size": "1 min",
  "what_to_show": ["TRADES"],
  "use_rth": true,
  "max_symbols": 25,
  "start_after": null,
  "symbols": null,
  "include_remapped": false,
  "sleep_seconds": 0.05,
  "max_runtime_seconds": 55
}
```

Fields:

| Field | Required | Notes |
| --- | --- | --- |
| `as_of_date` | yes | Trading date to fetch. |
| `bar_size` | no | Defaults to `1 min`. |
| `what_to_show` | no | Defaults to `TRADES`, `MIDPOINT`, `BID`, `ASK`, `ADJUSTED_LAST`. For dated intraday requests, use `TRADES` unless you are deliberately fetching quote series. |
| `use_rth` | no | Defaults to `true`. |
| `max_symbols` | no | Defaults to `25`, max `100`. |
| `start_after` | no | Cursor from the previous response. |
| `symbols` | no | Explicit symbol slug list. If omitted, pages the configured universe. |
| `include_remapped` | no | Defaults to `false`; suspicious remaps are skipped. |
| `sleep_seconds` | no | Delay between symbols. |
| `max_runtime_seconds` | no | Wall-clock budget for the HTTP response. |

`ADJUSTED_LAST` is reported as unsupported for explicit dated intraday requests.
Fetch raw intraday bars here and apply adjustment factors downstream.

### Response

```json
{
  "accepted": true,
  "session_client_id": 7,
  "market": "stockholm",
  "series_mode": "paged_batch",
  "query": {},
  "universe": {
    "current_universe_size": 955,
    "page_size": 25,
    "next_cursor": "volcar-b",
    "requested_page_next_cursor": "volcar-b"
  },
  "summary": {
    "requested_symbol_count": 25,
    "processed_symbol_count": 25,
    "ok_count": 24,
    "lookup_error_count": 1,
    "timeout_count": 0,
    "error_count": 0,
    "partial_count": 0,
    "skipped_remapped_count": 0,
    "unsupported_series_count": 0,
    "not_requested_series_count": 0,
    "resolves_cleanly_count": 24,
    "resolves_suspiciously_remapped_count": 0,
    "budget_exhausted": false,
    "elapsed_seconds": 18.42
  },
  "entries": [
    {
      "slug": "sive",
      "status": "ok",
      "classification": "resolves_cleanly",
      "flags": [],
      "resolved_contract": {},
      "series": {
        "TRADES": {
          "status": "ok",
          "bar_count": 510,
          "currency": "SEK",
          "bars": []
        }
      }
    }
  ]
}
```

Entry statuses:

```text
ok
lookup_error
timeout
error
partial
skipped_remapped
```

### Paging Loop

```bash
cursor=""
while :; do
  body=$(jq -n \
    --arg date "2026-04-29" \
    --arg cursor "$cursor" \
    '{
      as_of_date: $date,
      bar_size: "1 min",
      what_to_show: ["TRADES"],
      use_rth: true,
      max_symbols: 25,
      sleep_seconds: 0.05,
      max_runtime_seconds: 55
    } + (if $cursor == "" then {} else {start_after: $cursor} end)')

  response=$(curl -fsS -X POST "$API/v1/market-data/stockholm-intraday-backfill" \
    -H "Content-Type: application/json" \
    -d "$body")

  echo "$response" > "stockholm-page-${cursor:-first}.json"
  cursor=$(echo "$response" | jq -r '.universe.next_cursor // ""')
  test -n "$cursor" || break
done
```

## RL Historical Fetch Mode

The RL observation endpoint can still fetch old bars directly when explicitly
requested:

```json
{
  "deployment_key": "long_trial_106_virtual_shared_01",
  "symbols": ["AXFO"],
  "as_of": "2026-04-29T09:07:30+02:00",
  "fetch": {
    "mode": "historical_bars",
    "exchange": "SMART",
    "primary_exchange": "SFB",
    "currency": "SEK",
    "duration": "25 D",
    "bar_size": "1 min",
    "what_to_show": "TRADES",
    "use_rth": true
  }
}
```

This returns:

```json
{
  "accepted": true,
  "source_mode": "ibkr_historical_bars",
  "fetched_symbols": ["AXFO"],
  "streamed_symbols": [],
  "rl_observation": {}
}
```

Use this only for diagnostics and controlled backfills. The production live
runner should use stream bars and enqueue missing-data repair instead of doing a
live historical request each cycle.

## Backfill Repair Semantics

When `/v1/rl/observations/build` uses `fetch.mode=market_stream` and stream bars
are missing, it can enqueue a durable backfill request instead of fetching old
bars inline.

Default behavior:

- `fetch.backfill_missing=true`
- one coalesced request per symbol/trade-date/instrument/bar contract
- status starts as `PENDING`
- background worker drains due requests through the historical session
- persisted bar source is `ibkr_historical_backfill_1m`

Backfill statuses:

```text
PENDING
RUNNING
SUCCEEDED
FAILED_RETRYABLE
FAILED_FINAL
```

This is the safe repair path for live RL coverage gaps.

## Quick Decision Guide

| Need | Use |
| --- | --- |
| One symbol old bars | `POST /v1/market-data/historical-bars` |
| A page of Stockholm old intraday bars | `POST /v1/market-data/stockholm-intraday-backfill` |
| Live model observations | `POST /v1/rl/observations/build` with `fetch.mode=market_stream` |
| Diagnostic model observation from old bars | `POST /v1/rl/observations/build` with `fetch.mode=historical_bars` |
| Repair missing stream coverage | Let observation builder enqueue backfill and let the worker drain it |
