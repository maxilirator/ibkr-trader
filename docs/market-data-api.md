# Market Data API Contract

This document describes the HTTP contract for market-data consumers in this
repo. It is based on the FastAPI routes in `src/ibkr_trader/api/server.py` and
the market-data adapters under `src/ibkr_trader/ibkr/`.

The live operator stack runs on `quant`. Use the Trader API, not IB Gateway
directly:

```bash
export API=http://quant.geisler.se:8000
```

For local development the server default is `http://127.0.0.1:8000`, but do not
leave a local API, broker probe, stream process, or RL runner connected to IB
Gateway after a diagnostic run.

## API Shape

- Transport: HTTP JSON.
- Authentication: no application auth is implemented in this repo yet; the
  intended boundary is a trusted LAN plus host/network controls.
- Decimal values: returned as strings, for example `"100.25"`.
- Timestamps: ISO 8601 where this service creates timestamps. Some IBKR
  historical bar timestamps are the broker-provided string.
- Default runtime timezone: `Europe/Stockholm`.
- Stockholm stock defaults: `security_type=STK`, `exchange=SMART`,
  `primary_exchange=SFB`, `currency=SEK`.
- Live market data and execution prices stay in the instrument native currency.

Common status responses:

| Status | Meaning |
| --- | --- |
| `200` | Request accepted and completed. |
| `400` | Invalid request payload, duplicate symbols, unsupported enum, contract lookup failure, or broker rejection that is safe to report as a request problem. |
| `429` | Broker pacing governor rejected the request. |
| `502` | Could not connect to IBKR through the configured API session. |
| `503` | Required runtime dependency is missing, such as the official `ibapi` package. |
| `504` | Broker request timed out. |

Error bodies use FastAPI's default shape:

```json
{
  "detail": "symbols must be a non-empty array of strings"
}
```

## Common Contract Fields

Most request bodies use this instrument shape:

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `symbol` | string | yes | Normalized to uppercase. For Stockholm share classes prefer canonical dash symbols such as `ERIC-B`; the stream endpoints can enrich them from identity metadata. |
| `symbols` | string array | endpoint-specific | Normalized to uppercase and deduplicated or rejected depending on endpoint. |
| `security_type` | string | no | Defaults to `STK`. |
| `exchange` | string | yes for one-shot endpoints, no for stream defaults | Use `SMART` for normal Stockholm routing through this API. |
| `primary_exchange` | string or null | no | Stockholm default is `SFB`. |
| `currency` | string | yes for one-shot endpoints, no for stream defaults | Stockholm default is `SEK`. |
| `local_symbol` | string or null | no | Optional broker local symbol override. |
| `isin` | string or null | no | Optional ISIN used for stricter Stockholm contract resolution. |

Market data type enum:

```text
LIVE
FROZEN
DELAYED
DELAYED_FROZEN
```

Tick-by-tick stream type enum:

```text
Last
AllLast
BidAsk
MidPoint
```

The parser accepts case-insensitive values and common separators, for example
`delayed_frozen`, `delayed-frozen`, or `DELAYED FROZEN`.

## Health Check

### `GET /healthz?refresh_broker_status=false`

Read process, broker-monitor, runtime, and cached broker status. Use
`refresh_broker_status=false` for a passive read that does not request broker
work.

```bash
curl -sS "$API/healthz?refresh_broker_status=false"
```

## Live Trading Stream Readiness

Live trading decisions must use the API-owned market stream as their market-data
truth. Historical bars are for diagnostics, backfills, and offline repair; they
must not be the live decision source for RL or scheduled policy entries.

Before the execution runtime submits a due entry order, it now verifies that the
target instrument is committed to the market stream and that the stream snapshot
has fresh symbol evidence. The readiness gate:

- subscribes the target instrument additively with `replace=false`, so an entry
  target cannot erase the RL desired universe;
- requires an active stream subscription for the target symbol;
- requires a fresh quote or 1-minute stream bar within
  `MARKET_STREAM_STALE_AFTER_SECONDS`;
- persists `entry_market_data_ready` with the trimmed quote/bar evidence before
  broker submission;
- persists `entry_submit_blocked_market_data_not_ready` and leaves the
  instruction in `ENTRY_PENDING` when the stream is missing, stopped, errored, or
  stale.

Broker callbacks, open orders, executions, and positions remain the execution
truth. The stream gate only answers whether the trading decision has current
market data; it does not replace broker reconciliation.

## Historical Bars

### `POST /v1/market-data/historical-bars?timeout=20`

Runs one read-only IBKR historical bar request through the historical/backfill
client session. The canonical client ID for this session is `8`.

Request body:

| Field | Type | Required | Default |
| --- | --- | --- | --- |
| `symbol` | string | yes | none |
| `security_type` | string | no | `STK` |
| `exchange` | string | yes | none |
| `currency` | string | yes | none |
| `primary_exchange` | string or null | no | `null` |
| `local_symbol` | string or null | no | `null` |
| `isin` | string or null | no | `null` |
| `duration` | string | yes | none |
| `bar_size` | string | yes | none |
| `what_to_show` | string | no | `TRADES` |
| `use_rth` | boolean | no | `true` |
| `end_at` | ISO timestamp or null | no | current broker default when omitted |

Example:

```bash
curl -sS -X POST "$API/v1/market-data/historical-bars?timeout=20" \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "SIVE",
    "security_type": "STK",
    "exchange": "SMART",
    "currency": "SEK",
    "primary_exchange": "SFB",
    "duration": "2 D",
    "bar_size": "5 mins",
    "what_to_show": "TRADES",
    "use_rth": true
  }'
```

Response shape:

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
    "bar_size": "5 mins",
    "what_to_show": "TRADES",
    "use_rth": true,
    "end_at": null
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
    "sec_ids": {
      "ISIN": "SE0003917798"
    }
  },
  "bar_count": 2,
  "currency": "SEK",
  "bars": [
    {
      "timestamp": "20260428  09:00:00",
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

## Stockholm Intraday Backfill

### `POST /v1/market-data/stockholm-intraday-backfill?timeout=20`

Collects one page of Stockholm intraday bars from the configured Stockholm
universe. This endpoint is a collector; it returns bars and paging cursors. It
does not persist bars in this repo.

Request body:

| Field | Type | Required | Default |
| --- | --- | --- | --- |
| `as_of_date` | `YYYY-MM-DD` | yes | none |
| `bar_size` | string | no | `1 min` |
| `what_to_show` | string array | no | `["TRADES", "MIDPOINT", "BID", "ASK", "ADJUSTED_LAST"]` |
| `use_rth` | boolean | no | `true` |
| `max_symbols` | integer | no | `25`, max `100` |
| `start_after` | string or null | no | `null` |
| `symbols` | string array or null | no | page from configured universe |
| `include_remapped` | boolean | no | `false` |
| `sleep_seconds` | number | no | `0.05` |
| `max_runtime_seconds` | number or null | no | `55`, max `3600` |

`ADJUSTED_LAST` is marked unsupported for explicit dated intraday requests and
should be applied downstream from another adjustment source.

Example:

```bash
curl -sS -X POST "$API/v1/market-data/stockholm-intraday-backfill" \
  -H "Content-Type: application/json" \
  -d '{
    "as_of_date": "2026-04-24",
    "bar_size": "1 min",
    "what_to_show": ["TRADES"],
    "use_rth": true,
    "max_symbols": 25,
    "sleep_seconds": 0.05,
    "max_runtime_seconds": 55
  }'
```

Response top-level fields:

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
  "entries": []
}
```

Entry statuses include `ok`, `lookup_error`, `timeout`, `error`, `partial`, and
`skipped_remapped`. Resolution classifications include `resolves_cleanly` and
`resolves_suspiciously_remapped`.

## Tick Stream Sample

### `POST /v1/market-data/tick-stream-sample?timeout=15`

Collects a short timed sample from IBKR tick-by-tick streams through the
dedicated streaming client session. The canonical client ID for this session is
`9`.

This endpoint is a diagnostic sample, not the production stream relay.

Request body:

| Field | Type | Required | Default |
| --- | --- | --- | --- |
| `symbol` | string | yes | none |
| `security_type` | string | no | `STK` |
| `exchange` | string | yes | none |
| `currency` | string | yes | none |
| `primary_exchange` | string or null | no | `null` |
| `local_symbol` | string or null | no | `null` |
| `isin` | string or null | no | `null` |
| `tick_types` | string array | no | `["Last", "BidAsk"]` |
| `duration_seconds` | number | no | `5.0`, max `60` |
| `max_events` | integer | no | `500` |
| `ignore_size` | boolean | no | `false` |

Example:

```bash
curl -sS -X POST "$API/v1/market-data/tick-stream-sample?timeout=15" \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "SIVE",
    "exchange": "SMART",
    "currency": "SEK",
    "primary_exchange": "SFB",
    "tick_types": ["Last", "BidAsk"],
    "duration_seconds": 3,
    "max_events": 50
  }'
```

Response shape:

```json
{
  "query": {
    "symbol": "SIVE",
    "exchange": "SMART",
    "currency": "SEK",
    "security_type": "STK",
    "primary_exchange": "SFB",
    "local_symbol": null,
    "isin": null,
    "tick_types": ["Last", "BidAsk"],
    "duration_seconds": 3.0,
    "max_events": 50,
    "ignore_size": false
  },
  "resolved_contract": {},
  "stream_window": {
    "started_at": "2026-04-28T07:00:00+00:00",
    "ended_at": "2026-04-28T07:00:03+00:00",
    "duration_seconds": 3.0,
    "max_events": 50
  },
  "event_count": 1,
  "events": [
    {
      "stream": "Last",
      "timestamp": "2026-04-28T07:00:01+00:00",
      "tick_type": 1,
      "price": "100.25",
      "size": "100",
      "exchange": "SFB",
      "special_conditions": null,
      "past_limit": false,
      "unreported": false
    }
  ],
  "errors": []
}
```

## Persistent Market Stream

The persistent market stream is the normal market-data path for RL and operator
views. One API-owned streaming client owns IBKR `reqMktData` subscriptions,
keeps top-of-book and last-price state, and builds in-memory 1-minute bars from
live last-price ticks.

The stream service also tracks desired subscriptions. A background supervisor
can reconnect and restore desired subscriptions when enabled.

### `POST /v1/market-data/stream/desired`

Writes the desired subscription set. This is the preferred endpoint for
automated runners. It does not have to open the broker socket synchronously; the
stream owner applies the desired set.

Request body can be symbol-based:

```json
{
  "symbols": ["AXFO", "AZN", "TELIA"],
  "exchange": "SMART",
  "primary_exchange": "SFB",
  "currency": "SEK",
  "security_type": "STK",
  "market_data_type": "LIVE",
  "replace": true
}
```

Or contract-based:

```json
{
  "contracts": [
    {
      "symbol": "ERIC-B",
      "exchange": "SMART",
      "primary_exchange": "SFB",
      "currency": "SEK",
      "security_type": "STK",
      "local_symbol": "ERIC B",
      "isin": "SE0000108656"
    }
  ],
  "market_data_type": "LIVE",
  "replace": true
}
```

`contracts` may also be sent as `instruments`. `replace` defaults to `true`.
When `replace=false`, the requested instruments are added to the existing
desired set.

Response shape:

```json
{
  "accepted": true,
  "mode": "streaming_market_data_desired",
  "session_client_id": 9,
  "stream": {
    "running": false,
    "desired_subscription_count": 3,
    "desired_symbols": ["AXFO", "AZN", "TELIA"],
    "subscribed_count": 0,
    "subscriptions": [],
    "quote_count": 0,
    "quotes": [],
    "bars_by_symbol": {},
    "errors": []
  }
}
```

### `POST /v1/market-data/stream/subscribe`

Applies the requested subscriptions immediately through the streaming client.
Use this for operator/API-owner diagnostics. Automated runners should normally
use `/desired`.

Request body is the same as `/desired`.

Response shape:

```json
{
  "accepted": true,
  "mode": "streaming_market_data",
  "session_client_id": 9,
  "stream": {}
}
```

### `GET /v1/market-data/stream/snapshot?symbols=AXFO,AZN&bar_limit=390`

Returns current stream state without opening a new broker request. This endpoint
also persists the current in-memory bars into the local market-stream bar table
and merges persisted bars into the returned `bars_by_symbol`.

Query params:

| Field | Type | Required | Default | Limit |
| --- | --- | --- | --- | --- |
| `symbols` | comma-separated string | no | all active symbols | none |
| `bar_limit` | integer | no | `390` | max `2000` |

Example:

```bash
curl -sS "$API/v1/market-data/stream/snapshot?symbols=AXFO,AZN&bar_limit=20"
```

Stream snapshot fields:

| Field | Notes |
| --- | --- |
| `running` | Whether the streaming client is currently connected. |
| `started_at` | Last stream-client start timestamp. |
| `last_error` | Last stream-level error string. |
| `consecutive_failures` | Consecutive connection failure count. |
| `cooldown_until` | Backoff timestamp if reconnect is cooling down. |
| `latest_market_data_at` | Latest quote or trade update timestamp. |
| `latest_market_data_age_seconds` | Age of latest market data. |
| `is_stale` | Whether connected stream data exceeded the stale threshold. |
| `desired_subscription_count` | Number of desired subscriptions. |
| `desired_symbols` | Desired subscription symbol keys. |
| `subscribed_count` | Number of active IBKR subscriptions. |
| `subscriptions` | Per-symbol IBKR request IDs and contract metadata. |
| `quotes` | Latest top-of-book, last, close, and size values. |
| `bars_by_symbol` | 1-minute OHLC bars keyed by symbol. |
| `persistent_bar_store` | Insert/update/read details from the local bar store. |
| `errors` | Recent broker stream errors. |

Quote shape:

```json
{
  "symbol": "AXFO",
  "exchange": "SMART",
  "currency": "SEK",
  "security_type": "STK",
  "primary_exchange": "SFB",
  "bid_price": "100.00",
  "ask_price": "100.10",
  "last_price": "100.05",
  "close_price": "99.90",
  "bid_size": "100",
  "ask_size": "200",
  "last_size": "50",
  "updated_at": "2026-04-28T07:01:00+00:00",
  "last_trade_at": "2026-04-28T07:01:00+00:00",
  "market_data_type": 1
}
```

Bar shape:

```json
{
  "timestamp": "2026-04-28T07:01:00+00:00",
  "open": "100.00",
  "high": "100.10",
  "low": "99.95",
  "close": "100.05",
  "volume": null,
  "bar_count": "4",
  "currency": "SEK",
  "source": "ibkr_live_market_stream_1m"
}
```

### `POST /v1/market-data/stream/stop`

Stops the stream service, cancels active subscriptions, and clears desired
subscriptions by default. Treat this as an operator action.

```bash
curl -sS -X POST "$API/v1/market-data/stream/stop"
```

Response:

```json
{
  "accepted": true,
  "mode": "streaming_market_data_stopped"
}
```

## Shortability Snapshot

### `POST /v1/market-data/shortability-snapshot?timeout=120`

Collects a Stockholm shortability snapshot. The default source is
`OFFICIAL_IBKR_PAGE`, which does not use the streaming client. The diagnostic
`BROKER_TICKS` source uses the streaming client session and generic tick `236`.

Request body:

| Field | Type | Required | Default |
| --- | --- | --- | --- |
| `symbols` | string array or null | no | configured Stockholm universe |
| `as_of_date` | `YYYY-MM-DD` or null | no | latest configured universe date |
| `exchange` | string | no | `SMART` |
| `primary_exchange` | string | no | `SFB` |
| `currency` | string | no | `SEK` |
| `security_type` | string | no | `STK` |
| `source` | string | no | `OFFICIAL_IBKR_PAGE` |
| `only_shortable` | boolean | no | `true` |
| `market_data_type` | enum | no | `LIVE` |
| `per_symbol_timeout_seconds` | number | no | `2.0` |
| `max_concurrent` | integer | no | `25` |
| `max_symbols` | integer or null | no | no explicit limit |
| `persist` | boolean | no | full-universe requests persist by default |

Example:

```bash
curl -sS -X POST "$API/v1/market-data/shortability-snapshot" \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["ABB", "SIVE", "VOLV-B"],
    "only_shortable": false,
    "source": "OFFICIAL_IBKR_PAGE",
    "persist": false
  }'
```

Response shape:

```json
{
  "accepted": true,
  "session_client_id": null,
  "stockholm_instruments_path": "../q-data/xsto/instruments/all.txt",
  "persisted_artifacts": null,
  "shortability_snapshot": {
    "source": "OFFICIAL_IBKR_PAGE",
    "source_url": "https://...",
    "source_updated_text": "...",
    "snapshot_at": "2026-04-28T07:00:00+00:00",
    "universe_as_of_date": "2026-04-28",
    "status_counts": {
      "shortable": 2,
      "not_shortable": 1
    },
    "entries": []
  }
}
```

Status vocabulary:

```text
shortable
not_shortable
locate_required
not_found
timeout
error
unknown_status
```

`locate_required`, `not_found`, `timeout`, `error`, and `unknown_status` remain
possible for the `BROKER_TICKS` diagnostic path.

## RL Observation Consumer

### `POST /v1/rl/observations/build?timeout=20`

This is not a raw market-data endpoint, but it is the model-facing consumer of
the market-data stream. If `source_bars` is omitted, it defaults to
`fetch.mode=market_stream`, reads `/stream/snapshot` internally, and builds
phase-1 5-minute observations from 1-minute stream bars.

Minimal request:

```json
{
  "deployment_key": "long_trial_106_virtual_shared_01",
  "symbols": ["AXFO"],
  "as_of": "2026-04-28T09:07:30+02:00",
  "fetch": {
    "mode": "market_stream",
    "bar_limit": 390,
    "backfill_missing": true
  },
  "history_overrides": {
    "AXFO": {
      "prev_close": "100",
      "history_features": {
        "prev_open_rel_close": 0.0,
        "prev_high_rel_close": 0.02,
        "prev_low_rel_close": -0.02,
        "prev_close_rel_open": 0.0,
        "prev_high_rel_low": 0.04,
        "trailing_intraday_realized_vol": 0.01,
        "trailing_session_count_norm": 1.0
      }
    }
  }
}
```

Response top-level fields:

```json
{
  "accepted": true,
  "source_mode": "market_stream",
  "fetched_symbols": [],
  "streamed_symbols": ["AXFO"],
  "backfill_request_count": 0,
  "backfill_requests": [],
  "account_key": "VIRTUALRL01",
  "book_key": "rl_shared_long_trial_106_virtual_01",
  "mode": "virtual",
  "rl_observation": {}
}
```

If stream bars are missing and `backfill_missing=true`, the endpoint enqueues a
market-data backfill request and returns a paused observation with
`model_decision.ready=false`.

## Client Code

### Bash

```bash
#!/usr/bin/env bash
set -euo pipefail

API="${API:-http://quant.geisler.se:8000}"

curl -fsS "$API/healthz?refresh_broker_status=false" >/dev/null

curl -fsS -X POST "$API/v1/market-data/stream/desired" \
  -H "Content-Type: application/json" \
  -d '{
    "symbols": ["AXFO", "AZN"],
    "exchange": "SMART",
    "primary_exchange": "SFB",
    "currency": "SEK",
    "market_data_type": "LIVE",
    "replace": true
  }'

curl -fsS "$API/v1/market-data/stream/snapshot?symbols=AXFO,AZN&bar_limit=20"
```

### Python

```python
from __future__ import annotations

import os
import time
from typing import Any

import requests


API = os.environ.get("API", "http://quant.geisler.se:8000").rstrip("/")


def request_json(method: str, path: str, **kwargs: Any) -> dict[str, Any]:
    response = requests.request(method, f"{API}{path}", timeout=kwargs.pop("timeout", 20), **kwargs)
    try:
        body = response.json()
    except ValueError:
        body = {"detail": response.text}
    if not response.ok:
        raise RuntimeError(f"{method} {path} failed with HTTP {response.status_code}: {body}")
    return body


def set_desired_stream(symbols: list[str]) -> dict[str, Any]:
    return request_json(
        "POST",
        "/v1/market-data/stream/desired",
        json={
            "symbols": symbols,
            "exchange": "SMART",
            "primary_exchange": "SFB",
            "currency": "SEK",
            "market_data_type": "LIVE",
            "replace": True,
        },
    )


def read_stream_snapshot(symbols: list[str], bar_limit: int = 20) -> dict[str, Any]:
    joined = ",".join(symbols)
    return request_json(
        "GET",
        f"/v1/market-data/stream/snapshot?symbols={joined}&bar_limit={bar_limit}",
    )


def read_historical_bars(symbol: str) -> dict[str, Any]:
    return request_json(
        "POST",
        "/v1/market-data/historical-bars",
        json={
            "symbol": symbol,
            "security_type": "STK",
            "exchange": "SMART",
            "primary_exchange": "SFB",
            "currency": "SEK",
            "duration": "2 D",
            "bar_size": "1 min",
            "what_to_show": "TRADES",
            "use_rth": True,
        },
        timeout=30,
    )


if __name__ == "__main__":
    symbols = ["AXFO", "AZN"]
    desired = set_desired_stream(symbols)
    print("desired:", desired["stream"].get("desired_symbols"))

    for _ in range(5):
        snapshot = read_stream_snapshot(symbols)
        stream = snapshot["stream"]
        print("running:", stream.get("running"), "quotes:", stream.get("quote_count"))
        print("bars:", {symbol: len(bars) for symbol, bars in stream.get("bars_by_symbol", {}).items()})
        time.sleep(1)
```

### Node.js

```js
const API = (process.env.API ?? 'http://quant.geisler.se:8000').replace(/\/+$/, '');

async function requestJson(path, init = {}) {
  const response = await fetch(`${API}${path}`, {
    ...init,
    headers: {
      accept: 'application/json',
      ...(init.body ? { 'content-type': 'application/json' } : {}),
      ...(init.headers ?? {})
    }
  });
  const text = await response.text();
  const body = text ? JSON.parse(text) : null;
  if (!response.ok) {
    throw new Error(`${init.method ?? 'GET'} ${path} failed with HTTP ${response.status}: ${JSON.stringify(body)}`);
  }
  return body;
}

async function main() {
  await requestJson('/healthz?refresh_broker_status=false');

  const desired = await requestJson('/v1/market-data/stream/desired', {
    method: 'POST',
    body: JSON.stringify({
      symbols: ['AXFO', 'AZN'],
      exchange: 'SMART',
      primary_exchange: 'SFB',
      currency: 'SEK',
      market_data_type: 'LIVE',
      replace: true
    })
  });
  console.log('desired symbols:', desired.stream.desired_symbols);

  const snapshot = await requestJson('/v1/market-data/stream/snapshot?symbols=AXFO,AZN&bar_limit=20');
  console.log('running:', snapshot.stream.running);
  console.log('bars:', Object.fromEntries(
    Object.entries(snapshot.stream.bars_by_symbol ?? {}).map(([symbol, bars]) => [symbol, bars.length])
  ));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
```

## OpenAPI

FastAPI exposes generated schema and docs when the service is running:

```text
GET /openapi.json
GET /docs
```

Use the repo contract above as the operational source of truth for market-data
usage, especially around which endpoints open broker sessions and which ones
are safe cached/local reads.
