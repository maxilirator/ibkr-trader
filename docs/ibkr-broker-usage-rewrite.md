# IBKR Broker Usage Rewrite

This is the project document for reducing IB Gateway instability caused by
our own API usage pattern. It is intentionally broader than a guardrail: the
target is a smaller broker surface, fewer live calls, explicit pacing, and a
local market-data path that RL can trust without repeatedly asking Gateway.

## Problem Statement

The Gateway has been stable over quiet weekend periods while the dashboard and
RL runner still perform cached HTTP reads. It becomes unstable on trading days
when the API stack repeatedly touches live broker sessions:

- client `9`: API-owned market-data stream subscribe and repair
- client `8`: historical bars and controlled backfill
- client `7`: diagnostics, probes, and contract lookup
- client `0`: runtime snapshots, reconciliation, order submit, and cancel

The failure sequence observed on `quant` is:

1. RL and dashboard paths keep reading local/cache state.
2. Live broker paths begin returning `socket connected but no nextValidId`.
3. Historical bars, heartbeat probe, runtime snapshots, and any direct stream
   repair continue to retry around the half-alive Gateway.
4. IBC later reports `Existing session detected`, `Shutdown progress`, or a
   deadlock/shutdown state.

The design response is to make Gateway a scarce external dependency, not a
general-purpose data lake.

## Design Principles

- One process owns IBKR API access: the live trader API on `quant`.
- One long-lived stream client owns market data subscriptions.
- Subscriptions are diffed, not re-sent as identical full sets every cycle.
- RL reads locally persisted bars and stream state; RL does not fan out broker
  requests in the minute loop.
- Historical data is warmup/backfill only, serialized and paced.
- `nextValidId` startup failure is a global broker health failure, not an
  invitation for more clients to try.
- Pacing is explicit, configurable, and visible in health telemetry.
- Code that bypasses the owner model should be removed or isolated behind
  operator-only tooling.

## Current Call Inventory

| Path | Client | Native IBKR Calls | Intended Future |
| --- | ---: | --- | --- |
| `/v1/market-data/stream/desired` | none | Local desired-state write | Runner path; safe to call every cycle when the desired set changes |
| `/v1/market-data/stream/subscribe` | `9` | `reqMktData`, `cancelMktData`, `reqMarketDataType` | API/operator stream-owner path only, diff-only and paced |
| `/v1/market-data/stream/snapshot` | none | Local memory/db read | Keep, can be frequent |
| `/v1/market-data/historical-bars` | `8` | `reqContractDetails`, `reqHistoricalData` | Operator/backfill only |
| `/v1/rl/observations/build` | none in request path | Enqueues coalesced day backfill requests only | Local/cache only; paused symbols wait for backfill worker |
| `/v1/ibkr/probe` | `7` | `reqCurrentTime`, `get_next_valid_id` | Keep, slow probe only during bad health |
| runtime snapshot/reconcile | `0` | open orders, executions, account updates, positions | Keep, but stop under circuit breaker |
| shortability/tick samples | `7`/`9` | ad hoc market-data/sample calls | Move to scheduled/operator-only paths |

## Pacing Layer

The first slice introduces a process-local governor:

- `IBKR_API_MAX_REQUESTS_PER_SECOND`
- `IBKR_API_PACING_TIMEOUT_SECONDS`
- `IBKR_MARKET_DATA_LINE_LIMIT`
- `IBKR_HISTORICAL_REQUESTS_PER_10_MINUTES`

Defaults are intentionally below the public IBKR ceilings:

- `45` API request permits per second
- `80` market-data lines
- `50` historical requests per 10 minutes

The governor is shared by:

- canonical managed broker sessions for clients `0`, `7`, and `8`
- the live market stream service for client `9`

This is not the final architecture. It prevents obvious overload while the
larger rewrite removes unnecessary broker calls from the active path.

## Target Architecture

### 1. Market Data Owner

The API owns a desired subscription set:

- symbols wanted by active live instructions
- symbols wanted by active RL deployments
- benchmark/context symbols such as `OMXS30`

The stream service applies a diff:

- add only missing subscriptions
- cancel only removed subscriptions
- do not call `reqMktData` for unchanged symbols
- do not reconnect simply because a dashboard page refreshed
- let RL publish the desired set without opening or repairing broker
  subscriptions from the runner process
- on IBKR connectivity `1101`, mark active subscriptions for resubscribe from
  the desired set
- on IBKR connectivity `1102`, record that data was maintained and do not churn
  subscriptions

### 2. Local Bar Store

The stream service converts ticks into 1-minute bars and persists them. RL
observation building reads:

- current in-memory stream bars
- persisted stream bars
- precomputed overnight/backfill bars

The minute loop must not call `/v1/market-data/historical-bars` as a normal
fallback. `scripts/run_rl_agents.py` now defaults to metadata/history-feature
inputs only; live historical calls require the explicit
`--allow-live-historical-backfill` diagnostic flag.

Live RL follows the same observed-row semantics as the research lane:

- do not synthesize no-trade 5-minute bars
- aggregate only provider/IB rows that actually exist
- track coverage metadata separately from the model path
- pause a symbol when observed complete 5-minute coverage falls below
  `RL_OBSERVED_BAR_MIN_COVERAGE_RATIO`
- enqueue one symbol/day historical request and leave the symbol paused while
  `market_data_backfill_request` is pending or running

The observation request path never performs the historical call itself. The
background backfill worker drains the durable queue under the historical pacing
limit (`IBKR_HISTORICAL_REQUESTS_PER_10_MINUTES`) and persists returned 1-minute
bars into the local market stream store. If IB returns no rows for a period, the
coverage request can still complete; the RL symbol remains paused only if the
observed coverage is genuinely below policy after the backfill.

### 3. Health-Based Broker Circuit

When any managed broker client sees:

- socket open but no `nextValidId`
- IBC stuck shutdown
- existing session detected
- deadlock reported

then live broker paths should enter a global degraded state:

- no stream repair
- no historical bars
- no runtime snapshots except a deliberately paced recovery probe
- dashboard continues to show local read models and the exact broker-health
  reason

This is implemented as a shared broker circuit in the API process. A managed
session `nextValidId` startup failure trips the circuit for the slow-probe
window. Primary, diagnostic, historical/backfill, and market-stream repair
paths consult the same circuit. A forced health probe can still be run
deliberately by the operator.

### 4. Client-ID Ownership

Client IDs remain stable:

- `0`: trading/runtime/order state
- `7`: health and controlled diagnostics
- `8`: historical bars and controlled backfill
- `9`: streaming

Client `7` is no longer the shared home for history and RL backfills. It still
owns diagnostics/probes and some controlled read-only lookup paths.

## Cleanup Plan

Remove or quarantine code paths in this order:

1. Live RL historical fallback in the per-minute loop.
2. Repeated full-set stream subscribe calls from the runner when the signature
   did not change.
3. Dashboard or health refreshes that trigger live broker calls by default.
4. Ad hoc sample endpoints that open extra broker sessions without operator
   intent.
5. Legacy smoke scripts that hit live Gateway by default from development
   machines.

Each removal should have a test proving the replacement path still surfaces
truthful state. Do not delete broker functionality that is still the only way
to recover or audit live orders.

## Migration Steps

1. Add shared pacing and line limits. Done in the first slice.
2. Expose pacing state in `/healthz` and `/v1/ibkr/telemetry`. Done in the
   first slice.
3. Move RL observation building to local stream/persisted bars only in the live
   runner. Started: the promoted runner now defaults away from live historical
   backfill.
4. Add an API-owned desired-subscription endpoint so the runner can publish
   intent without forcing immediate broker repair. Done: use
   `/v1/market-data/stream/desired`; the stream service reconciles active IBKR
   subscriptions from one owner.
5. Add a global broker health gate that all live broker paths consult. Done for
   managed primary/diagnostic/historical sessions and market-stream repair.
6. Split or remove overloaded diagnostic client use. Started: historical bars
   and RL/backfill paths now use client `8`.
7. Add observed-bar coverage gating and durable RL backfill queue. Done:
   `/v1/rl/observations/build` enqueues coalesced symbol/day backfill requests
   and returns paused per-symbol observations instead of hammering IB.
8. Remove obsolete ad hoc broker callers once the new owner paths are proven.

## Rollout Rules

- Test locally without a second local API talking to Gateway.
- Deploy only to the `quant` user services.
- Restart application services only; do not restart IB Gateway as part of this
  project unless the operator explicitly asks.
- After deploy, observe:
  - pacing snapshot
  - active market-data line count
  - stream desired/subscribed symbol counts
  - `nextValidId` failures
  - Gateway IBC dialog extraction

## Open Decisions

- Whether to keep historical/backfill fully offline during live hours, despite
  the dedicated client ID.
- Whether stream stale repair should reconnect automatically during open
  market, or require operator approval after a `nextValidId` failure.
- Whether full Stockholm exchange collection belongs in this API process or in
  a separate nightly collector with its own Gateway session and pacing profile.
