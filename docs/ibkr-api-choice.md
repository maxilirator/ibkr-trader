# IBKR API Choice

## Recommendation

Use the **TWS API via IB Gateway** as the primary execution and market-data integration for this repository.

## Why

IBKR's current API overview says:

- Web API is REST over HTTPS with OAuth 2.0 and covers trading functionality, live market data, scanners, and intraday portfolio updates.
- TWS API is built on a TCP socket connection and is intended for fast paced, data intensive, and complex trading.
- TWS API has near-full parity with Trader Workstation functionality and can be paired with IB Gateway for lower-overhead automated deployments.
- FIX is offered for professional traders, organizations, and institutions.

For a professional quant execution stack that needs rich order lifecycle handling, streaming data, and broker callback control, TWS API is the strongest first fit.

Official sources:

- https://ibkrcampus.com/campus/ibkr-api-page/getting-started/
- https://ibkrcampus.com/campus/ibkr-api-page/twsapi-doc/
- https://ibkrcampus.com/campus/ibkr-api-page/twsapi-ref/
- https://ibkrcampus.com/campus/ibkr-api-page/cpapi-v1/

## Decision Table

### TWS API

Use for:

- low-latency order submission and callback-driven execution
- streaming market data and account updates
- complex order handling and order lifecycle reconciliation
- durable connection from our own execution service to IB Gateway

Useful native features from the official reference include:

- `GoodAfterTime`
- `GoodTillDate`
- order `Conditions`
- `ParentId`
- `OcaGroup`

These help implement parts of scheduled and contingent logic, but they do not replace our own multi-day workflow engine.

### Web API

Use later for:

- service-style integrations where OAuth 2.0 is operationally attractive
- selected account and trading functions where REST is more convenient
- internal tooling that benefits from request/response patterns

The Web API also supports websocket topics and bracket/OCA order structures. That makes it useful, but for a primary execution engine it is still secondary to TWS API for this repo.

### FIX

Consider only if:

- you need institutional FIX workflows
- you already operate FIX infrastructure
- IBKR grants the appropriate setup for your account structure

FIX is important, but it is not the fastest path to getting this system live.

## How to model the user example

Example instruction:

`place limit order at 09:25 with prices X, after fill place take profit at +2% and stop loss at -15%, if still open next morning sell at opening`

Recommended implementation:

1. Store this as one high-level instruction object in our system.
2. At the scheduled time, create the entry order.
3. When fill events arrive, create the exit bracket or exit policy state.
4. Before the next session opens, evaluate whether the position is still open.
5. If still open, cancel outstanding child orders if needed and place the opening exit workflow.
6. Persist every transition and reconcile against live IBKR state on restart.

This is safer than encoding the whole behavior as a single opaque broker-side construction.

## Data backend implications

Building an internal intraday data backend is technically possible through IBKR APIs, but it should be treated as an ingestion service with explicit limits and entitlements:

- API market data requires the proper subscriptions and API market-data acknowledgement.
- Off-platform API data is licensed differently from on-platform TWS display data.
- Concurrent data access is limited by market data line allocations.
- Tick-by-tick and market depth have tighter limits than ordinary top-of-book lines.

For shortability, IBKR documents a "Shortable" tick and notes that actual shortable share counts require TWS 974+; they also point to an FTP site for more detailed shortability data outside TWS.

Official sources:

- https://ibkrcampus.com/campus/ibkr-api-page/market-data-subscriptions/
- https://interactivebrokers.github.io/tws-api/tick_types.html

