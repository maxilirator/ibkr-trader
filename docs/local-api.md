# Local API

This repository now includes a small FastAPI control plane intended for **local-only** access.

Current runtime scope:

- Stockholm equities first
- `Europe/Stockholm` as the runtime timezone
- q-data Stockholm session calendar as the next-session scheduler input

## Why this shape

For this system, using the official IBKR Python API directly inside a local service is a good fit:

- we keep IBKR session management in one process
- AI and orchestration layers can call a simple HTTP API
- we avoid exposing the broker API on the network
- we retain full control over validation, scheduling, and audit behavior

This is especially useful once multiple internal components need to submit instructions, validate intents, or request broker state without each one opening its own IBKR connection.

The runtime scheduler uses `Europe/Stockholm` as the default local timezone and reads the Stockholm session calendar from `SESSION_CALENDAR_PATH`.

If the configured parquet file cannot be read directly, the scheduler will use the sibling `.csv` file when it exists.

## Security posture

The local API is designed to accept loopback traffic only:

- default bind host is `127.0.0.1`
- startup rejects non-loopback bind targets when local-only mode is enabled
- middleware rejects requests whose client address is not loopback

This is a strong starting point, but it is not the final security model for a production quant system. Later we should add:

- process-level auth between local services
- OS firewall rules
- separate live and paper environments
- stricter instruction authorization and audit controls

## Endpoints

### `GET /healthz`

Returns basic process status and local-only configuration.

### `POST /v1/ibkr/probe`

Runs the current broker probe and returns:

- host
- port
- client ID
- broker-reported current time
- next valid order ID

This is the fastest end-to-end check that the official IBKR API path is healthy.

### `POST /v1/contracts/resolve`

Runs a read-only IBKR contract lookup using `reqContractDetails`.

Use it to verify:

- whether a symbol resolves uniquely
- the IBKR `conId`
- primary exchange and currency
- valid exchanges and trading class
- basic market metadata before any order-preview logic exists

Example request body:

```json
{
  "symbol": "SIVE",
  "security_type": "STK",
  "exchange": "XSTO",
  "currency": "SEK",
  "primary_exchange": "XSTO",
  "isin": "SE0003917798"
}
```

### `POST /v1/accounts/summary`

Runs a read-only IBKR account summary query through the diagnostic client ID.

Use it to fetch the fields we need for sizing and risk checks, especially:

- `NetLiquidation`
- `BuyingPower`
- `AvailableFunds`
- `ExcessLiquidity`

Example request body:

```json
{
  "account_id": "DU1234567",
  "tags": ["NetLiquidation", "BuyingPower", "AvailableFunds", "ExcessLiquidity"]
}
```

### `POST /v1/market-data/historical-bars`

Runs a read-only IBKR historical-bars request through the diagnostic client ID.

Use it to:

- fetch historical bars in the instrument's native trading currency
- verify the resolved contract before building data ingestion jobs
- build the first native-currency bar ingestion path for the internal data backend

Example request body:

```json
{
  "symbol": "SIVE",
  "security_type": "STK",
  "exchange": "SMART",
  "currency": "SEK",
  "primary_exchange": "SFB",
  "duration": "2 D",
  "bar_size": "5 mins",
  "what_to_show": "TRADES",
  "use_rth": true
}
```

### `POST /v1/market-data/tick-stream-sample`

Collects a short live sample from IBKR's tick-by-tick streaming API through the dedicated streaming client session.

Use it to:

- verify streaming connectivity separately from execution and diagnostic sessions
- collect raw live ticks for a short sampling window
- validate which tick-by-tick streams are entitled for the current IBKR session

It currently supports:

- `Last`
- `AllLast`
- `BidAsk`
- `MidPoint`

Important current behavior:

- this is a timed sample endpoint, not a long-lived socket relay yet
- it is intended as the first raw-data primitive for the future parquet ingestion service
- if IBKR rejects the requested tick-by-tick stream, the endpoint returns the broker error directly

### `POST /v1/orders/preview`

Builds a read-only broker preview for an instruction batch using one diagnostic IBKR session.

Currency rule:

- market prices stay in the instrument's native trading currency
- order prices stay in the instrument's native trading currency
- fills and future execution events should be reported in the instrument's native trading currency
- account-level sizing may start from account currency and convert explicitly into instrument currency

It currently:

- validates the instruction batch
- reads account summary
- resolves the broker contract
- constructs the broker order fields we would send at execution time
- reports unresolved cases explicitly instead of guessing

Current safe behavior:

- ready for `target_quantity`
- ready for `target_notional` when `limit_price` is present
- ready for `fraction_of_account_nav`, including cross-currency sizing via IBKR historical FX midpoint data when the pair is available
- unresolved when IBKR cannot provide a usable FX conversion for the requested currency pair

This is intentional. We should not guess FX sizing in a production trading system, so the preview only uses broker-derived FX data.

### `POST /v1/orders/submit`

Submits a **manual paper broker order** immediately through the primary IBKR client session.

This endpoint is intentionally narrower than the durable instruction flow:

- exactly one instruction per request
- stock orders only
- `LIMIT` orders only
- whole-share quantities only
- uses the canonical instruction contract, but submits immediately instead of scheduling

It currently:

- validates the canonical instruction batch
- resolves the broker account
- resolves the IBKR contract
- computes quantity using the same sizing logic as preview
- places the paper order through the primary IBKR client session
- returns the broker order status payload

Important current behavior:

- this is an operator/manual paper-trading path
- it is not wired to persisted `ENTRY_PENDING` instructions yet
- use it for broker-path smoke tests and careful manual paper validation

### `POST /v1/orders/{order_id}/cancel`

Cancels a paper broker order by IBKR order ID through the primary client session.

This endpoint is the cleanup companion to `POST /v1/orders/submit`.

### `POST /v1/instructions/submit`

Accepts the canonical instruction batch, validates it, computes the Stockholm runtime schedule, and persists the instruction into Postgres.

It currently:

- validates the instruction batch
- computes runtime schedule metadata, including next-session-open for Stockholm instruments
- persists the instruction in `ENTRY_PENDING` state
- writes an initial `instruction_submitted` lifecycle event
- returns the stored instruction state back to the caller

Important current behavior:

- this endpoint does **not** place a broker order yet
- it is a durable control-plane submit, not a live execution submit

### `GET /v1/instructions/{instruction_id}`

Returns the persisted control-plane status for one instruction.

It currently includes:

- current execution state
- submit/expire timestamps
- entry broker IDs and broker status
- entry/exit fill fields already reconciled into Postgres
- the instruction event trail by default

Optional query parameter:

- `include_events=true|false`

### `POST /v1/instructions/{instruction_id}/submit-entry`

Submits a persisted `ENTRY_PENDING` instruction to the broker through the primary IBKR client session.

It currently:

- loads the persisted canonical instruction from Postgres
- requires current state `ENTRY_PENDING`
- resolves and submits the broker order using the same sizing and contract logic as manual submit
- stores `broker_order_id`, `broker_perm_id`, `broker_client_id`, and `broker_order_status` on the instruction record
- writes an `entry_order_submitted` event
- moves instruction state to `ENTRY_SUBMITTED`

This is the first DB-backed execution bridge between durable instructions and IBKR.

### `POST /v1/instructions/{instruction_id}/cancel-entry`

Cancels a persisted `ENTRY_SUBMITTED` entry order through the primary IBKR client session.

It currently:

- looks up the persisted instruction and its `broker_order_id`
- cancels the broker order
- updates `broker_order_status`
- writes an `entry_order_cancelled` event
- moves instruction state to `ENTRY_CANCELLED`

### `POST /v1/runtime/run-once`

Runs one MVP execution-runtime cycle against persisted instructions.

It currently:

- submits any due `ENTRY_PENDING` instructions
- fetches IBKR open orders and executions
- reconciles entry fills into persisted state
- submits a take-profit exit after a full entry fill when `exit.take_profit_pct` is present
- submits a forced market exit when `force_exit_next_session_open` is due from the Stockholm session calendar
- marks instructions `COMPLETED` after exit fills are reconciled

Example request body:

```json
{
  "now_at": "2026-04-13T09:00:00+02:00",
  "instruction_ids": ["2026-04-13-GTW05-long_risk_book-AAPL-long-01"],
  "timeout": 10
}
```

Important current behavior:

- this is a polling-style MVP runtime cycle, not the final long-lived broker process
- it uses the primary IBKR client session
- it is safe for manual operator-driven paper testing
- when `instruction_ids` is provided, the cycle only touches that selected set
- it now retries transient IBKR client-id reuse / reconnect churn a small number of times before failing the cycle
- a persistent runtime-owned IBKR connection is still the next step

### `POST /v1/instructions/schedule-preview`

Accepts the canonical instruction batch and returns a read-only runtime schedule view.

It currently:

- projects `entry.submit_at` and `entry.expire_at` into both UTC and the configured runtime timezone
- makes the active entry window explicit before we wire order submission to it
- resolves `force_exit_next_session_open` for Stockholm instruments from the shared q-data session calendar

Current scheduling rule:

- the runtime default timezone is `Europe/Stockholm`
- Stockholm next-session exits come from the local q-data session calendar
- next-session exits are not guessed from wall-clock dates

This matches the current project scope: Stockholm first.

### `POST /v1/instructions/validate`

Accepts a JSON instruction batch payload and validates it against the execution contract.

The canonical request format is defined in [instruction-contract.md](instruction-contract.md).

## Running it

Important config is read from the repo-root `.env` file automatically.

Install the server dependencies in your environment, then run:

```bash
source .venv/bin/activate
python3 -m ibkr_trader.api.server
```

Optional development reload:

```bash
source .venv/bin/activate
python3 -m ibkr_trader.api.server --reload
```

## Next server steps

The natural next endpoints are:

1. keep a long-lived runtime-owned IBKR connection instead of reconnecting per cycle
2. persist broker callbacks and fills beyond polling-only reconciliation
3. add restart reconciliation against IBKR open orders, executions, and positions
4. turn tick-stream sampling into a long-lived local streaming service
