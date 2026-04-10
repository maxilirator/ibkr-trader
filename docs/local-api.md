# Local API

This repository now includes a small FastAPI control plane intended for **local-only** access.

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

### `POST /v1/instructions/schedule-preview`

Accepts the canonical instruction batch and returns a read-only runtime schedule view.

It currently:

- projects `entry.submit_at` and `entry.expire_at` into both UTC and the configured runtime timezone
- makes the active entry window explicit before we wire order submission to it
- resolves `force_exit_next_session_open` for Stockholm instruments from the shared q-data session calendar when available
- otherwise keeps the instruction explicitly unresolved until a market calendar resolves it

Current scheduling rule:

- the runtime default timezone is `Europe/Stockholm`
- Stockholm next-session exits come from the local q-data session calendar
- next-session exits are not guessed from wall-clock dates
- non-Stockholm or unresolved markets stay explicitly unresolved until a real exchange calendar resolves them

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

1. `POST /v1/orders/preview`
2. `POST /v1/instructions/submit`
3. `POST /v1/orders/{id}/cancel`
4. `GET /v1/orders/{id}`
5. `GET /v1/positions`
6. `POST /v1/market-data/subscribe`
