# Local API

This repository now includes a small FastAPI control plane intended for **local-only** access.

## Why this shape

For this system, using the official IBKR Python API directly inside a local service is a good fit:

- we keep IBKR session management in one process
- AI and orchestration layers can call a simple HTTP API
- we avoid exposing the broker API on the network
- we retain full control over validation, scheduling, and audit behavior

This is especially useful once multiple internal components need to submit instructions, validate intents, or request broker state without each one opening its own IBKR connection.

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
