# Dashboard

Minimal SvelteKit dashboard for the LAN-visible IBKR Trader API.

## What it shows

- broker session health
- account summary
- current holdings
- open orders
- recent persisted instructions
- recent executions

## Runtime contract

The dashboard reads the Python API server and expects these endpoints:

- `GET /healthz`
- `GET /v1/read/operator-snapshot`
- `GET /v1/read/ledger-snapshot`

The API base URL is controlled by:

```bash
IBKR_TRADER_API_BASE_URL=http://quant.geisler.se:8000
```

## Run

Install Node.js first, then:

```bash
cd dashboard
npm install
IBKR_TRADER_API_BASE_URL=http://quant.geisler.se:8000 npm run dev -- --host 127.0.0.1 --port 4173
```

For a production-style Node build:

```bash
cd dashboard
npm install
npm run build
IBKR_TRADER_API_BASE_URL=http://127.0.0.1:8000 HOST=0.0.0.0 PORT=4173 ORIGIN=http://quant.geisler.se:4173 npm run start
```
