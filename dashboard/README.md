# Dashboard

Minimal SvelteKit dashboard for the local IBKR Trader API.

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
- `GET /v1/broker/runtime-snapshot`
- `GET /v1/instructions?limit=100`

The API base URL is controlled by:

```bash
IBKR_TRADER_API_BASE_URL=http://127.0.0.1:8000
```

## Run

Install Node.js first, then:

```bash
cd dashboard
npm install
IBKR_TRADER_API_BASE_URL=http://127.0.0.1:8000 npm run dev -- --host 127.0.0.1 --port 4173
```

For a production-style Node build:

```bash
cd dashboard
npm install
npm run build
IBKR_TRADER_API_BASE_URL=http://127.0.0.1:8000 node build
```
