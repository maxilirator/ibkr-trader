import { env } from '$env/dynamic/private';
import { normalizeBaseUrl } from '$lib/server/trader-api';

const DEFAULT_INTERVAL_MS = 1000;
const MAX_INTERVAL_MS = 10000;
const DEFAULT_BAR_LIMIT = 390;
const MAX_BAR_LIMIT = 2000;
const KEEPALIVE_MS = 15000;

function parseBoundedInteger(value, { fallback, minimum = 1, maximum }) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isInteger(parsed)) {
    return fallback;
  }
  return Math.min(Math.max(parsed, minimum), maximum);
}

function streamSnapshotUrl(apiBaseUrl, url) {
  const params = new URLSearchParams({
    bar_limit: String(
      parseBoundedInteger(url.searchParams.get('bar_limit'), {
        fallback: DEFAULT_BAR_LIMIT,
        maximum: MAX_BAR_LIMIT
      })
    )
  });
  const symbols = String(url.searchParams.get('symbols') ?? '').trim();
  if (symbols) {
    params.set('symbols', symbols);
  }
  return `${apiBaseUrl}/v1/market-data/stream/snapshot?${params.toString()}`;
}

function latestBarIdentity(bars) {
  if (!Array.isArray(bars) || bars.length === 0) {
    return null;
  }
  const latest = bars.at(-1);
  if (!latest || typeof latest !== 'object') {
    return null;
  }
  return [
    latest.timestamp,
    latest.open,
    latest.high,
    latest.low,
    latest.close,
    latest.bar_count
  ];
}

function streamSignature(stream) {
  const quotes = Array.isArray(stream?.quotes)
    ? stream.quotes.map((quote) => [
        quote?.symbol,
        quote?.bid_price,
        quote?.ask_price,
        quote?.last_price,
        quote?.close_price,
        quote?.updated_at,
        quote?.last_trade_at
      ])
    : [];
  const barsBySymbol =
    stream?.bars_by_symbol && typeof stream.bars_by_symbol === 'object'
      ? Object.entries(stream.bars_by_symbol)
          .sort(([left], [right]) => left.localeCompare(right))
          .map(([symbol, bars]) => [symbol, latestBarIdentity(bars)])
      : [];

  return JSON.stringify({
    running: stream?.running,
    latest_market_data_at: stream?.latest_market_data_at,
    latest_quote_at: stream?.latest_quote_at,
    latest_trade_at: stream?.latest_trade_at,
    desired_symbols: stream?.desired_symbols,
    subscribed_count: stream?.subscribed_count,
    quotes,
    bars_by_symbol: barsBySymbol
  });
}

function encodeSse(event, payload) {
  return `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
}

export async function GET({ fetch, request, url }) {
  const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
  const snapshotUrl = streamSnapshotUrl(apiBaseUrl, url);
  const intervalMs = parseBoundedInteger(url.searchParams.get('interval_ms'), {
    fallback: DEFAULT_INTERVAL_MS,
    minimum: 250,
    maximum: MAX_INTERVAL_MS
  });
  const encoder = new TextEncoder();
  let timeoutId = null;
  let closed = false;
  let lastSignature = null;
  let lastKeepaliveAt = 0;

  const body = new ReadableStream({
    start(controller) {
      function enqueue(text) {
        if (!closed) {
          controller.enqueue(encoder.encode(text));
        }
      }

      function close() {
        if (closed) {
          return;
        }
        closed = true;
        if (timeoutId !== null) {
          clearTimeout(timeoutId);
          timeoutId = null;
        }
        try {
          controller.close();
        } catch {
          // The client may already have gone away.
        }
      }

      async function tick() {
        if (closed || request.signal.aborted) {
          close();
          return;
        }

        try {
          const response = await fetch(snapshotUrl, {
            headers: { accept: 'application/json' },
            signal: request.signal
          });
          const payload = await response.json();
          if (!response.ok) {
            throw new Error(payload?.detail ?? payload?.message ?? `HTTP ${response.status}`);
          }

          const stream = payload?.stream ?? payload;
          const signature = streamSignature(stream);
          if (signature !== lastSignature) {
            lastSignature = signature;
            enqueue(
              encodeSse('stream', {
                received_at: new Date().toISOString(),
                stream
              })
            );
          } else if (Date.now() - lastKeepaliveAt >= KEEPALIVE_MS) {
            lastKeepaliveAt = Date.now();
            enqueue(': keepalive\n\n');
          }
        } catch (error) {
          if (!request.signal.aborted) {
            enqueue(
              encodeSse('stream-error', {
                received_at: new Date().toISOString(),
                message: error instanceof Error ? error.message : String(error)
              })
            );
          }
        }

        if (!closed) {
          timeoutId = setTimeout(tick, intervalMs);
        }
      }

      request.signal.addEventListener('abort', close, { once: true });
      enqueue(': connected\n\n');
      void tick();
    },
    cancel() {
      closed = true;
      if (timeoutId !== null) {
        clearTimeout(timeoutId);
      }
    }
  });

  return new Response(body, {
    headers: {
      'cache-control': 'no-cache, no-transform',
      connection: 'keep-alive',
      'content-type': 'text/event-stream; charset=utf-8',
      'x-accel-buffering': 'no'
    }
  });
}
