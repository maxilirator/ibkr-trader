const DEFAULT_INTERVAL_MS = 1000;
const MAX_INTERVAL_MS = 10000;
const KEEPALIVE_MS = 15000;

export function parseBoundedInteger(value, { fallback, minimum = 250, maximum = MAX_INTERVAL_MS } = {}) {
  const parsed = Number.parseInt(String(value ?? ''), 10);
  if (!Number.isInteger(parsed)) {
    return fallback;
  }
  return Math.min(Math.max(parsed, minimum), maximum);
}

export function encodeSse(event, payload) {
  return `event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`;
}

export function createSnapshotEventResponse({
  request,
  url,
  eventName,
  loadSnapshot,
  intervalMs = DEFAULT_INTERVAL_MS
}) {
  const resolvedIntervalMs = parseBoundedInteger(url.searchParams.get('interval_ms'), {
    fallback: intervalMs,
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
          // Client may already have disconnected.
        }
      }

      async function tick() {
        if (closed || request.signal.aborted) {
          close();
          return;
        }

        try {
          const payload = await loadSnapshot();
          const signature = JSON.stringify(payload);
          if (signature !== lastSignature) {
            lastSignature = signature;
            enqueue(
              encodeSse(eventName, {
                received_at: new Date().toISOString(),
                ...payload
              })
            );
          } else if (Date.now() - lastKeepaliveAt >= KEEPALIVE_MS) {
            lastKeepaliveAt = Date.now();
            enqueue(': keepalive\n\n');
          }
        } catch (error) {
          if (!request.signal.aborted) {
            enqueue(
              encodeSse(`${eventName}-error`, {
                received_at: new Date().toISOString(),
                message: error instanceof Error ? error.message : String(error)
              })
            );
          }
        }

        if (!closed) {
          timeoutId = setTimeout(tick, resolvedIntervalMs);
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
