export function normalizeBaseUrl(value) {
  const normalized = String(value ?? '').trim();
  if (!normalized) {
    throw new Error('IBKR_TRADER_API_BASE_URL is required');
  }
  return normalized.replace(/\/+$/, '');
}

export async function readJson(fetch, url, init = {}) {
  const { headers, signal, timeoutMs, ...fetchInit } = init;
  const timeoutController =
    Number.isFinite(timeoutMs) && timeoutMs > 0 ? new AbortController() : null;
  let timeoutId = null;
  let signalAbortHandler = null;

  try {
    if (timeoutController) {
      if (signal?.aborted) {
        timeoutController.abort(signal.reason);
      } else if (signal) {
        signalAbortHandler = () => timeoutController.abort(signal.reason);
        signal.addEventListener('abort', signalAbortHandler, { once: true });
      }
      timeoutId = setTimeout(() => timeoutController.abort(), timeoutMs);
    }

    const response = await fetch(url, {
      ...fetchInit,
      headers: {
        accept: 'application/json',
        ...(headers ?? {})
      },
      signal: timeoutController ? timeoutController.signal : signal
    });
    const text = await response.text();
    const body = text ? JSON.parse(text) : null;

    if (!response.ok) {
      const errorMessage =
        body?.detail ?? body?.message ?? (text || `HTTP ${response.status}`);
      return {
        ok: false,
        status: response.status,
        error: errorMessage,
        body
      };
    }

    return {
      ok: true,
      status: response.status,
      error: null,
      body
    };
  } catch (error) {
    const timedOut = error instanceof Error && error.name === 'AbortError' && timeoutController;
    return {
      ok: false,
      status: 0,
      error: timedOut
        ? `Request timed out after ${timeoutMs}ms`
        : error instanceof Error
          ? error.message
          : String(error),
      body: null
    };
  } finally {
    if (timeoutId !== null) {
      clearTimeout(timeoutId);
    }
    if (signal && signalAbortHandler) {
      signal.removeEventListener('abort', signalAbortHandler);
    }
  }
}

export function buildFallbackHealth(apiBaseUrl, errorMessage = null) {
  const unavailableMessage = errorMessage ?? 'healthz unavailable';

  return {
    status: 'degraded',
    api_base_url: apiBaseUrl,
    runtime_timezone: 'Europe/Stockholm',
    dashboard_health_error: unavailableMessage,
    broker_sessions: {
      primary: {
        connected: false,
        client_id: null,
        last_error: unavailableMessage
      },
      diagnostic: {
        connected: false,
        client_id: null,
        last_error: unavailableMessage
      }
    },
    broker_operations: {
      total_operations: 0,
      successful_operations: 0,
      failed_operations: 0,
      operations_last_60_seconds: 0,
      per_operation: {},
      recent_operations: []
    },
    broker_monitor: {
      status_checked_at: null,
      running: false,
      refresh_in_flight: false,
      heartbeat: {
        ok: null,
        is_stale: true,
        last_attempt_age_seconds: null,
        last_success_age_seconds: null,
        error: unavailableMessage
      },
      snapshot_refresh: {
        ok: null,
        is_stale: true,
        last_attempt_age_seconds: null,
        last_success_age_seconds: null,
        account_count: 0,
        portfolio_count: 0,
        position_count: 0,
        open_order_count: 0,
        execution_count: 0,
        error: unavailableMessage
      }
    },
    execution_runtime: {
      status: 'UNKNOWN',
      effective_status: 'UNKNOWN',
      is_stale: true,
      heartbeat_age_seconds: null,
      lease_seconds_remaining: null,
      last_error: unavailableMessage
    }
  };
}

export async function postJson(fetch, url, body) {
  return readJson(fetch, url, {
    method: 'POST',
    headers: {
      'content-type': 'application/json'
    },
    body: JSON.stringify(body)
  });
}

export async function patchJson(fetch, url, body) {
  return readJson(fetch, url, {
    method: 'PATCH',
    headers: {
      'content-type': 'application/json'
    },
    body: JSON.stringify(body)
  });
}

export async function postWithoutBody(fetch, url) {
  return readJson(fetch, url, {
    method: 'POST'
  });
}

export function buildEndpointErrorMap(results) {
  return Object.fromEntries(
    Object.entries(results).map(([key, value]) => [key, value.ok ? null : value.error])
  );
}
