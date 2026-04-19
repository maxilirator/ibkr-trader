const DEFAULT_API_BASE_URL = 'http://127.0.0.1:8000';

export function normalizeBaseUrl(value) {
  return (value || DEFAULT_API_BASE_URL).replace(/\/+$/, '');
}

export async function readJson(fetch, url, init = {}) {
  try {
    const response = await fetch(url, {
      headers: {
        accept: 'application/json',
        ...(init.headers ?? {})
      },
      ...init
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
    return {
      ok: false,
      status: 0,
      error: error instanceof Error ? error.message : String(error),
      body: null
    };
  }
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
