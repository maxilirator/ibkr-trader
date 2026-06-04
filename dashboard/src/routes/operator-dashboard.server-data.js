import { error } from '@sveltejs/kit';
import { readJson } from '$lib/server/trader-api';

const RECONCILIATION_RUN_READ_LIMIT = 120;
const OPERATOR_INSTRUCTION_READ_LIMIT = 500;
const OPERATOR_ORDER_READ_LIMIT = 500;

export function readOptionalField(formData, fieldName) {
  const value = formData.get(fieldName);
  if (value === null) {
    return null;
  }
  const normalized = String(value).trim();
  return normalized || null;
}

export function parsePositiveIntegerIds(rawValue) {
  if (!rawValue) {
    return [];
  }

  const values = String(rawValue)
    .split(/[\s,]+/)
    .map((value) => Number.parseInt(value.trim(), 10))
    .filter((value) => Number.isInteger(value) && value > 0);

  return [...new Set(values)];
}

export function parsePositiveIntegerIdsFromForm(formData, fieldName) {
  return parsePositiveIntegerIds(readOptionalField(formData, fieldName));
}

export function readBooleanField(formData, fieldName, defaultValue = false) {
  const value = readOptionalField(formData, fieldName);
  if (value === null) {
    return defaultValue;
  }
  return ['1', 'true', 'yes', 'on'].includes(value.toLowerCase());
}

export function readIntentCleanupSelector(formData) {
  const selector = {};
  for (const fieldName of [
    'account_key',
    'book_key',
    'book_side',
    'symbol',
    'exchange',
    'currency',
    'keep_instruction_id'
  ]) {
    const value = readOptionalField(formData, fieldName);
    if (value !== null) {
      selector[fieldName] = value;
    }
  }
  const instructionIds = readOptionalField(formData, 'instruction_ids');
  if (instructionIds !== null) {
    selector.instruction_ids = [
      ...new Set(
        instructionIds
          .split(/[\s,]+/)
          .map((value) => value.trim())
          .filter(Boolean)
      )
    ];
  }
  return selector;
}

export function intentCleanupSummary(cleanup, scopeLabel = 'Intent cleanup') {
  if (!cleanup) {
    return `${scopeLabel}: response missing intent_cleanup.`;
  }
  const actionCount = Number(cleanup.action_count ?? 0);
  const appliedCount = Number(cleanup.applied_action_count ?? 0);
  const pendingCount = Number(cleanup.cancelled_pending_count ?? 0);
  const submittedCount = Number(cleanup.cancelled_submitted_count ?? 0);
  const blockerCount = Number(cleanup.blocked_count ?? 0);
  const failedCount = Number(cleanup.failed_count ?? 0);
  const preserved = blockerCount > 0 ? `, ${blockerCount} position owner(s) preserved` : '';
  const failed = failedCount > 0 ? `, ${failedCount} failed` : '';
  return (
    `${scopeLabel}: ${cleanup.status ?? 'UNKNOWN'}; ${appliedCount}/${actionCount} ` +
    `action(s) applied, ${pendingCount} pending and ${submittedCount} submitted entry row(s) cancelled` +
    `${preserved}${failed}.`
  );
}

export function intentCleanupOk(cleanup) {
  return cleanup !== null && cleanup !== undefined && Number(cleanup.failed_count ?? 0) === 0;
}

export function virtualAccountKeysFromSnapshot(snapshot) {
  const accountKeys = new Set();
  const addVirtualAccount = (row) => {
    const accountKey = String(row?.account_key ?? '').trim();
    if (row?.is_virtual === true && accountKey) {
      accountKeys.add(accountKey);
    }
  };

  for (const row of snapshot?.accounts ?? []) addVirtualAccount(row);
  for (const row of snapshot?.positions ?? []) addVirtualAccount(row);
  for (const row of snapshot?.open_orders ?? []) addVirtualAccount(row);
  for (const row of snapshot?.instructions ?? []) addVirtualAccount(row);
  return [...accountKeys].sort();
}

export function operatorSnapshotUrl(apiBaseUrl) {
  return (
    `${apiBaseUrl}/v1/read/operator-snapshot` +
    `?instruction_limit=${OPERATOR_INSTRUCTION_READ_LIMIT}&candidate_limit=40` +
    `&order_limit=${OPERATOR_ORDER_READ_LIMIT}&fill_limit=50&attention_limit=20&reconciliation_run_limit=${RECONCILIATION_RUN_READ_LIMIT}`
  );
}

export function omxBenchmarkSnapshotUrl(apiBaseUrl) {
  const params = new URLSearchParams({
    symbols: 'OMXS30',
    bar_limit: '600'
  });
  return `${apiBaseUrl}/v1/market-data/stream/snapshot?${params.toString()}`;
}

export function parseFiniteNumber(value) {
  const parsed = Number.parseFloat(String(value ?? ''));
  if (!Number.isFinite(parsed)) {
    return null;
  }
  // IBKR uses very large sentinel values for unavailable index bid/ask fields.
  return Math.abs(parsed) < 1e12 ? parsed : null;
}

export function stockholmDstStartUtc(year) {
  const lastDay = new Date(Date.UTC(year, 2, 31));
  const lastSunday = 31 - lastDay.getUTCDay();
  return Date.UTC(year, 2, lastSunday, 1, 0, 0);
}

export function stockholmDstEndUtc(year) {
  const lastDay = new Date(Date.UTC(year, 9, 31));
  const lastSunday = 31 - lastDay.getUTCDay();
  return Date.UTC(year, 9, lastSunday, 1, 0, 0);
}

export function stockholmOffsetHours(year, month, day, hour, minute, second) {
  const standardUtc = Date.UTC(year, month - 1, day, hour - 1, minute, second);
  return standardUtc >= stockholmDstStartUtc(year) && standardUtc < stockholmDstEndUtc(year)
    ? 2
    : 1;
}

export function normalizeBarTimestamp(value) {
  if (!value) {
    return null;
  }
  const raw = String(value).trim();
  const parsed = new Date(raw);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toISOString();
  }

  const match = raw.match(
    /^(\d{4})(\d{2})(\d{2})\s+(\d{2}):(\d{2}):(\d{2})(?:\s+([A-Z]+))?$/
  );
  if (!match) {
    return null;
  }
  const [, yearText, monthText, dayText, hourText, minuteText, secondText, zoneText] =
    match;
  const year = Number.parseInt(yearText, 10);
  const month = Number.parseInt(monthText, 10);
  const day = Number.parseInt(dayText, 10);
  const hour = Number.parseInt(hourText, 10);
  const minute = Number.parseInt(minuteText, 10);
  const second = Number.parseInt(secondText, 10);
  const zone = String(zoneText ?? 'MET').toUpperCase();
  const offsetHours =
    zone === 'CET'
      ? 1
      : zone === 'CEST'
        ? 2
        : stockholmOffsetHours(year, month, day, hour, minute, second);
  return new Date(
    Date.UTC(year, month - 1, day, hour - offsetHours, minute, second)
  ).toISOString();
}

export function benchmarkPointFromQuote(quote) {
  const latest = parseFiniteNumber(quote?.last_price);
  const previousClose = parseFiniteNumber(quote?.close_price);
  const latestTimestamp = quote?.last_trade_at ?? quote?.updated_at;
  if (latest === null || previousClose === null || !latestTimestamp || previousClose === 0) {
    return null;
  }

  const end = new Date(latestTimestamp);
  if (Number.isNaN(end.getTime())) {
    return null;
  }
  const start = stockholmSessionOpenForTimestamp(end.toISOString()) ?? new Date(end.getTime() - 60_000).toISOString();
  return [
    {
      timestamp: start,
      value: previousClose,
      return_pct: 0
    },
    {
      timestamp: end.toISOString(),
      value: latest,
      return_pct: ((latest - previousClose) / previousClose) * 100
    }
  ];
}

export function stockholmDateKeyForTimestamp(timestamp) {
  const parsed = new Date(timestamp);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  const parts = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Europe/Stockholm',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(parsed);
  const byType = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${byType.year}-${byType.month}-${byType.day}`;
}

export function stockholmSessionOpenForTimestamp(timestamp) {
  const dateKey = stockholmDateKeyForTimestamp(timestamp);
  if (!dateKey) {
    return null;
  }
  const [year, month, day] = dateKey.split('-').map((part) => Number.parseInt(part, 10));
  const offsetHours = stockholmOffsetHours(year, month, day, 9, 0, 0);
  return new Date(Date.UTC(year, month - 1, day, 9 - offsetHours, 0, 0)).toISOString();
}

export function latestBenchmarkPointFromQuote(quote) {
  const latest = parseFiniteNumber(quote?.last_price);
  const latestTimestamp = quote?.last_trade_at ?? quote?.updated_at;
  if (latest === null || !latestTimestamp) {
    return null;
  }
  const timestamp = normalizeBarTimestamp(latestTimestamp);
  return timestamp ? { timestamp, value: latest } : null;
}

export function buildOmxBenchmarkFromBars(rawBars, { source = 'stream', quote = null } = {}) {
  const pointsByTimestamp = new Map();
  for (const bar of Array.isArray(rawBars) ? rawBars : []) {
    const timestamp = normalizeBarTimestamp(bar?.timestamp);
    const value = parseFiniteNumber(bar?.close);
    if (timestamp && value !== null) {
      pointsByTimestamp.set(timestamp, { timestamp, value });
    }
  }

  const quotePoint = latestBenchmarkPointFromQuote(quote);
  if (quotePoint) {
    pointsByTimestamp.set(quotePoint.timestamp, quotePoint);
  }

  const allBars = [...pointsByTimestamp.values()].sort(
    (left, right) => new Date(left.timestamp).getTime() - new Date(right.timestamp).getTime()
  );
  const latestDateKey = stockholmDateKeyForTimestamp(allBars.at(-1)?.timestamp);
  const validBars = allBars
    .filter((bar) => !latestDateKey || stockholmDateKeyForTimestamp(bar.timestamp) === latestDateKey)
    .map((bar) => ({
      timestamp: bar.timestamp,
      value: bar.value
    }))
    .filter((bar) => bar.value !== 0);
  const previousClose = parseFiniteNumber(quote?.close_price);
  const baseline = previousClose ?? validBars[0]?.value ?? null;
  if (baseline === null || baseline === 0) {
    return null;
  }

  const anchorTimestamp =
    previousClose !== null
      ? stockholmSessionOpenForTimestamp(validBars.at(-1)?.timestamp ?? quotePoint?.timestamp)
      : null;
  const anchorPoint =
    anchorTimestamp !== null
      ? [{ timestamp: anchorTimestamp, value: baseline, return_pct: 0 }]
      : [];
  const points = validBars.map((bar) => ({
    timestamp: bar.timestamp,
    value: bar.value,
    return_pct: ((bar.value - baseline) / baseline) * 100
  }));
  const allPoints = [...anchorPoint, ...points];
  const latest = allPoints.at(-1);
  return {
    label: 'OMX',
    symbol: 'OMXS30',
    status: allPoints.length > 1 ? 'ok' : 'insufficient_data',
    error: null,
    latest_return_pct: latest ? latest.return_pct : null,
    points: allPoints,
    source
  };
}

export function buildOmxBenchmark(result) {
  const fallback = {
    label: 'OMX',
    symbol: 'OMXS30',
    status: 'unavailable',
    error: null,
    latest_return_pct: null,
    points: []
  };

  if (!result.ok) {
    return {
      ...fallback,
      error: result.error
    };
  }

  const barsBySymbol = result.body?.stream?.bars_by_symbol ?? {};
  const quotes = result.body?.stream?.quotes ?? [];
  const quote = Array.isArray(quotes)
    ? quotes.find((item) => item?.symbol === 'OMXS30')
    : null;
  for (const symbol of ['OMXS30']) {
    const benchmark = buildOmxBenchmarkFromBars(barsBySymbol[symbol], {
      source: 'market_stream',
      quote
    });
    if (benchmark) {
      return benchmark;
    }
  }

  const quotePoints = benchmarkPointFromQuote(quote);
  if (quotePoints) {
    const latest = quotePoints.at(-1);
    return {
      ...fallback,
      symbol: 'OMXS30',
      label: 'OMX',
      status: 'ok',
      latest_return_pct: latest ? latest.return_pct : null,
      points: quotePoints
    };
  }

  return fallback;
}

let omxBenchmarkCache = {
  fetchedAt: 0,
  value: null
};

export async function readOmxBenchmark(fetch, apiBaseUrl) {
  const now = Date.now();
  if (omxBenchmarkCache.value && now - omxBenchmarkCache.fetchedAt < 60_000) {
    return omxBenchmarkCache.value;
  }

  const streamResult = await readJson(fetch, omxBenchmarkSnapshotUrl(apiBaseUrl), {
    timeoutMs: 2500
  });
  const streamBenchmark = buildOmxBenchmark(streamResult);
  if (streamBenchmark.status === 'ok' && streamBenchmark.points.length >= 30) {
    omxBenchmarkCache = { fetchedAt: now, value: streamBenchmark };
    return streamBenchmark;
  }

  omxBenchmarkCache = { fetchedAt: now, value: streamBenchmark };
  return streamBenchmark;
}

export function requireResponseBody(result, endpointName) {
  if (!result.ok) {
    throw error(result.status || 502, `${endpointName} failed: ${result.error}`);
  }
  if (!result.body || typeof result.body !== 'object') {
    throw error(502, `${endpointName} returned no JSON body`);
  }
  return result.body;
}

export function requireBodyField(result, fieldName, endpointName) {
  const body = requireResponseBody(result, endpointName);
  if (body[fieldName] === undefined || body[fieldName] === null) {
    throw error(502, `${endpointName} response missing ${fieldName}`);
  }
  return body[fieldName];
}

export async function readOperatorSnapshot(fetch, apiBaseUrl) {
  const result = await readJson(fetch, operatorSnapshotUrl(apiBaseUrl));
  if (!result.ok) {
    return {
      ok: false,
      status: result.status,
      error: result.error,
      snapshot: null
    };
  }
  if (!result.body || result.body.operator_snapshot === undefined || result.body.operator_snapshot === null) {
    return {
      ok: false,
      status: 502,
      error: 'Operator snapshot response missing operator_snapshot',
      snapshot: null
    };
  }

  return {
    ok: true,
    status: result.status,
    error: null,
    snapshot: result.body.operator_snapshot
  };
}
