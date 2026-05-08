import { error, fail } from '@sveltejs/kit';
import { env } from '$env/dynamic/private';
import {
  buildFallbackHealth,
  buildEndpointErrorMap,
  normalizeBaseUrl,
  postJson,
  postWithoutBody,
  readJson
} from '$lib/server/trader-api';

const RECONCILIATION_RUN_READ_LIMIT = 120;
const OPERATOR_INSTRUCTION_READ_LIMIT = 500;
const OPERATOR_ORDER_READ_LIMIT = 500;

function readOptionalField(formData, fieldName) {
  const value = formData.get(fieldName);
  if (value === null) {
    return null;
  }
  const normalized = String(value).trim();
  return normalized || null;
}

function parsePositiveIntegerIds(rawValue) {
  if (!rawValue) {
    return [];
  }

  const values = String(rawValue)
    .split(/[\s,]+/)
    .map((value) => Number.parseInt(value.trim(), 10))
    .filter((value) => Number.isInteger(value) && value > 0);

  return [...new Set(values)];
}

function parsePositiveIntegerIdsFromForm(formData, fieldName) {
  return parsePositiveIntegerIds(readOptionalField(formData, fieldName));
}

function readBooleanField(formData, fieldName, defaultValue = false) {
  const value = readOptionalField(formData, fieldName);
  if (value === null) {
    return defaultValue;
  }
  return ['1', 'true', 'yes', 'on'].includes(value.toLowerCase());
}

function readIntentCleanupSelector(formData) {
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

function intentCleanupSummary(cleanup, scopeLabel = 'Intent cleanup') {
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

function intentCleanupOk(cleanup) {
  return cleanup !== null && cleanup !== undefined && Number(cleanup.failed_count ?? 0) === 0;
}

function virtualAccountKeysFromSnapshot(snapshot) {
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

function operatorSnapshotUrl(apiBaseUrl) {
  return (
    `${apiBaseUrl}/v1/read/operator-snapshot` +
    `?instruction_limit=${OPERATOR_INSTRUCTION_READ_LIMIT}&candidate_limit=40` +
    `&order_limit=${OPERATOR_ORDER_READ_LIMIT}&fill_limit=50&attention_limit=20&reconciliation_run_limit=${RECONCILIATION_RUN_READ_LIMIT}`
  );
}

function omxBenchmarkSnapshotUrl(apiBaseUrl) {
  const params = new URLSearchParams({
    symbols: 'OMXS30',
    bar_limit: '390'
  });
  return `${apiBaseUrl}/v1/market-data/stream/snapshot?${params.toString()}`;
}

function parseFiniteNumber(value) {
  const parsed = Number.parseFloat(String(value ?? ''));
  if (!Number.isFinite(parsed)) {
    return null;
  }
  // IBKR uses very large sentinel values for unavailable index bid/ask fields.
  return Math.abs(parsed) < 1e12 ? parsed : null;
}

function stockholmDstStartUtc(year) {
  const lastDay = new Date(Date.UTC(year, 2, 31));
  const lastSunday = 31 - lastDay.getUTCDay();
  return Date.UTC(year, 2, lastSunday, 1, 0, 0);
}

function stockholmDstEndUtc(year) {
  const lastDay = new Date(Date.UTC(year, 9, 31));
  const lastSunday = 31 - lastDay.getUTCDay();
  return Date.UTC(year, 9, lastSunday, 1, 0, 0);
}

function stockholmOffsetHours(year, month, day, hour, minute, second) {
  const standardUtc = Date.UTC(year, month - 1, day, hour - 1, minute, second);
  return standardUtc >= stockholmDstStartUtc(year) && standardUtc < stockholmDstEndUtc(year)
    ? 2
    : 1;
}

function normalizeBarTimestamp(value) {
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

function benchmarkPointFromQuote(quote) {
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
  const start = new Date(end.getTime() - 60_000);
  return [
    {
      timestamp: start.toISOString(),
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

function buildOmxBenchmarkFromBars(rawBars, { source = 'stream' } = {}) {
  const validBars = (Array.isArray(rawBars) ? rawBars : [])
    .map((bar) => ({
      timestamp: normalizeBarTimestamp(bar.timestamp),
      value: parseFiniteNumber(bar.close)
    }))
    .filter((bar) => bar.timestamp && bar.value !== null);
  const first = validBars.find((bar) => bar.value !== 0);
  if (!first) {
    return null;
  }

  const points = validBars.map((bar) => ({
    timestamp: bar.timestamp,
    value: bar.value,
    return_pct: ((bar.value - first.value) / first.value) * 100
  }));
  const latest = points.at(-1);
  return {
    label: 'OMX',
    symbol: 'OMXS30',
    status: points.length > 1 ? 'ok' : 'insufficient_data',
    error: null,
    latest_return_pct: latest ? latest.return_pct : null,
    points,
    source
  };
}

function buildOmxBenchmark(result) {
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
  for (const symbol of ['OMXS30']) {
    const benchmark = buildOmxBenchmarkFromBars(barsBySymbol[symbol], {
      source: 'market_stream'
    });
    if (benchmark) {
      return benchmark;
    }
  }

  const quotes = result.body?.stream?.quotes ?? [];
  const quote = Array.isArray(quotes)
    ? quotes.find((item) => item?.symbol === 'OMXS30')
    : null;
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

async function readOmxBenchmark(fetch, apiBaseUrl) {
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

function requireResponseBody(result, endpointName) {
  if (!result.ok) {
    throw error(result.status || 502, `${endpointName} failed: ${result.error}`);
  }
  if (!result.body || typeof result.body !== 'object') {
    throw error(502, `${endpointName} returned no JSON body`);
  }
  return result.body;
}

function requireBodyField(result, fieldName, endpointName) {
  const body = requireResponseBody(result, endpointName);
  if (body[fieldName] === undefined || body[fieldName] === null) {
    throw error(502, `${endpointName} response missing ${fieldName}`);
  }
  return body[fieldName];
}

async function readOperatorSnapshot(fetch, apiBaseUrl) {
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

export async function load({ fetch }) {
  const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
  const healthUrl = `${apiBaseUrl}/healthz`;

  const [health, operatorSnapshot] = await Promise.all([
    readJson(fetch, healthUrl, { timeoutMs: 2500 }),
    readJson(fetch, operatorSnapshotUrl(apiBaseUrl))
  ]);
  const omxBenchmark = await readOmxBenchmark(fetch, apiBaseUrl);

  return {
    generatedAt: new Date().toISOString(),
    apiBaseUrl,
    errors: buildEndpointErrorMap({
      health,
      operatorSnapshot
    }),
    health: health.ok
      ? requireResponseBody(health, 'healthz')
      : buildFallbackHealth(apiBaseUrl, health.error),
    operatorSnapshot: requireBodyField(
      operatorSnapshot,
      'operator_snapshot',
      'Operator snapshot'
    ),
    omxBenchmark
  };
}

export const actions = {
  async archiveAllReconciliation({ fetch }) {
    const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
    const result = await postJson(
      fetch,
      `${apiBaseUrl}/v1/reconciliation-issues/archive-open`,
      {
        action: 'ARCHIVE',
        updated_by: 'dashboard',
        note: 'Operator dashboard archive all reconciliation warnings.'
      }
    );
    if (!result.ok) {
      return fail(result.status || 500, {
        reconciliationClearResult: {
          ok: false,
          message: result.error
        }
      });
    }

    const archivedCount =
      result.body?.reconciliation_issue_archive?.archived_issue_count ?? 0;
    return {
      reconciliationClearResult: {
        ok: true,
        message:
          archivedCount === 0
            ? 'No reconciliation issues needed archiving.'
            : `Archived ${archivedCount} reconciliation issues.`
      }
    };
  },

  async acknowledgeVisibleReconciliation({ fetch, request }) {
    const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
    const formData = await request.formData();
    const requestedIssueIds = parsePositiveIntegerIdsFromForm(formData, 'issue_ids');

    if (requestedIssueIds.length > 0) {
      let acknowledgedReconciliationIssueCount = 0;
      for (const issueId of requestedIssueIds) {
        const result = await postJson(
          fetch,
          `${apiBaseUrl}/v1/reconciliation-issues/${issueId}/review`,
          {
            action: 'ARCHIVE',
            updated_by: 'dashboard'
          }
        );
        if (!result.ok) {
          return fail(result.status || 500, {
            reconciliationClearResult: {
              ok: false,
              message:
                `Archived ${acknowledgedReconciliationIssueCount} reconciliation issues before failing: ` +
                `${result.error}`
            }
          });
        }
        acknowledgedReconciliationIssueCount += 1;
      }

      return {
        reconciliationClearResult: {
          ok: true,
          message:
            acknowledgedReconciliationIssueCount === 0
              ? 'No visible reconciliation issues needed archiving.'
              : `Archived ${acknowledgedReconciliationIssueCount} visible reconciliation issues.`
        }
      };
    }

    const snapshotResult = await readOperatorSnapshot(fetch, apiBaseUrl);
    if (!snapshotResult.ok) {
      return fail(snapshotResult.status || 500, {
        reconciliationClearResult: {
          ok: false,
          message: snapshotResult.error
        }
      });
    }

    const reconciliationRuns = snapshotResult.snapshot.recent_reconciliation_runs;
    const openReconciliationIssues = reconciliationRuns.flatMap((run) =>
      run.issues.filter((issue) => issue.operator_review.status === 'OPEN')
    );

    let acknowledgedReconciliationIssueCount = 0;
    for (const item of openReconciliationIssues) {
      const result = await postJson(
        fetch,
        `${apiBaseUrl}/v1/reconciliation-issues/${item.issue_id}/review`,
        {
          action: 'ARCHIVE',
          updated_by: 'dashboard'
        }
      );
      if (!result.ok) {
        return fail(result.status || 500, {
          reconciliationClearResult: {
            ok: false,
            message:
              `Archived ${acknowledgedReconciliationIssueCount} reconciliation issues before failing: ` +
              `${result.error}`
          }
        });
      }
      acknowledgedReconciliationIssueCount += 1;
    }

    return {
      reconciliationClearResult: {
        ok: true,
        message:
          acknowledgedReconciliationIssueCount === 0
            ? 'No open reconciliation issues needed archiving.'
            : `Archived ${acknowledgedReconciliationIssueCount} visible reconciliation issues.`
      }
    };
  },

  async acknowledgeAllLogs({ fetch, request }) {
    const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
    const formData = await request.formData();
    const requestedEventIds = parsePositiveIntegerIdsFromForm(formData, 'event_ids');
    const requestedIssueIds = parsePositiveIntegerIdsFromForm(formData, 'issue_ids');

    if (requestedEventIds.length > 0 || requestedIssueIds.length > 0) {
      let acknowledgedBrokerAttentionCount = 0;
      let acknowledgedReconciliationIssueCount = 0;

      for (const eventId of requestedEventIds) {
        const result = await postJson(
          fetch,
          `${apiBaseUrl}/v1/broker-attention/${eventId}/review`,
          {
            action: 'ARCHIVE',
            updated_by: 'dashboard'
          }
        );
        if (!result.ok) {
          return fail(result.status || 500, {
            acknowledgeAllLogsResult: {
              ok: false,
              message:
                `Archived ${acknowledgedBrokerAttentionCount} broker attention items and ` +
                `${acknowledgedReconciliationIssueCount} reconciliation issues before failing: ` +
                `${result.error}`
            }
          });
        }
        acknowledgedBrokerAttentionCount += 1;
      }

      for (const issueId of requestedIssueIds) {
        const result = await postJson(
          fetch,
          `${apiBaseUrl}/v1/reconciliation-issues/${issueId}/review`,
          {
            action: 'ARCHIVE',
            updated_by: 'dashboard'
          }
        );
        if (!result.ok) {
          return fail(result.status || 500, {
            acknowledgeAllLogsResult: {
              ok: false,
              message:
                `Archived ${acknowledgedBrokerAttentionCount} broker attention items and ` +
                `${acknowledgedReconciliationIssueCount} reconciliation issues before failing: ` +
                `${result.error}`
            }
          });
        }
        acknowledgedReconciliationIssueCount += 1;
      }

      const totalAcknowledged =
        acknowledgedBrokerAttentionCount + acknowledgedReconciliationIssueCount;
      return {
        acknowledgeAllLogsResult: {
          ok: true,
          message:
            totalAcknowledged === 0
              ? 'No visible broker-attention or reconciliation-log items needed archiving.'
              : `Archived ${acknowledgedBrokerAttentionCount} broker attention items and ` +
                `${acknowledgedReconciliationIssueCount} reconciliation issues.`
        }
      };
    }

    const snapshotResult = await readOperatorSnapshot(fetch, apiBaseUrl);
    if (!snapshotResult.ok) {
      return fail(snapshotResult.status || 500, {
        acknowledgeAllLogsResult: {
          ok: false,
          message: snapshotResult.error
        }
      });
    }

    const brokerAttention = snapshotResult.snapshot.recent_broker_attention;
    const reconciliationRuns = snapshotResult.snapshot.recent_reconciliation_runs;
    const openBrokerAttention = brokerAttention.filter(
      (item) => item.operator_review.status === 'OPEN'
    );
    const openReconciliationIssues = reconciliationRuns.flatMap((run) =>
      run.issues.filter((issue) => issue.operator_review.status === 'OPEN')
    );

    let acknowledgedBrokerAttentionCount = 0;
    let acknowledgedReconciliationIssueCount = 0;

    for (const item of openBrokerAttention) {
      const result = await postJson(
        fetch,
        `${apiBaseUrl}/v1/broker-attention/${item.event_id}/review`,
        {
          action: 'ARCHIVE',
          updated_by: 'dashboard'
        }
      );
      if (!result.ok) {
        return fail(result.status || 500, {
          acknowledgeAllLogsResult: {
            ok: false,
            message:
              `Archived ${acknowledgedBrokerAttentionCount} broker attention items and ` +
              `${acknowledgedReconciliationIssueCount} reconciliation issues before failing: ` +
              `${result.error}`
          }
        });
      }
      acknowledgedBrokerAttentionCount += 1;
    }

    for (const item of openReconciliationIssues) {
      const result = await postJson(
        fetch,
        `${apiBaseUrl}/v1/reconciliation-issues/${item.issue_id}/review`,
        {
          action: 'ARCHIVE',
          updated_by: 'dashboard'
        }
      );
      if (!result.ok) {
        return fail(result.status || 500, {
          acknowledgeAllLogsResult: {
            ok: false,
            message:
              `Archived ${acknowledgedBrokerAttentionCount} broker attention items and ` +
              `${acknowledgedReconciliationIssueCount} reconciliation issues before failing: ` +
              `${result.error}`
          }
        });
      }
      acknowledgedReconciliationIssueCount += 1;
    }

    const totalAcknowledged =
      acknowledgedBrokerAttentionCount + acknowledgedReconciliationIssueCount;
    return {
      acknowledgeAllLogsResult: {
        ok: true,
        message:
          totalAcknowledged === 0
            ? 'No open broker-attention or reconciliation-log items needed archiving.'
            : `Archived ${acknowledgedBrokerAttentionCount} broker attention items and ` +
              `${acknowledgedReconciliationIssueCount} reconciliation issues.`
      }
    };
  },

  async killSwitch({ fetch, request }) {
    const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
    const formData = await request.formData();
    const enabledValue = readOptionalField(formData, 'enabled');
    const reason = readOptionalField(formData, 'reason');

    if (enabledValue !== 'true' && enabledValue !== 'false') {
      return fail(400, {
        killSwitchResult: {
          ok: false,
          message: 'The requested kill switch state was invalid.'
        }
      });
    }

    const desiredState = enabledValue === 'true';
    const result = await postJson(fetch, `${apiBaseUrl}/v1/controls/kill-switch`, {
      enabled: desiredState,
      reason,
      updated_by: 'dashboard'
    });

    if (!result.ok) {
      return fail(result.status || 500, {
        killSwitchResult: {
          ok: false,
          message: result.error
        }
      });
    }

    return {
      killSwitchResult: {
        ok: true,
        message: desiredState
          ? 'Global kill switch enabled.'
          : 'Global kill switch disabled.'
      }
    };
  },

  async startupReconcile({ fetch }) {
    const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
    const result = await postJson(fetch, `${apiBaseUrl}/v1/runtime/startup-reconcile`, {
      timeout: 15
    });

    if (!result.ok) {
      return fail(result.status || 500, {
        startupReconcileResult: {
          ok: false,
          message: result.error
        }
      });
    }

    const runtimeResult = result.body?.runtime_result;
    if (!runtimeResult) {
      return fail(502, {
        startupReconcileResult: {
          ok: false,
          message: 'Startup reconciliation response missing runtime_result'
        }
      });
    }
    return {
      startupReconcileResult: {
        ok: true,
        message:
          `Startup reconciliation completed with status ${runtimeResult.status}: ` +
          `${runtimeResult.issue_count} issues, ${runtimeResult.action_count} actions.`
      }
    };
  },

  async intentCleanup({ fetch, request }) {
    const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
    const formData = await request.formData();
    const scope = readOptionalField(formData, 'scope') ?? 'group';
    const apply = readBooleanField(formData, 'apply', true);
    const cancelAllEntries = readBooleanField(formData, 'cancel_all_entries', true);
    const reason =
      readOptionalField(formData, 'reason') ??
      (scope === 'virtual_total'
        ? 'Dashboard total virtual intent cleanup; preserve position-owning close state.'
        : 'Dashboard per-item intent cleanup; preserve position-owning close state.');

    if (scope === 'virtual_total') {
      const snapshotResult = await readOperatorSnapshot(fetch, apiBaseUrl);
      if (!snapshotResult.ok) {
        return fail(snapshotResult.status || 500, {
          intentCleanupResult: {
            ok: false,
            message: snapshotResult.error
          }
        });
      }

      const accountKeys = virtualAccountKeysFromSnapshot(snapshotResult.snapshot);
      if (accountKeys.length === 0) {
        return {
          intentCleanupResult: {
            ok: true,
            message: 'Virtual intent cleanup: no virtual accounts were visible in the operator snapshot.'
          }
        };
      }

      const totals = {
        actionCount: 0,
        appliedCount: 0,
        pendingCount: 0,
        submittedCount: 0,
        blockerCount: 0,
        failedCount: 0
      };
      let processedCount = 0;
      for (const accountKey of accountKeys) {
        const result = await postJson(fetch, `${apiBaseUrl}/v1/instructions/intent-cleanup`, {
          requested_by: 'dashboard',
          reason,
          apply,
          cancel_all_entries: cancelAllEntries,
          account_key: accountKey,
          timeout: 10
        });

        if (!result.ok) {
          return fail(result.status || 500, {
            intentCleanupResult: {
              ok: false,
              message:
                `Cleaned ${processedCount}/${accountKeys.length} virtual account(s) before failing ` +
                `${accountKey}: ${result.error}`
            }
          });
        }

        const cleanup = result.body?.intent_cleanup;
        if (!cleanup) {
          return fail(502, {
            intentCleanupResult: {
              ok: false,
              message: `Virtual intent cleanup response for ${accountKey} was missing intent_cleanup.`
            }
          });
        }

        totals.actionCount += Number(cleanup.action_count ?? 0);
        totals.appliedCount += Number(cleanup.applied_action_count ?? 0);
        totals.pendingCount += Number(cleanup.cancelled_pending_count ?? 0);
        totals.submittedCount += Number(cleanup.cancelled_submitted_count ?? 0);
        totals.blockerCount += Number(cleanup.blocked_count ?? 0);
        totals.failedCount += Number(cleanup.failed_count ?? 0);
        processedCount += 1;
      }

      const message =
        `Virtual intent cleanup: ${processedCount} account(s); ` +
        `${totals.appliedCount}/${totals.actionCount} action(s) applied, ` +
        `${totals.pendingCount} pending and ${totals.submittedCount} submitted entry row(s) cancelled` +
        (totals.blockerCount > 0 ? `, ${totals.blockerCount} position owner(s) preserved` : '') +
        (totals.failedCount > 0 ? `, ${totals.failedCount} failed` : '') +
        '.';
      return totals.failedCount > 0
        ? fail(500, {
            intentCleanupResult: {
              ok: false,
              message
            }
          })
        : {
            intentCleanupResult: {
              ok: true,
              message
            }
          };
    }

    const selector = readIntentCleanupSelector(formData);
    if (Object.keys(selector).length === 0) {
      return fail(400, {
        intentCleanupResult: {
          ok: false,
          message: 'Intent cleanup needs an account, symbol, or instruction selector.'
        }
      });
    }

    const result = await postJson(fetch, `${apiBaseUrl}/v1/instructions/intent-cleanup`, {
      requested_by: 'dashboard',
      reason,
      apply,
      cancel_all_entries: cancelAllEntries,
      ...selector,
      timeout: 10
    });

    if (!result.ok) {
      return fail(result.status || 500, {
        intentCleanupResult: {
          ok: false,
          message: result.error
        }
      });
    }

    const cleanup = result.body?.intent_cleanup;
    const cleanupOk = intentCleanupOk(cleanup);
    return cleanupOk
      ? {
          intentCleanupResult: {
            ok: true,
            message: intentCleanupSummary(cleanup, 'Intent group cleanup')
          }
        }
      : fail(500, {
          intentCleanupResult: {
            ok: false,
            message: intentCleanupSummary(cleanup, 'Intent group cleanup')
          }
        });
  },

  async archiveDashboardNoise({ fetch }) {
    const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
    const expireBefore = new Date().toISOString();
    const staleCandidates = await postJson(fetch, `${apiBaseUrl}/v1/instructions/archive-set`, {
      requested_by: 'dashboard',
      reason: 'Dashboard archive of expired model-routed candidate rows.',
      states: ['MODEL_ROUTED_PENDING'],
      model_routed: true,
      expire_before: expireBefore,
      limit: 1000
    });

    if (!staleCandidates.ok) {
      return fail(staleCandidates.status || 500, {
        archiveResult: {
          ok: false,
          message: staleCandidates.error
        }
      });
    }

    const terminalExecutions = await postJson(fetch, `${apiBaseUrl}/v1/instructions/archive-set`, {
      requested_by: 'dashboard',
      reason: 'Dashboard archive of terminal execution instruction rows.',
      states: ['ENTRY_CANCELLED', 'COMPLETED', 'FAILED'],
      model_routed: false,
      expire_before: expireBefore,
      limit: 1000
    });

    if (!terminalExecutions.ok) {
      return fail(terminalExecutions.status || 500, {
        archiveResult: {
          ok: false,
          message: terminalExecutions.error
        }
      });
    }

    const staleArchive = staleCandidates.body?.archived_instruction_set;
    const terminalArchive = terminalExecutions.body?.archived_instruction_set;
    return {
      archiveResult: {
        ok: true,
        message:
          `Archived ${staleArchive?.archived_instruction_count ?? 0} expired RL candidates and ` +
          `${terminalArchive?.archived_instruction_count ?? 0} terminal execution instructions.`
      }
    };
  },

  async instructionRowAction({ fetch, request }) {
    const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
    const formData = await request.formData();
    const instructionId = readOptionalField(formData, 'instruction_id');
    const operation = readOptionalField(formData, 'operation');

    if (!instructionId) {
      return fail(400, {
        instructionRowActionResult: {
          ok: false,
          message: 'Instruction action is missing the instruction ID.'
        }
      });
    }

    const operationMap = {
      cancel_entry: {
        execute: () =>
          postWithoutBody(fetch, `${apiBaseUrl}/v1/instructions/${instructionId}/cancel-entry`),
        successMessage: `Cancelled submitted entry for instruction ${instructionId}.`
      },
      cancel_instruction: {
        execute: () =>
          postJson(fetch, `${apiBaseUrl}/v1/instructions/cancel-set`, {
            requested_by: 'dashboard',
            reason: 'Row-level instruction cancellation from the operator dashboard.',
            instruction_ids: [instructionId]
          }),
        successMessage: `Cancelled instruction ${instructionId}.`
      }
    };
    const selectedOperation = operationMap[operation];

    if (!selectedOperation) {
      return fail(400, {
        instructionRowActionResult: {
          ok: false,
          message: 'Instruction action type was invalid.'
        }
      });
    }

    const result = await selectedOperation.execute();
    if (!result.ok) {
      return fail(result.status || 500, {
        instructionRowActionResult: {
          ok: false,
          message: result.error
        }
      });
    }

    return {
      instructionRowActionResult: {
        ok: true,
        message: selectedOperation.successMessage
      }
    };
  },

  async orderRowAction({ fetch, request }) {
    const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
    const formData = await request.formData();
    const externalOrderId = readOptionalField(formData, 'external_order_id');

    if (!externalOrderId) {
      return fail(400, {
        orderRowActionResult: {
          ok: false,
          message: 'Order action is missing the broker order ID.'
        }
      });
    }

    const normalizedOrderId = Number.parseInt(externalOrderId, 10);
    if (!Number.isInteger(normalizedOrderId) || normalizedOrderId <= 0) {
      return fail(400, {
        orderRowActionResult: {
          ok: false,
          message: 'Broker order ID must be a positive integer.'
        }
      });
    }

    const result = await postWithoutBody(
      fetch,
      `${apiBaseUrl}/v1/orders/${normalizedOrderId}/cancel`
    );

    if (!result.ok) {
      return fail(result.status || 500, {
        orderRowActionResult: {
          ok: false,
          message: result.error
        }
      });
    }

    return {
      orderRowActionResult: {
        ok: true,
        message: `Cancelled broker order ${normalizedOrderId}.`
      }
    };
  },

  async brokerAttentionAction({ fetch, request }) {
    const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
    const formData = await request.formData();
    const eventId = readOptionalField(formData, 'event_id');
    const eventIds = parsePositiveIntegerIdsFromForm(formData, 'event_ids');
    const operation = readOptionalField(formData, 'operation');

    const normalizedEventIds =
      eventIds.length > 0
        ? eventIds
        : parsePositiveIntegerIds(eventId ?? '');
    if (normalizedEventIds.length === 0) {
      return fail(400, {
        brokerAttentionActionResult: {
          ok: false,
          message: 'Broker attention action is missing a valid event ID list.'
        }
      });
    }

    if (!operation) {
      return fail(400, {
        brokerAttentionActionResult: {
          ok: false,
          message: 'Broker attention action type was invalid.'
        }
      });
    }

    let processedCount = 0;
    for (const normalizedEventId of normalizedEventIds) {
      const result = await postJson(
        fetch,
        `${apiBaseUrl}/v1/broker-attention/${normalizedEventId}/review`,
        {
          action: operation,
          updated_by: 'dashboard'
        }
      );

      if (!result.ok) {
        return fail(result.status || 500, {
          brokerAttentionActionResult: {
            ok: false,
            message:
              `Archived ${processedCount} broker attention items before failing: ${result.error}`
          }
        });
      }
      processedCount += 1;
    }

    return {
      brokerAttentionActionResult: {
        ok: true,
        message:
          processedCount === 1
            ? `Archived broker attention item ${normalizedEventIds[0]}.`
            : `Archived ${processedCount} broker attention items.`
      }
    };
  },

  async reconciliationIssueAction({ fetch, request }) {
    const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
    const formData = await request.formData();
    const issueId = readOptionalField(formData, 'issue_id');
    const issueIds = parsePositiveIntegerIdsFromForm(formData, 'issue_ids');
    const operation = readOptionalField(formData, 'operation');

    const normalizedIssueIds =
      issueIds.length > 0
        ? issueIds
        : parsePositiveIntegerIds(issueId ?? '');
    if (normalizedIssueIds.length === 0) {
      return fail(400, {
        reconciliationIssueActionResult: {
          ok: false,
          message: 'Reconciliation issue action is missing a valid issue ID list.'
        }
      });
    }

    if (!operation) {
      return fail(400, {
        reconciliationIssueActionResult: {
          ok: false,
          message: 'Reconciliation issue action type was invalid.'
        }
      });
    }

    let processedCount = 0;
    for (const normalizedIssueId of normalizedIssueIds) {
      const result = await postJson(
        fetch,
        `${apiBaseUrl}/v1/reconciliation-issues/${normalizedIssueId}/review`,
        {
          action: operation,
          updated_by: 'dashboard'
        }
      );

      if (!result.ok) {
        return fail(result.status || 500, {
          reconciliationIssueActionResult: {
            ok: false,
            message:
              `Archived ${processedCount} reconciliation issues before failing: ${result.error}`
          }
        });
      }
      processedCount += 1;
    }

    return {
      reconciliationIssueActionResult: {
        ok: true,
        message:
          processedCount === 1
            ? `Archived reconciliation issue ${normalizedIssueIds[0]}.`
            : `Archived ${processedCount} reconciliation issues.`
      }
    };
  }
};
