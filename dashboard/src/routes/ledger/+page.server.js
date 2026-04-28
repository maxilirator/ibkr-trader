import { error } from '@sveltejs/kit';
import { env } from '$env/dynamic/private';
import {
  buildFallbackHealth,
  buildEndpointErrorMap,
  normalizeBaseUrl,
  readJson
} from '$lib/server/trader-api';

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

export async function load({ fetch, url }) {
  const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
  const focusInstructionId = url.searchParams.get('instruction_id')?.trim() || null;
  const params = new URLSearchParams({
    instruction_event_limit: '80',
    order_event_limit: '80',
    fill_limit: '60',
    control_event_limit: '30',
    cancellation_limit: '30',
    reconciliation_issue_limit: '40'
  });

  if (focusInstructionId) {
    params.set('focus_instruction_id', focusInstructionId);
  }

  const ledgerSnapshotUrl = `${apiBaseUrl}/v1/read/ledger-snapshot?${params.toString()}`;
  const healthUrl = `${apiBaseUrl}/healthz`;

  const [health, ledgerSnapshot] = await Promise.all([
    readJson(fetch, healthUrl, { timeoutMs: 2500 }),
    readJson(fetch, ledgerSnapshotUrl)
  ]);

  return {
    generatedAt: new Date().toISOString(),
    apiBaseUrl,
    focusInstructionId,
    errors: buildEndpointErrorMap({
      health,
      ledgerSnapshot
    }),
    health: health.ok
      ? requireResponseBody(health, 'healthz')
      : buildFallbackHealth(apiBaseUrl, health.error),
    ledgerSnapshot: requireBodyField(ledgerSnapshot, 'ledger_snapshot', 'Ledger snapshot')
  };
}
