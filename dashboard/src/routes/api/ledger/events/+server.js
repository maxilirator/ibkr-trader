import { env } from '$env/dynamic/private';
import {
  buildEndpointErrorMap,
  buildFallbackHealth,
  normalizeBaseUrl,
  readJson
} from '$lib/server/trader-api';
import { createSnapshotEventResponse } from '$lib/server/snapshot-events';

function requireBodyField(result, fieldName) {
  if (!result.ok || !result.body || result.body[fieldName] === undefined || result.body[fieldName] === null) {
    throw new Error(result.error ?? `response missing ${fieldName}`);
  }
  return result.body[fieldName];
}

function ledgerSnapshotUrl(apiBaseUrl, url) {
  const params = new URLSearchParams({
    instruction_event_limit: '80',
    order_event_limit: '80',
    fill_limit: '60',
    control_event_limit: '30',
    cancellation_limit: '30',
    reconciliation_issue_limit: '40'
  });
  const focusInstructionId = url.searchParams.get('instruction_id')?.trim();
  if (focusInstructionId) {
    params.set('focus_instruction_id', focusInstructionId);
  }
  return `${apiBaseUrl}/v1/read/ledger-snapshot?${params.toString()}`;
}

export function GET({ fetch, request, url }) {
  const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);

  return createSnapshotEventResponse({
    request,
    url,
    eventName: 'ledger',
    async loadSnapshot() {
      const [health, ledgerSnapshot] = await Promise.all([
        readJson(fetch, `${apiBaseUrl}/healthz`, { timeoutMs: 2500 }),
        readJson(fetch, ledgerSnapshotUrl(apiBaseUrl, url))
      ]);

      return {
        apiBaseUrl,
        errors: buildEndpointErrorMap({ health, ledgerSnapshot }),
        health: health.ok ? health.body : buildFallbackHealth(apiBaseUrl, health.error),
        ledgerSnapshot: requireBodyField(ledgerSnapshot, 'ledger_snapshot')
      };
    }
  });
}
