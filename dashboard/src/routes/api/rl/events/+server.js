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

function rlDashboardUrl(apiBaseUrl) {
  return (
    `${apiBaseUrl}/v1/read/rl-dashboard` +
    '?model_limit=50&deployment_limit=50&action_limit=150&candidate_limit=80'
  );
}

export function GET({ fetch, request, url }) {
  const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);

  return createSnapshotEventResponse({
    request,
    url,
    eventName: 'rl',
    async loadSnapshot() {
      const [health, rlDashboard] = await Promise.all([
        readJson(fetch, `${apiBaseUrl}/healthz`, { timeoutMs: 2500 }),
        readJson(fetch, rlDashboardUrl(apiBaseUrl))
      ]);

      return {
        apiBaseUrl,
        errors: buildEndpointErrorMap({ health, rlDashboard }),
        health: health.ok ? health.body : buildFallbackHealth(apiBaseUrl, health.error),
        rlDashboard: requireBodyField(rlDashboard, 'rl_dashboard')
      };
    }
  });
}
