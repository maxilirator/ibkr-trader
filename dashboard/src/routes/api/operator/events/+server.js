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

export function GET({ fetch, request, url }) {
  const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);

  return createSnapshotEventResponse({
    request,
    url,
    eventName: 'operator',
    async loadSnapshot() {
      const [health, operatorSnapshot] = await Promise.all([
        readJson(fetch, `${apiBaseUrl}/healthz`, { timeoutMs: 2500 }),
        readJson(fetch, `${apiBaseUrl}/v1/read/operator-snapshot`)
      ]);

      return {
        apiBaseUrl,
        errors: buildEndpointErrorMap({ health, operatorSnapshot }),
        health: health.ok ? health.body : buildFallbackHealth(apiBaseUrl, health.error),
        operatorSnapshot: requireBodyField(operatorSnapshot, 'operator_snapshot')
      };
    }
  });
}
