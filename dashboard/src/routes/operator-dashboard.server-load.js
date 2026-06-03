import { env } from '$env/dynamic/private';
import {
  buildFallbackHealth,
  buildEndpointErrorMap,
  normalizeBaseUrl,
  passiveHealthUrl,
  readJson
} from '$lib/server/trader-api';
import {
  operatorSnapshotUrl,
  readOmxBenchmark,
  requireBodyField,
  requireResponseBody
} from './operator-dashboard.server-data.js';

export async function load({ fetch }) {
  const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
  const healthUrl = passiveHealthUrl(apiBaseUrl);

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
