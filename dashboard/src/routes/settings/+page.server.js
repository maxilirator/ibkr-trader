import { error } from '@sveltejs/kit';
import { env } from '$env/dynamic/private';
import { normalizeBaseUrl, readJson } from '$lib/server/trader-api';

/**
 * Load the read-only runtime settings registry.
 *
 * The page shows what the running API actually resolved, so a failed read is
 * surfaced as an error rather than rendered as an empty table. An empty table
 * would read as "no settings configured", which is a different and misleading
 * statement.
 */
export async function load({ fetch }) {
  const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
  const result = await readJson(fetch, `${apiBaseUrl}/v1/settings`, {
    timeoutMs: 10000
  });

  if (!result.ok) {
    throw error(result.status || 502, `Settings registry read failed: ${result.error}`);
  }
  if (!result.body || !Array.isArray(result.body.settings)) {
    throw error(502, 'Settings registry response did not contain a settings list');
  }

  return {
    settings: result.body.settings,
    categories: Array.isArray(result.body.categories) ? result.body.categories : [],
    errorCount: Number(result.body.error_count ?? 0),
    undeclaredKeys: Array.isArray(result.body.undeclared_keys)
      ? result.body.undeclared_keys
      : [],
    readOnly: result.body.read_only !== false
  };
}
