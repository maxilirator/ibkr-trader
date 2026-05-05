import { error, fail } from '@sveltejs/kit';
import { env } from '$env/dynamic/private';
import {
  buildFallbackHealth,
  buildEndpointErrorMap,
  normalizeBaseUrl,
  patchJson,
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

function requireActionField(result, fieldName, endpointName, resultKey) {
  if (!result.ok) {
    return fail(result.status || 500, {
      [resultKey]: {
        ok: false,
        message: result.error
      }
    });
  }
  if (!result.body || result.body[fieldName] === undefined || result.body[fieldName] === null) {
    return fail(502, {
      [resultKey]: {
        ok: false,
        message: `${endpointName} response missing ${fieldName}`
      }
    });
  }
  return result.body[fieldName];
}

function readOptionalField(formData, fieldName) {
  const value = formData.get(fieldName);
  if (value === null) {
    return null;
  }
  const normalized = String(value).trim();
  return normalized || null;
}

function parseTextList(rawValue, { uppercase = false } = {}) {
  if (!rawValue) {
    return [];
  }

  const values = String(rawValue)
    .split(/[\s,]+/)
    .map((value) => value.trim())
    .filter(Boolean)
    .map((value) => (uppercase ? value.toUpperCase() : value));

  return [...new Set(values)];
}

function parseJsonObject(rawValue, fieldName) {
  if (!rawValue) {
    return {};
  }

  try {
    const parsed = JSON.parse(rawValue);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error(`${fieldName} must be a JSON object`);
    }
    return parsed;
  } catch (error) {
    throw new Error(error instanceof Error ? error.message : String(error));
  }
}

function rlDashboardUrl(apiBaseUrl) {
  return (
    `${apiBaseUrl}/v1/read/rl-dashboard` +
    '?model_limit=50&deployment_limit=50&action_limit=150&candidate_limit=80'
  );
}

export async function load({ fetch }) {
  const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
  const healthUrl = `${apiBaseUrl}/healthz`;

  const [health, rlDashboard] = await Promise.all([
    readJson(fetch, healthUrl, { timeoutMs: 2500 }),
    readJson(fetch, rlDashboardUrl(apiBaseUrl))
  ]);

  return {
    generatedAt: new Date().toISOString(),
    apiBaseUrl,
    errors: buildEndpointErrorMap({
      health,
      rlDashboard
    }),
    health: health.ok
      ? requireResponseBody(health, 'healthz')
      : buildFallbackHealth(apiBaseUrl, health.error),
    rlDashboard: requireBodyField(rlDashboard, 'rl_dashboard', 'RL dashboard')
  };
}

export const actions = {
  async updateDeployment({ fetch, request }) {
    const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
    const formData = await request.formData();

    let riskLimits;
    let actionConstraints;
    let metadata;

    try {
      riskLimits = parseJsonObject(readOptionalField(formData, 'risk_limits'), 'risk_limits');
      actionConstraints = parseJsonObject(
        readOptionalField(formData, 'action_constraints'),
        'action_constraints'
      );
      metadata = parseJsonObject(readOptionalField(formData, 'metadata'), 'metadata');
    } catch (error) {
      return fail(400, {
        updateDeploymentResult: {
          ok: false,
          message: error instanceof Error ? error.message : String(error)
        }
      });
    }

    const deploymentKey = readOptionalField(formData, 'deployment_key');
    const payload = {
      status: readOptionalField(formData, 'status'),
      allowed_symbols: parseTextList(readOptionalField(formData, 'allowed_symbols'), {
        uppercase: true
      }),
      risk_limits: riskLimits,
      action_constraints: actionConstraints,
      metadata
    };

    const result = await patchJson(
      fetch,
      `${apiBaseUrl}/v1/rl/deployments/${deploymentKey}`,
      payload
    );
    if (!result.ok) {
      return fail(result.status || 500, {
        updateDeploymentResult: {
          ok: false,
          message: result.error
        }
      });
    }
    const traderDeployment = requireActionField(
      result,
      'trader_deployment',
      'Update deployment',
      'updateDeploymentResult'
    );
    if (traderDeployment?.deployment_key === undefined) {
      return fail(502, {
        updateDeploymentResult: {
          ok: false,
          message: 'Update deployment response missing trader_deployment.deployment_key'
        }
      });
    }

    return {
      updateDeploymentResult: {
        ok: true,
        message: `Updated deployment ${traderDeployment.deployment_key}.`
      }
    };
  }
};
