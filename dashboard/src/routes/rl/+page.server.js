import { fail } from '@sveltejs/kit';
import { env } from '$env/dynamic/private';
import {
  buildEndpointErrorMap,
  normalizeBaseUrl,
  postJson,
  readJson
} from '$lib/server/trader-api';

function defaultRLDashboard() {
  return {
    generated_at: null,
    summary: {
      model_count: 0,
      deployment_count: 0,
      live_deployment_count: 0,
      running_deployment_count: 0,
      stale_heartbeat_count: 0,
      recent_action_count: 0
    },
    models: [],
    deployments: [],
    recent_actions: []
  };
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
  return `${apiBaseUrl}/v1/read/rl-dashboard?model_limit=50&deployment_limit=50&action_limit=150`;
}

export async function load({ fetch }) {
  const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
  const healthUrl = `${apiBaseUrl}/healthz`;

  const [health, rlDashboard] = await Promise.all([
    readJson(fetch, healthUrl),
    readJson(fetch, rlDashboardUrl(apiBaseUrl))
  ]);

  return {
    generatedAt: new Date().toISOString(),
    apiBaseUrl,
    errors: buildEndpointErrorMap({
      health,
      rlDashboard
    }),
    health: health.body,
    rlDashboard: rlDashboard.body?.rl_dashboard ?? defaultRLDashboard(),
    recommendedShortActionSpace:
      rlDashboard.body?.recommended_short_action_space ?? []
  };
}

export const actions = {
  async registerModel({ fetch, request }) {
    const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
    const formData = await request.formData();

    let observationContract;
    let metadata;

    try {
      observationContract = parseJsonObject(
        readOptionalField(formData, 'observation_contract'),
        'observation_contract'
      );
      metadata = parseJsonObject(readOptionalField(formData, 'metadata'), 'metadata');
    } catch (error) {
      return fail(400, {
        registerModelResult: {
          ok: false,
          message: error instanceof Error ? error.message : String(error)
        }
      });
    }

    const payload = {
      model_key: readOptionalField(formData, 'model_key'),
      display_name: readOptionalField(formData, 'display_name'),
      strategy_family: readOptionalField(formData, 'strategy_family'),
      side: readOptionalField(formData, 'side') ?? 'SHORT',
      source_workflow_path: readOptionalField(formData, 'source_workflow_path'),
      promoted_checkpoint_path: readOptionalField(formData, 'promoted_checkpoint_path'),
      execution_mapping_version: readOptionalField(formData, 'execution_mapping_version'),
      action_space: parseTextList(readOptionalField(formData, 'action_space')),
      observation_contract: observationContract,
      metadata
    };

    const result = await postJson(fetch, `${apiBaseUrl}/v1/rl/models/register`, payload);
    if (!result.ok) {
      return fail(result.status || 500, {
        registerModelResult: {
          ok: false,
          message: result.error
        }
      });
    }

    return {
      registerModelResult: {
        ok: true,
        message: `Registered model ${result.body?.trader_model?.model_key ?? payload.model_key}.`
      }
    };
  },

  async createDeployment({ fetch, request }) {
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
        createDeploymentResult: {
          ok: false,
          message: error instanceof Error ? error.message : String(error)
        }
      });
    }

    const payload = {
      deployment_key: readOptionalField(formData, 'deployment_key'),
      model_key: readOptionalField(formData, 'model_key'),
      account_key: readOptionalField(formData, 'account_key'),
      book_key: readOptionalField(formData, 'book_key'),
      mode: readOptionalField(formData, 'mode') ?? 'paper',
      status: readOptionalField(formData, 'status') ?? 'draft',
      allowed_symbols: parseTextList(readOptionalField(formData, 'allowed_symbols'), {
        uppercase: true
      }),
      risk_limits: riskLimits,
      action_constraints: actionConstraints,
      metadata
    };

    const result = await postJson(fetch, `${apiBaseUrl}/v1/rl/deployments`, payload);
    if (!result.ok) {
      return fail(result.status || 500, {
        createDeploymentResult: {
          ok: false,
          message: result.error
        }
      });
    }

    return {
      createDeploymentResult: {
        ok: true,
        message:
          `Created deployment ${result.body?.trader_deployment?.deployment_key ?? payload.deployment_key}.`
      }
    };
  },

  async logAction({ fetch, request }) {
    const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
    const formData = await request.formData();

    let actionPayload;

    try {
      actionPayload = parseJsonObject(readOptionalField(formData, 'payload'), 'payload');
    } catch (error) {
      return fail(400, {
        logActionResult: {
          ok: false,
          message: error instanceof Error ? error.message : String(error)
        }
      });
    }

    const payload = {
      deployment_key: readOptionalField(formData, 'deployment_key'),
      symbol: readOptionalField(formData, 'symbol'),
      action_name: readOptionalField(formData, 'action_name'),
      observed_at: readOptionalField(formData, 'observed_at'),
      state_before: readOptionalField(formData, 'state_before'),
      state_after: readOptionalField(formData, 'state_after'),
      action_status: readOptionalField(formData, 'action_status') ?? 'logged',
      instruction_id: readOptionalField(formData, 'instruction_id'),
      note: readOptionalField(formData, 'note'),
      payload: actionPayload
    };

    const result = await postJson(fetch, `${apiBaseUrl}/v1/rl/actions/log`, payload);
    if (!result.ok) {
      return fail(result.status || 500, {
        logActionResult: {
          ok: false,
          message: result.error
        }
      });
    }

    return {
      logActionResult: {
        ok: true,
        message:
          `Logged ${result.body?.trader_action?.action_name ?? payload.action_name} for ${result.body?.trader_action?.symbol ?? payload.symbol}.`
      }
    };
  },

  async updateHeartbeat({ fetch, request }) {
    const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
    const formData = await request.formData();

    let metrics;

    try {
      metrics = parseJsonObject(readOptionalField(formData, 'metrics'), 'metrics');
    } catch (error) {
      return fail(400, {
        updateHeartbeatResult: {
          ok: false,
          message: error instanceof Error ? error.message : String(error)
        }
      });
    }

    const deploymentKey = readOptionalField(formData, 'deployment_key');
    const payload = {
      status: readOptionalField(formData, 'status'),
      last_seen_at: readOptionalField(formData, 'last_seen_at'),
      last_bar_at: readOptionalField(formData, 'last_bar_at'),
      last_action_at: readOptionalField(formData, 'last_action_at'),
      runtime_error: readOptionalField(formData, 'runtime_error'),
      metrics
    };

    const result = await postJson(
      fetch,
      `${apiBaseUrl}/v1/rl/deployments/${deploymentKey}/heartbeat`,
      payload
    );
    if (!result.ok) {
      return fail(result.status || 500, {
        updateHeartbeatResult: {
          ok: false,
          message: result.error
        }
      });
    }

    return {
      updateHeartbeatResult: {
        ok: true,
        message:
          `Updated heartbeat for ${result.body?.trader_heartbeat?.deployment_key ?? deploymentKey}.`
      }
    };
  }
};
