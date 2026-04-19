import { fail } from '@sveltejs/kit';
import { env } from '$env/dynamic/private';
import {
  buildEndpointErrorMap,
  normalizeBaseUrl,
  postJson,
  postWithoutBody,
  readJson
} from '$lib/server/trader-api';

function readOptionalField(formData, fieldName) {
  const value = formData.get(fieldName);
  if (value === null) {
    return null;
  }
  const normalized = String(value).trim();
  return normalized || null;
}

function parseInstructionIds(rawValue) {
  if (!rawValue) {
    return null;
  }

  const instructionIds = rawValue
    .split(/[\s,]+/)
    .map((value) => value.trim())
    .filter(Boolean);

  return instructionIds.length > 0 ? instructionIds : null;
}

export async function load({ fetch }) {
  const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
  const operatorSnapshotUrl =
    `${apiBaseUrl}/v1/read/operator-snapshot` +
    '?instruction_limit=50&order_limit=50&fill_limit=50&attention_limit=20&reconciliation_run_limit=12';
  const healthUrl = `${apiBaseUrl}/healthz`;

  const [health, operatorSnapshot] = await Promise.all([
    readJson(fetch, healthUrl),
    readJson(fetch, operatorSnapshotUrl)
  ]);

  const operatorSnapshotBody = operatorSnapshot.body?.operator_snapshot ?? {
    generated_at: null,
    kill_switch: {
      enabled: false,
      reason: null,
      updated_by: null,
      last_changed_at: null,
      latest_event_at: null
    },
    accounts: [],
    positions: [],
    open_orders: [],
    recent_fills: [],
    recent_broker_attention: [],
    recent_reconciliation_runs: [],
    instructions: []
  };

  return {
    generatedAt: new Date().toISOString(),
    apiBaseUrl,
    errors: buildEndpointErrorMap({
      health,
      operatorSnapshot
    }),
    health: health.body,
    operatorSnapshot: operatorSnapshotBody
  };
}

export const actions = {
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

  async cancelInstructionSet({ fetch, request }) {
    const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
    const formData = await request.formData();
    const batchId = readOptionalField(formData, 'batch_id');
    const accountKey = readOptionalField(formData, 'account_key');
    const bookKey = readOptionalField(formData, 'book_key');
    const reason = readOptionalField(formData, 'reason');
    const instructionIds = parseInstructionIds(
      readOptionalField(formData, 'instruction_ids')
    );

    if (!batchId && !accountKey && !bookKey && !instructionIds) {
      return fail(400, {
        cancelSetResult: {
          ok: false,
          message:
            'Provide at least one selector: batch ID, account key, book key, or instruction IDs.'
        }
      });
    }

    const result = await postJson(fetch, `${apiBaseUrl}/v1/instructions/cancel-set`, {
      requested_by: 'dashboard',
      reason,
      batch_id: batchId,
      account_key: accountKey,
      book_key: bookKey,
      instruction_ids: instructionIds
    });

    if (!result.ok) {
      return fail(result.status || 500, {
        cancelSetResult: {
          ok: false,
          message: result.error
        }
      });
    }

    const cancellation = result.body?.cancelled_instruction_set;
    return {
      cancelSetResult: {
        ok: true,
        message:
          `Cancellation request completed: ${cancellation?.matched_instruction_count ?? 0} matched, ` +
          `${cancellation?.cancelled_pending_count ?? 0} pending cancelled, ` +
          `${cancellation?.cancelled_submitted_count ?? 0} submitted cancelled, ` +
          `${cancellation?.skipped_count ?? 0} skipped.`
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
      submit_entry: {
        execute: () =>
          postWithoutBody(fetch, `${apiBaseUrl}/v1/instructions/${instructionId}/submit-entry`),
        successMessage: `Submitted entry for instruction ${instructionId}.`
      },
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
  }
};
