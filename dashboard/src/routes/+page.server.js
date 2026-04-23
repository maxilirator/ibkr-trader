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

function operatorSnapshotUrl(apiBaseUrl) {
  return (
    `${apiBaseUrl}/v1/read/operator-snapshot` +
    '?instruction_limit=50&order_limit=50&fill_limit=50&attention_limit=20&reconciliation_run_limit=12'
  );
}

function defaultOperatorSnapshot() {
  return {
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
}

async function readOperatorSnapshot(fetch, apiBaseUrl) {
  const result = await readJson(fetch, operatorSnapshotUrl(apiBaseUrl));
  if (!result.ok) {
    return {
      ok: false,
      status: result.status,
      error: result.error,
      snapshot: defaultOperatorSnapshot()
    };
  }

  return {
    ok: true,
    status: result.status,
    error: null,
    snapshot: result.body?.operator_snapshot ?? defaultOperatorSnapshot()
  };
}

export async function load({ fetch }) {
  const apiBaseUrl = normalizeBaseUrl(env.IBKR_TRADER_API_BASE_URL);
  const healthUrl = `${apiBaseUrl}/healthz`;

  const [health, operatorSnapshot] = await Promise.all([
    readJson(fetch, healthUrl),
    readJson(fetch, operatorSnapshotUrl(apiBaseUrl))
  ]);

  const operatorSnapshotBody = operatorSnapshot.body?.operator_snapshot ?? defaultOperatorSnapshot();

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
            action: 'ACKNOWLEDGE',
            updated_by: 'dashboard'
          }
        );
        if (!result.ok) {
          return fail(result.status || 500, {
            reconciliationClearResult: {
              ok: false,
              message:
                `Cleared ${acknowledgedReconciliationIssueCount} reconciliation issues before failing: ` +
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
              ? 'No visible reconciliation issues needed clearing.'
              : `Cleared ${acknowledgedReconciliationIssueCount} visible reconciliation issues.`
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

    const reconciliationRuns = snapshotResult.snapshot.recent_reconciliation_runs ?? [];
    const openReconciliationIssues = reconciliationRuns.flatMap((run) =>
      (run.issues ?? []).filter((issue) => (issue.operator_review?.status ?? 'OPEN') === 'OPEN')
    );

    let acknowledgedReconciliationIssueCount = 0;
    for (const item of openReconciliationIssues) {
      const result = await postJson(
        fetch,
        `${apiBaseUrl}/v1/reconciliation-issues/${item.issue_id}/review`,
        {
          action: 'ACKNOWLEDGE',
          updated_by: 'dashboard'
        }
      );
      if (!result.ok) {
        return fail(result.status || 500, {
          reconciliationClearResult: {
            ok: false,
            message:
              `Cleared ${acknowledgedReconciliationIssueCount} reconciliation issues before failing: ` +
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
            ? 'No open reconciliation issues needed clearing.'
            : `Cleared ${acknowledgedReconciliationIssueCount} visible reconciliation issues.`
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
            action: 'ACKNOWLEDGE',
            updated_by: 'dashboard'
          }
        );
        if (!result.ok) {
          return fail(result.status || 500, {
            acknowledgeAllLogsResult: {
              ok: false,
              message:
                `Cleared ${acknowledgedBrokerAttentionCount} broker attention items and ` +
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
            action: 'ACKNOWLEDGE',
            updated_by: 'dashboard'
          }
        );
        if (!result.ok) {
          return fail(result.status || 500, {
            acknowledgeAllLogsResult: {
              ok: false,
              message:
                `Cleared ${acknowledgedBrokerAttentionCount} broker attention items and ` +
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
              ? 'No visible broker-attention or reconciliation-log items needed clearing.'
              : `Cleared ${acknowledgedBrokerAttentionCount} broker attention items and ` +
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

    const brokerAttention = snapshotResult.snapshot.recent_broker_attention ?? [];
    const reconciliationRuns = snapshotResult.snapshot.recent_reconciliation_runs ?? [];
    const openBrokerAttention = brokerAttention.filter(
      (item) => (item.operator_review?.status ?? 'OPEN') === 'OPEN'
    );
    const openReconciliationIssues = reconciliationRuns.flatMap((run) =>
      (run.issues ?? []).filter((issue) => (issue.operator_review?.status ?? 'OPEN') === 'OPEN')
    );

    let acknowledgedBrokerAttentionCount = 0;
    let acknowledgedReconciliationIssueCount = 0;

    for (const item of openBrokerAttention) {
      const result = await postJson(
        fetch,
        `${apiBaseUrl}/v1/broker-attention/${item.event_id}/review`,
        {
          action: 'ACKNOWLEDGE',
          updated_by: 'dashboard'
        }
      );
      if (!result.ok) {
        return fail(result.status || 500, {
          acknowledgeAllLogsResult: {
            ok: false,
            message:
              `Cleared ${acknowledgedBrokerAttentionCount} broker attention items and ` +
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
          action: 'ACKNOWLEDGE',
          updated_by: 'dashboard'
        }
      );
      if (!result.ok) {
        return fail(result.status || 500, {
          acknowledgeAllLogsResult: {
            ok: false,
            message:
              `Cleared ${acknowledgedBrokerAttentionCount} broker attention items and ` +
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
            ? 'No open broker-attention or reconciliation-log items needed clearing.'
            : `Cleared ${acknowledgedBrokerAttentionCount} broker attention items and ` +
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
    return {
      startupReconcileResult: {
        ok: true,
        message:
          `Startup reconciliation completed with status ${runtimeResult?.status ?? 'unknown'}: ` +
          `${runtimeResult?.issue_count ?? 0} issues, ${runtimeResult?.action_count ?? 0} actions.`
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
              `Cleared ${processedCount} broker attention items before failing: ${result.error}`
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
            ? `Cleared broker attention item ${normalizedEventIds[0]}.`
            : `Cleared ${processedCount} broker attention items.`
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
              `Cleared ${processedCount} reconciliation issues before failing: ${result.error}`
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
            ? `Cleared reconciliation issue ${normalizedIssueIds[0]}.`
            : `Cleared ${processedCount} reconciliation issues.`
      }
    };
  }
};
