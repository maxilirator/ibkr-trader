import { env } from '$env/dynamic/private';
import {
  buildEndpointErrorMap,
  normalizeBaseUrl,
  readJson
} from '$lib/server/trader-api';

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
    readJson(fetch, healthUrl),
    readJson(fetch, ledgerSnapshotUrl)
  ]);

  const ledgerSnapshotBody = ledgerSnapshot.body?.ledger_snapshot ?? {
    generated_at: null,
    focus_instruction: null,
    summary: {
      instruction_count: 0,
      instruction_event_count: 0,
      broker_order_count: 0,
      broker_order_event_count: 0,
      execution_fill_count: 0,
      control_event_count: 0,
      instruction_set_cancellation_count: 0,
      reconciliation_issue_count: 0
    },
    instruction_events: [],
    broker_order_events: [],
    recent_fills: [],
    control_events: [],
    instruction_set_cancellations: [],
    reconciliation_issues: []
  };

  return {
    generatedAt: new Date().toISOString(),
    apiBaseUrl,
    focusInstructionId,
    errors: buildEndpointErrorMap({
      health,
      ledgerSnapshot
    }),
    health: health.body,
    ledgerSnapshot: ledgerSnapshotBody
  };
}
