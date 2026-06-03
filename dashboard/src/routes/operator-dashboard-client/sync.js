import {
  accounts,
  brokerMonitor,
  executionInstructions,
  omxBenchmark,
  openOrders,
  positions,
  recentFills,
  rlCandidateInstructions
} from './view-state.js';
import { positionOwningInstructionStates, normalizedStatus } from './instructions.js';
import { formatReturnPct, parseFiniteNumber } from './formatting.js';
import {
  compactCount,
  freshnessClass,
  instrumentKeys,
  latestTimestamp
} from './status.js';

export function buildStateSyncSummary() {
  const snapshotRefresh = brokerMonitor?.snapshot_refresh ?? {};
  const brokerSnapshotAt = snapshotRefresh.captured_at ?? snapshotRefresh.last_success_at ?? null;
  const brokerSnapshotHealthy = snapshotRefresh.ok === true && snapshotRefresh.is_stale !== true;
  const livePositions = positions.filter((position) => !position.is_virtual);
  const liveOpenOrders = openOrders.filter((order) => !order.is_virtual);
  const positionKeys = new Set(positions.flatMap((position) => instrumentKeys(position)));
  const visibleInstructionIds = new Set(executionInstructions.map((instruction) => instruction.record_id));
  const activePositionInstructions = executionInstructions.filter((instruction) =>
    positionOwningInstructionStates.has(instruction.state)
  );
  const instructionsWithoutPosition = activePositionInstructions.filter((instruction) => {
    const keys = instrumentKeys(instruction);
    return keys.length > 0 && !keys.some((key) => positionKeys.has(key));
  });
  const fillInstructionIds = new Set(
    recentFills.map((fill) => fill.instruction_record_id).filter(Boolean)
  );
  const brokerFilledInstructionsWithPartialLocalEvidence = executionInstructions.filter(
    (instruction) =>
      normalizedStatus(instruction.broker_order_status) === 'FILLED' &&
      ((parseFiniteNumber(instruction.entry_filled_quantity) ?? 0) <= 0 ||
        !fillInstructionIds.has(instruction.record_id))
  );
  const openOrdersWithoutVisibleInstruction = openOrders.filter(
    (order) => order.instruction_record_id && !visibleInstructionIds.has(order.instruction_record_id)
  );
  const brokerOpenOrderCount = Number(snapshotRefresh.open_order_count ?? 0);
  const brokerPositionCount = Number(snapshotRefresh.position_count ?? 0);
  const countMismatchWarnings = [];

  if (!brokerSnapshotHealthy) {
    countMismatchWarnings.push({
      className: 'bad',
      text:
        snapshotRefresh.error ??
        'Broker snapshot refresh is stale or has not completed, so holdings and open-order counts may lag broker state.'
    });
  }

  if (brokerSnapshotHealthy && brokerOpenOrderCount !== liveOpenOrders.length) {
    countMismatchWarnings.push({
      className: 'warn',
      text: `Broker snapshot reports ${brokerOpenOrderCount} live open orders, while durable live open-order rows show ${liveOpenOrders.length}.`
    });
  }

  if (brokerSnapshotHealthy && brokerPositionCount !== livePositions.length) {
    countMismatchWarnings.push({
      className: 'warn',
      text: `Broker snapshot reports ${brokerPositionCount} live positions, while durable live holding rows show ${livePositions.length}.`
    });
  }

  if (instructionsWithoutPosition.length > 0) {
    countMismatchWarnings.push({
      className: 'warn',
      text: `${instructionsWithoutPosition.length} active position instruction(s) are visible without a matching holding snapshot.`
    });
  }

  if (brokerFilledInstructionsWithPartialLocalEvidence.length > 0) {
    countMismatchWarnings.push({
      className: 'warn',
      text: `${brokerFilledInstructionsWithPartialLocalEvidence.length} broker-filled entry instruction(s) lack complete local fill evidence; order-status fallback may be ahead of Recent Fills.`
    });
  }

  if (openOrdersWithoutVisibleInstruction.length > 0) {
    countMismatchWarnings.push({
      className: 'warn',
      text: `${openOrdersWithoutVisibleInstruction.length} open order(s) are linked to instructions outside the visible instruction slice.`
    });
  }

  if (omxBenchmark?.status !== 'ok') {
    countMismatchWarnings.push({
      className: 'warn',
      text: `OMX benchmark is ${omxBenchmark?.status ?? 'unavailable'}; account charts will show the account line without a trusted index comparison.`
    });
  }

  const latestAccountsAt = latestTimestamp(accounts, 'snapshot_at');
  const latestPositionsAt = latestTimestamp(positions, 'snapshot_at');
  const latestOrdersAt = latestTimestamp(openOrders, ['last_status_at', 'submitted_at']);
  const latestFillsAt = latestTimestamp(recentFills, 'executed_at');
  const latestInstructionsAt = latestTimestamp(executionInstructions, ['activity_at', 'updated_at']);
  const latestCandidatesAt = latestTimestamp(rlCandidateInstructions, ['activity_at', 'updated_at']);
  const latestOmxAt = latestTimestamp(omxBenchmark?.points, 'timestamp');

  return {
    className:
      countMismatchWarnings.some((warning) => warning.className === 'bad')
        ? 'bad'
        : countMismatchWarnings.length > 0
          ? 'warn'
          : 'ok',
    label:
      countMismatchWarnings.some((warning) => warning.className === 'bad')
        ? 'Needs attention'
        : countMismatchWarnings.length > 0
          ? 'Check sync'
          : 'In sync',
    warnings: countMismatchWarnings,
    items: [
      {
        label: 'Broker Snapshot',
        countLabel: `${compactCount(snapshotRefresh.position_count)} live positions · ${compactCount(snapshotRefresh.open_order_count)} live orders`,
        at: brokerSnapshotAt,
        className: brokerSnapshotHealthy ? freshnessClass(brokerSnapshotAt, 180) : 'bad',
        source: 'IBKR monitor'
      },
      {
        label: 'Accounts',
        countLabel: `${accounts.length} rows`,
        at: latestAccountsAt,
        className: freshnessClass(latestAccountsAt, 180),
        source: 'account snapshots'
      },
      {
        label: 'Holdings',
        countLabel: `${positions.length} rows · ${livePositions.length} live`,
        at: latestPositionsAt,
        className: freshnessClass(latestPositionsAt, 180),
        source: 'position snapshots'
      },
      {
        label: 'Open Orders',
        countLabel: `${openOrders.length} rows · ${liveOpenOrders.length} live`,
        at: latestOrdersAt,
        className: freshnessClass(latestOrdersAt, 180),
        source: 'broker-order ledger'
      },
      {
        label: 'Fills',
        countLabel: `${recentFills.length} rows`,
        at: latestFillsAt,
        className: recentFills.length === 0 ? 'neutral' : freshnessClass(latestFillsAt, 3600),
        source: 'execution fills'
      },
      {
        label: 'Instructions',
        countLabel: `${executionInstructions.length} rows`,
        at: latestInstructionsAt,
        className: executionInstructions.length === 0 ? 'neutral' : freshnessClass(latestInstructionsAt, 300),
        source: 'runtime queue'
      },
      {
        label: 'RL Candidates',
        countLabel: `${rlCandidateInstructions.length} active source rows`,
        at: latestCandidatesAt,
        className: rlCandidateInstructions.length === 0 ? 'neutral' : 'ok',
        source: 'daily model-routed list'
      },
      {
        label: omxBenchmark?.label ?? 'OMX',
        countLabel: formatReturnPct(omxBenchmark?.latest_return_pct),
        at: latestOmxAt,
        className: omxBenchmark?.status === 'ok' ? freshnessClass(latestOmxAt, 300) : 'warn',
        source: omxBenchmark?.symbol ?? 'benchmark stream'
      }
    ]
  };
}

