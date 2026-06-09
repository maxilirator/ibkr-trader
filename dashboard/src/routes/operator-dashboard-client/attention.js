import { summarizeRefs, uniqueIds } from './filters.js';
import { parseTimestamp } from './status.js';

export function isOpenReview(review) {
  return review.status === 'OPEN';
}

export function brokerAttentionEventLabel(eventType) {
  if (eventType === 'broker_warning') return 'Broker warning';
  if (eventType === 'order_error_callback') return 'Order error';
  return String(eventType ?? '').replaceAll('_', ' ');
}

export function groupBrokerAttentionRows(rows) {
  const groupedRows = new Map();

  for (const row of rows) {
    if (!isOpenReview(row.operator_review)) {
      continue;
    }

    const groupKey = [
      row.account_key,
      row.symbol,
      row.event_type,
      row.message
    ].join('|');
    const currentGroup = groupedRows.get(groupKey) ?? {
      key: groupKey,
      accountKey: row.account_key,
      accountLabel: row.account_label,
      symbol: row.symbol,
      eventType: brokerAttentionEventLabel(row.event_type),
      message: row.message,
      latestAt: row.event_at,
      latestStatusAfter: row.status_after,
      eventIds: [],
      orderRefs: [],
      notes: [],
      count: 0
    };

    currentGroup.count += 1;
    currentGroup.eventIds.push(Number(row.event_id));
    if (row.order_ref) currentGroup.orderRefs.push(row.order_ref);
    if (row.note) currentGroup.notes.push(row.note);

    if (parseTimestamp(row.event_at)?.getTime() >= (parseTimestamp(currentGroup.latestAt)?.getTime() ?? 0)) {
      currentGroup.latestAt = row.event_at;
      currentGroup.latestStatusAfter = row.status_after;
      currentGroup.accountLabel = row.account_label ?? currentGroup.accountLabel;
    }

    groupedRows.set(groupKey, currentGroup);
  }

  return [...groupedRows.values()]
    .map((group) => ({
      ...group,
      eventIds: uniqueIds(group.eventIds),
      eventIdsCsv: uniqueIds(group.eventIds).join(','),
      orderRefSummary: summarizeRefs(group.orderRefs),
      noteSummary: summarizeRefs(group.notes)
    }))
    .sort((left, right) => {
      const leftAt = parseTimestamp(left.latestAt)?.getTime() ?? 0;
      const rightAt = parseTimestamp(right.latestAt)?.getTime() ?? 0;
      return rightAt - leftAt;
    });
}
export function reconciliationGroupMessageKey(stage, message) {
  const normalized = String(message ?? '').toLowerCase();
  if (stage === 'broker_snapshot') {
    if (normalized.includes('nextvalidid') || normalized.includes('api startup')) {
      return 'broker api startup did not complete';
    }
    if (normalized.includes('cooling down')) {
      return 'broker api connection cooling down';
    }
    if (normalized.includes('timed out') || normalized.includes('timeout')) {
      return 'broker api snapshot timed out';
    }
    if (normalized.includes('connection refused') || normalized.includes('socket')) {
      return 'broker api socket unavailable';
    }
  }
  return String(message ?? '');
}

export function groupReconciliationRuns(runs) {
  const groupedRows = new Map();

  for (const run of runs) {
    for (const issue of run.issues) {
      if (!isOpenReview(issue.operator_review)) {
        continue;
      }

      const messageKey = reconciliationGroupMessageKey(issue.stage, issue.message);
      const groupKey = [
        run.run_kind,
        issue.stage,
        issue.severity,
        issue.instruction_id ?? '',
        messageKey
      ].join('|');

      const currentGroup = groupedRows.get(groupKey) ?? {
        key: groupKey,
        runKind: run.run_kind,
        stage: issue.stage,
        severity: issue.severity,
        instructionId: issue.instruction_id,
        message: issue.message,
        latestAt: issue.observed_at,
        issueIds: [],
        runIds: [],
        runStatuses: [],
        runCompletedAts: [],
        suppressedCount: 0,
        count: 0
      };

      const suppressedRepeats = Number(run.metadata_json?.suppressed_reconciliation_repeats ?? 0);
      currentGroup.count += 1;
      if (Number.isFinite(suppressedRepeats) && suppressedRepeats > currentGroup.suppressedCount) {
        currentGroup.suppressedCount = suppressedRepeats;
      }
      currentGroup.issueIds.push(Number(issue.issue_id));
      currentGroup.runIds.push(Number(run.run_id));
      currentGroup.runStatuses.push(run.status);
      currentGroup.runCompletedAts.push(run.completed_at);

      const latestObservedAt = parseTimestamp(issue.observed_at)?.getTime() ?? 0;
      const latestRunAt = parseTimestamp(run.completed_at)?.getTime() ?? 0;
      const effectiveLatestAt = Math.max(latestObservedAt, latestRunAt);
      if (effectiveLatestAt >= (parseTimestamp(currentGroup.latestAt)?.getTime() ?? 0)) {
        currentGroup.latestAt = new Date(effectiveLatestAt).toISOString();
        currentGroup.message = issue.message;
      }

      groupedRows.set(groupKey, currentGroup);
    }
  }

  return [...groupedRows.values()]
    .map((group) => ({
      ...group,
      issueIds: uniqueIds(group.issueIds),
      issueIdsCsv: uniqueIds(group.issueIds).join(','),
      runCount: uniqueIds(group.runIds).length,
      latestCompletedAt: group.runCompletedAts
        .map((value) => parseTimestamp(value))
        .filter(Boolean)
        .sort((left, right) => right.getTime() - left.getTime())[0]
        ?.toISOString() ?? null
    }))
    .sort((left, right) => {
      const leftAt = parseTimestamp(left.latestAt)?.getTime() ?? 0;
      const rightAt = parseTimestamp(right.latestAt)?.getTime() ?? 0;
      return rightAt - leftAt;
    });
}
