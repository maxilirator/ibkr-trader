import { executionInstructions, openOrders, referenceNow } from './view-state.js';
import {
  formatTimestamp,
  latestTimestamp,
  normalizedSymbol,
  parseTimestamp
} from './status.js';
import {
  displayOrderPrice,
  formatPrice,
  formatQuantity,
  parseFiniteNumber
} from './formatting.js';
import { orderSpreadLabel } from './reviews.js';
import { summarizeRefs } from './filters.js';

const terminalInstructionStates = new Set(['ENTRY_CANCELLED', 'NEEDS_REVIEW', 'COMPLETED', 'FAILED']);
export const entryOwningInstructionStates = new Set(['ENTRY_PENDING', 'ENTRY_SUBMITTED']);
export const positionOwningInstructionStates = new Set(['POSITION_OPEN', 'EXIT_PENDING']);
const closedOrderStatuses = new Set([
'API_CANCELLED',
'CANCELLED',
'ERROR',
'FILLED',
'INACTIVE',
'NOT_FOUND_AT_BROKER',
'REJECTED'
]);

export function instructionWindowState(instruction) {
  const submitAt = parseTimestamp(instruction.submit_at);
  const expireAt = parseTimestamp(instruction.expire_at);
  const state = instruction.state ?? 'UNKNOWN';

  if (state === 'EXIT_PENDING') {
    const exitPlanState = instructionExitPlanState(instruction);
    if (exitPlanState) {
      return exitPlanState;
    }
    return {
      label: 'Exit Active',
      className: 'ok',
      detail: expireAt
        ? `Entry window closed ${formatTimestamp(instruction.expire_at)}; exit workflow is still active.`
        : 'Exit workflow is still active.',
      isScheduled: false,
      isOpen: true,
      isExpired: false
    };
  }

  if (state === 'POSITION_OPEN') {
    const exitPlanState = instructionExitPlanState(instruction);
    if (exitPlanState) {
      return exitPlanState;
    }
    return {
      label: 'Position Open',
      className: 'ok',
      detail: expireAt
        ? `Entry window closed ${formatTimestamp(instruction.expire_at)}; runtime still owns the position.`
        : 'Runtime still owns the position.',
      isScheduled: false,
      isOpen: true,
      isExpired: false
    };
  }

  if (state === 'COMPLETED') {
    return {
      label: 'Completed',
      className: 'neutral',
      detail: 'Instruction lifecycle completed.',
      isScheduled: false,
      isOpen: false,
      isExpired: false
    };
  }

  if (state === 'ENTRY_CANCELLED') {
    return {
      label: 'Cancelled',
      className: 'neutral',
      detail: 'Entry path cancelled and no longer active.',
      isScheduled: false,
      isOpen: false,
      isExpired: false
    };
  }

  if (state === 'NEEDS_REVIEW') {
    return {
      label: 'Needs Review',
      className: 'bad',
      detail: 'Broker intervention stopped the entry path. Review is required before any resubmission.',
      isScheduled: false,
      isOpen: false,
      isExpired: false
    };
  }

  if (state === 'FAILED') {
    return {
      label: 'Failed',
      className: 'bad',
      detail: 'Instruction requires ledger review.',
      isScheduled: false,
      isOpen: false,
      isExpired: false
    };
  }

  if (!submitAt || !expireAt) {
    return {
      label: 'Unknown',
      className: 'warn',
      detail: 'Schedule timestamps are unavailable.',
      isScheduled: false,
      isOpen: false,
      isExpired: false
    };
  }

  if (referenceNow < submitAt) {
    return {
      label: 'Scheduled',
      className: 'neutral',
      detail: `Opens ${formatTimestamp(instruction.submit_at)}`,
      isScheduled: true,
      isOpen: false,
      isExpired: false
    };
  }

  if (referenceNow >= expireAt) {
    return {
      label: 'Expired',
      className: 'bad',
      detail: `Expired ${formatTimestamp(instruction.expire_at)}`,
      isScheduled: false,
      isOpen: false,
      isExpired: true
    };
  }

  return {
    label: 'Open',
    className: 'ok',
    detail: `Closes ${formatTimestamp(instruction.expire_at)}`,
    isScheduled: false,
    isOpen: true,
    isExpired: false
  };
}

export function isRlCandidateInstruction(instruction) {
  return (
    instruction.state === 'MODEL_ROUTED_PENDING' ||
    instruction.order_type === 'MODEL_ROUTED' ||
    instruction.payload?.instruction?.execution?.mode === 'model_routed'
  );
}

export function rlCandidateModelId(instruction) {
  return (
    instruction.payload?.instruction?.execution?.model_id ??
    instruction.payload?.instruction?.model ??
    'n/a'
  );
}

export function rlCandidateWindowDisplay(instruction) {
  return `${formatTimestamp(instruction.submit_at)} to ${formatTimestamp(instruction.expire_at)}`;
}

export function instructionGuidance(instruction) {
  const windowState = instructionWindowState(instruction);
  const forceNextOpen = instructionForcesNextOpenExit(instruction);
  const nextOpenAt = instructionNextSessionOpenAt(instruction);
  const nextOpenPassed = forceNextOpen && nextOpenAt && referenceNow > nextOpenAt;
  const liveEntryOrder = liveEntryOrderForInstruction(instruction, openOrders);
  const liveExitOrder = liveMarketExitOrderForInstruction(instruction, openOrders);

  if (instruction.state === 'ENTRY_PENDING') {
    if (windowState.isScheduled) {
      return 'Waiting for the scheduled entry window to open. Runtime will submit it automatically when due.';
    }
    if (windowState.isExpired) {
      return 'The entry window already passed. This row now needs cancellation or ledger review.';
    }
    return 'The entry window is active. Runtime should submit it automatically without operator intervention.';
  }

  if (instruction.state === 'ENTRY_SUBMITTED') {
    if (liveEntryOrder) {
      return workingEntryGuidance(liveEntryOrder);
    }
    if (windowState.isExpired) {
      return 'The broker entry is past expiry. Runtime should cancel or reconcile it.';
    }
    return 'The broker entry is active. Cancel it if it should not stay working.';
  }

  if (instruction.state === 'POSITION_OPEN') {
    if (forceNextOpen) {
      if (liveExitOrder) {
        return 'Entry filled. A matching live market exit order is working at broker; runtime should not duplicate it.';
      }
      if (nextOpenPassed) {
        return 'Entry filled. The forced next-open timestamp has passed; verify that a live market exit order covers the position.';
      }
      return 'Entry filled. Next-session-open forced market exit is armed; runtime owns the close.';
    }
    return 'Entry filled. Runtime is now responsible for exit management.';
  }

  if (instruction.state === 'EXIT_PENDING') {
    if (forceNextOpen) {
      if (liveExitOrder) {
        return 'Exit workflow is active. A matching live market exit order is working at broker; runtime should not duplicate it.';
      }
      if (nextOpenPassed) {
        return 'Exit workflow is active. The forced next-open timestamp has passed; runtime treats it as due until a live market exit or completion is reconciled.';
      }
      return 'Exit workflow is active. Next-session-open forced market exit is armed even if an older protective exit row was cancelled.';
    }
    return 'Exit workflow is active and still awaiting completion.';
  }

  if (instruction.state === 'ENTRY_CANCELLED') {
    return 'The entry path was cancelled and will not submit again.';
  }

  if (instruction.state === 'NEEDS_REVIEW') {
    return 'The broker stopped this entry and flagged it for operator review. Verify the reason before any new submission.';
  }

  if (instruction.state === 'COMPLETED') {
    return 'This instruction has completed its lifecycle.';
  }

  if (instruction.state === 'FAILED') {
    return 'This instruction hit a failure and should be reviewed in the ledger.';
  }

  return 'Review the ledger before taking any manual action on this instruction.';
}

export function instructionPrimaryAction(instruction) {
  const windowState = instructionWindowState(instruction);

  if (instruction.state === 'ENTRY_PENDING') {
    return {
      operation: 'cancel_instruction',
      label: windowState.isExpired ? 'Cancel Stale' : 'Cancel Pending',
      className: 'inline-button danger'
    };
  }

  if (instruction.state === 'ENTRY_SUBMITTED' && instruction.broker_order_id) {
    return {
      operation: 'cancel_entry',
      label: windowState.isExpired ? 'Cancel Expired Entry' : 'Cancel Entry',
      className: 'inline-button danger'
    };
  }

  return null;
}

export function hasInstructionAction(instruction) {
  return !terminalInstructionStates.has(instruction.state);
}

export function instructionOrderDisplay(instruction, kind) {
  if (kind === 'entry') {
    return (
      instruction.entry_order_display ??
      `${instruction.broker_order_id ?? 'n/a'} / ${instruction.broker_order_status ?? 'n/a'}`
    );
  }

  return (
    instruction.exit_order_display ??
    `${instruction.exit_order_id ?? 'n/a'} / ${instruction.exit_order_status ?? 'n/a'}`
  );
}

export function normalizedStatus(value) {
  const normalized = String(value ?? '').trim().toUpperCase();
  return normalized || null;
}

export function isOpenBrokerOrder(order) {
  const status = normalizedStatus(order?.status);
  return !closedOrderStatuses.has(status);
}

export function isMarketOrder(order) {
  const orderType = normalizedStatus(order?.order_type);
  return orderType === 'MKT' || orderType === 'MARKET';
}

export function exitSideForInstruction(instruction) {
  return normalizedStatus(instruction?.side) === 'SELL' ? 'BUY' : 'SELL';
}

export function exitSideForPosition(position, instruction) {
  const quantity = parseFiniteNumber(position?.quantity);
  if (quantity !== null && quantity < 0) return 'BUY';
  if (quantity !== null && quantity > 0) return 'SELL';
  return exitSideForInstruction(instruction);
}

export function orderMatchesPositionAccount(order, position) {
  const orderAccount = normalizedStatus(order?.account_key);
  const positionAccount = normalizedStatus(position?.account_key);
  return !orderAccount || !positionAccount || orderAccount === positionAccount;
}

export function orderMatchesPositionInstrument(order, position) {
  const orderSymbols = [order?.local_symbol, order?.symbol].map(normalizedSymbol).filter(Boolean);
  const positionSymbols = [position?.local_symbol, position?.symbol].map(normalizedSymbol).filter(Boolean);
  return orderSymbols.some((symbol) => positionSymbols.includes(symbol));
}

export function orderQuantityCoversPosition(order, position) {
  const orderQuantity = parseFiniteNumber(order?.total_quantity);
  const positionQuantity = parseFiniteNumber(position?.quantity);
  if (orderQuantity === null || positionQuantity === null) {
    return true;
  }
  return Math.abs(orderQuantity) >= Math.abs(positionQuantity);
}

export function orderMatchesInstructionAccount(order, instruction) {
  const orderAccount = normalizedStatus(order?.account_key);
  const instructionAccount = normalizedStatus(instruction?.account_key);
  return !orderAccount || !instructionAccount || orderAccount === instructionAccount;
}

export function orderMatchesInstructionInstrument(order, instruction) {
  const payloadInstrument = instruction?.payload?.instruction?.instrument ?? {};
  const orderSymbols = [order?.local_symbol, order?.symbol].map(normalizedSymbol).filter(Boolean);
  const instructionSymbols = [
    instruction?.symbol,
    payloadInstrument?.local_symbol,
    payloadInstrument?.symbol
  ].map(normalizedSymbol).filter(Boolean);
  return orderSymbols.some((symbol) => instructionSymbols.includes(symbol));
}

export function remainingExitQuantityForInstruction(instruction) {
  const entryQuantity = parseFiniteNumber(
    instruction?.entry_filled_quantity ?? instruction?.entry_submitted_quantity
  );
  if (entryQuantity === null) {
    return null;
  }
  const exitQuantity = parseFiniteNumber(instruction?.exit_filled_quantity) ?? 0;
  return Math.max(0, Math.abs(entryQuantity) - Math.abs(exitQuantity));
}

export function orderQuantityCoversInstruction(order, instruction) {
  const orderQuantity = parseFiniteNumber(order?.total_quantity);
  const remainingQuantity = remainingExitQuantityForInstruction(instruction);
  if (orderQuantity === null || remainingQuantity === null) {
    return true;
  }
  return Math.abs(orderQuantity) >= remainingQuantity;
}

export function liveMarketExitOrderForPosition(position, instruction, orderRows = openOrders) {
  const expectedSide = exitSideForPosition(position, instruction);
  return (orderRows ?? []).find(
    (order) =>
      isOpenBrokerOrder(order) &&
      isMarketOrder(order) &&
      normalizedStatus(order?.side) === expectedSide &&
      orderMatchesPositionAccount(order, position) &&
      orderMatchesPositionInstrument(order, position) &&
      orderQuantityCoversPosition(order, position)
  );
}

export function liveMarketExitOrderForInstruction(instruction, orderRows = openOrders) {
  const expectedSide = exitSideForInstruction(instruction);
  return (orderRows ?? []).find(
    (order) =>
      isOpenBrokerOrder(order) &&
      isMarketOrder(order) &&
      normalizedStatus(order?.side) === expectedSide &&
      orderMatchesInstructionAccount(order, instruction) &&
      orderMatchesInstructionInstrument(order, instruction) &&
      orderQuantityCoversInstruction(order, instruction)
  );
}

export function liveEntryOrderForInstruction(instruction, orderRows = openOrders) {
  const expectedSide = normalizedStatus(instruction?.side);
  return (orderRows ?? []).find(
    (order) =>
      isOpenBrokerOrder(order) &&
      normalizedStatus(order?.order_role) === 'ENTRY' &&
      normalizedStatus(order?.side) === expectedSide &&
      (
        String(order?.instruction_record_id ?? '') === String(instruction?.record_id ?? '') ||
        String(order?.order_ref ?? '') === String(instruction?.instruction_id ?? '') ||
        String(order?.external_order_id ?? '') === String(instruction?.broker_order_id ?? '')
      ) &&
      orderMatchesInstructionAccount(order, instruction) &&
      orderMatchesInstructionInstrument(order, instruction)
  );
}

export function workingEntryGuidance(order) {
  const orderId = order?.external_order_id ? `order ${order.external_order_id}` : openOrderReference(order);
  const symbol = order?.local_symbol ?? order?.symbol ?? 'symbol';
  const limitPrice = displayOrderPrice(order?.limit_price);
  const stopPrice = displayOrderPrice(order?.stop_price);
  const priceParts = [];
  if (limitPrice !== 'n/a') priceParts.push(`limit ${limitPrice}`);
  if (stopPrice !== 'n/a') priceParts.push(`stop ${stopPrice}`);
  const priceText = priceParts.length > 0 ? ` at ${priceParts.join(', ')}` : '';
  const marketText = order?.reference_market_price
    ? ` Market ${formatPrice(order.reference_market_price)}, ${orderSpreadLabel(order)}.`
    : '';
  return (
    `Working broker entry ${orderId}: ${order?.side ?? 'n/a'} ` +
    `${formatQuantity(order?.total_quantity)} ${symbol} ${order?.order_type ?? 'order'}${priceText}.` +
    marketText
  );
}

export function openOrderReference(order) {
  return (
    order?.order_ref ??
    (order?.external_perm_id ? `perm ${order.external_perm_id}` : null) ??
    (order?.external_order_id ? `order ${order.external_order_id}` : null) ??
    'broker order'
  );
}

export function instructionForcesNextOpenExit(instruction) {
  return instruction?.payload?.instruction?.exit?.force_exit_next_session_open === true;
}

export function instructionNextSessionExit(instruction) {
  return instruction?.runtime_schedule?.next_session_exit ?? null;
}

export function instructionNextSessionOpenAt(instruction) {
  const nextSessionExit = instructionNextSessionExit(instruction);
  return parseTimestamp(
    nextSessionExit?.next_session_open_utc ?? nextSessionExit?.next_session_open_local
  );
}

export function instructionExitPlanState(instruction) {
  const liveExitOrder = liveMarketExitOrderForInstruction(instruction, openOrders);
  if (liveExitOrder) {
    return {
      label: 'Exit order live',
      className: 'ok',
      detail:
        `${instruction.state}; ${liveExitOrder.side} ` +
        `${formatQuantity(liveExitOrder.total_quantity)} ${liveExitOrder.order_type} ` +
        `${openOrderReference(liveExitOrder)} is working at broker.`,
      isScheduled: false,
      isOpen: true,
      isExpired: false
    };
  }

  if (!instructionForcesNextOpenExit(instruction)) {
    return null;
  }

  const nextOpenAt = instructionNextSessionOpenAt(instruction);
  if (nextOpenAt && referenceNow > nextOpenAt) {
    return {
      label: 'Next open passed',
      className: 'bad',
      detail:
        `${instruction.state}; forced next-open time passed ` +
        `${formatTimestamp(nextOpenAt.toISOString())}, and no matching live market exit order is visible.`,
      isScheduled: false,
      isOpen: true,
      isExpired: true
    };
  }
  if (nextOpenAt) {
    return {
      label: 'Next open armed',
      className: 'ok',
      detail:
        `${instruction.state}; runtime will submit the forced market exit near ` +
        `${formatTimestamp(nextOpenAt.toISOString())}.`,
      isScheduled: true,
      isOpen: true,
      isExpired: false
    };
  }
  return {
    label: 'Next open unresolved',
    className: 'warn',
    detail:
      `${instruction.state}; force_exit_next_session_open is set, ` +
      'but the runtime schedule is unavailable in the dashboard snapshot.',
    isScheduled: false,
    isOpen: true,
    isExpired: false
  };
}

export function normalizeIntentText(value, { upper = true } = {}) {
  const normalized = String(value ?? '').trim();
  return upper ? normalized.toUpperCase() : normalized.toLowerCase();
}

export function instructionBookSide(instruction) {
  const payloadInstruction = instruction?.payload?.instruction ?? {};
  const bookSide =
    payloadInstruction?.account?.book_side ??
    payloadInstruction?.intent?.position_side;
  if (bookSide) {
    return normalizeIntentText(bookSide);
  }
  return normalizeIntentText(instruction?.side) === 'SELL' ? 'SHORT' : 'LONG';
}

export function accountLabelLookup(accountRows) {
  return new Map(
    (accountRows ?? [])
      .map((account) => [
        normalizeIntentText(account.account_key),
        account.account_label ?? account.account_key
      ])
      .filter(([accountKey]) => accountKey)
  );
}

export function instructionCleanupSelector(instruction) {
  return {
    account_key: normalizeIntentText(instruction.account_key),
    book_key: normalizeIntentText(instruction.book_key, { upper: false }),
    book_side: instructionBookSide(instruction),
    symbol: normalizeIntentText(instruction.symbol),
    exchange: normalizeIntentText(instruction.exchange),
    currency: normalizeIntentText(instruction.currency)
  };
}

export function instructionCleanupGroupKey(instruction) {
  const selector = instructionCleanupSelector(instruction);
  return [
    selector.account_key,
    selector.book_key,
    selector.book_side,
    selector.symbol,
    selector.exchange,
    selector.currency
  ].join('|');
}

export function cleanupGroupLatestTimestamp(group) {
  return latestTimestamp(group.instructions, ['activity_at', 'updated_at', 'created_at', 'submit_at']);
}

export function cleanupGroupClassName(group) {
  if (group.entryCount > 0 && group.positionOwnerCount > 0) return 'warn';
  if (group.entryCount > 1) return 'warn';
  if (group.entryCount > 0) return 'neutral';
  return 'ok';
}

export function groupIntentCleanupRows(instructionRows, accountRows) {
  const labels = accountLabelLookup(accountRows);
  const groups = new Map();

  for (const instruction of instructionRows ?? []) {
    if (!entryOwningInstructionStates.has(instruction.state) && !positionOwningInstructionStates.has(instruction.state)) {
      continue;
    }
    const selector = instructionCleanupSelector(instruction);
    if (!selector.account_key || !selector.symbol) {
      continue;
    }
    const key = instructionCleanupGroupKey(instruction);
    const group = groups.get(key) ?? {
      key,
      selector,
      accountLabel: labels.get(selector.account_key) ?? selector.account_key,
      isVirtual: false,
      instructions: [],
      entries: [],
      positionOwners: [],
      forceNextOpen: false
    };

    group.instructions.push(instruction);
    group.isVirtual = group.isVirtual || instruction.is_virtual === true;
    group.forceNextOpen = group.forceNextOpen || instructionForcesNextOpenExit(instruction);
    if (entryOwningInstructionStates.has(instruction.state)) {
      group.entries.push(instruction);
    }
    if (positionOwningInstructionStates.has(instruction.state)) {
      group.positionOwners.push(instruction);
    }
    groups.set(key, group);
  }

  return [...groups.values()]
    .map((group) => ({
      ...group,
      entryCount: group.entries.length,
      submittedEntryCount: group.entries.filter((instruction) => instruction.state === 'ENTRY_SUBMITTED').length,
      pendingEntryCount: group.entries.filter((instruction) => instruction.state === 'ENTRY_PENDING').length,
      positionOwnerCount: group.positionOwners.length,
      latestAt: cleanupGroupLatestTimestamp(group),
      entryRefs: summarizeRefs(group.entries.map((instruction) => instruction.instruction_id)),
      ownerRefs: summarizeRefs(group.positionOwners.map((instruction) => instruction.instruction_id)),
      className: cleanupGroupClassName({
        entryCount: group.entries.length,
        positionOwnerCount: group.positionOwners.length
      })
    }))
    .sort((left, right) => {
      if (left.entryCount !== right.entryCount) return right.entryCount - left.entryCount;
      if (left.positionOwnerCount !== right.positionOwnerCount) return right.positionOwnerCount - left.positionOwnerCount;
      const leftAt = parseTimestamp(left.latestAt)?.getTime() ?? 0;
      const rightAt = parseTimestamp(right.latestAt)?.getTime() ?? 0;
      return rightAt - leftAt;
    });
}

export function candidateLifecyclePolicy(instruction) {
  return instruction?.payload?.instruction?.lifecycle ?? null;
}

export function candidateLifecycleLabel(instruction) {
  const lifecycle = candidateLifecyclePolicy(instruction);
  if (!lifecycle) {
    return 'Policy missing';
  }
  const entryLimit = lifecycle.max_entry_orders ?? 'n/a';
  const exitLimit = lifecycle.max_exit_orders ?? 'n/a';
  return `${entryLimit} entry / ${exitLimit} exit`;
}

export function groupSourceIntentRows(candidateRows, accountRows) {
  const labels = accountLabelLookup(accountRows);
  const groups = new Map();

  for (const instruction of candidateRows ?? []) {
    const selector = instructionCleanupSelector(instruction);
    if (!selector.account_key || !selector.symbol) {
      continue;
    }
    const key = instructionCleanupGroupKey(instruction);
    const group = groups.get(key) ?? {
      key,
      selector,
      accountLabel: labels.get(selector.account_key) ?? selector.account_key,
      isVirtual: false,
      candidates: [],
      modelIds: new Set(),
      states: new Set(),
      policyLabels: new Set()
    };

    group.candidates.push(instruction);
    group.isVirtual = group.isVirtual || instruction.is_virtual === true;
    group.modelIds.add(rlCandidateModelId(instruction));
    group.states.add(instruction.state ?? 'UNKNOWN');
    group.policyLabels.add(candidateLifecycleLabel(instruction));
    groups.set(key, group);
  }

  return [...groups.values()]
    .map((group) => ({
      ...group,
      candidateCount: group.candidates.length,
      modelLabel: summarizeRefs([...group.modelIds]),
      stateLabel: summarizeRefs([...group.states]),
      policyLabel: summarizeRefs([...group.policyLabels]),
      latestAt: latestTimestamp(group.candidates, ['activity_at', 'updated_at', 'created_at', 'submit_at']),
      candidateRefs: summarizeRefs(group.candidates.map((instruction) => instruction.instruction_id))
    }))
    .sort((left, right) => {
      const leftAt = parseTimestamp(left.latestAt)?.getTime() ?? 0;
      const rightAt = parseTimestamp(right.latestAt)?.getTime() ?? 0;
      if (rightAt !== leftAt) return rightAt - leftAt;
      return left.selector.symbol.localeCompare(right.selector.symbol);
    });
}

export function positionInstructionMatches(position, instruction) {
  if (!position || !instruction) return false;
  if (!positionOwningInstructionStates.has(instruction.state)) return false;
  if (String(position.account_key ?? '').toUpperCase() !== String(instruction.account_key ?? '').toUpperCase()) {
    return false;
  }
  return normalizedSymbol(position.local_symbol ?? position.symbol) === normalizedSymbol(instruction.symbol);
}

export function activeInstructionsForPosition(position, instructionRows = executionInstructions) {
  return instructionRows
    .filter((instruction) => positionInstructionMatches(position, instruction))
    .sort((left, right) => {
      const leftAt = parseTimestamp(left.activity_at ?? left.updated_at)?.getTime() ?? 0;
      const rightAt = parseTimestamp(right.activity_at ?? right.updated_at)?.getTime() ?? 0;
      return rightAt - leftAt;
    });
}

export function positionExitPlan(position, instructionRows = executionInstructions) {
  const owningInstructions = activeInstructionsForPosition(position, instructionRows);
  const primaryInstruction = owningInstructions[0];
  if (!primaryInstruction) {
    return {
      label: 'No owner',
      className: 'bad',
      detail: 'No active execution instruction owns this holding.',
      instructionId: null
    };
  }
  const liveExitOrder = liveMarketExitOrderForPosition(position, primaryInstruction, openOrders);
  if (liveExitOrder) {
    return {
      label: 'Exit order live',
      className: 'ok',
      detail:
        `${primaryInstruction.state}; ${liveExitOrder.side} ` +
        `${formatQuantity(liveExitOrder.total_quantity)} ${liveExitOrder.order_type} ` +
        `${openOrderReference(liveExitOrder)} is working at broker.`,
      instructionId: primaryInstruction.instruction_id
    };
  }
  if (instructionForcesNextOpenExit(primaryInstruction)) {
    const nextOpenAt = instructionNextSessionOpenAt(primaryInstruction);
    if (nextOpenAt && referenceNow > nextOpenAt) {
      return {
        label: 'Next open passed',
        className: 'bad',
        detail:
          `${primaryInstruction.state}; forced next-open time passed ` +
          `${formatTimestamp(nextOpenAt.toISOString())}, and no matching live market exit order is visible.`,
        instructionId: primaryInstruction.instruction_id
      };
    }
    if (nextOpenAt) {
      return {
        label: 'Next open armed',
        className: 'ok',
        detail:
          `${primaryInstruction.state}; runtime will submit the forced market exit near ` +
          `${formatTimestamp(nextOpenAt.toISOString())}.`,
        instructionId: primaryInstruction.instruction_id
      };
    }
    return {
      label: 'Next open unresolved',
      className: 'warn',
      detail:
        `${primaryInstruction.state}; force_exit_next_session_open is set, ` +
        'but the runtime schedule is unavailable in the dashboard snapshot.',
      instructionId: primaryInstruction.instruction_id
    };
  }
  return {
    label: 'No next-open flag',
    className: 'warn',
    detail: `${primaryInstruction.state}; this instruction does not request force_exit_next_session_open.`,
    instructionId: primaryInstruction.instruction_id
  };
}

export function positionExitPlanSearchText(position, instructionRows = executionInstructions) {
  const exitPlan = positionExitPlan(position, instructionRows);
  return [
    exitPlan.label,
    exitPlan.className,
    exitPlan.detail,
    exitPlan.instructionId
  ].filter(Boolean).join(' ');
}
