<script>
  import { browser } from '$app/environment';
  import { applyAction, enhance } from '$app/forms';
  import { onMount } from 'svelte';
  import './operator-dashboard-client/operator-dashboard.css';
  import DashboardOverview from './operator-dashboard-client/components/DashboardOverview.svelte';
  import DashboardControls from './operator-dashboard-client/components/DashboardControls.svelte';
  import DashboardAccountsOperations from './operator-dashboard-client/components/DashboardAccountsOperations.svelte';
  import DashboardTables from './operator-dashboard-client/components/DashboardTables.svelte';
  import {
    defaultDashboardFilters,
    matchesFilterValue,
    parseStoredFilters,
    uniqueIds
  } from './operator-dashboard-client/filters.js';
  import {
    applyMarketStreamToAccounts,
    applyMarketStreamToOpenOrders,
    applyMarketStreamToPositions,
    buildLiveOmxBenchmark,
    buildMarketStreamMarks
  } from './operator-dashboard-client/market-stream.js';
  import {
    ageSeconds,
    compactCount,
    formatAge,
    formatTimestamp,
    formatTimestampOrNull,
    freshnessClass,
    monitorClass,
    monitorLabel,
    parseTimestamp,
    classForConnection,
    connectionLabel,
    ibGatewayClass,
    ibGatewayDetail,
    ibGatewayLabel,
    killSwitchClass,
    killSwitchLabel,
    executionRuntimeClass,
    executionRuntimeLabel,
    runStatusClass
  } from './operator-dashboard-client/status.js';
  import {
    displayOrderPrice,
    formatMoney,
    formatPrice,
    formatQuantity,
    formatReturnPct,
    formatSignedMoney,
    moneyTone,
    parseFiniteNumber
  } from './operator-dashboard-client/formatting.js';
  import { accountDayChart } from './operator-dashboard-client/performance.js';
  import {
    fillExitPnlLabel,
    fillExitPnlSearchText,
    fillStrategyLabel,
    marketDirectionArrow,
    marketDirectionClass,
    operatorReviewActions,
    operatorReviewClass,
    operatorReviewDetail,
    operatorReviewLabel,
    orderFillSpreadLabel,
    orderSpreadLabel,
    orderTriggerDetail
  } from './operator-dashboard-client/reviews.js';
  import {
    activeInstructionsForPosition,
    candidateLifecycleLabel,
    candidateLifecyclePolicy,
    groupIntentCleanupRows,
    groupSourceIntentRows,
    hasInstructionAction,
    instructionGuidance,
    instructionOrderDisplay,
    instructionPrimaryAction,
    instructionWindowState,
    isRlCandidateInstruction,
    liveEntryOrderForInstruction,
    liveMarketExitOrderForInstruction,
    liveMarketExitOrderForPosition,
    normalizedStatus,
    positionExitPlan,
    positionExitPlanSearchText,
    rlCandidateModelId,
    rlCandidateWindowDisplay,
    workingEntryGuidance
  } from './operator-dashboard-client/instructions.js';
  import {
    groupBrokerAttentionRows,
    groupReconciliationRuns
  } from './operator-dashboard-client/attention.js';
  import { buildStateSyncSummary } from './operator-dashboard-client/sync.js';
  import { setDashboardViewState } from './operator-dashboard-client/view-state.js';

  export let data;
  export let form;

  let liveHealth = data.health;
  let liveGeneratedAt = data.generatedAt;
  let liveErrors = data.errors;
  let operatorSnapshot = data.operatorSnapshot;
  let killSwitch = operatorSnapshot.kill_switch;
  let accounts = operatorSnapshot.accounts;
  let positions = operatorSnapshot.positions;
  let openOrders = operatorSnapshot.open_orders;
  let recentFills = operatorSnapshot.recent_fills;
  let brokerAttention = operatorSnapshot.recent_broker_attention;
  let reconciliationRuns = operatorSnapshot.recent_reconciliation_runs;
  let instructions = operatorSnapshot.instructions;
  let marketTimeZone = liveHealth.runtime_timezone;
  let brokerMonitor = liveHealth.broker_monitor;
  let ibGateway = liveHealth.ibgateway ?? null;
  let executionRuntime = liveHealth.execution_runtime;
  let omxBenchmark = data.omxBenchmark;
  let marketStreamSnapshot = null;
  let marketStreamMarks = new Map();
  let marketStreamStatus = {
    connected: false,
    received_at: null,
    latest_market_data_at: null,
    running: null,
    last_error: null
  };
  let endpointErrors = [];
  let warningRuns = [];
  let killSwitchResult = null;
  let startupReconcileResult = null;
  let archiveResult = null;
  let instructionRowActionResult = null;
  let intentCleanupResult = null;
  let orderRowActionResult = null;
  let brokerAttentionActionResult = null;
  let reconciliationIssueActionResult = null;
  let acknowledgeAllLogsResult = null;
  let reconciliationClearResult = null;
  let referenceNow = new Date();
  let liveSnapshotStatus = {
    connected: false,
    received_at: null,
    last_error: null
  };
  let dashboardFilters = defaultDashboardFilters();
  let filtersLoaded = false;
  let buttonStates = {};
  let filteredPositions = [];
  let filteredOpenOrders = [];
  let filteredRecentFills = [];
  let rlCandidateInstructions = [];
  let executionInstructions = [];
  let filteredInstructions = [];
  let sourceIntentGroups = [];
  let intentCleanupGroups = [];
  let actionableIntentCleanupGroups = [];
  let virtualIntentCleanupGroupCount = 0;
  let aggregatedBrokerAttention = [];
  let filteredBrokerAttention = [];
  let aggregatedReconciliation = [];
  let filteredReconciliation = [];
  let visibleBrokerAttentionEventIds = [];
  let visibleReconciliationIssueIds = [];
  let stateSync = null;
  const FILTER_STORAGE_KEY = 'ibkr-trader-operator-filters/v4';
  const BUTTON_CLICK_TO_WORK_MS = 140;
  const BUTTON_SUCCESS_RESET_MS = 1600;
  const BUTTON_ERROR_RESET_MS = 2200;
  const RECONCILIATION_GROUP_DISPLAY_LIMIT = 12;
  let timestampFormatter = new Intl.DateTimeFormat('sv-SE', {
    timeZone: marketTimeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    timeZoneName: 'short'
  });
  function resetFilterSection(sectionName) {
    const defaults = defaultDashboardFilters();
    dashboardFilters = {
      ...dashboardFilters,
      [sectionName]: defaults[sectionName]
    };
  }

  function sectionHasActiveFilters(sectionName) {
    return Object.values(dashboardFilters[sectionName] ?? {}).some(
      (value) => String(value ?? '').trim() !== ''
    );
  }

  $: marketStreamMarks = buildMarketStreamMarks(marketStreamSnapshot);
  $: killSwitch = operatorSnapshot.kill_switch;
  $: accounts = applyMarketStreamToAccounts(
    operatorSnapshot.accounts,
    operatorSnapshot.positions,
    marketStreamMarks
  );
  $: positions = applyMarketStreamToPositions(operatorSnapshot.positions, marketStreamMarks);
  $: openOrders = applyMarketStreamToOpenOrders(operatorSnapshot.open_orders, marketStreamMarks);
  $: recentFills = operatorSnapshot.recent_fills;
  $: brokerAttention = operatorSnapshot.recent_broker_attention;
  $: reconciliationRuns = operatorSnapshot.recent_reconciliation_runs;
  $: instructions = operatorSnapshot.instructions;
  $: marketTimeZone = liveHealth.runtime_timezone;
  $: brokerMonitor = liveHealth.broker_monitor;
  $: ibGateway = liveHealth.ibgateway ?? null;
  $: executionRuntime = liveHealth.execution_runtime;
  $: omxBenchmark = buildLiveOmxBenchmark(data.omxBenchmark, marketStreamSnapshot);
  $: endpointErrors = Object.entries(liveErrors).filter(([, value]) => value);
  $: warningRuns = reconciliationRuns.filter((run) => Number(run.issue_count) > 0);
  $: killSwitchResult = form?.killSwitchResult ?? null;
  $: startupReconcileResult = form?.startupReconcileResult ?? null;
  $: archiveResult = form?.archiveResult ?? null;
  $: instructionRowActionResult = form?.instructionRowActionResult ?? null;
  $: intentCleanupResult = form?.intentCleanupResult ?? null;
  $: orderRowActionResult = form?.orderRowActionResult ?? null;
  $: brokerAttentionActionResult = form?.brokerAttentionActionResult ?? null;
  $: reconciliationIssueActionResult = form?.reconciliationIssueActionResult ?? null;
  $: acknowledgeAllLogsResult = form?.acknowledgeAllLogsResult ?? null;
  $: reconciliationClearResult = form?.reconciliationClearResult ?? null;
  $: timestampFormatter = new Intl.DateTimeFormat('sv-SE', {
    timeZone: marketTimeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    timeZoneName: 'short'
  });


  function marketStreamStatusClass() {
    if (marketStreamStatus.last_error) {
      return marketStreamStatus.connected ? 'warn' : 'bad';
    }
    if (marketStreamStatus.running === false) {
      return 'bad';
    }
    const age = ageSeconds(marketStreamStatus.latest_market_data_at);
    if (age === null) {
      return marketStreamStatus.connected ? 'warn' : 'bad';
    }
    if (age <= 15) {
      return 'ok';
    }
    if (age <= 180) {
      return 'warn';
    }
    return 'bad';
  }

  function marketStreamStatusLabel() {
    if (marketStreamStatus.last_error && !marketStreamStatus.connected) {
      return 'Disconnected';
    }
    if (marketStreamStatus.running === false) {
      return 'Stopped';
    }
    if (marketStreamStatus.latest_market_data_at) {
      return 'Live';
    }
    return marketStreamStatus.connected ? 'Listening' : 'Connecting';
  }

  function marketStreamStatusDetail() {
    if (marketStreamStatus.last_error) {
      return marketStreamStatus.last_error;
    }
    if (marketStreamStatus.latest_market_data_at) {
      return `Latest tick ${formatTimestamp(marketStreamStatus.latest_market_data_at)}`;
    }
    if (marketStreamStatus.received_at) {
      return `Snapshot received ${formatTimestamp(marketStreamStatus.received_at)}`;
    }
    return 'Waiting for stream snapshot';
  }

  function openMarketStreamEvents() {
    if (!browser || !window.EventSource) {
      return null;
    }
    const source = new EventSource('/api/market-stream/events?bar_limit=600');
    source.onopen = () => {
      marketStreamStatus = {
        ...marketStreamStatus,
        connected: true,
        last_error: null
      };
    };
    source.addEventListener('stream', (event) => {
      try {
        const payload = JSON.parse(event.data);
        const stream = payload?.stream ?? {};
        marketStreamSnapshot = stream;
        marketStreamStatus = {
          connected: true,
          received_at: payload?.received_at ?? new Date().toISOString(),
          latest_market_data_at:
            stream.latest_market_data_at ?? stream.latest_quote_at ?? stream.latest_trade_at ?? null,
          running: stream.running ?? null,
          last_error: stream.last_error ?? null
        };
      } catch (error) {
        marketStreamStatus = {
          ...marketStreamStatus,
          connected: true,
          last_error: error instanceof Error ? error.message : String(error)
        };
      }
    });
    source.addEventListener('stream-error', (event) => {
      let message = 'Live market stream unavailable';
      try {
        message = JSON.parse(event.data)?.message ?? message;
      } catch {
        // Keep the generic message.
      }
      marketStreamStatus = {
        ...marketStreamStatus,
        connected: false,
        received_at: new Date().toISOString(),
        last_error: message
      };
    });
    source.onerror = () => {
      marketStreamStatus = {
        ...marketStreamStatus,
        connected: false,
        last_error: 'Live market event stream disconnected.'
      };
    };
    return source;
  }

  function openOperatorEvents() {
    if (!browser || !window.EventSource) {
      return null;
    }
    const source = new EventSource('/api/operator/events?interval_ms=1000');
    source.onopen = () => {
      liveSnapshotStatus = {
        ...liveSnapshotStatus,
        connected: true,
        last_error: null
      };
    };
    source.addEventListener('operator', (event) => {
      try {
        const payload = JSON.parse(event.data);
        liveHealth = payload.health ?? liveHealth;
        liveErrors = payload.errors ?? liveErrors;
        liveGeneratedAt = payload.received_at ?? new Date().toISOString();
        if (payload.operatorSnapshot) {
          operatorSnapshot = payload.operatorSnapshot;
        }
        liveSnapshotStatus = {
          connected: true,
          received_at: payload.received_at ?? new Date().toISOString(),
          last_error: null
        };
      } catch (error) {
        liveSnapshotStatus = {
          ...liveSnapshotStatus,
          connected: true,
          last_error: error instanceof Error ? error.message : String(error)
        };
      }
    });
    source.addEventListener('operator-error', (event) => {
      let message = 'Operator live state unavailable';
      try {
        message = JSON.parse(event.data)?.message ?? message;
      } catch {
        // Keep the generic message.
      }
      liveSnapshotStatus = {
        connected: false,
        received_at: new Date().toISOString(),
        last_error: message
      };
    });
    source.onerror = () => {
      liveSnapshotStatus = {
        ...liveSnapshotStatus,
        connected: false,
        last_error: 'Operator live event stream disconnected.'
      };
    };
    return source;
  }

  function liveSnapshotStatusClass() {
    if (liveSnapshotStatus.last_error) return liveSnapshotStatus.connected ? 'warn' : 'bad';
    return liveSnapshotStatus.connected ? 'ok' : 'warn';
  }

  function liveSnapshotStatusLabel() {
    if (liveSnapshotStatus.last_error && !liveSnapshotStatus.connected) return 'Disconnected';
    if (liveSnapshotStatus.last_error) return 'Degraded';
    return liveSnapshotStatus.connected ? 'Live' : 'Connecting';
  }

  function liveSnapshotStatusDetail() {
    if (liveSnapshotStatus.last_error) return liveSnapshotStatus.last_error;
    if (liveSnapshotStatus.received_at) return `Snapshot pushed ${formatTimestamp(liveSnapshotStatus.received_at)}`;
    return 'Opening live dashboard stream';
  }

  onMount(() => {
    if (browser) {
      dashboardFilters = parseStoredFilters(window.localStorage.getItem(FILTER_STORAGE_KEY));
      filtersLoaded = true;
    }

    const operatorEvents = openOperatorEvents();
    const marketStreamEvents = openMarketStreamEvents();
    const clockIntervalId = window.setInterval(() => {
      referenceNow = new Date();
    }, 5000);

    return () => {
      operatorEvents?.close();
      marketStreamEvents?.close();
      window.clearInterval(clockIntervalId);
    };
  });

  $: if (browser && filtersLoaded) {
    window.localStorage.setItem(FILTER_STORAGE_KEY, JSON.stringify(dashboardFilters));
  }


  function setButtonState(actionKey, nextState) {
    buttonStates = {
      ...buttonStates,
      [actionKey]: nextState
    };
  }

  function clearButtonState(actionKey) {
    const nextStates = { ...buttonStates };
    delete nextStates[actionKey];
    buttonStates = nextStates;
  }

  function buttonState(actionKey) {
    return buttonStates[actionKey] ?? 'idle';
  }

  function buttonIsBusy(actionKey) {
    const currentState = buttonState(actionKey);
    return currentState === 'clicking' || currentState === 'working';
  }

  function buttonStateClass(actionKey) {
    const currentState = buttonState(actionKey);
    if (currentState === 'clicking') return 'is-clicking';
    if (currentState === 'working') return 'is-working';
    if (currentState === 'success') return 'is-success';
    if (currentState === 'error') return 'is-error';
    return '';
  }

  function buttonLabel(actionKey, baseLabel) {
    const currentState = buttonState(actionKey);
    if (currentState === 'clicking') return 'Clicking…';
    if (currentState === 'working') return 'Working…';
    if (currentState === 'success') return 'Done';
    if (currentState === 'error') return 'Retry';
    return baseLabel;
  }

  function enhanceDashboardAction(defaultActionKey = 'dashboard-action') {
    return ({ submitter }) => {
      const actionKey = submitter?.dataset?.actionKey ?? defaultActionKey;
      setButtonState(actionKey, 'clicking');

      const transitionTimer = window.setTimeout(() => {
        if (buttonState(actionKey) === 'clicking') {
          setButtonState(actionKey, 'working');
        }
      }, BUTTON_CLICK_TO_WORK_MS);

      return async ({ result }) => {
        window.clearTimeout(transitionTimer);
        await applyAction(result);

        if (result.type === 'success') {
          setButtonState(actionKey, 'success');
          window.setTimeout(() => clearButtonState(actionKey), BUTTON_SUCCESS_RESET_MS);
          return;
        }

        setButtonState(actionKey, 'error');
        window.setTimeout(() => clearButtonState(actionKey), BUTTON_ERROR_RESET_MS);
      };
    };
  }

  $: filteredPositions = positions.filter((position) =>
    matchesFilterValue(position.account_label ?? position.account_key, dashboardFilters.positions.account) &&
    matchesFilterValue(position.local_symbol ?? position.symbol, dashboardFilters.positions.symbol) &&
    matchesFilterValue(position.primary_exchange ?? position.exchange, dashboardFilters.positions.exchange) &&
    matchesFilterValue(position.currency, dashboardFilters.positions.currency) &&
    matchesFilterValue(position.quantity, dashboardFilters.positions.quantity) &&
    matchesFilterValue(position.average_cost ?? 'n/a', dashboardFilters.positions.averageCost) &&
    matchesFilterValue(position.market_price ?? 'n/a', dashboardFilters.positions.marketPrice) &&
    matchesFilterValue(position.market_value ?? 'n/a', dashboardFilters.positions.marketValue) &&
    matchesFilterValue(position.unrealized_pnl ?? 'n/a', dashboardFilters.positions.unrealizedPnl) &&
    matchesFilterValue(positionExitPlanSearchText(position, executionInstructions), dashboardFilters.positions.exitPlan)
  );

  $: filteredOpenOrders = openOrders.filter((order) =>
    matchesFilterValue(order.account_label ?? order.account_key, dashboardFilters.openOrders.account) &&
    matchesFilterValue(order.local_symbol ?? order.symbol, dashboardFilters.openOrders.symbol) &&
    matchesFilterValue(order.order_role, dashboardFilters.openOrders.role) &&
    matchesFilterValue(order.order_purpose ?? 'n/a', dashboardFilters.openOrders.purpose) &&
    matchesFilterValue(order.side, dashboardFilters.openOrders.side) &&
    matchesFilterValue(order.total_quantity ?? 'n/a', dashboardFilters.openOrders.quantity) &&
    matchesFilterValue(order.order_type, dashboardFilters.openOrders.type) &&
    matchesFilterValue(displayOrderPrice(order.limit_price), dashboardFilters.openOrders.limit) &&
    matchesFilterValue(displayOrderPrice(order.stop_price), dashboardFilters.openOrders.stop) &&
    matchesFilterValue(orderFillSpreadLabel(order), dashboardFilters.openOrders.vsFill) &&
    matchesFilterValue(order.reference_market_price ?? 'n/a', dashboardFilters.openOrders.market) &&
    matchesFilterValue(orderSpreadLabel(order), dashboardFilters.openOrders.vsMkt) &&
    matchesFilterValue(order.status, dashboardFilters.openOrders.status) &&
    matchesFilterValue(order.reject_reason ?? order.warning_text ?? 'n/a', dashboardFilters.openOrders.warning)
  );

  $: filteredRecentFills = recentFills.filter((fill) =>
    matchesFilterValue(formatTimestamp(fill.executed_at), dashboardFilters.recentFills.time) &&
    matchesFilterValue(fill.account_label ?? fill.account_key, dashboardFilters.recentFills.account) &&
    matchesFilterValue(fill.symbol, dashboardFilters.recentFills.symbol) &&
    matchesFilterValue(fill.side ?? 'n/a', dashboardFilters.recentFills.side) &&
    matchesFilterValue(fillStrategyLabel(fill), dashboardFilters.recentFills.strat) &&
    matchesFilterValue(fill.quantity, dashboardFilters.recentFills.quantity) &&
    matchesFilterValue(fill.price, dashboardFilters.recentFills.price) &&
    matchesFilterValue(`${fill.commission ?? 'n/a'} ${fill.commission_currency ?? ''}`, dashboardFilters.recentFills.fee) &&
    matchesFilterValue(fillExitPnlSearchText(fill), dashboardFilters.recentFills.pnl)
  );

  $: rlCandidateInstructions = instructions.filter((instruction) =>
    isRlCandidateInstruction(instruction)
  );
  $: executionInstructions = instructions.filter(
    (instruction) => !isRlCandidateInstruction(instruction)
  );
  $: sourceIntentGroups = groupSourceIntentRows(rlCandidateInstructions, accounts);
  $: intentCleanupGroups = groupIntentCleanupRows(executionInstructions, accounts);
  $: actionableIntentCleanupGroups = intentCleanupGroups.filter((group) => group.entryCount > 0);
  $: virtualIntentCleanupGroupCount = actionableIntentCleanupGroups.filter((group) => group.isVirtual).length;
  $: filteredInstructions = executionInstructions.filter((instruction) => {
    const lifecycle = instructionWindowState(instruction);
    return (
      matchesFilterValue(instruction.instruction_id, dashboardFilters.instructions.instruction) &&
      matchesFilterValue(instruction.symbol, dashboardFilters.instructions.symbol) &&
      matchesFilterValue(instruction.state, dashboardFilters.instructions.state) &&
      matchesFilterValue(lifecycle.label, dashboardFilters.instructions.lifecycle) &&
      matchesFilterValue(instructionGuidance(instruction), dashboardFilters.instructions.guidance) &&
      matchesFilterValue(instructionOrderDisplay(instruction, 'entry'), dashboardFilters.instructions.entryOrder) &&
      matchesFilterValue(instructionOrderDisplay(instruction, 'exit'), dashboardFilters.instructions.exitOrder) &&
      matchesFilterValue(formatTimestamp(instruction.updated_at), dashboardFilters.instructions.updated)
    );
  });

  $: aggregatedBrokerAttention = groupBrokerAttentionRows(brokerAttention);
  $: filteredBrokerAttention = aggregatedBrokerAttention;
  $: visibleBrokerAttentionEventIds = uniqueIds(
    filteredBrokerAttention.flatMap((group) => group.eventIds)
  );

  $: aggregatedReconciliation = groupReconciliationRuns(reconciliationRuns);
  $: filteredReconciliation = aggregatedReconciliation.slice(0, RECONCILIATION_GROUP_DISPLAY_LIMIT);
  $: visibleReconciliationIssueIds = uniqueIds(
    filteredReconciliation.flatMap((group) => group.issueIds)
  );
$: setDashboardViewState({
  liveHealth,
  killSwitch,
  accounts,
  positions,
  openOrders,
  recentFills,
  brokerAttention,
  reconciliationRuns,
  instructions,
  brokerMonitor,
  ibGateway,
  executionRuntime,
  omxBenchmark,
  marketStreamMarks,
  marketStreamStatus,
  liveSnapshotStatus,
  referenceNow,
  timestampFormatter,
  executionInstructions,
  rlCandidateInstructions
});
$: stateSync = buildStateSyncSummary();
</script>

<svelte:head>
  <title>IBKR Trader Operator Dashboard</title>
</svelte:head>

<div class="page">
  <DashboardOverview
    {data}
    {liveGeneratedAt}
    {operatorSnapshot}
    {liveSnapshotStatusClass}
    {liveSnapshotStatusLabel}
    {liveSnapshotStatusDetail}
    {marketStreamStatusClass}
    {marketStreamStatusLabel}
    {marketStreamStatusDetail}
    {marketTimeZone}
    {liveHealth}
    {brokerMonitor}
    {executionRuntime}
    {killSwitch}
    {accounts}
    {positions}
    {openOrders}
    {rlCandidateInstructions}
    {executionInstructions}
    {brokerAttention}
    {warningRuns}
    {stateSync}
    {endpointErrors}
  />

  <DashboardControls
    {killSwitch}
    {killSwitchResult}
    {archiveResult}
    {startupReconcileResult}
    {intentCleanupResult}
    {sourceIntentGroups}
    {actionableIntentCleanupGroups}
    {intentCleanupGroups}
    {virtualIntentCleanupGroupCount}
    {buttonStateClass}
    {buttonIsBusy}
    {buttonLabel}
    {enhanceDashboardAction}
  />

  <DashboardAccountsOperations
    {accounts}
    {filteredBrokerAttention}
    {acknowledgeAllLogsResult}
    {brokerAttentionActionResult}
    {visibleBrokerAttentionEventIds}
    {visibleReconciliationIssueIds}
    {filteredReconciliation}
    {aggregatedReconciliation}
    {reconciliationClearResult}
    {reconciliationIssueActionResult}
    {buttonStateClass}
    {buttonIsBusy}
    {buttonLabel}
    {enhanceDashboardAction}
  />

  <DashboardTables
    {positions}
    {filteredPositions}
    {openOrders}
    {filteredOpenOrders}
    {recentFills}
    {filteredRecentFills}
    {rlCandidateInstructions}
    {executionInstructions}
    {filteredInstructions}
    bind:dashboardFilters
    {orderRowActionResult}
    {instructionRowActionResult}
    {resetFilterSection}
    {sectionHasActiveFilters}
    {buttonStateClass}
    {buttonIsBusy}
    {buttonLabel}
    {enhanceDashboardAction}
  />
</div>
