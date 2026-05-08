<script>
  import { browser } from '$app/environment';
  import { applyAction, enhance } from '$app/forms';
  import { invalidateAll } from '$app/navigation';
  import { onMount } from 'svelte';

  export let data;
  export let form;

  let operatorSnapshot = data.operatorSnapshot;
  let killSwitch = operatorSnapshot.kill_switch;
  let accounts = operatorSnapshot.accounts;
  let positions = operatorSnapshot.positions;
  let openOrders = operatorSnapshot.open_orders;
  let recentFills = operatorSnapshot.recent_fills;
  let brokerAttention = operatorSnapshot.recent_broker_attention;
  let reconciliationRuns = operatorSnapshot.recent_reconciliation_runs;
  let instructions = operatorSnapshot.instructions;
  let marketTimeZone = data.health.runtime_timezone;
  let brokerMonitor = data.health.broker_monitor;
  let ibGateway = data.health.ibgateway ?? null;
  let executionRuntime = data.health.execution_runtime;
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
  let refreshInFlight = false;
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
  const terminalInstructionStates = new Set(['ENTRY_CANCELLED', 'COMPLETED', 'FAILED']);
  const entryOwningInstructionStates = new Set(['ENTRY_PENDING', 'ENTRY_SUBMITTED']);
  const positionOwningInstructionStates = new Set(['POSITION_OPEN', 'EXIT_PENDING']);
  const closedOrderStatuses = new Set([
    'API_CANCELLED',
    'CANCELLED',
    'ERROR',
    'FILLED',
    'INACTIVE',
    'NOT_FOUND_AT_BROKER',
    'REJECTED'
  ]);
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
  const quantityFormatter = new Intl.NumberFormat('en-US', {
    minimumFractionDigits: 0,
    maximumFractionDigits: 4
  });
  const priceFormatter = new Intl.NumberFormat('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });
  const moneyFormatter = new Intl.NumberFormat('en-US', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  });

  function defaultDashboardFilters() {
    return {
      positions: {
        account: '',
        symbol: '',
        exchange: '',
        currency: '',
        quantity: '',
        averageCost: '',
        marketPrice: '',
        marketValue: '',
        unrealizedPnl: '',
        exitPlan: ''
      },
      openOrders: {
        account: '',
        symbol: '',
        role: '',
        purpose: '',
        side: '',
        quantity: '',
        type: '',
        limit: '',
        stop: '',
        vsFill: '',
        market: '',
        vsMkt: '',
        status: '',
        warning: ''
      },
      recentFills: {
        time: '',
        account: '',
        symbol: '',
        side: '',
        strat: '',
        quantity: '',
        price: '',
        fee: '',
        pnl: ''
      },
      instructions: {
        instruction: '',
        symbol: '',
        state: '',
        lifecycle: '',
        guidance: '',
        entryOrder: '',
        exitOrder: '',
        updated: ''
      },
      brokerAttention: {},
      reconciliation: {}
    };
  }

  function parseStoredFilters(rawValue) {
    const defaults = defaultDashboardFilters();
    if (!rawValue) {
      return defaults;
    }

    try {
      const parsed = JSON.parse(rawValue);
      for (const [sectionName, sectionDefaults] of Object.entries(defaults)) {
        const parsedSection =
          parsed && typeof parsed === 'object' && parsed[sectionName] && typeof parsed[sectionName] === 'object'
            ? parsed[sectionName]
            : {};
        defaults[sectionName] = Object.fromEntries(
          Object.keys(sectionDefaults).map((key) => [key, String(parsedSection[key] ?? '')])
        );
      }
      return defaults;
    } catch {
      return defaults;
    }
  }

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

  function normalizeSearchText(value) {
    if (value === null || value === undefined) return '';
    if (Array.isArray(value)) return value.map((item) => normalizeSearchText(item)).join(' ');
    return String(value).toLowerCase();
  }

  function matchesFilterValue(value, filterValue) {
    const normalizedFilter = String(filterValue ?? '').trim().toLowerCase();
    if (!normalizedFilter) {
      return true;
    }
    return normalizeSearchText(value).includes(normalizedFilter);
  }

  function uniqueIds(values) {
    return [...new Set(values.filter((value) => Number.isInteger(value) && value > 0))];
  }

  function summarizeRefs(values) {
    const uniqueValues = [...new Set(values.filter(Boolean))];
    if (uniqueValues.length === 0) {
      return null;
    }
    if (uniqueValues.length <= 2) {
      return uniqueValues.join(', ');
    }
    return `${uniqueValues.slice(0, 2).join(', ')} +${uniqueValues.length - 2} more`;
  }

  $: operatorSnapshot = data.operatorSnapshot;
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
  $: marketTimeZone = data.health.runtime_timezone;
  $: brokerMonitor = data.health.broker_monitor;
  $: ibGateway = data.health.ibgateway ?? null;
  $: executionRuntime = data.health.execution_runtime;
  $: omxBenchmark = buildLiveOmxBenchmark(data.omxBenchmark, marketStreamSnapshot);
  $: endpointErrors = Object.entries(data.errors).filter(([, value]) => value);
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

  function brokerConnected(role) {
    return data.health.broker_sessions[role].connected === true;
  }

  function sessionStatus(role) {
    const session = data.health.broker_sessions[role] ?? {};
    const heartbeat = brokerMonitor?.heartbeat ?? {};
    if (heartbeat.is_stale) {
      return { label: 'Stale check', className: 'warn' };
    }
    if (heartbeat.ok === false) {
      return { label: 'Gateway failing', className: 'bad' };
    }
    if (session.connected === true) {
      return { label: 'Connected', className: 'ok' };
    }
    if (session.cooldown_seconds_remaining !== null && session.cooldown_seconds_remaining !== undefined) {
      return { label: 'Cooling down', className: 'warn' };
    }
    if (role === 'primary' && !session.last_error && Number(session.consecutive_failures ?? 0) === 0) {
      return { label: 'Idle', className: 'ok' };
    }
    return { label: 'Disconnected', className: 'bad' };
  }

  function connectionLabel(role) {
    return sessionStatus(role).label;
  }

  function classForConnection(role) {
    return sessionStatus(role).className;
  }

  function runStatusClass(status) {
    if (status === 'CLEAN') return 'ok';
    if (status === 'WARNINGS') return 'warn';
    return 'bad';
  }

  function killSwitchClass() {
    return killSwitch.enabled ? 'bad' : 'ok';
  }

  function killSwitchLabel() {
    return killSwitch.enabled ? 'Enabled' : 'Disabled';
  }

  function monitorLabel(status) {
    if (status?.is_stale) return 'Stale';
    if (status?.ok === true) return 'Healthy';
    if (status?.ok === false) return 'Failing';
    return 'Unknown';
  }

  function monitorClass(status) {
    if (status?.is_stale) return 'warn';
    if (status?.ok === true) return 'ok';
    if (status?.ok === false) return 'bad';
    return 'warn';
  }

  function ibGatewayLabel() {
    if (!ibGateway) return 'Unknown';
    const status = ibGateway.status;
    if (status === 'stuck_shutdown_after_existing_session') return 'Session Conflict';
    if (status === 'shutdown_in_progress') return 'Shutting Down';
    if (status === 'stuck_shutdown') return 'Stuck Shutdown';
    if (status === 'existing_session_detected') return 'Existing Session';
    if (status === 'restart_in_progress') return 'Restarting';
    if (status === 'second_factor') return '2FA Pending';
    if (status === 'deadlock_reported') return 'Deadlock';
    if (status === 'login_completed_after_restart_2fa') return 'Restart / 2FA OK';
    if (status === 'login_completed_with_config_warning') return 'Config Review';
    if (status === 'login_completed') return 'Login Complete';
    if (status === 'disabled') return 'Disabled';
    return status ?? 'Unknown';
  }

  function ibGatewayClass() {
    if (ibGateway?.severity === 'bad') return 'bad';
    if (ibGateway?.severity === 'ok') return 'ok';
    return 'warn';
  }

  function ibGatewayDetail() {
    if (!ibGateway) return 'No Gateway diagnostics have been collected.';
    const details = [ibGateway.summary].filter(Boolean);
    if (ibGateway.latest_dialog) details.push(`Dialog: ${ibGateway.latest_dialog}`);
    if (ibGateway.existing_session_detected_at) {
      details.push(`Existing session ${formatTimestamp(ibGateway.existing_session_detected_at)}`);
    }
    for (const warning of ibGateway.configuration_warnings ?? []) {
      details.push(warning);
    }
    if (ibGateway.latest_event_at) {
      details.push(`Latest ${formatTimestamp(ibGateway.latest_event_at)}`);
    }
    if (ibGateway.error) details.push(ibGateway.error);
    return details.join(' · ') || ibGateway.status || 'No recent Gateway UI state.';
  }

  function executionRuntimeLabel() {
    return executionRuntime?.effective_status ?? executionRuntime?.status ?? 'Unknown';
  }

  function executionRuntimeClass() {
    const status = executionRuntime?.effective_status ?? executionRuntime?.status;
    if (!status) return 'warn';
    if (status === 'RUNNING') return 'ok';
    if (status === 'DEGRADED') return 'warn';
    if (status === 'STALE') return 'bad';
    if (status === 'STOPPED' || status === 'DISABLED') return 'warn';
    return 'bad';
  }

  function parseTimestamp(value) {
    if (!value) return null;
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  function formatTimestamp(value) {
    const parsed = parseTimestamp(value);
    if (!parsed) {
      return value ?? 'n/a';
    }
    return timestampFormatter.format(parsed);
  }

  function formatTimestampOrNull(value) {
    const parsed = parseTimestamp(value);
    if (!parsed) {
      return null;
    }
    return timestampFormatter.format(parsed);
  }

  function ageSeconds(value) {
    const parsed = parseTimestamp(value);
    if (!parsed) return null;
    return Math.max(0, Math.round((referenceNow.getTime() - parsed.getTime()) / 1000));
  }

  function formatAge(value) {
    const seconds = ageSeconds(value);
    if (seconds === null) return 'no timestamp';
    if (seconds < 60) return `${seconds}s ago`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m ago`;
    const hours = Math.floor(minutes / 60);
    const remainder = minutes % 60;
    return remainder > 0 ? `${hours}h ${remainder}m ago` : `${hours}h ago`;
  }

  function latestTimestamp(rows, fields) {
    const fieldNames = Array.isArray(fields) ? fields : [fields];
    let latest = null;
    for (const row of rows ?? []) {
      for (const fieldName of fieldNames) {
        const parsed = parseTimestamp(row?.[fieldName]);
        if (!parsed) continue;
        if (!latest || parsed.getTime() > latest.getTime()) {
          latest = parsed;
        }
      }
    }
    return latest ? latest.toISOString() : null;
  }

  function freshnessClass(value, maxAgeSeconds = 180) {
    const seconds = ageSeconds(value);
    if (seconds === null) return 'bad';
    if (seconds <= maxAgeSeconds) return 'ok';
    if (seconds <= maxAgeSeconds * 4) return 'warn';
    return 'bad';
  }

  function compactCount(value) {
    return Number.isFinite(Number(value)) ? String(value) : 'n/a';
  }

  function normalizedSymbol(value) {
    return String(value ?? '').trim().toUpperCase().replace(/\s+/g, '-').replace(/[._]/g, '-');
  }

  function instrumentKeys(row) {
    const account = String(row?.account_key ?? '').trim().toUpperCase();
    const symbols = [
      normalizedSymbol(row?.symbol),
      normalizedSymbol(row?.local_symbol),
      normalizedSymbol(row?.primary_exchange ? `${row.primary_exchange}:${row.symbol}` : null)
    ].filter(Boolean);
    return symbols.map((symbol) => `${account}|${symbol}`);
  }

  function buildStateSyncSummary() {
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

  function parseFiniteNumber(value) {
    const parsed = Number.parseFloat(String(value ?? ''));
    if (!Number.isFinite(parsed)) return null;
    return Math.abs(parsed) < 1e12 ? parsed : null;
  }

  function formatNumericValue(value, formatter, { zeroAsUnavailable = false } = {}) {
    if (value === null || value === undefined || value === '') {
      return 'n/a';
    }

    const parsed = parseFiniteNumber(value);
    if (parsed === null) {
      return String(value);
    }
    if (zeroAsUnavailable && parsed === 0) {
      return 'n/a';
    }
    return formatter.format(parsed);
  }

  function formatQuantity(value) {
    return formatNumericValue(value, quantityFormatter);
  }

  function formatPrice(value, options = {}) {
    return formatNumericValue(value, priceFormatter, options);
  }

  function formatMoney(value) {
    return formatNumericValue(value, moneyFormatter);
  }

  function formatSignedMoney(value) {
    const parsed = parseFiniteNumber(value);
    if (parsed === null) return 'n/a';
    const prefix = parsed > 0 ? '+' : '';
    return `${prefix}${moneyFormatter.format(parsed)}`;
  }

  function moneyTone(value) {
    const parsed = parseFiniteNumber(value);
    if (parsed === null || parsed === 0) return 'subtle';
    return parsed > 0 ? 'ok' : 'bad';
  }

  function formatReturnPct(value) {
    const parsed = parseFiniteNumber(value);
    if (parsed === null) return 'n/a';
    const prefix = parsed > 0 ? '+' : '';
    return `${prefix}${parsed.toFixed(2)}%`;
  }

  function formatSignedNumber(value, digits = 2) {
    const parsed = parseFiniteNumber(value);
    if (parsed === null) return 'n/a';
    const prefix = parsed > 0 ? '+' : '';
    return `${prefix}${parsed.toFixed(digits)}`;
  }

  function formatAbsoluteNumber(value, digits = 2) {
    const parsed = parseFiniteNumber(value);
    if (parsed === null) return 'n/a';
    return Math.abs(parsed).toFixed(digits);
  }

  function formatPlainNumber(value, digits = 8) {
    const parsed = parseFiniteNumber(value);
    if (parsed === null) {
      return null;
    }
    const rounded = Number.parseFloat(parsed.toFixed(digits));
    return String(rounded);
  }

  function formatSignedDecimal(value, digits = 2) {
    const parsed = parseFiniteNumber(value);
    if (parsed === null) {
      return null;
    }
    const text = parsed.toFixed(digits);
    return parsed > 0 ? `+${text}` : text;
  }

  function firstFinite(values) {
    for (const value of values) {
      const parsed = parseFiniteNumber(value);
      if (parsed !== null) {
        return parsed;
      }
    }
    return null;
  }

  function streamPayload(snapshot) {
    if (!snapshot || typeof snapshot !== 'object') {
      return {};
    }
    return snapshot.stream && typeof snapshot.stream === 'object' ? snapshot.stream : snapshot;
  }

  function streamSymbolKeys(symbol) {
    const normalized = String(symbol ?? '').trim().toUpperCase();
    if (!normalized) {
      return [];
    }
    const keys = new Set([normalized]);
    if (normalized.includes('-')) {
      keys.add(normalized.replaceAll('-', ' '));
    }
    if (normalized.includes(' ')) {
      keys.add(normalized.replaceAll(' ', '-'));
    }
    return [...keys];
  }

  function streamRowSymbol(row) {
    return row?.symbol ?? row?.local_symbol ?? '';
  }

  function streamQuotePrice(quote) {
    const bid = parseFiniteNumber(quote?.bid_price);
    const ask = parseFiniteNumber(quote?.ask_price);
    const bidAskMidpoint = bid !== null && ask !== null ? (bid + ask) / 2 : null;
    return firstFinite([
      quote?.last_price,
      bidAskMidpoint,
      quote?.midpoint_price,
      quote?.close_price,
      bid,
      ask
    ]);
  }

  function latestStreamBar(bars) {
    if (!Array.isArray(bars)) {
      return null;
    }
    for (let index = bars.length - 1; index >= 0; index -= 1) {
      if (bars[index] && typeof bars[index] === 'object') {
        return bars[index];
      }
    }
    return null;
  }

  function previousStreamBar(bars) {
    if (!Array.isArray(bars)) {
      return null;
    }
    let seenLatest = false;
    for (let index = bars.length - 1; index >= 0; index -= 1) {
      if (!bars[index] || typeof bars[index] !== 'object') {
        continue;
      }
      if (!seenLatest) {
        seenLatest = true;
        continue;
      }
      return bars[index];
    }
    return null;
  }

  function streamTimestamp(value) {
    const parsed = parseTimestamp(value);
    return parsed ? parsed.toISOString() : null;
  }

  function buildMarketStreamMarks(snapshot) {
    const stream = streamPayload(snapshot);
    const quoteBySymbol = new Map();
    const barsBySymbol = new Map();
    const quotes = Array.isArray(stream.quotes) ? stream.quotes : [];
    const rawBarsBySymbol =
      stream.bars_by_symbol && typeof stream.bars_by_symbol === 'object'
        ? stream.bars_by_symbol
        : {};

    for (const quote of quotes) {
      if (!quote || typeof quote !== 'object') {
        continue;
      }
      for (const key of streamSymbolKeys(quote.symbol)) {
        quoteBySymbol.set(key, quote);
      }
    }

    for (const [symbol, bars] of Object.entries(rawBarsBySymbol)) {
      for (const key of streamSymbolKeys(symbol)) {
        barsBySymbol.set(key, Array.isArray(bars) ? bars : []);
      }
    }

    const marks = new Map();
    const keys = new Set([...quoteBySymbol.keys(), ...barsBySymbol.keys()]);
    for (const key of keys) {
      const quote = quoteBySymbol.get(key);
      const bars = barsBySymbol.get(key) ?? [];
      const latestBar = latestStreamBar(bars);
      const previousBar = previousStreamBar(bars);
      let price = quote ? streamQuotePrice(quote) : null;
      let source = 'quote';
      let observedAt = quote
        ? streamTimestamp(quote.last_trade_at ?? quote.updated_at)
        : null;

      if (price === null && latestBar) {
        price = parseFiniteNumber(latestBar.close);
        observedAt = streamTimestamp(latestBar.timestamp);
        source = 'bar';
      } else if (!observedAt && latestBar) {
        observedAt = streamTimestamp(latestBar.timestamp);
      }
      if (price === null) {
        continue;
      }

      let previousPrice = previousBar ? parseFiniteNumber(previousBar.close) : null;
      if (previousPrice === null && quote) {
        previousPrice = parseFiniteNumber(quote.close_price);
      }
      let direction = null;
      if (previousPrice !== null) {
        direction = price > previousPrice ? 'UP' : price < previousPrice ? 'DOWN' : 'UNCHANGED';
      }
      const canonicalSymbol = String(quote?.symbol ?? key).trim().toUpperCase();
      const mark = {
        symbol: canonicalSymbol,
        price,
        previous_price: previousPrice,
        observed_at: observedAt,
        source,
        direction
      };
      for (const candidate of [...streamSymbolKeys(key), ...streamSymbolKeys(canonicalSymbol)]) {
        marks.set(candidate, mark);
      }
    }
    return marks;
  }

  function marketMarkForRow(row, marks = marketStreamMarks) {
    for (const key of streamSymbolKeys(streamRowSymbol(row))) {
      const mark = marks.get(key);
      if (mark) {
        return mark;
      }
    }
    return null;
  }

  function applyMarketStreamToPositions(basePositions, marks) {
    if (!marks || marks.size === 0) {
      return basePositions ?? [];
    }
    let changed = false;
    const rows = (basePositions ?? []).map((position) => {
      const quantity = parseFiniteNumber(position.quantity);
      const mark = marketMarkForRow(position, marks);
      if (quantity === null || mark === null) {
        return position;
      }
      const averageCost = parseFiniteNumber(position.average_cost);
      const marketValue = quantity * mark.price;
      const unrealizedPnl =
        averageCost !== null ? quantity * (mark.price - averageCost) : null;
      changed = true;
      return {
        ...position,
        market_price: formatPlainNumber(mark.price),
        market_value: formatPlainNumber(marketValue),
        unrealized_pnl: formatPlainNumber(unrealizedPnl),
        market_price_at: mark.observed_at,
        market_data_source: 'market_stream'
      };
    });
    return changed ? rows : (basePositions ?? []);
  }

  function latestIsoTimestamp(left, right) {
    const leftDate = parseTimestamp(left);
    const rightDate = parseTimestamp(right);
    if (!leftDate) {
      return rightDate ? rightDate.toISOString() : null;
    }
    if (!rightDate) {
      return leftDate.toISOString();
    }
    return leftDate.getTime() >= rightDate.getTime()
      ? leftDate.toISOString()
      : rightDate.toISOString();
  }

  function enrichAccountDayPerformance(account, netLiquidation, markedAt) {
    const markedDate = parseTimestamp(markedAt);
    const dayPerformance = account.day_performance;
    if (!markedDate || !dayPerformance || typeof dayPerformance !== 'object') {
      return account;
    }
    const points = Array.isArray(dayPerformance.points) ? [...dayPerformance.points] : [];
    let startValue = parseFiniteNumber(dayPerformance.start_net_liquidation);
    if (startValue === null && points.length > 0) {
      startValue = parseFiniteNumber(points[0]?.net_liquidation);
    }
    if (startValue === null || startValue === 0) {
      return account;
    }

    const latestReturn = ((netLiquidation - startValue) / startValue) * 100;
    const point = {
      snapshot_at: markedDate.toISOString(),
      net_liquidation: formatPlainNumber(netLiquidation),
      return_pct: formatSignedDecimal(latestReturn) ?? '0.00'
    };
    const latestPointAt = parseTimestamp(points.at(-1)?.snapshot_at ?? points.at(-1)?.timestamp);
    if (!latestPointAt || markedDate.getTime() > latestPointAt.getTime()) {
      points.push(point);
    } else if (markedDate.getTime() === latestPointAt.getTime()) {
      points[points.length - 1] = point;
    }

    return {
      ...account,
      day_performance: {
        ...dayPerformance,
        latest_at: markedDate.toISOString(),
        latest_net_liquidation: formatPlainNumber(netLiquidation),
        latest_return_pct: formatSignedDecimal(latestReturn),
        points
      }
    };
  }

  function incrementMap(map, key, amount = 1) {
    map.set(key, (map.get(key) ?? 0) + amount);
  }

  function applyMarketStreamToAccounts(baseAccounts, basePositions, marks) {
    const accountsToMark = baseAccounts ?? [];
    if (!marks || marks.size === 0) {
      return accountsToMark;
    }

    const virtualAccounts = new Set(
      accountsToMark
        .filter((account) => account?.is_virtual)
        .map((account) => String(account.account_key ?? ''))
    );
    const accountPositionCounts = new Map();
    const accountMarkedPositionCounts = new Map();
    const accountStreamMarketValues = new Map();
    const accountDeltas = new Map();
    const accountLatestAt = new Map();

    for (const position of basePositions ?? []) {
      const accountKey = String(position?.account_key ?? '');
      const quantity = parseFiniteNumber(position?.quantity);
      if (!accountKey || quantity === null || quantity === 0) {
        continue;
      }
      incrementMap(accountPositionCounts, accountKey);
      const mark = marketMarkForRow(position, marks);
      if (!mark) {
        continue;
      }

      const marketValue = quantity * mark.price;
      let oldMarketValue = parseFiniteNumber(position.market_value);
      const oldMarketValueWasAvailable = oldMarketValue !== null;
      if (oldMarketValue === null) {
        const oldMarketPrice = parseFiniteNumber(position.market_price);
        oldMarketValue = oldMarketPrice !== null ? quantity * oldMarketPrice : 0;
      }
      incrementMap(accountMarkedPositionCounts, accountKey);
      incrementMap(accountStreamMarketValues, accountKey, marketValue);
      if (mark.observed_at) {
        accountLatestAt.set(
          accountKey,
          latestIsoTimestamp(accountLatestAt.get(accountKey), mark.observed_at)
        );
      }
      if (virtualAccounts.has(accountKey) || (oldMarketValueWasAvailable && oldMarketValue !== 0)) {
        incrementMap(accountDeltas, accountKey, marketValue - oldMarketValue);
      }
    }

    let changed = false;
    const rows = accountsToMark.map((account) => {
      const accountKey = String(account?.account_key ?? '');
      const currentNet = parseFiniteNumber(account?.net_liquidation);
      if (!accountKey || currentNet === null) {
        return account;
      }

      let valuationMethod = 'mark_delta';
      let delta = accountDeltas.get(accountKey);
      let streamNet = null;
      if (
        !virtualAccounts.has(accountKey) &&
        (accountPositionCounts.get(accountKey) ?? 0) > 0 &&
        accountMarkedPositionCounts.get(accountKey) === accountPositionCounts.get(accountKey)
      ) {
        const cashValue = parseFiniteNumber(account.total_cash_value);
        if (cashValue !== null) {
          streamNet = cashValue + (accountStreamMarketValues.get(accountKey) ?? 0);
          delta = streamNet - currentNet;
          valuationMethod = 'cash_plus_stream_positions';
        }
      }
      if (streamNet === null && delta !== undefined) {
        streamNet = currentNet + delta;
      }
      if (streamNet === null) {
        return account;
      }

      changed = true;
      const markedAt = accountLatestAt.get(accountKey) ?? null;
      const nextAccount = {
        ...account,
        net_liquidation: formatPlainNumber(streamNet),
        stream_valuation: {
          source: 'market_stream',
          method: valuationMethod,
          base_net_liquidation: formatPlainNumber(currentNet),
          mark_delta: formatPlainNumber(delta),
          stream_position_market_value: formatPlainNumber(accountStreamMarketValues.get(accountKey)),
          marked_at: markedAt
        }
      };
      return enrichAccountDayPerformance(nextAccount, streamNet, markedAt);
    });
    return changed ? rows : accountsToMark;
  }

  function workingOrderPrice(order) {
    return firstFinite([order?.working_price, order?.limit_price, order?.stop_price]);
  }

  function applyMarketStreamToOpenOrders(baseOpenOrders, marks) {
    if (!marks || marks.size === 0) {
      return baseOpenOrders ?? [];
    }
    let changed = false;
    const rows = (baseOpenOrders ?? []).map((order) => {
      const mark = marketMarkForRow(order, marks);
      if (!mark) {
        return order;
      }
      const workingPrice = workingOrderPrice(order);
      const nextOrder = {
        ...order,
        reference_market_price: formatPlainNumber(mark.price),
        reference_market_price_at: mark.observed_at,
        last_market_price_direction: mark.direction,
        market_data_source: 'market_stream'
      };
      if (workingPrice !== null) {
        const spread = workingPrice - mark.price;
        nextOrder.price_spread = formatSignedDecimal(spread);
        nextOrder.price_spread_pct =
          mark.price !== 0 ? formatSignedDecimal((spread / mark.price) * 100) : null;
        nextOrder.spread_reference =
          order.working_price_reference ?? (order.limit_price ? 'LIMIT' : 'STOP');
      }
      changed = true;
      return nextOrder;
    });
    return changed ? rows : (baseOpenOrders ?? []);
  }

  function streamBarsForSymbol(snapshot, symbol) {
    const stream = streamPayload(snapshot);
    const barsBySymbol =
      stream.bars_by_symbol && typeof stream.bars_by_symbol === 'object'
        ? stream.bars_by_symbol
        : {};
    for (const key of streamSymbolKeys(symbol)) {
      const bars = barsBySymbol[key];
      if (Array.isArray(bars)) {
        return bars;
      }
    }
    return [];
  }

  function buildLiveOmxBenchmark(fallbackBenchmark, snapshot) {
    const bars = streamBarsForSymbol(snapshot, 'OMXS30');
    const validBars = bars
      .map((bar) => ({
        timestamp: streamTimestamp(bar?.timestamp),
        value: parseFiniteNumber(bar?.close)
      }))
      .filter((bar) => bar.timestamp && bar.value !== null);
    const fallbackPoints = Array.isArray(fallbackBenchmark?.points)
      ? fallbackBenchmark.points
          .map((point) => ({
            timestamp: streamTimestamp(point?.timestamp),
            value: parseFiniteNumber(point?.value)
          }))
          .filter((point) => point.timestamp && point.value !== null)
      : [];
    const mergedByTimestamp = new Map();
    for (const point of [...fallbackPoints, ...validBars]) {
      mergedByTimestamp.set(point.timestamp, point);
    }
    const mergedPoints = [...mergedByTimestamp.values()].sort(
      (left, right) => new Date(left.timestamp).getTime() - new Date(right.timestamp).getTime()
    );
    const first = mergedPoints.find((bar) => bar.value !== 0);
    if (!first) {
      return fallbackBenchmark;
    }

    const points = mergedPoints.map((bar) => ({
      timestamp: bar.timestamp,
      value: bar.value,
      return_pct: ((bar.value - first.value) / first.value) * 100
    }));
    const latest = points.at(-1);
    return {
      ...(fallbackBenchmark ?? {}),
      label: 'OMX',
      symbol: 'OMXS30',
      status: points.length > 1 ? 'ok' : 'insufficient_data',
      error: null,
      latest_return_pct: latest?.return_pct ?? null,
      points,
      source: 'market_stream'
    };
  }

  function normalizePerformancePoints(points, valueField = 'return_pct') {
    return (points ?? [])
      .map((point) => {
        const timestamp = parseTimestamp(point.timestamp ?? point.snapshot_at);
        const value = parseFiniteNumber(point[valueField]);
        if (!timestamp || value === null) {
          return null;
        }
        return {
          timestamp,
          value
        };
      })
      .filter(Boolean);
  }

  function stockholmDstStartUtc(year) {
    const lastDay = new Date(Date.UTC(year, 2, 31));
    const lastSunday = 31 - lastDay.getUTCDay();
    return Date.UTC(year, 2, lastSunday, 1, 0, 0);
  }

  function stockholmDstEndUtc(year) {
    const lastDay = new Date(Date.UTC(year, 9, 31));
    const lastSunday = 31 - lastDay.getUTCDay();
    return Date.UTC(year, 9, lastSunday, 1, 0, 0);
  }

  function stockholmOffsetHours(year, month, day, hour, minute, second = 0) {
    const standardUtc = Date.UTC(year, month - 1, day, hour - 1, minute, second);
    return standardUtc >= stockholmDstStartUtc(year) && standardUtc < stockholmDstEndUtc(year)
      ? 2
      : 1;
  }

  function stockholmDateKey(date) {
    const parts = new Intl.DateTimeFormat('sv-SE', {
      timeZone: 'Europe/Stockholm',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit'
    }).formatToParts(date);
    const byType = Object.fromEntries(parts.map((part) => [part.type, part.value]));
    return `${byType.year}-${byType.month}-${byType.day}`;
  }

  function stockholmLocalDate(dateKey, hour, minute) {
    const [year, month, day] = dateKey.split('-').map((part) => Number.parseInt(part, 10));
    const offsetHours = stockholmOffsetHours(year, month, day, hour, minute);
    return new Date(Date.UTC(year, month - 1, day, hour - offsetHours, minute, 0));
  }

  function sessionWindowForPoints(points) {
    const latestPoint = points
      .map((point) => point.timestamp)
      .filter((timestamp) => timestamp instanceof Date && !Number.isNaN(timestamp.getTime()))
      .sort((left, right) => left.getTime() - right.getTime())
      .at(-1);
    const dateKey = stockholmDateKey(latestPoint ?? referenceNow);
    return {
      dateKey,
      open: stockholmLocalDate(dateKey, 9, 0),
      close: stockholmLocalDate(dateKey, 17, 30)
    };
  }

  function tradingSessionPoints(points, session) {
    const openTime = session.open.getTime();
    const closeTime = session.close.getTime();
    return points
      .filter((point) => {
        const timestamp = point.timestamp.getTime();
        return timestamp >= openTime && timestamp <= closeTime;
      })
      .sort((left, right) => left.timestamp.getTime() - right.timestamp.getTime());
  }

  function anchorSessionSeries(points, session, { extendToClose = false } = {}) {
    const sessionPoints = tradingSessionPoints(points, session);
    if (sessionPoints.length === 0) {
      return [];
    }
    const anchorValue = sessionPoints[0].value;
    const anchored = sessionPoints.map((point) => ({
      timestamp: point.timestamp,
      value: point.value - anchorValue
    }));
    if (anchored[0].timestamp.getTime() !== session.open.getTime()) {
      anchored.unshift({
        timestamp: session.open,
        value: 0
      });
    } else {
      anchored[0] = {
        timestamp: session.open,
        value: 0
      };
    }
    const latest = anchored.at(-1);
    if (extendToClose && latest && latest.timestamp.getTime() < session.close.getTime()) {
      anchored.push({
        timestamp: session.close,
        value: latest.value
      });
    }
    return anchored;
  }

  function interpolateSeriesValue(points, timestamp) {
    if (points.length === 0) {
      return null;
    }
    const time = timestamp.getTime();
    if (time <= points[0].timestamp.getTime()) {
      return points[0].value;
    }
    for (let index = 1; index < points.length; index += 1) {
      const previous = points[index - 1];
      const next = points[index];
      const previousTime = previous.timestamp.getTime();
      const nextTime = next.timestamp.getTime();
      if (time <= nextTime) {
        if (nextTime === previousTime) {
          return next.value;
        }
        const fraction = (time - previousTime) / (nextTime - previousTime);
        return previous.value + (next.value - previous.value) * fraction;
      }
    }
    return points.at(-1).value;
  }

  function accountDayChart(account) {
    const rawAccountPoints = normalizePerformancePoints(account.day_performance?.points);
    const rawBenchmarkPoints = normalizePerformancePoints(omxBenchmark?.points);
    const session = sessionWindowForPoints([...rawAccountPoints, ...rawBenchmarkPoints]);
    const extendToClose = referenceNow.getTime() >= session.close.getTime();
    const accountPoints = anchorSessionSeries(rawAccountPoints, session, { extendToClose });
    const benchmarkPoints = anchorSessionSeries(rawBenchmarkPoints, session, { extendToClose });
    if (accountPoints.length < 2) {
      return {
        ready: false,
        message: 'Waiting for at least two account snapshots from this trading session.'
      };
    }

    const width = 320;
    const height = 120;
    const left = 12;
    const right = 308;
    const top = 12;
    const bottom = 98;
    const openTime = session.open.getTime();
    const closeTime = session.close.getTime();
    const benchmarkAvailable = benchmarkPoints.length >= 2 && omxBenchmark?.status === 'ok';
    const relativePoints = accountPoints.map((point) => {
      const benchmarkValue = benchmarkAvailable
        ? interpolateSeriesValue(benchmarkPoints, point.timestamp)
        : 0;
      return {
        timestamp: point.timestamp,
        value: point.value - (benchmarkValue ?? 0)
      };
    });
    const yValues = accountPoints
      .map((point) => point.value)
      .concat(benchmarkAvailable ? benchmarkPoints.map((point) => point.value) : [])
      .concat(0);
    const maxAbsValue = Math.max(...yValues.map((value) => Math.abs(value)), 0.05) * 1.18;
    const minValue = -maxAbsValue;
    const maxValue = maxAbsValue;

    const xFor = (date) => {
      if (closeTime === openTime) return left;
      const clampedTime = Math.min(Math.max(date.getTime(), openTime), closeTime);
      return left + ((clampedTime - openTime) / (closeTime - openTime)) * (right - left);
    };
    const yFor = (value) => bottom - ((value - minValue) / (maxValue - minValue)) * (bottom - top);
    const pathFor = (points) =>
      points
        .map((point, index) => `${index === 0 ? 'M' : 'L'} ${xFor(point.timestamp).toFixed(2)} ${yFor(point.value).toFixed(2)}`)
        .join(' ');
    const latestAccountRaw = accountPoints.at(-1)?.value ?? null;
    const latestBenchmarkRaw = benchmarkAvailable
      ? interpolateSeriesValue(benchmarkPoints, accountPoints.at(-1).timestamp)
      : null;
    const latestRelative = relativePoints.at(-1)?.value ?? null;
    const zeroPath = `M ${left} ${yFor(0).toFixed(2)} L ${right} ${yFor(0).toFixed(2)}`;

    return {
      ready: true,
      accountPath: pathFor(accountPoints),
      benchmarkPath: benchmarkAvailable ? pathFor(benchmarkPoints) : null,
      zeroPath,
      yMin: minValue,
      yMax: maxValue,
      latestAccount: latestAccountRaw,
      latestBenchmark: latestBenchmarkRaw,
      latestRelative,
      benchmarkAvailable,
      benchmarkLabel: omxBenchmark?.symbol ?? 'OMX',
      openLabel: '09:00',
      closeLabel: '17:30'
    };
  }

  function operatorReviewClass(review) {
    const status = review.status;
    if (status !== 'OPEN') return 'neutral';
    return 'warn';
  }

  function operatorReviewLabel(review) {
    const status = review.status;
    return status === 'OPEN' ? 'OPEN' : 'ARCHIVED';
  }

  function operatorReviewActions(review) {
    const status = review.status;
    if (status !== 'OPEN') {
      return [];
    }
    return [{ operation: 'ARCHIVE', label: 'Archive', className: 'inline-button neutral' }];
  }

  function operatorReviewDetail(review) {
    if (!review?.latest_action_type) {
      return 'Not archived yet.';
    }

    const reviewedAt = formatTimestampOrNull(review.latest_action_at) ?? 'unknown time';
    const reviewedBy = review.latest_action_by ?? 'unknown operator';
    return `Archived by ${reviewedBy} at ${reviewedAt}`;
  }

  function marketDirectionArrow(direction) {
    if (direction === 'UP') return '↑';
    if (direction === 'DOWN') return '↓';
    if (direction === 'UNCHANGED') return '→';
    return '';
  }

  function marketDirectionClass(direction) {
    if (direction === 'UP') return 'ok';
    if (direction === 'DOWN') return 'bad';
    return 'subtle';
  }

  function orderSpreadLabel(order) {
    const spread = parseFiniteNumber(order.price_spread);
    const spreadPct = parseFiniteNumber(order.price_spread_pct);
    if (spread === null) {
      return 'n/a';
    }

    const direction = spread > 0 ? 'above mkt' : spread < 0 ? 'below mkt' : 'at mkt';
    const pctSuffix = spreadPct !== null ? ` (${formatSignedNumber(spreadPct)}%)` : '';
    return `${formatAbsoluteNumber(spread)} ${direction}${pctSuffix}`;
  }

  function orderTriggerDetail(order) {
    if (!order.working_price) {
      return null;
    }
    const reference = order.working_price_reference ?? order.spread_reference ?? 'trigger';
    return `${reference} ${order.working_price}`;
  }

  function orderFillSpreadLabel(order) {
    if (!order.fill_price_spread) {
      return 'n/a';
    }

    const pctSuffix = order.fill_price_spread_pct ? ` (${order.fill_price_spread_pct}%)` : '';
    return `${order.fill_price_spread}${pctSuffix}`;
  }

  function fillExitPnlLabel(fill) {
    if (!fill.realized_pnl) {
      return fill.order_role === 'EXIT' ? 'pending' : 'n/a';
    }
    return `${formatSignedMoney(fill.realized_pnl)} ${fill.realized_pnl_currency ?? fill.currency}`;
  }

  function fillStrategyLabel(fill) {
    const side = String(fill.position_side ?? '').trim().toUpperCase();
    if (side === 'LONG') {
      return 'Long';
    }
    if (side === 'SHORT') {
      return 'Short';
    }
    return 'n/a';
  }

  function fillExitPnlSearchText(fill) {
    return [
      fill.order_role,
      fillExitPnlLabel(fill),
      fill.realized_pnl_gross ? `gross ${fill.realized_pnl_gross}` : null,
      fill.realized_pnl_basis_price ? `basis ${fill.realized_pnl_basis_price}` : null
    ].filter(Boolean).join(' ');
  }

  function displayOrderPrice(value) {
    return formatPrice(value, { zeroAsUnavailable: true });
  }

  async function refreshDashboard() {
    if (refreshInFlight) {
      return;
    }
    if (typeof document !== 'undefined' && document.visibilityState === 'hidden') {
      return;
    }
    refreshInFlight = true;
    try {
      await invalidateAll();
    } finally {
      refreshInFlight = false;
    }
  }

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
    const source = new EventSource('/api/market-stream/events?bar_limit=1');
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

  onMount(() => {
    if (browser) {
      dashboardFilters = parseStoredFilters(window.localStorage.getItem(FILTER_STORAGE_KEY));
      filtersLoaded = true;
    }

    const marketStreamEvents = openMarketStreamEvents();
    const clockIntervalId = window.setInterval(() => {
      referenceNow = new Date();
    }, 5000);

    return () => {
      marketStreamEvents?.close();
      window.clearInterval(clockIntervalId);
    };
  });

  $: if (browser && filtersLoaded) {
    window.localStorage.setItem(FILTER_STORAGE_KEY, JSON.stringify(dashboardFilters));
  }

  function instructionWindowState(instruction) {
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

  function isRlCandidateInstruction(instruction) {
    return (
      instruction.state === 'MODEL_ROUTED_PENDING' ||
      instruction.order_type === 'MODEL_ROUTED' ||
      instruction.payload?.instruction?.execution?.mode === 'model_routed'
    );
  }

  function rlCandidateModelId(instruction) {
    return (
      instruction.payload?.instruction?.execution?.model_id ??
      instruction.payload?.instruction?.model ??
      'n/a'
    );
  }

  function rlCandidateWindowDisplay(instruction) {
    return `${formatTimestamp(instruction.submit_at)} to ${formatTimestamp(instruction.expire_at)}`;
  }

  function instructionGuidance(instruction) {
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

    if (instruction.state === 'COMPLETED') {
      return 'This instruction has completed its lifecycle.';
    }

    if (instruction.state === 'FAILED') {
      return 'This instruction hit a failure and should be reviewed in the ledger.';
    }

    return 'Review the ledger before taking any manual action on this instruction.';
  }

  function instructionPrimaryAction(instruction) {
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

  function hasInstructionAction(instruction) {
    return !terminalInstructionStates.has(instruction.state);
  }

  function instructionOrderDisplay(instruction, kind) {
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

  function normalizedStatus(value) {
    const normalized = String(value ?? '').trim().toUpperCase();
    return normalized || null;
  }

  function isOpenBrokerOrder(order) {
    const status = normalizedStatus(order?.status);
    return !closedOrderStatuses.has(status);
  }

  function isMarketOrder(order) {
    const orderType = normalizedStatus(order?.order_type);
    return orderType === 'MKT' || orderType === 'MARKET';
  }

  function exitSideForInstruction(instruction) {
    return normalizedStatus(instruction?.side) === 'SELL' ? 'BUY' : 'SELL';
  }

  function exitSideForPosition(position, instruction) {
    const quantity = parseFiniteNumber(position?.quantity);
    if (quantity !== null && quantity < 0) return 'BUY';
    if (quantity !== null && quantity > 0) return 'SELL';
    return exitSideForInstruction(instruction);
  }

  function orderMatchesPositionAccount(order, position) {
    const orderAccount = normalizedStatus(order?.account_key);
    const positionAccount = normalizedStatus(position?.account_key);
    return !orderAccount || !positionAccount || orderAccount === positionAccount;
  }

  function orderMatchesPositionInstrument(order, position) {
    const orderSymbols = [order?.local_symbol, order?.symbol].map(normalizedSymbol).filter(Boolean);
    const positionSymbols = [position?.local_symbol, position?.symbol].map(normalizedSymbol).filter(Boolean);
    return orderSymbols.some((symbol) => positionSymbols.includes(symbol));
  }

  function orderQuantityCoversPosition(order, position) {
    const orderQuantity = parseFiniteNumber(order?.total_quantity);
    const positionQuantity = parseFiniteNumber(position?.quantity);
    if (orderQuantity === null || positionQuantity === null) {
      return true;
    }
    return Math.abs(orderQuantity) >= Math.abs(positionQuantity);
  }

  function orderMatchesInstructionAccount(order, instruction) {
    const orderAccount = normalizedStatus(order?.account_key);
    const instructionAccount = normalizedStatus(instruction?.account_key);
    return !orderAccount || !instructionAccount || orderAccount === instructionAccount;
  }

  function orderMatchesInstructionInstrument(order, instruction) {
    const payloadInstrument = instruction?.payload?.instruction?.instrument ?? {};
    const orderSymbols = [order?.local_symbol, order?.symbol].map(normalizedSymbol).filter(Boolean);
    const instructionSymbols = [
      instruction?.symbol,
      payloadInstrument?.local_symbol,
      payloadInstrument?.symbol
    ].map(normalizedSymbol).filter(Boolean);
    return orderSymbols.some((symbol) => instructionSymbols.includes(symbol));
  }

  function remainingExitQuantityForInstruction(instruction) {
    const entryQuantity = parseFiniteNumber(
      instruction?.entry_filled_quantity ?? instruction?.entry_submitted_quantity
    );
    if (entryQuantity === null) {
      return null;
    }
    const exitQuantity = parseFiniteNumber(instruction?.exit_filled_quantity) ?? 0;
    return Math.max(0, Math.abs(entryQuantity) - Math.abs(exitQuantity));
  }

  function orderQuantityCoversInstruction(order, instruction) {
    const orderQuantity = parseFiniteNumber(order?.total_quantity);
    const remainingQuantity = remainingExitQuantityForInstruction(instruction);
    if (orderQuantity === null || remainingQuantity === null) {
      return true;
    }
    return Math.abs(orderQuantity) >= remainingQuantity;
  }

  function liveMarketExitOrderForPosition(position, instruction, orderRows = openOrders) {
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

  function liveMarketExitOrderForInstruction(instruction, orderRows = openOrders) {
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

  function liveEntryOrderForInstruction(instruction, orderRows = openOrders) {
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

  function workingEntryGuidance(order) {
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

  function openOrderReference(order) {
    return (
      order?.order_ref ??
      (order?.external_perm_id ? `perm ${order.external_perm_id}` : null) ??
      (order?.external_order_id ? `order ${order.external_order_id}` : null) ??
      'broker order'
    );
  }

  function instructionForcesNextOpenExit(instruction) {
    return instruction?.payload?.instruction?.exit?.force_exit_next_session_open === true;
  }

  function instructionNextSessionExit(instruction) {
    return instruction?.runtime_schedule?.next_session_exit ?? null;
  }

  function instructionNextSessionOpenAt(instruction) {
    const nextSessionExit = instructionNextSessionExit(instruction);
    return parseTimestamp(
      nextSessionExit?.next_session_open_utc ?? nextSessionExit?.next_session_open_local
    );
  }

  function instructionExitPlanState(instruction) {
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

  function normalizeIntentText(value, { upper = true } = {}) {
    const normalized = String(value ?? '').trim();
    return upper ? normalized.toUpperCase() : normalized.toLowerCase();
  }

  function instructionBookSide(instruction) {
    const payloadInstruction = instruction?.payload?.instruction ?? {};
    const bookSide =
      payloadInstruction?.account?.book_side ??
      payloadInstruction?.intent?.position_side;
    if (bookSide) {
      return normalizeIntentText(bookSide);
    }
    return normalizeIntentText(instruction?.side) === 'SELL' ? 'SHORT' : 'LONG';
  }

  function accountLabelLookup(accountRows) {
    return new Map(
      (accountRows ?? [])
        .map((account) => [
          normalizeIntentText(account.account_key),
          account.account_label ?? account.account_key
        ])
        .filter(([accountKey]) => accountKey)
    );
  }

  function instructionCleanupSelector(instruction) {
    return {
      account_key: normalizeIntentText(instruction.account_key),
      book_key: normalizeIntentText(instruction.book_key, { upper: false }),
      book_side: instructionBookSide(instruction),
      symbol: normalizeIntentText(instruction.symbol),
      exchange: normalizeIntentText(instruction.exchange),
      currency: normalizeIntentText(instruction.currency)
    };
  }

  function instructionCleanupGroupKey(instruction) {
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

  function cleanupGroupLatestTimestamp(group) {
    return latestTimestamp(group.instructions, ['activity_at', 'updated_at', 'created_at', 'submit_at']);
  }

  function cleanupGroupClassName(group) {
    if (group.entryCount > 0 && group.positionOwnerCount > 0) return 'warn';
    if (group.entryCount > 1) return 'warn';
    if (group.entryCount > 0) return 'neutral';
    return 'ok';
  }

  function groupIntentCleanupRows(instructionRows, accountRows) {
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

  function candidateLifecyclePolicy(instruction) {
    return instruction?.payload?.instruction?.lifecycle ?? null;
  }

  function candidateLifecycleLabel(instruction) {
    const lifecycle = candidateLifecyclePolicy(instruction);
    if (!lifecycle) {
      return 'Policy missing';
    }
    const entryLimit = lifecycle.max_entry_orders ?? 'n/a';
    const exitLimit = lifecycle.max_exit_orders ?? 'n/a';
    return `${entryLimit} entry / ${exitLimit} exit`;
  }

  function groupSourceIntentRows(candidateRows, accountRows) {
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

  function positionInstructionMatches(position, instruction) {
    if (!position || !instruction) return false;
    if (!positionOwningInstructionStates.has(instruction.state)) return false;
    if (String(position.account_key ?? '').toUpperCase() !== String(instruction.account_key ?? '').toUpperCase()) {
      return false;
    }
    return normalizedSymbol(position.local_symbol ?? position.symbol) === normalizedSymbol(instruction.symbol);
  }

  function activeInstructionsForPosition(position, instructionRows = executionInstructions) {
    return instructionRows
      .filter((instruction) => positionInstructionMatches(position, instruction))
      .sort((left, right) => {
        const leftAt = parseTimestamp(left.activity_at ?? left.updated_at)?.getTime() ?? 0;
        const rightAt = parseTimestamp(right.activity_at ?? right.updated_at)?.getTime() ?? 0;
        return rightAt - leftAt;
      });
  }

  function positionExitPlan(position, instructionRows = executionInstructions) {
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

  function positionExitPlanSearchText(position, instructionRows = executionInstructions) {
    const exitPlan = positionExitPlan(position, instructionRows);
    return [
      exitPlan.label,
      exitPlan.className,
      exitPlan.detail,
      exitPlan.instructionId
    ].filter(Boolean).join(' ');
  }

  function isOpenReview(review) {
    return review.status === 'OPEN';
  }

  function groupBrokerAttentionRows(rows) {
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
        eventType: row.event_type,
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

  function reconciliationGroupMessageKey(stage, message) {
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

  function groupReconciliationRuns(runs) {
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
          await refreshDashboard();
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
  $: stateSync = buildStateSyncSummary();
</script>

<svelte:head>
  <title>IBKR Trader Operator Dashboard</title>
</svelte:head>

<div class="page">
  <header class="hero" id="overview">
    <div class="hero-copy">
      <p class="eyebrow">IBKR Trader</p>
      <h1>Operator Dashboard</h1>
      <p class="lede">
        Durable operator view over accounts, positions, orders, fills, instructions,
        broker attention, and reconciliation history.
      </p>
    </div>
    <div class="hero-meta">
      <div>
        <span>API</span>
        <strong>{data.apiBaseUrl}</strong>
      </div>
      <div>
        <span>Page updated</span>
        <strong>{formatTimestamp(data.generatedAt)}</strong>
        <button class="inline-button neutral hero-refresh" type="button" on:click={refreshDashboard} disabled={refreshInFlight}>
          {refreshInFlight ? 'Refreshing...' : 'Refresh Snapshot'}
        </button>
      </div>
      <div>
        <span>Snapshot generated</span>
        <strong>{formatTimestamp(operatorSnapshot.generated_at)}</strong>
      </div>
      <div>
        <span>Live market data</span>
        <strong class={marketStreamStatusClass()}>{marketStreamStatusLabel()}</strong>
        <small>{marketStreamStatusDetail()}</small>
      </div>
      <div>
        <span>Market timezone</span>
        <strong>{marketTimeZone}</strong>
      </div>
    </div>
  </header>

  <section class="stat-grid">
    <article class="stat-card">
      <span>Primary Broker Session</span>
      <strong class={classForConnection('primary')}>{connectionLabel('primary')}</strong>
      <small>Client ID {data.health.broker_sessions.primary.client_id}, on demand</small>
    </article>

    <article class="stat-card">
      <span>Diagnostic Session</span>
      <strong class={classForConnection('diagnostic')}>{connectionLabel('diagnostic')}</strong>
      <small>Client ID {data.health.broker_sessions.diagnostic.client_id}</small>
    </article>

    <article class="stat-card">
      <span>Gateway Heartbeat</span>
      <strong class={monitorClass(brokerMonitor.heartbeat)}>
        {monitorLabel(brokerMonitor.heartbeat)}
      </strong>
      <small>
        {#if brokerMonitor.heartbeat?.is_stale}
          Last check {formatTimestampOrNull(brokerMonitor.heartbeat?.last_attempt_at) ?? 'never'}
        {:else}
          {formatTimestampOrNull(brokerMonitor.heartbeat?.last_success_at) ??
            brokerMonitor.heartbeat?.error ??
            'No heartbeat has completed yet.'}
        {/if}
      </small>
    </article>

    <article class="stat-card">
      <span>Gateway UI State</span>
      <strong class={ibGatewayClass()}>{ibGatewayLabel()}</strong>
      <small>{ibGatewayDetail()}</small>
    </article>

    <article class="stat-card">
      <span>Snapshot Refresh</span>
      <strong class={monitorClass(brokerMonitor.snapshot_refresh)}>
        {monitorLabel(brokerMonitor.snapshot_refresh)}
      </strong>
      <small>
        {#if brokerMonitor.snapshot_refresh?.is_stale}
          Last check {formatTimestampOrNull(brokerMonitor.snapshot_refresh?.last_attempt_at) ?? 'never'}
        {:else if brokerMonitor.snapshot_refresh?.ok === true}
          {brokerMonitor.snapshot_refresh.account_count} accounts ·
          {brokerMonitor.snapshot_refresh.position_count} positions ·
          {brokerMonitor.snapshot_refresh.open_order_count} open orders
        {:else}
          {brokerMonitor.snapshot_refresh?.error ?? 'No snapshot refresh has completed yet.'}
        {/if}
      </small>
    </article>

    <article class="stat-card">
      <span>Execution Runtime</span>
      <strong class={executionRuntimeClass()}>{executionRuntimeLabel()}</strong>
      <small>
        {#if executionRuntime?.is_stale}
          Last heartbeat {formatTimestampOrNull(executionRuntime?.heartbeat_at) ?? 'never'}
        {:else}
          {formatTimestampOrNull(executionRuntime?.last_successful_cycle_at) ??
            executionRuntime?.last_error ??
            'No execution-runtime status has been persisted yet.'}
        {/if}
      </small>
    </article>

    <article class="stat-card">
      <span>Kill Switch</span>
      <strong class={killSwitchClass()}>{killSwitchLabel()}</strong>
      <small>{killSwitch.reason ?? 'New entries are allowed.'}</small>
    </article>

    <article class="stat-card">
      <span>Accounts</span>
      <strong>{accounts.length}</strong>
      <small>Latest durable account snapshots</small>
    </article>

    <article class="stat-card">
      <span>Open Positions</span>
      <strong>{positions.length}</strong>
      <small>Latest non-zero position snapshots</small>
    </article>

    <article class="stat-card">
      <span>Open Orders</span>
      <strong>{openOrders.length}</strong>
      <small>Persisted broker orders not in a terminal state</small>
    </article>

    <article class="stat-card">
      <span>RL Candidates</span>
      <strong>{rlCandidateInstructions.length}</strong>
      <small>Daily source names retained for bar-by-bar RL decisions</small>
    </article>

    <article class="stat-card">
      <span>Execution Queue</span>
      <strong>{executionInstructions.length}</strong>
      <small>Translated orders owned by the trader runtime</small>
    </article>

    <article class="stat-card">
      <span>Broker Attention</span>
      <strong>{brokerAttention.length}</strong>
      <small>Recent rejects and warning signals</small>
    </article>

    <article class="stat-card">
      <span>Reconciliation Warnings</span>
      <strong>{warningRuns.length}</strong>
      <small>Recent runs with issues</small>
    </article>
  </section>

  {#if stateSync}
    <section class={`panel sync-panel ${stateSync.className === 'bad' ? 'danger' : ''}`} id="state-sync">
      <div class="panel-head">
        <div>
          <h2>State Sync</h2>
          <p>Shows which source each dashboard section came from and whether the broker snapshot agrees with the persisted ledger rows.</p>
        </div>
        <span class={`pill ${stateSync.className}`}>{stateSync.label}</span>
      </div>

      <div class="sync-grid">
        {#each stateSync.items as item}
          <div class="sync-item">
            <div class="sync-item-head">
              <strong>{item.label}</strong>
              <span class={`status-dot ${item.className}`}></span>
            </div>
            <span>{item.countLabel}</span>
            <small>{item.source}</small>
            <small>{item.at ? `${formatAge(item.at)} · ${formatTimestamp(item.at)}` : 'No timestamp available'}</small>
          </div>
        {/each}
      </div>

      {#if stateSync.warnings.length > 0}
        <ul class="sync-warning-list">
          {#each stateSync.warnings as warning}
            <li class={warning.className}>{warning.text}</li>
          {/each}
        </ul>
      {/if}
    </section>
  {/if}

  {#if endpointErrors.length > 0}
    <section class="panel danger">
      <div class="panel-head">
        <h2>Endpoint Errors</h2>
        <p>The dashboard shows real failures when parts of the stack are unavailable.</p>
      </div>
      <ul class="attention-list">
        {#each endpointErrors as [name, value]}
          <li>
            <strong>{name}</strong>
            <span>{value}</span>
          </li>
        {/each}
      </ul>
    </section>
  {/if}

  {#if killSwitch.enabled}
    <section class="panel danger">
      <div class="panel-head">
        <h2>Kill Switch Active</h2>
        <p>
          New entries are blocked in the API and runtime until the durable kill switch is
          disabled.
        </p>
      </div>
      <ul class="attention-list">
        <li>
          <strong>Reason</strong>
          <span>{killSwitch.reason ?? 'No reason was recorded.'}</span>
        </li>
        <li>
          <strong>Updated by</strong>
          <span>{killSwitch.updated_by ?? 'n/a'}</span>
        </li>
        <li>
          <strong>Changed at</strong>
          <span>{formatTimestamp(killSwitch.last_changed_at)}</span>
        </li>
      </ul>
    </section>
  {/if}

  <section class="two-up" id="controls">
    <section class={`panel control-panel ${killSwitch.enabled ? 'danger' : ''}`}>
      <div class="panel-head">
        <div>
          <h2>Kill Switch Control</h2>
          <p>
            Toggle the durable global kill switch. This blocks new entry submissions in both the
            API and runtime.
          </p>
        </div>
        <span class={`pill ${killSwitch.enabled ? 'bad' : 'ok'}`}>{killSwitchLabel()}</span>
      </div>

      {#if killSwitchResult}
        <p class={`action-feedback ${killSwitchResult.ok ? 'ok' : 'bad'}`}>
          {killSwitchResult.message}
        </p>
      {/if}

      <form
        method="POST"
        action="?/killSwitch"
        class="control-form"
        use:enhance={enhanceDashboardAction('kill-switch-toggle')}
      >
        <input
          type="hidden"
          name="enabled"
          value={killSwitch.enabled ? 'false' : 'true'}
        />
        <label>
          <span>Reason</span>
          <textarea
            name="reason"
            rows="3"
            placeholder={
              killSwitch.enabled
                ? 'Optional note for disabling the kill switch'
                : 'Why are we blocking new entries?'
            }
          >{killSwitch.reason ?? ''}</textarea>
        </label>

        <div class="form-actions">
          <button
            class={`action-button ${killSwitch.enabled ? 'neutral' : 'danger'} ${buttonStateClass('kill-switch-toggle')}`}
            type="submit"
            data-action-key="kill-switch-toggle"
            disabled={buttonIsBusy('kill-switch-toggle')}
          >
            {buttonLabel(
              'kill-switch-toggle',
              killSwitch.enabled ? 'Disable Kill Switch' : 'Enable Kill Switch'
            )}
          </button>
        </div>
      </form>
    </section>

    <section class="panel control-panel">
      <div class="panel-head">
        <div>
          <h2>Archive Dashboard Rows</h2>
          <p>
            Hide expired RL candidates and terminal instruction rows from the default dashboard while keeping
            their audit history in the API.
          </p>
        </div>
      </div>

      {#if archiveResult}
        <p class={`action-feedback ${archiveResult.ok ? 'ok' : 'bad'}`}>
          {archiveResult.message}
        </p>
      {/if}

      <form
        method="POST"
        action="?/archiveDashboardNoise"
        class="control-form"
        use:enhance={enhanceDashboardAction('archive-dashboard-noise')}
      >
        <div class="form-actions">
          <button
            class={`action-button ${buttonStateClass('archive-dashboard-noise')}`}
            type="submit"
            data-action-key="archive-dashboard-noise"
            disabled={buttonIsBusy('archive-dashboard-noise')}
          >
            {buttonLabel('archive-dashboard-noise', 'Archive Old Rows')}
          </button>
        </div>
      </form>
    </section>
  </section>

  <section class="panel control-panel">
    <div class="panel-head">
      <div>
        <h2>Reconciliation Control</h2>
        <p>
          Run a fresh startup reconciliation pass against persisted state and the current broker
          snapshot when warnings need a direct operator check.
        </p>
      </div>
    </div>

    {#if startupReconcileResult}
      <p class={`action-feedback ${startupReconcileResult.ok ? 'ok' : 'bad'}`}>
        {startupReconcileResult.message}
      </p>
    {/if}

    <form
      method="POST"
      action="?/startupReconcile"
      class="control-form"
      use:enhance={enhanceDashboardAction('startup-reconcile')}
    >
      <div class="form-actions">
        <button
          class={`action-button ${buttonStateClass('startup-reconcile')}`}
          type="submit"
          data-action-key="startup-reconcile"
          disabled={buttonIsBusy('startup-reconcile')}
        >
          {buttonLabel('startup-reconcile', 'Run Startup Reconciliation')}
        </button>
      </div>
    </form>
  </section>

  <section class="panel control-panel" id="intent-cleanup">
    <div class="panel-head">
      <div>
        <h2>Intent Cleanup</h2>
        <p>Entry rows only; position owners and next-open close flags stay in the ledger.</p>
      </div>
      <div class="panel-tools">
        <span class="subtle">
          {sourceIntentGroups.length} source intent(s) · {actionableIntentCleanupGroups.length} cleanable group(s)
        </span>
        <form
          method="POST"
          action="?/intentCleanup"
          class="inline-action-form"
          use:enhance={enhanceDashboardAction('intent-cleanup-virtual-total')}
        >
          <input type="hidden" name="scope" value="virtual_total" />
          <input type="hidden" name="apply" value="true" />
          <input type="hidden" name="cancel_all_entries" value="true" />
          <button
            class={`inline-button danger ${buttonStateClass('intent-cleanup-virtual-total')}`}
            type="submit"
            data-action-key="intent-cleanup-virtual-total"
            disabled={buttonIsBusy('intent-cleanup-virtual-total') || virtualIntentCleanupGroupCount === 0}
          >
            {buttonLabel('intent-cleanup-virtual-total', 'Clean Virtual Entries')}
          </button>
        </form>
      </div>
    </div>

    {#if intentCleanupResult}
      <p class={`action-feedback ${intentCleanupResult.ok ? 'ok' : 'bad'}`}>
        {intentCleanupResult.message}
      </p>
    {/if}

    {#if sourceIntentGroups.length === 0 && intentCleanupGroups.length === 0}
      <p class="empty">No active intent groups are visible.</p>
    {:else}
      {#if sourceIntentGroups.length > 0}
        <h3 class="section-subhead">Model-Routed Source Intents</h3>
        <div class="table-wrap intent-table-wrap">
          <table>
            <thead>
              <tr>
                <th>Intent</th>
                <th>Account</th>
                <th>Model</th>
                <th>State</th>
                <th>Policy</th>
                <th>Latest</th>
              </tr>
            </thead>
            <tbody>
              {#each sourceIntentGroups as group}
                <tr>
                  <td>
                    <strong>{group.selector.symbol}</strong>
                    <small class="row-detail">
                      {group.selector.exchange} · {group.selector.currency} · {group.selector.book_key}/{group.selector.book_side}
                    </small>
                    {#if group.candidateRefs}
                      <small class="row-detail mono">{group.candidateRefs}</small>
                    {/if}
                  </td>
                  <td>
                    {group.accountLabel}
                    {#if group.isVirtual}<span class="mini-badge">Virtual</span>{/if}
                    <small class="row-detail mono">{group.selector.account_key}</small>
                  </td>
                  <td>{group.modelLabel ?? 'n/a'}</td>
                  <td>
                    <span class="pill neutral">{group.stateLabel ?? 'UNKNOWN'}</span>
                    <small class="row-detail">{group.candidateCount} source row(s)</small>
                  </td>
                  <td>{group.policyLabel ?? 'n/a'}</td>
                  <td>{formatTimestamp(group.latestAt)}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {/if}

      {#if intentCleanupGroups.length > 0}
        <h3 class="section-subhead">Broker Execution Intents</h3>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Intent</th>
                <th>Account</th>
                <th>Entries</th>
                <th>Position Owner</th>
                <th>Latest</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {#each intentCleanupGroups as group}
                <tr>
                  <td>
                    <strong>{group.selector.symbol}</strong>
                    <small class="row-detail">
                      {group.selector.exchange} · {group.selector.currency} · {group.selector.book_key}/{group.selector.book_side}
                    </small>
                  </td>
                  <td>
                    {group.accountLabel}
                    {#if group.isVirtual}<span class="mini-badge">Virtual</span>{/if}
                    <small class="row-detail mono">{group.selector.account_key}</small>
                  </td>
                  <td>
                    <span class={`pill ${group.className}`}>
                      {group.entryCount} active
                    </span>
                    <small class="row-detail">
                      {group.pendingEntryCount} pending · {group.submittedEntryCount} submitted
                    </small>
                    {#if group.entryRefs}
                      <small class="row-detail mono">{group.entryRefs}</small>
                    {/if}
                  </td>
                  <td>
                    {#if group.positionOwnerCount > 0}
                      <span class="pill ok">
                        {group.forceNextOpen ? 'Next Open' : 'Owned'}
                      </span>
                      <small class="row-detail">{group.positionOwnerCount} owner row(s)</small>
                      {#if group.ownerRefs}
                        <small class="row-detail mono">{group.ownerRefs}</small>
                      {/if}
                    {:else}
                      <span class="pill neutral">None</span>
                    {/if}
                  </td>
                  <td>{formatTimestamp(group.latestAt)}</td>
                  <td class="actions-cell">
                    {#if group.entryCount > 0}
                      {@const cleanupKey = `intent-cleanup-${group.key}`}
                      <form
                        method="POST"
                        action="?/intentCleanup"
                        class="inline-action-form"
                        use:enhance={enhanceDashboardAction(cleanupKey)}
                      >
                        <input type="hidden" name="scope" value="group" />
                        <input type="hidden" name="apply" value="true" />
                        <input type="hidden" name="cancel_all_entries" value="true" />
                        <input type="hidden" name="account_key" value={group.selector.account_key} />
                        <input type="hidden" name="book_key" value={group.selector.book_key} />
                        <input type="hidden" name="book_side" value={group.selector.book_side} />
                        <input type="hidden" name="symbol" value={group.selector.symbol} />
                        <input type="hidden" name="exchange" value={group.selector.exchange} />
                        <input type="hidden" name="currency" value={group.selector.currency} />
                        <input
                          type="hidden"
                          name="reason"
                          value={`Dashboard intent cleanup for ${group.selector.account_key} ${group.selector.book_side} ${group.selector.symbol}.`}
                        />
                        <button
                          class={`inline-button danger ${buttonStateClass(cleanupKey)}`}
                          type="submit"
                          data-action-key={cleanupKey}
                          disabled={buttonIsBusy(cleanupKey)}
                        >
                          {buttonLabel(cleanupKey, 'Clean Entries')}
                        </button>
                      </form>
                    {:else}
                      <span class="subtle">No entry rows</span>
                    {/if}
                  </td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {/if}
    {/if}
  </section>

  <section class="panel" id="accounts">
    <div class="panel-head">
      <h2>Accounts</h2>
      <p>Latest persisted account snapshots from the ledger.</p>
    </div>
    {#if accounts.length === 0}
      <p class="empty">No durable account snapshots are available yet.</p>
    {:else}
      <div class="account-grid">
        {#each accounts as account}
          <article class="account-card">
            <div class="account-title">
              <h3>{account.account_label ?? account.account_key}</h3>
              <div class="pill-row compact">
                <span class="pill neutral">{account.account_key}</span>
                {#if account.is_virtual}
                  <span class="pill warn">Virtual</span>
                {/if}
              </div>
            </div>
            <dl>
              <div><dt>Snapshot</dt><dd>{formatTimestamp(account.snapshot_at)}</dd></div>
              <div><dt>Net liquidation</dt><dd>{formatMoney(account.net_liquidation)} {account.currency ?? account.base_currency ?? ''}</dd></div>
              <div><dt>Total cash</dt><dd>{formatMoney(account.total_cash_value)} {account.currency ?? account.base_currency ?? ''}</dd></div>
              <div><dt>Buying power</dt><dd>{formatMoney(account.buying_power)} {account.currency ?? account.base_currency ?? ''}</dd></div>
              <div><dt>Available funds</dt><dd>{formatMoney(account.available_funds)} {account.currency ?? account.base_currency ?? ''}</dd></div>
              <div><dt>Excess liquidity</dt><dd>{formatMoney(account.excess_liquidity)} {account.currency ?? account.base_currency ?? ''}</dd></div>
              <div><dt>Cushion</dt><dd>{account.cushion ?? 'n/a'}</dd></div>
            </dl>

            {#if true}
              {@const chart = accountDayChart(account)}
              <div class="account-chart">
                <div class="account-chart-head">
                  <div>
                    <span>Today vs OMX</span>
                    <strong>{formatReturnPct(chart.latestRelative)}</strong>
                  </div>
                </div>

                {#if chart.ready}
                  <svg class="performance-chart" viewBox="0 0 320 120" role="img" aria-label={`Trading day performance for ${account.account_key} versus OMX`}>
                    <path class="chart-zero" d={chart.zeroPath}></path>
                    {#if chart.benchmarkPath}
                      <path class="chart-line benchmark-line" d={chart.benchmarkPath}></path>
                    {/if}
                    <path class="chart-line account-line" d={chart.accountPath}></path>
                  </svg>
                  <div class="chart-axis-labels">
                    <span>{chart.openLabel}</span>
                    <span>{chart.closeLabel}</span>
                  </div>
                  <div class="chart-legend">
                    <span><i class="account-dot"></i>Account {formatReturnPct(chart.latestAccount)}</span>
                    <span class:subtle={!chart.benchmarkAvailable}>
                      <i class="benchmark-dot"></i>{chart.benchmarkLabel} {formatReturnPct(chart.latestBenchmark)}
                    </span>
                  </div>
                {:else}
                  <p class="chart-empty">{chart.message}</p>
                {/if}
              </div>
            {/if}
          </article>
        {/each}
      </div>
    {/if}
  </section>

  <section class="two-up" id="operations">
    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>Broker Attention</h2>
          <p>Active broker-side warnings and rejects, grouped so repeated noise collapses into one row.</p>
        </div>
        <div class="panel-tools">
          <span class="subtle">{filteredBrokerAttention.length} active groups</span>
          <form
            method="POST"
            action="?/acknowledgeAllLogs"
            class="inline-action-form"
            use:enhance={enhanceDashboardAction('clear-all-visible-logs')}
          >
            <input type="hidden" name="event_ids" value={visibleBrokerAttentionEventIds.join(',')} />
            <input type="hidden" name="issue_ids" value={visibleReconciliationIssueIds.join(',')} />
            <button
              class={`inline-button neutral ${buttonStateClass('clear-all-visible-logs')}`}
              type="submit"
              data-action-key="clear-all-visible-logs"
              disabled={buttonIsBusy('clear-all-visible-logs') || (visibleBrokerAttentionEventIds.length === 0 && visibleReconciliationIssueIds.length === 0)}
            >
              {buttonLabel('clear-all-visible-logs', 'Archive All Visible')}
            </button>
          </form>
        </div>
      </div>
      {#if acknowledgeAllLogsResult}
        <p class={`action-feedback ${acknowledgeAllLogsResult.ok ? 'ok' : 'bad'}`}>
          {acknowledgeAllLogsResult.message}
        </p>
      {/if}
      {#if brokerAttentionActionResult}
        <p class={`action-feedback ${brokerAttentionActionResult.ok ? 'ok' : 'bad'}`}>
          {brokerAttentionActionResult.message}
        </p>
      {/if}
      {#if filteredBrokerAttention.length === 0}
        <p class="empty">No active broker attention items are visible.</p>
      {:else}
        <ul class="attention-list">
          {#each filteredBrokerAttention as attention}
            <li>
              <div class="attention-main">
                <span class="pill warn">{attention.eventType}</span>
                <strong>{attention.symbol}</strong>
                <span>{attention.accountLabel ?? attention.accountKey}</span>
                <span class="pill neutral">{attention.count}x</span>
              </div>
              <p>{attention.message}</p>
              <small>
                {formatTimestamp(attention.latestAt)}
                {#if attention.orderRefSummary}
                  · <span class="mono">{attention.orderRefSummary}</span>
                {/if}
              </small>
              {#if attention.latestStatusAfter}
                <small>Status after: {attention.latestStatusAfter}</small>
              {/if}
              {#if attention.noteSummary}
                <small>{attention.noteSummary}</small>
              {/if}
              <div class="inline-actions">
                <form
                  method="POST"
                  action="?/brokerAttentionAction"
                  class="inline-action-form"
                  use:enhance={enhanceDashboardAction(`broker-attention-${attention.key}`)}
                >
                  <input type="hidden" name="event_ids" value={attention.eventIdsCsv} />
                  <input type="hidden" name="operation" value="ARCHIVE" />
                  <button
                    class={`inline-button neutral ${buttonStateClass(`broker-attention-${attention.key}`)}`}
                    type="submit"
                    data-action-key={`broker-attention-${attention.key}`}
                    disabled={buttonIsBusy(`broker-attention-${attention.key}`)}
                  >
                    {buttonLabel(`broker-attention-${attention.key}`, 'Archive')}
                  </button>
                </form>
              </div>
            </li>
          {/each}
        </ul>
      {/if}
    </section>

    <section class="panel">
      <div class="panel-head">
        <div>
          <h2>Recent Reconciliation Runs</h2>
          <p>Active reconciliation warnings grouped across recent runs so repeated issues collapse cleanly.</p>
        </div>
        <div class="panel-tools">
          <span class="subtle">
            {filteredReconciliation.length} of {aggregatedReconciliation.length} active groups
          </span>
          <form
            method="POST"
            action="?/archiveAllReconciliation"
            class="inline-action-form"
            use:enhance={enhanceDashboardAction('archive-all-reconciliation')}
          >
            <button
              class={`inline-button neutral ${buttonStateClass('archive-all-reconciliation')}`}
              type="submit"
              data-action-key="archive-all-reconciliation"
              disabled={buttonIsBusy('archive-all-reconciliation') || aggregatedReconciliation.length === 0}
            >
              {buttonLabel('archive-all-reconciliation', 'Archive All')}
            </button>
          </form>
        </div>
      </div>
      {#if reconciliationClearResult}
        <p class={`action-feedback ${reconciliationClearResult.ok ? 'ok' : 'bad'}`}>
          {reconciliationClearResult.message}
        </p>
      {/if}
      {#if reconciliationIssueActionResult}
        <p class={`action-feedback ${reconciliationIssueActionResult.ok ? 'ok' : 'bad'}`}>
          {reconciliationIssueActionResult.message}
        </p>
      {/if}
      {#if filteredReconciliation.length === 0}
        <p class="empty">No active reconciliation warnings are visible.</p>
      {:else}
        <div class="reconciliation-list">
          {#each filteredReconciliation as run}
            <article class="reconciliation-card">
              <div class="reconciliation-topline">
                <div>
                  <h3>{run.runKind}</h3>
                  <p>{formatTimestamp(run.latestAt)}</p>
                </div>
                <div class="run-pills">
                  <span class={`pill ${run.severity === 'ERROR' ? 'bad' : 'warn'}`}>{run.severity}</span>
                  <span class="pill neutral">{run.count + run.suppressedCount}x</span>
                  <span class="pill neutral">{run.runCount} runs</span>
                </div>
              </div>
              <ul class="issue-list">
                <li>
                  <div class="issue-main">
                    <strong>{run.stage}</strong>
                    <span class="pill neutral">{run.count + run.suppressedCount}x</span>
                  </div>
                  <span>{run.message}</span>
                  {#if run.suppressedCount > 0}
                    <small>
                      Suppressed {run.suppressedCount} repeated broker-down audit
                      {run.suppressedCount === 1 ? '' : 's'} in the current cooldown window.
                    </small>
                  {/if}
                  {#if run.instructionId}
                    <small class="mono">{run.instructionId}</small>
                  {/if}
                  {#if run.latestCompletedAt}
                    <small>Latest run completed at {formatTimestamp(run.latestCompletedAt)}</small>
                  {/if}
                  <div class="inline-actions">
                    <form
                      method="POST"
                      action="?/reconciliationIssueAction"
                      class="inline-action-form"
                      use:enhance={enhanceDashboardAction(`reconciliation-${run.key}`)}
                    >
                      <input type="hidden" name="issue_ids" value={run.issueIdsCsv} />
                      <input type="hidden" name="operation" value="ARCHIVE" />
                      <button
                        class={`inline-button neutral ${buttonStateClass(`reconciliation-${run.key}`)}`}
                        type="submit"
                        data-action-key={`reconciliation-${run.key}`}
                        disabled={buttonIsBusy(`reconciliation-${run.key}`)}
                      >
                        {buttonLabel(`reconciliation-${run.key}`, 'Archive')}
                      </button>
                    </form>
                  </div>
                </li>
              </ul>
            </article>
          {/each}
        </div>
      {/if}
    </section>
  </section>

  <section class="panel" id="positions">
    <div class="panel-head">
      <div>
        <h2>Current Holdings</h2>
        <p>Latest non-zero position snapshots persisted in the ledger.</p>
      </div>
      <div class="panel-tools">
        <span class="subtle">{filteredPositions.length} of {positions.length} visible</span>
        <button
          class="inline-button neutral"
          type="button"
          on:click={() => resetFilterSection('positions')}
          disabled={!sectionHasActiveFilters('positions')}
        >
          Clear Filters
        </button>
      </div>
    </div>
    {#if positions.length === 0}
      <p class="empty">No durable open positions are available yet.</p>
    {:else if filteredPositions.length === 0}
      <p class="empty">
        No holdings match the active filters. Clear filters to show all {positions.length}
        holdings.
      </p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Account</th>
              <th>Symbol</th>
              <th>Exchange</th>
              <th>Currency</th>
              <th>Quantity</th>
              <th>Average Cost</th>
              <th>Market Price</th>
              <th>Market Value</th>
              <th>Unrealized PnL</th>
              <th>Exit Plan</th>
            </tr>
            <tr class="filter-row">
              <th><input bind:value={dashboardFilters.positions.account} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.positions.symbol} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.positions.exchange} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.positions.currency} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.positions.quantity} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.positions.averageCost} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.positions.marketPrice} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.positions.marketValue} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.positions.unrealizedPnl} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.positions.exitPlan} placeholder="Filter" /></th>
            </tr>
          </thead>
          <tbody>
            {#each filteredPositions as position}
              {@const exitPlan = positionExitPlan(position, executionInstructions)}
              <tr>
                <td>
                  {position.account_label ?? position.account_key}
                  {#if position.is_virtual}<span class="mini-badge">Virtual</span>{/if}
                </td>
                <td>{position.local_symbol ?? position.symbol}</td>
                <td>{position.primary_exchange ?? position.exchange}</td>
                <td>{position.currency}</td>
                <td>{formatQuantity(position.quantity)}</td>
                <td>{formatPrice(position.average_cost)}</td>
                <td>{formatPrice(position.market_price)}</td>
                <td>{formatMoney(position.market_value)}</td>
                <td>{formatMoney(position.unrealized_pnl)}</td>
                <td>
                  <span class={`pill ${exitPlan.className}`}>{exitPlan.label}</span>
                  <small class="row-detail">{exitPlan.detail}</small>
                  {#if exitPlan.instructionId}
                    <small class="row-detail mono">{exitPlan.instructionId}</small>
                  {/if}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>

  <section class="panel" id="orders">
    <div class="panel-head">
      <div>
        <h2>Open Orders</h2>
        <p>Durable broker-order rows that are still operationally open.</p>
      </div>
      <div class="panel-tools">
        <span class="subtle">{filteredOpenOrders.length} of {openOrders.length} visible</span>
        <button
          class="inline-button neutral"
          type="button"
          on:click={() => resetFilterSection('openOrders')}
          disabled={!sectionHasActiveFilters('openOrders')}
        >
          Clear Filters
        </button>
      </div>
    </div>
    {#if orderRowActionResult}
      <p class={`action-feedback ${orderRowActionResult.ok ? 'ok' : 'bad'}`}>
        {orderRowActionResult.message}
      </p>
    {/if}
    {#if openOrders.length === 0}
      <p class="empty">No open broker orders are persisted right now.</p>
    {:else if filteredOpenOrders.length === 0}
      <p class="empty">
        No open orders match the active filters. Clear filters to show all {openOrders.length}
        open orders.
      </p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Account</th>
              <th>Symbol</th>
              <th>Role</th>
              <th>Purpose</th>
              <th>Side</th>
              <th>Quantity</th>
              <th>Type</th>
              <th>Limit</th>
              <th>Stop</th>
              <th>Vs Fill</th>
              <th>Market</th>
              <th>Trigger Gap</th>
              <th>Status</th>
              <th>Warning</th>
              <th>Action</th>
            </tr>
            <tr class="filter-row">
              <th><input bind:value={dashboardFilters.openOrders.account} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.openOrders.symbol} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.openOrders.role} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.openOrders.purpose} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.openOrders.side} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.openOrders.quantity} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.openOrders.type} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.openOrders.limit} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.openOrders.stop} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.openOrders.vsFill} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.openOrders.market} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.openOrders.vsMkt} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.openOrders.status} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.openOrders.warning} placeholder="Filter" /></th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {#each filteredOpenOrders as order}
              <tr>
                <td>
                  {order.account_label ?? order.account_key}
                  {#if order.is_virtual}<span class="mini-badge">Virtual</span>{/if}
                </td>
                <td>{order.local_symbol ?? order.symbol}</td>
                <td>{order.order_role}</td>
                <td>{order.order_purpose ?? 'n/a'}</td>
                <td>{order.side}</td>
                <td>{formatQuantity(order.total_quantity)}</td>
                <td>{order.order_type}</td>
                <td>{displayOrderPrice(order.limit_price)}</td>
                <td>{displayOrderPrice(order.stop_price)}</td>
                <td>
                  {#if order.fill_basis_price}
                    <div>{orderFillSpreadLabel(order)}</div>
                    <small class="row-detail">
                      from {formatPrice(order.fill_basis_price)}
                      {#if order.fill_basis_at}
                        at {formatTimestamp(order.fill_basis_at)}
                      {/if}
                    </small>
                  {:else}
                    <span class="subtle">n/a</span>
                  {/if}
                </td>
                <td>
                  {#if order.reference_market_price}
                    <div class="market-cell">
                      <span>{formatPrice(order.reference_market_price)}</span>
                      {#if marketDirectionArrow(order.last_market_price_direction)}
                        <span class={`market-arrow ${marketDirectionClass(order.last_market_price_direction)}`}>
                          {marketDirectionArrow(order.last_market_price_direction)}
                        </span>
                      {/if}
                    </div>
                    {#if order.reference_market_price_at}
                      <small class="row-detail">as of {formatTimestamp(order.reference_market_price_at)}</small>
                    {/if}
                  {:else}
                    <span class="subtle">n/a</span>
                  {/if}
                </td>
                <td>
                  <div>{orderSpreadLabel(order)}</div>
                  {#if orderTriggerDetail(order)}
                    <small class="row-detail">{orderTriggerDetail(order)}</small>
                  {/if}
                </td>
                <td>{order.status}</td>
                <td>{order.reject_reason ?? order.warning_text ?? 'n/a'}</td>
                <td>
                  {#if order.external_order_id}
                    <form
                      method="POST"
                      action="?/orderRowAction"
                      class="inline-action-form"
                      use:enhance={enhanceDashboardAction(`cancel-order-${order.external_order_id}`)}
                    >
                      <input type="hidden" name="external_order_id" value={order.external_order_id} />
                      <button
                        class={`inline-button danger ${buttonStateClass(`cancel-order-${order.external_order_id}`)}`}
                        type="submit"
                        data-action-key={`cancel-order-${order.external_order_id}`}
                        disabled={buttonIsBusy(`cancel-order-${order.external_order_id}`)}
                      >
                        {buttonLabel(`cancel-order-${order.external_order_id}`, 'Cancel Order')}
                      </button>
                    </form>
                  {:else}
                    <span class="subtle">No action</span>
                  {/if}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>

  <section class="panel" id="fills">
    <div class="panel-head">
      <div>
        <h2>Recent Fills</h2>
        <p>Latest persisted execution fills.</p>
      </div>
      <div class="panel-tools">
        <span class="subtle">{filteredRecentFills.length} of {recentFills.length} visible</span>
        <button
          class="inline-button neutral"
          type="button"
          on:click={() => resetFilterSection('recentFills')}
          disabled={!sectionHasActiveFilters('recentFills')}
        >
          Clear Filters
        </button>
      </div>
    </div>
    {#if recentFills.length === 0}
      <p class="empty">No execution fills have been recorded yet.</p>
    {:else if filteredRecentFills.length === 0}
      <p class="empty">
        No fills match the active filters. Clear filters to show all {recentFills.length}
        fills.
      </p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Account</th>
              <th>Symbol</th>
              <th>Side</th>
              <th>Strat</th>
              <th>Quantity</th>
              <th>Price</th>
              <th>Fee</th>
              <th>PnL</th>
            </tr>
            <tr class="filter-row">
              <th><input bind:value={dashboardFilters.recentFills.time} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.recentFills.account} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.recentFills.symbol} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.recentFills.side} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.recentFills.strat} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.recentFills.quantity} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.recentFills.price} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.recentFills.fee} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.recentFills.pnl} placeholder="Filter" /></th>
            </tr>
          </thead>
          <tbody>
            {#each filteredRecentFills as fill}
              <tr>
                <td>{formatTimestamp(fill.executed_at)}</td>
                <td>
                  {fill.account_label ?? fill.account_key}
                  {#if fill.is_virtual}<span class="mini-badge">Virtual</span>{/if}
                </td>
                <td>{fill.symbol}</td>
                <td>{fill.side ?? 'n/a'}</td>
                <td>{fillStrategyLabel(fill)}</td>
                <td>{formatQuantity(fill.quantity)}</td>
                <td>{formatPrice(fill.price)}</td>
                <td>{formatMoney(fill.commission)} {fill.commission_currency ?? ''}</td>
                <td>
                  <span class={moneyTone(fill.realized_pnl)}>
                    {fillExitPnlLabel(fill)}
                  </span>
                  {#if fill.realized_pnl_basis_price}
                    <small class="row-detail">
                      Basis {formatPrice(fill.realized_pnl_basis_price)}
                      {#if fill.realized_pnl_gross && fill.realized_pnl_gross !== fill.realized_pnl}
                        · Gross {formatSignedMoney(fill.realized_pnl_gross)}
                      {/if}
                    </small>
                  {/if}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>

  <section class="panel" id="rl-candidates">
    <div class="panel-head">
      <div>
        <h2>RL Candidate Feed</h2>
        <p>Daily source names whose model decision window is still scheduled or open.</p>
      </div>
      <div class="panel-tools">
        <span class="subtle">{rlCandidateInstructions.length} active source rows</span>
      </div>
    </div>
    {#if rlCandidateInstructions.length === 0}
      <p class="empty">No active model-routed RL candidates are currently loaded.</p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Candidate</th>
              <th>Symbol</th>
              <th>Model</th>
              <th>Account / Book</th>
              <th>Side</th>
              <th>Window</th>
              <th>Queued</th>
            </tr>
          </thead>
          <tbody>
            {#each rlCandidateInstructions as instruction}
              <tr>
                <td class="mono">{instruction.instruction_id}</td>
                <td>{instruction.symbol}</td>
                <td>{rlCandidateModelId(instruction)}</td>
                <td>
                  {instruction.account_key}
                  {#if instruction.is_virtual}<span class="mini-badge">Virtual</span>{/if}
                  <small class="row-detail">{instruction.book_key}</small>
                </td>
                <td>{instruction.side}</td>
                <td>{rlCandidateWindowDisplay(instruction)}</td>
                <td>{formatTimestamp(instruction.updated_at)}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>

  <section class="panel" id="instructions">
    <div class="panel-head">
      <div>
        <h2>Execution Instructions</h2>
        <p>Translated instructions that can be submitted, cancelled, filled, or reconciled.</p>
      </div>
      <div class="panel-tools">
        <span class="subtle">{filteredInstructions.length} of {executionInstructions.length} visible</span>
        <button
          class="inline-button neutral"
          type="button"
          on:click={() => resetFilterSection('instructions')}
          disabled={!sectionHasActiveFilters('instructions')}
        >
          Clear Filters
        </button>
      </div>
    </div>
    {#if instructionRowActionResult}
      <p class={`action-feedback ${instructionRowActionResult.ok ? 'ok' : 'bad'}`}>
        {instructionRowActionResult.message}
      </p>
    {/if}
    {#if executionInstructions.length === 0}
      <p class="empty">No translated execution instructions were found.</p>
    {:else if filteredInstructions.length === 0}
      <p class="empty">
        No execution instructions match the active filters. Clear filters to show all
        {executionInstructions.length} instructions.
      </p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Instruction</th>
              <th>Symbol</th>
              <th>State</th>
              <th>Lifecycle</th>
              <th>Guidance</th>
              <th>Entry Order</th>
              <th>Exit Order</th>
              <th>Updated</th>
              <th>Actions</th>
            </tr>
            <tr class="filter-row">
              <th><input bind:value={dashboardFilters.instructions.instruction} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.instructions.symbol} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.instructions.state} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.instructions.lifecycle} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.instructions.guidance} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.instructions.entryOrder} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.instructions.exitOrder} placeholder="Filter" /></th>
              <th><input bind:value={dashboardFilters.instructions.updated} placeholder="Filter" /></th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {#each filteredInstructions as instruction}
              {@const windowState = instructionWindowState(instruction)}
              {@const primaryAction = instructionPrimaryAction(instruction)}
              <tr>
                <td class="mono">{instruction.instruction_id}</td>
                <td>{instruction.symbol}</td>
                <td>
                  <span class={`pill ${instruction.state === 'FAILED' ? 'bad' : 'neutral'}`}>
                    {instruction.state}
                  </span>
                </td>
                <td>
                  <span class={`pill ${windowState.className}`}>{windowState.label}</span>
                  <small class="row-detail">{windowState.detail}</small>
                </td>
                <td class="guidance-cell">{instructionGuidance(instruction)}</td>
                <td>{instructionOrderDisplay(instruction, 'entry')}</td>
                <td>{instructionOrderDisplay(instruction, 'exit')}</td>
                <td>{formatTimestamp(instruction.updated_at)}</td>
                <td class="actions-cell">
                  {#if primaryAction && hasInstructionAction(instruction)}
                    <form
                      method="POST"
                      action="?/instructionRowAction"
                      class="inline-action-form"
                      use:enhance={enhanceDashboardAction(`instruction-${instruction.instruction_id}-${primaryAction.operation}`)}
                    >
                      <input type="hidden" name="instruction_id" value={instruction.instruction_id} />
                      <input type="hidden" name="operation" value={primaryAction.operation} />
                      <button
                        class={`${primaryAction.className} ${buttonStateClass(`instruction-${instruction.instruction_id}-${primaryAction.operation}`)}`}
                        type="submit"
                        data-action-key={`instruction-${instruction.instruction_id}-${primaryAction.operation}`}
                        disabled={buttonIsBusy(`instruction-${instruction.instruction_id}-${primaryAction.operation}`)}
                      >
                        {buttonLabel(
                          `instruction-${instruction.instruction_id}-${primaryAction.operation}`,
                          primaryAction.label
                        )}
                      </button>
                    </form>
                  {:else}
                    <span class="subtle">No write action</span>
                  {/if}

                  <a class="inline-button subtle-link" href={`/ledger?instruction_id=${encodeURIComponent(instruction.instruction_id)}`}>
                    Ledger
                  </a>
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>
</div>

<style>
  :global(:root) {
    color-scheme: light;
    --bg-accent: rgba(221, 180, 84, 0.24);
    --bg-start: #f4f3ee;
    --bg-end: #ece9e1;
    --text-primary: #1d2228;
    --text-secondary: #485562;
    --text-muted: #6a7783;
    --surface: rgba(255, 255, 255, 0.88);
    --surface-strong: rgba(255, 255, 255, 0.95);
    --border: rgba(29, 34, 40, 0.1);
    --border-strong: rgba(29, 34, 40, 0.16);
    --shadow: rgba(29, 34, 40, 0.08);
    --ok: #0e7a49;
    --warn: #b36a11;
    --bad: #b43333;
    --account-line: #1769aa;
    --benchmark-line: #c06a00;
    --danger-bg: rgba(180, 51, 51, 0.08);
    --danger-border: rgba(180, 51, 51, 0.24);
    --table-row-hover: rgba(29, 34, 40, 0.03);
  }

  @media (prefers-color-scheme: dark) {
    :global(:root) {
      color-scheme: dark;
      --bg-accent: rgba(226, 174, 45, 0.14);
      --bg-start: #091117;
      --bg-end: #111b22;
      --text-primary: #eef4f6;
      --text-secondary: #b3c2cc;
      --text-muted: #90a1ad;
      --surface: rgba(14, 24, 31, 0.88);
      --surface-strong: rgba(16, 28, 37, 0.96);
      --border: rgba(179, 194, 204, 0.12);
      --border-strong: rgba(179, 194, 204, 0.18);
      --shadow: rgba(0, 0, 0, 0.35);
      --ok: #59d58f;
      --warn: #f0b04f;
      --bad: #ff8c8c;
      --account-line: #76b7ff;
      --benchmark-line: #f5a623;
      --danger-bg: rgba(255, 140, 140, 0.08);
      --danger-border: rgba(255, 140, 140, 0.22);
      --table-row-hover: rgba(179, 194, 204, 0.06);
    }
  }

  :global(body) {
    margin: 0;
    font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
    color: var(--text-primary);
    background:
      radial-gradient(circle at top left, var(--bg-accent), transparent 30rem),
      linear-gradient(180deg, var(--bg-start) 0%, var(--bg-end) 100%);
  }

  .page {
    width: 100%;
    box-sizing: border-box;
    padding: 2rem 1.25rem 4rem;
  }

  .inline-button {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    padding: 0.5rem 0.8rem;
    border-radius: 999px;
    border: 1px solid var(--border);
    color: var(--text-secondary);
    text-decoration: none;
    font-size: 0.85rem;
    font-weight: 600;
    transition:
      transform 120ms ease,
      border-color 120ms ease,
      background 120ms ease,
      color 120ms ease;
  }

  .inline-button:hover {
    transform: translateY(-1px);
    border-color: var(--border-strong);
    background: var(--surface-strong);
    color: var(--text-primary);
  }

  .hero {
    display: flex;
    justify-content: space-between;
    align-items: end;
    gap: 2rem;
    margin-bottom: 1.5rem;
  }

  .hero-copy {
    max-width: 48rem;
  }

  .eyebrow {
    margin: 0 0 0.5rem;
    font-size: 0.82rem;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    color: var(--text-muted);
  }

  h1 {
    margin: 0;
    font-size: clamp(2rem, 4vw, 3.2rem);
    line-height: 1;
  }

  .lede {
    margin: 0.8rem 0 0;
    color: var(--text-secondary);
    line-height: 1.55;
    max-width: 42rem;
  }

  .hero-meta {
    display: grid;
    gap: 0.8rem;
    min-width: min(23rem, 100%);
    padding: 1rem 1.15rem;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 1rem;
    box-shadow: 0 20px 50px -35px var(--shadow);
    backdrop-filter: blur(16px);
  }

  .hero-meta span {
    display: block;
    font-size: 0.76rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    color: var(--text-muted);
    margin-bottom: 0.25rem;
  }

  .hero-meta strong {
    font-size: 0.95rem;
    word-break: break-word;
  }

  .hero-meta small {
    display: block;
    margin-top: 0.25rem;
    color: var(--text-muted);
    line-height: 1.35;
  }

  .hero-refresh {
    margin-top: 0.45rem;
  }

  .stat-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(165px, 1fr));
    gap: 1rem;
    margin-bottom: 1.25rem;
  }

  .stat-card,
  .panel,
  .account-card,
  .reconciliation-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 1rem;
    box-shadow: 0 20px 50px -35px var(--shadow);
    backdrop-filter: blur(14px);
  }

  .stat-card {
    padding: 1rem;
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
  }

  .stat-card span {
    font-size: 0.82rem;
    color: var(--text-muted);
  }

  .stat-card strong {
    font-size: 1.5rem;
    line-height: 1.1;
  }

  .stat-card small,
  .subtle,
  .neutral {
    color: var(--text-muted);
  }

  .ok {
    color: var(--ok);
  }

  .warn {
    color: var(--warn);
  }

  .bad {
    color: var(--bad);
  }

  .sync-panel {
    border-color: var(--border-strong);
  }

  .sync-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
    gap: 0;
    border: 1px solid var(--border);
    border-radius: 0.85rem;
    overflow: hidden;
    background: var(--surface-strong);
  }

  .sync-item {
    min-height: 7.25rem;
    display: grid;
    align-content: start;
    gap: 0.28rem;
    padding: 0.85rem;
    border-right: 1px solid var(--border);
    border-bottom: 1px solid var(--border);
  }

  .sync-item-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 0.5rem;
  }

  .sync-item span,
  .sync-item small {
    color: var(--text-muted);
  }

  .status-dot {
    width: 0.7rem;
    height: 0.7rem;
    border-radius: 999px;
    background: var(--text-muted);
    flex: 0 0 auto;
  }

  .status-dot.ok {
    background: var(--ok);
  }

  .status-dot.warn {
    background: var(--warn);
  }

  .status-dot.bad {
    background: var(--bad);
  }

  .sync-warning-list {
    list-style: none;
    margin: 0.85rem 0 0;
    padding: 0;
    display: grid;
    gap: 0.45rem;
  }

  .sync-warning-list li {
    border-left: 0.25rem solid var(--warn);
    padding: 0.5rem 0.65rem;
    background: var(--surface-strong);
    color: var(--text-secondary);
  }

  .sync-warning-list li.bad {
    border-left-color: var(--bad);
  }

  .panel {
    padding: 1rem 1rem 1.15rem;
    margin-bottom: 1.25rem;
  }

  .control-panel {
    min-height: 100%;
  }

  .panel.danger {
    background: linear-gradient(180deg, var(--danger-bg), transparent);
    border-color: var(--danger-border);
  }

  .panel-head {
    display: flex;
    justify-content: space-between;
    align-items: end;
    gap: 1rem;
    margin-bottom: 0.95rem;
  }

  .panel-tools {
    display: flex;
    gap: 0.65rem;
    align-items: center;
    justify-content: flex-end;
    flex-wrap: wrap;
  }

  .panel-head h2,
  .account-card h3,
  .reconciliation-card h3 {
    margin: 0;
    font-size: 1.05rem;
  }

  .panel-head p,
  .reconciliation-topline p {
    margin: 0.25rem 0 0;
    color: var(--text-secondary);
  }

  .section-subhead {
    margin: 0.85rem 0 0.45rem;
    font-size: 0.9rem;
    font-weight: 700;
  }

  .account-grid,
  .two-up {
    display: grid;
    gap: 1rem;
  }

  .account-grid {
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  }

  .two-up {
    grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
  }

  .account-card {
    padding: 1rem;
  }

  .account-chart {
    margin-top: 0.95rem;
    border-top: 1px solid var(--border);
    padding-top: 0.8rem;
    display: grid;
    gap: 0.55rem;
  }

  .account-chart-head {
    display: flex;
    justify-content: space-between;
    gap: 1rem;
    align-items: start;
  }

  .account-chart-head span {
    display: block;
    color: var(--text-muted);
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: 0.2rem;
  }

  .account-chart-head strong {
    font-size: 1.15rem;
  }

  .account-chart-head small,
  .chart-empty,
  .chart-legend {
    color: var(--text-muted);
  }

  .performance-chart {
    width: 100%;
    height: auto;
    min-height: 7.5rem;
    border: 1px solid var(--border);
    border-radius: 0.85rem;
    background: color-mix(in oklab, var(--surface-strong) 82%, transparent);
  }

  .chart-zero {
    fill: none;
    stroke: var(--border);
    stroke-width: 1;
    stroke-dasharray: 4 4;
  }

  .chart-line {
    fill: none;
    stroke-width: 2.5;
    stroke-linecap: round;
    stroke-linejoin: round;
  }

  .account-line {
    stroke: var(--account-line);
  }

  .benchmark-line {
    stroke: var(--benchmark-line);
    stroke-dasharray: 6 5;
  }

  .chart-axis-labels {
    display: flex;
    justify-content: space-between;
    margin-top: -0.25rem;
    color: var(--text-muted);
    font-size: 0.74rem;
    line-height: 1;
  }

  .chart-legend {
    display: flex;
    flex-wrap: wrap;
    gap: 0.7rem;
    font-size: 0.82rem;
  }

  .chart-legend span {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
  }

  .chart-legend i {
    width: 0.62rem;
    height: 0.62rem;
    border-radius: 999px;
    display: inline-block;
  }

  .account-dot {
    background: var(--account-line);
  }

  .benchmark-dot {
    background: var(--benchmark-line);
  }

  .chart-empty {
    margin: 0;
    min-height: 7.5rem;
    border: 1px dashed var(--border);
    border-radius: 0.85rem;
    display: grid;
    place-items: center;
    text-align: center;
    padding: 0.85rem;
  }

  .account-title,
  .reconciliation-topline {
    display: flex;
    justify-content: space-between;
    gap: 1rem;
    align-items: start;
    margin-bottom: 0.75rem;
  }

  dl {
    margin: 0;
    display: grid;
    gap: 0.55rem;
  }

  dl div {
    display: flex;
    justify-content: space-between;
    gap: 1rem;
    border-top: 1px solid var(--border);
    padding-top: 0.55rem;
  }

  dt {
    color: var(--text-muted);
  }

  dd {
    margin: 0;
    text-align: right;
  }

  .table-wrap {
    overflow-x: auto;
  }

  .intent-table-wrap {
    margin-bottom: 0.85rem;
  }

  .filter-row th {
    padding-top: 0.35rem;
    padding-bottom: 0.6rem;
    background: transparent;
    border-top: none;
  }

  .filter-row input {
    width: 100%;
    min-width: 5.5rem;
    box-sizing: border-box;
    border: 1px solid var(--border);
    border-radius: 0.7rem;
    padding: 0.45rem 0.55rem;
    font: inherit;
    font-size: 0.82rem;
    color: var(--text-primary);
    background: var(--surface-strong);
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.95rem;
  }

  th,
  td {
    padding: 0.72rem 0.65rem;
    border-top: 1px solid var(--border);
    text-align: left;
    vertical-align: top;
  }

  th {
    color: var(--text-muted);
    font-weight: 600;
    font-size: 0.82rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }

  tbody tr:hover {
    background: var(--table-row-hover);
  }

  .pill {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    border-radius: 999px;
    padding: 0.22rem 0.65rem;
    font-size: 0.76rem;
    font-weight: 700;
    letter-spacing: 0.03em;
    border: 1px solid currentColor;
  }

  .pill.neutral {
    color: var(--text-secondary);
    border-color: var(--border-strong);
  }

  .pill-row.compact {
    display: flex;
    flex-wrap: wrap;
    gap: 0.35rem;
    justify-content: flex-end;
  }

  .mini-badge {
    display: inline-flex;
    margin-left: 0.35rem;
    padding: 0.08rem 0.35rem;
    border: 1px solid var(--warn);
    border-radius: 999px;
    color: var(--warn);
    font-size: 0.68rem;
    font-weight: 700;
    text-transform: uppercase;
  }

  .attention-list,
  .issue-list,
  .reconciliation-list {
    list-style: none;
    margin: 0;
    padding: 0;
  }

  .attention-list {
    display: grid;
    gap: 0.85rem;
  }

  .attention-list li,
  .issue-list li {
    display: grid;
    gap: 0.25rem;
    padding: 0.9rem 0.95rem;
    border: 1px solid var(--border);
    border-radius: 0.9rem;
    background: var(--surface-strong);
  }

  .attention-main,
  .issue-main,
  .run-pills {
    display: flex;
    gap: 0.5rem;
    align-items: center;
    flex-wrap: wrap;
  }

  .reconciliation-list {
    display: grid;
    gap: 0.9rem;
  }

  .reconciliation-card {
    padding: 1rem;
  }

  .issue-list {
    display: grid;
    gap: 0.65rem;
    margin-top: 0.85rem;
  }

  .mono {
    font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
  }

  .market-cell {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
  }

  .market-arrow {
    font-weight: 700;
    font-size: 0.95rem;
  }

  .empty {
    color: var(--text-muted);
    margin: 0.25rem 0 0;
  }

  .control-form {
    display: grid;
    gap: 0.95rem;
  }

  .form-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 0.85rem;
  }

  .control-form label {
    display: grid;
    gap: 0.42rem;
  }

  .control-form label span {
    font-size: 0.82rem;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.06em;
  }

  .control-form input,
  .control-form textarea {
    width: 100%;
    box-sizing: border-box;
    border: 1px solid var(--border);
    border-radius: 0.85rem;
    padding: 0.8rem 0.9rem;
    font: inherit;
    color: var(--text-primary);
    background: var(--surface-strong);
  }

  .control-form textarea {
    resize: vertical;
    min-height: 3.4rem;
  }

  .full-width {
    grid-column: 1 / -1;
  }

  .form-actions {
    display: flex;
    justify-content: flex-start;
    gap: 0.75rem;
    flex-wrap: wrap;
  }

  .action-button {
    border: 1px solid transparent;
    border-radius: 999px;
    padding: 0.78rem 1.15rem;
    font: inherit;
    font-weight: 700;
    letter-spacing: 0.02em;
    color: #fffaf0;
    background: linear-gradient(135deg, #0e7a49 0%, #199d61 100%);
    cursor: pointer;
  }

  .action-button:disabled,
  .inline-button:disabled {
    cursor: wait;
    opacity: 0.8;
  }

  .action-button.danger {
    background: linear-gradient(135deg, #8e2f2f 0%, #b43333 100%);
  }

  .inline-action-form {
    margin: 0;
  }

  .inline-actions {
    display: flex;
    gap: 0.5rem;
    flex-wrap: wrap;
    margin-top: 0.35rem;
  }

  .actions-cell {
    display: flex;
    gap: 0.55rem;
    flex-wrap: wrap;
    align-items: center;
  }

  .inline-button {
    border: 1px solid transparent;
    font: inherit;
    font-weight: 700;
    letter-spacing: 0.02em;
    color: #fffaf0;
    background: linear-gradient(135deg, #0e7a49 0%, #199d61 100%);
    cursor: pointer;
    white-space: nowrap;
  }

  .inline-button.danger {
    background: linear-gradient(135deg, #8e2f2f 0%, #b43333 100%);
  }

  .inline-button.neutral {
    color: var(--text-primary);
    border-color: var(--border-strong);
    background: var(--surface-strong);
  }

  .inline-button.subtle-link {
    color: var(--text-primary);
    border-color: var(--border-strong);
    background: var(--surface-strong);
  }

  .action-button.is-clicking,
  .inline-button.is-clicking {
    transform: scale(0.98);
    filter: saturate(1.08);
  }

  .action-button.is-working,
  .inline-button.is-working {
    animation: dashboard-pulse 1s ease-in-out infinite;
  }

  .action-button.is-success,
  .inline-button.is-success {
    animation: dashboard-success 0.8s ease;
  }

  .action-button.is-error,
  .inline-button.is-error {
    animation: dashboard-error 0.8s ease;
  }

  .guidance-cell {
    min-width: 15rem;
    max-width: 24rem;
  }

  .row-detail {
    display: block;
    margin-top: 0.35rem;
    color: var(--text-muted);
  }

  .action-button.neutral {
    color: var(--text-primary);
    border-color: var(--border-strong);
    background: var(--surface-strong);
  }

  .action-feedback {
    margin: 0 0 0.95rem;
    padding: 0.85rem 0.95rem;
    border: 1px solid var(--border);
    border-radius: 0.85rem;
    background: var(--surface-strong);
    font-weight: 600;
  }

  @keyframes dashboard-pulse {
    0%,
    100% {
      transform: translateY(0);
      box-shadow: 0 0 0 0 rgba(25, 157, 97, 0.15);
    }
    50% {
      transform: translateY(-1px);
      box-shadow: 0 0 0 0.4rem rgba(25, 157, 97, 0.08);
    }
  }

  @keyframes dashboard-success {
    0% {
      box-shadow: 0 0 0 0 rgba(14, 122, 73, 0.3);
    }
    60% {
      box-shadow: 0 0 0 0.45rem rgba(14, 122, 73, 0.12);
    }
    100% {
      box-shadow: 0 0 0 0 rgba(14, 122, 73, 0);
    }
  }

  @keyframes dashboard-error {
    0% {
      box-shadow: 0 0 0 0 rgba(180, 51, 51, 0.3);
    }
    60% {
      box-shadow: 0 0 0 0.45rem rgba(180, 51, 51, 0.12);
    }
    100% {
      box-shadow: 0 0 0 0 rgba(180, 51, 51, 0);
    }
  }

  @media (max-width: 900px) {
    .hero,
    .panel-head,
    .account-title,
    .reconciliation-topline {
      flex-direction: column;
      align-items: start;
    }

    .form-actions {
      width: 100%;
      justify-content: start;
    }

    .action-button {
      width: 100%;
      justify-content: center;
    }

    dl div {
      flex-direction: column;
      gap: 0.15rem;
    }

    dd {
      text-align: left;
    }
  }
</style>
