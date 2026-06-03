<script>
  import {
    classForConnection,
    connectionLabel,
    executionRuntimeClass,
    executionRuntimeLabel,
    formatAge,
    formatTimestamp,
    formatTimestampOrNull,
    ibGatewayClass,
    ibGatewayDetail,
    ibGatewayLabel,
    killSwitchClass,
    killSwitchLabel,
    monitorClass,
    monitorLabel
  } from '../status.js';

  export let data;
  export let liveGeneratedAt;
  export let operatorSnapshot;
  export let liveSnapshotStatusClass;
  export let liveSnapshotStatusLabel;
  export let liveSnapshotStatusDetail;
  export let marketStreamStatusClass;
  export let marketStreamStatusLabel;
  export let marketStreamStatusDetail;
  export let marketTimeZone;
  export let liveHealth;
  export let brokerMonitor;
  export let killSwitch;
  export let accounts;
  export let positions;
  export let openOrders;
  export let rlCandidateInstructions;
  export let executionInstructions;
  export let brokerAttention;
  export let warningRuns;
  export let stateSync;
  export let endpointErrors;
</script>

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
        <strong>{formatTimestamp(liveGeneratedAt)}</strong>
        <small class={liveSnapshotStatusClass()}>{liveSnapshotStatusLabel()} · {liveSnapshotStatusDetail()}</small>
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
      <small>Client ID {liveHealth.broker_sessions.primary.client_id}, on demand</small>
    </article>

    <article class="stat-card">
      <span>Diagnostic Session</span>
      <strong class={classForConnection('diagnostic')}>{connectionLabel('diagnostic')}</strong>
      <small>Client ID {liveHealth.broker_sessions.diagnostic.client_id}</small>
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
