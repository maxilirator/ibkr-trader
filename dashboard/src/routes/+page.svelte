<script>
  export let data;
  export let form;

  const operatorSnapshot = data.operatorSnapshot ?? {};
  const killSwitch = operatorSnapshot.kill_switch ?? {
    enabled: false,
    reason: null,
    updated_by: null,
    last_changed_at: null
  };
  const accounts = operatorSnapshot.accounts ?? [];
  const positions = operatorSnapshot.positions ?? [];
  const openOrders = operatorSnapshot.open_orders ?? [];
  const recentFills = operatorSnapshot.recent_fills ?? [];
  const brokerAttention = operatorSnapshot.recent_broker_attention ?? [];
  const reconciliationRuns = operatorSnapshot.recent_reconciliation_runs ?? [];
  const instructions = operatorSnapshot.instructions ?? [];
  const brokerMonitor = data.health?.broker_monitor ?? {
    heartbeat: { ok: null, last_success_at: null, error: null },
    snapshot_refresh: {
      ok: null,
      last_success_at: null,
      error: null,
      account_count: 0,
      position_count: 0,
      open_order_count: 0
    }
  };
  const executionRuntime = data.health?.execution_runtime ?? null;
  const endpointErrors = Object.entries(data.errors ?? {}).filter(([, value]) => value);
  const warningRuns = reconciliationRuns.filter((run) => Number(run.issue_count ?? 0) > 0);
  const killSwitchResult = form?.killSwitchResult ?? null;
  const cancelSetResult = form?.cancelSetResult ?? null;
  const instructionRowActionResult = form?.instructionRowActionResult ?? null;
  const orderRowActionResult = form?.orderRowActionResult ?? null;
  const referenceNow = new Date(operatorSnapshot.generated_at ?? data.generatedAt);
  const terminalInstructionStates = new Set(['ENTRY_CANCELLED', 'COMPLETED', 'FAILED']);

  function brokerConnected(role) {
    return data.health?.broker_sessions?.[role]?.connected === true;
  }

  function connectionLabel(role) {
    return brokerConnected(role) ? 'Connected' : 'Disconnected';
  }

  function classForConnection(role) {
    return brokerConnected(role) ? 'ok' : 'bad';
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

  function monitorLabel(ok) {
    if (ok === true) return 'Healthy';
    if (ok === false) return 'Failing';
    return 'Unknown';
  }

  function monitorClass(ok) {
    if (ok === true) return 'ok';
    if (ok === false) return 'bad';
    return 'warn';
  }

  function executionRuntimeLabel() {
    return executionRuntime?.status ?? 'Unknown';
  }

  function executionRuntimeClass() {
    if (!executionRuntime?.status) return 'warn';
    if (executionRuntime.status === 'RUNNING') return 'ok';
    if (executionRuntime.status === 'DEGRADED') return 'warn';
    if (executionRuntime.status === 'STOPPED') return 'warn';
    return 'bad';
  }

  function parseTimestamp(value) {
    if (!value) return null;
    const parsed = new Date(value);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  function instructionWindowState(instruction) {
    const submitAt = parseTimestamp(instruction.submit_at);
    const expireAt = parseTimestamp(instruction.expire_at);

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
        detail: `Opens ${instruction.submit_at}`,
        isScheduled: true,
        isOpen: false,
        isExpired: false
      };
    }

    if (referenceNow >= expireAt) {
      return {
        label: 'Expired',
        className: 'bad',
        detail: `Expired ${instruction.expire_at}`,
        isScheduled: false,
        isOpen: false,
        isExpired: true
      };
    }

    return {
      label: 'Open',
      className: 'ok',
      detail: `Closes ${instruction.expire_at}`,
      isScheduled: false,
      isOpen: true,
      isExpired: false
    };
  }

  function instructionGuidance(instruction) {
    const windowState = instructionWindowState(instruction);

    if (instruction.state === 'ENTRY_PENDING') {
      if (windowState.isScheduled) {
        return 'Waiting for the entry window to open. It should not be submitted yet.';
      }
      if (windowState.isExpired) {
        return 'The entry window already passed. This row now needs cancellation or ledger review.';
      }
      return 'The entry window is active. You can still submit it manually if that is intentional.';
    }

    if (instruction.state === 'ENTRY_SUBMITTED') {
      if (windowState.isExpired) {
        return 'The broker entry is past expiry. Runtime should cancel or reconcile it.';
      }
      return 'The broker entry is active. Cancel it if it should not stay working.';
    }

    if (instruction.state === 'POSITION_OPEN') {
      return 'Entry filled. Runtime is now responsible for exit management.';
    }

    if (instruction.state === 'EXIT_PENDING') {
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
      if (windowState.isOpen) {
        return { operation: 'submit_entry', label: 'Submit Entry', className: 'inline-button' };
      }
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
</script>

<svelte:head>
  <title>IBKR Trader Operator Dashboard</title>
  <meta http-equiv="refresh" content="15" />
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
        <strong>{data.generatedAt}</strong>
      </div>
      <div>
        <span>Snapshot generated</span>
        <strong>{operatorSnapshot.generated_at ?? 'n/a'}</strong>
      </div>
    </div>
  </header>

  <section class="stat-grid">
    <article class="stat-card">
      <span>Primary Broker Session</span>
      <strong class={classForConnection('primary')}>{connectionLabel('primary')}</strong>
      <small>Client ID {data.health?.broker_sessions?.primary?.client_id ?? 'n/a'}</small>
    </article>

    <article class="stat-card">
      <span>Diagnostic Session</span>
      <strong class={classForConnection('diagnostic')}>{connectionLabel('diagnostic')}</strong>
      <small>Client ID {data.health?.broker_sessions?.diagnostic?.client_id ?? 'n/a'}</small>
    </article>

    <article class="stat-card">
      <span>Gateway Heartbeat</span>
      <strong class={monitorClass(brokerMonitor.heartbeat?.ok)}>
        {monitorLabel(brokerMonitor.heartbeat?.ok)}
      </strong>
      <small>
        {brokerMonitor.heartbeat?.last_success_at ??
          brokerMonitor.heartbeat?.error ??
          'No heartbeat has completed yet.'}
      </small>
    </article>

    <article class="stat-card">
      <span>Snapshot Refresh</span>
      <strong class={monitorClass(brokerMonitor.snapshot_refresh?.ok)}>
        {monitorLabel(brokerMonitor.snapshot_refresh?.ok)}
      </strong>
      <small>
        {#if brokerMonitor.snapshot_refresh?.ok === true}
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
        {executionRuntime?.last_successful_cycle_at ??
          executionRuntime?.last_error ??
          'No execution-runtime status has been persisted yet.'}
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
      <span>Instruction Queue</span>
      <strong>{instructions.length}</strong>
      <small>Most recent persisted instructions</small>
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
          <span>{killSwitch.last_changed_at ?? 'n/a'}</span>
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

      <form method="POST" action="?/killSwitch" class="control-form">
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
          <button class={`action-button ${killSwitch.enabled ? 'neutral' : 'danger'}`} type="submit">
            {killSwitch.enabled ? 'Disable Kill Switch' : 'Enable Kill Switch'}
          </button>
        </div>
      </form>
    </section>

    <section class="panel control-panel">
      <div class="panel-head">
        <div>
          <h2>Cancel Instruction Set</h2>
          <p>
            Cancel matching entry instructions through the durable control plane. Existing
            positions are left alone.
          </p>
        </div>
      </div>

      {#if cancelSetResult}
        <p class={`action-feedback ${cancelSetResult.ok ? 'ok' : 'bad'}`}>
          {cancelSetResult.message}
        </p>
      {/if}

      <form method="POST" action="?/cancelInstructionSet" class="control-form">
        <div class="form-grid">
          <label>
            <span>Batch ID</span>
            <input name="batch_id" type="text" placeholder="live_ops_20260419" />
          </label>

          <label>
            <span>Account Key</span>
            <input name="account_key" type="text" placeholder="U25245596" />
          </label>

          <label>
            <span>Book Key</span>
            <input name="book_key" type="text" placeholder="long_risk_book" />
          </label>

          <label class="full-width">
            <span>Instruction IDs</span>
            <textarea
              name="instruction_ids"
              rows="3"
              placeholder="One or more instruction IDs, separated by commas, spaces, or new lines"
            ></textarea>
          </label>

          <label class="full-width">
            <span>Reason</span>
            <textarea
              name="reason"
              rows="3"
              placeholder="Why are we cancelling this instruction set?"
            ></textarea>
          </label>
        </div>

        <div class="form-actions">
          <button class="action-button" type="submit">Cancel Matching Entries</button>
        </div>
      </form>
    </section>
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
              <span class="pill neutral">{account.account_key}</span>
            </div>
            <dl>
              <div><dt>Snapshot</dt><dd>{account.snapshot_at}</dd></div>
              <div><dt>Net liquidation</dt><dd>{account.net_liquidation ?? 'n/a'} {account.currency ?? account.base_currency ?? ''}</dd></div>
              <div><dt>Total cash</dt><dd>{account.total_cash_value ?? 'n/a'} {account.currency ?? account.base_currency ?? ''}</dd></div>
              <div><dt>Buying power</dt><dd>{account.buying_power ?? 'n/a'} {account.currency ?? account.base_currency ?? ''}</dd></div>
              <div><dt>Available funds</dt><dd>{account.available_funds ?? 'n/a'} {account.currency ?? account.base_currency ?? ''}</dd></div>
              <div><dt>Excess liquidity</dt><dd>{account.excess_liquidity ?? 'n/a'} {account.currency ?? account.base_currency ?? ''}</dd></div>
              <div><dt>Cushion</dt><dd>{account.cushion ?? 'n/a'}</dd></div>
            </dl>
          </article>
        {/each}
      </div>
    {/if}
  </section>

  <section class="two-up" id="operations">
    <section class="panel">
      <div class="panel-head">
        <h2>Broker Attention</h2>
        <p>Recent broker-side warnings and rejects captured in the durable ledger.</p>
      </div>
      {#if brokerAttention.length === 0}
        <p class="empty">No recent broker attention items were found.</p>
      {:else}
        <ul class="attention-list">
          {#each brokerAttention as attention}
            <li>
              <div class="attention-main">
                <span class="pill warn">{attention.event_type}</span>
                <strong>{attention.symbol}</strong>
                <span>{attention.account_label ?? attention.account_key}</span>
              </div>
              <p>{attention.message}</p>
              <small>
                {attention.event_at}
                {#if attention.order_ref}
                  · <span class="mono">{attention.order_ref}</span>
                {/if}
              </small>
            </li>
          {/each}
        </ul>
      {/if}
    </section>

    <section class="panel">
      <div class="panel-head">
        <h2>Recent Reconciliation Runs</h2>
        <p>Durable audit rows from runtime and startup reconciliation passes.</p>
      </div>
      {#if reconciliationRuns.length === 0}
        <p class="empty">No reconciliation runs have been recorded yet.</p>
      {:else}
        <div class="reconciliation-list">
          {#each reconciliationRuns as run}
            <article class="reconciliation-card">
              <div class="reconciliation-topline">
                <div>
                  <h3>{run.run_kind}</h3>
                  <p>{run.started_at} → {run.completed_at}</p>
                </div>
                <div class="run-pills">
                  <span class={`pill ${runStatusClass(run.status)}`}>{run.status}</span>
                  <span class="pill neutral">{run.action_count} actions</span>
                  <span class="pill neutral">{run.issue_count} issues</span>
                </div>
              </div>

              {#if (run.issues ?? []).length > 0}
                <ul class="issue-list">
                  {#each run.issues as issue}
                    <li>
                      <strong>{issue.stage}</strong>
                      <span>{issue.message}</span>
                      {#if issue.instruction_id}
                        <small class="mono">{issue.instruction_id}</small>
                      {/if}
                    </li>
                  {/each}
                </ul>
              {:else}
                <p class="empty subtle">No issues were recorded for this run.</p>
              {/if}
            </article>
          {/each}
        </div>
      {/if}
    </section>
  </section>

  <section class="panel" id="positions">
    <div class="panel-head">
      <h2>Current Holdings</h2>
      <p>Latest non-zero position snapshots persisted in the ledger.</p>
    </div>
    {#if positions.length === 0}
      <p class="empty">No durable open positions are available yet.</p>
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
            </tr>
          </thead>
          <tbody>
            {#each positions as position}
              <tr>
                <td>{position.account_label ?? position.account_key}</td>
                <td>{position.local_symbol ?? position.symbol}</td>
                <td>{position.primary_exchange ?? position.exchange}</td>
                <td>{position.currency}</td>
                <td>{position.quantity}</td>
                <td>{position.average_cost ?? 'n/a'}</td>
                <td>{position.market_price ?? 'n/a'}</td>
                <td>{position.market_value ?? 'n/a'}</td>
                <td>{position.unrealized_pnl ?? 'n/a'}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>

  <section class="panel" id="orders">
    <div class="panel-head">
      <h2>Open Orders</h2>
      <p>Durable broker-order rows that are still operationally open.</p>
    </div>
    {#if orderRowActionResult}
      <p class={`action-feedback ${orderRowActionResult.ok ? 'ok' : 'bad'}`}>
        {orderRowActionResult.message}
      </p>
    {/if}
    {#if openOrders.length === 0}
      <p class="empty">No open broker orders are persisted right now.</p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Account</th>
              <th>Symbol</th>
              <th>Role</th>
              <th>Side</th>
              <th>Quantity</th>
              <th>Type</th>
              <th>Limit</th>
              <th>Stop</th>
              <th>Status</th>
              <th>Warning</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            {#each openOrders as order}
              <tr>
                <td>{order.account_label ?? order.account_key}</td>
                <td>{order.local_symbol ?? order.symbol}</td>
                <td>{order.order_role}</td>
                <td>{order.side}</td>
                <td>{order.total_quantity ?? 'n/a'}</td>
                <td>{order.order_type}</td>
                <td>{order.limit_price ?? 'n/a'}</td>
                <td>{order.stop_price ?? 'n/a'}</td>
                <td>{order.status}</td>
                <td>{order.reject_reason ?? order.warning_text ?? 'n/a'}</td>
                <td>
                  {#if order.external_order_id}
                    <form method="POST" action="?/orderRowAction" class="inline-action-form">
                      <input type="hidden" name="external_order_id" value={order.external_order_id} />
                      <button class="inline-button danger" type="submit">Cancel Order</button>
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
      <h2>Recent Fills</h2>
      <p>Latest persisted execution fills.</p>
    </div>
    {#if recentFills.length === 0}
      <p class="empty">No execution fills have been recorded yet.</p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Account</th>
              <th>Symbol</th>
              <th>Side</th>
              <th>Quantity</th>
              <th>Price</th>
            </tr>
          </thead>
          <tbody>
            {#each recentFills as fill}
              <tr>
                <td>{fill.executed_at}</td>
                <td>{fill.account_label ?? fill.account_key}</td>
                <td>{fill.symbol}</td>
                <td>{fill.side ?? 'n/a'}</td>
                <td>{fill.quantity}</td>
                <td>{fill.price}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>

  <section class="panel" id="instructions">
    <div class="panel-head">
      <h2>Recent Instructions</h2>
      <p>Most recently updated persisted instructions from the control plane.</p>
    </div>
    {#if instructionRowActionResult}
      <p class={`action-feedback ${instructionRowActionResult.ok ? 'ok' : 'bad'}`}>
        {instructionRowActionResult.message}
      </p>
    {/if}
    {#if instructions.length === 0}
      <p class="empty">No persisted instructions were found.</p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Instruction</th>
              <th>Symbol</th>
              <th>State</th>
              <th>Window</th>
              <th>Guidance</th>
              <th>Entry Order</th>
              <th>Exit Order</th>
              <th>Updated</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {#each instructions as instruction}
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
                <td>{instruction.broker_order_id ?? 'n/a'} / {instruction.broker_order_status ?? 'n/a'}</td>
                <td>{instruction.exit_order_id ?? 'n/a'} / {instruction.exit_order_status ?? 'n/a'}</td>
                <td>{instruction.updated_at}</td>
                <td class="actions-cell">
                  {#if primaryAction && hasInstructionAction(instruction)}
                    <form method="POST" action="?/instructionRowAction" class="inline-action-form">
                      <input type="hidden" name="instruction_id" value={instruction.instruction_id} />
                      <input type="hidden" name="operation" value={primaryAction.operation} />
                      <button class={primaryAction.className} type="submit">{primaryAction.label}</button>
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
    max-width: 1380px;
    margin: 0 auto;
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
  .subtle {
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

  .action-button.danger {
    background: linear-gradient(135deg, #8e2f2f 0%, #b43333 100%);
  }

  .inline-action-form {
    margin: 0;
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

  .inline-button.subtle-link {
    color: var(--text-primary);
    border-color: var(--border-strong);
    background: var(--surface-strong);
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
