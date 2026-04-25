<script>
  import { browser } from '$app/environment';
  import { applyAction, enhance } from '$app/forms';
  import { invalidateAll } from '$app/navigation';
  import { onMount } from 'svelte';

  export let data;
  export let form;

  const AUTO_REFRESH_INTERVAL_MS = 15000;
  const BUTTON_CLICK_TO_WORK_MS = 120;
  const BUTTON_SUCCESS_RESET_MS = 1600;
  const BUTTON_ERROR_RESET_MS = 2200;

  let rlDashboard = {};
  let summary = {
    model_count: 0,
    deployment_count: 0,
    live_deployment_count: 0,
    running_deployment_count: 0,
    stale_heartbeat_count: 0,
    recent_action_count: 0
  };
  let models = [];
  let deployments = [];
  let recentActions = [];
  let endpointErrors = [];
  let recommendedShortActionSpace = [];
  let refreshInFlight = false;
  let buttonStates = {};

  $: rlDashboard = data.rlDashboard ?? {};
  $: summary = rlDashboard.summary ?? {
    model_count: 0,
    deployment_count: 0,
    live_deployment_count: 0,
    running_deployment_count: 0,
    stale_heartbeat_count: 0,
    recent_action_count: 0
  };
  $: models = rlDashboard.models ?? [];
  $: deployments = rlDashboard.deployments ?? [];
  $: recentActions = rlDashboard.recent_actions ?? [];
  $: endpointErrors = Object.entries(data.errors ?? {}).filter(([, value]) => value);
  $: recommendedShortActionSpace = data.recommendedShortActionSpace ?? [];

  function setButtonState(buttonKey, phase, message = null) {
    buttonStates = {
      ...buttonStates,
      [buttonKey]: {
        phase,
        message
      }
    };
  }

  function resetButtonState(buttonKey) {
    buttonStates = {
      ...buttonStates,
      [buttonKey]: {
        phase: 'idle',
        message: null
      }
    };
  }

  function scheduleButtonReset(buttonKey, delayMs) {
    setTimeout(() => {
      resetButtonState(buttonKey);
    }, delayMs);
  }

  function getButtonState(buttonKey) {
    return buttonStates[buttonKey] ?? { phase: 'idle', message: null };
  }

  function formEnhancer(buttonKey) {
    return enhance(async ({ result }) => {
      setButtonState(buttonKey, 'clicked');
      await new Promise((resolve) => setTimeout(resolve, BUTTON_CLICK_TO_WORK_MS));
      setButtonState(buttonKey, 'working');

      await applyAction(result);

      if (result.type === 'success') {
        setButtonState(buttonKey, 'done');
        await invalidateAll();
        scheduleButtonReset(buttonKey, BUTTON_SUCCESS_RESET_MS);
        return;
      }

      setButtonState(buttonKey, 'error');
      scheduleButtonReset(buttonKey, BUTTON_ERROR_RESET_MS);
    });
  }

  function formatTimestamp(value) {
    if (!value) return 'n/a';
    return new Intl.DateTimeFormat('sv-SE', {
      timeZone: 'Europe/Stockholm',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    }).format(new Date(value));
  }

  function stringifyJson(value) {
    return JSON.stringify(value ?? {}, null, 2);
  }

  function summarizeSymbols(symbols) {
    if (!symbols || symbols.length === 0) return 'All resolved symbols';
    if (symbols.length <= 4) return symbols.join(', ');
    return `${symbols.slice(0, 4).join(', ')} +${symbols.length - 4} more`;
  }

  async function refreshNow() {
    refreshInFlight = true;
    try {
      await invalidateAll();
    } finally {
      refreshInFlight = false;
    }
  }

  onMount(() => {
    const interval = setInterval(() => {
      invalidateAll();
    }, AUTO_REFRESH_INTERVAL_MS);
    return () => clearInterval(interval);
  });
</script>

<svelte:head>
  <title>RL Trader Dashboard</title>
</svelte:head>

<section class="page-shell">
  <header class="hero">
    <div>
      <p class="eyebrow">RL Trader</p>
      <h1>Model Registry And Execution Pane</h1>
      <p class="lede">
        This page is the early operator guide for account-bound RL deployments. Models,
        deployments, heartbeats, and action logs are durable already; instruction translation
        is the next layer we’ll attach.
      </p>
    </div>

    <div class="hero-actions">
      <button type="button" class="secondary" on:click={refreshNow} disabled={refreshInFlight}>
        {refreshInFlight ? 'Refreshing...' : 'Refresh'}
      </button>
    </div>
  </header>

  {#if endpointErrors.length > 0}
    <section class="panel error-panel">
      <h2>Endpoint Errors</h2>
      <ul>
        {#each endpointErrors as [name, message]}
          <li><strong>{name}</strong>: {message}</li>
        {/each}
      </ul>
    </section>
  {/if}

  <section class="summary-grid">
    <article class="summary-card">
      <span>Models</span>
      <strong>{summary.model_count}</strong>
    </article>
    <article class="summary-card">
      <span>Deployments</span>
      <strong>{summary.deployment_count}</strong>
    </article>
    <article class="summary-card">
      <span>Live</span>
      <strong>{summary.live_deployment_count}</strong>
    </article>
    <article class="summary-card">
      <span>Running</span>
      <strong>{summary.running_deployment_count}</strong>
    </article>
    <article class="summary-card">
      <span>Stale Heartbeats</span>
      <strong>{summary.stale_heartbeat_count}</strong>
    </article>
    <article class="summary-card">
      <span>Recent Actions</span>
      <strong>{summary.recent_action_count}</strong>
    </article>
  </section>

  <section class="panel">
    <div class="panel-header">
      <div>
        <h2>Promoted Short Action Space</h2>
        <p>
          This is the fixed action set currently exposed by the short-side research line and
          should stay synchronized with the registered model metadata.
        </p>
      </div>
    </div>

    <div class="pill-row">
      {#each recommendedShortActionSpace as actionName}
        <span class="pill">{actionName}</span>
      {/each}
    </div>
  </section>

  <section class="form-grid">
    <article class="panel">
      <div class="panel-header">
        <div>
          <h2>Register Model</h2>
          <p>Record promoted workflow lineage, action space, and observation contract.</p>
        </div>
      </div>

      {#if form?.registerModelResult}
        <p class:ok={form.registerModelResult.ok} class:error-text={!form.registerModelResult.ok}>
          {form.registerModelResult.message}
        </p>
      {/if}

      <form method="POST" action="?/registerModel" use:enhance={formEnhancer('registerModel')}>
        <label>
          <span>Model Key</span>
          <input name="model_key" value="short_trial36_v1" required />
        </label>
        <label>
          <span>Display Name</span>
          <input name="display_name" value="Short Trial 36 V1" required />
        </label>
        <label>
          <span>Strategy Family</span>
          <input name="strategy_family" value="canonical_short_live_execution_policy" required />
        </label>
        <label>
          <span>Side</span>
          <select name="side">
            <option value="SHORT">SHORT</option>
            <option value="LONG">LONG</option>
            <option value="MIXED">MIXED</option>
          </select>
        </label>
        <label>
          <span>Source Workflow Path</span>
          <input
            name="source_workflow_path"
            value="/home/mattias/dev/q-training-bucket-booster/workflows/canonical/short_live/v1/execution_policy_short_trial36_v1.yaml"
          />
        </label>
        <label>
          <span>Promoted Checkpoint Path</span>
          <input
            name="promoted_checkpoint_path"
            value="/home/mattias/dev/q-training-bucket-booster/artifacts/analysis/short_trial_36_ex_short_true_rl_dqn_w128_volnorm_market_context_triseed_v1/continuation/true_rl_dqn_w128_seed140/best_dqn_state.pt"
          />
        </label>
        <label>
          <span>Execution Mapping Version</span>
          <input name="execution_mapping_version" value="short_actions_v1" />
        </label>
        <label class="full-width">
          <span>Action Space</span>
          <textarea name="action_space" rows="4">{recommendedShortActionSpace.join('\n')}</textarea>
        </label>
        <label class="full-width">
          <span>Observation Contract JSON</span>
          <textarea name="observation_contract" rows="10">{`{
  "bar_family": "stockholm_intraday_1m_v1",
  "required_series": ["TRADES", "MIDPOINT", "BID", "ASK", "ADJUSTED_LAST"],
  "include_market_context": true,
  "include_vol_normalized_intraday_state": true,
  "feature_schema_version": "short_live_v1"
}`}</textarea>
        </label>
        <label class="full-width">
          <span>Metadata JSON</span>
          <textarea name="metadata" rows="6">{`{
  "notes": ["Promoted short-side true RL recipe"],
  "canonical_seed": 140
}`}</textarea>
        </label>

        <button
          type="submit"
          class="primary {getButtonState('registerModel').phase}"
          disabled={getButtonState('registerModel').phase === 'working'}
        >
          {#if getButtonState('registerModel').phase === 'working'}
            Registering...
          {:else if getButtonState('registerModel').phase === 'done'}
            Registered
          {:else if getButtonState('registerModel').phase === 'error'}
            Retry Register
          {:else}
            Register Model
          {/if}
        </button>
      </form>
    </article>

    <article class="panel">
      <div class="panel-header">
        <div>
          <h2>Create Deployment</h2>
          <p>Bind one RL deployment to one real account and one internal book.</p>
        </div>
      </div>

      {#if form?.createDeploymentResult}
        <p class:ok={form.createDeploymentResult.ok} class:error-text={!form.createDeploymentResult.ok}>
          {form.createDeploymentResult.message}
        </p>
      {/if}

      <form method="POST" action="?/createDeployment" use:enhance={formEnhancer('createDeployment')}>
        <label>
          <span>Deployment Key</span>
          <input name="deployment_key" value="short_trial36_live_01" required />
        </label>
        <label>
          <span>Model Key</span>
          <input name="model_key" value="short_trial36_v1" required />
        </label>
        <label>
          <span>IBKR Account</span>
          <input name="account_key" placeholder="U25245596" required />
        </label>
        <label>
          <span>Book Key</span>
          <input name="book_key" value="rl_short_trial36_live_01" required />
        </label>
        <label>
          <span>Mode</span>
          <select name="mode">
            <option value="paper">paper</option>
            <option value="live">live</option>
          </select>
        </label>
        <label>
          <span>Status</span>
          <select name="status">
            <option value="draft">draft</option>
            <option value="paused">paused</option>
            <option value="running">running</option>
            <option value="degraded">degraded</option>
            <option value="stopped">stopped</option>
          </select>
        </label>
        <label class="full-width">
          <span>Allowed Symbols</span>
          <textarea name="allowed_symbols" rows="3" placeholder="SIVE VOLV-B ABB"></textarea>
        </label>
        <label class="full-width">
          <span>Risk Limits JSON</span>
          <textarea name="risk_limits" rows="8">{`{
  "max_open_positions": 8,
  "max_notional_per_name_sek": 25000,
  "max_daily_turnover_sek": 200000
}`}</textarea>
        </label>
        <label class="full-width">
          <span>Action Constraints JSON</span>
          <textarea name="action_constraints" rows="8">{`{
  "position_side": "SHORT",
  "allow_actions": ["skip", "wait", "market_entry", "cancel_entry", "exit_market", "clear_exit", "entry_prevclose_88bp", "exit_tp_180bp"],
  "state_machine_version": "short_symbol_state_v1"
}`}</textarea>
        </label>
        <label class="full-width">
          <span>Metadata JSON</span>
          <textarea name="metadata" rows="5">{`{
  "operator_notes": "One real account per autonomous deployment."
}`}</textarea>
        </label>

        <button
          type="submit"
          class="primary {getButtonState('createDeployment').phase}"
          disabled={getButtonState('createDeployment').phase === 'working'}
        >
          {#if getButtonState('createDeployment').phase === 'working'}
            Creating...
          {:else if getButtonState('createDeployment').phase === 'done'}
            Created
          {:else if getButtonState('createDeployment').phase === 'error'}
            Retry Create
          {:else}
            Create Deployment
          {/if}
        </button>
      </form>
    </article>
  </section>

  <section class="form-grid">
    <article class="panel">
      <div class="panel-header">
        <div>
          <h2>Log Action</h2>
          <p>Append a model action so we can inspect the feed and state transitions early.</p>
        </div>
      </div>

      {#if form?.logActionResult}
        <p class:ok={form.logActionResult.ok} class:error-text={!form.logActionResult.ok}>
          {form.logActionResult.message}
        </p>
      {/if}

      <form method="POST" action="?/logAction" use:enhance={formEnhancer('logAction')}>
        <label>
          <span>Deployment Key</span>
          <input name="deployment_key" value="short_trial36_live_01" required />
        </label>
        <label>
          <span>Symbol</span>
          <input name="symbol" value="SIVE" required />
        </label>
        <label>
          <span>Action</span>
          <select name="action_name">
            {#each recommendedShortActionSpace as actionName}
              <option value={actionName}>{actionName}</option>
            {/each}
          </select>
        </label>
        <label>
          <span>Observed At</span>
          <input name="observed_at" placeholder="2026-04-25T09:25:00+02:00" />
        </label>
        <label>
          <span>State Before</span>
          <input name="state_before" value="FLAT" />
        </label>
        <label>
          <span>State After</span>
          <input name="state_after" value="ENTRY_PENDING" />
        </label>
        <label>
          <span>Action Status</span>
          <input name="action_status" value="logged" />
        </label>
        <label>
          <span>Instruction ID</span>
          <input name="instruction_id" placeholder="optional until translation exists" />
        </label>
        <label class="full-width">
          <span>Note</span>
          <input name="note" value="Manual early dashboard log for execution-shape walkthrough." />
        </label>
        <label class="full-width">
          <span>Payload JSON</span>
          <textarea name="payload" rows="6">{`{
  "reason": "dashboard_walkthrough",
  "policy_confidence": 0.73
}`}</textarea>
        </label>

        <button
          type="submit"
          class="primary {getButtonState('logAction').phase}"
          disabled={getButtonState('logAction').phase === 'working'}
        >
          {#if getButtonState('logAction').phase === 'working'}
            Logging...
          {:else if getButtonState('logAction').phase === 'done'}
            Logged
          {:else if getButtonState('logAction').phase === 'error'}
            Retry Log
          {:else}
            Log Action
          {/if}
        </button>
      </form>
    </article>

    <article class="panel">
      <div class="panel-header">
        <div>
          <h2>Update Heartbeat</h2>
          <p>Feed runtime liveness, last bar, and freshness state into the operator view.</p>
        </div>
      </div>

      {#if form?.updateHeartbeatResult}
        <p class:ok={form.updateHeartbeatResult.ok} class:error-text={!form.updateHeartbeatResult.ok}>
          {form.updateHeartbeatResult.message}
        </p>
      {/if}

      <form method="POST" action="?/updateHeartbeat" use:enhance={formEnhancer('updateHeartbeat')}>
        <label>
          <span>Deployment Key</span>
          <input name="deployment_key" value="short_trial36_live_01" required />
        </label>
        <label>
          <span>Status</span>
          <select name="status">
            <option value="running">running</option>
            <option value="paused">paused</option>
            <option value="degraded">degraded</option>
            <option value="stopped">stopped</option>
          </select>
        </label>
        <label>
          <span>Last Seen At</span>
          <input name="last_seen_at" placeholder="2026-04-25T09:30:00+02:00" />
        </label>
        <label>
          <span>Last Bar At</span>
          <input name="last_bar_at" placeholder="2026-04-25T09:29:00+02:00" />
        </label>
        <label>
          <span>Last Action At</span>
          <input name="last_action_at" placeholder="2026-04-25T09:25:00+02:00" />
        </label>
        <label class="full-width">
          <span>Runtime Error</span>
          <input name="runtime_error" placeholder="leave blank when healthy" />
        </label>
        <label class="full-width">
          <span>Metrics JSON</span>
          <textarea name="metrics" rows="7">{`{
  "bar_lag_seconds": 4,
  "action_queue_depth": 0,
  "policy_step_ms": 18
}`}</textarea>
        </label>

        <button
          type="submit"
          class="primary {getButtonState('updateHeartbeat').phase}"
          disabled={getButtonState('updateHeartbeat').phase === 'working'}
        >
          {#if getButtonState('updateHeartbeat').phase === 'working'}
            Updating...
          {:else if getButtonState('updateHeartbeat').phase === 'done'}
            Updated
          {:else if getButtonState('updateHeartbeat').phase === 'error'}
            Retry Update
          {:else}
            Update Heartbeat
          {/if}
        </button>
      </form>
    </article>
  </section>

  <section class="panel">
    <div class="panel-header">
      <div>
        <h2>Deployments</h2>
        <p>One real account is the hard boundary; the book is the internal sleeve.</p>
      </div>
    </div>

    {#if deployments.length === 0}
      <p class="empty-state">No deployments yet.</p>
    {:else}
      <div class="deployment-grid">
        {#each deployments as deployment}
          <article class="deployment-card">
            <div class="card-head">
              <div>
                <h3>{deployment.deployment_key}</h3>
                <p>{deployment.model_display_name} · {deployment.model_key}</p>
              </div>
              <span class="status {deployment.status}">{deployment.status}</span>
            </div>

            <dl class="detail-grid">
              <div><dt>Account</dt><dd>{deployment.account_key}</dd></div>
              <div><dt>Book</dt><dd>{deployment.book_key}</dd></div>
              <div><dt>Mode</dt><dd>{deployment.mode}</dd></div>
              <div><dt>Updated</dt><dd>{formatTimestamp(deployment.updated_at)}</dd></div>
              <div class="full">
                <dt>Allowed Symbols</dt>
                <dd>{summarizeSymbols(deployment.allowed_symbols)}</dd>
              </div>
            </dl>

            {#if deployment.heartbeat}
              <div class="heartbeat {deployment.heartbeat.is_stale ? 'stale' : 'fresh'}">
                <strong>Heartbeat · {deployment.heartbeat.status}</strong>
                <span>Seen {formatTimestamp(deployment.heartbeat.last_seen_at)}</span>
                <span>Last bar {formatTimestamp(deployment.heartbeat.last_bar_at)}</span>
                <span>Last action {formatTimestamp(deployment.heartbeat.last_action_at)}</span>
                {#if deployment.heartbeat.runtime_error}
                  <span class="error-text">{deployment.heartbeat.runtime_error}</span>
                {/if}
              </div>
            {:else}
              <div class="heartbeat missing">No heartbeat yet.</div>
            {/if}
          </article>
        {/each}
      </div>
    {/if}
  </section>

  <section class="panel">
    <div class="panel-header">
      <div>
        <h2>Models</h2>
        <p>Registry view of promoted workflow lineage and expected observation contract.</p>
      </div>
    </div>

    {#if models.length === 0}
      <p class="empty-state">No models registered yet.</p>
    {:else}
      <div class="model-grid">
        {#each models as model}
          <article class="model-card">
            <div class="card-head">
              <div>
                <h3>{model.display_name}</h3>
                <p>{model.model_key}</p>
              </div>
              <span class="status neutral">{model.side}</span>
            </div>

            <p class="subtle">{model.strategy_family}</p>
            <div class="pill-row">
              {#each model.action_space as actionName}
                <span class="pill">{actionName}</span>
              {/each}
            </div>

            <details>
              <summary>Observation Contract</summary>
              <pre>{stringifyJson(model.observation_contract)}</pre>
            </details>
          </article>
        {/each}
      </div>
    {/if}
  </section>

  <section class="panel">
    <div class="panel-header">
      <div>
        <h2>Recent Actions</h2>
        <p>Append-only action stream for the RL runtime and its execution translation layer.</p>
      </div>
    </div>

    {#if recentActions.length === 0}
      <p class="empty-state">No actions logged yet.</p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Deployment</th>
              <th>Account</th>
              <th>Symbol</th>
              <th>Action</th>
              <th>Status</th>
              <th>State</th>
              <th>Instruction</th>
            </tr>
          </thead>
          <tbody>
            {#each recentActions as action}
              <tr>
                <td>{formatTimestamp(action.action_at)}</td>
                <td>
                  <strong>{action.deployment_key}</strong>
                  <div class="muted">{action.model_display_name}</div>
                </td>
                <td>{action.account_key}</td>
                <td>{action.symbol}</td>
                <td>{action.action_name}</td>
                <td><span class="status neutral">{action.action_status}</span></td>
                <td>{action.state_before ?? 'n/a'} → {action.state_after ?? 'n/a'}</td>
                <td>{action.instruction_id ?? 'n/a'}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>
</section>

<style>
  .page-shell {
    padding: 1.4rem;
    display: grid;
    gap: 1.2rem;
  }

  .hero,
  .panel,
  .summary-card,
  .deployment-card,
  .model-card {
    border: 1px solid var(--panel-border);
    background: var(--surface);
    border-radius: 1.2rem;
    box-shadow: 0 12px 30px var(--shadow);
  }

  .hero,
  .panel {
    padding: 1.2rem 1.25rem;
  }

  .hero {
    display: flex;
    justify-content: space-between;
    gap: 1rem;
    align-items: flex-start;
  }

  .eyebrow {
    margin: 0 0 0.35rem;
    color: var(--accent);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    font-size: 0.78rem;
    font-weight: 700;
  }

  h1,
  h2,
  h3,
  p {
    margin: 0;
  }

  h1 {
    font-size: clamp(1.6rem, 2vw, 2.2rem);
    margin-bottom: 0.45rem;
  }

  .lede,
  .subtle,
  .muted,
  .panel-header p,
  .heartbeat span,
  .detail-grid dt,
  .empty-state {
    color: var(--muted);
  }

  .hero-actions {
    display: flex;
    align-items: center;
  }

  .summary-grid,
  .form-grid,
  .deployment-grid,
  .model-grid {
    display: grid;
    gap: 1rem;
  }

  .summary-grid {
    grid-template-columns: repeat(auto-fit, minmax(10rem, 1fr));
  }

  .summary-card {
    padding: 1rem 1.05rem;
    display: grid;
    gap: 0.4rem;
  }

  .summary-card span {
    color: var(--muted);
    font-size: 0.9rem;
  }

  .summary-card strong {
    font-size: 1.65rem;
  }

  .form-grid {
    grid-template-columns: repeat(auto-fit, minmax(24rem, 1fr));
  }

  .panel-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 1rem;
    margin-bottom: 1rem;
  }

  form {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 0.85rem;
  }

  label {
    display: grid;
    gap: 0.35rem;
  }

  label span {
    font-size: 0.88rem;
    color: var(--muted);
  }

  .full-width {
    grid-column: 1 / -1;
  }

  input,
  select,
  textarea,
  button {
    font: inherit;
  }

  input,
  select,
  textarea {
    width: 100%;
    box-sizing: border-box;
    border: 1px solid var(--panel-border);
    border-radius: 0.85rem;
    background: var(--surface-strong);
    color: var(--text);
    padding: 0.7rem 0.8rem;
  }

  textarea {
    resize: vertical;
    min-height: 4.5rem;
  }

  button {
    border: 1px solid transparent;
    border-radius: 999px;
    padding: 0.72rem 1rem;
    cursor: pointer;
    transition:
      transform 140ms ease,
      opacity 140ms ease,
      background 140ms ease,
      border-color 140ms ease;
  }

  button:hover:not(:disabled) {
    transform: translateY(-1px);
  }

  button:disabled {
    cursor: wait;
    opacity: 0.7;
  }

  .primary {
    grid-column: 1 / -1;
    background: linear-gradient(135deg, var(--accent), var(--accent-2));
    color: white;
    font-weight: 700;
  }

  .secondary {
    background: var(--surface-strong);
    color: var(--text);
    border-color: var(--panel-border);
  }

  .primary.working {
    filter: saturate(0.8);
  }

  .primary.done {
    background: color-mix(in oklab, var(--ok) 84%, white 16%);
  }

  .primary.error {
    background: color-mix(in oklab, var(--bad) 85%, white 15%);
  }

  .ok {
    color: var(--ok);
    font-weight: 600;
  }

  .error-panel,
  .error-text {
    color: var(--bad);
  }

  .error-panel {
    background: var(--danger-bg);
    border-color: var(--danger-border);
  }

  .pill-row {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
  }

  .pill,
  .status {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border-radius: 999px;
    padding: 0.28rem 0.7rem;
    font-size: 0.82rem;
    font-weight: 700;
  }

  .pill,
  .status.neutral {
    background: color-mix(in oklab, var(--accent) 14%, transparent);
    color: var(--text);
  }

  .status.running,
  .status.live {
    background: color-mix(in oklab, var(--ok) 18%, transparent);
    color: var(--ok);
  }

  .status.degraded,
  .status.paused {
    background: color-mix(in oklab, var(--warn) 16%, transparent);
    color: var(--warn);
  }

  .status.stopped,
  .status.draft {
    background: color-mix(in oklab, var(--bad) 12%, transparent);
    color: var(--bad);
  }

  .deployment-grid,
  .model-grid {
    grid-template-columns: repeat(auto-fit, minmax(19rem, 1fr));
  }

  .deployment-card,
  .model-card {
    padding: 1rem;
    display: grid;
    gap: 0.9rem;
  }

  .card-head {
    display: flex;
    justify-content: space-between;
    gap: 1rem;
    align-items: flex-start;
  }

  .detail-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 0.75rem;
    margin: 0;
  }

  .detail-grid div {
    display: grid;
    gap: 0.12rem;
  }

  .detail-grid .full {
    grid-column: 1 / -1;
  }

  .detail-grid dd {
    margin: 0;
  }

  .heartbeat {
    display: grid;
    gap: 0.22rem;
    border: 1px solid var(--panel-border);
    border-radius: 0.9rem;
    padding: 0.75rem 0.85rem;
    background: var(--surface-strong);
  }

  .heartbeat.fresh {
    border-color: color-mix(in oklab, var(--ok) 24%, var(--panel-border));
  }

  .heartbeat.stale,
  .heartbeat.missing {
    border-color: color-mix(in oklab, var(--warn) 30%, var(--panel-border));
  }

  details summary {
    cursor: pointer;
    font-weight: 700;
  }

  pre {
    margin: 0.6rem 0 0;
    padding: 0.9rem;
    border-radius: 0.9rem;
    background: color-mix(in oklab, var(--bg) 75%, black 6%);
    overflow-x: auto;
    font-size: 0.83rem;
  }

  .table-wrap {
    overflow-x: auto;
  }

  table {
    width: 100%;
    border-collapse: collapse;
  }

  th,
  td {
    text-align: left;
    padding: 0.75rem 0.6rem;
    border-bottom: 1px solid var(--panel-border);
    vertical-align: top;
  }

  tbody tr:hover {
    background: var(--table-row-hover);
  }

  @media (max-width: 820px) {
    .hero {
      flex-direction: column;
    }

    form {
      grid-template-columns: 1fr;
    }

    .detail-grid {
      grid-template-columns: 1fr;
    }
  }
</style>
