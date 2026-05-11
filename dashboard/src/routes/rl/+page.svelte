<script>
  import { applyAction, enhance } from '$app/forms';
  import { onMount } from 'svelte';

  export let data;

  const LIVE_SNAPSHOT_INTERVAL_MS = 1000;
  const BUTTON_CLICK_TO_WORK_MS = 120;
  const BUTTON_SUCCESS_RESET_MS = 1600;
  const BUTTON_ERROR_RESET_MS = 2200;

  let liveErrors = data.errors;
  let rlDashboard = data.rlDashboard;
  let summary = rlDashboard.summary;
  let models = rlDashboard.models;
  let deployments = rlDashboard.deployments;
  let candidates = rlDashboard.candidates ?? [];
  let recentActions = rlDashboard.recent_actions;
  let endpointErrors = [];
  let buttonStates = {};
  let liveSnapshotStatus = {
    connected: false,
    received_at: null,
    last_error: null
  };

  $: summary = rlDashboard.summary;
  $: models = rlDashboard.models;
  $: deployments = rlDashboard.deployments;
  $: candidates = rlDashboard.candidates ?? [];
  $: recentActions = rlDashboard.recent_actions;
  $: endpointErrors = Object.entries(liveErrors).filter(([, value]) => value);

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
    return () => {
      setButtonState(buttonKey, 'clicked');
      const transitionTimer = window.setTimeout(() => {
        if (getButtonState(buttonKey).phase === 'clicked') {
          setButtonState(buttonKey, 'working');
        }
      }, BUTTON_CLICK_TO_WORK_MS);

      return async ({ result }) => {
        window.clearTimeout(transitionTimer);
        await applyAction(result);

        if (result.type === 'success') {
          setButtonState(buttonKey, 'done');
          scheduleButtonReset(buttonKey, BUTTON_SUCCESS_RESET_MS);
          return;
        }

        setButtonState(buttonKey, 'error');
        scheduleButtonReset(buttonKey, BUTTON_ERROR_RESET_MS);
      };
    };
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

  function summarizeSymbols(symbols, deployment = null) {
    if (!symbols || symbols.length === 0) {
      if (deployment?.metadata?.daily_universe_source === 'model_routed_candidates') {
        return 'Daily model-routed candidates';
      }
      return 'No deployment allow-list';
    }
    if (symbols.length <= 4) return symbols.join(', ');
    return `${symbols.slice(0, 4).join(', ')} +${symbols.length - 4} more`;
  }

  function symbolsText(symbols) {
    return (symbols ?? []).join('\n');
  }

  function heartbeatMetric(deployment, key, fallback = 0) {
    return deployment?.heartbeat?.metrics?.[key] ?? fallback;
  }

  function heartbeatTiming(deployment) {
    return deployment?.heartbeat?.metrics?.timing ?? {};
  }

  function formatSeconds(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return 'n/a';
    const seconds = Number(value);
    if (seconds < 1) return `${Math.round(seconds * 1000)} ms`;
    return `${seconds.toFixed(2)} s`;
  }

  function formatPercent(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return 'n/a';
    return `${Number(value).toFixed(2)}%`;
  }

  function candidateModelId(candidate) {
    return candidate.model_id ?? candidate.trace?.model_id ?? 'n/a';
  }

  function candidateWindow(candidate) {
    const window = candidate.execution_window ?? {};
    return `${formatTimestamp(window.start_at)} to ${formatTimestamp(window.end_at)}`;
  }

  function candidateTargetNotional(candidate) {
    const notional = candidate.sizing?.target_notional;
    return notional ? `${notional} ${candidate.currency ?? ''}`.trim() : 'n/a';
  }

  onMount(() => {
    if (!window.EventSource) {
      return;
    }
    const source = new EventSource(`/api/rl/events?interval_ms=${LIVE_SNAPSHOT_INTERVAL_MS}`);
    source.onopen = () => {
      liveSnapshotStatus = { ...liveSnapshotStatus, connected: true, last_error: null };
    };
    source.addEventListener('rl', (event) => {
      try {
        const payload = JSON.parse(event.data);
        liveErrors = payload.errors ?? liveErrors;
        if (payload.rlDashboard) {
          rlDashboard = payload.rlDashboard;
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
    source.addEventListener('rl-error', (event) => {
      let message = 'RL live state unavailable';
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
        last_error: 'RL live event stream disconnected.'
      };
    };
    return () => source.close();
  });

  function liveSnapshotStatusClass() {
    if (liveSnapshotStatus.last_error) return liveSnapshotStatus.connected ? 'degraded' : 'stopped';
    return liveSnapshotStatus.connected ? 'running' : 'paused';
  }

  function liveSnapshotStatusLabel() {
    if (liveSnapshotStatus.last_error && !liveSnapshotStatus.connected) return 'Disconnected';
    if (liveSnapshotStatus.last_error) return 'Degraded';
    return liveSnapshotStatus.connected ? 'Live' : 'Connecting';
  }

  function liveSnapshotStatusDetail() {
    if (liveSnapshotStatus.last_error) return liveSnapshotStatus.last_error;
    if (liveSnapshotStatus.received_at) return `Snapshot pushed ${formatTimestamp(liveSnapshotStatus.received_at)}`;
    return 'Opening live RL stream';
  }
</script>

<svelte:head>
  <title>RL Trader Dashboard</title>
</svelte:head>

<section class="page-shell">
  <header class="hero">
    <div>
      <p class="eyebrow">RL Trader</p>
      <h1>Model Registry And Execution State</h1>
      <p class="lede">
        This page is the operator view for account-bound RL deployments, runner heartbeats,
        action logs, and virtual account state.
      </p>
    </div>

    <div class="hero-actions">
      <span class={`status ${liveSnapshotStatusClass()}`}>{liveSnapshotStatusLabel()}</span>
      <small>{liveSnapshotStatusDetail()}</small>
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
      <span>Virtual</span>
      <strong>{summary.virtual_deployment_count}</strong>
    </article>
    <article class="summary-card">
      <span>Running</span>
      <strong>{summary.running_deployment_count}</strong>
    </article>
    <article class="summary-card">
      <span>Candidates</span>
      <strong>{summary.candidate_count ?? candidates.length}</strong>
    </article>
    <article class="summary-card">
      <span>Bar-Ready</span>
      <strong>{summary.bar_ready_candidate_count ?? 0}</strong>
    </article>
    <article class="summary-card">
      <span>Any Bars</span>
      <strong>{summary.stream_any_bar_candidate_count ?? 0}</strong>
    </article>
    <article class="summary-card">
      <span>Evaluated</span>
      <strong>{summary.evaluated_candidate_count ?? 0}</strong>
    </article>
    <article class="summary-card">
      <span>Stale Bars</span>
      <strong>{summary.stale_decision_bar_candidate_count ?? 0}</strong>
    </article>
    <article class="summary-card">
      <span>No Current Bar</span>
      <strong>{summary.not_ready_candidate_count ?? 0}</strong>
    </article>
    <article class="summary-card">
      <span>Backfilled</span>
      <strong>{summary.backfilled_symbol_count ?? 0}</strong>
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
              <div>
                <dt>Account</dt>
                <dd>
                  {deployment.account_key}
                  {#if deployment.is_virtual}<span class="mini-badge">Virtual</span>{/if}
                </dd>
              </div>
              <div><dt>Book</dt><dd>{deployment.book_key}</dd></div>
              <div><dt>Mode</dt><dd>{deployment.mode}</dd></div>
              <div><dt>Updated</dt><dd>{formatTimestamp(deployment.updated_at)}</dd></div>
              <div class="full">
                <dt>Allowed Symbols</dt>
                <dd>{summarizeSymbols(deployment.allowed_symbols, deployment)}</dd>
              </div>
            </dl>

            {#if deployment.heartbeat}
              <div class="heartbeat {deployment.heartbeat.is_stale ? 'stale' : 'fresh'}">
                <strong>Heartbeat · {deployment.heartbeat.status}</strong>
                <span>Seen {formatTimestamp(deployment.heartbeat.last_seen_at)}</span>
                <span>Last bar {formatTimestamp(deployment.heartbeat.last_bar_at)}</span>
                <span>Last action {formatTimestamp(deployment.heartbeat.last_action_at)}</span>
                <span>
                  Fresh {heartbeatMetric(deployment, 'fresh_decision_bar_candidate_count')}
                  / Active {heartbeatMetric(deployment, 'active_candidate_count')}
                </span>
                <span>
                  Stale {heartbeatMetric(deployment, 'stale_decision_bar_candidate_count')}
                  · No bar {heartbeatMetric(deployment, 'not_ready_candidate_count')}
                  · Done {heartbeatMetric(deployment, 'already_processed_candidate_count')}
                </span>
                {#if deployment.heartbeat.metrics?.target_decision_bar_ended_at}
                  <span>Target bar {formatTimestamp(deployment.heartbeat.metrics.target_decision_bar_ended_at)}</span>
                {/if}
                {#if deployment.heartbeat.metrics?.timing}
                  <span class:warning-text={heartbeatTiming(deployment).cadence_over_budget}>
                    Loop {formatSeconds(heartbeatTiming(deployment).total_seconds)}
                    · Budget {formatPercent(heartbeatTiming(deployment).cadence_budget_used_pct)}
                    · Per name {formatSeconds(heartbeatTiming(deployment).seconds_per_active_candidate)}
                  </span>
                {/if}
                {#if deployment.heartbeat.runtime_error}
                  <span class="error-text">{deployment.heartbeat.runtime_error}</span>
                {/if}
              </div>
            {:else}
              <div class="heartbeat missing">No heartbeat yet.</div>
            {/if}

            <details class="deployment-edit">
              <summary>Edit Deployment</summary>
              <form
                method="POST"
                action="?/updateDeployment"
                use:enhance={formEnhancer(`updateDeployment-${deployment.deployment_key}`)}
              >
                <input type="hidden" name="deployment_key" value={deployment.deployment_key} />
                <label>
                  <span>Status</span>
                  <select name="status">
                    <option value="draft" selected={deployment.status === 'draft'}>draft</option>
                    <option value="paused" selected={deployment.status === 'paused'}>paused</option>
                    <option value="running" selected={deployment.status === 'running'}>running</option>
                    <option value="degraded" selected={deployment.status === 'degraded'}>degraded</option>
                    <option value="stopped" selected={deployment.status === 'stopped'}>stopped</option>
                  </select>
                </label>
                <label class="full-width">
                  <span>Allowed Symbols</span>
                  <textarea name="allowed_symbols" rows="4">{symbolsText(deployment.allowed_symbols)}</textarea>
                </label>
                <label class="full-width">
                  <span>Risk Limits JSON</span>
                  <textarea name="risk_limits" rows="5">{stringifyJson(deployment.risk_limits)}</textarea>
                </label>
                <label class="full-width">
                  <span>Action Constraints JSON</span>
                  <textarea name="action_constraints" rows="6">{stringifyJson(deployment.action_constraints)}</textarea>
                </label>
                <label class="full-width">
                  <span>Metadata JSON</span>
                  <textarea name="metadata" rows="5">{stringifyJson(deployment.metadata)}</textarea>
                </label>
                <button
                  type="submit"
                  class="secondary {getButtonState(`updateDeployment-${deployment.deployment_key}`).phase}"
                  disabled={['clicked', 'working'].includes(getButtonState(`updateDeployment-${deployment.deployment_key}`).phase)}
                >
                  {#if ['clicked', 'working'].includes(getButtonState(`updateDeployment-${deployment.deployment_key}`).phase)}
                    Updating...
                  {:else if getButtonState(`updateDeployment-${deployment.deployment_key}`).phase === 'done'}
                    Updated
                  {:else if getButtonState(`updateDeployment-${deployment.deployment_key}`).phase === 'error'}
                    Retry Update
                  {:else}
                    Update Deployment
                  {/if}
                </button>
              </form>
            </details>
          </article>
        {/each}
      </div>
    {/if}
  </section>

  <section class="panel">
    <div class="panel-header">
      <div>
        <h2>Candidate Feed</h2>
        <p>
          Model-routed names waiting for the runner to process bar by bar.
          {summary.bar_ready_candidate_count ?? 0} have the current completed decision bar;
          {summary.stream_any_bar_candidate_count ?? 0} have any stream bars.
        </p>
      </div>
    </div>

    {#if candidates.length === 0}
      <p class="empty-state">No model-routed candidates are currently loaded.</p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Queued</th>
              <th>Symbol</th>
              <th>Model</th>
              <th>Account / Book</th>
              <th>Side</th>
              <th>Target</th>
              <th>Window</th>
              <th>Candidate</th>
            </tr>
          </thead>
          <tbody>
            {#each candidates as candidate}
              <tr>
                <td>{formatTimestamp(candidate.updated_at)}</td>
                <td>{candidate.symbol}</td>
                <td>{candidateModelId(candidate)}</td>
                <td>
                  {candidate.account_key}
                  {#if candidate.is_virtual}<span class="mini-badge">Virtual</span>{/if}
                  <div class="muted">{candidate.book_key}</div>
                </td>
                <td>{candidate.side}</td>
                <td>{candidateTargetNotional(candidate)}</td>
                <td>{candidateWindow(candidate)}</td>
                <td class="mono">{candidate.instruction_id}</td>
              </tr>
            {/each}
          </tbody>
        </table>
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
    width: 100%;
    box-sizing: border-box;
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
    transition:
      opacity 140ms ease,
      background 140ms ease,
      border-color 140ms ease;
  }

  button:disabled {
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

  .secondary.done {
    border-color: var(--ok);
    color: var(--ok);
  }

  .secondary.error {
    border-color: var(--bad);
    color: var(--bad);
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

  .warning-text {
    color: var(--warn);
    font-weight: 700;
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

  .deployment-edit summary {
    color: var(--muted);
    font-weight: 700;
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
