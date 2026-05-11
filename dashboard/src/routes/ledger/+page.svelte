<script>
  import { page } from '$app/stores';
  import { onMount } from 'svelte';

  export let data;

  const LIVE_SNAPSHOT_INTERVAL_MS = 1000;

  let liveHealth = data.health;
  let liveGeneratedAt = data.generatedAt;
  let liveErrors = data.errors;
  let ledgerSnapshot = data.ledgerSnapshot;
  let brokerMonitor = liveHealth.broker_monitor;
  let summary = ledgerSnapshot.summary;
  let focusInstruction = ledgerSnapshot.focus_instruction;
  let instructionEvents = ledgerSnapshot.instruction_events;
  let brokerOrderEvents = ledgerSnapshot.broker_order_events;
  let recentFills = ledgerSnapshot.recent_fills;
  let controlEvents = ledgerSnapshot.control_events;
  let instructionSetCancellations = ledgerSnapshot.instruction_set_cancellations;
  let reconciliationIssues = ledgerSnapshot.reconciliation_issues;
  let endpointErrors = [];
  let liveSnapshotStatus = {
    connected: false,
    received_at: null,
    last_error: null
  };

  $: brokerMonitor = liveHealth.broker_monitor;
  $: summary = ledgerSnapshot.summary;
  $: focusInstruction = ledgerSnapshot.focus_instruction;
  $: instructionEvents = ledgerSnapshot.instruction_events;
  $: brokerOrderEvents = ledgerSnapshot.broker_order_events;
  $: recentFills = ledgerSnapshot.recent_fills;
  $: controlEvents = ledgerSnapshot.control_events;
  $: instructionSetCancellations = ledgerSnapshot.instruction_set_cancellations;
  $: reconciliationIssues = ledgerSnapshot.reconciliation_issues;
  $: endpointErrors = Object.entries(liveErrors).filter(([, value]) => value);

  onMount(() => {
    if (!window.EventSource) {
      return;
    }
    const params = new URLSearchParams({ interval_ms: String(LIVE_SNAPSHOT_INTERVAL_MS) });
    const focusInstructionId = $page.url.searchParams.get('instruction_id')?.trim();
    if (focusInstructionId) {
      params.set('instruction_id', focusInstructionId);
    }
    const source = new EventSource(`/api/ledger/events?${params.toString()}`);
    source.onopen = () => {
      liveSnapshotStatus = { ...liveSnapshotStatus, connected: true, last_error: null };
    };
    source.addEventListener('ledger', (event) => {
      try {
        const payload = JSON.parse(event.data);
        liveHealth = payload.health ?? liveHealth;
        liveErrors = payload.errors ?? liveErrors;
        liveGeneratedAt = payload.received_at ?? new Date().toISOString();
        if (payload.ledgerSnapshot) {
          ledgerSnapshot = payload.ledgerSnapshot;
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
    source.addEventListener('ledger-error', (event) => {
      let message = 'Ledger live state unavailable';
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
        last_error: 'Ledger live event stream disconnected.'
      };
    };

    return () => {
      source.close();
    };
  });

  function brokerConnected(role) {
    return liveHealth.broker_sessions[role].connected === true;
  }

  function connectionLabel(role) {
    const heartbeat = brokerMonitor?.heartbeat ?? {};
    const session = liveHealth.broker_sessions[role] ?? {};
    if (heartbeat.is_stale) return 'Stale check';
    if (heartbeat.ok === false) return 'Gateway failing';
    if (session.connected === true) return 'Connected';
    if (session.cooldown_seconds_remaining !== null && session.cooldown_seconds_remaining !== undefined) {
      return 'Cooling down';
    }
    if (role === 'primary' && !session.last_error && Number(session.consecutive_failures ?? 0) === 0) {
      return 'Idle';
    }
    return 'Disconnected';
  }

  function connectionClass(role) {
    const heartbeat = brokerMonitor?.heartbeat ?? {};
    const session = liveHealth.broker_sessions[role] ?? {};
    if (heartbeat.is_stale) return 'warn';
    if (heartbeat.ok === false) return 'bad';
    if (session.connected === true) return 'ok';
    if (session.cooldown_seconds_remaining !== null && session.cooldown_seconds_remaining !== undefined) {
      return 'warn';
    }
    if (role === 'primary' && !session.last_error && Number(session.consecutive_failures ?? 0) === 0) {
      return 'ok';
    }
    return 'bad';
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
    if (liveSnapshotStatus.received_at) return `Snapshot pushed ${liveSnapshotStatus.received_at}`;
    return 'Opening live ledger stream';
  }
</script>

<svelte:head>
  <title>IBKR Trader Ledger Dashboard</title>
</svelte:head>

<div class="page">
  <header class="hero">
    <div class="hero-copy">
      <p class="eyebrow">IBKR Trader</p>
      <h1>Ledger Dashboard</h1>
      <p class="lede">
        Append-only operational history from the durable ledger: instruction events,
        broker order events, fills, operator controls, cancellations, and reconciliation issues.
      </p>
    </div>

    <div class="hero-meta">
      <div>
        <span>API</span>
        <strong>{data.apiBaseUrl}</strong>
      </div>
      <div>
        <span>Page updated</span>
        <strong>{liveGeneratedAt}</strong>
        <small class={liveSnapshotStatusClass()}>{liveSnapshotStatusLabel()} · {liveSnapshotStatusDetail()}</small>
      </div>
      <div>
        <span>Snapshot generated</span>
        <strong>{ledgerSnapshot.generated_at}</strong>
      </div>
    </div>
  </header>

  {#if focusInstruction}
    <section class="panel focus-panel">
      <div class="panel-head">
        <div>
          <h2>Focused Instruction</h2>
          <p>The ledger view is filtered to one persisted instruction.</p>
        </div>
        <a class="pill neutral clear-link" href="/ledger">Clear Filter</a>
      </div>

      <div class="focus-grid">
        <article class="focus-card">
          <span>Instruction</span>
          <strong class="mono">{focusInstruction.instruction_id}</strong>
          <small>
            {focusInstruction.symbol} · {focusInstruction.account_key} · {focusInstruction.book_key}
            {#if focusInstruction.is_virtual} · Virtual{/if}
          </small>
        </article>
        <article class="focus-card">
          <span>State</span>
          <strong>{focusInstruction.state}</strong>
          <small>Updated {focusInstruction.updated_at}</small>
        </article>
        <article class="focus-card">
          <span>Entry Window</span>
          <strong>{focusInstruction.submit_at}</strong>
          <small>Expires {focusInstruction.expire_at}</small>
        </article>
        <article class="focus-card">
          <span>Broker Orders</span>
          <strong>{focusInstruction.broker_order_id ?? 'n/a'}</strong>
          <small>{focusInstruction.broker_order_status ?? 'No live entry order'}</small>
        </article>
      </div>
    </section>
  {/if}

  {#if endpointErrors.length > 0}
    <section class="panel danger">
      <div class="panel-head">
        <h2>Endpoint Errors</h2>
        <p>The ledger dashboard shows real failures instead of fallback data.</p>
      </div>
      <ul class="event-list">
        {#each endpointErrors as [name, value]}
          <li>
            <strong>{name}</strong>
            <span>{value}</span>
          </li>
        {/each}
      </ul>
    </section>
  {/if}

  <section class="stat-grid">
    <article class="stat-card">
      <span>Primary Broker Session</span>
      <strong class={connectionClass('primary')}>{connectionLabel('primary')}</strong>
      <small>Client ID {liveHealth.broker_sessions.primary.client_id}, on demand</small>
    </article>
    <article class="stat-card">
      <span>Instruction Rows</span>
      <strong>{summary.instruction_count}</strong>
      <small>Persisted intent rows in scope</small>
    </article>
    <article class="stat-card">
      <span>Instruction Events</span>
      <strong>{summary.instruction_event_count}</strong>
      <small>Append-only instruction lifecycle events</small>
    </article>
    <article class="stat-card">
      <span>Broker Orders</span>
      <strong>{summary.broker_order_count}</strong>
      <small>Persisted broker order envelopes in scope</small>
    </article>
    <article class="stat-card">
      <span>Broker Order Events</span>
      <strong>{summary.broker_order_event_count}</strong>
      <small>Broker callback and order lifecycle rows</small>
    </article>
    <article class="stat-card">
      <span>Fills</span>
      <strong>{summary.execution_fill_count}</strong>
      <small>Durable executions in scope</small>
    </article>
    <article class="stat-card">
      <span>Control Events</span>
      <strong>{summary.control_event_count}</strong>
      <small>Kill switch and future controls history</small>
    </article>
    <article class="stat-card">
      <span>Cancellation Requests</span>
      <strong>{summary.instruction_set_cancellation_count}</strong>
      <small>Operator cancellation audit rows</small>
    </article>
    <article class="stat-card">
      <span>Reconciliation Issues</span>
      <strong>{summary.reconciliation_issue_count}</strong>
      <small>Durable warning and error rows in scope</small>
    </article>
  </section>

  <section class="panel">
    <div class="panel-head">
      <h2>Instruction Events</h2>
      <p>Append-only instruction lifecycle history.</p>
    </div>
    {#if instructionEvents.length === 0}
      <p class="empty">No instruction events were found for the current scope.</p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>When</th>
              <th>Instruction</th>
              <th>Symbol</th>
              <th>Event</th>
              <th>State</th>
              <th>Source</th>
              <th>Note</th>
            </tr>
          </thead>
          <tbody>
            {#each instructionEvents as event}
              <tr>
                <td>{event.event_at}</td>
                <td class="mono">
                  {event.instruction_id}
                  {#if event.is_virtual}<span class="mini-badge">Virtual</span>{/if}
                </td>
                <td>{event.symbol}</td>
                <td>{event.event_type}</td>
                <td>{event.state_before ?? 'n/a'} → {event.state_after ?? 'n/a'}</td>
                <td>{event.source}</td>
                <td>{event.note ?? 'n/a'}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>

  <section class="panel">
    <div class="panel-head">
      <h2>Broker Order Events</h2>
      <p>Broker callback history and order-state transitions.</p>
    </div>
    {#if brokerOrderEvents.length === 0}
      <p class="empty">No broker order events were found for the current scope.</p>
    {:else}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>When</th>
              <th>Symbol</th>
              <th>Instruction</th>
              <th>Order</th>
              <th>Event</th>
              <th>Status</th>
              <th>Message</th>
            </tr>
          </thead>
          <tbody>
            {#each brokerOrderEvents as event}
              <tr>
                <td>{event.event_at}</td>
                <td>{event.symbol}</td>
                <td class="mono">
                  {event.instruction_id ?? 'n/a'}
                  {#if event.is_virtual}<span class="mini-badge">Virtual</span>{/if}
                </td>
                <td>{event.external_order_id ?? event.broker_order_id}</td>
                <td>{event.event_type}</td>
                <td>{event.status_before ?? 'n/a'} → {event.status_after ?? 'n/a'}</td>
                <td>{event.message ?? event.note ?? 'n/a'}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </section>

  <section class="two-up">
    <section class="panel">
      <div class="panel-head">
        <h2>Recent Fills</h2>
        <p>Durable execution rows written independently of instruction summary state.</p>
      </div>
      {#if recentFills.length === 0}
        <p class="empty">No fills were found for the current scope.</p>
      {:else}
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>When</th>
                <th>Instruction</th>
                <th>Symbol</th>
                <th>Side</th>
                <th>Qty</th>
                <th>Price</th>
                <th>Commission</th>
              </tr>
            </thead>
            <tbody>
              {#each recentFills as fill}
                <tr>
                  <td>{fill.executed_at}</td>
                  <td class="mono">
                    {fill.instruction_id ?? 'n/a'}
                    {#if fill.is_virtual}<span class="mini-badge">Virtual</span>{/if}
                  </td>
                  <td>{fill.symbol}</td>
                  <td>{fill.side ?? 'n/a'}</td>
                  <td>{fill.quantity}</td>
                  <td>{fill.price}</td>
                  <td>{fill.commission ?? 'n/a'} {fill.commission_currency ?? ''}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {/if}
    </section>

    <section class="panel">
      <div class="panel-head">
        <h2>Control Events</h2>
        <p>Operator control changes, starting with the global kill switch.</p>
      </div>
      {#if controlEvents.length === 0}
        <p class="empty">No control events were recorded yet.</p>
      {:else}
        <ul class="event-list">
          {#each controlEvents as event}
            <li>
              <div class="event-main">
                <span class={`pill ${event.enabled ? 'bad' : 'ok'}`}>{event.event_type}</span>
                <strong>{event.control_key}</strong>
                <span>{event.updated_by ?? 'n/a'}</span>
              </div>
              <p>{event.reason ?? event.note ?? 'No reason was recorded.'}</p>
              <small>{event.event_at} · {event.source}</small>
            </li>
          {/each}
        </ul>
      {/if}
    </section>
  </section>

  <section class="two-up">
    <section class="panel">
      <div class="panel-head">
        <h2>Instruction Set Cancellations</h2>
        <p>Durable audit rows for operator-triggered cancellation requests.</p>
      </div>
      {#if instructionSetCancellations.length === 0}
        <p class="empty">No instruction-set cancellation rows were found.</p>
      {:else}
        <ul class="event-list">
          {#each instructionSetCancellations as cancellation}
            <li>
              <div class="event-main">
                <span class="pill neutral">{cancellation.status}</span>
                <strong>{cancellation.requested_by}</strong>
                <span>{cancellation.requested_at}</span>
              </div>
              <p>{cancellation.reason ?? 'No reason was recorded.'}</p>
              <small>
                matched {cancellation.matched_instruction_count}
                · pending {cancellation.cancelled_pending_count}
                · submitted {cancellation.cancelled_submitted_count}
                · skipped {cancellation.skipped_count}
                · failed {cancellation.failed_count}
              </small>
            </li>
          {/each}
        </ul>
      {/if}
    </section>

    <section class="panel">
      <div class="panel-head">
        <h2>Reconciliation Issues</h2>
        <p>Warnings and mismatches discovered by startup or runtime reconciliation.</p>
      </div>
      {#if reconciliationIssues.length === 0}
        <p class="empty">No reconciliation issues were found for the current scope.</p>
      {:else}
        <ul class="event-list">
          {#each reconciliationIssues as issue}
            <li>
              <div class="event-main">
                <span class={`pill ${issue.severity === 'ERROR' ? 'bad' : 'warn'}`}>{issue.severity}</span>
                <strong>{issue.stage}</strong>
                <span>{issue.run_kind}</span>
              </div>
              <p>{issue.message}</p>
              <small>
                {issue.observed_at}
                {#if issue.instruction_id}
                  · <span class="mono">{issue.instruction_id}</span>
                {/if}
              </small>
            </li>
          {/each}
        </ul>
      {/if}
    </section>
  </section>
</div>

<style>
  .page {
    width: 100%;
    box-sizing: border-box;
    padding: 2rem 1.25rem 4rem;
  }

  .hero {
    display: flex;
    justify-content: space-between;
    align-items: end;
    gap: 2rem;
    margin-bottom: 1.5rem;
  }

  .hero-copy {
    max-width: 50rem;
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
    max-width: 44rem;
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
  .focus-card {
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
  .empty {
    color: var(--text-muted);
  }

  .panel {
    padding: 1rem 1rem 1.15rem;
    margin-bottom: 1.25rem;
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

  .panel-head h2 {
    margin: 0;
    font-size: 1.05rem;
  }

  .panel-head p {
    margin: 0.25rem 0 0;
    color: var(--text-secondary);
  }

  .focus-grid,
  .two-up {
    display: grid;
    gap: 1rem;
  }

  .focus-grid {
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  }

  .two-up {
    grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
  }

  .focus-card {
    padding: 1rem;
    display: grid;
    gap: 0.35rem;
  }

  .focus-card span {
    color: var(--text-muted);
    font-size: 0.82rem;
  }

  .focus-card strong {
    font-size: 1rem;
  }

  .focus-card small {
    color: var(--text-secondary);
  }

  .table-wrap {
    overflow-x: auto;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.94rem;
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
    text-decoration: none;
  }

  .pill.neutral {
    color: var(--text-secondary);
    border-color: var(--border-strong);
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

  .ok {
    color: var(--ok);
  }

  .warn {
    color: var(--warn);
  }

  .bad {
    color: var(--bad);
  }

  .mono {
    font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
  }

  .event-list {
    list-style: none;
    margin: 0;
    padding: 0;
    display: grid;
    gap: 0.85rem;
  }

  .event-list li {
    display: grid;
    gap: 0.25rem;
    padding: 0.9rem 0.95rem;
    border: 1px solid var(--border);
    border-radius: 0.9rem;
    background: var(--surface-strong);
  }

  .event-main {
    display: flex;
    gap: 0.5rem;
    align-items: center;
    flex-wrap: wrap;
  }

  .event-list p {
    margin: 0;
    color: var(--text-secondary);
  }

  .event-list small {
    color: var(--text-muted);
  }

  .clear-link:hover {
    border-color: var(--border-strong);
    background: var(--surface-strong);
  }

  @media (max-width: 900px) {
    .hero,
    .panel-head {
      flex-direction: column;
      align-items: start;
    }
  }
</style>
