<script>
  import { formatTimestamp, killSwitchLabel } from '../status.js';

  export let killSwitch;
  export let killSwitchResult;
  export let archiveResult;
  export let startupReconcileResult;
  export let intentCleanupResult;
  export let sourceIntentGroups;
  export let actionableIntentCleanupGroups;
  export let intentCleanupGroups;
  export let virtualIntentCleanupGroupCount;
  export let buttonStateClass;
  export let buttonIsBusy;
  export let buttonLabel;
  export let enhanceDashboardAction;
</script>

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
