<script>
  import { accountDayChart } from '../performance.js';
  import { formatMoney, formatReturnPct } from '../formatting.js';
  import { formatTimestamp } from '../status.js';

  export let accounts;
  export let filteredBrokerAttention;
  export let acknowledgeAllLogsResult;
  export let brokerAttentionActionResult;
  export let visibleBrokerAttentionEventIds;
  export let visibleReconciliationIssueIds;
  export let filteredReconciliation;
  export let aggregatedReconciliation;
  export let reconciliationClearResult;
  export let reconciliationIssueActionResult;
  export let buttonStateClass;
  export let buttonIsBusy;
  export let buttonLabel;
  export let enhanceDashboardAction;
</script>

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
