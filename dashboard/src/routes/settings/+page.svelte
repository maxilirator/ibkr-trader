<script>
  export let data;

  $: settings = data.settings ?? [];
  $: categories = data.categories ?? [];
  $: undeclaredKeys = data.undeclaredKeys ?? [];
  $: errorCount = data.errorCount ?? 0;

  $: byCategory = categories.map((category) => ({
    category,
    rows: settings.filter((setting) => setting.category === category)
  }));

  function formatValue(value) {
    if (value === null || value === undefined) return '—';
    if (typeof value === 'boolean') return value ? 'true' : 'false';
    return String(value);
  }
</script>

<svelte:head>
  <title>Settings · IBKR Trader</title>
</svelte:head>

<main class="page">
  <header class="page-header">
    <div>
      <h1>Runtime settings</h1>
      <p class="subtitle">
        Declared non-secret operational settings and the value the API actually
        resolved. Secrets are not stored here &mdash; they live in the protected
        bootstrap environment file.
      </p>
    </div>
    <span class="badge read-only">Read only</span>
  </header>

  {#if errorCount > 0}
    <div class="notice bad">
      <strong>{errorCount} setting{errorCount === 1 ? '' : 's'} could not be parsed.</strong>
      The runtime is using the declared default for those; the stored value is not
      in effect.
    </div>
  {/if}

  {#if undeclaredKeys.length > 0}
    <div class="notice warn">
      <strong>Stored settings with no definition:</strong>
      {undeclaredKeys.join(', ')}. These rows affect nothing &mdash; they are either
      a typo or a setting removed from the code.
    </div>
  {/if}

  {#if settings.length === 0}
    <p class="empty">The API returned no declared settings.</p>
  {/if}

  {#each byCategory as group (group.category)}
    <section class="panel">
      <h2>{group.category}</h2>
      <table>
        <thead>
          <tr>
            <th>Setting</th>
            <th>Effective</th>
            <th>Default</th>
            <th>Source</th>
            <th>Changed by</th>
          </tr>
        </thead>
        <tbody>
          {#each group.rows as row (row.key)}
            <tr class:has-error={row.error}>
              <td>
                <code>{row.key}</code>
                <small>{row.description}</small>
                {#if row.error}
                  <small class="error">{row.error}</small>
                {/if}
              </td>
              <td class="value">{formatValue(row.effective_value)}</td>
              <td class="value muted">{formatValue(row.default_value)}</td>
              <td>
                <span class="badge" class:db={row.source === 'database'}>
                  {row.source}
                </span>
              </td>
              <td class="muted">
                {row.updated_by ?? '—'}
                {#if row.updated_at}
                  <small>{row.updated_at}</small>
                {/if}
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </section>
  {/each}
</main>

<style>
  .page {
    max-width: 68rem;
    margin: 0 auto;
    padding: 1.6rem 1.4rem 3rem;
    display: grid;
    gap: 1.2rem;
  }

  .page-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 1rem;
  }

  h1 {
    margin: 0 0 0.35rem;
    font-size: 1.4rem;
  }

  .subtitle {
    margin: 0;
    max-width: 48rem;
    color: var(--text-secondary);
    font-size: 0.9rem;
  }

  .panel {
    border: 1px solid var(--panel-border);
    border-radius: 0.9rem;
    background: var(--surface);
    padding: 1rem 1.1rem 1.2rem;
    box-shadow: 0 1px 2px var(--shadow);
  }

  h2 {
    margin: 0 0 0.7rem;
    font-size: 0.95rem;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--text-secondary);
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.88rem;
  }

  th {
    text-align: left;
    padding: 0.45rem 0.6rem;
    border-bottom: 1px solid var(--border-strong);
    color: var(--text-secondary);
    font-size: 0.78rem;
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  td {
    padding: 0.6rem;
    border-bottom: 1px solid var(--border);
    vertical-align: top;
  }

  tr:hover td {
    background: var(--table-row-hover);
  }

  tr.has-error td {
    background: var(--danger-bg);
  }

  code {
    font-family: "IBM Plex Mono", ui-monospace, monospace;
    font-size: 0.84rem;
  }

  small {
    display: block;
    margin-top: 0.2rem;
    color: var(--text-muted);
    font-size: 0.78rem;
  }

  small.error {
    color: var(--bad);
  }

  .value {
    font-family: "IBM Plex Mono", ui-monospace, monospace;
    white-space: nowrap;
  }

  .muted {
    color: var(--text-muted);
  }

  .badge {
    display: inline-block;
    padding: 0.15rem 0.55rem;
    border-radius: 999px;
    border: 1px solid var(--panel-border);
    font-size: 0.76rem;
    color: var(--text-secondary);
  }

  .badge.db {
    border-color: color-mix(in oklab, var(--accent) 45%, var(--panel-border));
    background: color-mix(in oklab, var(--accent) 20%, transparent);
    color: var(--text);
  }

  .badge.read-only {
    white-space: nowrap;
  }

  .notice {
    border-radius: 0.7rem;
    padding: 0.7rem 0.9rem;
    font-size: 0.88rem;
    border: 1px solid var(--panel-border);
  }

  .notice.bad {
    border-color: var(--danger-border);
    background: var(--danger-bg);
  }

  .notice.warn {
    border-color: color-mix(in oklab, var(--warn) 40%, var(--panel-border));
    background: color-mix(in oklab, var(--warn) 12%, transparent);
  }

  .empty {
    color: var(--text-muted);
  }
</style>
