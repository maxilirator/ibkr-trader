<script>
  import { page } from '$app/stores';

  const navigationItems = [
    { href: '/', label: 'Operator Dashboard' },
    { href: '/ledger', label: 'Ledger Dashboard' }
  ];

  function isActive(href, pathname) {
    if (href === '/') {
      return pathname === '/';
    }
    return pathname.startsWith(href);
  }
</script>

<div class="layout-shell">
  <header class="app-header">
    <a class="brand" href="/">
      <span class="brand-mark">IB</span>
      <span class="brand-copy">
        <strong>IBKR Trader</strong>
        <small>Operator Console</small>
      </span>
    </a>

    <nav class="page-nav" aria-label="Pages">
      {#each navigationItems as item}
        <a href={item.href} class:active={isActive(item.href, $page.url.pathname)}>
          {item.label}
        </a>
      {/each}
    </nav>
  </header>

  <slot />
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

    --bg: var(--bg-start);
    --panel-border: var(--border);
    --text: var(--text-primary);
    --muted: var(--text-muted);
    --accent: #b36a11;
    --accent-2: #d79c2c;
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

      --bg: var(--bg-start);
      --panel-border: var(--border);
      --text: var(--text-primary);
      --muted: var(--text-muted);
      --accent: #d8a347;
      --accent-2: #f0c46b;
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

  .layout-shell {
    min-height: 100vh;
  }

  .app-header {
    position: sticky;
    top: 0;
    z-index: 20;
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 1rem;
    padding: 0.9rem 1.4rem;
    border-bottom: 1px solid var(--panel-border);
    background:
      linear-gradient(135deg, color-mix(in oklab, var(--bg) 90%, var(--accent) 10%), var(--bg)),
      var(--bg);
    backdrop-filter: blur(14px);
  }

  .brand {
    display: inline-flex;
    align-items: center;
    gap: 0.8rem;
    color: inherit;
    text-decoration: none;
  }

  .brand-mark {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 2.35rem;
    height: 2.35rem;
    border-radius: 0.85rem;
    background: linear-gradient(135deg, var(--accent), var(--accent-2));
    color: white;
    font-weight: 800;
    letter-spacing: 0.08em;
  }

  .brand-copy {
    display: grid;
    gap: 0.1rem;
  }

  .brand-copy strong {
    font-size: 0.98rem;
  }

  .brand-copy small {
    color: var(--muted);
  }

  .page-nav {
    display: flex;
    align-items: center;
    gap: 0.65rem;
    flex-wrap: wrap;
  }

  .page-nav a {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    padding: 0.55rem 0.95rem;
    border: 1px solid var(--panel-border);
    border-radius: 999px;
    color: var(--muted);
    text-decoration: none;
    transition:
      border-color 140ms ease,
      background 140ms ease,
      color 140ms ease,
      transform 140ms ease;
  }

  .page-nav a:hover,
  .page-nav a.active {
    color: var(--text);
    border-color: color-mix(in oklab, var(--accent) 45%, var(--panel-border));
    background: color-mix(in oklab, var(--accent) 18%, transparent);
    transform: translateY(-1px);
  }

  @media (max-width: 820px) {
    .app-header {
      align-items: flex-start;
      flex-direction: column;
    }
  }
</style>
