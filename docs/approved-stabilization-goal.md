# Approved IBKR Trader stabilization goal

Status: approved by Mattias

## Outcome

Operate one dedicated IB Gateway service and a source-independent, fail-closed trader deployment with protected bootstrap configuration, audited non-secret settings, existing-dashboard control-plane visibility, verified rollback, and evidence-backed stability observation.

## Approval gates

Explicit Mattias approval remains required before:

- production application cutover;
- any Gateway restart, stop, or configuration change;
- enabling watchdog restart authority;
- disabling the global kill switch.

## Non-negotiable rules

- The global kill switch remains enabled throughout this goal.
- The existing SvelteKit trader dashboard is the canonical trader and future stack UI. Extend it; do not replace it or add a parallel dashboard.
- Secrets remain in `/etc/ibkr-trader/bootstrap.env`; PostgreSQL settings are non-secret and audited.
- The legacy `ibgateway.service` remains disabled and exactly one Gateway process is allowed.
- The watchdog remains alert-only while `WATCHDOG_RESTART_ENABLED=no`.
- The RL runner remains virtual.
- Every material mutation requires independent review and remediation of material findings.

## Phases

1. Recovery and stream policy: define/test named recovery and stream states before release cutover.
2. Secure bootstrap configuration: source-independent protected environment and fail-closed production startup.
3. Non-secret settings registry and existing dashboard `/settings` view.
4. Verifiable release pipeline: standalone provenance, active-tree comparison, source/import provenance, matched tests, reviewed evidence.
5. All-or-rollback application cutover: requires explicit approval.
6. Bounded stability observation: minimum watchdog, broker probe, monitor, session, and no-churn gates.

## Completion

Publish evidence covering release, source, unit, bootstrap, settings UI/API, Gateway/watchdog, tests, review verdicts, rollback, stability observation, and remaining IBKR 2FA limitations. Do not claim unattended 2FA is solved.
