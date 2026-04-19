# Agent Rules

This file is the repository-level execution contract for future work in `ibkr-trader`.

## General

- Treat the system as production-oriented infrastructure, even when a feature is still in MVP form.
- Prefer explicit failures over silent fallbacks when the real system state cannot be established safely.
- Keep the write path, runtime, ledger, and UI clearly separated in both code and design.

## Data Integrity

- Never add dummy data, placeholder rows, fake broker responses, or synthetic success states to make the system appear healthy.
- If a real dependency is unavailable or a real state cannot be resolved, raise a clear error.
- Do not hide broker, market-data, ledger, or reconciliation problems behind default values that look valid.
- Persist real raw broker payloads when they are needed for audit and debugging.

## Documentation and Code Clarity

- Document new modules, classes, and important functions with short factual docstrings.
- Add comments only when they explain a real design choice or operational constraint.
- Keep error messages concrete so operators can tell what failed, where it failed, and why it matters.

## Runtime and Ledger

- Instructions are intent, not the final operational truth.
- Broker orders, order events, fills, account snapshots, and position snapshots belong in the ledger as first-class records.
- UI views should prefer local projections over live broker requests where practical.

## Testing

- Add or update tests whenever schema, runtime state, or API behavior changes.
- Tests may use controlled fixtures and in-memory databases, but runtime code must never fabricate live-system values.
