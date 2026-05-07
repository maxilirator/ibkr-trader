# RL Operational Readiness

This is the morning checklist for making the long and short RL agents real.

## Ready In This Repo

- model registry and deployment registry
- shared virtual account support with market-watch quotes and virtual fills
- model-routed candidate intake as `MODEL_ROUTED_PENDING`
- persistent IBKR market-data stream subscribe/snapshot endpoints
- market-stream desired watchlist and automatic reconnect backoff
- explicit market-stream subscription cap and overflow reporting
- 1-minute source bars aggregated into model-facing 5-minute OHLC
- completed-5-minute-bar decision gating
- history and trailing-volatility feature payloads
- upstream static feature payload slot
- trader-local deployed model bundle registry in `src/ibkr_trader/rl/model_artifacts.py`
- registry bootstrap script: `scripts/bootstrap_rl_registry.py`
- candidate-list submitter: `scripts/submit_rl_candidate_lists.py`
- promoted DQN runner: `scripts/run_rl_agents.py`
- readiness checker: `scripts/check_operational_readiness.py`
- side-aware entry translation:
  - long `entry_prevclose_-50bp` -> `BUY/LONG` limit at previous close minus `0.50%`
  - short `entry_prevclose_88bp` -> `SELL/SHORT` limit at previous close plus `0.88%`
  - `market_entry` -> side-aware market entry
- durable RL-owned cancel and exit execution:
  - `cancel_entry` cancels the owned pending/submitted entry
  - long `exit_tp_200bp` -> `SELL` take-profit limit above entry fill
  - short `exit_tp_180bp` -> `BUY` take-profit limit below entry fill
  - `exit_market` submits an opposite-side market exit
  - `clear_exit` cancels the owned pending exit and keeps the position open
- virtual stream-crossing tests for long and short entries, exits, and cancel/clear actions

## Runner Contract

The promoted runner is `scripts/run_rl_agents.py`. It is designed to:

- poll `GET /v1/rl/candidates`
- treat the returned candidate set as the daily dynamic universe; do not assume
  fixed long/short counts
- require static candidate features from each model-routed instruction payload
- backfill prior 1-minute bars once per model/name/trade day and cache the
  previous-session/trailing-volatility override
- subscribe active names with `POST /v1/market-data/stream/subscribe`
- call `POST /v1/rl/observations/build` every minute from the stream buffer
- infer only when `model_decision.ready=true` and the `decision_id` is new
- infer only when the symbol has the current completed 5-minute decision bar;
  stale bars are reported as `stale_bar` instead of being traded late
- load the exact trader-local bundle checkpoint and action space
- call `POST /v1/rl/actions/translate` for entries, owned cancels, and owned exits
- write heartbeat with `last_bar_at` and `last_action_at`
- write decision coverage in heartbeat metrics:
  - `fresh_decision_bar_candidate_count`
  - `stale_decision_bar_candidate_count`
  - `not_ready_candidate_count`
  - `already_processed_candidate_count`
  - `evaluated_candidate_count`
  - `target_decision_bar_ended_at`
- recover per-deployment symbol state after restart
- bind candidates to deployment rows by `model_key`, `account_key`, `book_key`,
  and `mode`, so virtual, paper, and live deployments of the same model do not
  share state by accident

Install the runner data dependencies with:

```bash
uv sync --extra server --extra db --extra rl
```

The DQN loader also needs PyTorch. Install a CPU/GPU Torch wheel that matches
the host. Avoid accidentally pulling a large CUDA build into the API-only
environment.

Run a readiness check before the overnight/morning handoff:

```bash
uv run python scripts/check_operational_readiness.py \
  --api-base http://quant.geisler.se:8000 \
  --skip-local-model-bundles
```

Run the same command on the trading server without `--skip-local-model-bundles`
so it also verifies the trader-local bundle files. The command exits non-zero
when it finds a blocker.

Cancel/exit ownership is intentionally strict: the API mutates broker state only
when the action matches exactly one active RL-generated instruction tagged with
the same deployment and source instruction id. Ambiguous or missing ownership is
a conflict response, not a best-effort cancel.

## Important Model Input Truth

The model does not consume raw 1-minute bars. It expects the promoted `phase1`
contract:

- subscribe to active names once and update source bars from the stream
- aggregate to 5-minute OHLC
- make decisions only on completed 5-minute bars
- include a growing current-day prefix
- include previous-session/history/volatility features
- include market-context features when the model contract says so
- prepend the promoted model's normalized static candidate feature vector

If static features are missing, the runner should not call the DQN. This repo
will mark `features.static_features_ready=false`.

The trading server must not read candidate tapes or model files from a research
checkout. Research paths are allowed only in lineage metadata; runtime model
files come from `RL_MODEL_BUNDLE_ROOT`, and per-name static features come from
Quant API instructions.

## Safe Morning Virtual Test

1. Confirm exactly two deployments exist, one long and one short, both on
   `VIRTUALRL01`.
2. Submit model-routed candidate names for both deployments.
3. Subscribe the active names with `/v1/market-data/stream/subscribe`.
4. For each name, build observations from the stream plus static features.
5. Confirm `model_decision.ready=true` only at completed 5-minute boundaries.
6. Force or observe one long `entry_prevclose_-50bp` decision and one short
   `entry_prevclose_88bp` decision.
7. Call `/v1/rl/actions/translate` with `submit=true` and `log_action=true`.
8. Publish virtual market-watch quotes that do not cross the limit and verify
   orders stay submitted.
9. Publish crossing quotes and verify virtual fills.
10. Run `/v1/runtime/run-once` and check `/v1/read/rl-dashboard`.

## Live Switch Rule

Virtual and live use the same model input and action mapping. The switch to live
should only change:

- deployment `account_key`
- deployment `mode`
- deployment risk limits
- real IBKR account environment variables
- operator kill-switch posture

Do not change the action mapping or feature contract when switching modes.
The runner defaults to `--account-mode virtual`. Use `--account-mode live` only
after live deployment rows exist. Actual paper/live order submission also
requires `--execute-broker`; virtual submission uses `--execute-virtual`.

## Candidate Lifecycle

`MODEL_ROUTED_PENDING` rows are not broker orders. They are the daily model
universe. During the trading window they should remain visible because the
runner revisits each symbol bar by bar. After the window closes, expired source
candidates are archived before the next overnight payload lands. Open generated
instructions, positions, protective exits, and next-open exits stay visible
until they are cancelled, filled, exited, or otherwise resolved.

## Gateway Resilience

The trader should not spam IB Gateway during an outage:

- primary and diagnostic broker sessions use exponential connect cooldown
- market streaming uses the same cooldown and remembers desired symbols
- market streaming auto-reconnect restores the desired watchlist after recovery
- broker monitor skips snapshot refresh when the heartbeat already failed
- background execution runtime retries after broker exceptions with restart backoff

The API and dashboard should remain available while broker access is degraded.

## Stream Capacity Policy

The runner and API now use an explicit stream cap. The default is:

```dotenv
MARKET_STREAM_MAX_SUBSCRIPTIONS=120
```

The runner prioritizes active candidate symbols over dashboard benchmark
symbols. If the desired set is larger than the cap, overflow symbols are
reported in the runner heartbeat under `stream_plan`. The operational target is
to keep normal long plus short RL universes comfortably below the cap; if the
upstream payload starts producing larger daily universes, raise the cap only
after checking IBKR market-data line limits and host resource usage.

Dashboard truth rule:

- `Queued` means the candidate exists for the day.
- `Any Bars` means at least one local stream bar exists.
- `Bar-Ready` means the current completed 5-minute model bar exists.
- `Evaluated` means the runner actually made a model decision for that bar.
- `Stale Bars` means the runner intentionally did not trade because the symbol
  had only an older decision bar.
