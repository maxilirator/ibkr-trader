# Agent Operating Notes

## Live IBKR Access

- The live API/dashboard stack runs on `quant`, currently exposed through
  `http://quant.geisler.se:4173/` for the dashboard and the trader API service
  on that host.
- Do not leave a local trader API, RL runner, broker probe, or stream process
  running from a development machine if it can talk to IB Gateway. Two API
  processes talking to the same Gateway is an operational fault.
- Local runs are for tests and short diagnostics only. Stop any local process
  immediately after the test is done.
- Prefer cached/dashboard/read-model checks locally. Live broker actions,
  service restarts, and Gateway restarts belong on `quant` and require explicit
  operator intent.

