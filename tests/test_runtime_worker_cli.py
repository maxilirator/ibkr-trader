from __future__ import annotations

from datetime import datetime
from datetime import timezone
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from ibkr_trader.config import ApiServerConfig
from ibkr_trader.config import AppConfig
from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.orchestration import runtime_worker
from ibkr_trader.orchestration.runtime_worker import RuntimeCycleIssue
from ibkr_trader.orchestration.runtime_worker import RuntimeCycleResult


def _runtime_result(*, issues: tuple[RuntimeCycleIssue, ...] = ()) -> RuntimeCycleResult:
    cycle_at = datetime(2026, 4, 19, 8, 0, tzinfo=timezone.utc)
    return RuntimeCycleResult(
        cycle_started_at=cycle_at,
        cycle_completed_at=cycle_at,
        runtime_timezone="Europe/Stockholm",
        submitted_entries=(),
        cancelled_entries=(),
        filled_entries=(),
        submitted_exits=(),
        completed_instructions=(),
        issues=issues,
    )


class _FakeBrokerRole:
    def execute(self, operation_name: str, operation: object) -> object:  # pragma: no cover - safety only
        raise AssertionError(f"Unexpected execute call in test: {operation_name}")

    def drain_broker_callback_events(self) -> list[dict[str, object]]:
        return []


class _FakeCanonicalSyncSessions:
    def __init__(self) -> None:
        self.primary = _FakeBrokerRole()
        self.warmup_called = False
        self.shutdown_called = False

    def warmup(self) -> None:
        self.warmup_called = True

    def shutdown(self) -> None:
        self.shutdown_called = True


class RuntimeWorkerCliTests(TestCase):
    def setUp(self) -> None:
        self.app_config = AppConfig(
            environment="test",
            timezone="Europe/Stockholm",
            database_url="sqlite+pysqlite:///:memory:",
            session_calendar_path=Path("/tmp/day_sessions.parquet"),
            stockholm_instruments_path=Path("/tmp/all.txt"),
            stockholm_identity_path=Path("/tmp/identity.parquet"),
            api=ApiServerConfig(
                host="127.0.0.1",
                port=8000,
                require_loopback_only=False,
            ),
            ibkr=IbkrConnectionConfig(
                host="127.0.0.1",
                port=4001,
                client_id=0,
                diagnostic_client_id=7,
                streaming_client_id=9,
                account_id="U25245596",
            ),
        )

    def test_main_runs_startup_reconciliation_before_runtime_loop(self) -> None:
        call_order: list[str] = []
        fake_sessions = _FakeCanonicalSyncSessions()

        with (
            patch.object(runtime_worker.AppConfig, "from_env", return_value=self.app_config),
            patch.object(runtime_worker, "build_engine", return_value=object()),
            patch.object(runtime_worker, "create_session_factory", return_value=object()),
            patch.object(runtime_worker, "CanonicalSyncSessions", return_value=fake_sessions),
            patch.object(
                runtime_worker,
                "run_startup_reconciliation",
                side_effect=lambda *args, **kwargs: call_order.append("startup")
                or _runtime_result(),
            ),
            patch.object(
                runtime_worker,
                "run_runtime_cycle",
                side_effect=lambda *args, **kwargs: call_order.append("runtime")
                or _runtime_result(),
            ),
        ):
            exit_code = runtime_worker.main(["--once"])

        self.assertEqual(exit_code, 0)
        self.assertEqual(call_order, ["startup", "runtime"])
        self.assertTrue(fake_sessions.warmup_called)
        self.assertTrue(fake_sessions.shutdown_called)

    def test_main_blocks_runtime_loop_when_startup_reconciliation_has_issues(self) -> None:
        fake_sessions = _FakeCanonicalSyncSessions()
        startup_issues = (
            RuntimeCycleIssue(
                instruction_id=None,
                stage="broker_snapshot",
                message="snapshot mismatch",
            ),
        )

        with (
            patch.object(runtime_worker.AppConfig, "from_env", return_value=self.app_config),
            patch.object(runtime_worker, "build_engine", return_value=object()),
            patch.object(runtime_worker, "create_session_factory", return_value=object()),
            patch.object(runtime_worker, "CanonicalSyncSessions", return_value=fake_sessions),
            patch.object(
                runtime_worker,
                "run_startup_reconciliation",
                return_value=_runtime_result(issues=startup_issues),
            ),
            patch.object(runtime_worker, "run_runtime_cycle") as run_runtime_cycle,
        ):
            exit_code = runtime_worker.main(["--once"])

        self.assertEqual(exit_code, 2)
        run_runtime_cycle.assert_not_called()
        self.assertTrue(fake_sessions.shutdown_called)

    def test_main_allows_startup_issue_override(self) -> None:
        call_order: list[str] = []
        fake_sessions = _FakeCanonicalSyncSessions()
        startup_issues = (
            RuntimeCycleIssue(
                instruction_id=None,
                stage="broker_snapshot",
                message="snapshot mismatch",
            ),
        )

        with (
            patch.object(runtime_worker.AppConfig, "from_env", return_value=self.app_config),
            patch.object(runtime_worker, "build_engine", return_value=object()),
            patch.object(runtime_worker, "create_session_factory", return_value=object()),
            patch.object(runtime_worker, "CanonicalSyncSessions", return_value=fake_sessions),
            patch.object(
                runtime_worker,
                "run_startup_reconciliation",
                side_effect=lambda *args, **kwargs: call_order.append("startup")
                or _runtime_result(issues=startup_issues),
            ),
            patch.object(
                runtime_worker,
                "run_runtime_cycle",
                side_effect=lambda *args, **kwargs: call_order.append("runtime")
                or _runtime_result(),
            ),
        ):
            exit_code = runtime_worker.main(["--once", "--allow-startup-issues"])

        self.assertEqual(exit_code, 0)
        self.assertEqual(call_order, ["startup", "runtime"])

    def test_main_can_skip_startup_reconciliation_explicitly(self) -> None:
        fake_sessions = _FakeCanonicalSyncSessions()

        with (
            patch.object(runtime_worker.AppConfig, "from_env", return_value=self.app_config),
            patch.object(runtime_worker, "build_engine", return_value=object()),
            patch.object(runtime_worker, "create_session_factory", return_value=object()),
            patch.object(runtime_worker, "CanonicalSyncSessions", return_value=fake_sessions),
            patch.object(runtime_worker, "run_startup_reconciliation") as run_startup_reconciliation,
            patch.object(runtime_worker, "run_runtime_cycle", return_value=_runtime_result()),
        ):
            exit_code = runtime_worker.main(["--once", "--skip-startup-reconciliation"])

        self.assertEqual(exit_code, 0)
        run_startup_reconciliation.assert_not_called()
