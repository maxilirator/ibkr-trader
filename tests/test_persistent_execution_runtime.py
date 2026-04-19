from __future__ import annotations

from datetime import datetime
from datetime import timezone
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from ibkr_trader.config import ApiServerConfig
from ibkr_trader.config import AppConfig
from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.db.base import build_engine
from ibkr_trader.db.base import create_schema
from ibkr_trader.db.base import create_session_factory
from ibkr_trader.orchestration.runtime_service_state import read_runtime_service_status
from ibkr_trader.orchestration.runtime_worker import RuntimeCycleIssue
from ibkr_trader.orchestration.runtime_worker import RuntimeCycleResult
from ibkr_trader.orchestration.runtime_worker import run_persistent_execution_runtime


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
        raise AssertionError(f"Unexpected broker execute call in test: {operation_name}")

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


class PersistentExecutionRuntimeTests(TestCase):
    def setUp(self) -> None:
        self.engine = build_engine("sqlite+pysqlite:///:memory:")
        create_schema(self.engine)
        self.session_factory = create_session_factory(self.engine)
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

    def tearDown(self) -> None:
        self.engine.dispose()

    def test_persistent_runtime_records_clean_one_shot_lifecycle(self) -> None:
        fake_sessions = _FakeCanonicalSyncSessions()
        call_order: list[str] = []

        with (
            patch(
                "ibkr_trader.orchestration.runtime_worker.run_startup_reconciliation",
                side_effect=lambda *args, **kwargs: call_order.append("startup")
                or _runtime_result(),
            ),
            patch(
                "ibkr_trader.orchestration.runtime_worker.run_runtime_cycle",
                side_effect=lambda *args, **kwargs: call_order.append("runtime")
                or _runtime_result(),
            ),
        ):
            exit_code = run_persistent_execution_runtime(
                self.session_factory,
                self.app_config,
                fake_sessions,
                interval_seconds=5.0,
                timeout=10,
                once=True,
                emit_results=False,
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(call_order, ["startup", "runtime"])
        self.assertTrue(fake_sessions.warmup_called)
        self.assertTrue(fake_sessions.shutdown_called)

        status = read_runtime_service_status(self.session_factory)
        self.assertEqual(status.status, "STOPPED")
        self.assertIsNotNone(status.last_successful_cycle_at)
        self.assertIsNone(status.owner_token)

    def test_persistent_runtime_marks_startup_blocked_when_issues_are_not_allowed(self) -> None:
        fake_sessions = _FakeCanonicalSyncSessions()
        startup_issues = (
            RuntimeCycleIssue(
                instruction_id=None,
                stage="broker_snapshot",
                message="snapshot mismatch",
            ),
        )

        with (
            patch(
                "ibkr_trader.orchestration.runtime_worker.run_startup_reconciliation",
                return_value=_runtime_result(issues=startup_issues),
            ),
            patch("ibkr_trader.orchestration.runtime_worker.run_runtime_cycle") as run_runtime_cycle,
        ):
            exit_code = run_persistent_execution_runtime(
                self.session_factory,
                self.app_config,
                fake_sessions,
                interval_seconds=5.0,
                timeout=10,
                once=True,
                emit_results=False,
            )

        self.assertEqual(exit_code, 2)
        run_runtime_cycle.assert_not_called()
        status = read_runtime_service_status(self.session_factory)
        self.assertEqual(status.status, "STARTUP_BLOCKED")
        self.assertIsNone(status.owner_token)
