from __future__ import annotations

from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from ibkr_trader.config import ApiServerConfig
from ibkr_trader.config import AppConfig
from ibkr_trader.config import IbkrConnectionConfig
from ibkr_trader.orchestration import runtime_worker
from ibkr_trader.orchestration.runtime_service_state import RuntimeServiceLeaseError


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

    def test_main_delegates_to_persistent_runtime_runner(self) -> None:
        fake_sessions = object()
        with (
            patch.object(runtime_worker.AppConfig, "from_env", return_value=self.app_config),
            patch.object(runtime_worker, "build_engine", return_value=object()),
            patch.object(runtime_worker, "create_session_factory", return_value=object()),
            patch.object(runtime_worker, "CanonicalSyncSessions", return_value=fake_sessions),
            patch.object(runtime_worker, "run_persistent_execution_runtime", return_value=0) as runner,
        ):
            exit_code = runtime_worker.main(["--once", "--interval-seconds", "7"])

        self.assertEqual(exit_code, 0)
        runner.assert_called_once()
        _, kwargs = runner.call_args
        self.assertEqual(kwargs["interval_seconds"], 7.0)
        self.assertTrue(kwargs["once"])
        self.assertFalse(kwargs["skip_startup_reconciliation"])

    def test_main_returns_distinct_exit_code_for_runtime_lease_conflict(self) -> None:
        with (
            patch.object(runtime_worker.AppConfig, "from_env", return_value=self.app_config),
            patch.object(runtime_worker, "build_engine", return_value=object()),
            patch.object(runtime_worker, "create_session_factory", return_value=object()),
            patch.object(runtime_worker, "CanonicalSyncSessions", return_value=object()),
            patch.object(
                runtime_worker,
                "run_persistent_execution_runtime",
                side_effect=RuntimeServiceLeaseError("already owned"),
            ),
        ):
            exit_code = runtime_worker.main(["--once"])

        self.assertEqual(exit_code, 3)
