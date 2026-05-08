from __future__ import annotations

import unittest
from unittest.mock import Mock
from unittest.mock import patch

from ibkr_trader.ibkr.gateway_diagnostics import (
    format_gateway_diagnostic_hint,
    read_ibgateway_diagnostics,
)


class GatewayDiagnosticsTest(unittest.TestCase):
    def test_read_ibgateway_diagnostics_detects_stuck_existing_session_shutdown(
        self,
    ) -> None:
        journal = "\n".join(
            (
                "2026-05-08T13:27:14+0000 quant run[1]:     ExistingSessionDetectedAction=primaryoverride",
                "2026-05-08T13:27:38+0000 quant run[1]: 2026-05-08 13:27:38:578 IBC: Login has completed",
                "2026-05-08T13:50:57+0000 quant run[1]: 2026-05-08 13:50:57:309 IBC: detected dialog entitled: Existing session detected; event=Opened",
                "2026-05-08T13:50:57+0000 quant run[1]: 2026-05-08 13:50:57:309 IBC: Other session may be primary, so end this session and let the other one proceed (scenario 6)",
                "2026-05-08T13:50:57+0000 quant run[1]: 2026-05-08 13:50:57:309 IBC: Click button: Cancel",
                "2026-05-08T13:50:57+0000 quant run[1]: 2026-05-08 13:50:57:509 IBC: detected dialog entitled: Shutdown progress; event=Opened",
                "2026-05-08T13:50:57+0000 quant run[1]: 2026-05-08 13:50:57:510 IBC: CommandServer is shutdown",
            )
        )
        completed = Mock(returncode=0, stdout=journal, stderr="")

        with patch("subprocess.run", return_value=completed):
            diagnostics = read_ibgateway_diagnostics(
                unit="ibgateway-ibc.service",
                use_cache=False,
            )

        self.assertEqual(
            diagnostics["status"],
            "stuck_shutdown_after_existing_session",
        )
        self.assertEqual(diagnostics["severity"], "bad")
        self.assertEqual(
            diagnostics["configured_existing_session_action"],
            "primaryoverride",
        )
        self.assertEqual(
            diagnostics["existing_session_action"],
            "Click button: Cancel",
        )
        self.assertEqual(diagnostics["latest_dialog"], "Shutdown progress")

        hint = format_gateway_diagnostic_hint(diagnostics)
        self.assertIsNotNone(hint)
        self.assertIn("existing-session conflict", hint or "")
        self.assertIn(
            "configured ExistingSessionDetectedAction=primaryoverride",
            hint or "",
        )

    def test_unavailable_diagnostics_do_not_emit_hint(self) -> None:
        diagnostics = {
            "status": "unavailable",
            "severity": "warn",
            "summary": "Gateway diagnostics unavailable.",
        }

        self.assertIsNone(format_gateway_diagnostic_hint(diagnostics))

    def test_read_ibgateway_diagnostics_keeps_successful_restart_2fa_context(
        self,
    ) -> None:
        journal = "\n".join(
            (
                "2026-05-08T16:10:01+0000 quant run[1]: 2026-05-08 16:10:01:081 IBC: detected dialog entitled: Restart in progress; event=Opened",
                "2026-05-08T16:17:27+0000 quant run[1]: 2026-05-08 16:17:27:507 IBC: detected dialog entitled: Second Factor Authentication; event=Opened",
                "2026-05-08T16:17:30+0000 quant run[1]: 2026-05-08 16:17:30:378 IBC: Login has completed",
                "2026-05-08T16:17:30+0000 quant run[1]: 2026-05-08 16:17:30:379 IBC: detected dialog entitled: Trader Workstation Configuration; event=Closed",
            )
        )
        completed = Mock(returncode=0, stdout=journal, stderr="")

        with patch("subprocess.run", return_value=completed):
            diagnostics = read_ibgateway_diagnostics(
                unit="ibgateway-ibc.service",
                use_cache=False,
            )

        self.assertEqual(
            diagnostics["status"],
            "login_completed_after_restart_2fa",
        )
        self.assertEqual(diagnostics["severity"], "ok")
        self.assertEqual(
            diagnostics["summary"],
            "IB Gateway login completed after restart/2FA.",
        )
        self.assertEqual(
            diagnostics["restart_in_progress_at"],
            "2026-05-08T16:10:01+0000",
        )
        self.assertEqual(
            diagnostics["second_factor_at"],
            "2026-05-08T16:17:27+0000",
        )


if __name__ == "__main__":
    unittest.main()
