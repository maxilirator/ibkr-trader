"""Tests for source-independent, fail-closed bootstrap configuration.

The important assertions here are the negative ones: that production *refuses to
start* rather than quietly substituting a development default. Each unsafe
default that previously existed (throwaway database, paper-trading port) gets a
test proving it is no longer reachable in production.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from ibkr_trader.bootstrap import (
    BOOTSTRAP_ENV_PATH_VAR,
    BootstrapConfigurationError,
    bootstrap_env_path,
    is_production_environment,
    load_runtime_environment,
    require_production_value,
)
from ibkr_trader.config import AppConfig, IbkrConnectionConfig

COMPLETE_BOOTSTRAP = "\n".join(
    (
        "# protected production environment",
        "DATABASE_URL=postgresql://trader:secret@db.internal:5432/ibkr_trader",
        "IBKR_HOST=127.0.0.1",
        "IBKR_PORT=4001",
        "IBKR_ACCOUNT_IDS=U1234567",
        "",
    )
)


def _write_bootstrap(root: Path, text: str = COMPLETE_BOOTSTRAP) -> Path:
    path = root / "bootstrap.env"
    path.write_text(text, encoding="utf-8")
    path.chmod(0o600)
    return path


class ProductionEnvironmentDetectionTests(TestCase):
    def test_production_aliases_are_recognised(self) -> None:
        for value in ("production", "prod", "PRODUCTION", " Prod "):
            with self.subTest(value=value):
                self.assertTrue(is_production_environment(value))

    def test_non_production_values_are_not_production(self) -> None:
        for value in ("dev", "test", "staging", "", None):
            with self.subTest(value=value):
                self.assertFalse(is_production_environment(value))

    def test_live_is_not_a_production_alias(self) -> None:
        """`live` is an RL deployment mode here; the startup trigger must be
        unambiguous, so it is not overloaded to also mean production."""
        self.assertFalse(is_production_environment("live"))


@pytest.mark.real_bootstrap_default
class BootstrapPathTests(TestCase):
    def test_default_path_is_the_protected_etc_location(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(
                bootstrap_env_path(), Path("/etc/ibkr-trader/bootstrap.env")
            )

    def test_override_variable_is_honoured(self) -> None:
        with patch.dict(
            os.environ, {BOOTSTRAP_ENV_PATH_VAR: "/tmp/custom.env"}, clear=True
        ):
            self.assertEqual(bootstrap_env_path(), Path("/tmp/custom.env"))


class ProductionFailClosedTests(TestCase):
    def test_missing_bootstrap_file_refuses_to_start(self) -> None:
        with TemporaryDirectory() as temp_dir:
            missing = Path(temp_dir) / "absent.env"
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(BootstrapConfigurationError) as ctx:
                    load_runtime_environment(
                        environment="production", bootstrap_path=missing
                    )
        self.assertIn("does not exist", str(ctx.exception))
        self.assertIn(str(missing), str(ctx.exception))

    def test_world_readable_bootstrap_file_is_rejected(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = _write_bootstrap(Path(temp_dir))
            path.chmod(0o644)
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(BootstrapConfigurationError) as ctx:
                    load_runtime_environment(
                        environment="production", bootstrap_path=path
                    )
        self.assertIn("world-accessible", str(ctx.exception))

    def test_group_readable_bootstrap_file_is_accepted(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = _write_bootstrap(Path(temp_dir))
            path.chmod(0o640)
            with patch.dict(os.environ, {}, clear=True):
                result = load_runtime_environment(
                    environment="production", bootstrap_path=path
                )
        self.assertTrue(result.is_production)

    def test_incomplete_bootstrap_reports_every_missing_key_at_once(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = _write_bootstrap(
                Path(temp_dir), "IBKR_HOST=127.0.0.1\nIBKR_ACCOUNT_ID=U1\n"
            )
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(BootstrapConfigurationError) as ctx:
                    load_runtime_environment(
                        environment="production", bootstrap_path=path
                    )
        message = str(ctx.exception)
        self.assertIn("DATABASE_URL", message)
        self.assertIn("IBKR_PORT", message)

    def test_missing_account_is_rejected(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = _write_bootstrap(
                Path(temp_dir),
                "DATABASE_URL=postgresql://a@b/c\nIBKR_HOST=h\nIBKR_PORT=4001\n",
            )
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(BootstrapConfigurationError) as ctx:
                    load_runtime_environment(
                        environment="production", bootstrap_path=path
                    )
        self.assertIn("no IBKR account configured", str(ctx.exception))

    def test_empty_required_value_counts_as_missing(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = _write_bootstrap(
                Path(temp_dir),
                "DATABASE_URL=\nIBKR_HOST=h\nIBKR_PORT=4001\nIBKR_ACCOUNT_ID=U1\n",
            )
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(BootstrapConfigurationError) as ctx:
                    load_runtime_environment(
                        environment="production", bootstrap_path=path
                    )
        self.assertIn("DATABASE_URL", str(ctx.exception))

    def test_error_message_never_leaks_a_secret_value(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = _write_bootstrap(
                Path(temp_dir),
                "DATABASE_URL=postgresql://u:SUPERSECRET@h/db\nIBKR_HOST=h\n",
            )
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(BootstrapConfigurationError) as ctx:
                    load_runtime_environment(
                        environment="production", bootstrap_path=path
                    )
        self.assertNotIn("SUPERSECRET", str(ctx.exception))


class ProductionLoadTests(TestCase):
    def test_complete_bootstrap_populates_environment(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = _write_bootstrap(Path(temp_dir))
            with patch.dict(os.environ, {}, clear=True):
                result = load_runtime_environment(
                    environment="production", bootstrap_path=path
                )
                self.assertEqual(os.environ["IBKR_PORT"], "4001")
                self.assertEqual(os.environ["APP_ENV"], "production")

        self.assertTrue(result.is_production)
        self.assertEqual(result.source, "bootstrap")
        self.assertIn("DATABASE_URL", result.applied_keys)

    def test_protected_file_overrides_ambient_values_and_records_it(self) -> None:
        """A protected file that any inherited variable can override is not protected."""
        with TemporaryDirectory() as temp_dir:
            path = _write_bootstrap(Path(temp_dir))
            with patch.dict(os.environ, {"IBKR_PORT": "7497"}, clear=True):
                result = load_runtime_environment(
                    environment="production", bootstrap_path=path
                )
                self.assertEqual(os.environ["IBKR_PORT"], "4001")

        self.assertIn("IBKR_PORT", result.overridden_keys)

    def test_bootstrap_file_cannot_downgrade_app_env(self) -> None:
        """Otherwise the file could disable the enforcement that validated it."""
        with TemporaryDirectory() as temp_dir:
            path = _write_bootstrap(
                Path(temp_dir), COMPLETE_BOOTSTRAP + "APP_ENV=dev\n"
            )
            with patch.dict(os.environ, {}, clear=True):
                load_runtime_environment(
                    environment="production", bootstrap_path=path
                )
                self.assertEqual(os.environ["APP_ENV"], "production")

    def test_production_never_reads_the_checkout_dotenv(self) -> None:
        """Source-independence: the checkout must not supply production values."""
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = _write_bootstrap(root)
            dotenv = root / ".env"
            dotenv.write_text("DATABASE_URL=postgresql://leaked@from/checkout\n", "utf-8")
            with patch.dict(os.environ, {}, clear=True):
                load_runtime_environment(
                    environment="production",
                    bootstrap_path=path,
                    dotenv_path=dotenv,
                )
                self.assertNotIn("leaked", os.environ["DATABASE_URL"])

    def test_audit_payload_contains_key_names_but_no_values(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = _write_bootstrap(Path(temp_dir))
            with patch.dict(os.environ, {}, clear=True):
                payload = load_runtime_environment(
                    environment="production", bootstrap_path=path
                ).to_payload()

        serialized = repr(payload)
        self.assertIn("DATABASE_URL", serialized)
        self.assertNotIn("secret", serialized)


class NonProductionTests(TestCase):
    def test_missing_bootstrap_is_not_fatal_outside_production(self) -> None:
        with TemporaryDirectory() as temp_dir:
            missing = Path(temp_dir) / "absent.env"
            dotenv = Path(temp_dir) / ".env"
            with patch.dict(os.environ, {}, clear=True):
                result = load_runtime_environment(
                    environment="dev", bootstrap_path=missing, dotenv_path=dotenv
                )
        self.assertFalse(result.is_production)
        self.assertEqual(result.source, "none")

    def test_dotenv_is_applied_outside_production(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            dotenv = root / ".env"
            dotenv.write_text("APP_TIMEZONE=Europe/Stockholm\n", encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                result = load_runtime_environment(
                    environment="dev",
                    bootstrap_path=root / "absent.env",
                    dotenv_path=dotenv,
                )
                self.assertEqual(os.environ["APP_TIMEZONE"], "Europe/Stockholm")
        self.assertEqual(result.source, "dotenv")

    def test_explicit_environment_wins_outside_production(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            dotenv = root / ".env"
            dotenv.write_text("APP_TIMEZONE=UTC\n", encoding="utf-8")
            with patch.dict(os.environ, {"APP_TIMEZONE": "Europe/Stockholm"}, clear=True):
                load_runtime_environment(
                    environment="dev",
                    bootstrap_path=root / "absent.env",
                    dotenv_path=dotenv,
                )
                self.assertEqual(os.environ["APP_TIMEZONE"], "Europe/Stockholm")

    def test_world_readable_bootstrap_is_only_a_warning_outside_production(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = _write_bootstrap(root)
            path.chmod(0o644)
            with patch.dict(os.environ, {}, clear=True):
                result = load_runtime_environment(
                    environment="dev",
                    bootstrap_path=path,
                    dotenv_path=root / ".env",
                )
        self.assertTrue(result.warnings)
        self.assertIn("world-accessible", result.warnings[0])


class UnsafeDefaultsAreUnreachableInProductionTests(TestCase):
    """Each previously-reachable unsafe default now fails closed."""

    def test_paper_trading_port_is_not_defaulted_in_production(self) -> None:
        with patch.dict(os.environ, {"APP_ENV": "production"}, clear=True):
            with self.assertRaises(BootstrapConfigurationError) as ctx:
                IbkrConnectionConfig.from_env()
        self.assertIn("IBKR_HOST", str(ctx.exception))

    def test_production_port_is_used_when_supplied(self) -> None:
        with patch.dict(
            os.environ,
            {
                "APP_ENV": "production",
                "IBKR_HOST": "127.0.0.1",
                "IBKR_PORT": "4001",
                "IBKR_ACCOUNT_ID": "U1234567",
            },
            clear=True,
        ):
            config = IbkrConnectionConfig.from_env()
        self.assertEqual(config.port, 4001)

    def test_development_defaults_still_work(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            config = IbkrConnectionConfig.from_env()
        self.assertEqual(config.port, 7497)


class RequireProductionValueTests(TestCase):
    def test_blank_values_are_rejected(self) -> None:
        for value in (None, "", "   "):
            with self.subTest(value=value):
                with self.assertRaises(BootstrapConfigurationError):
                    require_production_value("DATABASE_URL", value)

    def test_value_is_stripped_and_returned(self) -> None:
        self.assertEqual(require_production_value("IBKR_HOST", "  host  "), "host")


class DotenvCannotDeclareProductionTests(TestCase):
    """Regression tests for the escalation hole.

    APP_ENV was read once before the .env load and again after it. A
    checkout-local .env could therefore declare production *after* the gate had
    decided not to engage: the protected file was never opened, and the app ran
    against whatever the source tree supplied - including the throwaway
    DATABASE_URL and the paper IBKR_PORT - while reporting environment=production.
    """

    ESCALATING_DOTENV = "\n".join(
        (
            "APP_ENV=production",
            "DATABASE_URL=postgresql://postgres:postgres@localhost:5432/ibkr_trader",
            "IBKR_HOST=127.0.0.1",
            "IBKR_PORT=7497",
            "IBKR_ACCOUNT_ID=UXXXXXXX",
            "",
        )
    )

    def test_dotenv_declaring_production_is_refused(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            dotenv = root / ".env"
            dotenv.write_text(self.ESCALATING_DOTENV, encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(BootstrapConfigurationError) as ctx:
                    load_runtime_environment(
                        bootstrap_path=root / "absent.env", dotenv_path=dotenv
                    )
        message = str(ctx.exception)
        self.assertIn("must not declare production", message)
        self.assertIn(".env", message)

    def test_escalated_dotenv_never_yields_the_paper_port(self) -> None:
        """The concrete outcome the hole produced."""
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            dotenv = root / ".env"
            dotenv.write_text(self.ESCALATING_DOTENV, encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(BootstrapConfigurationError):
                    load_runtime_environment(
                        bootstrap_path=root / "absent.env", dotenv_path=dotenv
                    )
                # Startup aborted, so nothing consumed the paper port.
                self.assertNotEqual(os.environ.get("IBKR_PORT"), "7497")

    def test_non_production_app_env_from_dotenv_is_still_honoured(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            dotenv = root / ".env"
            dotenv.write_text("APP_ENV=staging\n", encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                result = load_runtime_environment(
                    bootstrap_path=root / "absent.env", dotenv_path=dotenv
                )
        self.assertEqual(result.environment, "staging")
        self.assertFalse(result.is_production)

    def test_app_config_environment_comes_from_the_load_result(self) -> None:
        """A second os.environ read is what allowed the escalation."""
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            dotenv = root / ".env"
            dotenv.write_text(self.ESCALATING_DOTENV, encoding="utf-8")
            with patch.dict(
                os.environ,
                {"IBKR_TRADER_BOOTSTRAP_ENV": str(root / "absent.env")},
                clear=True,
            ):
                with patch("ibkr_trader.config.DEFAULT_ENV_FILE", dotenv):
                    with self.assertRaises(BootstrapConfigurationError):
                        AppConfig.from_env()


class UnreadableBootstrapTests(TestCase):
    """`Path.exists()` propagates EACCES rather than returning False."""

    def _locked_dir(self, root: Path) -> Path:
        protected = root / "etc"
        protected.mkdir()
        path = protected / "bootstrap.env"
        path.write_text(COMPLETE_BOOTSTRAP, encoding="utf-8")
        path.chmod(0o600)
        protected.chmod(0o000)
        return path

    def test_production_reports_a_concrete_access_error(self) -> None:
        if os.geteuid() == 0:
            self.skipTest("root traverses any directory")
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = self._locked_dir(root)
            try:
                with patch.dict(os.environ, {}, clear=True):
                    with self.assertRaises(BootstrapConfigurationError) as ctx:
                        load_runtime_environment(
                            environment="production", bootstrap_path=path
                        )
            finally:
                path.parent.chmod(0o755)
        self.assertIn("could not be accessed", str(ctx.exception))

    def test_non_production_startup_survives_an_unreadable_bootstrap(self) -> None:
        """It is documented as optional outside production; a dev box or CI
        runner with a locked-down /etc must still start."""
        if os.geteuid() == 0:
            self.skipTest("root traverses any directory")
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = self._locked_dir(root)
            try:
                with patch.dict(os.environ, {}, clear=True):
                    result = load_runtime_environment(
                        environment="dev",
                        bootstrap_path=path,
                        dotenv_path=root / "absent.env",
                    )
            finally:
                path.parent.chmod(0o755)
        self.assertFalse(result.is_production)
        self.assertTrue(any("could not be accessed" in w for w in result.warnings))


class PaperPortRefusalTests(TestCase):
    def _bootstrap_with_port(self, root: Path, port: str, extra: str = "") -> Path:
        return _write_bootstrap(
            root,
            "DATABASE_URL=postgresql://u@h/db\nIBKR_HOST=127.0.0.1\n"
            f"IBKR_PORT={port}\nIBKR_ACCOUNT_ID=U1\n{extra}",
        )

    def test_paper_port_is_refused_in_production(self) -> None:
        for port in ("7497", "7496"):
            with self.subTest(port=port):
                with TemporaryDirectory() as temp_dir:
                    path = self._bootstrap_with_port(Path(temp_dir), port)
                    with patch.dict(os.environ, {}, clear=True):
                        with self.assertRaises(BootstrapConfigurationError) as ctx:
                            load_runtime_environment(
                                environment="production", bootstrap_path=path
                            )
                self.assertIn("paper-trading port", str(ctx.exception))

    def test_paper_port_can_be_deliberately_acknowledged(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = self._bootstrap_with_port(
                Path(temp_dir), "7497", "IBKR_ALLOW_PAPER_PORT_IN_PRODUCTION=1\n"
            )
            with patch.dict(os.environ, {}, clear=True):
                result = load_runtime_environment(
                    environment="production", bootstrap_path=path
                )
        self.assertTrue(result.is_production)

    def test_live_port_is_accepted(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = self._bootstrap_with_port(Path(temp_dir), "4001")
            with patch.dict(os.environ, {}, clear=True):
                result = load_runtime_environment(
                    environment="production", bootstrap_path=path
                )
        self.assertTrue(result.is_production)

    def test_unparseable_port_is_reported_by_bootstrap_not_by_int(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = self._bootstrap_with_port(Path(temp_dir), "4001 # live port")
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(BootstrapConfigurationError) as ctx:
                    load_runtime_environment(
                        environment="production", bootstrap_path=path
                    )
        self.assertIn("IBKR_PORT is not an integer", str(ctx.exception))


class BootstrapPermissionTests(TestCase):
    def test_group_writable_file_is_rejected(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = _write_bootstrap(Path(temp_dir))
            path.chmod(0o660)
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(BootstrapConfigurationError) as ctx:
                    load_runtime_environment(
                        environment="production", bootstrap_path=path
                    )
        self.assertIn("group-writable", str(ctx.exception))

    def test_relative_override_path_is_rejected_in_production(self) -> None:
        with patch.dict(
            os.environ, {"IBKR_TRADER_BOOTSTRAP_ENV": "relative/bootstrap.env"}, clear=True
        ):
            with self.assertRaises(BootstrapConfigurationError) as ctx:
                load_runtime_environment(environment="production")
        self.assertIn("absolute", str(ctx.exception))


class EscalationClassRegressionTests(TestCase):
    """Every known route by which a non-production start could become production.

    These are grouped deliberately: three separate bugs all had the same root
    cause - APP_ENV being read from os.environ in more than one place, and the
    candidate file being resolved differently when inspected than when applied.
    """

    def test_duplicate_app_env_key_cannot_smuggle_production(self) -> None:
        """The pre-check and the apply step must resolve duplicates identically.

        Last-wins on inspection and first-wins on apply meant a file could be
        vetted as APP_ENV=dev and applied as APP_ENV=production.
        """
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            dotenv = root / ".env"
            dotenv.write_text(
                "APP_ENV=production\nAPP_ENV=dev\n"
                "DATABASE_URL=postgresql://postgres:postgres@localhost:5432/ibkr_trader\n"
                "IBKR_PORT=7497\n",
                encoding="utf-8",
            )
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(BootstrapConfigurationError):
                    load_runtime_environment(
                        bootstrap_path=root / "absent.env", dotenv_path=dotenv
                    )
                self.assertIsNone(os.environ.get("APP_ENV"))
                self.assertIsNone(os.environ.get("IBKR_PORT"))

    def test_reversed_duplicate_order_resolves_to_the_inspected_value(self) -> None:
        """The mirror case. `dev` first wins, so this is genuinely not an
        escalation - and, critically, the value applied is the value inspected."""
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            dotenv = root / ".env"
            dotenv.write_text("APP_ENV=dev\nAPP_ENV=production\n", encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                result = load_runtime_environment(
                    bootstrap_path=root / "absent.env", dotenv_path=dotenv
                )
                self.assertEqual(os.environ["APP_ENV"], "dev")
        self.assertEqual(result.environment, "dev")
        self.assertFalse(result.is_production)

    def test_bootstrap_file_cannot_escalate_when_the_unit_has_not(self) -> None:
        """The documented cutover's own intermediate state: the file exists but
        the unit does not yet set APP_ENV. It must not self-promote, because the
        production branch - and all its validation - would never run."""
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            path = _write_bootstrap(
                root,
                "APP_ENV=production\nIBKR_HOST=127.0.0.1\n"
                "IBKR_PORT=7497\nIBKR_ACCOUNT_ID=U1\n",
            )
            with patch.dict(os.environ, {}, clear=True):
                with self.assertRaises(BootstrapConfigurationError) as ctx:
                    load_runtime_environment(
                        bootstrap_path=path, dotenv_path=root / "absent.env"
                    )
                self.assertIsNone(os.environ.get("APP_ENV"))
                self.assertIsNone(os.environ.get("IBKR_PORT"))
        self.assertIn("must not declare production", str(ctx.exception))

    def test_home_relative_override_is_refused_in_production(self) -> None:
        """expanduser() ran before the absolute check, so ~/x.env slipped past
        the guard whose own purpose was to reject it."""
        with patch.dict(
            os.environ,
            {"IBKR_TRADER_BOOTSTRAP_ENV": "~/evil-bootstrap.env"},
            clear=True,
        ):
            with self.assertRaises(BootstrapConfigurationError) as ctx:
                load_runtime_environment(environment="production")
        self.assertIn("absolute", str(ctx.exception))

    def test_malformed_production_value_is_refused_not_degraded(self) -> None:
        """`production # live` matches no alias, so it silently became dev: the
        operator believes production is on while the gate disengages."""
        # Note " prod " is NOT here: it strips to a valid alias and is accepted.
        for value in ("production # live", "Production;", '"production"'):
            with self.subTest(value=value):
                with patch.dict(os.environ, {}, clear=True):
                    with self.assertRaises(BootstrapConfigurationError) as ctx:
                        load_runtime_environment(environment=value)
                self.assertIn("not a recognised environment", str(ctx.exception))

    def test_app_env_in_environ_always_agrees_with_the_result(self) -> None:
        """The invariant the whole defect class violated.

        A split brain - result says dev while os.environ says production -
        is how IbkrConnectionConfig reached the paper port while AppConfig
        skipped the DATABASE_URL requirement.
        """
        cases = (
            ("dev", "APP_ENV=dev\n"),
            ("staging", "APP_ENV=staging\n"),
            ("dev", ""),
        )
        for expected, dotenv_text in cases:
            with self.subTest(dotenv=dotenv_text):
                with TemporaryDirectory() as temp_dir:
                    root = Path(temp_dir)
                    dotenv = root / ".env"
                    dotenv.write_text(dotenv_text, encoding="utf-8")
                    with patch.dict(os.environ, {}, clear=True):
                        result = load_runtime_environment(
                            bootstrap_path=root / "absent.env", dotenv_path=dotenv
                        )
                        self.assertEqual(os.environ["APP_ENV"], result.environment)
                        self.assertEqual(result.environment, expected)
                        self.assertFalse(
                            is_production_environment(os.environ["APP_ENV"])
                        )

    def test_non_production_start_is_not_over_blocked(self) -> None:
        """The fix must not make ordinary development startup fail."""
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            dotenv = root / ".env"
            dotenv.write_text("APP_TIMEZONE=UTC\nDATABASE_URL=x\n", encoding="utf-8")
            with patch.dict(os.environ, {}, clear=True):
                result = load_runtime_environment(
                    bootstrap_path=root / "absent.env", dotenv_path=dotenv
                )
                self.assertEqual(os.environ["APP_TIMEZONE"], "UTC")
        self.assertFalse(result.is_production)


class ProductionDetectionPrecisionTests(TestCase):
    """The malformed-production check must not become a startup outage.

    An earlier substring test refused any value containing a production token,
    which would have blocked `nonprod`, `preprod` and `prod-eu` from starting at
    all. A safety check that takes down legitimate environments is worse than
    the silent degradation it was guarding against, so both directions are
    pinned here.
    """

    LEGITIMATE_NON_PRODUCTION = (
        "dev",
        "test",
        "staging",
        "nonprod",
        "non-production",
        "preprod",
        "prod-eu",
        "uat-nonprod",
        "production-mirror",
        "reproduction",
    )

    MALFORMED_PRODUCTION_ATTEMPTS = (
        "production # live",
        "Production;",
        '"production"',
        "'prod'",
        "production.",
    )

    def test_legitimate_non_production_names_start_normally(self) -> None:
        for value in self.LEGITIMATE_NON_PRODUCTION:
            with self.subTest(value=value):
                with TemporaryDirectory() as temp_dir:
                    root = Path(temp_dir)
                    with patch.dict(os.environ, {}, clear=True):
                        result = load_runtime_environment(
                            environment=value,
                            bootstrap_path=root / "absent.env",
                            dotenv_path=root / "absent.dotenv",
                        )
                self.assertFalse(result.is_production)
                self.assertEqual(result.environment, value)

    def test_malformed_production_attempts_are_refused(self) -> None:
        for value in self.MALFORMED_PRODUCTION_ATTEMPTS:
            with self.subTest(value=value):
                with TemporaryDirectory() as temp_dir:
                    root = Path(temp_dir)
                    with patch.dict(os.environ, {}, clear=True):
                        with self.assertRaises(BootstrapConfigurationError):
                            load_runtime_environment(
                                environment=value,
                                bootstrap_path=root / "absent.env",
                                dotenv_path=root / "absent.dotenv",
                            )

    def test_exact_aliases_still_engage_the_production_gate(self) -> None:
        for value in ("production", "prod", " PROD "):
            with self.subTest(value=value):
                with TemporaryDirectory() as temp_dir:
                    root = Path(temp_dir)
                    with patch.dict(os.environ, {}, clear=True):
                        # Engaging the gate with no file present must fail closed.
                        with self.assertRaises(BootstrapConfigurationError) as ctx:
                            load_runtime_environment(
                                environment=value,
                                bootstrap_path=root / "absent.env",
                            )
                self.assertIn("does not exist", str(ctx.exception))
