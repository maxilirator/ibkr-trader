"""Tests for the read-only, non-secret runtime settings registry.

Two properties matter most and are asserted directly: that a secret can never be
declared as a database-backed setting, and that an unparseable stored value is
reported as an error rather than silently replaced by the default (which would
show an operator a value the runtime is not using).
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import patch

from sqlalchemy.orm import sessionmaker

from ibkr_trader.db.base import build_engine, create_schema, session_scope
from ibkr_trader.db.models import RuntimeSettingRecord
from ibkr_trader.settings_registry import (
    SECRET_KEY_MARKERS,
    SETTING_DEFINITIONS,
    SETTING_DEFINITIONS_BY_KEY,
    SettingDefinition,
    SettingsRegistryError,
    SettingType,
    assert_not_secret_key,
    read_settings_registry,
    serialize_settings_registry,
)


class SecretExclusionTests(TestCase):
    def test_no_declared_setting_is_secret_bearing(self) -> None:
        """The non-secret split must be enforced, not merely documented."""
        for definition in SETTING_DEFINITIONS:
            with self.subTest(key=definition.key):
                assert_not_secret_key(definition.key)

    def test_secret_looking_keys_are_rejected(self) -> None:
        for key in (
            "DATABASE_URL",
            "IBKR_GATEWAY_PASSWORD",
            "SOME_API_KEY",
            "OAUTH_TOKEN",
            "SERVICE_CREDENTIAL",
            "APP_SECRET",
            "PG_DSN",
        ):
            with self.subTest(key=key):
                with self.assertRaises(SettingsRegistryError):
                    assert_not_secret_key(key)

    def test_every_marker_is_actually_enforced(self) -> None:
        for marker in SECRET_KEY_MARKERS:
            with self.subTest(marker=marker):
                with self.assertRaises(SettingsRegistryError):
                    assert_not_secret_key(f"PREFIX_{marker}_SUFFIX")

    def test_ordinary_operational_keys_are_allowed(self) -> None:
        for key in (
            "MARKET_STREAM_STALE_AFTER_SECONDS",
            "BROKER_MONITOR_ENABLED",
            "EXECUTION_RUNTIME_INTERVAL_SECONDS",
        ):
            with self.subTest(key=key):
                assert_not_secret_key(key)


class SettingDefinitionParsingTests(TestCase):
    def test_each_type_parses(self) -> None:
        cases = (
            (SettingType.STRING, "  hello  ", "hello"),
            (SettingType.INTEGER, " 42 ", 42),
            (SettingType.FLOAT, " 1.5 ", 1.5),
            (SettingType.BOOLEAN, "TRUE", True),
            (SettingType.BOOLEAN, "off", False),
        )
        for value_type, raw, expected in cases:
            with self.subTest(value_type=value_type, raw=raw):
                definition = SettingDefinition(
                    key="X", value_type=value_type, default=None,
                    description="", category="c",
                )
                self.assertEqual(definition.parse(raw), expected)

    def test_invalid_values_raise(self) -> None:
        cases = (
            (SettingType.INTEGER, "not-a-number"),
            (SettingType.FLOAT, "1.2.3"),
            (SettingType.BOOLEAN, "maybe"),
        )
        for value_type, raw in cases:
            with self.subTest(value_type=value_type, raw=raw):
                definition = SettingDefinition(
                    key="X", value_type=value_type, default=None,
                    description="", category="c",
                )
                with self.assertRaises(ValueError):
                    definition.parse(raw)

    def test_every_default_matches_its_declared_type(self) -> None:
        """A default that does not match its own type would be reported to the
        operator as the effective value while being unusable."""
        expected = {
            SettingType.STRING: str,
            SettingType.INTEGER: int,
            SettingType.FLOAT: float,
            SettingType.BOOLEAN: bool,
        }
        for definition in SETTING_DEFINITIONS:
            with self.subTest(key=definition.key):
                # bool is a subclass of int; check it first.
                if definition.value_type is SettingType.BOOLEAN:
                    self.assertIsInstance(definition.default, bool)
                else:
                    self.assertNotIsInstance(definition.default, bool)
                    self.assertIsInstance(
                        definition.default, expected[definition.value_type]
                    )

    def test_definitions_are_unique_and_described(self) -> None:
        keys = [definition.key for definition in SETTING_DEFINITIONS]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(len(SETTING_DEFINITIONS_BY_KEY), len(keys))
        for definition in SETTING_DEFINITIONS:
            with self.subTest(key=definition.key):
                self.assertTrue(definition.description.strip())
                self.assertTrue(definition.category.strip())


class SettingsRegistryReadTests(TestCase):
    def _session_factory(self, temp_dir: str) -> sessionmaker:
        database_path = Path(temp_dir) / "settings.db"
        engine = build_engine(f"sqlite+pysqlite:///{database_path}")
        create_schema(engine)
        return sessionmaker(bind=engine, expire_on_commit=False)

    def test_absent_rows_report_the_runtime_default(self) -> None:
        with TemporaryDirectory() as temp_dir:
            factory = self._session_factory(temp_dir)
            with patch.dict(os.environ, {}, clear=True):
                snapshot = read_settings_registry(factory)

        self.assertEqual(len(snapshot.settings), len(SETTING_DEFINITIONS))
        self.assertEqual(snapshot.undeclared_keys, ())
        for setting in snapshot.settings:
            with self.subTest(key=setting.key):
                self.assertEqual(setting.runtime_source, "default")
                self.assertEqual(setting.runtime_value, setting.default_value)
                self.assertFalse(setting.has_stored_value)
                self.assertFalse(setting.drifted)
                self.assertIsNone(setting.error)

    def test_stored_row_is_reported_as_stored_not_as_applied(self) -> None:
        """THE critical property. Nothing in the runtime reads this table, so a
        stored value must never be presented as the value in effect."""
        with TemporaryDirectory() as temp_dir:
            factory = self._session_factory(temp_dir)
            with session_scope(factory) as session:
                session.add(
                    RuntimeSettingRecord(
                        setting_key="MARKET_STREAM_STALE_AFTER_SECONDS",
                        value="45",
                        value_type="float",
                        category="market-stream",
                        updated_by="mattias",
                        note="tightened after an outage",
                    )
                )
            snapshot = read_settings_registry(factory)

        setting = next(
            item
            for item in snapshot.settings
            if item.key == "MARKET_STREAM_STALE_AFTER_SECONDS"
        )
        self.assertEqual(setting.stored_value, 45.0)
        self.assertTrue(setting.has_stored_value)
        # The runtime never read the row, so its value is unchanged...
        self.assertEqual(setting.runtime_value, 180.0)
        self.assertEqual(setting.runtime_source, "default")
        # ...and the disagreement is surfaced rather than hidden.
        self.assertTrue(setting.drifted)
        self.assertEqual(setting.updated_by, "mattias")
        self.assertIsNone(setting.error)

    def test_unparseable_stored_value_is_reported_not_hidden(self) -> None:
        """The operator must be able to tell that the stored value is not in use."""
        with TemporaryDirectory() as temp_dir:
            factory = self._session_factory(temp_dir)
            with session_scope(factory) as session:
                session.add(
                    RuntimeSettingRecord(
                        setting_key="MARKET_DATA_BACKFILL_BATCH_SIZE",
                        value="three",
                        value_type="integer",
                        category="market-data",
                    )
                )
            snapshot = read_settings_registry(factory)

        setting = next(
            item
            for item in snapshot.settings
            if item.key == "MARKET_DATA_BACKFILL_BATCH_SIZE"
        )
        self.assertIsNotNone(setting.error)
        self.assertIn("three", setting.error)
        # Shown as unset rather than as a value, and the runtime is unaffected.
        self.assertIsNone(setting.stored_value)
        self.assertEqual(setting.runtime_value, setting.default_value)

    def test_undeclared_stored_rows_are_surfaced(self) -> None:
        """A row nobody declares affects nothing; hiding it would mislead."""
        with TemporaryDirectory() as temp_dir:
            factory = self._session_factory(temp_dir)
            with session_scope(factory) as session:
                session.add(
                    RuntimeSettingRecord(
                        setting_key="MARKET_STREAM_STALE_AFTER_SECOND",  # typo
                        value="45",
                        value_type="float",
                    )
                )
            snapshot = read_settings_registry(factory)

        self.assertIn("MARKET_STREAM_STALE_AFTER_SECOND", snapshot.undeclared_keys)

    def test_payload_is_serializable_and_marked_read_only(self) -> None:
        with TemporaryDirectory() as temp_dir:
            factory = self._session_factory(temp_dir)
            payload = serialize_settings_registry(read_settings_registry(factory))

        self.assertTrue(payload["read_only"])
        self.assertFalse(payload["stored_values_are_applied"])
        self.assertEqual(payload["error_count"], 0)
        self.assertEqual(payload["drift_count"], 0)
        self.assertEqual(len(payload["settings"]), len(SETTING_DEFINITIONS))
        self.assertIn("market-stream", payload["categories"])

    def test_payload_contains_no_secret_bearing_key(self) -> None:
        with TemporaryDirectory() as temp_dir:
            factory = self._session_factory(temp_dir)
            payload = serialize_settings_registry(read_settings_registry(factory))

        serialized = repr(payload).upper()
        for marker in SECRET_KEY_MARKERS:
            with self.subTest(marker=marker):
                self.assertNotIn(marker, serialized)


class SettingsApiTests(TestCase):
    """The endpoint must be read-only and must not expose a write path."""

    def _app(self, database_url: str):
        from ibkr_trader.api.server import create_app
        from ibkr_trader.config import ApiServerConfig, AppConfig, IbkrConnectionConfig

        return create_app(
            AppConfig(
                environment="test",
                timezone="Europe/Stockholm",
                database_url=database_url,
                session_calendar_path=Path("/tmp/day_sessions.parquet"),
                stockholm_instruments_path=Path("/tmp/all.txt"),
                stockholm_identity_path=Path("/tmp/identity.parquet"),
                api=ApiServerConfig(
                    host="127.0.0.1", port=8000, require_loopback_only=False
                ),
                ibkr=IbkrConnectionConfig(
                    host="127.0.0.1",
                    port=4001,
                    client_id=0,
                    diagnostic_client_id=7,
                    streaming_client_id=9,
                    account_id="DU1234567",
                ),
                broker_warmup_enabled=False,
            )
        )

    def test_endpoint_returns_the_declared_registry(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "settings_api.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            engine.dispose()

            with TestClient(self._app(database_url)) as client:
                response = client.get("/v1/settings")

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["accepted"])
        self.assertTrue(body["read_only"])
        self.assertEqual(len(body["settings"]), len(SETTING_DEFINITIONS))
        self.assertFalse(body["stored_values_are_applied"])
        for row in body["settings"]:
            self.assertIn("runtime_value", row)
            self.assertIn("stored_value", row)
            self.assertIn("runtime_source", row)
            self.assertIn("drifted", row)

    def test_there_is_no_settings_write_endpoint(self) -> None:
        """The registry reports state; it must not become a second way to
        change trading behaviour."""
        try:
            import fastapi.testclient  # noqa: F401  - availability check only
        except (ModuleNotFoundError, RuntimeError):
            self.skipTest("fastapi test dependencies are not installed")

        with TemporaryDirectory() as temp_dir:
            database_path = Path(temp_dir) / "settings_api_ro.db"
            database_url = f"sqlite+pysqlite:///{database_path}"
            engine = build_engine(database_url)
            create_schema(engine)
            engine.dispose()

            app = self._app(database_url)
            settings_routes = [
                (route.path, sorted(route.methods))
                for route in app.routes
                if getattr(route, "path", "").startswith("/v1/settings")
            ]

        self.assertTrue(settings_routes)
        for path, methods in settings_routes:
            with self.subTest(path=path):
                self.assertNotIn("POST", methods)
                self.assertNotIn("PUT", methods)
                self.assertNotIn("PATCH", methods)
                self.assertNotIn("DELETE", methods)


class RuntimeValueTruthfulnessTests(TestCase):
    """The registry must never claim the runtime is using a value it is not.

    `config.py` resolves configuration from the process environment; nothing
    reads the `runtime_setting` table. Presenting a stored row as the effective
    value would be a fabricated success state, which is the specific failure the
    repository rules prohibit.
    """

    def _session_factory(self, temp_dir: str) -> sessionmaker:
        database_path = Path(temp_dir) / "truth.db"
        engine = build_engine(f"sqlite+pysqlite:///{database_path}")
        create_schema(engine)
        return sessionmaker(bind=engine, expire_on_commit=False)

    def test_no_declared_setting_is_read_from_the_database_by_config(self) -> None:
        """Guards the assumption the reporting is built on. If a future change
        wires the table into config.py, this test should fail and the wording
        ('stored, not applied') must be revisited."""
        source = Path("src/ibkr_trader/config.py").read_text(encoding="utf-8")
        self.assertNotIn("RuntimeSettingRecord", source)
        self.assertNotIn("settings_registry", source)

    def test_runtime_value_follows_the_environment_not_the_database(self) -> None:
        with TemporaryDirectory() as temp_dir:
            factory = self._session_factory(temp_dir)
            with session_scope(factory) as session:
                session.add(
                    RuntimeSettingRecord(
                        setting_key="MARKET_STREAM_MAX_SUBSCRIPTIONS",
                        value="7",
                        value_type="integer",
                    )
                )
            with patch.dict(
                os.environ, {"MARKET_STREAM_MAX_SUBSCRIPTIONS": "33"}, clear=True
            ):
                snapshot = read_settings_registry(factory)

        setting = next(
            item
            for item in snapshot.settings
            if item.key == "MARKET_STREAM_MAX_SUBSCRIPTIONS"
        )
        self.assertEqual(setting.runtime_value, 33)
        self.assertEqual(setting.runtime_source, "environment")
        self.assertEqual(setting.stored_value, 7)
        self.assertTrue(setting.drifted)

    def test_agreeing_values_are_not_reported_as_drift(self) -> None:
        with TemporaryDirectory() as temp_dir:
            factory = self._session_factory(temp_dir)
            with session_scope(factory) as session:
                session.add(
                    RuntimeSettingRecord(
                        setting_key="MARKET_STREAM_MAX_SUBSCRIPTIONS",
                        value="33",
                        value_type="integer",
                    )
                )
            with patch.dict(
                os.environ, {"MARKET_STREAM_MAX_SUBSCRIPTIONS": "33"}, clear=True
            ):
                snapshot = read_settings_registry(factory)

        setting = next(
            item
            for item in snapshot.settings
            if item.key == "MARKET_STREAM_MAX_SUBSCRIPTIONS"
        )
        self.assertFalse(setting.drifted)

    def test_invalid_environment_value_is_reported_not_hidden(self) -> None:
        with TemporaryDirectory() as temp_dir:
            factory = self._session_factory(temp_dir)
            with patch.dict(
                os.environ, {"MARKET_STREAM_MAX_SUBSCRIPTIONS": "lots"}, clear=True
            ):
                snapshot = read_settings_registry(factory)

        setting = next(
            item
            for item in snapshot.settings
            if item.key == "MARKET_STREAM_MAX_SUBSCRIPTIONS"
        )
        self.assertIsNotNone(setting.error)
        self.assertIn("lots", setting.error)
        self.assertEqual(setting.runtime_value, setting.default_value)

    def test_declared_defaults_match_config_defaults(self) -> None:
        """A default shown on the dashboard that differs from the real code
        default is misinformation to an operator."""
        source = Path("src/ibkr_trader/config.py").read_text(encoding="utf-8")
        for definition in SETTING_DEFINITIONS:
            with self.subTest(key=definition.key):
                match = re.search(
                    rf'getenv\(\s*"{re.escape(definition.key)}",\s*\n?\s*"([^"]*)"',
                    source,
                )
                if match is None:
                    self.skipTest(f"{definition.key} not read via getenv in config.py")
                self.assertEqual(
                    definition.parse(match.group(1)),
                    definition.default,
                    f"{definition.key}: registry default disagrees with config.py",
                )
