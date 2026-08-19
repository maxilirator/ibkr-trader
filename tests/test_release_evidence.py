"""Tests for the release evidence builder.

The value of this tool is entirely in the two things a commit hash alone cannot
tell you: whether the deployed tree was edited in place, and whether the code
that will actually be imported is the code that was reviewed. Both are asserted
here against real files.
"""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from scripts.build_release_evidence import (
    _dirty_paths,
    build_evidence,
    collect_active_tree,
    collect_import_provenance,
    collect_provenance,
    compare,
)


class DirtyPathParsingTests(TestCase):
    """A fixed-offset slice mis-parsed unstaged entries when the surrounding
    command output had been stripped, silently truncating the first path."""

    def test_handles_staged_unstaged_and_untracked(self) -> None:
        porcelain = " M src/a.py\nM  src/b.py\n?? scripts/c.py\n"
        self.assertEqual(
            _dirty_paths(porcelain), ["scripts/c.py", "src/a.py", "src/b.py"]
        )

    def test_survives_a_stripped_first_line(self) -> None:
        self.assertEqual(_dirty_paths("M src/a.py"), ["src/a.py"])

    def test_empty_input(self) -> None:
        self.assertEqual(_dirty_paths(""), [])
        self.assertEqual(_dirty_paths(None), [])


class ProvenanceTests(TestCase):
    def test_reports_a_commit_and_dirty_state(self) -> None:
        provenance = collect_provenance()
        self.assertIsNotNone(provenance["commit"])
        self.assertIn("dirty", provenance)
        self.assertIsInstance(provenance["dirty_paths"], list)


class ActiveTreeTests(TestCase):
    def test_hashes_real_source_files(self) -> None:
        tree = collect_active_tree()
        self.assertGreater(tree["file_count"], 50)
        self.assertIn("src/ibkr_trader/bootstrap.py", tree["files"])
        self.assertEqual(len(tree["tree_hash"]), 64)

    def test_excludes_bytecode(self) -> None:
        tree = collect_active_tree()
        for name in tree["files"]:
            with self.subTest(name=name):
                self.assertFalse(name.endswith(".pyc"))
                self.assertNotIn("__pycache__", name)


class ImportProvenanceTests(TestCase):
    def test_modules_resolve_inside_the_repository(self) -> None:
        modules = collect_import_provenance()
        for name, item in modules.items():
            with self.subTest(module=name):
                self.assertIsNone(item.get("error"))
                self.assertTrue(item["inside_repo"])
                self.assertEqual(len(item["sha256"]), 64)


class CompareTests(TestCase):
    def _evidence(self) -> dict:
        return build_evidence(with_tests=False)

    def test_identical_trees_match(self) -> None:
        recorded = self._evidence()
        current = json.loads(json.dumps(recorded))
        # Neutralise the dirty flag: this repo may legitimately be dirty while
        # the test runs, and that is asserted separately below.
        recorded["provenance"]["dirty"] = False
        current["provenance"]["dirty"] = False
        matches, findings = compare(recorded, current)
        self.assertTrue(matches, findings)

    def test_an_edit_in_place_is_detected_despite_an_unchanged_commit(self) -> None:
        """The whole point: deployment is a file copy, so a host-side edit leaves
        the commit hash untouched and is invisible to git log."""
        recorded = self._evidence()
        recorded["provenance"]["dirty"] = False
        current = json.loads(json.dumps(recorded))
        target = "src/ibkr_trader/bootstrap.py"
        current["active_tree"]["files"][target] = "0" * 64

        matches, findings = compare(recorded, current)
        self.assertFalse(matches)
        self.assertTrue(any(target in finding for finding in findings))
        self.assertEqual(current["provenance"]["commit"], recorded["provenance"]["commit"])

    def test_missing_and_unexpected_files_are_both_reported(self) -> None:
        recorded = self._evidence()
        recorded["provenance"]["dirty"] = False
        current = json.loads(json.dumps(recorded))
        removed = sorted(recorded["active_tree"]["files"])[0]
        del current["active_tree"]["files"][removed]
        current["active_tree"]["files"]["src/ibkr_trader/rogue.py"] = "1" * 64

        matches, findings = compare(recorded, current)
        self.assertFalse(matches)
        self.assertTrue(any("missing on this tree" in f for f in findings))
        self.assertTrue(any("unexpected on this tree" in f for f in findings))

    def test_a_differing_commit_is_reported(self) -> None:
        recorded = self._evidence()
        recorded["provenance"]["dirty"] = False
        current = json.loads(json.dumps(recorded))
        current["provenance"]["commit"] = "0" * 40

        matches, findings = compare(recorded, current)
        self.assertFalse(matches)
        self.assertTrue(any("commit differs" in f for f in findings))

    def test_a_dirty_current_tree_is_reported(self) -> None:
        recorded = self._evidence()
        recorded["provenance"]["dirty"] = False
        current = json.loads(json.dumps(recorded))
        current["provenance"]["dirty"] = True
        current["provenance"]["dirty_paths"] = ["src/ibkr_trader/bootstrap.py"]

        matches, findings = compare(recorded, current)
        self.assertFalse(matches)
        self.assertTrue(any("dirty" in f for f in findings))


class EvidenceDocumentTests(TestCase):
    def test_document_is_json_serializable_and_complete(self) -> None:
        evidence = build_evidence(with_tests=False)
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "evidence.json"
            path.write_text(json.dumps(evidence, indent=2, sort_keys=True), "utf-8")
            reloaded = json.loads(path.read_text(encoding="utf-8"))

        for key in ("provenance", "active_tree", "imports", "tests", "python"):
            self.assertIn(key, reloaded)
        self.assertEqual(reloaded["schema_version"], 1)
