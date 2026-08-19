#!/usr/bin/env python3
"""Produce verifiable release evidence for a deployment.

The question this answers is narrow and specific: **is the code running on the
host the code someone reviewed?** Deployments here are file copies to
`quant.geisler.se`, so nothing otherwise ties a running process back to a commit.
A release note that merely states a commit hash is an assertion, not evidence.

Four independent checks, because each catches something the others cannot:

* **Provenance** - the commit, branch and dirty state the artefact was built
  from. Establishes what the build *claims* to be.
* **Active-tree comparison** - a content hash of every tracked source file,
  computed from the working tree rather than from git metadata. Catches an
  edit-in-place on the host, which leaves the commit hash untouched and is
  invisible to `git log`.
* **Import provenance** - the resolved filesystem path and hash of each module
  the runtime will actually import. Catches a shadowing install: a stale copy
  earlier on ``sys.path`` means the reviewed source is present but not the code
  that runs.
* **Test result** - the suite outcome for this exact tree.

Read-only: no network, no broker connection, no database, no writes outside the
output file.

Usage::

    python scripts/build_release_evidence.py --output var/release-evidence.json
    python scripts/build_release_evidence.py --compare var/release-evidence.json

``--compare`` re-computes the evidence and diffs it against a previously recorded
file, which is the check to run *on the host after deploying*.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]

#: Modules whose resolved import path is worth pinning: the write path, the
#: broker interface, and the controls that gate trading.
PROVENANCE_MODULES = (
    "ibkr_trader.bootstrap",
    "ibkr_trader.config",
    "ibkr_trader.settings_registry",
    "ibkr_trader.orchestration.operator_controls",
    "ibkr_trader.ibkr.recovery_policy",
    "ibkr_trader.ibkr.order_execution",
    "ibkr_trader.ibkr.short_sale_validation",
    "ibkr_trader.db.models",
)


def _git(*args: str) -> str | None:
    """Run a git command, returning None when git or the repo is unavailable."""
    try:
        result = subprocess.run(
            ["git", "-C", str(REPO_ROOT), *args],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _dirty_paths(porcelain: str | None) -> list[str]:
    """Extract paths from ``git status --porcelain`` output.

    Split on whitespace rather than slicing a fixed offset: the status prefix is
    two columns plus a space, but an unstaged-only change starts with a space,
    and any surrounding strip of the command output shifts every offset by one.
    """
    paths: list[str] = []
    for line in (porcelain or "").splitlines():
        parts = line.split(maxsplit=1)
        if len(parts) == 2 and parts[1].strip():
            paths.append(parts[1].strip())
    return sorted(paths)


def collect_provenance() -> dict[str, Any]:
    """What the build claims to be."""
    dirty = _git("status", "--porcelain")
    return {
        "commit": _git("rev-parse", "HEAD"),
        "branch": _git("rev-parse", "--abbrev-ref", "HEAD"),
        "commit_subject": _git("log", "-1", "--format=%s"),
        "commit_at": _git("log", "-1", "--format=%cI"),
        # A dirty tree is recorded, never silently tolerated: an artefact built
        # from uncommitted changes cannot be reproduced from its commit alone.
        "dirty": bool(dirty) if dirty is not None else None,
        "dirty_paths": _dirty_paths(dirty),
    }


def collect_active_tree(paths: tuple[str, ...] = ("src", "scripts", "ops")) -> dict[str, Any]:
    """Hash the working tree, not git's view of it.

    Deployment is a file copy, so the host's tree can drift from its commit
    without git noticing. Hashing files directly is the only way to detect an
    edit made in place after deployment.
    """
    files: dict[str, str] = {}
    for root_name in paths:
        root = REPO_ROOT / root_name
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
                continue
            files[str(path.relative_to(REPO_ROOT))] = _hash_file(path)

    combined = hashlib.sha256()
    for name in sorted(files):
        combined.update(name.encode())
        combined.update(files[name].encode())

    return {
        "file_count": len(files),
        "tree_hash": combined.hexdigest(),
        "files": files,
    }


def collect_import_provenance() -> dict[str, Any]:
    """Where the runtime's own modules actually resolve from.

    A reviewed file in the checkout proves nothing if an older copy shadows it
    on ``sys.path``. This records the path Python resolves and its hash.
    """
    import importlib

    modules: dict[str, Any] = {}
    for name in PROVENANCE_MODULES:
        try:
            module = importlib.import_module(name)
        except Exception as exc:  # noqa: BLE001 - recorded, not hidden
            modules[name] = {"error": f"{type(exc).__name__}: {exc}"}
            continue
        origin = getattr(module, "__file__", None)
        if origin is None:
            modules[name] = {"error": "module has no __file__"}
            continue
        path = Path(origin).resolve()
        modules[name] = {
            "path": str(path),
            "sha256": _hash_file(path) if path.is_file() else None,
            "inside_repo": str(path).startswith(str(REPO_ROOT)),
        }
    return modules


def run_tests(enabled: bool) -> dict[str, Any]:
    """Run the suite against this exact tree."""
    if not enabled:
        return {"ran": False, "reason": "skipped by --no-tests"}

    executable = REPO_ROOT / ".venv" / "bin" / "pytest"
    command = [str(executable) if executable.exists() else "pytest", "-q"]
    try:
        result = subprocess.run(
            command, cwd=REPO_ROOT, capture_output=True, text=True, check=False
        )
    except OSError as exc:
        return {"ran": False, "reason": f"could not run pytest: {exc}"}

    tail = result.stdout.strip().splitlines()
    return {
        "ran": True,
        "passed": result.returncode == 0,
        "exit_code": result.returncode,
        "summary": tail[-1] if tail else "",
    }


def build_evidence(*, with_tests: bool) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "provenance": collect_provenance(),
        "active_tree": collect_active_tree(),
        "imports": collect_import_provenance(),
        "tests": run_tests(with_tests),
        "python": sys.version.split()[0],
    }


def _summarize(evidence: dict[str, Any]) -> str:
    provenance = evidence["provenance"]
    tree = evidence["active_tree"]
    tests = evidence["tests"]
    lines = [
        f"commit      {provenance['commit']} ({provenance['branch']})",
        f"dirty       {provenance['dirty']}",
        f"tree hash   {tree['tree_hash']}  ({tree['file_count']} files)",
        f"python      {evidence['python']}",
    ]
    if tests.get("ran"):
        lines.append(f"tests       {'PASS' if tests['passed'] else 'FAIL'} - {tests['summary']}")
    else:
        lines.append(f"tests       not run ({tests.get('reason')})")

    outside = [
        name
        for name, item in evidence["imports"].items()
        if item.get("inside_repo") is False
    ]
    errors = [name for name, item in evidence["imports"].items() if item.get("error")]
    if outside:
        lines.append(f"imports     WARNING resolved outside the repo: {', '.join(outside)}")
    if errors:
        lines.append(f"imports     WARNING failed to import: {', '.join(errors)}")
    if not outside and not errors:
        lines.append("imports     all resolved inside the repo")
    return "\n".join(lines)


def compare(recorded: dict[str, Any], current: dict[str, Any]) -> tuple[bool, list[str]]:
    """Diff recorded evidence against the current tree.

    The file-level diff is the point: knowing *which* files differ turns "this
    host does not match the release" into something actionable.
    """
    findings: list[str] = []

    old_commit = recorded["provenance"]["commit"]
    new_commit = current["provenance"]["commit"]
    if old_commit != new_commit:
        findings.append(f"commit differs: recorded {old_commit}, current {new_commit}")

    old_files = recorded["active_tree"]["files"]
    new_files = current["active_tree"]["files"]

    for name in sorted(set(old_files) - set(new_files)):
        findings.append(f"missing on this tree: {name}")
    for name in sorted(set(new_files) - set(old_files)):
        findings.append(f"unexpected on this tree: {name}")
    for name in sorted(set(old_files) & set(new_files)):
        if old_files[name] != new_files[name]:
            findings.append(f"content differs: {name}")

    if current["provenance"]["dirty"]:
        findings.append(
            "current tree is dirty: "
            + ", ".join(current["provenance"]["dirty_paths"][:10])
        )

    return (not findings), findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="write evidence JSON here")
    parser.add_argument(
        "--compare", type=Path, help="compare the current tree against recorded evidence"
    )
    parser.add_argument(
        "--no-tests", action="store_true", help="skip running the test suite"
    )
    args = parser.parse_args(argv)

    evidence = build_evidence(with_tests=not args.no_tests)
    print(_summarize(evidence))

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(evidence, indent=2, sort_keys=True), "utf-8")
        print(f"\nwrote {args.output}")

    if args.compare:
        try:
            recorded = json.loads(args.compare.read_text(encoding="utf-8"))
        except OSError as exc:
            print(f"\ncould not read {args.compare}: {exc}")
            return 2
        matches, findings = compare(recorded, evidence)
        print()
        if matches:
            print("MATCH: this tree is identical to the recorded release.")
        else:
            print(f"MISMATCH: {len(findings)} difference(s) from the recorded release.")
            for finding in findings[:50]:
                print(f"  - {finding}")
            return 1

    if evidence["tests"].get("ran") and not evidence["tests"]["passed"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
