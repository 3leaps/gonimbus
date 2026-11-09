#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify dev and release prerequisite tools and minimum versions."""

from __future__ import annotations

from pathlib import Path
import re
import subprocess
import sys

SCOPES: dict[str, tuple[str, ...]] = {
    "dev": ("go", "goneat", "git", "python3", "jq", "shasum"),
    "release": (
        "go",
        "goneat",
        "decernor",
        "limensafe",
        "minisign",
        "gpg",
        "gpgv",
        "gh",
        "shasum",
        "python3",
        "git",
        "jq",
    ),
}

PROBES: dict[str, dict[str, object]] = {
    "go": {
        "cmd": ["go", "version"],
        "regex": r"go([0-9]+\.[0-9]+(?:\.[0-9]+)?)",
        "hint": "Install Go >= 1.26: https://go.dev/dl/",
    },
    "goneat": {
        "cmd": ["goneat", "version"],
        "regex": r"v?([0-9]+\.[0-9]+\.[0-9]+)",
        "hint": "Run: make bootstrap",
    },
    "decernor": {
        "cmd": ["decernor", "version"],
        "regex": r"decernor\s+v?([0-9]+\.[0-9]+\.[0-9]+)",
        "hint": "Install decernor >= 0.1.9 via sfetch --repo 3leaps/decernor --latest",
    },
    "limensafe": {
        "cmd": ["limensafe", "version"],
        "regex": r"limensafe\s+v?([0-9]+\.[0-9]+\.[0-9]+)",
        "hint": "Install limensafe >= 0.2.0 via sfetch --repo 3leaps/limensafe --latest",
    },
    "minisign": {
        "cmd": ["minisign", "-v"],
        "regex": r"minisign\s+([0-9]+\.[0-9]+(?:\.[0-9]+)?)",
        "hint": "Install minisign (e.g. brew install minisign)",
    },
    "gpg": {
        "cmd": ["gpg", "--version"],
        "regex": r"gpg \(GnuPG\) ([0-9]+\.[0-9]+\.[0-9]+)",
        "hint": "Install GnuPG >= 2.4 (e.g. brew install gnupg)",
    },
    "gpgv": {
        "cmd": ["gpgv", "--version"],
        "regex": r"gpgv \(GnuPG\) ([0-9]+\.[0-9]+\.[0-9]+)",
        "hint": "Install GnuPG >= 2.4 (e.g. brew install gnupg)",
    },
    "gh": {
        "cmd": ["gh", "--version"],
        "regex": r"gh version ([0-9]+\.[0-9]+\.[0-9]+)",
        "hint": "Install GitHub CLI (e.g. brew install gh)",
    },
    "shasum": {
        "cmd": ["shasum", "-v"],
        "regex": r"([0-9]+\.[0-9]+(?:\.[0-9]+)?)",
        "hint": "Install perl shasum utility",
    },
    "python3": {
        "cmd": ["python3", "--version"],
        "regex": r"Python ([0-9]+\.[0-9]+\.[0-9]+)",
        "hint": "Install Python >= 3.11",
    },
    "git": {
        "cmd": ["git", "--version"],
        "regex": r"git version ([0-9]+\.[0-9]+\.[0-9]+)",
        "hint": "Install Git >= 2.39",
    },
    "jq": {
        "cmd": ["jq", "--version"],
        "regex": r"jq-([0-9]+\.[0-9]+(?:\.[0-9]+)?)",
        "hint": "Install jq (e.g. brew install jq)",
    },
}


def parse_semver(raw: str) -> tuple[int, int, int]:
    parts = raw.strip().split(".")
    if len(parts) == 2:
        parts.append("0")
    if len(parts) != 3 or not all(p.isdigit() for p in parts):
        raise ValueError(f"invalid version {raw!r}")
    return int(parts[0]), int(parts[1]), int(parts[2])


def load_min_versions(path: Path) -> dict[str, tuple[int, int, int]]:
    versions: dict[str, tuple[int, int, int]] = {}
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split()
        if len(fields) != 2:
            raise ValueError(f"{path}:{line_no}: expected '<tool> <min_semver>'")
        tool, ver = fields
        if tool in versions:
            raise ValueError(f"{path}:{line_no}: duplicate tool {tool!r}")
        versions[tool] = parse_semver(ver)
    return versions


def probe_tool(tool: str, min_version: tuple[int, int, int]) -> tuple[bool, str]:
    spec = PROBES[tool]
    cmd = list(spec["cmd"])  # type: ignore[arg-type]
    pattern = re.compile(str(spec["regex"]))
    hint = str(spec["hint"])
    min_str = ".".join(str(x) for x in min_version)
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except FileNotFoundError:
        return False, f"{tool}: not found (need >= {min_str}; {hint})"
    except subprocess.TimeoutExpired:
        return False, f"{tool}: version probe timed out"
    output = f"{proc.stdout}\n{proc.stderr}"
    if proc.returncode != 0:
        return False, f"{tool}: probe exited {proc.returncode} ({hint})"
    match = pattern.search(output)
    if not match:
        return False, f"{tool}: could not parse version from output ({hint})"
    found_raw = match.group(1)
    try:
        found = parse_semver(found_raw)
    except ValueError as exc:
        return False, f"{tool}: {exc}"
    if found < min_version:
        return (
            False,
            f"{tool}: found {found_raw}, need >= {min_str} ({hint})",
        )
    return True, f"{tool}: {found_raw} (>= {min_str})"


def main() -> int:
    scope = sys.argv[1] if len(sys.argv) > 1 else "dev"
    if scope not in SCOPES:
        print(
            f"error: unknown doctor scope {scope!r} (expected dev or release)",
            file=sys.stderr,
        )
        return 2
    root = Path(__file__).resolve().parent.parent
    versions_file = root / "config" / "doctor-versions.txt"
    try:
        min_versions = load_min_versions(versions_file)
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    failures = 0
    for tool in SCOPES[scope]:
        if tool not in min_versions:
            print(
                f"error: {tool} missing from {versions_file}",
                file=sys.stderr,
            )
            return 2
        ok, message = probe_tool(tool, min_versions[tool])
        prefix = "[ok]" if ok else "[missing]"
        stream = sys.stdout if ok else sys.stderr
        print(f"{prefix} {message}", file=stream)
        if not ok:
            failures += 1
    if failures:
        print(
            f"error: doctor ({scope}) found {failures} issue(s)",
            file=sys.stderr,
        )
        return 1
    print(f"[ok] doctor ({scope}) passed ({len(SCOPES[scope])} tools verified)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
