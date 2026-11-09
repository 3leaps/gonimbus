#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify or update the SHA-256 manifest of all committed workflow files."""

from __future__ import annotations

import hashlib
import os
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
PIN_FILE = ROOT / "config" / "release" / "release-workflow.sha256"
LINE_RE = re.compile(r"^([0-9a-f]{64})  (\.github/(?:workflows|actions)/.+)$")
ALLOWED_GIT_ENV = frozenset(
    {
        "GIT_COMMITTER_NAME",
        "GIT_COMMITTER_EMAIL",
        "GIT_AUTHOR_NAME",
        "GIT_AUTHOR_EMAIL",
    }
)


def _git_env() -> dict[str, str]:
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("GIT_") or key in ALLOWED_GIT_ENV
    }
    env["GIT_NO_REPLACE_OBJECTS"] = "1"
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    env["GIT_ATTR_NOSYSTEM"] = "1"
    return env


def _git(*args: str, text: bool = False) -> subprocess.CompletedProcess:
    return subprocess.run(
        [
            "git",
            "--no-replace-objects",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-C",
            str(ROOT),
            *args,
        ],
        capture_output=True,
        text=text,
        check=False,
        env=_git_env(),
    )


def fail(message: str) -> None:
    print(f"error: {message}", file=sys.stderr)
    sys.exit(1)


def sha256_bytes(data: bytes) -> str:
    if b"\r" in data:
        fail("workflow contains CR bytes")
    return hashlib.sha256(data).hexdigest()


def discover_working_paths() -> list[str]:
    paths: list[str] = []
    workflows = ROOT / ".github" / "workflows"
    if workflows.is_dir():
        for path in workflows.iterdir():
            if path.is_file() and path.suffix in (".yml", ".yaml"):
                paths.append(path.relative_to(ROOT).as_posix())
    actions = ROOT / ".github" / "actions"
    if actions.is_dir():
        for path in actions.rglob("*"):
            if path.is_file():
                paths.append(path.relative_to(ROOT).as_posix())
    paths.sort()
    if ".github/workflows/release.yml" not in paths:
        fail("required workflow missing: .github/workflows/release.yml")
    return paths


def discover_commit_paths(commit: str) -> list[str]:
    result = _git(
        "ls-tree",
        "-r",
        "--name-only",
        commit,
        "--",
        ".github/workflows",
        ".github/actions",
        text=True,
    )
    if result.returncode != 0:
        fail(f"cannot list workflow tree at {commit}")
    paths = [
        line
        for line in result.stdout.splitlines()
        if (line.startswith(".github/workflows/") and line.endswith((".yml", ".yaml")))
        or line.startswith(".github/actions/")
    ]
    paths.sort()
    if ".github/workflows/release.yml" not in paths:
        fail(f"required workflow missing at {commit}: .github/workflows/release.yml")
    return paths


def read_pins(pin_bytes: bytes | None = None) -> dict[str, str]:
    raw_bytes = PIN_FILE.read_bytes() if pin_bytes is None else pin_bytes
    if b"\r" in raw_bytes:
        fail("workflow pin manifest contains CR bytes")
    raw = raw_bytes.decode("utf-8")
    pins: dict[str, str] = {}
    ordered_paths: list[str] = []
    for raw_line in raw.splitlines():
        if not raw_line or raw_line.startswith("#"):
            continue
        match = LINE_RE.match(raw_line)
        if not match:
            fail(f"malformed workflow pin line: {raw_line}")
        digest, rel = match.groups()
        if rel in pins:
            fail(f"duplicate workflow pin entry: {rel}")
        pins[rel] = digest
        ordered_paths.append(rel)
    if ordered_paths != sorted(ordered_paths):
        fail("workflow pin entries are not sorted")
    return pins


def compute_working_pins() -> dict[str, str]:
    pins: dict[str, str] = {}
    for rel in discover_working_paths():
        path = ROOT / rel
        if not path.is_file() or path.is_symlink():
            fail(f"missing or symlinked workflow file: {rel}")
        pins[rel] = sha256_bytes(path.read_bytes())
    return pins


def format_pins(pins: dict[str, str]) -> str:
    return "".join(f"{pins[rel]}  {rel}\n" for rel in sorted(pins))


def check() -> None:
    expected = read_pins()
    actual = compute_working_pins()
    if expected != actual:
        fail(
            "workflow files do not match config/release/release-workflow.sha256 "
            "(run: python3 scripts/release-workflow-pin.py update)"
        )
    print(
        f"[ok] {len(actual)} workflow file(s) match config/release/release-workflow.sha256"
    )


def verify_commit(commit: str) -> None:
    head_pins = read_pins()
    pin_blob = _git(
        "cat-file", "blob", f"{commit}:config/release/release-workflow.sha256"
    )
    if pin_blob.returncode != 0:
        fail(f"cannot read config/release/release-workflow.sha256 from commit {commit}")
    commit_pins = read_pins(pin_blob.stdout)
    if commit_pins != head_pins:
        fail(f"commit {commit} workflow pin manifest differs from HEAD")
    commit_paths = discover_commit_paths(commit)
    if sorted(commit_pins) != commit_paths:
        fail(f"commit {commit} workflow tree differs from pinned workflow list")
    for rel, expected_digest in commit_pins.items():
        blob = _git("cat-file", "blob", f"{commit}:{rel}")
        if blob.returncode != 0:
            fail(f"cannot read {rel} from commit {commit}")
        actual_digest = sha256_bytes(blob.stdout)
        if actual_digest != expected_digest:
            fail(
                f"commit {commit} workflow {rel} digest {actual_digest} "
                f"does not match pinned {expected_digest}"
            )
    print(
        f"[ok] commit {commit} workflows match config/release/release-workflow.sha256"
    )


def update() -> None:
    PIN_FILE.parent.mkdir(parents=True, exist_ok=True)
    pins = compute_working_pins()
    PIN_FILE.write_text(format_pins(pins), encoding="utf-8", newline="\n")
    print(f"[ok] updated {PIN_FILE.relative_to(ROOT)} ({len(pins)} files)")


def main(argv: list[str]) -> None:
    mode = argv[1] if len(argv) > 1 else "check"
    if mode == "check" and len(argv) == 2:
        check()
    elif mode == "update" and len(argv) == 2:
        update()
    elif mode == "verify-commit" and len(argv) == 3:
        verify_commit(argv[2])
    else:
        fail("usage: release-workflow-pin.py [check|update|verify-commit <commit>]")


if __name__ == "__main__":
    main(sys.argv)
