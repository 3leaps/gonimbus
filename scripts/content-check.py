#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Refuse generic secret and local-path shapes in tracked files.

Usage: content-check.py [ROOT [REF]]   (default: the repository root, HEAD)

Vocabulary-free and safe to run anywhere, including CI: it looks for shapes,
never for names. It refuses a home directory under /Users or /home, the root
home, and a Windows user profile (except synthetic fixture names), a
private-key block (OpenSSH, PKCS, EC or OpenPGP),
GitHub and AWS access tokens, and developer "remove before commit" markers.
It reads the committed blobs at REF, symlink targets included, through git;
the working tree is never read, so a local edit cannot hide a committed leak.

The organization's own vocabulary is a separate, maintainer-only scan
(scripts/surface-scan.sh); it never runs in CI.
"""

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from committed_tree import committed  # noqa: E402

# Synthetic home names used by fixtures and CI runners, never real people.
SYNTHETIC = r"(?:someone|operator|user|example|runner|runneradmin|ubuntu)"
# A name matches the allowlist only as a whole name (someone2 is not someone).
NAME_END = r"(?![A-Za-z0-9._-])"
SHAPES = {
    # Any user name: mixed case, digits, a single character, with or without a
    # trailing slash. Not preceded by a URL or path character.
    "home directory": re.compile(
        r"(?<![A-Za-z0-9_.:/-])/(?:Users|home)/(?!" + SYNTHETIC + NAME_END + r")"
        r"[A-Za-z0-9_][A-Za-z0-9._-]{0,63}" + NAME_END
    ),
    "Windows user profile": re.compile(
        r"\b[A-Za-z]:[\\/]Users[\\/](?!" + SYNTHETIC + NAME_END + r")"
        r"[A-Za-z0-9_][A-Za-z0-9._ -]{0,63}?(?=[\\/\"'\s]|$)",
        re.IGNORECASE,
    ),
    # The root user's home holds keys and shell history on any Unix host.
    "root home directory": re.compile(r"(?<![A-Za-z0-9_.:/-])/root/[A-Za-z0-9._-]"),
    # OpenSSH, PKCS#1/#8, EC and encrypted keys, and the OpenPGP secret-key block.
    "private key block": re.compile(
        r"-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY(?: BLOCK)?-----"
    ),
    "GitHub token": re.compile(
        r"\b(?:ghp|gho|ghu|ghs|ghr|github_pat)_[A-Za-z0-9_]{20,}"
    ),
    "AWS access key": re.compile(
        r"\b(?:AKIA|ASIA)(?![0-9A-Z]{9}EXAMPLE\b)[0-9A-Z]{16}\b"
    ),
    # The markers are joined from fragments so this file never holds one whole.
    "remove-before-commit marker": re.compile(
        r"\b(?:"
        + "|".join(
            "_".join(parts)
            for parts in (
                ("DO", "NOT", "COMMIT"),
                ("TODO", "REMOVE", "BEFORE", "PUSH"),
                ("REMOVE", "BEFORE", "COMMIT"),
                ("XXX", "REMOVE", "ME"),
            )
        )
        + r")\b"
    ),
}


def main():
    args = sys.argv[1:]
    root = Path(args[0] if args else Path(__file__).resolve().parent.parent)
    ref = args[1] if len(args) > 1 else "HEAD"
    findings = []
    for rel, _mode, data in committed(root, ref):
        if b"\0" in data[:8192]:
            continue  # binary
        text = data.decode("utf-8", "replace")
        for number, line in enumerate(text.splitlines(), 1):
            for label, shape in SHAPES.items():
                if shape.search(line):
                    findings.append(f"{rel}:{number}: {label}")
    for finding in findings:
        print(f"error: {finding}", file=sys.stderr)
    if findings:
        sys.exit(
            f"content-check: {len(findings)} finding(s); this file is public once pushed"
        )
    print(
        "[ok] content-check: no local paths, keys, tokens or commit markers in tracked files"
    )


if __name__ == "__main__":
    main()
