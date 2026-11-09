# SPDX-License-Identifier: Apache-2.0
"""Read every committed blob in a tree through git, never the working tree.

Shared by content-check.py and surface-scan.sh. Unlike `git archive`, this
does not honour export-ignore or any other attribute: what is committed is
what gets checked. A symlink is a blob whose content is its target path.
"""

import os
import subprocess

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


def committed(root, ref):
    """Yield (path, mode, bytes) for every blob in the tree at REF."""
    env = _git_env()
    base_cmd = [
        "git",
        "--no-replace-objects",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.fsmonitor=false",
        "-C",
        str(root),
    ]
    listing = subprocess.run(
        [*base_cmd, "ls-tree", "-r", "-z", "--full-tree", ref],
        capture_output=True,
        check=True,
        env=env,
    ).stdout
    entries = []
    for record in listing.split(b"\0"):
        if not record:
            continue
        meta, _, name = record.partition(b"\t")
        mode, kind, oid = meta.split(b" ")
        if kind == b"blob":
            entries.append(
                (
                    name.decode("utf-8", "surrogateescape"),
                    mode.decode(),
                    oid.decode(),
                )
            )
    if not entries:
        return
    batch = subprocess.run(
        [*base_cmd, "cat-file", "--batch"],
        input=b"".join(oid.encode() + b"\n" for _, _, oid in entries),
        capture_output=True,
        check=True,
        env=env,
    ).stdout
    offset = 0
    for name, mode, oid in entries:
        header_end = batch.index(b"\n", offset)
        header = batch[offset:header_end].split(b" ")
        if len(header) != 3 or header[0].decode() != oid or header[1] != b"blob":
            raise ValueError(f"unexpected git cat-file output for {name}")
        size = int(header[2])
        start = header_end + 1
        yield name, mode, batch[start : start + size]
        offset = start + size + 1
