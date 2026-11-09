#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Write every committed blob at REF under DEST, for scanning.

Usage: export-committed-tree.py ROOT REF DEST

DEST must be an empty directory. Each blob is written as a regular file at its
path; a symlink blob is written as a file holding its target, so its text is
scanned rather than followed. Unlike `git archive`, export-ignore and other
attributes are not honoured. Prints the number of files written.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from committed_tree import committed  # noqa: E402


def main():
    if len(sys.argv) != 4:
        sys.exit("usage: export-committed-tree.py ROOT REF DEST")
    root, ref, dest = sys.argv[1], sys.argv[2], Path(sys.argv[3])
    if not dest.is_dir() or any(dest.iterdir()):
        sys.exit("error: DEST must be an empty directory")
    base = dest.resolve()
    count = 0
    for name, _mode, data in committed(root, ref):
        target = (base / name).resolve()
        if base not in target.parents:
            sys.exit(f"error: committed path escapes the export directory: {name!r}")
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, "xb") as handle:
            handle.write(data)
        count += 1
    print(count)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError) as error:
        sys.exit(f"error: {error}")
