#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Refuse a catalog that is, or is the same file as, anything in a repository.

Usage: surface-catalog-path.py CATALOG ROOT [ROOT...]
"""

import os
import sys

REFUSED = "error: the catalog must be outside this repository and its worktrees"


def identity(path):
    st = os.stat(path)
    return st.st_dev, st.st_ino


def ancestors(path):
    for spelled in (os.path.abspath(path), os.path.realpath(path)):
        directory = os.path.dirname(spelled)
        while True:
            yield directory
            parent = os.path.dirname(directory)
            if parent == directory:
                break
            directory = parent


def main():
    if len(sys.argv) < 3:
        sys.exit("usage: surface-catalog-path.py CATALOG ROOT [ROOT...]")
    catalog = sys.argv[1]
    roots = {identity(root) for root in sys.argv[2:]}
    for directory in ancestors(catalog):
        try:
            if identity(directory) in roots:
                sys.exit(REFUSED)
        except OSError:
            continue
    stat = os.stat(catalog)
    if stat.st_nlink > 1:
        for root in sys.argv[2:]:
            for dirpath, _dirnames, filenames in os.walk(root):
                for name in filenames:
                    try:
                        other = os.lstat(os.path.join(dirpath, name))
                    except OSError:
                        continue
                    if (other.st_dev, other.st_ino) == (stat.st_dev, stat.st_ino):
                        sys.exit(REFUSED + " (hard link)")


if __name__ == "__main__":
    try:
        main()
    except OSError as error:
        sys.exit(f"error: {error}")
