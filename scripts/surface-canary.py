#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Print the canary term of a limensafe catalog.

Usage: surface-canary.py CATALOG REPLACEMENT_ID

The canary is the single alias of the one entity whose replacement_suggestion
is REPLACEMENT_ID. The term is read from the maintainer's catalog at run time,
so this repository never contains it. Fails unless exactly one such entity
with exactly one alias exists and the catalog holds at least one other entity.
"""

import sys


def entities(lines):
    """Yield (aliases, replacement) per entity in a catalog's entities list."""
    current = None
    in_aliases = False
    for raw in lines:
        line = raw.rstrip("\n")
        stripped = line.strip()
        indent = len(line) - len(line.lstrip(" "))
        if stripped.startswith("- id:") and indent <= 4:
            if current is not None:
                yield current
            current = {"aliases": [], "replacement": None}
            in_aliases = False
            continue
        if current is None:
            continue
        if stripped == "aliases:":
            in_aliases = True
            continue
        if in_aliases and stripped.startswith("- "):
            current["aliases"].append(stripped[2:].strip().strip("'\""))
            continue
        in_aliases = False
        if stripped.startswith("replacement_suggestion:"):
            current["replacement"] = stripped.split(":", 1)[1].strip().strip("'\"")
    if current is not None:
        yield current


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: surface-canary.py CATALOG REPLACEMENT_ID")
    path, wanted = sys.argv[1], sys.argv[2]
    with open(path, encoding="utf-8") as handle:
        matches = [e for e in entities(handle) if e["replacement"] == wanted]
    if len(matches) != 1:
        sys.exit(
            f"error: the catalog must hold exactly one {wanted} entity (found {len(matches)})"
        )
    aliases = matches[0]["aliases"]
    if len(aliases) != 1 or not aliases[0]:
        sys.exit(f"error: the {wanted} entity must have exactly one alias")
    with open(path, encoding="utf-8") as handle:
        others = sum(1 for e in entities(handle) if e["replacement"] != wanted)
    if others < 1:
        sys.exit(f"error: the catalog holds no entity besides {wanted}")
    print(aliases[0])


if __name__ == "__main__":
    try:
        main()
    except OSError as error:
        sys.exit(f"error: {error}")
