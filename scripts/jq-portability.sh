#!/usr/bin/env bash
# SPDX-License-Identifier: MIT OR Apache-2.0
# jq 1.6 (the stock version on Debian 12) does not accept a jq keyword as a
# variable name ($module, $import, $if, ...), while jq 1.7 does. Refuse any
# --arg/--argjson binding to a keyword in tracked scripts, workflows and the
# Makefile, so release steps run with either version.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$root"
keywords='module|import|include|def|if|then|elif|else|end|as|reduce|foreach|try|catch|label|and|or|not|__loc__'
if git grep -n -E -- "--arg(json)?[[:space:]]+(${keywords})[[:space:]]" -- 'scripts/*' '.github/**' Makefile ':!scripts/jq-portability.sh' ':!scripts/jq-portability.test.sh'; then
	echo 'jq-portability: a jq variable is named after a jq keyword (jq 1.6 refuses it)' >&2
	exit 1
fi
# macOS ships GNU Bash 3.2 (/bin/bash), where expanding "${arr[@]}" or
# "${arr[*]}" on an empty array fails under `set -u` (`unbound variable`) and
# an EXIT trap can mask the non-zero status. Every script must parse under
# bash -n, and every `[@]`/`[*]` array expansion must use the Bash 3.2 guard
# `${name[@]+"${name[@]}"}` or `${name[*]+"${name[*]}"}` regardless of how the
# array was declared.
for script in scripts/*.sh; do
	bash -n "$script"
done
"${PYTHON:-python3}" -I - <<'PY'
import pathlib
import re
import sys

SAFE_GUARD = re.compile(
    r'([ \t(=]"?)\$\{([A-Za-z_][A-Za-z0-9_]*)\[([@*])\]\+\"\$\{\2\[\3\]\}\"\}'
)
UNSAFE_ARRAY = re.compile(
    r"\$\{([A-Za-z_][A-Za-z0-9_]*)\[(?!(?:[0-9]+|\$\{#\1\[@\]\}\s*-\s*1)\])"
)

for path in sorted(pathlib.Path("scripts").glob("*.sh")):
    if path.name in ("jq-portability.sh", "jq-portability.test.sh"):
        continue
    raw = path.read_text(encoding="utf-8")
    # Remove single-line safe guards before folding line continuations so a
    # comment or partial prefix ending in "\" or "$" can never join with the
    # next line to manufacture a fake safe guard, and never strip guards when
    # non-comment lines use backtick command substitution.
    scrubbed_lines = []
    for raw_line in raw.split("\n"):
        line = raw_line[:-1] if raw_line.endswith("\r") else raw_line
        if "#" not in line and "`" not in line:
            line = SAFE_GUARD.sub(r"\1", line)
        scrubbed_lines.append(line)
    scrubbed = "\n".join(scrubbed_lines)
    folded = re.sub(r"\\\r?\n", "", scrubbed)
    match = UNSAFE_ARRAY.search(scrubbed) or UNSAFE_ARRAY.search(folded)
    if match:
        print(
            f"jq-portability: {path}: empty array {match.group(1)} expanded without Bash 3.2 ${{arr[@]+\"${{arr[@]}}\"}} guard",
            file=sys.stderr,
        )
        sys.exit(1)
PY
echo '[ok] jq variables avoid jq keywords'
