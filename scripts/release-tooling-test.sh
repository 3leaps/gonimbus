#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$root"
suites="$root/config/release/tooling-suites.txt"
[[ -f "$suites" && ! -L "$suites" ]] || {
    echo 'error: missing config/release/tooling-suites.txt' >&2
    exit 1
}
expected="$(LC_ALL=C sort "$suites")"
actual="$(find "$root/scripts" -maxdepth 1 -name '*.test.sh' -print | sed 's|.*/||; s|\.test\.sh$||' | LC_ALL=C sort)"
[[ "$expected" == "$actual" ]] || {
    echo 'error: scripts/*.test.sh does not match config/release/tooling-suites.txt' >&2
    diff -u <(printf '%s\n' "$expected") <(printf '%s\n' "$actual") >&2 || true
    exit 1
}
while IFS= read -r suite; do
    [[ -n "$suite" ]] || continue
    "$root/scripts/$suite.test.sh"
done < "$suites"
echo "[ok] all $(wc -l < "$suites" | tr -d ' ') release tooling suites passed"
