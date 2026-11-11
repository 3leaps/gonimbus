#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Verify only the pinned upstream durable-index/v0 fixture corpus; no backend
# or runtime conformance claim is produced here.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! command -v goneat > /dev/null 2>&1; then
    if [[ -n "${HOME:-}" && -x "$HOME/.local/bin/goneat" ]]; then
        export PATH="$HOME/.local/bin:$PATH"
    else
        echo "error: goneat not found on PATH; run 'make bootstrap' first" >&2
        exit 1
    fi
fi

fixture_root="$(mktemp -d "${TMPDIR:-/tmp}/gonimbus-contract-fixtures.XXXXXX")"
trap 'rm -rf "$fixture_root"' EXIT HUP INT TERM

cp -R internal/contracts/durableindex/v0/upstream/. "$fixture_root/"
go run ./internal/contracts/durableindex/cmd/verifyassets "$fixture_root"

(
    cd "$fixture_root"
    "${PYTHON:-python3}" -I -B scripts/test-durable-index-controls.py
    if command -v node > /dev/null 2>&1 && node -e 'const [maj, min] = process.versions.node.split(".").map(Number); process.exit(maj > 22 || (maj === 22 && min >= 6) ? 0 : 1)' 2> /dev/null; then
        node --experimental-strip-types scripts/durable-index-reader.ts
    fi
)

printf '%s\n' '[ok] contract-fixture-integrity: pinned durable-index/v0 corpus verified (fixture integrity only)'
