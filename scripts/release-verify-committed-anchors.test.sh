#!/usr/bin/env bash
# §2.5 in-tree verification: no maintainer env vars; mismatch fails.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
bin="${GONIMBUS_DECERNOR_BIN:-$(command -v decernor || true)}"
[[ -n "$bin" && -x "$bin" ]] || {
    echo 'error: GONIMBUS_DECERNOR_BIN required (CI installs Decernor from config/release/decernor-ci-sha256.txt)' >&2
    exit 1
}
export GONIMBUS_DECERNOR_BIN="$bin"
# Must not require the maintainer-only pin variables.
env -u GONIMBUS_PGP_KEY_ID -u GONIMBUS_GPG_SIGNING_FINGERPRINT -u GONIMBUS_MINISIGN_PUB \
    "$root/scripts/release-verify-committed-anchors.sh" > /dev/null
scratch="$(mktemp -d)"
restore() {
    cp "$scratch/good.txt" "$root/keys/expected-fingerprints.txt"
    rm -rf "$scratch"
}
cp "$root/keys/expected-fingerprints.txt" "$scratch/good.txt"
trap restore EXIT
# A mutated minisign pin must fail closed.
python3 - "$root/keys/expected-fingerprints.txt" "$scratch/bad.txt" << 'PY'
from pathlib import Path
import sys
text = Path(sys.argv[1]).read_text()
lines = text.splitlines()
lines[1] = 'minisign ' + ('0' * 64)
Path(sys.argv[2]).write_text('\n'.join(lines) + '\n')
PY
cp "$scratch/bad.txt" "$root/keys/expected-fingerprints.txt"
if env -u GONIMBUS_PGP_KEY_ID -u GONIMBUS_GPG_SIGNING_FINGERPRINT -u GONIMBUS_MINISIGN_PUB \
    "$root/scripts/release-verify-committed-anchors.sh" > /dev/null 2>&1; then
    cp "$scratch/good.txt" "$root/keys/expected-fingerprints.txt"
    echo 'error: mismatched minisign anchor was accepted' >&2
    exit 1
fi
cp "$scratch/good.txt" "$root/keys/expected-fingerprints.txt"
echo '[ok] committed-anchor verification uses only in-tree material'
