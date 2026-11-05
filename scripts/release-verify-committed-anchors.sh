#!/usr/bin/env bash
# PDR-0008 §2.5: verify committed public exports against both anchors with the
# designated checker. In-tree material only; no maintainer env vars.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$root"
# shellcheck source=scripts/release-decernor.sh
source "$root/scripts/release-decernor.sh"
resolve_release_decernor ceremony || {
    echo 'error: trusted Decernor identity/version check failed' >&2
    exit 1
}
gpg_pin=docs/security/release-signing-keys.asc
minisign_pin=docs/security/gonimbus-minisign.pub
anchors=keys/expected-fingerprints.txt
anchors_ndjson=keys/expected-fingerprints.ndjson
for path in "$gpg_pin" "$minisign_pin" "$anchors" "$anchors_ndjson"; do
    [[ -f "$path" && -s "$path" && ! -L "$path" ]] || {
        echo "error: committed $path missing or not a regular file" >&2
        exit 1
    }
done
"$RELEASE_DECERNOR_BIN" fingerprint verify \
    --anchors "$anchors" \
    --anchors-ndjson "$anchors_ndjson" \
    --gpg "$gpg_pin" --minisign "$minisign_pin" > /dev/null || {
    echo 'error: committed public exports do not match the fingerprint anchors' >&2
    exit 1
}
echo '[ok] committed public exports match both fingerprint anchors'
