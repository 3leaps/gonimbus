#!/usr/bin/env bash
# Maintainer-only: derive the two public anchors from approved public exports.
# Never overwrites existing anchors.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd -P)"
# shellcheck source=scripts/release-decernor.sh
source "$root/scripts/release-decernor.sh"
cd "$root"
stop() {
    echo "STOP: $1" >&2
    exit 1
}
resolve_release_decernor ceremony || stop 'trusted Decernor identity/version check failed'
: "${GONIMBUS_MINISIGN_PUB:?public minisign export required}"
pin=docs/security/release-signing-keys.asc
[[ -f "$pin" && -s "$pin" && ! -L "$pin" &&
    -f "$GONIMBUS_MINISIGN_PUB" && -s "$GONIMBUS_MINISIGN_PUB" && ! -L "$GONIMBUS_MINISIGN_PUB" ]] ||
    stop 'both public exports required'
[[ ! -L keys && (! -e keys || -d keys) ]] || stop 'keys must be a regular directory, not a symlink'

require_public_only() {
    local rc
    if "$RELEASE_DECERNOR_BIN" fingerprint "$1" --kind "$2" \
        --class private --fail-on-empty --path-mode none > /dev/null; then
        stop "private material in $2 public export"
    else
        rc=$?
        [[ "$rc" -eq 3 ]] || stop "$2 private-material check failed"
    fi
}
require_public_only "$pin" gpg
require_public_only "$GONIMBUS_MINISIGN_PUB" minisign

scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
"$RELEASE_DECERNOR_BIN" fingerprint "$pin" --class public --kind gpg \
    --format ndjson --path-mode none --gpg-role primary > "$scratch/gpg.ndjson" ||
    stop 'public GPG primary extraction failed (pin must contain exactly one public primary)'
"$RELEASE_DECERNOR_BIN" fingerprint "$GONIMBUS_MINISIGN_PUB" --class public --kind minisign \
    --format ndjson --path-mode none > "$scratch/minisign.ndjson" ||
    stop 'minisign public export rejected'
python3 - "$scratch" << 'PY'
import json
import pathlib
import sys
base = pathlib.Path(sys.argv[1])
g = [json.loads(line) for line in (base / 'gpg.ndjson').read_text().splitlines() if line]
m = [json.loads(line) for line in (base / 'minisign.ndjson').read_text().splitlines() if line and json.loads(line).get('fingerprint_scheme') == 'minisign-public-blob-sha256-v1']
if len(g) != 1 or g[0].get('fingerprint_scheme') != 'openpgp-fingerprint-v1' or g[0].get('key_role') != 'primary':
    raise SystemExit('STOP: pin must contain exactly one public primary')
if len(m) != 1:
    raise SystemExit('STOP: minisign public export rejected')
(base / 'expected-fingerprints.ndjson').write_text(''.join(json.dumps(r, separators=(',', ':')) + '\n' for r in (g[0], m[0])))
(base / 'expected-fingerprints.txt').write_text('gpg ' + g[0]['fingerprint'] + '\nminisign ' + m[0]['fingerprint'] + '\n')
(base / 'gpg.json').write_text(json.dumps(g[0]) + '\n')
(base / 'minisign.json').write_text(json.dumps(m[0]) + '\n')
PY
for kind in gpg minisign; do
    "$RELEASE_DECERNOR_BIN" validate --schema "$root/schemas/fingerprint-record.v0.schema.json" \
        --data "$scratch/$kind.json" > /dev/null
done
"$RELEASE_DECERNOR_BIN" fingerprint "$pin" --class public --kind gpg \
    --format ndjson --path-mode none --gpg-role primary > "$scratch/gpg.verify.ndjson"
"$RELEASE_DECERNOR_BIN" fingerprint "$GONIMBUS_MINISIGN_PUB" --class public --kind minisign \
    --format ndjson --path-mode none > "$scratch/minisign.verify.ndjson"
cmp "$scratch/gpg.ndjson" "$scratch/gpg.verify.ndjson"
cmp "$scratch/minisign.ndjson" "$scratch/minisign.verify.ndjson"
"$root/scripts/install-release-anchors.sh" "$scratch" "$root/keys" \
    "$RELEASE_DECERNOR_BIN" fingerprint verify \
    --anchors "$root/keys/expected-fingerprints.txt" \
    --anchors-ndjson "$root/keys/expected-fingerprints.ndjson" \
    --gpg "$root/docs/security/release-signing-keys.asc" --minisign "$GONIMBUS_MINISIGN_PUB" > /dev/null
if [[ -x "$root/scripts/validate-release-anchors.sh" ]]; then
    "$root/scripts/validate-release-anchors.sh" > /dev/null || {
        rm -f "$root/keys/expected-fingerprints.txt" "$root/keys/expected-fingerprints.ndjson"
        stop 'generated anchors failed validation'
    }
fi
cat "$root/keys/expected-fingerprints.txt"
echo '[ok] generated public fingerprint anchors for review'
